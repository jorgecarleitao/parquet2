use std::convert::TryInto;

use super::utils::ValuesDef;

use parquet::{
    encoding::{hybrid_rle::HybridRleDecoder, Encoding},
    error::Result,
    metadata::ColumnDescriptor,
    page::{split_buffer, DataPage, PrimitivePageDict},
    read::levels::get_bit_width,
    types::NativeType,
};

fn read_buffer_impl<T: NativeType, I: Iterator<Item = u32>>(
    def_levels: I,
    values: &[u8],
    max_def_level: u32,
) -> Vec<Option<T>> {
    let chunks = values.chunks_exact(std::mem::size_of::<T>());
    assert_eq!(chunks.remainder().len(), 0);

    let iterator = ValuesDef::new(chunks, def_levels, max_def_level);

    iterator
        .map(|maybe_id| {
            maybe_id.map(|chunk| {
                // unwrap is infalible due to the chunk size.
                let chunk: T::Bytes = match chunk.try_into() {
                    Ok(v) => v,
                    Err(_) => panic!(),
                };
                T::from_le_bytes(chunk)
            })
        })
        .collect()
}

fn read_buffer<T: NativeType>(
    def_levels: &[u8],
    values: &[u8],
    length: u32,
    def_level_encoding: (&Encoding, i16),
) -> Vec<Option<T>> {
    let max_def_level = def_level_encoding.1 as u32;
    match (def_level_encoding.0, max_def_level == 0) {
        (Encoding::Rle, true) => read_buffer_impl(
            std::iter::repeat(0).take(length as usize),
            values,
            max_def_level,
        ),
        (Encoding::Rle, false) => {
            let num_bits = get_bit_width(def_level_encoding.1);
            let def_levels = HybridRleDecoder::new(def_levels, num_bits, length as usize);
            read_buffer_impl(def_levels, values, max_def_level)
        }
        _ => todo!(),
    }
}

fn read_dict_buffer_impl<T: NativeType, I: Iterator<Item = u32>>(
    def_levels: I,
    values: &[u8],
    length: u32,
    max_def_level: u32,
    dict: &PrimitivePageDict<T>,
) -> Vec<Option<T>> {
    let dict_values = dict.values();

    let bit_width = values[0];
    let values = &values[1..];

    let indices = HybridRleDecoder::new(values, bit_width as u32, length as usize);

    let iterator = ValuesDef::new(indices, def_levels, max_def_level);

    iterator
        .map(|maybe_id| maybe_id.map(|id| dict_values[id as usize]))
        .collect()
}

fn read_dict_buffer<'a, T: NativeType>(
    def_levels: &'a [u8],
    values: &'a [u8],
    length: u32,
    dict: &'a PrimitivePageDict<T>,
    def_level_encoding: (&Encoding, i16),
) -> Vec<Option<T>> {
    let max_def_level = def_level_encoding.1 as u32;
    match (def_level_encoding.0, max_def_level == 0) {
        (Encoding::Rle, true) => read_dict_buffer_impl(
            std::iter::repeat(0).take(length as usize),
            values,
            length,
            max_def_level,
            dict,
        ),
        (Encoding::Rle, false) => {
            let num_bits = get_bit_width(def_level_encoding.1);
            let def_levels = HybridRleDecoder::new(def_levels, num_bits, length as usize);
            read_dict_buffer_impl(def_levels, values, length, max_def_level, dict)
        }
        _ => todo!(),
    }
}

pub fn page_dict_to_vec<T: NativeType>(
    page: &DataPage,
    descriptor: &ColumnDescriptor,
) -> Result<Vec<Option<T>>> {
    assert_eq!(descriptor.max_rep_level(), 0);

    let (_, def_levels, values) = split_buffer(page, descriptor);

    match (&page.encoding(), &page.dictionary_page()) {
        (Encoding::RleDictionary, Some(dict)) | (Encoding::PlainDictionary, Some(dict)) => {
            Ok(read_dict_buffer::<T>(
                def_levels,
                values,
                page.num_values() as u32,
                dict.as_any().downcast_ref().unwrap(),
                (&Encoding::Rle, descriptor.max_def_level()),
            ))
        }
        _ => todo!(),
    }
}

pub fn page_to_vec<T: NativeType>(
    page: &DataPage,
    descriptor: &ColumnDescriptor,
) -> Result<Vec<Option<T>>> {
    assert_eq!(descriptor.max_rep_level(), 0);

    let (_, def_levels, values) = split_buffer(page, descriptor);

    match (&page.encoding(), &page.dictionary_page()) {
        (Encoding::Plain, None) => Ok(read_buffer::<T>(
            def_levels,
            values,
            page.num_values() as u32,
            (
                &page.definition_level_encoding(),
                descriptor.max_def_level(),
            ),
        )),
        _ => todo!(),
    }
}
