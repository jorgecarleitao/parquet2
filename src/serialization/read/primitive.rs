use std::convert::TryInto;

use parquet_format::Encoding;

use super::levels::{consume_level, split_buffer_v1};
use super::utils::ValuesDef;
use crate::encoding::{bitpacking, uleb128};
use crate::error::{ParquetError, Result};
use crate::metadata::ColumnDescriptor;
use crate::read::PageHeader;
use crate::{
    read::{Page, PrimitivePageDict},
    types::NativeType,
};

fn read_buffer<T: NativeType>(
    def_levels: &[u8],
    values: &[u8],
    length: u32,
    def_level_encoding: (&Encoding, i16),
) -> Vec<Option<T>> {
    let def_levels = consume_level(def_levels, length, def_level_encoding);

    let chunks = values.chunks_exact(std::mem::size_of::<T>());
    assert_eq!(chunks.remainder().len(), 0);

    let iterator = ValuesDef::new(chunks, def_levels.into_iter(), def_level_encoding.1 as u32);

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

fn read_dict_buffer<'a, T: NativeType>(
    def_levels: &'a [u8],
    values: &'a [u8],
    length: u32,
    dict: &'a PrimitivePageDict<T>,
    def_level_encoding: (&Encoding, i16),
) -> Vec<Option<T>> {
    let dict_values = dict.values();

    // skip bytes from levels
    let def_levels = consume_level(def_levels, length, def_level_encoding);

    let bit_width = values[0];
    let values = &values[1..];

    let (_, consumed) = uleb128::decode(&values);
    let values = &values[consumed..];

    let indices = bitpacking::Decoder::new(values, bit_width, length as usize);

    let iterator = ValuesDef::new(indices, def_levels.into_iter(), def_level_encoding.1 as u32);

    iterator
        .map(|maybe_id| maybe_id.map(|id| dict_values[id as usize]))
        .collect()
}

pub fn page_dict_to_vec<T: NativeType>(
    page: &Page,
    descriptor: &ColumnDescriptor,
) -> Result<Vec<Option<T>>> {
    assert_eq!(descriptor.max_rep_level(), 0);
    match page.header() {
        PageHeader::V1(header) => match (page.encoding(), page.dictionary_page()) {
            (Encoding::PlainDictionary, Some(dict)) => {
                let (_, def_levels, values) =
                    split_buffer_v1(page.buffer(), false, descriptor.max_def_level() > 0);
                Ok(read_dict_buffer::<T>(
                    def_levels,
                    values,
                    page.num_values() as u32,
                    dict.as_any().downcast_ref().unwrap(),
                    (
                        &header.definition_level_encoding,
                        descriptor.max_def_level(),
                    ),
                ))
            }
            (_, None) => Err(ParquetError::OutOfSpec(
                "A dictionary-encoded page MUST be preceeded by a dictionary page".to_string(),
            )),
            _ => todo!(),
        },
        PageHeader::V2(header) => match (&header.encoding, &page.dictionary_page()) {
            (Encoding::RleDictionary, Some(dict)) | (Encoding::PlainDictionary, Some(dict)) => {
                let (def_levels, values) = page
                    .buffer()
                    .split_at(header.definition_levels_byte_length as usize);
                Ok(read_dict_buffer::<T>(
                    def_levels,
                    values,
                    page.num_values() as u32,
                    dict.as_any().downcast_ref().unwrap(),
                    (&Encoding::Rle, descriptor.max_def_level()),
                ))
            }
            _ => todo!(),
        },
    }
}

pub fn page_to_vec<T: NativeType>(
    page: &Page,
    descriptor: &ColumnDescriptor,
) -> Result<Vec<Option<T>>> {
    assert_eq!(descriptor.max_rep_level(), 0);
    match page.header() {
        PageHeader::V1(header) => match (&header.encoding, &page.dictionary_page()) {
            (Encoding::Plain, None) => {
                let (_, def_levels, values) =
                    split_buffer_v1(page.buffer(), false, descriptor.max_def_level() > 0);
                Ok(read_buffer::<T>(
                    def_levels,
                    values,
                    page.num_values() as u32,
                    (
                        &header.definition_level_encoding,
                        descriptor.max_def_level(),
                    ),
                ))
            }
            _ => todo!(),
        },
        PageHeader::V2(header) => match (&header.encoding, &page.dictionary_page()) {
            (Encoding::Plain, None) => {
                let (def_levels, values) = page
                    .buffer()
                    .split_at(header.definition_levels_byte_length as usize);
                Ok(read_buffer::<T>(
                    def_levels,
                    values,
                    page.num_values() as u32,
                    (&Encoding::Rle, descriptor.max_def_level()),
                ))
            }
            _ => todo!(),
        },
    }
}
