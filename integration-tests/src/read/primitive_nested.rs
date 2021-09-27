use std::convert::TryInto;

use super::Array;

use parquet::{
    encoding::{bitpacking, hybrid_rle::HybridRleDecoder, uleb128, Encoding},
    error::{ParquetError, Result},
    metadata::ColumnDescriptor,
    page::{split_buffer, DataPage, PrimitivePageDict},
    read::levels::get_bit_width,
    types::NativeType,
};

fn read_buffer<T: NativeType>(values: &[u8]) -> impl Iterator<Item = T> + '_ {
    let chunks = values.chunks_exact(std::mem::size_of::<T>());
    chunks.map(|chunk| {
        // unwrap is infalible due to the chunk size.
        let chunk: T::Bytes = match chunk.try_into() {
            Ok(v) => v,
            Err(_) => panic!(),
        };
        T::from_le_bytes(chunk)
    })
}

// todo: generalize i64 -> T
fn compose_array<I: Iterator<Item = u32>, F: Iterator<Item = u32>, G: Iterator<Item = i64>>(
    rep_levels: I,
    def_levels: F,
    max_rep: u32,
    max_def: u32,
    mut values: G,
) -> Array {
    let mut outer = vec![];
    let mut inner = vec![];

    assert_eq!(max_rep, 1);
    assert_eq!(max_def, 3);
    let mut prev_def = 0;
    rep_levels
        .into_iter()
        .zip(def_levels.into_iter())
        .for_each(|(rep, def)| {
            match rep {
                1 => {}
                0 => {
                    if prev_def > 1 {
                        let old = std::mem::take(&mut inner);
                        outer.push(Some(Array::Int64(old)));
                    }
                }
                _ => unreachable!(),
            }
            match def {
                3 => inner.push(Some(values.next().unwrap())),
                2 => inner.push(None),
                1 => outer.push(Some(Array::Int64(vec![]))),
                0 => outer.push(None),
                _ => unreachable!(),
            }
            prev_def = def;
        });
    outer.push(Some(Array::Int64(inner)));
    Array::List(outer)
}

fn read_array_impl<T: NativeType, I: Iterator<Item = i64>>(
    rep_levels: &[u8],
    def_levels: &[u8],
    values: I,
    length: usize,
    rep_level_encoding: (&Encoding, i16),
    def_level_encoding: (&Encoding, i16),
) -> Array {
    let max_rep_level = rep_level_encoding.1 as u32;
    let max_def_level = def_level_encoding.1 as u32;

    match (
        (rep_level_encoding.0, max_rep_level == 0),
        (def_level_encoding.0, max_def_level == 0),
    ) {
        ((Encoding::Rle, true), (Encoding::Rle, true)) => compose_array(
            std::iter::repeat(0).take(length as usize),
            std::iter::repeat(0).take(length as usize),
            max_rep_level,
            max_def_level,
            values,
        ),
        ((Encoding::Rle, false), (Encoding::Rle, true)) => {
            let num_bits = get_bit_width(rep_level_encoding.1);
            let rep_levels = HybridRleDecoder::new(rep_levels, num_bits, length);
            compose_array(
                rep_levels,
                std::iter::repeat(0).take(length as usize),
                max_rep_level,
                max_def_level,
                values,
            )
        }
        ((Encoding::Rle, true), (Encoding::Rle, false)) => {
            let num_bits = get_bit_width(def_level_encoding.1);
            let def_levels = HybridRleDecoder::new(def_levels, num_bits, length);
            compose_array(
                std::iter::repeat(0).take(length as usize),
                def_levels,
                max_rep_level,
                max_def_level,
                values,
            )
        }
        ((Encoding::Rle, false), (Encoding::Rle, false)) => {
            let rep_levels =
                HybridRleDecoder::new(rep_levels, get_bit_width(rep_level_encoding.1), length);
            let def_levels =
                HybridRleDecoder::new(def_levels, get_bit_width(def_level_encoding.1), length);
            compose_array(rep_levels, def_levels, max_rep_level, max_def_level, values)
        }
        _ => todo!(),
    }
}

fn read_array<T: NativeType>(
    rep_levels: &[u8],
    def_levels: &[u8],
    values: &[u8],
    length: u32,
    rep_level_encoding: (&Encoding, i16),
    def_level_encoding: (&Encoding, i16),
) -> Array {
    let values = read_buffer::<i64>(values);
    read_array_impl::<T, _>(
        rep_levels,
        def_levels,
        values,
        length as usize,
        rep_level_encoding,
        def_level_encoding,
    )
}

pub fn page_to_array<T: NativeType>(
    page: &DataPage,
    descriptor: &ColumnDescriptor,
) -> Result<Array> {
    let (rep_levels, def_levels, values) = split_buffer(page, descriptor);

    match (&page.encoding(), &page.dictionary_page()) {
        (Encoding::Plain, None) => Ok(read_array::<T>(
            rep_levels,
            def_levels,
            values,
            page.num_values() as u32,
            (
                &page.repetition_level_encoding(),
                descriptor.max_rep_level(),
            ),
            (
                &page.definition_level_encoding(),
                descriptor.max_def_level(),
            ),
        )),
        _ => todo!(),
    }
}

fn read_dict_array<T: NativeType>(
    rep_levels: &[u8],
    def_levels: &[u8],
    values: &[u8],
    length: u32,
    dict: &PrimitivePageDict<i64>,
    rep_level_encoding: (&Encoding, i16),
    def_level_encoding: (&Encoding, i16),
) -> Array {
    let dict_values = dict.values();

    let bit_width = values[0];
    let values = &values[1..];

    let (_, consumed) = uleb128::decode(values);
    let values = &values[consumed..];

    let indices = bitpacking::Decoder::new(values, bit_width, length as usize);

    let values = indices.map(|id| dict_values[id as usize]);

    read_array_impl::<T, _>(
        rep_levels,
        def_levels,
        values,
        length as usize,
        rep_level_encoding,
        def_level_encoding,
    )
}

pub fn page_dict_to_array<T: NativeType>(
    page: &DataPage,
    descriptor: &ColumnDescriptor,
) -> Result<Array> {
    assert_eq!(descriptor.max_rep_level(), 1);

    let (rep_levels, def_levels, values) = split_buffer(page, descriptor);

    match (page.encoding(), &page.dictionary_page()) {
        (Encoding::PlainDictionary, Some(dict)) => Ok(read_dict_array::<T>(
            rep_levels,
            def_levels,
            values,
            page.num_values() as u32,
            dict.as_any().downcast_ref().unwrap(),
            (
                &page.repetition_level_encoding(),
                descriptor.max_rep_level(),
            ),
            (
                &page.definition_level_encoding(),
                descriptor.max_def_level(),
            ),
        )),
        (_, None) => Err(ParquetError::OutOfSpec(
            "A dictionary-encoded page MUST be preceeded by a dictionary page".to_string(),
        )),
        _ => todo!(),
    }
}
