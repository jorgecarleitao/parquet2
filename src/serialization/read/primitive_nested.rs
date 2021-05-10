use std::convert::TryInto;

use parquet_format::Encoding;

use super::{levels, Array};
use crate::encoding::{bitpacking, uleb128};
use crate::metadata::ColumnDescriptor;
use crate::{
    error::{ParquetError, Result},
    read::PrimitivePageDict,
};
use crate::{read::Page, types::NativeType};

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
    assert!(max_def >= 1);
    let mut first = true;
    rep_levels
        .into_iter()
        .zip(def_levels.into_iter())
        .for_each(|(rep, def)| {
            match rep {
                1 => {}
                0 => {
                    if !first {
                        let old = std::mem::take(&mut inner);
                        outer.push(Some(Array::Int64(old)));
                    }
                }
                _ => unreachable!(),
            }
            match max_def - def {
                0 => inner.push(Some(values.next().unwrap())),
                1 => inner.push(None),
                2 => outer.push(Some(Array::Int64(vec![]))),
                3 => outer.push(None),
                _ => unreachable!(),
            }
            first = false;
        });
    outer.push(Some(Array::Int64(inner)));
    Array::List(outer)
}

fn read_array<'a, T: NativeType>(
    values: &'a [u8],
    length: u32,
    rep_level_encoding: (&Encoding, i16),
    def_level_encoding: (&Encoding, i16),
) -> Array {
    let (values, rep_levels) = levels::consume_level(values, length, rep_level_encoding);
    let (values, def_levels) = levels::consume_level(values, length, def_level_encoding);

    let iter = read_buffer::<i64>(values);

    compose_array(
        rep_levels.into_iter(),
        def_levels.into_iter(),
        rep_level_encoding.1 as u32,
        def_level_encoding.1 as u32,
        iter,
    )
}

pub fn page_to_array<T: NativeType>(page: &Page, descriptor: &ColumnDescriptor) -> Result<Array> {
    match page {
        Page::V1(page) => match (&page.header.encoding, &page.dictionary_page) {
            (Encoding::Plain, None) => Ok(read_array::<T>(
                &page.buffer,
                page.header.num_values as u32,
                (
                    &page.header.repetition_level_encoding,
                    descriptor.max_rep_level(),
                ),
                (
                    &page.header.definition_level_encoding,
                    descriptor.max_def_level(),
                ),
            )),
            _ => todo!(),
        },
        Page::V2(_) => todo!(),
    }
}

fn read_dict_array<'a>(
    values: &'a [u8],
    length: u32,
    dict: &'a PrimitivePageDict<i64>,
    rep_level_encoding: (&Encoding, i16),
    def_level_encoding: (&Encoding, i16),
) -> Array {
    let dict_values = dict.values();

    let (values, rep_levels) = levels::consume_level(values, length, rep_level_encoding);
    let (values, def_levels) = levels::consume_level(values, length, def_level_encoding);

    let bit_width = values[0];
    let values = &values[1..];

    let (_, consumed) = uleb128::decode(&values);
    let values = &values[consumed..];

    let indices = bitpacking::Decoder::new(values, bit_width, length as usize);

    let values = indices.map(|id| dict_values[id as usize]);

    compose_array(
        rep_levels.into_iter(),
        def_levels.into_iter(),
        rep_level_encoding.1 as u32,
        def_level_encoding.1 as u32,
        values,
    )
}

pub fn page_dict_to_array<T: NativeType>(
    page: &Page,
    descriptor: &ColumnDescriptor,
) -> Result<Array> {
    assert_eq!(descriptor.max_rep_level(), 1);
    match page {
        Page::V1(page) => match (&page.header.encoding, &page.dictionary_page) {
            (Encoding::PlainDictionary, Some(dict)) => Ok(read_dict_array(
                &page.buffer,
                page.header.num_values as u32,
                dict.as_any().downcast_ref().unwrap(),
                (
                    &page.header.repetition_level_encoding,
                    descriptor.max_rep_level(),
                ),
                (
                    &page.header.definition_level_encoding,
                    descriptor.max_def_level(),
                ),
            )),
            (_, None) => Err(ParquetError::OutOfSpec(
                "A dictionary-encoded page MUST be preceeded by a dictionary page".to_string(),
            )),
            _ => todo!(),
        },
        Page::V2(_) => todo!(),
    }
}
