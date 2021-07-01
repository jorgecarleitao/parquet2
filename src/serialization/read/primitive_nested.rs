use std::convert::TryInto;

use parquet_format::Encoding;

use super::{levels, Array};
use crate::encoding::{bitpacking, uleb128};
use crate::metadata::ColumnDescriptor;
use crate::read::PageHeader;
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

fn read_array<'a, T: NativeType>(
    rep_levels: &'a [u8],
    def_levels: &'a [u8],
    values: &'a [u8],
    length: u32,
    rep_level_encoding: (&Encoding, i16),
    def_level_encoding: (&Encoding, i16),
) -> Array {
    let rep_levels = levels::consume_level(rep_levels, length, rep_level_encoding);
    let def_levels = levels::consume_level(&def_levels, length, def_level_encoding);

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
    match page.header() {
        PageHeader::V1(header) => match (&page.encoding(), &page.dictionary_page()) {
            (Encoding::Plain, None) => {
                let (rep_levels, def_levels, values) =
                    levels::split_buffer_v1(page.buffer(), true, true);
                Ok(read_array::<T>(
                    rep_levels,
                    def_levels,
                    values,
                    page.num_values() as u32,
                    (
                        &header.repetition_level_encoding,
                        descriptor.max_rep_level(),
                    ),
                    (
                        &header.definition_level_encoding,
                        descriptor.max_def_level(),
                    ),
                ))
            }
            _ => todo!(),
        },
        PageHeader::V2(_) => todo!(),
    }
}

fn read_dict_array<'a>(
    rep_levels: &'a [u8],
    def_levels: &'a [u8],
    values: &'a [u8],
    length: u32,
    dict: &'a PrimitivePageDict<i64>,
    rep_level_encoding: (&Encoding, i16),
    def_level_encoding: (&Encoding, i16),
) -> Array {
    let dict_values = dict.values();

    let rep_levels = levels::consume_level(rep_levels, length, rep_level_encoding);
    let def_levels = levels::consume_level(def_levels, length, def_level_encoding);

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
    match page.header() {
        PageHeader::V1(header) => match (&page.encoding(), &page.dictionary_page()) {
            (Encoding::PlainDictionary, Some(dict)) => {
                let (rep_levels, def_levels, values) =
                    levels::split_buffer_v1(page.buffer(), true, true);
                Ok(read_dict_array(
                    rep_levels,
                    def_levels,
                    values,
                    page.num_values() as u32,
                    dict.as_any().downcast_ref().unwrap(),
                    (
                        &header.repetition_level_encoding,
                        descriptor.max_rep_level(),
                    ),
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
        PageHeader::V2(_) => todo!(),
    }
}
