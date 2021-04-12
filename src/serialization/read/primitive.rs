use std::convert::{TryFrom, TryInto};

use parquet_format::Encoding;

use super::levels;
use super::utils::ValuesDef;
use crate::encoding::{bitpacking, uleb128};
use crate::error::{ParquetError, Result};
use crate::metadata::ColumnDescriptor;
use crate::{
    read::{Page, PrimitivePageDict},
    types::NativeType,
};

fn read_buffer<'a, T: NativeType>(
    values: &'a [u8],
    length: u32,
    rep_level_encoding: (&Encoding, i16),
    def_level_encoding: (&Encoding, i16),
) -> Vec<Option<T>> {
    // skip bytes from levels
    let offset = levels::needed_bytes(values, length, def_level_encoding);
    let def_levels = levels::decode(values, length, def_level_encoding);
    let values = &values[offset..];
    let offset = levels::needed_bytes(values, length, rep_level_encoding);
    let values = &values[offset..];

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
    values: &'a [u8],
    length: u32,
    dict: &'a PrimitivePageDict<T>,
    rep_level_encoding: (&Encoding, i16),
    def_level_encoding: (&Encoding, i16),
) -> Vec<Option<T>> {
    let dict_values = dict.values();

    // skip bytes from levels
    let offset = levels::needed_bytes(values, length, def_level_encoding);
    let def_levels = levels::decode(values, length, def_level_encoding);
    let values = &values[offset..];
    let offset = levels::needed_bytes(values, length, rep_level_encoding);
    let values = &values[offset..];

    let bit_width = values[0];
    let values = &values[1..];

    let (_, consumed) = uleb128::decode(&values);
    let values = &values[consumed..];

    let mut new_values = vec![0; bitpacking::required_capacity(length)];
    bitpacking::decode(&values, bit_width, &mut new_values);
    new_values.truncate(length as usize);

    let iterator = ValuesDef::new(
        new_values.into_iter(),
        def_levels.into_iter(),
        def_level_encoding.1 as u32,
    );

    iterator
        .map(|maybe_id| maybe_id.map(|id| dict_values[id as usize]))
        .collect()
}

pub fn page_dict_to_vec<T: NativeType>(
    page: &Page,
    descriptor: &ColumnDescriptor,
) -> Result<Vec<Option<T>>> {
    match page {
        Page::V1(page) => match (&page.header.encoding, &page.dictionary_page) {
            (Encoding::PlainDictionary, Some(dict)) => Ok(read_dict_buffer::<T>(
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

pub fn page_to_vec<'a, T: NativeType>(
    page: &'a Page,
    descriptor: &ColumnDescriptor,
) -> Result<Vec<Option<T>>>
where
    <T as NativeType>::Bytes: TryFrom<&'a [u8]>,
    <<T as NativeType>::Bytes as TryFrom<&'a [u8]>>::Error: std::fmt::Debug,
{
    match page {
        Page::V1(page) => match (&page.header.encoding, &page.dictionary_page) {
            (Encoding::Plain, None) => Ok(read_buffer::<T>(
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
