use parquet_format::Encoding;

use super::levels::consume_level;
use crate::error::{ParquetError, Result};
use crate::metadata::ColumnDescriptor;
use crate::read::{Page, PageHeader};
use crate::serialization::read::levels;
use crate::serialization::read::utils::ValuesDef;
use crate::{
    encoding::{bitpacking, plain_byte_array, uleb128},
    read::BinaryPageDict,
};

fn read_dict_buffer(
    def_levels: &[u8],
    values: &[u8],
    length: u32,
    dict: &BinaryPageDict,
    def_level_encoding: (&Encoding, i16),
) -> Vec<Option<Vec<u8>>> {
    let dict_values = dict.values();
    let dict_offsets = dict.offsets();

    let levels = consume_level(def_levels, length, def_level_encoding);

    let bit_width = values[0];
    let values = &values[1..];

    let (_, consumed) = uleb128::decode(&values);
    let values = &values[consumed..];

    let mut indices = bitpacking::Decoder::new(values, bit_width, length as usize);

    let is_valid = levels.into_iter().map(|x| x == def_level_encoding.1 as u32);
    is_valid
        .map(|is_valid| {
            if is_valid {
                let id = indices.next().unwrap() as usize;
                let start = dict_offsets[id] as usize;
                let end = dict_offsets[id + 1] as usize;
                Some(dict_values[start..end].to_vec())
            } else {
                None
            }
        })
        .collect()
}

pub fn page_dict_to_vec(
    page: &Page,
    descriptor: &ColumnDescriptor,
) -> Result<Vec<Option<Vec<u8>>>> {
    assert_eq!(descriptor.max_rep_level(), 0);
    match page.header() {
        PageHeader::V1(header) => match (&page.encoding(), &page.dictionary_page()) {
            (Encoding::PlainDictionary, Some(dict)) => {
                let (_, def_levels, values) =
                    levels::split_buffer_v1(page.buffer(), false, descriptor.max_def_level() > 0);
                Ok(read_dict_buffer(
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
            (_, None) => Err(general_err!(
                "Dictionary-encoded page requires a dictionary"
            )),
            _ => todo!(),
        },
        _ => todo!(),
    }
}

fn read_plain_buffer(
    def_levels: &[u8],
    values: &[u8],
    length: u32,
    def_level_encoding: (&Encoding, i16),
) -> Vec<Option<Vec<u8>>> {
    let def_levels = consume_level(def_levels, length, def_level_encoding);

    let decoded_values =
        plain_byte_array::Decoder::new(values, length as usize).map(|bytes| bytes.to_vec());

    ValuesDef::new(
        decoded_values,
        def_levels.into_iter(),
        def_level_encoding.1 as u32,
    )
    .collect()
}

pub fn page_to_vec(page: &Page, descriptor: &ColumnDescriptor) -> Result<Vec<Option<Vec<u8>>>> {
    assert_eq!(descriptor.max_rep_level(), 0);
    match page.header() {
        PageHeader::V1(header) => match (&page.encoding(), &page.dictionary_page()) {
            (Encoding::Plain, None) => {
                let (_, def_levels, values) =
                    levels::split_buffer_v1(page.buffer(), false, descriptor.max_def_level() > 0);
                Ok(read_plain_buffer(
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
        _ => todo!(),
    }
}
