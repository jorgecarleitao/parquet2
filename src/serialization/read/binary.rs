use parquet_format::Encoding;

use super::levels::consume_level;
use crate::error::{ParquetError, Result};
use crate::metadata::ColumnDescriptor;
use crate::read::Page;
use crate::serialization::read::utils::ValuesDef;
use crate::{
    encoding::{hybrid_rle, plain_byte_array},
    read::BinaryPageDict,
};

fn read_dict_buffer(
    values: &[u8],
    length: u32,
    dict: &BinaryPageDict,
    def_level_encoding: (&Encoding, i16),
) -> Vec<Option<Vec<u8>>> {
    let dict_values = dict.values();
    let dict_offsets = dict.offsets();

    let (values, def_levels) = consume_level(values, length, def_level_encoding);

    let bit_width = values[0];
    let values = &values[1..];

    let indices = hybrid_rle::Decoder::new(values, bit_width as u32, length as usize);

    let decoded_values = indices.map(|id| {
        let id = id as usize;
        let start = dict_offsets[id] as usize;
        let end = dict_offsets[id + 1] as usize;

        dict_values[start..end].to_vec()
    });

    ValuesDef::new(
        decoded_values,
        def_levels.into_iter(),
        def_level_encoding.1 as u32,
    )
    .collect()
}

pub fn page_dict_to_vec(
    page: &Page,
    descriptor: &ColumnDescriptor,
) -> Result<Vec<Option<Vec<u8>>>> {
    assert_eq!(descriptor.max_rep_level(), 0);
    match page {
        Page::V1(page) => match (&page.header.encoding, &page.dictionary_page) {
            (Encoding::PlainDictionary, Some(dict)) => Ok(read_dict_buffer(
                &page.buffer,
                page.header.num_values as u32,
                dict.as_any().downcast_ref().unwrap(),
                (
                    &page.header.definition_level_encoding,
                    descriptor.max_def_level(),
                ),
            )),
            (_, None) => Err(general_err!(
                "Dictionary-encoded page requires a dictionary"
            )),
            _ => todo!(),
        },
        _ => todo!(),
    }
}

fn read_plain_buffer(
    values: &[u8],
    length: u32,
    def_level_encoding: (&Encoding, i16),
) -> Vec<Option<Vec<u8>>> {
    let (values, def_levels) = consume_level(values, length, def_level_encoding);

    let decoded_values = plain_byte_array::Decoder::new(values).map(|bytes| bytes.to_vec());

    ValuesDef::new(
        decoded_values,
        def_levels.into_iter(),
        def_level_encoding.1 as u32,
    )
    .collect()
}

pub fn page_to_vec(page: &Page, descriptor: &ColumnDescriptor) -> Result<Vec<Option<Vec<u8>>>> {
    assert_eq!(descriptor.max_rep_level(), 0);
    match page {
        Page::V1(page) => match (&page.header.encoding, &page.dictionary_page) {
            (Encoding::Plain, None) => Ok(read_plain_buffer(
                &page.buffer,
                page.header.num_values as u32,
                (
                    &page.header.definition_level_encoding,
                    descriptor.max_def_level(),
                ),
            )),
            _ => todo!(),
        },
        _ => todo!(),
    }
}
