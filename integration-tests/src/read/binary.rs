use parquet::{
    encoding::{bitpacking, plain_byte_array, uleb128, Encoding},
    error::Result,
    metadata::ColumnDescriptor,
    read::{levels, BinaryPageDict, Page, PageHeader},
};

use super::utils::ValuesDef;
use levels::{get_bit_width, RLEDecoder};

fn read_dict_buffer_impl<I: Iterator<Item = u32>>(
    def_levels: I,
    values: &[u8],
    length: u32,
    max_def_level: u32,
    dict: &BinaryPageDict,
) -> Vec<Option<Vec<u8>>> {
    let dict_values = dict.values();
    let dict_offsets = dict.offsets();

    let bit_width = values[0];
    let values = &values[1..];

    let (_, consumed) = uleb128::decode(&values);
    let values = &values[consumed..];

    let indices = bitpacking::Decoder::new(values, bit_width, length as usize);

    let iterator = ValuesDef::new(indices, def_levels, max_def_level);

    iterator
        .map(|maybe_id| {
            maybe_id.map(|id| {
                let start = dict_offsets[id as usize] as usize;
                let end = dict_offsets[id as usize + 1] as usize;
                dict_values[start..end].to_vec()
            })
        })
        .collect()
}

fn read_dict_buffer(
    def_levels: &[u8],
    values: &[u8],
    length: u32,
    dict: &BinaryPageDict,
    def_level_encoding: (&Encoding, i16),
) -> Vec<Option<Vec<u8>>> {
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
            let def_levels = RLEDecoder::new(def_levels, num_bits, length);
            read_dict_buffer_impl(def_levels, values, length, max_def_level, dict)
        }
        _ => todo!(),
    }
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
            (_, None) => todo!("Dictionary-encoded page requires a dictionary"),
            _ => todo!(),
        },
        _ => todo!(),
    }
}

fn read_buffer_impl<I: Iterator<Item = u32>>(
    def_levels: I,
    values: &[u8],
    length: u32,
    max_def_level: u32,
) -> Vec<Option<Vec<u8>>> {
    let decoded_values =
        plain_byte_array::Decoder::new(values, length as usize).map(|bytes| bytes.to_vec());

    ValuesDef::new(decoded_values, def_levels, max_def_level).collect()
}

fn read_buffer(
    def_levels: &[u8],
    values: &[u8],
    length: u32,
    def_level_encoding: (&Encoding, i16),
) -> Vec<Option<Vec<u8>>> {
    let max_def_level = def_level_encoding.1 as u32;
    match (def_level_encoding.0, max_def_level == 0) {
        (Encoding::Rle, true) => read_buffer_impl(
            std::iter::repeat(0).take(length as usize),
            values,
            length,
            max_def_level,
        ),
        (Encoding::Rle, false) => {
            let num_bits = get_bit_width(def_level_encoding.1);
            let def_levels = RLEDecoder::new(def_levels, num_bits, length);
            read_buffer_impl(def_levels, values, length, max_def_level)
        }
        _ => todo!(),
    }
}

pub fn page_to_vec(page: &Page, descriptor: &ColumnDescriptor) -> Result<Vec<Option<Vec<u8>>>> {
    assert_eq!(descriptor.max_rep_level(), 0);
    match page.header() {
        PageHeader::V1(header) => match (&page.encoding(), &page.dictionary_page()) {
            (Encoding::Plain, None) => {
                let (_, def_levels, values) =
                    levels::split_buffer_v1(page.buffer(), false, descriptor.max_def_level() > 0);
                Ok(read_buffer(
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
