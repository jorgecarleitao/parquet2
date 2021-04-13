use parquet_format::Encoding;

use super::levels::consume_level;
use crate::error::{ParquetError, Result};
use crate::metadata::ColumnDescriptor;
use crate::read::Page;
use crate::{
    encoding::{bitpacking, uleb128},
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

    let (values, _) = consume_level(values, length, def_level_encoding);

    let bit_width = values[0];
    let values = &values[1..];

    let (_, consumed) = uleb128::decode(&values);
    let values = &values[consumed..];

    let mut decompressed = vec![0; bitpacking::required_capacity(length)];
    bitpacking::decode(&values, bit_width, &mut decompressed);
    decompressed.truncate(length as usize);

    decompressed
        .into_iter()
        .map(|id| {
            let id = id as usize;
            let start = dict_offsets[id] as usize;
            let end = dict_offsets[id + 1] as usize;
            Some(dict_values[start..end].to_vec())
        })
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
