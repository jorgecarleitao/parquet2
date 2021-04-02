use parquet_format::Encoding;

use super::super::levels;
use crate::errors::{ParquetError, Result};
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
    rep_level_encoding: (&Encoding, i16),
    def_level_encoding: (&Encoding, i16),
) -> Vec<Option<Vec<u8>>> {
    let dict_values = dict.values();
    let dict_offsets = dict.offsets();

    // skip bytes from levels
    let offset = levels::needed_bytes(values, length, def_level_encoding);
    let values = &values[offset..];
    let offset = levels::needed_bytes(values, length, rep_level_encoding);
    let values = &values[offset..];

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
    match page {
        Page::V1(page) => match (&page.encoding, &page.dictionary_page) {
            (Encoding::PlainDictionary, Some(dict)) => Ok(read_dict_buffer(
                &page.buf,
                page.num_values,
                dict.as_any().downcast_ref().unwrap(),
                (&page.rep_level_encoding, descriptor.max_rep_level()),
                (&page.def_level_encoding, descriptor.max_def_level()),
            )),
            (_, None) => Err(general_err!(
                "Dictionary-encoded page requires a dictionary"
            )),
            _ => todo!(),
        },
        _ => todo!(),
    }
}
