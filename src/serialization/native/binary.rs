use parquet_format::Encoding;

use super::levels;
use crate::encoding::{bitpacking, get_length, uleb128};
use crate::errors::{ParquetError, Result};
use crate::metadata::ColumnDescriptor;
use crate::read::{Page, PageDict};

fn read_buffer(values: &[u8], length: u32) -> Vec<Option<Vec<u8>>> {
    let mut values = values;
    (0..length)
        .map(|_| {
            let length = get_length(values) as usize;
            let item = values[4..4 + length].to_vec();
            values = &values[4 + length..];
            Some(item)
        })
        .collect()
}

fn read_dict_buffer(
    values: &[u8],
    length: u32,
    dict: &PageDict,
    rep_level_encoding: (&Encoding, i16),
    def_level_encoding: (&Encoding, i16),
) -> Vec<Option<Vec<u8>>> {
    // todo: parse these once per group, not once per page
    let dict_values = read_buffer(&dict.buf, dict.num_values);

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
        .map(|id| dict_values[id as usize].clone())
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
                &dict,
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
