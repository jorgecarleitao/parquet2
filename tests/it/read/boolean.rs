use parquet2::encoding::hybrid_rle::HybridRleDecoder;
use parquet2::encoding::Encoding;
use parquet2::error::Result;
use parquet2::page::{split_buffer, DataPage, DataPageHeader};
use parquet2::read::levels::get_bit_width;

use super::utils::ValuesDef;

const BIT_MASK: [u8; 8] = [1, 2, 4, 8, 16, 32, 64, 128];

/// Returns whether bit at position `i` in `byte` is set or not
#[inline]
pub fn is_set(byte: u8, i: usize) -> bool {
    (byte & BIT_MASK[i]) != 0
}

/// Returns whether bit at position `i` in `data` is set or not
#[inline]
pub fn get_bit(data: &[u8], i: usize) -> bool {
    // in PLAIN:
    // * Most significant byte is the last one
    // * Most significant bit is the last one
    // note that this is different from Arrow, where most significant byte is the first
    is_set(data[data.len() - 1 - i / 8], i % 8)
}

fn read_buffer_impl<I: Iterator<Item = u32>>(
    def_levels: I,
    values: &[u8],
    length: usize,
    max_def_level: u32,
) -> Vec<Option<bool>> {
    let decoded_values = (0..length).map(|i| get_bit(values, i));

    ValuesDef::new(decoded_values, def_levels, max_def_level).collect()
}

fn read_buffer(
    def_levels: &[u8],
    values: &[u8],
    length: usize,
    def_level_encoding: (&Encoding, i16),
) -> Vec<Option<bool>> {
    let max_def_level = def_level_encoding.1 as u32;
    match (def_level_encoding.0, max_def_level == 0) {
        (Encoding::Rle, true) | (Encoding::BitPacked, true) => read_buffer_impl(
            std::iter::repeat(0).take(length),
            values,
            length,
            max_def_level,
        ),
        (Encoding::Rle, false) => {
            let num_bits = get_bit_width(def_level_encoding.1);
            let def_levels = HybridRleDecoder::new(def_levels, num_bits, length);
            read_buffer_impl(def_levels, values, length, max_def_level)
        }
        _ => todo!(),
    }
}

pub fn page_to_vec(page: &DataPage) -> Result<Vec<Option<bool>>> {
    let (_, def_levels, values) = split_buffer(page);

    match page.header() {
        DataPageHeader::V1(_) => match page.encoding() {
            Encoding::Plain | Encoding::PlainDictionary => Ok(read_buffer(
                def_levels,
                values,
                page.num_values(),
                (
                    &page.definition_level_encoding(),
                    page.descriptor.max_def_level,
                ),
            )),
            _ => todo!(),
        },
        DataPageHeader::V2(_) => match page.encoding() {
            Encoding::Plain | Encoding::PlainDictionary => Ok(read_buffer(
                def_levels,
                values,
                page.num_values(),
                (
                    &page.definition_level_encoding(),
                    page.descriptor.max_def_level,
                ),
            )),
            _ => todo!(),
        },
    }
}
