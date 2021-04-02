use arrow2::bitmap::MutableBitmap;
use parquet_format::Encoding;

use crate::encoding::{ceil8, get_length, hybrid_rle, log2};

#[inline]
fn get_bit_width(max_level: i16) -> u32 {
    log2(max_level as u64 + 1)
}

pub fn needed_bytes(values: &[u8], length: u32, a: (&Encoding, i16)) -> usize {
    match a {
        (_, 0) => 0, // no rep levels
        (Encoding::Rle, _) => {
            let length = get_length(values);
            // 4 consumed to read `length`
            4 + length as usize
        }
        (Encoding::BitPacked, max_level) => {
            let bit_width = log2(max_level as u64 + 1) as u8;
            ceil8(length as usize * bit_width as usize)
        }
        _ => unreachable!(),
    }
}

#[inline]
pub fn decode(values: &[u8], length: u32, a: (&Encoding, i16), bitmap: &mut MutableBitmap) {
    match a {
        (_, 0) => (), // no levels
        (Encoding::Rle, 1) => {
            rle_bitmap_decode(&values[4..4 + get_length(values) as usize], length, bitmap)
        }
        (Encoding::BitPacked, _) => {
            todo!()
        }
        _ => unreachable!(),
    }
}

#[inline]
fn rle_bitmap_decode(values: &[u8], length: u32, bitmap: &mut MutableBitmap) {
    let length = length as usize;
    let runner = hybrid_rle::Decoder::new(&values, 1);

    bitmap.reserve(length);
    runner.for_each(|run| match run {
        hybrid_rle::HybridEncoded::Bitpacked(compressed) => {
            let previous_len = values.len();
            println!("{:?}", compressed);
        }
        hybrid_rle::HybridEncoded::Rle(pack, items) => {
            let is_set = pack[0] == 1;
            bitmap.extend_constant(items, is_set)
        }
    });
}
