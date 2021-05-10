use parquet_format::Encoding;

use crate::encoding::{get_length, hybrid_rle, log2};

#[inline]
fn get_bit_width(max_level: i16) -> u32 {
    log2(max_level as u64)
}

pub fn needed_bytes(values: &[u8], _length: u32, encoding: (&Encoding, i16)) -> usize {
    match encoding {
        (_, 0) => 0, // no levels
        (Encoding::Rle, _) => {
            let length = get_length(values);
            // 4 consumed to read `length`
            4 + length as usize
        }
        (Encoding::BitPacked, _) => {
            todo!()
        }
        _ => unreachable!(),
    }
}

#[inline]
pub fn decode(values: &[u8], length: u32, encoding: (&Encoding, i16)) -> Vec<u32> {
    match encoding {
        (_, 0) => vec![0; length as usize], // no levels => required => all zero
        (Encoding::Rle, max_length) => {
            let bit_width = get_bit_width(max_length);
            let values = &values[4..4 + get_length(values) as usize];
            let num_bits = bit_width as u32;
            hybrid_rle::Decoder::new(values, num_bits, length as usize).collect()
        }
        (Encoding::BitPacked, _) => {
            todo!()
        }
        _ => unreachable!(),
    }
}

pub fn consume_level<'a>(
    values: &'a [u8],
    length: u32,
    level_encoding: (&Encoding, i16),
) -> (&'a [u8], Vec<u32>) {
    if level_encoding.1 > 0 {
        let offset = needed_bytes(values, length, level_encoding);
        let def_levels = decode(values, length, level_encoding);
        (&values[offset..], def_levels)
    } else {
        (values, vec![0; length as usize])
    }
}
