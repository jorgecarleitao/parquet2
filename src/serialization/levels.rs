use parquet_format::Encoding;

use crate::encoding::{bitpacking, get_length, hybrid_rle, log2};

#[inline]
fn get_bit_width(max_level: i16) -> u32 {
    log2(max_level as u64 + 1)
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
        (_, 0) => vec![], // no levels
        (Encoding::Rle, max_length) => {
            let bit_width = get_bit_width(max_length);
            rle_decode(
                &values[4..4 + get_length(values) as usize],
                bit_width as u32,
                length,
            )
        }
        (Encoding::BitPacked, _) => {
            todo!()
        }
        _ => unreachable!(),
    }
}

#[inline]
pub fn rle_decode(values: &[u8], num_bits: u32, length: u32) -> Vec<u32> {
    let length = length as usize;
    let runner = hybrid_rle::Decoder::new(&values, num_bits);

    let mut values = Vec::with_capacity(length);
    runner.for_each(|run| match run {
        hybrid_rle::HybridEncoded::Bitpacked(compressed) => {
            let previous_len = values.len();
            let pack_length = (compressed.len() as u32 / num_bits) * 8;
            values.extend(std::iter::repeat(0).take(bitpacking::required_capacity(pack_length)));
            bitpacking::decode(compressed, num_bits as u8, &mut values[previous_len..]);
            values.truncate(length);
        }
        hybrid_rle::HybridEncoded::Rle(pack, items) => {
            let mut bytes = [0u8; std::mem::size_of::<u32>()];
            pack.iter()
                .enumerate()
                .for_each(|(i, byte)| bytes[i] = *byte);
            let value = u32::from_le_bytes(bytes);
            values.extend(std::iter::repeat(value).take(items))
        }
    });
    values
}
