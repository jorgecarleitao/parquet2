use crate::encoding::ceil8;
use crate::read::levels::get_bit_width;

use super::super::bitpacking;
use super::super::uleb128;
use super::super::zigzag_leb128;

/// Encodes an iterator of `i32` according to parquet's `DELTA_BINARY_PACKED`.
/// # Implementation
/// * This function does not allocate on the heap.
/// * The number of mini-blocks is always 1. This may change in the future.
pub fn encode<I: Iterator<Item = i32>>(mut iterator: I, buffer: &mut Vec<u8>) {
    let block_size = 128;
    let mini_blocks = 1;

    let mut container = [0u8; 10];
    let encoded_len = uleb128::encode(block_size, &mut container);
    buffer.extend_from_slice(&container[..encoded_len]);

    let encoded_len = uleb128::encode(mini_blocks, &mut container);
    buffer.extend_from_slice(&container[..encoded_len]);

    let length = iterator.size_hint().1.unwrap();
    let encoded_len = uleb128::encode(length as u64, &mut container);
    buffer.extend_from_slice(&container[..encoded_len]);

    let mut values = [0i64; 128];
    let mut deltas = [0u32; 128];

    let first_value = iterator.next().unwrap().into();
    let (container, encoded_len) = zigzag_leb128::encode(first_value);
    buffer.extend_from_slice(&container[..encoded_len]);

    let mut prev = first_value;
    let mut length = iterator.size_hint().1.unwrap();
    while length != 0 {
        for (i, v) in (0..128).zip(&mut iterator) {
            let v: i64 = v.into();
            values[i] = v - prev;
            prev = v;
        }
        let consumed = std::cmp::min(length - iterator.size_hint().1.unwrap(), 128);
        let values = &values[..consumed];

        let min_delta = *values.iter().min().unwrap();
        let max_delta = *values.iter().max().unwrap();

        values.iter().zip(deltas.iter_mut()).for_each(|(v, d)| {
            *d = (v - min_delta) as u32;
        });

        // <min delta> <list of bitwidths of miniblocks> <miniblocks>
        let (container, encoded_len) = zigzag_leb128::encode(min_delta);
        buffer.extend_from_slice(&container[..encoded_len]);

        let num_bits = get_bit_width((max_delta - min_delta) as i16) as u8;
        buffer.push(num_bits);

        if num_bits > 0 {
            let start = buffer.len();

            // bitpack encode all (deltas.len = 128 which is a multiple of 32)
            let bytes_needed = start + ceil8(deltas.len() * num_bits as usize);
            buffer.resize(bytes_needed, 0);
            bitpacking::encode(deltas.as_ref(), num_bits, &mut buffer[start..]);

            let bytes_needed = start + ceil8(deltas.len() * num_bits as usize);
            buffer.truncate(bytes_needed);
        }

        length = iterator.size_hint().1.unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constant_delta() {
        // header: [128, 1, 1, 5, 2]:
        //  block size: 128    <=u> 128, 1
        //  mini-blocks: 1     <=u> 1
        //  elements: 5        <=u> 5
        //  first_value: 2     <=z> 1
        // block1: [2, 0, 0, 0, 0]
        //  min_delta: 1        <=z> 2
        //  bitwidth: 0
        let data = 1i32..=5;
        let expected = vec![128u8, 1, 1, 5, 2, 2, 0];

        let mut buffer = vec![];
        encode(data, &mut buffer);
        assert_eq!(expected, buffer);
    }

    #[test]
    fn negative_min_delta() {
        // max - min = 1 - -4 = 5
        let data = vec![1i32, 2, 3, 4, 5, 1];
        // header: [128, 1, 4, 6, 2]
        //  block size: 128    <=u> 128, 1
        //  mini-blocks: 1     <=u> 1
        //  elements: 6        <=u> 5
        //  first_value: 2     <=z> 1
        // block1: [7, 3, 253, 255]
        //  min_delta: -4        <=z> 7
        //  bitwidth: 3
        //  values: [5, 5, 5, 5, 0] <=b> [
        //      0b01101101
        //      0b00001011
        // ]
        let mut expected = vec![128u8, 1, 1, 6, 2, 7, 3, 0b01101101, 0b00001011];
        expected.extend(std::iter::repeat(0).take(128 * 3 / 8 - 2)); // 128 values, 3 bits, 2 already used

        let mut buffer = vec![];
        encode(data.into_iter(), &mut buffer);
        assert_eq!(expected, buffer);
    }
}
