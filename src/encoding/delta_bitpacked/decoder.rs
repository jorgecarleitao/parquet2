use super::super::bitpacking;
use super::super::uleb128;
use super::super::zigzag_leb128;

#[derive(Debug)]
struct Block<'a> {
    // this is the minimum delta that must be added to every value.
    min_delta: i64,
    num_mini_blocks: usize,
    values_per_mini_block: usize,
    bitwidths: &'a [u8],
    values: &'a [u8],
    remaining: usize,
    current_index: usize, // invariant: < values_per_mini_block
    // None represents a relative delta of zero, in which case there is no miniblock.
    current_miniblock: Option<bitpacking::Decoder<'a>>,
}

impl<'a> Block<'a> {
    pub fn new(mut values: &'a [u8], num_mini_blocks: usize, values_per_mini_block: usize) -> Self {
        let (min_delta, consumed) = zigzag_leb128::decode(values);
        values = &values[consumed..];

        let mut bitwidths = &values[..num_mini_blocks];
        values = &values[num_mini_blocks..];

        // read first bitwidth
        let num_bits = bitwidths[0];
        bitwidths = &bitwidths[1..];

        let current_miniblock = if num_bits > 0 {
            Some(bitpacking::Decoder::new(
                values,
                num_bits,
                values_per_mini_block,
            ))
        } else {
            None
        };

        Self {
            min_delta,
            num_mini_blocks,
            values_per_mini_block,
            bitwidths,
            remaining: num_mini_blocks * values_per_mini_block,
            values,
            current_index: 0,
            current_miniblock,
        }
    }
}

impl<'a> Iterator for Block<'a> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let result = self.min_delta
            + self
                .current_miniblock
                .as_mut()
                .map(|x| x.next().unwrap())
                .unwrap_or(0) as i64;
        self.current_index += 1;
        if self.current_index == self.values_per_mini_block {
            // read first bitwidth
            let num_bits = self.bitwidths[0];
            self.bitwidths = &self.bitwidths[1..];
            self.current_miniblock = if num_bits > 0 {
                Some(bitpacking::Decoder::new(
                    self.values,
                    num_bits,
                    self.values_per_mini_block,
                ))
            } else {
                None
            };
            self.current_index = 0;
        }
        self.remaining -= 1;

        Some(result as u32)
    }
}

/// An iterator that, given a slice of bytes, returns `HybridEncoded`
#[derive(Debug)]
pub struct Decoder<'a> {
    block_size: u64,
    num_mini_blocks: u64,
    values_per_mini_block: usize,
    total_count: u64,
    first_value: i64, // the cumulative
    values: &'a [u8],
    current_block: Block<'a>,
}

impl<'a> Decoder<'a> {
    pub fn new(mut values: &'a [u8]) -> Self {
        let (block_size, consumed) = uleb128::decode(values);
        assert_eq!(block_size % 128, 0);
        values = &values[consumed..];
        let (num_mini_blocks, consumed) = uleb128::decode(values);
        values = &values[consumed..];
        let (total_count, consumed) = uleb128::decode(values);
        values = &values[consumed..];
        let (first_value, consumed) = zigzag_leb128::decode(values);
        values = &values[consumed..];

        let values_per_mini_block = (block_size / num_mini_blocks) as usize;
        assert_eq!(values_per_mini_block % 8, 0);

        let current_block = Block::new(values, num_mini_blocks as usize, values_per_mini_block);

        Self {
            block_size,
            values,
            total_count,
            num_mini_blocks,
            values_per_mini_block,
            first_value,
            current_block,
        }
    }
}

impl<'a> Iterator for Decoder<'a> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        if self.total_count == 0 {
            return None;
        }
        self.total_count -= 1;
        let delta = self.current_block.next().unwrap();
        let result = Some(self.first_value as u32);
        self.first_value += delta as i64;
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_spec() {
        let expected = (1u32..=5).collect::<Vec<_>>();
        // VALIDATED FROM SPARK==3.1.1
        // header: [128, 1, 4, 5, 2]
        // block size: 128, 1
        // mini-blocks: 4
        // elements: 5
        // first_value: 2 <=z> 1
        // block1: [2, 0, 0, 0, 0]
        // min_delta: 2 <=z> 1
        // bit_width: 0
        let data = &[128, 1, 4, 5, 2, 2, 0, 0, 0, 0];

        let r = Decoder::new(data).collect::<Vec<_>>();

        assert_eq!(expected, r);
    }

    #[test]
    fn case2() {
        let expected = vec![1u32, 2, 3, 4, 5, 1];
        // VALIDATED FROM SPARK==3.1.1
        // header: [128, 1, 4, 6, 2]
        // block size: 128, 1 <=u> 128
        // mini-blocks: 4     <=u> 4
        // elements: 6        <=u> 6
        // first_value: 2     <=z> 1
        // block1: [7, 3, 0, 0, 0]
        // min_delta: 7       <=z> -4
        // bit_widths: [3, 0, 0, 0]
        // values: [
        //      0b01101101
        //      0b00001011
        //      ...
        // ]                  <=b> [3, 3, 3, 3, 0]
        let data = &[
            128, 1, 4, 6, 2, 7, 3, 0, 0, 0, 0b01101101, 0b00001011, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ];

        let r = Decoder::new(data).collect::<Vec<_>>();

        assert_eq!(expected, r);
    }
}
