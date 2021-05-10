use super::super::uleb128;
use super::{super::ceil8, HybridEncoded};

/// An iterator that, given a slice of bytes, returns `HybridEncoded`
pub struct Decoder<'a> {
    inner: std::iter::Flatten<HybridIterator<'a>>,
}

impl<'a> Decoder<'a> {
    pub fn new(values: &'a [u8], num_bits: u32, length: usize) -> Self {
        Self {
            inner: HybridIterator {
                values,
                num_bits,
                remaining: length,
            }
            .flatten(),
        }
    }
}

impl<'a> Iterator for Decoder<'a> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub struct HybridIterator<'a> {
    values: &'a [u8],
    num_bits: u32,
    remaining: usize,
}

impl<'a> Iterator for HybridIterator<'a> {
    type Item = HybridEncoded<'a>;

    fn next(&mut self) -> Option<HybridEncoded<'a>> {
        if self.values.is_empty() {
            return None;
        }
        let (indicator, consumed) = uleb128::decode(self.values);
        self.values = &self.values[consumed..];
        if indicator & 1 == 1 {
            // is bitpacking
            let num_bits = self.num_bits as usize;
            let num_bytes = (indicator as usize >> 1) * num_bits;
            let length = (indicator as usize >> 1) * 8;
            let run_length = std::cmp::min(length, self.remaining);
            let result = Some(HybridEncoded::Bitpacked {
                compressed: &self.values[..num_bytes],
                num_bits,
                run_length,
            });
            self.remaining -= run_length;
            self.values = &self.values[num_bytes..];
            result
        } else {
            // is rle
            let run_length = indicator as usize >> 1;
            // repeated-value := value that is repeated, using a fixed-width of round-up-to-next-byte(bit-width)
            let rle_bytes = ceil8(self.num_bits as usize);

            let pack = &self.values[0..rle_bytes];
            let mut value_bytes = [0u8; std::mem::size_of::<u32>()];
            pack.iter()
                .enumerate()
                .for_each(|(i, byte)| value_bytes[i] = *byte);
            let value = u32::from_le_bytes(value_bytes);

            let result = Some(HybridEncoded::Rle { value, run_length });
            self.values = &self.values[rle_bytes..];
            self.remaining -= run_length;
            result
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basics_1() {
        let bit_width = 1;
        let length = 5;
        let values = vec![
            2, 0, 0, 0, // length
            0b00000011, 0b00001011, // data
        ];
        let expected = vec![1, 1, 0, 1, 0];

        let decoder = Decoder::new(&values[4..6], bit_width, length);

        let result = decoder.collect::<Vec<_>>();

        assert_eq!(result, expected);
    }

    #[test]
    fn basics_2() {
        // This test was validated by the result of what pyarrow3 outputs when
        // the bitmap is used.
        let bit_width = 1;
        let length = 10;
        let values = vec![
            3, 0, 0, 0, // length
            0b00000101, 0b11101011, 0b00000010, // data
        ];
        let expected = vec![1, 1, 0, 1, 0, 1, 1, 1, 0, 1];

        let decoder = Decoder::new(&values[4..4 + 3], bit_width, length);

        let result = decoder.collect::<Vec<_>>();

        assert_eq!(result, expected);
    }

    #[test]
    fn basics_3() {
        let bit_width = 1;
        let length = 8;
        let values = vec![
            2, 0, 0, 0,          // length
            0b00010000, // data
            0b00000001,
        ];
        let expected = vec![1_u32; length];

        let decoder = Decoder::new(&values[4..4 + 2], bit_width, length);

        let result = decoder.collect::<Vec<_>>();

        assert_eq!(result, expected);
    }

    #[test]
    fn rle_and_bit_packed() {
        let bit_width = 1;
        let length = 8;
        let values = vec![
            4, 0, 0, 0,          // length
            0b00001000, // data
            0b00000001, 0b00000011, 0b00001010,
        ];
        let expected = vec![1, 1, 1, 1, 0, 1, 0, 1];

        let decoder = Decoder::new(&values[4..4 + 4], bit_width, length);

        let result = decoder.collect::<Vec<_>>();

        assert_eq!(result, expected);
    }
}
