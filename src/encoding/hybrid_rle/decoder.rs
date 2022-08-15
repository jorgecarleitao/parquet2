use super::super::uleb128;
use super::{super::ceil8, HybridEncoded};

/// An iterator that, given a slice of bytes, returns `HybridEncoded`
#[derive(Debug, Clone)]
pub struct Decoder<'a> {
    values: &'a [u8],
    num_bits: usize,
}

impl<'a> Decoder<'a> {
    pub fn new(values: &'a [u8], num_bits: usize) -> Self {
        Self { values, num_bits }
    }

    /// Returns the number of bits being used by this decoder.
    #[inline]
    pub fn num_bits(&self) -> usize {
        self.num_bits
    }
}

impl<'a> Iterator for Decoder<'a> {
    type Item = HybridEncoded<'a>;

    #[inline] // -18% improvement in bench
    fn next(&mut self) -> Option<Self::Item> {
        if self.values.is_empty() {
            return None;
        }
        let (indicator, consumed) = uleb128::decode(self.values);
        self.values = &self.values[consumed..];
        if self.values.is_empty() {
            return None;
        };
        if indicator & 1 == 1 {
            // is bitpacking
            let bytes = (indicator as usize >> 1) * self.num_bits;
            let bytes = std::cmp::min(bytes, self.values.len());
            let (result, remaining) = self.values.split_at(bytes);
            let result = Some(HybridEncoded::Bitpacked(result));
            self.values = remaining;
            result
        } else {
            // is rle
            let run_length = indicator as usize >> 1;
            // repeated-value := value that is repeated, using a fixed-width of round-up-to-next-byte(bit-width)
            let rle_bytes = ceil8(self.num_bits);
            let (result, remaining) = self.values.split_at(rle_bytes);
            let result = Some(HybridEncoded::Rle(result, run_length));
            self.values = remaining;
            result
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::super::super::bitpacked;

    #[test]
    fn basics_1() {
        let bit_width = 1usize;
        let length = 5;
        let values = vec![
            2, 0, 0, 0, // length
            0b00000011, 0b00001011, // data
        ];

        let mut decoder = Decoder::new(&values[4..6], bit_width);

        let run = decoder.next().unwrap();

        if let HybridEncoded::Bitpacked(values) = run {
            assert_eq!(values, &[0b00001011]);
            let result =
                bitpacked::Decoder::<u32>::new(values, bit_width, length).collect::<Vec<_>>();
            assert_eq!(result, &[1, 1, 0, 1, 0]);
        } else {
            panic!()
        };
    }

    #[test]
    fn basics_2() {
        // This test was validated by the result of what pyarrow3 outputs when
        // the bitmap is used.
        let bit_width = 1;
        let values = vec![
            3, 0, 0, 0, // length
            0b00000101, 0b11101011, 0b00000010, // data
        ];
        let expected = &[1, 1, 0, 1, 0, 1, 1, 1, 0, 1];

        let mut decoder = Decoder::new(&values[4..4 + 3], bit_width);

        let run = decoder.next().unwrap();

        if let HybridEncoded::Bitpacked(values) = run {
            assert_eq!(values, &[0b11101011, 0b00000010]);
            let result = bitpacked::Decoder::<u32>::new(values, bit_width, 10).collect::<Vec<_>>();
            assert_eq!(result, expected);
        } else {
            panic!()
        };
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

        let mut decoder = Decoder::new(&values[4..4 + 2], bit_width);

        let run = decoder.next().unwrap();

        if let HybridEncoded::Rle(values, items) = run {
            assert_eq!(values, &[0b00000001]);
            assert_eq!(items, length);
        } else {
            panic!()
        };
    }
}
