// See https://github.com/apache/parquet-format/blob/master/Encodings.md#run-length-encoding--bit-packing-hybrid-rle--3

use super::ceil8;
use super::uleb128;

#[derive(Debug, PartialEq, Eq)]
pub enum HybridEncoded<'a> {
    /// A bitpacked slice. The consumer must know its bit-width to unpack it.
    Bitpacked(&'a [u8]),
    /// A RLE-encoded slice.
    Rle(&'a [u8], usize),
}

/// An iterator that, given a slice of bytes, returns `HybridEncoded`
pub struct Decoder<'a> {
    values: &'a [u8],
    rle_items: usize,
}

impl<'a> Decoder<'a> {
    pub fn new(values: &'a [u8], num_bits: u32) -> Self {
        Self {
            values,
            rle_items: ceil8(num_bits as usize),
        }
    }
}

impl<'a> Iterator for Decoder<'a> {
    type Item = HybridEncoded<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.values.is_empty() {
            return None;
        }
        let (indicator, consumed) = uleb128::decode(self.values);
        self.values = &self.values[consumed..];
        if indicator & 1 == 1 {
            // is bitpacking
            let items = indicator as usize >> 1;
            let result = Some(HybridEncoded::Bitpacked(&self.values[..items]));
            self.values = &self.values[items..];
            result
        } else {
            // is rle
            let decompressed_items = indicator as usize >> 1;
            let result = Some(HybridEncoded::Rle(
                &self.values[..self.rle_items],
                decompressed_items,
            ));
            self.values = &self.values[self.rle_items..];
            result
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::super::bitpacking;

    #[test]
    fn basics_1() {
        let bit_width = 1;
        let length = 5;
        let values = vec![
            2, 0, 0, 0, // length
            0b00000011, 0b00001011, // data
        ];

        let mut decoder = Decoder::new(&values[4..6], bit_width);

        let run = decoder.next().unwrap();

        if let HybridEncoded::Bitpacked(values) = run {
            assert_eq!(values, &[0b00001011]);
            let mut de = vec![0; 32];
            bitpacking::decode(&values, bit_width as u8, &mut de);
            let result = &de[..length];
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
            let mut de = vec![0; 32];
            bitpacking::decode(&values, bit_width as u8, &mut de);
            let result = &de[..10];
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
