use super::super::delta_bitpacked;
use super::super::delta_length_byte_array;

/// Decodes according to [Delta strings](https://github.com/apache/parquet-format/blob/master/Encodings.md#delta-strings-delta_byte_array--7),
/// prefixes, lengths and values
/// # Implementation
/// This struct does not allocate on the heap.
#[derive(Debug)]
pub struct Decoder<'a> {
    values: &'a [u8],
    prefix_lengths: delta_bitpacked::Decoder<'a>,
}

impl<'a> Decoder<'a> {
    pub fn new(values: &'a [u8]) -> Self {
        let prefix_lengths = delta_bitpacked::Decoder::new(values);
        Self {
            values,
            prefix_lengths,
        }
    }

    pub fn into_lengths(self) -> delta_length_byte_array::Decoder<'a> {
        assert_eq!(self.prefix_lengths.size_hint().0, 0);
        delta_length_byte_array::Decoder::new(&self.values[self.prefix_lengths.consumed_bytes()..])
    }
}

impl<'a> Iterator for Decoder<'a> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        self.prefix_lengths.next().map(|x| x as u32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bla() {
        // VALIDATED from spark==3.1.1
        let data = &[
            128, 1, 4, 2, 0, 0, 0, 0, 0, 0, 128, 1, 4, 2, 10, 0, 0, 0, 0, 0, 72, 101, 108, 108,
            111, 87, 111, 114, 108, 100,
            // extra bytes are not from spark, but they should be ignored by the decoder
            // because they are beyond the sum of all lengths.
            1, 2, 3,
        ];
        // result of encoding
        let expected = &["Hello", "World"];
        let expected_lengths = expected.iter().map(|x| x.len() as i32).collect::<Vec<_>>();
        let expected_prefixes = vec![0, 0];
        let expected_values = expected.join("");
        let expected_values = expected_values.as_bytes();

        let mut decoder = Decoder::new(data);
        let prefixes = decoder.by_ref().collect::<Vec<_>>();
        assert_eq!(prefixes, expected_prefixes);

        // move to the lengths
        let mut decoder = decoder.into_lengths();

        let lengths = decoder.by_ref().collect::<Vec<_>>();
        assert_eq!(lengths, expected_lengths);

        // move to the values
        let values = decoder.into_values();
        assert_eq!(values, expected_values);
    }
}
