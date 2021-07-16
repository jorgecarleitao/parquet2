use super::super::delta_bitpacked;

/// Decodes [Delta-length byte array](https://github.com/apache/parquet-format/blob/master/Encodings.md#delta-length-byte-array-delta_length_byte_array--6)
/// lengths and values.
/// # Implementation
/// This struct does not allocate on the heap.
/// # Example
/// ```
/// use parquet2::encoding::delta_length_byte_array::Decoder;
///
/// let expected = &["Hello", "World"];
/// let expected_lengths = expected.iter().map(|x| x.len() as i32).collect::<Vec<_>>();
/// let expected_values = expected.join("");
/// let expected_values = expected_values.as_bytes();
/// let data = &[
///     128, 1, 4, 2, 10, 0, 0, 0, 0, 0, 72, 101, 108, 108, 111, 87, 111, 114, 108, 100,
/// ];
///
/// let mut decoder = Decoder::new(data);
///
/// // Extract the lengths
/// let lengths = decoder.by_ref().collect::<Vec<_>>();
/// assert_eq!(lengths, expected_lengths);
///
/// // Extract the values. This _must_ be called after consuming all lengths by reference (see above).
/// let values = decoder.into_values();
///
/// assert_eq!(values, expected_values);
#[derive(Debug)]
pub struct Decoder<'a> {
    values: &'a [u8],
    lengths: delta_bitpacked::Decoder<'a>,
    total_length: u32,
}

impl<'a> Decoder<'a> {
    pub fn new(values: &'a [u8]) -> Self {
        let lengths = delta_bitpacked::Decoder::new(values);
        Self {
            values,
            lengths,
            total_length: 0,
        }
    }

    /// Consumes this decoder and returns the slice of concatenated values.
    /// # Panics
    /// This function panics if this iterator has not been fully consumed.
    pub fn into_values(self) -> &'a [u8] {
        assert_eq!(self.lengths.size_hint().0, 0);
        let start = self.lengths.consumed_bytes();
        &self.values[start..start + self.total_length as usize]
    }
}

impl<'a> Iterator for Decoder<'a> {
    type Item = i32;

    fn next(&mut self) -> Option<Self::Item> {
        let result = self.lengths.next();
        if let Some(result) = result {
            self.total_length += result as u32
        }
        result
    }
}
