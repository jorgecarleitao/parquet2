/// Decodes according to [Plain strings](https://github.com/apache/parquet-format/blob/master/Encodings.md#plain-plain--0),
/// prefixes, lengths and values
/// # Implementation
/// This struct does not allocate on the heap.

#[derive(Debug)]
pub struct BinaryIter<'a> {
    values: &'a [u8],
    length: Option<usize>,
}

impl<'a> BinaryIter<'a> {
    pub fn new(values: &'a [u8], length: Option<usize>) -> Self {
        Self { values, length }
    }
}

impl<'a> Iterator for BinaryIter<'a> {
    type Item = &'a [u8];

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.values.len() < 4 {
            return None;
        }
        if let Some(x) = self.length.as_mut() {
            *x = x.saturating_sub(1)
        }
        let length = u32::from_le_bytes(self.values[0..4].try_into().unwrap()) as usize;
        self.values = &self.values[4..];
        let result = &self.values[..length];
        self.values = &self.values[length..];
        Some(result)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.length.unwrap_or_default(), self.length)
    }
}
