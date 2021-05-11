use crate::encoding::get_length;

/// Decodes according to [Plain strings](https://github.com/apache/parquet-format/blob/master/Encodings.md#plain-plain--0),
/// prefixes, lengths and values
/// # Implementation
/// This struct does not allocate on the heap.
#[derive(Debug)]
pub struct Decoder<'a> {
    values: &'a [u8],
    index: usize,
}

impl<'a> Decoder<'a> {
    pub fn new(values: &'a [u8]) -> Self {
        Self { values, index: 0 }
    }
}

impl<'a> Iterator for Decoder<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        let values = self.values;
        let index = self.index;
        if index + 4 < values.len() {
            let next_len = get_length(values) as usize;
            let next_index = index + 4 + next_len;

            let result = Some(&values[index + 4..next_index]);
            self.index = next_index;

            result
        } else {
            None
        }
    }
}
