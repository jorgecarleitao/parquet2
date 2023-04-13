use std::marker::PhantomData;
use crate::error::Error;
use crate::types::NativeType;

/// Decodes according to [Byte Stream Split](https://github.com/apache/parquet-format/blob/master/Encodings.md#byte-stream-split-byte_stream_split--9).
/// # Implementation
/// This struct does not allocate on the heap.
#[derive(Debug)]
pub struct Decoder<'a, T: NativeType> {
    values: &'a [u8],
    buffer: Vec<u8>,
    num_elements: usize,
    current: usize,
    element_size: usize,
    element_type: PhantomData<T>
}

impl<'a, T: NativeType> Decoder<'a, T> {
    pub fn try_new(values: &'a [u8]) -> Result<Self, Error> {
        let element_size = std::mem::size_of::<T>();
        let values_size = values.len();
        if values_size % element_size != 0 {
            return Err(Error::oos("Value array is not a multiple of element size"));
        }
        let num_elements = values.len() / element_size;
        Ok(Self {
            values,
            buffer: vec![0_u8; element_size],
            num_elements,
            current: 0,
            element_size,
            element_type: PhantomData
        })
    }
}

impl<'a, T: NativeType> Iterator for Decoder<'a, T> {
    type Item = Result<T, Error>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.num_elements {
            return None
        }

        for n in 0..self.element_size {
            self.buffer[n] = self.values[(self.num_elements * n) + self.current]
        }

        let value = T::from_le_bytes(self.buffer.as_slice().try_into().unwrap());

        self.current += 1;

        return Some(Ok(value));
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.num_elements, Some(self.num_elements))
    }
}
