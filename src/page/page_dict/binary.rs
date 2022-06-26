use std::{any::Any, sync::Arc};

use crate::error::Error;
use crate::{encoding::get_length, schema::types::PhysicalType};

use super::DictPage;

#[derive(Debug)]
pub struct BinaryPageDict {
    values: Vec<u8>,
    offsets: Vec<i32>,
}

impl BinaryPageDict {
    pub fn new(values: Vec<u8>, offsets: Vec<i32>) -> Self {
        Self { values, offsets }
    }

    pub fn values(&self) -> &[u8] {
        &self.values
    }

    pub fn offsets(&self) -> &[i32] {
        &self.offsets
    }

    #[inline]
    pub fn value(&self, index: usize) -> Result<&[u8], Error> {
        let end = *self.offsets.get(index + 1).ok_or_else(|| {
            Error::OutOfSpec(
                "The data page has an index larger than the dictionary page values".to_string(),
            )
        })?;
        let end: usize = end.try_into()?;
        let start: usize = self.offsets[index].try_into()?;
        Ok(&self.values[start..end])
    }
}

impl DictPage for BinaryPageDict {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn physical_type(&self) -> &PhysicalType {
        &PhysicalType::ByteArray
    }
}

fn read_plain(bytes: &[u8], length: usize) -> Result<(Vec<u8>, Vec<i32>), Error> {
    let mut bytes = bytes;
    let mut values = Vec::new();
    let mut offsets = Vec::with_capacity(length as usize + 1);
    offsets.push(0);

    let mut current_length = 0;
    offsets.reserve(length);
    for _ in 0..length {
        let slot_length = get_length(bytes).unwrap();
        bytes = &bytes[4..];
        current_length += slot_length as i32;

        if slot_length > bytes.len() {
            return Err(Error::OutOfSpec(
                "The string on a dictionary page has a length that is out of bounds".to_string(),
            ));
        }
        let (result, remaining) = bytes.split_at(slot_length);

        values.extend_from_slice(result);
        bytes = remaining;
        offsets.push(current_length);
    }

    Ok((values, offsets))
}

pub fn read(buf: &[u8], num_values: usize) -> Result<Arc<dyn DictPage>, Error> {
    let (values, offsets) = read_plain(buf, num_values)?;
    Ok(Arc::new(BinaryPageDict::new(values, offsets)))
}
