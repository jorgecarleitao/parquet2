use std::{any::Any, sync::Arc};

use crate::error::{Error, Result};
use crate::schema::types::PhysicalType;

use super::DictPage;

#[derive(Debug)]
pub struct FixedLenByteArrayPageDict {
    values: Vec<u8>,
    physical_type: PhysicalType,
    size: usize,
}

impl FixedLenByteArrayPageDict {
    pub fn new(values: Vec<u8>, physical_type: PhysicalType, size: usize) -> Self {
        Self {
            values,
            physical_type,
            size,
        }
    }

    pub fn values(&self) -> &[u8] {
        &self.values
    }

    pub fn size(&self) -> usize {
        self.size
    }

    #[inline]
    pub fn value(&self, index: usize) -> Result<&[u8]> {
        self.values
            .get(index * self.size..(index + 1) * self.size)
            .ok_or_else(|| {
                Error::OutOfSpec(
                    "The data page has an index larger than the dictionary page values".to_string(),
                )
            })
    }
}

impl DictPage for FixedLenByteArrayPageDict {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn physical_type(&self) -> &PhysicalType {
        &self.physical_type
    }
}

fn read_plain(bytes: &[u8], size: usize, length: usize) -> Vec<u8> {
    bytes[..size * length].to_vec()
}

pub fn read(buf: &[u8], size: usize, num_values: usize) -> Result<Arc<dyn DictPage>> {
    let values = read_plain(buf, size, num_values);
    Ok(Arc::new(FixedLenByteArrayPageDict::new(
        values,
        PhysicalType::FixedLenByteArray(size),
        size,
    )))
}
