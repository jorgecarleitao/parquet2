use std::{any::Any, sync::Arc};

use crate::error::Result;
use crate::schema::types::PhysicalType;

use super::PageDict;

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
}

impl PageDict for FixedLenByteArrayPageDict {
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

pub fn read_page_dict(
    buf: &[u8],
    physical_type: PhysicalType,
    num_values: u32,
) -> Result<Arc<dyn PageDict>> {
    let size = if let PhysicalType::FixedLenByteArray(size) = &physical_type {
        *size as usize
    } else {
        panic!()
    };
    let values = read_plain(buf, size, num_values as usize);
    Ok(Arc::new(FixedLenByteArrayPageDict::new(
        values,
        physical_type,
        size,
    )))
}
