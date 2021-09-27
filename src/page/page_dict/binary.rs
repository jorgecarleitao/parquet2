use std::{any::Any, sync::Arc};

use crate::error::Result;
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
}

impl DictPage for BinaryPageDict {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn physical_type(&self) -> &PhysicalType {
        &PhysicalType::ByteArray
    }
}

fn read_plain(bytes: &[u8], length: usize) -> (Vec<u8>, Vec<i32>) {
    let mut bytes = bytes;
    let mut values = Vec::new();
    let mut offsets = Vec::with_capacity(length as usize + 1);
    offsets.push(0);

    let mut current_length = 0;
    offsets.extend((0..length).map(|_| {
        let slot_length = get_length(bytes) as i32;
        current_length += slot_length;
        values.extend_from_slice(&bytes[4..4 + slot_length as usize]);
        bytes = &bytes[4 + slot_length as usize..];
        current_length
    }));

    (values, offsets)
}

pub fn read(buf: &[u8], num_values: usize) -> Result<Arc<dyn DictPage>> {
    let (values, offsets) = read_plain(buf, num_values);
    Ok(Arc::new(BinaryPageDict::new(values, offsets)))
}
