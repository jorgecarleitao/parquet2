use std::{any::Any, sync::Arc};

use crate::error::Result;
use crate::{schema::types::PhysicalType, types, types::NativeType};

use super::PageDict;

#[derive(Debug)]
pub struct PrimitivePageDict<T: NativeType> {
    values: Vec<T>,
    physical_type: PhysicalType,
}

impl<T: NativeType> PrimitivePageDict<T> {
    pub fn new(values: Vec<T>, physical_type: PhysicalType) -> Self {
        Self {
            values,
            physical_type,
        }
    }

    pub fn values(&self) -> &[T] {
        &self.values
    }
}

impl<T: NativeType> PageDict for PrimitivePageDict<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn physical_type(&self) -> &PhysicalType {
        &self.physical_type
    }
}

fn read_plain<T: NativeType>(values: &[u8]) -> Vec<T> {
    // read in plain
    let chunks = values.chunks_exact(std::mem::size_of::<T>());
    assert_eq!(chunks.remainder().len(), 0);
    chunks.map(|chunk| types::decode(chunk)).collect()
}

pub fn read_page_dict<T: NativeType>(
    buf: &[u8],
    num_values: u32,
    _is_sorted: bool,
    physical_type: PhysicalType,
) -> Result<Arc<dyn PageDict>> {
    let typed_size = num_values as usize * std::mem::size_of::<T>();
    let values = read_plain::<T>(&buf[..typed_size]);
    Ok(Arc::new(PrimitivePageDict::new(values, physical_type)))
}
