use std::{any::Any, sync::Arc};

use crate::error::Result;
use crate::{schema::types::PhysicalType, types, types::NativeType};

use super::DictPage;

#[derive(Debug)]
pub struct PrimitivePageDict<T: NativeType> {
    values: Vec<T>,
}

impl<T: NativeType> PrimitivePageDict<T> {
    pub fn new(values: Vec<T>) -> Self {
        Self { values }
    }

    pub fn values(&self) -> &[T] {
        &self.values
    }
}

impl<T: NativeType> DictPage for PrimitivePageDict<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn physical_type(&self) -> &PhysicalType {
        &T::TYPE
    }
}

fn read_plain<T: NativeType>(values: &[u8]) -> Vec<T> {
    // read in plain
    let chunks = values.chunks_exact(std::mem::size_of::<T>());
    assert_eq!(chunks.remainder().len(), 0);
    chunks.map(|chunk| types::decode(chunk)).collect()
}

pub fn read<T: NativeType>(
    buf: &[u8],
    num_values: usize,
    _is_sorted: bool,
) -> Result<Arc<dyn DictPage>> {
    let typed_size = num_values * std::mem::size_of::<T>();
    let values = read_plain::<T>(&buf[..typed_size]);
    Ok(Arc::new(PrimitivePageDict::new(values)))
}
