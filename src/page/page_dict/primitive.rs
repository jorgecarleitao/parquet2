use std::{any::Any, sync::Arc};

use crate::error::{Error, Result};
use crate::{
    schema::types::PhysicalType,
    types::{decode, NativeType},
};

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

fn read_plain<T: NativeType>(values: &[u8]) -> Result<Vec<T>> {
    // read in plain
    if values.len() % std::mem::size_of::<T>() != 0 {
        return Err(Error::OutOfSpec(
            "A dictionary page with primitive values must contain a multiple of their format."
                .to_string(),
        ));
    }
    Ok(values
        .chunks_exact(std::mem::size_of::<T>())
        .map(decode::<T>)
        .collect())
}

pub fn read<T: NativeType>(
    buf: &[u8],
    num_values: usize,
    _is_sorted: bool,
) -> Result<Arc<dyn DictPage>> {
    let typed_size = num_values * std::mem::size_of::<T>();
    let values = read_plain::<T>(&buf[..typed_size])?;
    Ok(Arc::new(PrimitivePageDict::new(values)))
}
