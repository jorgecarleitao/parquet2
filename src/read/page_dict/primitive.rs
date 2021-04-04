use std::{any::Any, convert::TryFrom, convert::TryInto, sync::Arc};

use crate::error::Result;
use crate::{schema::types::PhysicalType, types::NativeType};

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

fn read_plain<'a, T: NativeType>(values: &'a [u8]) -> Vec<T>
where
    <T as NativeType>::Bytes: TryFrom<&'a [u8]>,
    <<T as NativeType>::Bytes as TryFrom<&'a [u8]>>::Error: std::fmt::Debug,
{
    // read in plain
    let chunks = values.chunks_exact(std::mem::size_of::<T>());
    assert_eq!(chunks.remainder().len(), 0);
    chunks
        .map(|chunk| {
            let chunk: T::Bytes = chunk.try_into().unwrap();
            T::from_le_bytes(chunk)
        })
        .collect()
}

pub fn read_page_dict<'a, T: NativeType>(
    buf: &'a [u8],
    _num_values: u32,
    _is_sorted: bool,
    physical_type: PhysicalType,
) -> Result<Arc<dyn PageDict>>
where
    <T as NativeType>::Bytes: TryFrom<&'a [u8]>,
    <<T as NativeType>::Bytes as TryFrom<&'a [u8]>>::Error: std::fmt::Debug,
{
    let values = read_plain::<T>(buf);
    Ok(Arc::new(PrimitivePageDict::new(values, physical_type)))
}
