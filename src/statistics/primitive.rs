use std::sync::Arc;

use parquet_format_async_temp::Statistics as ParquetStatistics;

use super::Statistics;
use crate::metadata::ColumnDescriptor;
use crate::types;
use crate::{
    error::{ParquetError, Result},
    schema::types::PhysicalType,
};

#[derive(Debug, Clone, PartialEq)]
pub struct PrimitiveStatistics<T: types::NativeType> {
    pub descriptor: ColumnDescriptor,
    pub null_count: Option<i64>,
    pub distinct_count: Option<i64>,
    pub max_value: Option<T>,
    pub min_value: Option<T>,
}

impl<T: types::NativeType> Statistics for PrimitiveStatistics<T> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn physical_type(&self) -> &PhysicalType {
        &T::TYPE
    }

    fn null_count(&self) -> Option<i64> {
        self.null_count
    }
}

pub fn read<T: types::NativeType>(
    v: &ParquetStatistics,
    descriptor: ColumnDescriptor,
) -> Result<Arc<dyn Statistics>> {
    if let Some(ref v) = v.max_value {
        if v.len() != std::mem::size_of::<T>() {
            return Err(ParquetError::OutOfSpec(
                "The max_value of statistics MUST be plain encoded".to_string(),
            ));
        }
    };
    if let Some(ref v) = v.min_value {
        if v.len() != std::mem::size_of::<T>() {
            return Err(ParquetError::OutOfSpec(
                "The min_value of statistics MUST be plain encoded".to_string(),
            ));
        }
    };

    Ok(Arc::new(PrimitiveStatistics::<T> {
        descriptor,
        null_count: v.null_count,
        distinct_count: v.distinct_count,
        max_value: v.max_value.as_ref().map(|x| types::decode(x)),
        min_value: v.min_value.as_ref().map(|x| types::decode(x)),
    }))
}

pub fn write<T: types::NativeType>(v: &PrimitiveStatistics<T>) -> ParquetStatistics {
    ParquetStatistics {
        null_count: v.null_count,
        distinct_count: v.distinct_count,
        max_value: v.max_value.map(|x| x.to_le_bytes().as_ref().to_vec()),
        min_value: v.min_value.map(|x| x.to_le_bytes().as_ref().to_vec()),
        min: None,
        max: None,
    }
}
