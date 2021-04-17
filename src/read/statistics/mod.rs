mod binary;
mod fixed_len_binary;
mod primitive;

use std::{any::Any, sync::Arc};

use parquet_format::Statistics as ParquetStatistics;

use crate::error::Result;
use crate::schema::types::PhysicalType;

pub use binary::BinaryStatistics;
pub use fixed_len_binary::FixedLenStatistics;
pub use primitive::PrimitiveStatistics;

pub trait Statistics: Send + Sync + std::fmt::Debug {
    fn as_any(&self) -> &dyn Any;

    fn physical_type(&self) -> &PhysicalType;
}

pub fn read_statistics(
    statistics: &ParquetStatistics,
    physical_type: &PhysicalType,
) -> Result<Arc<dyn Statistics>> {
    match physical_type {
        PhysicalType::Boolean => todo!(),
        PhysicalType::Int32 => primitive::read::<i32>(statistics),
        PhysicalType::Int64 => primitive::read::<i64>(statistics),
        PhysicalType::Int96 => primitive::read::<[u32; 3]>(statistics),
        PhysicalType::Float => primitive::read::<f32>(statistics),
        PhysicalType::Double => primitive::read::<f64>(statistics),
        PhysicalType::ByteArray => binary::read(statistics),
        PhysicalType::FixedLenByteArray(size) => fixed_len_binary::read(statistics, *size),
    }
}
