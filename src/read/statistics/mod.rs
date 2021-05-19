mod binary;
mod boolean;
mod fixed_len_binary;
mod primitive;

use std::{any::Any, sync::Arc};

use parquet_format::Statistics as ParquetStatistics;

use crate::{error::Result, metadata::FileMetaData};
use crate::{
    metadata::ColumnChunkMetaData,
    schema::types::{ParquetType, PhysicalType},
};

pub use binary::BinaryStatistics;
pub use boolean::BooleanStatistics;
pub use fixed_len_binary::FixedLenStatistics;
pub use primitive::PrimitiveStatistics;

/// A trait used to describe specific statistics. Each physical type has its own struct.
/// Match the [`Statistics::physical_type`] to each type and downcast accordingly.
pub trait Statistics: Send + Sync + std::fmt::Debug {
    fn as_any(&self) -> &dyn Any;

    fn physical_type(&self) -> &PhysicalType;
}

/// Deserializes a raw parquet statistics into [`Statistics`].
/// # Error
/// This function errors if it is not possible to read the statistics to the
/// corresponding `physical_type`.
pub fn deserialize_statistics(
    statistics: &ParquetStatistics,
    physical_type: &PhysicalType,
) -> Result<Arc<dyn Statistics>> {
    match physical_type {
        PhysicalType::Boolean => boolean::read(statistics),
        PhysicalType::Int32 => primitive::read::<i32>(statistics),
        PhysicalType::Int64 => primitive::read::<i64>(statistics),
        PhysicalType::Int96 => primitive::read::<[u32; 3]>(statistics),
        PhysicalType::Float => primitive::read::<f32>(statistics),
        PhysicalType::Double => primitive::read::<f64>(statistics),
        PhysicalType::ByteArray => binary::read(statistics),
        PhysicalType::FixedLenByteArray(size) => fixed_len_binary::read(statistics, *size),
    }
}

/// Deserializes statistics in a column chunk.
/// # Error
/// Errors when the statistics cannot be deserialized. `None` when the statistics
/// is not available.
pub fn deserialize_column_statistics(
    column: &ColumnChunkMetaData,
) -> Result<Option<Arc<dyn Statistics>>> {
    let physical_type = match column.column_descriptor().type_() {
        ParquetType::PrimitiveType { physical_type, .. } => physical_type,
        _ => unreachable!(),
    };
    column
        .statistics()
        .as_ref()
        .map(|statistics| deserialize_statistics(&statistics, physical_type))
        .transpose()
}

/// Deserializes all row groups' statistics, converting them into [`Statistics`].
/// The first `Vec` corresponds to row groups, the second to columns.
/// Errors emerge when the statistics cannot be deserialized. Options emerge when the statistics
/// are not available.
/// # Implementation
/// This operation has no IO and amounts to deserializing all statistics.
#[allow(clippy::type_complexity)]
pub fn read_statistics(metadata: &FileMetaData) -> Vec<Vec<Result<Option<Arc<dyn Statistics>>>>> {
    metadata
        .row_groups
        .iter()
        .map(|row_group| {
            row_group
                .columns()
                .iter()
                .map(deserialize_column_statistics)
                .collect::<Vec<_>>()
        })
        .collect()
}
