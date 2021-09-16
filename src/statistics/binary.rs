use std::sync::Arc;

use parquet_format_async_temp::Statistics as ParquetStatistics;

use super::Statistics;
use crate::{error::Result, metadata::ColumnDescriptor, schema::types::PhysicalType};

#[derive(Debug, Clone, PartialEq)]
pub struct BinaryStatistics {
    pub descriptor: ColumnDescriptor,
    pub null_count: Option<i64>,
    pub distinct_count: Option<i64>,
    pub max_value: Option<Vec<u8>>,
    pub min_value: Option<Vec<u8>>,
}

impl Statistics for BinaryStatistics {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn physical_type(&self) -> &PhysicalType {
        &PhysicalType::ByteArray
    }

    fn null_count(&self) -> Option<i64> {
        self.null_count
    }
}

pub fn read(v: &ParquetStatistics, descriptor: ColumnDescriptor) -> Result<Arc<dyn Statistics>> {
    Ok(Arc::new(BinaryStatistics {
        descriptor,
        null_count: v.null_count,
        distinct_count: v.distinct_count,
        max_value: v.max_value.clone(),
        min_value: v.min_value.clone(),
    }))
}

pub fn write(v: &BinaryStatistics) -> ParquetStatistics {
    ParquetStatistics {
        null_count: v.null_count,
        distinct_count: v.distinct_count,
        max_value: v.max_value.clone(),
        min_value: v.min_value.clone(),
        min: None,
        max: None,
    }
}
