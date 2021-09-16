use std::sync::Arc;

use parquet_format_async_temp::Statistics as ParquetStatistics;

use super::Statistics;
use crate::{
    error::{ParquetError, Result},
    schema::types::PhysicalType,
};

#[derive(Debug, Clone, PartialEq)]
pub struct FixedLenStatistics {
    pub null_count: Option<i64>,
    pub distinct_count: Option<i64>,
    pub max_value: Option<Vec<u8>>,
    pub min_value: Option<Vec<u8>>,
    pub(self) physical_type: PhysicalType,
}

impl Statistics for FixedLenStatistics {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn physical_type(&self) -> &PhysicalType {
        &self.physical_type
    }

    fn null_count(&self) -> Option<i64> {
        self.null_count
    }
}

pub fn read(v: &ParquetStatistics, size: i32) -> Result<Arc<dyn Statistics>> {
    if let Some(ref v) = v.max_value {
        if v.len() != size as usize {
            return Err(ParquetError::OutOfSpec(
                "The max_value of statistics MUST be plain encoded".to_string(),
            ));
        }
    };
    if let Some(ref v) = v.min_value {
        if v.len() != size as usize {
            return Err(ParquetError::OutOfSpec(
                "The min_value of statistics MUST be plain encoded".to_string(),
            ));
        }
    };

    Ok(Arc::new(FixedLenStatistics {
        null_count: v.null_count,
        distinct_count: v.distinct_count,
        max_value: v.max_value.clone().map(|mut x| {
            x.truncate(size as usize);
            x
        }),
        min_value: v.min_value.clone().map(|mut x| {
            x.truncate(size as usize);
            x
        }),
        physical_type: PhysicalType::FixedLenByteArray(size),
    }))
}

pub fn write(v: &FixedLenStatistics) -> ParquetStatistics {
    ParquetStatistics {
        null_count: v.null_count,
        distinct_count: v.distinct_count,
        max_value: v.max_value.clone(),
        min_value: v.min_value.clone(),
        min: None,
        max: None,
    }
}
