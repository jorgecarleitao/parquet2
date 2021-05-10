use std::sync::Arc;

use parquet_format::Statistics as ParquetStatistics;

use super::Statistics;
use crate::{
    error::{ParquetError, Result},
    schema::types::PhysicalType,
};

#[derive(Debug)]
pub struct BooleanStatistics {
    pub null_count: Option<i64>,
    pub distinct_count: Option<i64>,
    pub max_value: Option<bool>,
    pub min_value: Option<bool>,
}

impl Statistics for BooleanStatistics {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn physical_type(&self) -> &PhysicalType {
        &PhysicalType::Boolean
    }
}

pub fn read(v: &ParquetStatistics) -> Result<Arc<dyn Statistics>> {
    if let Some(ref v) = v.max_value {
        if v.len() != std::mem::size_of::<bool>() {
            return Err(ParquetError::OutOfSpec(
                "The max_value of statistics MUST be plain encoded".to_string(),
            ));
        }
    };
    if let Some(ref v) = v.min_value {
        if v.len() != std::mem::size_of::<bool>() {
            return Err(ParquetError::OutOfSpec(
                "The min_value of statistics MUST be plain encoded".to_string(),
            ));
        }
    };

    Ok(Arc::new(BooleanStatistics {
        null_count: v.null_count,
        distinct_count: v.distinct_count,
        max_value: v.max_value.as_ref().and_then(|x| x.get(0)).map(|x| *x != 0),
        min_value: v.min_value.as_ref().and_then(|x| x.get(0)).map(|x| *x != 0),
    }))
}
