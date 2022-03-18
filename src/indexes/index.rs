use std::any::Any;

use parquet_format_async_temp::ColumnIndex;

use crate::parquet_bridge::BoundaryOrder;
use crate::{error::ParquetError, schema::types::PhysicalType, types::NativeType};

pub trait Index: Send + Sync + std::fmt::Debug {
    fn as_any(&self) -> &dyn Any;

    fn physical_type(&self) -> &PhysicalType;
}

/// An index of a column of [`NativeType`] physical representation
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct NativeIndex<T: NativeType> {
    pub indexes: Vec<PageIndex<T>>,
    pub boundary_order: BoundaryOrder,
}

/// The index of a page, containing the min and max values of the page.
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct PageIndex<T> {
    /// The minimum value in the page. It is None when all values are null
    pub min: Option<T>,
    /// The maximum value in the page. It is None when all values are null
    pub max: Option<T>,
    /// The number of null values in the page
    pub null_count: Option<i64>,
}

impl<T: NativeType> TryFrom<ColumnIndex> for NativeIndex<T> {
    type Error = ParquetError;

    fn try_from(index: ColumnIndex) -> Result<Self, ParquetError> {
        let len = index.min_values.len();

        let null_counts = index
            .null_counts
            .map(|x| x.into_iter().map(Some).collect::<Vec<_>>())
            .unwrap_or_else(|| vec![None; len]);

        let indexes = index
            .min_values
            .iter()
            .zip(index.max_values.into_iter())
            .zip(index.null_pages.into_iter())
            .zip(null_counts.into_iter())
            .map(|(((min, max), is_null), null_count)| {
                let (min, max) = if is_null {
                    (None, None)
                } else {
                    let min = min.as_slice().try_into()?;
                    let max = max.as_slice().try_into()?;
                    (Some(T::from_le_bytes(min)), Some(T::from_le_bytes(max)))
                };
                Ok(PageIndex {
                    min,
                    max,
                    null_count,
                })
            })
            .collect::<Result<Vec<_>, ParquetError>>()?;

        Ok(Self {
            indexes,
            boundary_order: index.boundary_order.try_into()?,
        })
    }
}

impl<T: NativeType> Index for NativeIndex<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn physical_type(&self) -> &PhysicalType {
        &T::TYPE
    }
}

/// An index of a column of bytes physical type
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct ByteIndex {
    pub indexes: Vec<PageIndex<Vec<u8>>>,
    pub boundary_order: BoundaryOrder,
}

impl TryFrom<ColumnIndex> for ByteIndex {
    type Error = ParquetError;

    fn try_from(index: ColumnIndex) -> Result<Self, ParquetError> {
        let len = index.min_values.len();

        let null_counts = index
            .null_counts
            .map(|x| x.into_iter().map(Some).collect::<Vec<_>>())
            .unwrap_or_else(|| vec![None; len]);

        let indexes = index
            .min_values
            .into_iter()
            .zip(index.max_values.into_iter())
            .zip(index.null_pages.into_iter())
            .zip(null_counts.into_iter())
            .map(|(((min, max), is_null), null_count)| {
                let (min, max) = if is_null {
                    (None, None)
                } else {
                    (Some(min), Some(max))
                };
                Ok(PageIndex {
                    min,
                    max,
                    null_count,
                })
            })
            .collect::<Result<Vec<_>, ParquetError>>()?;

        Ok(Self {
            indexes,
            boundary_order: index.boundary_order.try_into()?,
        })
    }
}

impl Index for ByteIndex {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn physical_type(&self) -> &PhysicalType {
        &PhysicalType::ByteArray
    }
}

/// An index of a column of fixed len byte physical type
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct FixedLenByteIndex {
    pub type_: PhysicalType,
    pub indexes: Vec<PageIndex<Vec<u8>>>,
    pub boundary_order: BoundaryOrder,
}

impl TryFrom<(ColumnIndex, i32)> for FixedLenByteIndex {
    type Error = ParquetError;

    fn try_from((index, size): (ColumnIndex, i32)) -> Result<Self, ParquetError> {
        let len = index.min_values.len();

        let null_counts = index
            .null_counts
            .map(|x| x.into_iter().map(Some).collect::<Vec<_>>())
            .unwrap_or_else(|| vec![None; len]);

        let indexes = index
            .min_values
            .into_iter()
            .zip(index.max_values.into_iter())
            .zip(index.null_pages.into_iter())
            .zip(null_counts.into_iter())
            .map(|(((min, max), is_null), null_count)| {
                let (min, max) = if is_null {
                    (None, None)
                } else {
                    (Some(min), Some(max))
                };
                Ok(PageIndex {
                    min,
                    max,
                    null_count,
                })
            })
            .collect::<Result<Vec<_>, ParquetError>>()?;

        Ok(Self {
            type_: PhysicalType::FixedLenByteArray(size),
            indexes,
            boundary_order: index.boundary_order.try_into()?,
        })
    }
}

impl Index for FixedLenByteIndex {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn physical_type(&self) -> &PhysicalType {
        &self.type_
    }
}
