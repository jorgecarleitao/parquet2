use parquet_format_async_temp::BoundaryOrder;
use parquet_format_async_temp::ColumnIndex;
use parquet_format_async_temp::OffsetIndex;
use parquet_format_async_temp::PageLocation;

use crate::error::{ParquetError, Result};
pub use crate::metadata::KeyValue;
use crate::statistics::serialize_statistics;

use crate::write::page::{is_data_page, PageWriteSpec};

pub fn serialize_column_index(pages: &[PageWriteSpec]) -> Result<ColumnIndex> {
    let mut null_pages = Vec::with_capacity(pages.len());
    let mut min_values = Vec::with_capacity(pages.len());
    let mut max_values = Vec::with_capacity(pages.len());
    let mut null_counts = Vec::with_capacity(pages.len());

    pages
        .iter()
        .filter(|x| is_data_page(x))
        .try_for_each(|spec| {
            if let Some(stats) = &spec.statistics {
                let stats = serialize_statistics(stats.as_ref());

                let null_count = stats.null_count.ok_or_else(|| {
                    ParquetError::OutOfSpec("null count of a page is required".to_string())
                })?;

                null_counts.push(null_count);
                if null_count as usize == spec.num_values {
                    min_values.push(vec![0]);
                    max_values.push(vec![0]);
                    null_pages.push(true)
                } else {
                    min_values.push(stats.min_value.ok_or_else(|| {
                        ParquetError::OutOfSpec("min value of a page is required".to_string())
                    })?);
                    max_values.push(stats.max_value.ok_or_else(|| {
                        ParquetError::OutOfSpec("max value of a page is required".to_string())
                    })?);
                    null_pages.push(false)
                };

                Result::Ok(())
            } else {
                Err(ParquetError::OutOfSpec(
                    "options were set to write statistics but some pages miss them".to_string(),
                ))
            }
        })?;
    Ok(ColumnIndex {
        null_pages,
        min_values,
        max_values,
        boundary_order: BoundaryOrder::UNORDERED,
        null_counts: Some(null_counts),
    })
}

pub fn serialize_offset_index(pages: &[PageWriteSpec]) -> Result<OffsetIndex> {
    let mut first_row_index = 0;
    let page_locations = pages
        .iter()
        .filter(|x| is_data_page(x))
        .map(|spec| {
            let location = PageLocation {
                offset: spec.offset.try_into()?,
                compressed_page_size: spec.bytes_written.try_into()?,
                first_row_index,
            };
            let num_rows = spec.num_rows.ok_or_else(|| {
                ParquetError::OutOfSpec(
                    "options were set to write statistics but some data pages miss number of rows"
                        .to_string(),
                )
            })?;
            first_row_index += num_rows as i64;
            Ok(location)
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(OffsetIndex { page_locations })
}
