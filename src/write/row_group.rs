use std::{
    error::Error,
    io::{Seek, Write},
};

use parquet_format::{CompressionCodec, RowGroup};

use crate::{
    error::{ParquetError, Result},
    metadata::SchemaDescriptor,
    read::CompressedPage,
};

use super::column_chunk::write_column_chunk;

fn same_elements<T: PartialEq + Copy>(arr: &[T]) -> Option<Option<T>> {
    if arr.is_empty() {
        return Some(None);
    }
    let first = &arr[0];
    if arr.iter().all(|item| item == first) {
        Some(Some(*first))
    } else {
        None
    }
}

pub fn write_row_group<
    W,
    I,  // iterator over pages
    II, // iterator over columns
    E,  // external error any of the iterators may emit
>(
    writer: &mut W,
    schema: &SchemaDescriptor,
    codec: CompressionCodec,
    columns: II,
) -> Result<RowGroup>
where
    W: Write + Seek,
    I: Iterator<Item = std::result::Result<CompressedPage, E>>,
    II: Iterator<Item = std::result::Result<I, E>>,
    E: Error + 'static,
{
    let descriptors = schema.columns();
    let column_iter = descriptors.iter().zip(columns);

    let columns = column_iter
        .map(|(descriptor, page_iter)| {
            write_column_chunk(
                writer,
                descriptor,
                codec,
                page_iter.map_err(ParquetError::from_external_error)?,
            )
        })
        .collect::<Result<Vec<_>>>()?;

    // compute row group stats
    let num_rows = columns
        .iter()
        .map(|c| c.meta_data.as_ref().unwrap().num_values)
        .collect::<Vec<_>>();
    let num_rows = match same_elements(&num_rows) {
        None => return Err(general_err!("Every column chunk in a row group MUST have the same number of rows. The columns have rows: {:?}", num_rows)),
        Some(None) => 0,
        Some(Some(v)) => v
    };

    let total_byte_size = columns
        .iter()
        .map(|c| c.meta_data.as_ref().unwrap().total_compressed_size)
        .sum();

    Ok(RowGroup {
        columns,
        total_byte_size,
        num_rows,
        sorting_columns: None,
    })
}
