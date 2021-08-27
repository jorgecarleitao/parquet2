use std::{
    error::Error,
    io::{Seek, Write},
};

use futures::{AsyncSeek, AsyncWrite};
use parquet_format_async_temp::RowGroup;

use crate::{
    compression::Compression,
    error::{ParquetError, Result},
    metadata::ColumnDescriptor,
    page::CompressedPage,
};

use super::{
    column_chunk::{write_column_chunk, write_column_chunk_async},
    DynIter,
};

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
    E, // external error any of the iterators may emit
>(
    writer: &mut W,
    descriptors: &[ColumnDescriptor],
    compression: Compression,
    columns: DynIter<std::result::Result<DynIter<std::result::Result<CompressedPage, E>>, E>>,
) -> Result<RowGroup>
where
    W: Write + Seek,
    E: Error + Send + Sync + 'static,
{
    let column_iter = descriptors.iter().zip(columns);

    let columns = column_iter
        .map(|(descriptor, page_iter)| {
            write_column_chunk(
                writer,
                descriptor,
                compression,
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
        file_offset: None,
        total_compressed_size: None,
        ordinal: None,
    })
}

pub async fn write_row_group_async<
    'a,
    W,
    E, // external error any of the iterators may emit
>(
    writer: &mut W,
    descriptors: &[ColumnDescriptor],
    compression: Compression,
    columns: DynIter<
        'a,
        std::result::Result<DynIter<'a, std::result::Result<CompressedPage, E>>, E>,
    >,
) -> Result<RowGroup>
where
    W: AsyncWrite + AsyncSeek + Unpin + Send,
    E: Error + Send + Sync + 'static,
{
    let column_iter = descriptors.iter().zip(columns);

    let mut columns = vec![];
    for (descriptor, page_iter) in column_iter {
        let spec = write_column_chunk_async(
            writer,
            descriptor,
            compression,
            page_iter.map_err(ParquetError::from_external_error)?,
        )
        .await?;
        columns.push(spec);
    }

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
        file_offset: None,
        total_compressed_size: None,
        ordinal: None,
    })
}
