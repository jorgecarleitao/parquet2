use std::io::Write;

use futures::AsyncWrite;
use parquet_format_async_temp::RowGroup;

use crate::{
    compression::Compression,
    error::{ParquetError, Result},
    metadata::ColumnDescriptor,
    page::CompressedPage,
};

use super::{
    column_chunk::{write_column_chunk, write_column_chunk_async},
    DynIter, DynStreamingIterator,
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
    'a,
    W,
    E, // external error any of the iterators may emit
>(
    writer: &mut W,
    mut offset: u64,
    descriptors: &[ColumnDescriptor],
    compression: Compression,
    columns: DynIter<'a, std::result::Result<DynStreamingIterator<'a, CompressedPage, E>, E>>,
) -> Result<(RowGroup, u64)>
where
    W: Write,
    ParquetError: From<E>,
    E: std::error::Error,
{
    let column_iter = descriptors.iter().zip(columns);

    let initial = offset;
    let columns = column_iter
        .map(|(descriptor, page_iter)| {
            let (column, size) =
                write_column_chunk(writer, offset, descriptor, compression, page_iter?)?;
            offset += size;
            Ok(column)
        })
        .collect::<Result<Vec<_>>>()?;
    let bytes_written = offset - initial;

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

    Ok((
        RowGroup {
            columns,
            total_byte_size,
            num_rows,
            sorting_columns: None,
            file_offset: None,
            total_compressed_size: None,
            ordinal: None,
        },
        bytes_written,
    ))
}

pub async fn write_row_group_async<
    'a,
    W,
    E, // external error any of the iterators may emit
>(
    writer: &mut W,
    mut offset: u64,
    descriptors: &[ColumnDescriptor],
    compression: Compression,
    columns: DynIter<'a, std::result::Result<DynStreamingIterator<'a, CompressedPage, E>, E>>,
) -> Result<(RowGroup, u64)>
where
    W: AsyncWrite + Unpin + Send,
    ParquetError: From<E>,
    E: std::error::Error,
{
    let column_iter = descriptors.iter().zip(columns);

    let initial = offset;
    let mut columns = vec![];
    for (descriptor, page_iter) in column_iter {
        let (spec, size) =
            write_column_chunk_async(writer, offset, descriptor, compression, page_iter?).await?;
        offset += size as u64;
        columns.push(spec);
    }
    let bytes_written = offset - initial;

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

    Ok((
        RowGroup {
            columns,
            total_byte_size,
            num_rows,
            sorting_columns: None,
            file_offset: None,
            total_compressed_size: None,
            ordinal: None,
        },
        bytes_written,
    ))
}
