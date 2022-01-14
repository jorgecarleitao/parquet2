use std::io::Write;

use futures::AsyncWrite;
use parquet_format_async_temp::{ColumnChunk, RowGroup};

use crate::{
    compression::Compression,
    error::{ParquetError, Result},
    metadata::{ColumnChunkMetaData, ColumnDescriptor},
    page::CompressedPage,
};

use super::{
    column_chunk::{write_column_chunk, write_column_chunk_async},
    DynIter, DynStreamingIterator,
};

pub struct ColumnOffsetsMetadata {
    pub dictionary_page_offset: Option<i64>,
    pub data_page_offset: Option<i64>,
}

impl ColumnOffsetsMetadata {
    pub fn from_column_chunk(column_chunk: &ColumnChunk) -> ColumnOffsetsMetadata {
        ColumnOffsetsMetadata {
            dictionary_page_offset: column_chunk
                .meta_data
                .as_ref()
                .map(|meta| meta.dictionary_page_offset)
                .unwrap_or(None),
            data_page_offset: column_chunk
                .meta_data
                .as_ref()
                .map(|meta| meta.data_page_offset),
        }
    }

    pub fn from_column_chunk_metadata(
        column_chunk_metadata: &ColumnChunkMetaData,
    ) -> ColumnOffsetsMetadata {
        ColumnOffsetsMetadata {
            dictionary_page_offset: column_chunk_metadata.dictionary_page_offset(),
            data_page_offset: Some(column_chunk_metadata.data_page_offset()),
        }
    }

    pub fn calc_row_group_file_offset(&self) -> Option<i64> {
        self.dictionary_page_offset
            .filter(|x| *x > 0_i64)
            .or(self.data_page_offset)
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
    num_rows: usize,
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
    let file_offest = columns
        .iter()
        .next()
        .map(|column_chunk| {
            ColumnOffsetsMetadata::from_column_chunk(column_chunk).calc_row_group_file_offset()
        })
        .unwrap_or(None);

    let total_byte_size = columns
        .iter()
        .map(|c| c.meta_data.as_ref().unwrap().total_compressed_size)
        .sum();

    Ok((
        RowGroup {
            columns,
            total_byte_size,
            num_rows: num_rows as i64,
            sorting_columns: None,
            file_offset: file_offest,
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
    num_rows: usize,
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
    let file_offest = columns
        .iter()
        .next()
        .map(|column_chunk| {
            ColumnOffsetsMetadata::from_column_chunk(column_chunk).calc_row_group_file_offset()
        })
        .unwrap_or(None);

    let total_byte_size = columns
        .iter()
        .map(|c| c.meta_data.as_ref().unwrap().total_compressed_size)
        .sum();

    Ok((
        RowGroup {
            columns,
            total_byte_size,
            num_rows: num_rows as i64,
            sorting_columns: None,
            file_offset: file_offest,
            total_compressed_size: None,
            ordinal: None,
        },
        bytes_written,
    ))
}
