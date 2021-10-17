use std::io::Write;

use futures::stream::Stream;
use futures::StreamExt;
use futures::TryStreamExt;

use parquet_format_async_temp::FileMetaData;

pub use crate::metadata::KeyValue;
use crate::{
    error::{ParquetError, Result},
    metadata::SchemaDescriptor,
};

use super::file::{end_file, start_file};
use super::{row_group::write_row_group, RowGroupIter, WriteOptions};

pub async fn write_stream<'a, W, S, E>(
    writer: &mut W,
    row_groups: S,
    schema: SchemaDescriptor,
    options: WriteOptions,
    created_by: Option<String>,
    key_value_metadata: Option<Vec<KeyValue>>,
) -> Result<u64>
where
    W: Write,
    S: Stream<Item = std::result::Result<RowGroupIter<'a, E>, E>>,
    ParquetError: From<E>,
    E: std::error::Error,
{
    let mut offset = start_file(writer)? as u64;

    let row_groups = row_groups
        .map(|row_group| {
            let (group, size) = write_row_group(
                writer,
                offset,
                schema.columns(),
                options.compression,
                row_group?,
            )?;
            offset += size;
            Result::Ok(group)
        })
        .try_collect::<Vec<_>>()
        .await?;

    // compute file stats
    let num_rows = row_groups.iter().map(|group| group.num_rows).sum();

    let metadata = FileMetaData::new(
        options.version.into(),
        schema.into_thrift()?,
        num_rows,
        row_groups,
        key_value_metadata,
        created_by,
        None,
        None,
        None,
    );

    let len = end_file(writer, metadata)?;
    Ok(offset + len)
}

pub use super::stream_stream::write_stream_stream;
