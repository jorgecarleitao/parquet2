use std::io::Write;

use futures::pin_mut;
use futures::stream::Stream;
use futures::Future;
use futures::StreamExt;

use parquet_format_async_temp::FileMetaData;

pub use crate::metadata::KeyValue;
use crate::{
    error::{ParquetError, Result},
    metadata::SchemaDescriptor,
};

use super::file::{end_file, start_file};
use super::{row_group::write_row_group, RowGroupIter, WriteOptions};

pub async fn write_stream<'a, W, S, E, F>(
    writer: &mut W,
    row_groups: S,
    schema: SchemaDescriptor,
    options: WriteOptions,
    created_by: Option<String>,
    key_value_metadata: Option<Vec<KeyValue>>,
) -> Result<u64>
where
    W: Write,
    F: Future<Output = std::result::Result<RowGroupIter<'a, E>, E>>,
    S: Stream<Item = F>,
    ParquetError: From<E>,
    E: std::error::Error,
{
    let mut offset = start_file(writer)? as u64;

    let mut groups = vec![];
    pin_mut!(row_groups);
    while let Some(row_group) = row_groups.next().await {
        let (group, size) = write_row_group(
            writer,
            offset,
            schema.columns(),
            options.compression,
            row_group.await?,
        )?;
        offset += size;
        groups.push(group);
    }

    // compute file stats
    let num_rows = groups.iter().map(|group| group.num_rows).sum();

    let metadata = FileMetaData::new(
        options.version.into(),
        schema.into_thrift()?,
        num_rows,
        groups,
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
