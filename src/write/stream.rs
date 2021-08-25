use futures::stream::Stream;
use futures::StreamExt;
use futures::TryStreamExt;

use std::{
    error::Error,
    io::{Seek, Write},
};

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
) -> Result<()>
where
    W: Write + Seek,
    S: Stream<Item = std::result::Result<RowGroupIter<E>, E>>,
    E: Error + Send + Sync + 'static,
{
    start_file(writer)?;

    let row_groups = row_groups
        .map(|row_group| {
            write_row_group(
                writer,
                schema.columns(),
                options.compression,
                row_group.map_err(ParquetError::from_external_error)?,
            )
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

    end_file(writer, metadata)?;
    Ok(())
}

pub use super::stream_stream::write_stream_stream;
