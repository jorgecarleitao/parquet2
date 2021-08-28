use std::{error::Error, io::Write};

use futures::{pin_mut, stream::Stream, AsyncWrite, AsyncWriteExt, StreamExt};

use parquet_format_async_temp::{
    thrift::protocol::{TCompactOutputStreamProtocol, TOutputStreamProtocol},
    FileMetaData,
};

use crate::{
    error::{ParquetError, Result},
    metadata::{KeyValue, SchemaDescriptor},
    FOOTER_SIZE, PARQUET_MAGIC,
};

use super::{row_group::write_row_group_async, RowGroupIter, WriteOptions};

async fn start_file<W: AsyncWrite + Unpin>(writer: &mut W) -> Result<u64> {
    writer.write_all(&PARQUET_MAGIC).await?;
    Ok(PARQUET_MAGIC.len() as u64)
}

async fn end_file<W: AsyncWrite + Unpin + Send>(
    mut writer: &mut W,
    metadata: FileMetaData,
) -> Result<u64> {
    // Write file metadata
    let mut protocol = TCompactOutputStreamProtocol::new(&mut writer);
    let metadata_len = metadata.write_to_out_stream_protocol(&mut protocol).await? as i32;
    protocol.flush().await?;

    // Write footer
    let metadata_bytes = metadata_len.to_le_bytes();
    let mut footer_buffer = [0u8; FOOTER_SIZE as usize];
    (0..4).for_each(|i| {
        footer_buffer[i] = metadata_bytes[i];
    });

    (&mut footer_buffer[4..]).write_all(&PARQUET_MAGIC)?;
    writer.write_all(&footer_buffer).await?;
    Ok(metadata_len as u64 + FOOTER_SIZE)
}

/// Given a stream of [`RowGroupIter`] and and an `async` writer, returns a future
/// of writing a parquet file to the writer.
pub async fn write_stream_stream<'a, W, S, E>(
    writer: &mut W,
    row_groups: S,
    schema: SchemaDescriptor,
    options: WriteOptions,
    created_by: Option<String>,
    key_value_metadata: Option<Vec<KeyValue>>,
) -> Result<u64>
where
    W: AsyncWrite + Unpin + Send,
    S: Stream<Item = std::result::Result<RowGroupIter<'a, E>, E>>,
    E: Error + Send + Sync + 'static,
{
    let mut offset = start_file(writer).await?;

    let mut row_groups_c = vec![];

    pin_mut!(row_groups);
    while let Some(row_group) = row_groups.next().await {
        let (row_group, size) = write_row_group_async(
            writer,
            offset,
            schema.columns(),
            options.compression,
            row_group.map_err(ParquetError::from_external_error)?,
        )
        .await?;
        offset += size;
        row_groups_c.push(row_group);
    }

    // compute file stats
    let num_rows = row_groups_c.iter().map(|group| group.num_rows).sum();

    let metadata = FileMetaData::new(
        options.version.into(),
        schema.into_thrift()?,
        num_rows,
        row_groups_c,
        key_value_metadata,
        created_by,
        None,
        None,
        None,
    );

    let len = end_file(writer, metadata).await?;
    Ok(offset + len)
}
