use std::{
    error::Error,
    io::{SeekFrom, Write},
};

use futures::{
    pin_mut, stream::Stream, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt, StreamExt,
};

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

async fn start_file<W: AsyncWrite + Unpin>(writer: &mut W) -> Result<()> {
    Ok(writer.write_all(&PARQUET_MAGIC).await?)
}

async fn end_file<W: AsyncWrite + AsyncSeek + Unpin + Send>(
    mut writer: &mut W,
    metadata: FileMetaData,
) -> Result<()> {
    // Write file metadata
    let start_pos = writer.seek(SeekFrom::Current(0)).await?;
    {
        let mut protocol = TCompactOutputStreamProtocol::new(&mut writer);
        metadata.write_to_out_stream_protocol(&mut protocol).await?;
        protocol.flush().await?
    }
    let end_pos = writer.seek(SeekFrom::Current(0)).await?;
    let metadata_len = (end_pos - start_pos) as i32;

    // Write footer
    let metadata_len = metadata_len.to_le_bytes();
    let mut footer_buffer = [0u8; FOOTER_SIZE as usize];
    (0..4).for_each(|i| {
        footer_buffer[i] = metadata_len[i];
    });

    (&mut footer_buffer[4..]).write_all(&PARQUET_MAGIC)?;
    writer.write_all(&footer_buffer).await?;
    Ok(())
}

/// Given a stream of [`RowGroupIter`] and and an `async` writer, returns a future
/// of writing a parquet file to the writer.
pub async fn write_stream_stream<W, S, E>(
    writer: &mut W,
    row_groups: S,
    schema: SchemaDescriptor,
    options: WriteOptions,
    created_by: Option<String>,
    key_value_metadata: Option<Vec<KeyValue>>,
) -> Result<()>
where
    W: AsyncWrite + AsyncSeek + Unpin + Send,
    S: Stream<Item = std::result::Result<RowGroupIter<E>, E>>,
    E: Error + Send + Sync + 'static,
{
    start_file(writer).await?;

    let mut row_groups_c = vec![];

    pin_mut!(row_groups);
    while let Some(row_group) = row_groups.next().await {
        //for row_group in row_groups {
        let row_group = write_row_group_async(
            writer,
            schema.columns(),
            options.compression,
            row_group.map_err(ParquetError::from_external_error)?,
        )
        .await?;
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

    end_file(writer, metadata).await?;
    Ok(())
}
