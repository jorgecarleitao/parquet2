use std::io::Write;

use futures::{AsyncWrite, AsyncWriteExt};

use parquet_format_async_temp::{
    thrift::protocol::{TCompactOutputStreamProtocol, TOutputStreamProtocol},
    FileMetaData, RowGroup,
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

/// An interface to write a parquet file asynchronously.
/// Use `start` to write the header, `write` to write a row group,
/// and `end` to write the footer.
pub struct FileStreamer<W: AsyncWrite + Unpin + Send> {
    writer: W,
    schema: SchemaDescriptor,
    options: WriteOptions,
    created_by: Option<String>,

    offset: u64,
    row_groups: Vec<RowGroup>,
}

// Accessors
impl<W: AsyncWrite + Unpin + Send> FileStreamer<W> {
    /// The options assigned to the file
    pub fn options(&self) -> &WriteOptions {
        &self.options
    }

    /// The [`SchemaDescriptor`] assigned to this file
    pub fn schema(&self) -> &SchemaDescriptor {
        &self.schema
    }
}

impl<W: AsyncWrite + Unpin + Send> FileStreamer<W> {
    /// Returns a new [`FileStreamer`].
    pub fn new(
        writer: W,
        schema: SchemaDescriptor,
        options: WriteOptions,
        created_by: Option<String>,
    ) -> Self {
        Self {
            writer,
            schema,
            options,
            created_by,
            offset: 0,
            row_groups: vec![],
        }
    }

    /// Writes the header of the file
    pub async fn start(&mut self) -> Result<()> {
        self.offset = start_file(&mut self.writer).await? as u64;
        Ok(())
    }

    /// Writes a row group to the file.
    pub async fn write<E>(&mut self, row_group: RowGroupIter<'_, E>, num_rows: usize) -> Result<()>
    where
        ParquetError: From<E>,
        E: std::error::Error,
    {
        if self.offset == 0 {
            return Err(ParquetError::General(
                "You must call `start` before writing the first row group".to_string(),
            ));
        }
        let (group, size) = write_row_group_async(
            &mut self.writer,
            self.offset,
            self.schema.columns(),
            self.options.compression,
            row_group,
            num_rows,
        )
        .await?;
        self.offset += size;
        self.row_groups.push(group);
        Ok(())
    }

    /// Writes the footer of the parquet file. Returns the total size of the file and the
    /// underlying writer.
    pub async fn end(mut self, key_value_metadata: Option<Vec<KeyValue>>) -> Result<(u64, W)> {
        // compute file stats
        let num_rows = self.row_groups.iter().map(|group| group.num_rows).sum();

        let metadata = FileMetaData::new(
            self.options.version.into(),
            self.schema.into_thrift()?,
            num_rows,
            self.row_groups,
            key_value_metadata,
            self.created_by,
            None,
            None,
            None,
        );

        let len = end_file(&mut self.writer, metadata).await?;
        Ok((self.offset + len, self.writer))
    }
}
