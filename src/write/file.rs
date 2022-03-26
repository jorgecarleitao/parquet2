use std::io::Write;

use parquet_format_async_temp::FileMetaData;

use parquet_format_async_temp::thrift::protocol::TCompactOutputProtocol;
use parquet_format_async_temp::thrift::protocol::TOutputProtocol;
use parquet_format_async_temp::RowGroup;

use crate::{
    error::{Error, Result},
    metadata::SchemaDescriptor,
    FOOTER_SIZE, PARQUET_MAGIC,
};

use super::indexes::{write_column_index, write_offset_index};
use super::page::PageWriteSpec;
use super::{row_group::write_row_group, RowGroupIter, WriteOptions};

pub use crate::metadata::KeyValue;

pub(super) fn start_file<W: Write>(writer: &mut W) -> Result<u64> {
    writer.write_all(&PARQUET_MAGIC)?;
    Ok(PARQUET_MAGIC.len() as u64)
}

pub(super) fn end_file<W: Write>(mut writer: &mut W, metadata: FileMetaData) -> Result<u64> {
    // Write metadata
    let mut protocol = TCompactOutputProtocol::new(&mut writer);
    let metadata_len = metadata.write_to_out_protocol(&mut protocol)? as i32;
    protocol.flush()?;

    // Write footer
    let metadata_bytes = metadata_len.to_le_bytes();
    let mut footer_buffer = [0u8; FOOTER_SIZE as usize];
    (0..4).for_each(|i| {
        footer_buffer[i] = metadata_bytes[i];
    });

    (&mut footer_buffer[4..]).write_all(&PARQUET_MAGIC)?;
    writer.write_all(&footer_buffer)?;
    Ok(metadata_len as u64 + FOOTER_SIZE)
}

/// An interface to write a parquet file.
/// Use `start` to write the header, `write` to write a row group,
/// and `end` to write the footer.
pub struct FileWriter<W: Write> {
    writer: W,
    schema: SchemaDescriptor,
    options: WriteOptions,
    created_by: Option<String>,

    offset: u64,
    row_groups: Vec<RowGroup>,
    page_specs: Vec<Vec<Vec<PageWriteSpec>>>,
}

// Accessors
impl<W: Write> FileWriter<W> {
    /// The options assigned to the file
    pub fn options(&self) -> &WriteOptions {
        &self.options
    }

    /// The [`SchemaDescriptor`] assigned to this file
    pub fn schema(&self) -> &SchemaDescriptor {
        &self.schema
    }
}

impl<W: Write> FileWriter<W> {
    /// Returns a new [`FileWriter`].
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
            page_specs: vec![],
        }
    }

    /// Writes the header of the file
    pub fn start(&mut self) -> Result<()> {
        self.offset = start_file(&mut self.writer)? as u64;
        Ok(())
    }

    /// Writes a row group to the file.
    ///
    /// This call is IO-bounded
    pub fn write<E>(&mut self, row_group: RowGroupIter<'_, E>) -> Result<()>
    where
        Error: From<E>,
        E: std::error::Error,
    {
        if self.offset == 0 {
            return Err(Error::General(
                "You must call `start` before writing the first row group".to_string(),
            ));
        }
        let ordinal = self.row_groups.len();
        let (group, specs, size) = write_row_group(
            &mut self.writer,
            self.offset,
            self.schema.columns(),
            self.options.compression,
            row_group,
            ordinal,
        )?;
        self.offset += size;
        self.row_groups.push(group);
        self.page_specs.push(specs);
        Ok(())
    }

    /// Writes the footer of the parquet file. Returns the total size of the file and the
    /// underlying writer.
    pub fn end(mut self, key_value_metadata: Option<Vec<KeyValue>>) -> Result<(u64, W)> {
        // compute file stats
        let num_rows = self.row_groups.iter().map(|group| group.num_rows).sum();

        if self.options.write_statistics {
            // write column indexes
            self.row_groups
                .iter_mut()
                .zip(self.page_specs.iter())
                .try_for_each(|(group, pages)| {
                    group.columns.iter_mut().zip(pages.iter()).try_for_each(
                        |(column, pages)| {
                            let offset = self.offset;
                            column.column_index_offset = Some(offset as i64);
                            self.offset += write_column_index(&mut self.writer, pages)?;
                            let length = self.offset - offset;
                            column.column_index_length = Some(length as i32);
                            Result::Ok(())
                        },
                    )?;
                    Result::Ok(())
                })?;

            // write offset index
            self.row_groups
                .iter_mut()
                .zip(self.page_specs.iter())
                .try_for_each(|(group, pages)| {
                    group.columns.iter_mut().zip(pages.iter()).try_for_each(
                        |(column, pages)| {
                            let offset = self.offset;
                            column.offset_index_offset = Some(offset as i64);
                            self.offset += write_offset_index(&mut self.writer, pages)?;
                            column.offset_index_length = Some((self.offset - offset) as i32);
                            Result::Ok(())
                        },
                    )?;
                    Result::Ok(())
                })?;
        };

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

        let len = end_file(&mut self.writer, metadata)?;
        Ok((self.offset + len, self.writer))
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::File, io::Cursor};

    use super::*;

    use crate::error::Result;
    use crate::read::read_metadata;
    use crate::tests::get_path;

    #[test]
    fn empty_file() -> Result<()> {
        let mut testdata = get_path();
        testdata.push("alltypes_plain.parquet");
        let mut file = File::open(testdata).unwrap();

        let mut metadata = read_metadata(&mut file)?;

        // take away all groups and rows
        metadata.row_groups = vec![];
        metadata.num_rows = 0;

        let mut writer = Cursor::new(vec![]);

        // write the file
        start_file(&mut writer)?;
        end_file(&mut writer, metadata.into_thrift()?)?;

        let a = writer.into_inner();

        // read it again:
        let result = read_metadata(&mut Cursor::new(a));
        assert!(result.is_ok());

        Ok(())
    }
}
