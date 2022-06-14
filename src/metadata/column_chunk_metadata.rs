use std::sync::Arc;

use parquet_format_async_temp::{ColumnChunk, ColumnMetaData, Encoding};

use super::column_descriptor::ColumnDescriptor;
use crate::compression::Compression;
use crate::error::{Error, Result};
use crate::schema::types::PhysicalType;
use crate::statistics::{deserialize_statistics, Statistics};

/// Metadata for a column chunk.
// This contains the `ColumnDescriptor` associated with the chunk so that deserializers have
// access to the descriptor (e.g. physical, converted, logical).
#[derive(Debug, Clone)]
pub struct ColumnChunkMetaData {
    column_chunk: ColumnChunk,
    column_descr: ColumnDescriptor,
}

// Represents common operations for a column chunk.
impl ColumnChunkMetaData {
    /// Returns a new [`ColumnChunkMetaData`]
    pub fn new(column_chunk: ColumnChunk, column_descr: ColumnDescriptor) -> Self {
        Self {
            column_chunk,
            column_descr,
        }
    }

    /// File where the column chunk is stored.
    ///
    /// If not set, assumed to belong to the same file as the metadata.
    /// This path is relative to the current file.
    pub fn file_path(&self) -> &Option<String> {
        &self.column_chunk.file_path
    }

    /// Byte offset in `file_path()`.
    pub fn file_offset(&self) -> i64 {
        self.column_chunk.file_offset
    }

    /// Returns this column's [`ColumnChunk`]
    pub fn column_chunk(&self) -> &ColumnChunk {
        &self.column_chunk
    }

    /// The column's [`ColumnMetaData`]
    pub fn metadata(&self) -> &ColumnMetaData {
        self.column_chunk.meta_data.as_ref().unwrap()
    }

    /// The [`ColumnDescriptor`] for this column. This descriptor contains the physical and logical type
    /// of the pages.
    pub fn descriptor(&self) -> &ColumnDescriptor {
        &self.column_descr
    }

    /// The [`PhysicalType`] of this column.
    pub fn physical_type(&self) -> PhysicalType {
        self.column_descr.descriptor.primitive_type.physical_type
    }

    /// Decodes the raw statistics into [`Statistics`].
    pub fn statistics(&self) -> Option<Result<Arc<dyn Statistics>>> {
        self.metadata()
            .statistics
            .as_ref()
            .map(|x| deserialize_statistics(x, self.column_descr.descriptor.primitive_type.clone()))
    }

    /// Total number of values in this column chunk. Note that this is not necessarily the number
    /// of rows. E.g. the (nested) array `[[1, 2], [3]]` has 2 rows and 3 values.
    pub fn num_values(&self) -> i64 {
        self.metadata().num_values
    }

    /// [`Compression`] for this column.
    pub fn compression(&self) -> Compression {
        self.metadata().codec.try_into().unwrap()
    }

    /// Returns the total compressed data size of this column chunk.
    pub fn compressed_size(&self) -> i64 {
        self.metadata().total_compressed_size
    }

    /// Returns the total uncompressed data size of this column chunk.
    pub fn uncompressed_size(&self) -> i64 {
        self.metadata().total_uncompressed_size
    }

    /// Returns the offset for the column data.
    pub fn data_page_offset(&self) -> i64 {
        self.metadata().data_page_offset
    }

    /// Returns `true` if this column chunk contains a index page, `false` otherwise.
    pub fn has_index_page(&self) -> bool {
        self.metadata().index_page_offset.is_some()
    }

    /// Returns the offset for the index page.
    pub fn index_page_offset(&self) -> Option<i64> {
        self.metadata().index_page_offset
    }

    /// Returns the offset for the dictionary page, if any.
    pub fn dictionary_page_offset(&self) -> Option<i64> {
        self.metadata().dictionary_page_offset
    }

    /// Returns the encoding for this column
    pub fn column_encoding(&self) -> &Vec<Encoding> {
        &self.metadata().encodings
    }

    /// Returns the offset and length in bytes of the column chunk within the file
    pub fn byte_range(&self) -> (u64, u64) {
        let col_start = if let Some(dict_page_offset) = self.dictionary_page_offset() {
            dict_page_offset
        } else {
            self.data_page_offset()
        };
        let col_len = self.compressed_size();
        assert!(
            col_start >= 0 && col_len >= 0,
            "column start and length should not be negative"
        );
        (col_start as u64, col_len as u64)
    }

    /// Method to convert from Thrift.
    pub(crate) fn try_from_thrift(
        column_descr: ColumnDescriptor,
        column_chunk: ColumnChunk,
    ) -> Result<Self> {
        // validate metadata
        if let Some(meta) = &column_chunk.meta_data {
            let _: usize = meta.total_compressed_size.try_into()?;

            if let Some(offset) = meta.dictionary_page_offset {
                let _: usize = offset.try_into()?;
            }
            let _: usize = meta.data_page_offset.try_into()?;

            let _: Compression = meta.codec.try_into()?;
        } else {
            return Err(Error::OutOfSpec(
                "Column chunk requires metdata".to_string(),
            ));
        }

        Ok(Self {
            column_chunk,
            column_descr,
        })
    }

    /// Method to convert to Thrift.
    pub fn into_thrift(self) -> ColumnChunk {
        self.column_chunk
    }
}
