use parquet_format::{ColumnChunk, ColumnMetaData, Encoding};

use super::{column_descriptor::ColumnDescriptor, column_path::ColumnPath};
use crate::errors::{ParquetError, Result};
use crate::{compression::CompressionCodec, schema::types::Type};

/// Metadata for a column chunk.
#[derive(Debug, Clone)]
pub struct ColumnChunkMetaData {
    column_type: Type,
    column_path: ColumnPath,
    column_descr: ColumnDescriptor,
    encodings: Vec<Encoding>,
    file_path: Option<String>,
    file_offset: i64,
    num_values: i64,
    compression: CompressionCodec,
    total_compressed_size: i64,
    total_uncompressed_size: i64,
    data_page_offset: i64,
    index_page_offset: Option<i64>,
    dictionary_page_offset: Option<i64>,
    //statistics: Option<Statistics>,
}

/// Represents common operations for a column chunk.
impl ColumnChunkMetaData {
    /// File where the column chunk is stored.
    ///
    /// If not set, assumed to belong to the same file as the metadata.
    /// This path is relative to the current file.
    pub fn file_path(&self) -> Option<&String> {
        self.file_path.as_ref()
    }

    /// Byte offset in `file_path()`.
    pub fn file_offset(&self) -> i64 {
        self.file_offset
    }

    /// Type of this column. Must be primitive.
    pub fn column_type(&self) -> &Type {
        &self.column_type
    }

    /// Path (or identifier) of this column.
    pub fn column_path(&self) -> &ColumnPath {
        &self.column_path
    }

    /// Descriptor for this column.
    pub fn column_descriptor(&self) -> &ColumnDescriptor {
        &self.column_descr
    }

    /// All encodings used for this column.
    pub fn encodings(&self) -> &[Encoding] {
        &self.encodings
    }

    /// Total number of values in this column chunk.
    pub fn num_values(&self) -> i64 {
        self.num_values
    }

    /// CompressionCodec for this column.
    pub fn compression(&self) -> &CompressionCodec {
        &self.compression
    }

    /// Returns the total compressed data size of this column chunk.
    pub fn compressed_size(&self) -> i64 {
        self.total_compressed_size
    }

    /// Returns the total uncompressed data size of this column chunk.
    pub fn uncompressed_size(&self) -> i64 {
        self.total_uncompressed_size
    }

    /// Returns the offset for the column data.
    pub fn data_page_offset(&self) -> i64 {
        self.data_page_offset
    }

    /// Returns `true` if this column chunk contains a index page, `false` otherwise.
    pub fn has_index_page(&self) -> bool {
        self.index_page_offset.is_some()
    }

    /// Returns the offset for the index page.
    pub fn index_page_offset(&self) -> Option<i64> {
        self.index_page_offset
    }

    /// Returns the offset for the dictionary page, if any.
    pub fn dictionary_page_offset(&self) -> Option<i64> {
        self.dictionary_page_offset
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
    pub fn try_from_thrift(column_descr: ColumnDescriptor, cc: &ColumnChunk) -> Result<Self> {
        if cc.meta_data.is_none() {
            return Err(general_err!("Expected to have column metadata"));
        }
        let mut col_metadata = cc.meta_data.clone().unwrap();
        let column_type = col_metadata.type_;
        let column_path = ColumnPath::new(col_metadata.path_in_schema);
        let encodings = col_metadata
            .encodings
            .drain(0..)
            .map(Encoding::from)
            .collect();
        let compression = col_metadata.codec;
        let file_path = cc.file_path.clone();
        let file_offset = cc.file_offset;
        let num_values = col_metadata.num_values;
        let total_compressed_size = col_metadata.total_compressed_size;
        let total_uncompressed_size = col_metadata.total_uncompressed_size;
        let data_page_offset = col_metadata.data_page_offset;
        let index_page_offset = col_metadata.index_page_offset;
        let dictionary_page_offset = col_metadata.dictionary_page_offset;
        let result = ColumnChunkMetaData {
            column_type,
            column_path,
            column_descr,
            encodings,
            file_path,
            file_offset,
            num_values,
            compression,
            total_compressed_size,
            total_uncompressed_size,
            data_page_offset,
            index_page_offset,
            dictionary_page_offset,
        };
        Ok(result)
    }

    /// Method to convert to Thrift.
    pub fn to_thrift(&self) -> ColumnChunk {
        let column_metadata = ColumnMetaData {
            type_: self.column_type,
            encodings: self.encodings().to_vec(),
            path_in_schema: Vec::from(self.column_path.as_ref()),
            codec: self.compression,
            num_values: self.num_values,
            total_uncompressed_size: self.total_uncompressed_size,
            total_compressed_size: self.total_compressed_size,
            key_value_metadata: None,
            data_page_offset: self.data_page_offset,
            index_page_offset: self.index_page_offset,
            dictionary_page_offset: self.dictionary_page_offset,
            statistics: None,
            encoding_stats: None,
        };

        ColumnChunk {
            file_path: self.file_path().cloned(),
            file_offset: self.file_offset,
            meta_data: Some(column_metadata),
            offset_index_offset: None,
            offset_index_length: None,
            column_index_offset: None,
            column_index_length: None,
        }
    }
}
