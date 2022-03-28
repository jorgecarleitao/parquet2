use parquet_format_async_temp::RowGroup;

use super::{column_chunk_metadata::ColumnChunkMetaData, schema_descriptor::SchemaDescriptor};
use crate::{error::Result, write::ColumnOffsetsMetadata};

/// Metadata for a row group.
#[derive(Debug, Clone)]
pub struct RowGroupMetaData {
    columns: Vec<ColumnChunkMetaData>,
    num_rows: i64,
    total_byte_size: i64,
}

impl RowGroupMetaData {
    /// Create a new [`RowGroupMetaData`]
    pub fn new(
        columns: Vec<ColumnChunkMetaData>,
        num_rows: i64,
        total_byte_size: i64,
    ) -> RowGroupMetaData {
        Self {
            columns,
            num_rows,
            total_byte_size,
        }
    }

    /// Number of columns in this row group.
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    /// Returns column chunk metadata for `i`th column.
    pub fn column(&self, i: usize) -> &ColumnChunkMetaData {
        &self.columns[i]
    }

    /// Returns slice of column chunk metadata.
    pub fn columns(&self) -> &[ColumnChunkMetaData] {
        &self.columns
    }

    /// Number of rows in this row group.
    pub fn num_rows(&self) -> i64 {
        self.num_rows
    }

    /// Total byte size of all uncompressed column data in this row group.
    pub fn total_byte_size(&self) -> i64 {
        self.total_byte_size
    }

    /// Total size of all compressed column data in this row group.
    pub fn compressed_size(&self) -> i64 {
        self.columns.iter().map(|c| c.compressed_size()).sum()
    }

    /// Method to convert from Thrift.
    pub(crate) fn try_from_thrift(
        schema_descr: &SchemaDescriptor,
        rg: RowGroup,
    ) -> Result<RowGroupMetaData> {
        assert_eq!(schema_descr.columns().len(), rg.columns.len());
        let total_byte_size = rg.total_byte_size;
        let num_rows = rg.num_rows;
        let mut columns = vec![];
        for (cc, d) in rg.columns.into_iter().zip(schema_descr.columns()) {
            let cc = ColumnChunkMetaData::try_from_thrift(d.clone(), cc)?;
            columns.push(cc);
        }
        Ok(RowGroupMetaData {
            columns,
            num_rows,
            total_byte_size,
        })
    }

    /// Method to convert to Thrift.
    pub(crate) fn into_thrift(self) -> RowGroup {
        let file_offset = self
            .columns
            .iter()
            .map(|c| {
                ColumnOffsetsMetadata::from_column_chunk_metadata(c).calc_row_group_file_offset()
            })
            .next()
            .unwrap_or(None);
        let total_compressed_size = Some(self.compressed_size());
        RowGroup {
            columns: self.columns.into_iter().map(|v| v.into_thrift()).collect(),
            total_byte_size: self.total_byte_size,
            num_rows: self.num_rows,
            sorting_columns: None,
            file_offset,
            total_compressed_size,
            ordinal: None,
        }
    }
}
