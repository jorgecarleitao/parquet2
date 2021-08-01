use super::{column_order::ColumnOrder, schema_descriptor::SchemaDescriptor, RowGroupMetaData};
use crate::error::Result;

pub type KeyValue = parquet_format::KeyValue;

/// Metadata for a Parquet file.
// This is almost equal to `parquet_format::FileMetaData` but contains the descriptors,
// which are crucial to deserialize pages.
#[derive(Debug, Clone)]
pub struct FileMetaData {
    /// version of this file.
    pub version: i32,
    /// number of rows in the file.
    pub num_rows: i64,
    /// String message for application that wrote this file.
    ///
    /// This should have the following format:
    /// `<application> version <application version> (build <application build hash>)`.
    ///
    /// ```shell
    /// parquet-mr version 1.8.0 (build 0fda28af84b9746396014ad6a415b90592a98b3b)
    /// ```
    pub created_by: Option<String>,
    /// The row groups of this file
    pub row_groups: Vec<RowGroupMetaData>,
    /// key_value_metadata of this file.
    pub key_value_metadata: Option<Vec<KeyValue>>,
    /// schema descriptor.
    pub schema_descr: SchemaDescriptor,
    /// Column (sort) order used for `min` and `max` values of each column in this file.
    ///
    /// Each column order corresponds to one column, determined by its position in the
    /// list, matching the position of the column in the schema.
    ///
    /// When `None` is returned, there are no column orders available, and each column
    /// should be assumed to have undefined (legacy) column order.
    pub column_orders: Option<Vec<ColumnOrder>>,
}

impl FileMetaData {
    /// Creates new file metadata.
    pub fn new(
        version: i32,
        num_rows: i64,
        created_by: Option<String>,
        row_groups: Vec<RowGroupMetaData>,
        key_value_metadata: Option<Vec<KeyValue>>,
        schema_descr: SchemaDescriptor,
        column_orders: Option<Vec<ColumnOrder>>,
    ) -> Self {
        FileMetaData {
            version,
            num_rows,
            created_by,
            row_groups,
            key_value_metadata,
            schema_descr,
            column_orders,
        }
    }

    /// Returns the ['SchemaDescriptor`] that describes schema of this file.
    pub fn schema(&self) -> &SchemaDescriptor {
        &self.schema_descr
    }

    /// returns the metadata
    pub fn key_value_metadata(&self) -> &Option<Vec<KeyValue>> {
        &self.key_value_metadata
    }

    /// Returns column order for `i`th column in this file.
    /// If column orders are not available, returns undefined (legacy) column order.
    pub fn column_order(&self, i: usize) -> ColumnOrder {
        self.column_orders
            .as_ref()
            .map(|data| data[i])
            .unwrap_or(ColumnOrder::Undefined)
    }

    pub fn into_thrift(self) -> Result<parquet_format::FileMetaData> {
        Ok(parquet_format::FileMetaData {
            version: self.version,
            schema: self.schema_descr.into_thrift()?,
            num_rows: self.num_rows as i64,
            row_groups: self
                .row_groups
                .into_iter()
                .map(|v| v.into_thrift())
                .collect(),
            key_value_metadata: self.key_value_metadata,
            created_by: self.created_by,
            column_orders: None, // todo
            encryption_algorithm: None,
            footer_signing_key_metadata: None,
        })
    }
}
