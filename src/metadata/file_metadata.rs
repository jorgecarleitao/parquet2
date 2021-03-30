use crate::schema::types::ParquetType;

use super::{column_order::ColumnOrder, schema_descriptor::SchemaDescriptor};

pub type KeyValue = parquet_format::KeyValue;

/// Metadata for a Parquet file.
#[derive(Debug, Clone)]
pub struct FileMetaData {
    version: i32,
    num_rows: i64,
    created_by: Option<String>,
    key_value_metadata: Option<Vec<KeyValue>>,
    schema_descr: SchemaDescriptor,
    column_orders: Option<Vec<ColumnOrder>>,
}

impl FileMetaData {
    /// Creates new file metadata.
    pub fn new(
        version: i32,
        num_rows: i64,
        created_by: Option<String>,
        key_value_metadata: Option<Vec<KeyValue>>,
        schema_descr: SchemaDescriptor,
        column_orders: Option<Vec<ColumnOrder>>,
    ) -> Self {
        FileMetaData {
            version,
            num_rows,
            created_by,
            key_value_metadata,
            schema_descr,
            column_orders,
        }
    }

    /// Returns version of this file.
    pub fn version(&self) -> i32 {
        self.version
    }

    /// Returns number of rows in the file.
    pub fn num_rows(&self) -> i64 {
        self.num_rows
    }

    /// String message for application that wrote this file.
    ///
    /// This should have the following format:
    /// `<application> version <application version> (build <application build hash>)`.
    ///
    /// ```shell
    /// parquet-mr version 1.8.0 (build 0fda28af84b9746396014ad6a415b90592a98b3b)
    /// ```
    pub fn created_by(&self) -> &Option<String> {
        &self.created_by
    }

    /// Returns key_value_metadata of this file.
    pub fn key_value_metadata(&self) -> &Option<Vec<KeyValue>> {
        &self.key_value_metadata
    }

    /// Returns Parquet ['ParquetType`] that describes schema in this file.
    pub fn schema(&self) -> &ParquetType {
        self.schema_descr.root_schema()
    }

    /// Returns a reference to schema descriptor.
    pub fn schema_descr(&self) -> &SchemaDescriptor {
        &self.schema_descr
    }

    /// Column (sort) order used for `min` and `max` values of each column in this file.
    ///
    /// Each column order corresponds to one column, determined by its position in the
    /// list, matching the position of the column in the schema.
    ///
    /// When `None` is returned, there are no column orders available, and each column
    /// should be assumed to have undefined (legacy) column order.
    pub fn column_orders(&self) -> &Option<Vec<ColumnOrder>> {
        &self.column_orders
    }

    /// Returns column order for `i`th column in this file.
    /// If column orders are not available, returns undefined (legacy) column order.
    pub fn column_order(&self, i: usize) -> ColumnOrder {
        self.column_orders
            .as_ref()
            .map(|data| data[i])
            .unwrap_or(ColumnOrder::Undefined)
    }
}
