// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Contains information about available Parquet metadata.
//!
//! The hierarchy of metadata is as follows:
//!
//! [`ParquetMetaData`](struct.ParquetMetaData.html) contains
//! [`FileMetaData`](struct.FileMetaData.html) and zero or more
//! [`RowGroupMetaData`](struct.RowGroupMetaData.html) for each row group.
//!
//! [`FileMetaData`](struct.FileMetaData.html) includes file version, application specific
//! metadata.
//!
//! Each [`RowGroupMetaData`](struct.RowGroupMetaData.html) contains information about row
//! group and one or more [`ColumnChunkMetaData`](struct.ColumnChunkMetaData.html) for
//! each column chunk.
//!
//! [`ColumnChunkMetaData`](struct.ColumnChunkMetaData.html) has information about column
//! chunk (primitive leaf column), including encoding/compression, number of values, etc.

use super::file_metadata::FileMetaData;
use super::row_metadata::RowGroupMetaData;

/// Global Parquet metadata.
#[derive(Debug, Clone)]
pub struct ParquetMetaData {
    file_metadata: FileMetaData,
    row_groups: Vec<RowGroupMetaData>,
}

impl ParquetMetaData {
    /// Creates Parquet metadata from file metadata and a list of row group metadata `Arc`s
    /// for each available row group.
    pub fn new(file_metadata: FileMetaData, row_groups: Vec<RowGroupMetaData>) -> Self {
        Self {
            file_metadata,
            row_groups,
        }
    }

    /// Returns file metadata as reference.
    pub fn file_metadata(&self) -> &FileMetaData {
        &self.file_metadata
    }

    /// Returns number of row groups in this file.
    pub fn num_row_groups(&self) -> usize {
        self.row_groups.len()
    }

    /// Returns row group metadata for `i`th position.
    /// Position should be less than number of row groups `num_row_groups`.
    pub fn row_group(&self, i: usize) -> &RowGroupMetaData {
        &self.row_groups[i]
    }

    /// Returns slice of row groups in this file.
    pub fn row_groups(&self) -> &[RowGroupMetaData] {
        &self.row_groups
    }
}
