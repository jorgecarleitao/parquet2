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

use super::{ColumnPath, ParquetType};

/// A descriptor for leaf-level primitive columns.
/// This encapsulates information such as definition and repetition levels and is used to
/// re-assemble nested data.
#[derive(Debug, PartialEq)]
pub struct ColumnDescriptor {
    // The "leaf" primitive type of this column
    primitive_type: ParquetType,

    // The maximum definition level for this column
    max_def_level: i16,

    // The maximum repetition level for this column
    max_rep_level: i16,

    // The path of this column. For instance, "a.b.c.d".
    path: ColumnPath,
}

impl ColumnDescriptor {
    /// Creates new descriptor for leaf-level column.
    pub fn new(
        primitive_type: ParquetType,
        max_def_level: i16,
        max_rep_level: i16,
        path: ColumnPath,
    ) -> Self {
        Self {
            primitive_type,
            max_def_level,
            max_rep_level,
            path,
        }
    }

    /// Returns maximum definition level for this column.
    pub fn max_def_level(&self) -> i16 {
        self.max_def_level
    }

    /// Returns maximum repetition level for this column.
    pub fn max_rep_level(&self) -> i16 {
        self.max_rep_level
    }

    /// Returns [`ColumnPath`] for this column.
    pub fn path(&self) -> &ColumnPath {
        &self.path
    }

    /// Returns self type [`Type`](crate::schema::types::Type) for this leaf column.
    pub fn self_type(&self) -> &ParquetType {
        &self.primitive_type
    }

    /// Returns column name.
    pub fn name(&self) -> &str {
        self.primitive_type.name()
    }
}
