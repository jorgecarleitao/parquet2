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

//! Common Parquet errors and macros.

#[derive(Debug, PartialEq)]
pub enum ParquetError {
    /// General Parquet error.
    /// Returned when code violates normal workflow of working with Parquet files.
    General(String),
    /// When the parquet file is known to be out of spec.
    OutOfSpec(String),
}

impl std::fmt::Display for ParquetError {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ParquetError::General(message) => {
                write!(fmt, "{}", message)
            }
            ParquetError::OutOfSpec(message) => {
                write!(fmt, "{}", message)
            }
        }
    }
}

#[cfg(feature = "snappy")]
impl From<snap::Error> for ParquetError {
    fn from(e: snap::Error) -> ParquetError {
        ParquetError::General(format!("underlying snap error: {}", e))
    }
}

impl From<thrift::Error> for ParquetError {
    fn from(e: thrift::Error) -> ParquetError {
        ParquetError::General(format!("underlying thrift error: {}", e))
    }
}

impl From<std::io::Error> for ParquetError {
    fn from(e: std::io::Error) -> ParquetError {
        ParquetError::General(format!("underlying IO error: {}", e))
    }
}

/// A specialized `Result` for Parquet errors.
pub type Result<T> = std::result::Result<T, ParquetError>;

macro_rules! general_err {
    ($fmt:expr) => (ParquetError::General($fmt.to_owned()));
    ($fmt:expr, $($args:expr),*) => (ParquetError::General(format!($fmt, $($args),*)));
    ($e:expr, $fmt:expr) => (ParquetError::General($fmt.to_owned(), $e));
    ($e:ident, $fmt:expr, $($args:tt),*) => (
        ParquetError::General(&format!($fmt, $($args),*), $e));
}
