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

/// Represents a path in a nested schema
#[derive(Clone, PartialEq, Debug, Eq, Hash)]
pub struct ColumnPath {
    parts: Vec<String>,
}

impl ColumnPath {
    /// Creates new column path from vector of field names.
    pub fn new(parts: Vec<String>) -> Self {
        ColumnPath { parts }
    }

    pub fn string(&self) -> String {
        self.parts.join(".")
    }

    pub fn append(&mut self, mut tail: Vec<String>) {
        self.parts.append(&mut tail);
    }

    pub fn parts(&self) -> &[String] {
        &self.parts
    }
}

impl std::fmt::Display for ColumnPath {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self.string())
    }
}

impl From<Vec<String>> for ColumnPath {
    fn from(parts: Vec<String>) -> Self {
        ColumnPath { parts }
    }
}

impl<'a> From<&'a str> for ColumnPath {
    fn from(single_path: &str) -> Self {
        let s = String::from(single_path);
        ColumnPath::from(s)
    }
}

impl From<String> for ColumnPath {
    fn from(single_path: String) -> Self {
        let v = vec![single_path];
        ColumnPath { parts: v }
    }
}

impl AsRef<[String]> for ColumnPath {
    fn as_ref(&self) -> &[String] {
        &self.parts
    }
}
