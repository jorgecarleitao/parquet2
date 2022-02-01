#![forbid(unsafe_code)]

pub mod read;
pub mod write;

// The dynamic representation of values in native Rust. This is not exaustive.
// todo: maybe refactor this into serde/json?
#[derive(Debug, PartialEq)]
pub enum Array {
    UInt32(Vec<Option<u32>>),
    Int32(Vec<Option<i32>>),
    Int64(Vec<Option<i64>>),
    Int96(Vec<Option<[u32; 3]>>),
    Float32(Vec<Option<f32>>),
    Float64(Vec<Option<f64>>),
    Boolean(Vec<Option<bool>>),
    Binary(Vec<Option<Vec<u8>>>),
    List(Vec<Option<Array>>),
    Struct(Vec<Array>, Vec<bool>),
}

impl Array {
    pub fn len(&self) -> usize {
        match self {
            Array::UInt32(a) => a.len(),
            Array::Int32(a) => a.len(),
            Array::Int64(a) => a.len(),
            Array::Int96(a) => a.len(),
            Array::Float32(a) => a.len(),
            Array::Float64(a) => a.len(),
            Array::Boolean(a) => a.len(),
            Array::Binary(a) => a.len(),
            Array::List(a) => a.len(),
            Array::Struct(a, _) => a[0].len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// The dynamic representation of values in native Rust. This is not exaustive.
// todo: maybe refactor this into serde/json?
#[derive(Debug, PartialEq)]
pub enum Value {
    UInt32(Option<u32>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    Int96(Option<[u32; 3]>),
    Float32(Option<f32>),
    Float64(Option<f64>),
    Boolean(Option<bool>),
    Binary(Option<Vec<u8>>),
    List(Option<Array>),
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use crate::{Array, Value};
    use parquet::metadata::ColumnDescriptor;
    use parquet::schema::types::ParquetType;
    use parquet::schema::types::PhysicalType;
    use parquet::statistics::*;

    pub fn get_path() -> PathBuf {
        let dir = env!("CARGO_MANIFEST_DIR");
        PathBuf::from(dir).join("../testing/parquet-testing/data")
    }

    pub fn alltypes_plain(column: usize) -> Array {
        match column {
            0 => {
                // int32
                let expected = vec![4, 5, 6, 7, 2, 3, 0, 1];
                let expected = expected.into_iter().map(Some).collect::<Vec<_>>();
                Array::Int32(expected)
            }
            1 => {
                // bool
                let expected = vec![true, false, true, false, true, false, true, false];
                let expected = expected.into_iter().map(Some).collect::<Vec<_>>();
                Array::Boolean(expected)
            }
            2 => {
                // tiny_int
                let expected = vec![0, 1, 0, 1, 0, 1, 0, 1];
                let expected = expected.into_iter().map(Some).collect::<Vec<_>>();
                Array::Int32(expected)
            }
            3 => {
                // smallint_col
                let expected = vec![0, 1, 0, 1, 0, 1, 0, 1];
                let expected = expected.into_iter().map(Some).collect::<Vec<_>>();
                Array::Int32(expected)
            }
            4 => {
                // int_col
                let expected = vec![0, 1, 0, 1, 0, 1, 0, 1];
                let expected = expected.into_iter().map(Some).collect::<Vec<_>>();
                Array::Int32(expected)
            }
            5 => {
                // bigint_col
                let expected = vec![0, 10, 0, 10, 0, 10, 0, 10];
                let expected = expected.into_iter().map(Some).collect::<Vec<_>>();
                Array::Int64(expected)
            }
            6 => {
                // float32_col
                let expected = vec![0.0, 1.1, 0.0, 1.1, 0.0, 1.1, 0.0, 1.1];
                let expected = expected.into_iter().map(Some).collect::<Vec<_>>();
                Array::Float32(expected)
            }
            7 => {
                // float64_col
                let expected = vec![0.0, 10.1, 0.0, 10.1, 0.0, 10.1, 0.0, 10.1];
                let expected = expected.into_iter().map(Some).collect::<Vec<_>>();
                Array::Float64(expected)
            }
            8 => {
                // date_string_col
                let expected = vec![
                    vec![48, 51, 47, 48, 49, 47, 48, 57],
                    vec![48, 51, 47, 48, 49, 47, 48, 57],
                    vec![48, 52, 47, 48, 49, 47, 48, 57],
                    vec![48, 52, 47, 48, 49, 47, 48, 57],
                    vec![48, 50, 47, 48, 49, 47, 48, 57],
                    vec![48, 50, 47, 48, 49, 47, 48, 57],
                    vec![48, 49, 47, 48, 49, 47, 48, 57],
                    vec![48, 49, 47, 48, 49, 47, 48, 57],
                ];
                let expected = expected.into_iter().map(Some).collect::<Vec<_>>();
                Array::Binary(expected)
            }
            9 => {
                // string_col
                let expected = vec![
                    vec![48],
                    vec![49],
                    vec![48],
                    vec![49],
                    vec![48],
                    vec![49],
                    vec![48],
                    vec![49],
                ];
                let expected = expected.into_iter().map(Some).collect::<Vec<_>>();
                Array::Binary(expected)
            }
            10 => {
                // timestamp_col
                todo!()
            }
            _ => unreachable!(),
        }
    }

    pub fn alltypes_statistics(column: usize) -> Arc<dyn Statistics> {
        let descriptor_i32 = ColumnDescriptor::new(
            ParquetType::from_physical("col".to_string(), PhysicalType::Int32),
            1,
            0,
            vec!["col".to_string()],
            ParquetType::from_physical("col".to_string(), PhysicalType::Int32),
        );
        let descriptor_i64 = ColumnDescriptor::new(
            ParquetType::from_physical("col".to_string(), PhysicalType::Int64),
            1,
            0,
            vec!["col".to_string()],
            ParquetType::from_physical("col".to_string(), PhysicalType::Int64),
        );
        let descriptor_f32 = ColumnDescriptor::new(
            ParquetType::from_physical("col".to_string(), PhysicalType::Float),
            1,
            0,
            vec!["col".to_string()],
            ParquetType::from_physical("col".to_string(), PhysicalType::Float),
        );
        let descriptor_f64 = ColumnDescriptor::new(
            ParquetType::from_physical("col".to_string(), PhysicalType::Double),
            1,
            0,
            vec!["col".to_string()],
            ParquetType::from_physical("col".to_string(), PhysicalType::Double),
        );
        let descriptor_byte = ColumnDescriptor::new(
            ParquetType::from_physical("col".to_string(), PhysicalType::ByteArray),
            1,
            0,
            vec!["col".to_string()],
            ParquetType::from_physical("col".to_string(), PhysicalType::ByteArray),
        );

        match column {
            0 => Arc::new(PrimitiveStatistics::<i32> {
                descriptor: descriptor_i32,
                null_count: Some(0),
                distinct_count: None,
                min_value: Some(0),
                max_value: Some(7),
            }),
            1 => Arc::new(BooleanStatistics {
                null_count: Some(0),
                distinct_count: None,
                min_value: Some(false),
                max_value: Some(true),
            }),
            2 | 3 | 4 => Arc::new(PrimitiveStatistics::<i32> {
                descriptor: descriptor_i32,
                null_count: Some(0),
                distinct_count: None,
                min_value: Some(0),
                max_value: Some(1),
            }),
            5 => Arc::new(PrimitiveStatistics::<i64> {
                descriptor: descriptor_i64,
                null_count: Some(0),
                distinct_count: None,
                min_value: Some(0),
                max_value: Some(10),
            }),
            6 => Arc::new(PrimitiveStatistics::<f32> {
                descriptor: descriptor_f32,
                null_count: Some(0),
                distinct_count: None,
                min_value: Some(0.0),
                max_value: Some(1.1),
            }),
            7 => Arc::new(PrimitiveStatistics::<f64> {
                descriptor: descriptor_f64,
                null_count: Some(0),
                distinct_count: None,
                min_value: Some(0.0),
                max_value: Some(10.1),
            }),
            8 => Arc::new(BinaryStatistics {
                descriptor: descriptor_byte,
                null_count: Some(0),
                distinct_count: None,
                min_value: Some(vec![48, 49, 47, 48, 49, 47, 48, 57]),
                max_value: Some(vec![48, 52, 47, 48, 49, 47, 48, 57]),
            }),
            9 => Arc::new(BinaryStatistics {
                descriptor: descriptor_byte,
                null_count: Some(0),
                distinct_count: None,
                min_value: Some(vec![48]),
                max_value: Some(vec![49]),
            }),
            10 => {
                // timestamp_col
                todo!()
            }
            _ => unreachable!(),
        }
    }

    // these values match the values in `integration`
    pub fn pyarrow_optional(column: usize) -> Array {
        let i64_values = &[
            Some(0),
            Some(1),
            None,
            Some(3),
            None,
            Some(5),
            Some(6),
            Some(7),
            None,
            Some(9),
        ];
        let f64_values = &[
            Some(0.0),
            Some(1.0),
            None,
            Some(3.0),
            None,
            Some(5.0),
            Some(6.0),
            Some(7.0),
            None,
            Some(9.0),
        ];
        let string_values = &[
            Some(b"Hello".to_vec()),
            None,
            Some(b"aa".to_vec()),
            Some(b"".to_vec()),
            None,
            Some(b"abc".to_vec()),
            None,
            None,
            Some(b"def".to_vec()),
            Some(b"aaa".to_vec()),
        ];
        let bool_values = &[
            Some(true),
            None,
            Some(false),
            Some(false),
            None,
            Some(true),
            None,
            None,
            Some(true),
            Some(true),
        ];

        match column {
            0 => Array::Int64(i64_values.to_vec()),
            1 => Array::Float64(f64_values.to_vec()),
            2 => Array::Binary(string_values.to_vec()),
            3 => Array::Boolean(bool_values.to_vec()),
            4 => Array::Int64(i64_values.to_vec()),
            _ => unreachable!(),
        }
    }

    pub fn pyarrow_optional_stats(column: usize) -> (Option<i64>, Value, Value) {
        match column {
            0 => (Some(3), Value::Int64(Some(0)), Value::Int64(Some(9))),
            1 => (
                Some(3),
                Value::Float64(Some(0.0)),
                Value::Float64(Some(9.0)),
            ),
            2 => (
                Some(4),
                Value::Binary(Some(b"".to_vec())),
                Value::Binary(Some(b"def".to_vec())),
            ),
            3 => (
                Some(4),
                Value::Boolean(Some(false)),
                Value::Boolean(Some(true)),
            ),
            4 => (Some(3), Value::Int64(Some(0)), Value::Int64(Some(9))),
            _ => unreachable!(),
        }
    }

    // these values match the values in `integration`
    pub fn pyarrow_required(column: usize) -> Array {
        let i64_values = &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let f64_values = &[0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0];
        let string_values = &[
            "Hello", "bbb", "aa", "", "bbb", "abc", "bbb", "bbb", "def", "aaa",
        ];
        let bool_values = &[
            true, true, false, false, false, true, true, true, true, true,
        ];

        match column {
            0 => Array::Int64(i64_values.iter().map(|i| Some(*i as i64)).collect()),
            1 => Array::Float64(f64_values.iter().map(|f| Some(*f)).collect()),
            2 => Array::Binary(
                string_values
                    .iter()
                    .map(|s| Some(s.as_bytes().to_vec()))
                    .collect(),
            ),
            3 => Array::Boolean(bool_values.iter().map(|b| Some(*b)).collect()),
            4 => Array::Int64(i64_values.iter().map(|i| Some(*i as i64)).collect()),
            5 => Array::Int32(i64_values.iter().map(|i| Some(*i as i32)).collect()),
            6 => Array::Binary(
                string_values
                    .iter()
                    .map(|s| Some(s.as_bytes().to_vec()))
                    .collect(),
            ),
            _ => unreachable!(),
        }
    }

    pub fn pyarrow_required_stats(column: usize) -> (Option<i64>, Value, Value) {
        match column {
            0 => (Some(0), Value::Int64(Some(0)), Value::Int64(Some(9))),
            1 => (
                Some(3),
                Value::Float64(Some(0.0)),
                Value::Float64(Some(9.0)),
            ),
            2 => (
                Some(4),
                Value::Binary(Some(b"".to_vec())),
                Value::Binary(Some(b"def".to_vec())),
            ),
            3 => (
                Some(4),
                Value::Boolean(Some(false)),
                Value::Boolean(Some(true)),
            ),
            4 => (Some(3), Value::Int64(Some(0)), Value::Int64(Some(9))),
            5 => (Some(0), Value::Int32(Some(0)), Value::Int32(Some(9))),
            6 => (
                Some(4),
                Value::Binary(Some(b"".to_vec())),
                Value::Binary(Some(b"def".to_vec())),
            ),
            _ => unreachable!(),
        }
    }

    // these values match the values in `integration`
    pub fn pyarrow_nested_optional(column: usize) -> Array {
        //    [[0, 1], None, [2, None, 3], [4, 5, 6], [], [7, 8, 9], None, [10]]
        // def: 3, 3,  0,     3, 2,    3,   3, 3, 3,  1    3  3  3   0      3
        // rep: 0, 1,  0,     0, 1,    1,   0, 1, 1,  0,   0, 1, 1,  0,     0
        let data = vec![
            Some(Array::Int64(vec![Some(0), Some(1)])),
            None,
            Some(Array::Int64(vec![Some(2), None, Some(3)])),
            Some(Array::Int64(vec![Some(4), Some(5), Some(6)])),
            Some(Array::Int64(vec![])),
            Some(Array::Int64(vec![Some(7), Some(8), Some(9)])),
            None,
            Some(Array::Int64(vec![Some(10)])),
        ];

        match column {
            0 => Array::List(data),
            _ => unreachable!(),
        }
    }

    // these values match the values in `integration`
    pub fn pyarrow_struct_optional(column: usize) -> Array {
        let validity = vec![false, true, true, true, true, true, true, true, true, true];

        let string = vec![
            Some("Hello".to_string()),
            None,
            Some("aa".to_string()),
            Some("".to_string()),
            None,
            Some("abc".to_string()),
            None,
            None,
            Some("def".to_string()),
            Some("aaa".to_string()),
        ]
        .into_iter()
        .map(|s| s.map(|s| s.as_bytes().to_vec()))
        .collect::<Vec<_>>();
        let boolean = vec![
            Some(true),
            None,
            Some(false),
            Some(false),
            None,
            Some(true),
            None,
            None,
            Some(true),
            Some(true),
        ];

        match column {
            0 => {
                let string = string
                    .iter()
                    .zip(validity.iter())
                    .map(|(item, valid)| if *valid { item.clone() } else { None })
                    .collect();
                let boolean = boolean
                    .iter()
                    .zip(validity.iter())
                    .map(|(item, valid)| if *valid { *item } else { None })
                    .collect();
                Array::Struct(
                    vec![Array::Binary(string), Array::Boolean(boolean)],
                    validity,
                )
            }
            1 => Array::Struct(
                vec![Array::Binary(string), Array::Boolean(boolean)],
                vec![true; validity.len()],
            ),
            _ => unreachable!(),
        }
    }
}
