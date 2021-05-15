#[macro_use]
pub mod error;
pub mod compression;
pub mod encoding;
pub mod metadata;
pub mod read;
pub mod schema;
pub mod serialization;
pub mod types;
pub mod write;

const FOOTER_SIZE: u64 = 8;
const PARQUET_MAGIC: [u8; 4] = [b'P', b'A', b'R', b'1'];

/// The number of bytes read at the end of the parquet file on first read
const DEFAULT_FOOTER_READ_SIZE: u64 = 64 * 1024;

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::serialization::read::{Array, Value};

    pub fn get_path() -> PathBuf {
        let dir = env!("CARGO_MANIFEST_DIR");

        PathBuf::from(dir).join("testing/parquet-testing/data")
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
            6 => Array::Binary(string_values.to_vec()),
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
                Value::Binary(Some(b"Hello".to_vec())),
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
}
