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

    use crate::serialization::read::Array;

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
}
