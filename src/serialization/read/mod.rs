/// Serialization to Rust's Native types.
/// In comparison to Arrow, this in-memory format does not leverage logical types nor SIMD operations,
/// but OTOH it has no external dependencies and is very familiar to Rust developers.
mod binary;
mod boolean;
mod primitive;
mod primitive_nested;
mod utils;

pub mod levels;

use crate::error::{ParquetError, Result};
use crate::schema::types::ParquetType;
use crate::schema::types::PhysicalType;
use crate::{
    metadata::ColumnDescriptor,
    read::{decompress_page, CompressedPage},
};

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

/// Reads a page into an [`Array`].
/// This is CPU-intensive: decompress, decode and de-serialize.
pub fn page_to_array(page: CompressedPage, descriptor: &ColumnDescriptor) -> Result<Array> {
    let page = decompress_page(page)?;

    match (descriptor.type_(), descriptor.max_rep_level()) {
        (ParquetType::PrimitiveType { physical_type, .. }, 0) => match page.dictionary_page() {
            Some(_) => match physical_type {
                PhysicalType::Int32 => Ok(Array::Int32(primitive::page_dict_to_vec(
                    &page, descriptor,
                )?)),
                PhysicalType::Int64 => Ok(Array::Int64(primitive::page_dict_to_vec(
                    &page, descriptor,
                )?)),
                PhysicalType::Int96 => Ok(Array::Int96(primitive::page_dict_to_vec(
                    &page, descriptor,
                )?)),
                PhysicalType::Float => Ok(Array::Float32(primitive::page_dict_to_vec(
                    &page, descriptor,
                )?)),
                PhysicalType::Double => Ok(Array::Float64(primitive::page_dict_to_vec(
                    &page, descriptor,
                )?)),
                PhysicalType::ByteArray => {
                    Ok(Array::Binary(binary::page_dict_to_vec(&page, descriptor)?))
                }
                _ => todo!(),
            },
            None => match physical_type {
                PhysicalType::Boolean => {
                    Ok(Array::Boolean(boolean::page_to_vec(&page, descriptor)?))
                }
                PhysicalType::Int32 => Ok(Array::Int32(primitive::page_to_vec(&page, descriptor)?)),
                PhysicalType::Int64 => Ok(Array::Int64(primitive::page_to_vec(&page, descriptor)?)),
                PhysicalType::Int96 => Ok(Array::Int96(primitive::page_to_vec(&page, descriptor)?)),
                PhysicalType::Float => {
                    Ok(Array::Float32(primitive::page_to_vec(&page, descriptor)?))
                }
                PhysicalType::Double => {
                    Ok(Array::Float64(primitive::page_to_vec(&page, descriptor)?))
                }
                PhysicalType::ByteArray => {
                    Ok(Array::Binary(binary::page_to_vec(&page, descriptor)?))
                }
                _ => todo!(),
            },
        },
        (ParquetType::PrimitiveType { physical_type, .. }, _) => match page.dictionary_page() {
            None => match physical_type {
                PhysicalType::Int64 => {
                    Ok(primitive_nested::page_to_array::<i64>(&page, descriptor)?)
                }
                _ => todo!(),
            },
            Some(_) => match physical_type {
                PhysicalType::Int64 => Ok(primitive_nested::page_dict_to_array::<i64>(
                    &page, descriptor,
                )?),
                _ => todo!(),
            },
        },
        _ => Err(general_err!(
            "Nested types are not supported by this in-memory format"
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use crate::read::{
        get_page_iterator, read_metadata, BinaryStatistics, PrimitiveStatistics, Statistics,
    };
    use crate::tests::*;
    use crate::types::int96_to_i64_ns;

    use super::*;
    use crate::{error::Result, metadata::ColumnDescriptor, read::CompressedPage};

    fn prepare(
        path: &str,
        row_group: usize,
        column: usize,
    ) -> Result<(ColumnDescriptor, Vec<CompressedPage>)> {
        let mut file = File::open(path).unwrap();

        let metadata = read_metadata(&mut file)?;
        let descriptor = metadata.row_groups[row_group]
            .column(column)
            .descriptor()
            .clone();
        Ok((
            descriptor,
            get_page_iterator(&metadata, row_group, column, &mut file)?
                .collect::<Result<Vec<_>>>()?,
        ))
    }

    fn get_column(
        path: &str,
        column: usize,
    ) -> Result<(Array, Option<std::sync::Arc<dyn Statistics>>)> {
        let (descriptor, mut pages) = prepare(path, 0, column)?;
        assert_eq!(pages.len(), 1);

        let page = pages.pop().unwrap();

        let statistics = page.statistics().cloned();

        Ok((page_to_array(page, &descriptor)?, statistics))
    }

    fn test_column(column: usize) -> Result<()> {
        let mut path = get_path();
        path.push("alltypes_plain.parquet");
        let path = path.to_str().unwrap();
        let (result, _) = get_column(path, column)?;
        assert_eq!(result, alltypes_plain(column));
        Ok(())
    }

    #[test]
    fn int32() -> Result<()> {
        test_column(0)
    }

    #[test]
    fn bool() -> Result<()> {
        test_column(1)
    }

    #[test]
    fn tiny_int() -> Result<()> {
        test_column(2)
    }

    #[test]
    fn smallint_col() -> Result<()> {
        test_column(3)
    }

    #[test]
    fn int_col() -> Result<()> {
        test_column(4)
    }

    #[test]
    fn bigint_col() -> Result<()> {
        test_column(5)
    }

    #[test]
    fn float32_col() -> Result<()> {
        test_column(6)
    }

    #[test]
    fn float64_col() -> Result<()> {
        test_column(7)
    }

    #[test]
    fn date_string_col() -> Result<()> {
        test_column(8)
    }

    #[test]
    fn string_col() -> Result<()> {
        test_column(9)
    }

    #[test]
    fn timestamp_col() -> Result<()> {
        let mut path = get_path();
        path.push("alltypes_plain.parquet");
        let path = path.to_str().unwrap();

        let expected = vec![
            1235865600000000000i64,
            1235865660000000000,
            1238544000000000000,
            1238544060000000000,
            1233446400000000000,
            1233446460000000000,
            1230768000000000000,
            1230768060000000000,
        ];

        let expected = expected.into_iter().map(Some).collect::<Vec<_>>();
        let (array, _) = get_column(&path, 10)?;
        if let Array::Int96(array) = array {
            let a = array
                .into_iter()
                .map(|x| x.map(int96_to_i64_ns))
                .collect::<Vec<_>>();
            assert_eq!(expected, a);
        } else {
            panic!("Timestamp expected");
        };
        Ok(())
    }

    fn assert_eq_stats(expected: (Option<i64>, Value, Value), stats: &dyn Statistics) {
        match (expected.1, expected.2) {
            (Value::Int32(min), Value::Int32(max)) => {
                let s = stats
                    .as_any()
                    .downcast_ref::<PrimitiveStatistics<i32>>()
                    .unwrap();
                assert_eq!(expected.0, s.null_count);
                assert_eq!(s.min_value, min);
                assert_eq!(s.max_value, max);
            }
            (Value::Int64(min), Value::Int64(max)) => {
                let s = stats
                    .as_any()
                    .downcast_ref::<PrimitiveStatistics<i64>>()
                    .unwrap();
                assert_eq!(expected.0, s.null_count);
                assert_eq!(s.min_value, min);
                assert_eq!(s.max_value, max);
            }
            (Value::Float64(min), Value::Float64(max)) => {
                let s = stats
                    .as_any()
                    .downcast_ref::<PrimitiveStatistics<f64>>()
                    .unwrap();
                assert_eq!(expected.0, s.null_count);
                assert_eq!(s.min_value, min);
                assert_eq!(s.max_value, max);
            }
            (Value::Binary(min), Value::Binary(max)) => {
                let s = stats.as_any().downcast_ref::<BinaryStatistics>().unwrap();

                assert_eq!(s.min_value, min);
                assert_eq!(s.max_value, max);
            }

            _ => todo!(),
        }
    }

    fn test_pyarrow_integration(
        file: &str,
        column: usize,
        version: usize,
        required: bool,
    ) -> Result<()> {
        if std::env::var("PARQUET2_IGNORE_PYARROW_TESTS").is_ok() {
            return Ok(());
        }
        let path = if required {
            format!(
                "fixtures/pyarrow3/v{}/{}_{}_10.parquet",
                version, file, "required"
            )
        } else {
            format!(
                "fixtures/pyarrow3/v{}/{}_{}_10.parquet",
                version, file, "nullable"
            )
        };
        let (array, statistics) = get_column(&path, column)?;

        let expected = if file == "basic" {
            if required {
                pyarrow_required(column)
            } else {
                pyarrow_optional(column)
            }
        } else {
            pyarrow_nested_optional(column)
        };

        assert_eq!(expected, array);

        let expected_stats = if file == "basic" {
            if required {
                pyarrow_required_stats(column)
            } else {
                pyarrow_optional_stats(column)
            }
        } else {
            (Some(1), Value::Int64(Some(0)), Value::Int64(Some(10)))
        };

        assert_eq_stats(expected_stats, statistics.unwrap().as_ref());

        Ok(())
    }

    #[test]
    fn pyarrow_v1_int32_required() -> Result<()> {
        test_pyarrow_integration("basic", 0, 1, true)
    }

    #[test]
    fn pyarrow_v1_int32_optional() -> Result<()> {
        test_pyarrow_integration("basic", 0, 1, false)
    }

    #[test]
    fn pyarrow_v1_dict_string_required() -> Result<()> {
        test_pyarrow_integration("basic", 2, 1, true)
    }

    #[test]
    #[ignore = "optional strings are not yet supported, see https://github.com/jorgecarleitao/parquet2/pull/7"]
    fn pyarrow_v1_dict_string_optional() -> Result<()> {
        test_pyarrow_integration("basic", 2, 1, false)
    }

    #[test]
    fn pyarrow_v1_non_dict_string_required() -> Result<()> {
        test_pyarrow_integration("basic", 6, 1, true)
    }

    #[test]
    #[ignore = "optional non dictionary encoded strings seem to be written incorrectly by pyarrow, neither pyarrow itself nor rust parquet1 can read the generated file"]
    fn pyarrow_v1_non_dict_string_optional() -> Result<()> {
        test_pyarrow_integration("basic", 6, 1, false)
    }

    #[test]
    fn pyarrow_v1_list_nullable() -> Result<()> {
        test_pyarrow_integration("nested", 0, 1, false)
    }
}
