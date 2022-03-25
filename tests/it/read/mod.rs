/// Serialization to Rust's Native types.
/// In comparison to Arrow, this in-memory format does not leverage logical types nor SIMD operations,
/// but OTOH it has no external dependencies and is very familiar to Rust developers.
mod binary;
mod boolean;
mod primitive;
mod primitive_nested;
mod struct_;
mod utils;

use std::fs::File;

use parquet2::error::ParquetError;
use parquet2::error::Result;
use parquet2::metadata::ColumnChunkMetaData;
use parquet2::page::CompressedDataPage;
use parquet2::page::DataPage;
use parquet2::read::BasicDecompressor;
use parquet2::read::{get_column_iterator, get_field_columns, read_metadata};
use parquet2::read::{MutStreamingIterator, State};
use parquet2::schema::types::GroupConvertedType;
use parquet2::schema::types::ParquetType;
use parquet2::schema::types::PhysicalType;
use parquet2::statistics::{BinaryStatistics, BooleanStatistics, PrimitiveStatistics, Statistics};
use parquet2::types::int96_to_i64_ns;
use parquet2::FallibleStreamingIterator;

use super::*;

/// Reads a page into an [`Array`].
/// This is CPU-intensive: decompress, decode and de-serialize.
pub fn page_to_array(page: &DataPage) -> Result<Array> {
    let physical_type = page.descriptor.primitive_type.physical_type;
    match page.descriptor.max_rep_level {
        0 => match page.dictionary_page() {
            Some(_) => match physical_type {
                PhysicalType::Int32 => Ok(Array::Int32(primitive::page_dict_to_vec(page)?)),
                PhysicalType::Int64 => Ok(Array::Int64(primitive::page_dict_to_vec(page)?)),
                PhysicalType::Int96 => Ok(Array::Int96(primitive::page_dict_to_vec(page)?)),
                PhysicalType::Float => Ok(Array::Float32(primitive::page_dict_to_vec(page)?)),
                PhysicalType::Double => Ok(Array::Float64(primitive::page_dict_to_vec(page)?)),
                PhysicalType::ByteArray => Ok(Array::Binary(binary::page_dict_to_vec(page)?)),
                _ => todo!(),
            },
            None => match physical_type {
                PhysicalType::Boolean => Ok(Array::Boolean(boolean::page_to_vec(page)?)),
                PhysicalType::Int32 => Ok(Array::Int32(primitive::page_to_vec(page)?)),
                PhysicalType::Int64 => Ok(Array::Int64(primitive::page_to_vec(page)?)),
                PhysicalType::Int96 => Ok(Array::Int96(primitive::page_to_vec(page)?)),
                PhysicalType::Float => Ok(Array::Float32(primitive::page_to_vec(page)?)),
                PhysicalType::Double => Ok(Array::Float64(primitive::page_to_vec(page)?)),
                PhysicalType::ByteArray => Ok(Array::Binary(binary::page_to_vec(page)?)),
                _ => todo!(),
            },
        },
        _ => match page.dictionary_page() {
            None => match physical_type {
                PhysicalType::Int64 => Ok(primitive_nested::page_to_array::<i64>(page)?),
                _ => todo!(),
            },
            Some(_) => match physical_type {
                PhysicalType::Int64 => Ok(primitive_nested::page_dict_to_array::<i64>(page)?),
                _ => todo!(),
            },
        },
    }
}

/// Reads columns into an [`Array`].
/// This is CPU-intensive: decompress, decode and de-serialize.
pub fn columns_to_array<II, I>(mut columns: I, field: &ParquetType) -> Result<Array>
where
    II: Iterator<Item = Result<CompressedDataPage>>,
    I: MutStreamingIterator<Item = (II, ColumnChunkMetaData), Error = ParquetError>,
{
    let mut validity = vec![];
    let mut has_filled = false;
    let mut arrays = vec![];
    while let State::Some(mut new_iter) = columns.advance()? {
        if let Some((pages, _column)) = new_iter.get() {
            let mut iterator = BasicDecompressor::new(pages, vec![]);
            while let Some(page) = iterator.next()? {
                if !has_filled {
                    struct_::extend_validity(&mut validity, page);
                }
                // todo: this is wrong: multiple pages -> array
                arrays.push(page_to_array(page)?)
            }
        }
        has_filled = true;
        columns = new_iter;
    }

    match field {
        ParquetType::PrimitiveType { .. } => Ok(arrays.pop().unwrap()),
        ParquetType::GroupType { converted_type, .. } => {
            if let Some(converted_type) = converted_type {
                match converted_type {
                    GroupConvertedType::List => Ok(arrays.pop().unwrap()),
                    _ => todo!(),
                }
            } else {
                Ok(Array::Struct(arrays, validity))
            }
        }
    }
}

pub fn read_column<R: std::io::Read + std::io::Seek>(
    reader: &mut R,
    row_group: usize,
    field: usize,
) -> Result<(Array, Option<std::sync::Arc<dyn Statistics>>)> {
    let metadata = read_metadata(reader)?;

    let expected = metadata.row_groups[0].column(0).compressed_size();
    let chunk = metadata.row_groups[0].column(0).clone().into_thrift();
    assert_eq!(chunk.meta_data.unwrap().total_compressed_size, expected);

    let columns = get_column_iterator(reader, &metadata, row_group, field, None, vec![]);
    let field = &metadata.schema().fields()[field];

    let mut statistics = get_field_columns(&metadata, row_group, field)
        .map(|column_meta| column_meta.statistics().transpose())
        .collect::<Result<Vec<_>>>()?;

    let array = columns_to_array(columns, field)?;

    Ok((array, statistics.pop().unwrap()))
}

fn get_column(
    path: &str,
    column: usize,
) -> Result<(Array, Option<std::sync::Arc<dyn Statistics>>)> {
    let mut file = File::open(path).unwrap();
    read_column(&mut file, 0, column)
}

fn test_column(column: usize) -> Result<()> {
    let mut path = get_path();
    path.push("alltypes_plain.parquet");
    let path = path.to_str().unwrap();
    let (result, statistics) = get_column(path, column)?;
    // the file does not have statistics
    assert_eq!(statistics.as_ref().map(|x| x.as_ref()), None);
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
    let (array, _) = get_column(path, 10)?;
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
        (Value::Boolean(min), Value::Boolean(max)) => {
            let s = stats.as_any().downcast_ref::<BooleanStatistics>().unwrap();

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
    use_dictionary: bool,
    compression: &str,
) -> Result<()> {
    if std::env::var("PARQUET2_IGNORE_PYARROW_TESTS").is_ok() {
        return Ok(());
    }
    let required_s = if required { "required" } else { "nullable" };
    let use_dictionary_s = if use_dictionary { "dict" } else { "non_dict" };

    let path = format!(
        "fixtures/pyarrow3/v{}/{}{}/{}_{}_10.parquet",
        version, use_dictionary_s, compression, file, required_s
    );

    let (array, statistics) = get_column(&path, column)?;

    let expected = match (file, required) {
        ("basic", true) => pyarrow_required(column),
        ("basic", false) => pyarrow_optional(column),
        ("nested", false) => pyarrow_nested_optional(column),
        ("struct", false) => pyarrow_struct_optional(column),
        _ => todo!(),
    };

    assert_eq!(expected, array);

    let expected_stats = match (file, required) {
        ("basic", true) => pyarrow_required_stats(column),
        ("basic", false) => pyarrow_optional_stats(column),
        ("nested", false) => (Some(4), Value::Int64(Some(0)), Value::Int64(Some(10))),
        // incorrect: it is only picking the first stats
        ("struct", false) => (
            Some(4),
            Value::Boolean(Some(false)),
            Value::Boolean(Some(true)),
        ),
        _ => todo!(),
    };

    assert_eq_stats(expected_stats, statistics.unwrap().as_ref());

    Ok(())
}

#[test]
fn pyarrow_v1_dict_int64_required() -> Result<()> {
    test_pyarrow_integration("basic", 0, 1, true, true, "")
}

#[test]
fn pyarrow_v1_dict_int64_optional() -> Result<()> {
    test_pyarrow_integration("basic", 0, 1, false, true, "")
}

#[test]
fn pyarrow_v1_non_dict_int64_required() -> Result<()> {
    test_pyarrow_integration("basic", 0, 1, true, false, "")
}

#[test]
fn pyarrow_v1_non_dict_int64_optional() -> Result<()> {
    test_pyarrow_integration("basic", 0, 1, false, false, "")
}

#[test]
fn pyarrow_v1_non_dict_int64_optional_snappy() -> Result<()> {
    test_pyarrow_integration("basic", 0, 1, false, false, "/snappy")
}

#[test]
fn pyarrow_v1_non_dict_int64_optional_lz4() -> Result<()> {
    test_pyarrow_integration("basic", 0, 1, false, false, "/lz4")
}

#[test]
fn pyarrow_v2_non_dict_int64_optional() -> Result<()> {
    test_pyarrow_integration("basic", 0, 2, false, false, "")
}

#[test]
fn pyarrow_v2_non_dict_int64_required() -> Result<()> {
    test_pyarrow_integration("basic", 0, 2, true, false, "")
}

#[test]
fn pyarrow_v2_dict_int64_optional() -> Result<()> {
    test_pyarrow_integration("basic", 0, 2, false, true, "")
}

#[test]
fn pyarrow_v2_non_dict_int64_optional_compressed() -> Result<()> {
    test_pyarrow_integration("basic", 0, 2, false, false, "/snappy")
}

#[test]
fn pyarrow_v1_dict_string_required() -> Result<()> {
    test_pyarrow_integration("basic", 2, 1, true, true, "")
}

#[test]
fn pyarrow_v1_dict_string_optional() -> Result<()> {
    test_pyarrow_integration("basic", 2, 1, false, true, "")
}

#[test]
fn pyarrow_v1_non_dict_string_required() -> Result<()> {
    test_pyarrow_integration("basic", 2, 1, true, false, "")
}

#[test]
fn pyarrow_v1_non_dict_string_optional() -> Result<()> {
    test_pyarrow_integration("basic", 2, 1, false, false, "")
}

#[test]
fn pyarrow_v1_dict_list_optional() -> Result<()> {
    test_pyarrow_integration("nested", 0, 1, false, true, "")
}

#[test]
fn pyarrow_v1_non_dict_list_optional() -> Result<()> {
    test_pyarrow_integration("nested", 0, 1, false, false, "")
}

#[test]
fn pyarrow_v1_struct_optional() -> Result<()> {
    test_pyarrow_integration("struct", 0, 1, false, false, "")
}

#[test]
fn pyarrow_v2_struct_optional() -> Result<()> {
    test_pyarrow_integration("struct", 0, 2, false, false, "")
}

#[test]
fn pyarrow_v1_struct_required() -> Result<()> {
    test_pyarrow_integration("struct", 1, 1, false, false, "")
}

#[test]
fn pyarrow_v2_struct_required() -> Result<()> {
    test_pyarrow_integration("struct", 1, 2, false, false, "")
}
