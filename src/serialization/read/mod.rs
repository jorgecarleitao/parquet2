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

    use crate::read::{get_page_iterator, read_metadata};
    use crate::tests::*;
    use crate::types::int96_to_i64;

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
            .column_descriptor()
            .clone();
        Ok((
            descriptor,
            get_page_iterator(&metadata, row_group, column, &mut file)?
                .collect::<Result<Vec<_>>>()?,
        ))
    }

    fn get_column(path: &str, column: usize) -> Result<Array> {
        let (descriptor, mut pages) = prepare(path, 0, column)?;
        assert_eq!(pages.len(), 1);

        let page = pages.pop().unwrap();

        page_to_array(page, &descriptor)
    }

    fn test_column(column: usize) -> Result<()> {
        let mut path = get_path();
        path.push("alltypes_plain.parquet");
        let path = path.to_str().unwrap();
        let result = get_column(path, column)?;
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
            1235865600000i64,
            1235865660000,
            1238544000000,
            1238544060000,
            1233446400000,
            1233446460000,
            1230768000000,
            1230768060000,
        ];

        let expected = expected.into_iter().map(Some).collect::<Vec<_>>();
        let result = get_column(&path, 10)?;
        if let Array::Int96(result) = result {
            let a = result
                .into_iter()
                .map(|x| x.map(int96_to_i64))
                .collect::<Vec<_>>();
            assert_eq!(expected, a);
        } else {
            panic!("Timestamp expected");
        };
        Ok(())
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
        let array = get_column(&path, column)?;

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
    fn pyarrow_v1_list_nullable() -> Result<()> {
        test_pyarrow_integration("nested", 0, 1, false)
    }
}
