mod primitive;

use std::io::{Cursor, Read, Seek};
use std::sync::Arc;

use parquet2::compression::Compression;
use parquet2::error::Result;
use parquet2::indexes::{BoundaryOrder, Index, NativeIndex, PageIndex, PageLocation};
use parquet2::metadata::SchemaDescriptor;
use parquet2::read::{read_columns_indexes, read_metadata, read_pages_locations};
use parquet2::schema::types::{PhysicalType, PrimitiveType};
use parquet2::statistics::Statistics;
use parquet2::write::{Compressor, DynIter, DynStreamingIterator, FileWriter, Version};
use parquet2::{metadata::Descriptor, page::EncodedPage, write::WriteOptions};

use super::Array;
use super::{alltypes_plain, alltypes_statistics};
use primitive::array_to_page_v1;

pub fn array_to_page(
    array: &Array,
    options: &WriteOptions,
    descriptor: &Descriptor,
) -> Result<EncodedPage> {
    // using plain encoding format
    match array {
        Array::Int32(array) => primitive::array_to_page_v1(array, options, descriptor),
        Array::Int64(array) => primitive::array_to_page_v1(array, options, descriptor),
        Array::Int96(array) => primitive::array_to_page_v1(array, options, descriptor),
        Array::Float32(array) => primitive::array_to_page_v1(array, options, descriptor),
        Array::Float64(array) => primitive::array_to_page_v1(array, options, descriptor),
        _ => todo!(),
    }
}

fn read_column<R: Read + Seek>(reader: &mut R) -> Result<(Array, Option<Arc<dyn Statistics>>)> {
    let (a, statistics) = super::read::read_column(reader, 0, 0)?;
    Ok((a, statistics))
}

fn test_column(column: usize) -> Result<()> {
    let array = alltypes_plain(column);

    let options = WriteOptions {
        write_statistics: true,
        compression: Compression::Uncompressed,
        version: Version::V1,
    };

    // prepare schema
    let a = match array {
        Array::Int32(_) => "INT32",
        Array::Int64(_) => "INT64",
        Array::Int96(_) => "INT96",
        Array::Float32(_) => "FLOAT",
        Array::Float64(_) => "DOUBLE",
        _ => todo!(),
    };
    let schema =
        SchemaDescriptor::try_from_message(&format!("message schema {{ OPTIONAL {} col; }}", a))?;

    let a = schema.columns();

    let pages = DynStreamingIterator::new(Compressor::new_from_vec(
        DynIter::new(std::iter::once(array_to_page(
            &array,
            &options,
            &a[0].descriptor,
        ))),
        options.compression,
        vec![],
    ));
    let columns = std::iter::once(Ok(pages));

    let writer = Cursor::new(vec![]);
    let mut writer = FileWriter::new(writer, schema, options, None);

    writer.start()?;
    writer.write(DynIter::new(columns))?;
    let writer = writer.end(None)?.1;

    let data = writer.into_inner();

    let (result, statistics) = read_column(&mut Cursor::new(data))?;
    assert_eq!(array, result);
    let stats = alltypes_statistics(column);
    assert_eq!(
        statistics.as_ref().map(|x| x.as_ref()),
        Some(stats).as_ref().map(|x| x.as_ref())
    );
    Ok(())
}

#[test]
fn int32() -> Result<()> {
    test_column(0)
}

#[test]
#[ignore = "Native boolean writer not yet implemented"]
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
fn basic() -> Result<()> {
    let array = vec![
        Some(0),
        Some(1),
        Some(2),
        Some(3),
        Some(4),
        Some(5),
        Some(6),
    ];

    let options = WriteOptions {
        write_statistics: false,
        compression: Compression::Uncompressed,
        version: Version::V1,
    };

    let schema = SchemaDescriptor::try_from_message("message schema { OPTIONAL INT32 col; }")?;

    let pages = DynStreamingIterator::new(Compressor::new_from_vec(
        DynIter::new(std::iter::once(array_to_page_v1(
            &array,
            &options,
            &schema.columns()[0].descriptor,
        ))),
        options.compression,
        vec![],
    ));
    let columns = std::iter::once(Ok(pages));

    let writer = Cursor::new(vec![]);
    let mut writer = FileWriter::new(writer, schema, options, None);

    writer.start()?;
    writer.write(DynIter::new(columns))?;
    let writer = writer.end(None)?.1;

    let data = writer.into_inner();
    let mut reader = Cursor::new(data);

    let metadata = read_metadata(&mut reader)?;

    // validated against an equivalent array produced by pyarrow.
    let expected = 51;
    assert_eq!(
        metadata.row_groups[0].columns()[0].uncompressed_size(),
        expected
    );

    Ok(())
}

#[test]
fn indexes() -> Result<()> {
    let array1 = vec![Some(0), Some(1), None, Some(3), Some(4), Some(5), Some(6)];
    let array2 = vec![Some(10), Some(11)];

    let options = WriteOptions {
        write_statistics: true,
        compression: Compression::Uncompressed,
        version: Version::V1,
    };

    let schema = SchemaDescriptor::try_from_message("message schema { OPTIONAL INT32 col; }")?;

    let pages = vec![
        array_to_page_v1::<i32>(&array1, &options, &schema.columns()[0].descriptor),
        array_to_page_v1::<i32>(&array2, &options, &schema.columns()[0].descriptor),
    ];

    let pages = DynStreamingIterator::new(Compressor::new(
        DynIter::new(pages.into_iter()),
        options.compression,
        vec![],
    ));
    let columns = std::iter::once(Ok(pages));

    let writer = Cursor::new(vec![]);
    let mut writer = FileWriter::new(writer, schema, options, None);

    writer.start()?;
    writer.write(DynIter::new(columns))?;
    let writer = writer.end(None)?.1;

    let data = writer.into_inner();
    let mut reader = Cursor::new(data);

    let metadata = read_metadata(&mut reader)?;

    let columns = &metadata.row_groups[0].columns();

    let expected_page_locations = vec![vec![
        PageLocation {
            offset: 4,
            compressed_page_size: 63,
            first_row_index: 0,
        },
        PageLocation {
            offset: 67,
            compressed_page_size: 47,
            first_row_index: array1.len() as i64,
        },
    ]];
    let expected_index = vec![Box::new(NativeIndex::<i32> {
        primitive_type: PrimitiveType::from_physical("col".to_string(), PhysicalType::Int32),
        indexes: vec![
            PageIndex {
                min: Some(0),
                max: Some(6),
                null_count: Some(1),
            },
            PageIndex {
                min: Some(10),
                max: Some(11),
                null_count: Some(0),
            },
        ],
        boundary_order: BoundaryOrder::Unordered,
    }) as Box<dyn Index>];

    let indexes = read_columns_indexes(&mut reader, columns)?;
    assert_eq!(&indexes, &expected_index);

    let pages = read_pages_locations(&mut reader, columns)?;
    assert_eq!(pages, expected_page_locations);

    Ok(())
}
