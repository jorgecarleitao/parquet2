mod binary;
mod indexes;
mod primitive;

use std::io::{Cursor, Read, Seek};
use std::sync::Arc;

use parquet2::compression::Compression;
use parquet2::error::Result;
use parquet2::metadata::SchemaDescriptor;
use parquet2::read::read_metadata;
use parquet2::schema::types::{ParquetType, PhysicalType};
use parquet2::statistics::Statistics;
use parquet2::write::FileStreamer;
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
        Array::Binary(array) => binary::array_to_page_v1(array, options, descriptor),
        _ => todo!(),
    }
}

fn read_column<R: Read + Seek>(reader: &mut R) -> Result<(Array, Option<Arc<dyn Statistics>>)> {
    let (a, statistics) = super::read::read_column(reader, 0, "col")?;
    Ok((a, statistics))
}

async fn read_column_async<
    R: futures::AsyncRead + futures::AsyncSeek + Send + std::marker::Unpin,
>(
    reader: &mut R,
) -> Result<(Array, Option<Arc<dyn Statistics>>)> {
    let (a, statistics) = super::read::read_column_async(reader, 0, "col").await?;
    Ok((a, statistics))
}

fn test_column(column: &str, compression: CompressionEncode) -> Result<()> {
    let array = alltypes_plain(column);

    let options = WriteOptions {
        write_statistics: true,
        version: Version::V1,
    };

    // prepare schema
    let type_ = match array {
        Array::Int32(_) => PhysicalType::Int32,
        Array::Int64(_) => PhysicalType::Int64,
        Array::Int96(_) => PhysicalType::Int96,
        Array::Float32(_) => PhysicalType::Float,
        Array::Float64(_) => PhysicalType::Double,
        Array::Binary(_) => PhysicalType::ByteArray,
        _ => todo!(),
    };

    let schema = SchemaDescriptor::new(
        "schema".to_string(),
        vec![ParquetType::from_physical("col".to_string(), type_)],
    );

    let a = schema.columns();

    let pages = DynStreamingIterator::new(Compressor::new_from_vec(
        DynIter::new(std::iter::once(array_to_page(
            &array,
            &options,
            &a[0].descriptor,
        ))),
        compression,
        vec![],
    ));
    let columns = std::iter::once(Ok(pages));

    let writer = Cursor::new(vec![]);
    let mut writer = FileWriter::new(writer, schema, options, None);

    writer.start()?;
    writer.write(DynIter::new(columns))?;
    writer.end(None)?;

    let data = writer.into_inner().into_inner();

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
    test_column("id", CompressionEncode::Uncompressed)
}

#[test]
fn int32_snappy() -> Result<()> {
    test_column("id", CompressionEncode::Snappy)
}

#[test]
fn int32_lz4() -> Result<()> {
    test_column("id", CompressionEncode::Lz4Raw)
}

#[test]
fn int32_lz4_short_i32_array() -> Result<()> {
    test_column("id-short-array", CompressionEncode::Lz4Raw)
}

#[test]
fn int32_brotli() -> Result<()> {
    test_column("id", CompressionEncode::Brotli)
}

#[test]
#[ignore = "Native boolean writer not yet implemented"]
fn bool() -> Result<()> {
    test_column("bool_col", CompressionEncode::Uncompressed)
}

#[test]
fn tinyint() -> Result<()> {
    test_column("tinyint_col", CompressionEncode::Uncompressed)
}

#[test]
fn smallint_col() -> Result<()> {
    test_column("smallint_col", CompressionEncode::Uncompressed)
}

#[test]
fn int_col() -> Result<()> {
    test_column("int_col", CompressionEncode::Uncompressed)
}

#[test]
fn bigint_col() -> Result<()> {
    test_column("bigint_col", CompressionEncode::Uncompressed)
}

#[test]
fn float_col() -> Result<()> {
    test_column("float_col", CompressionEncode::Uncompressed)
}

#[test]
fn double_col() -> Result<()> {
    test_column("double_col", CompressionEncode::Uncompressed)
}

#[test]
fn string_col() -> Result<()> {
    test_column("string_col", CompressionEncode::Uncompressed)
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
        version: Version::V1,
    };

    let schema = SchemaDescriptor::new(
        "schema".to_string(),
        vec![ParquetType::from_physical(
            "col".to_string(),
            PhysicalType::Int32,
        )],
    );

    let pages = DynStreamingIterator::new(Compressor::new_from_vec(
        DynIter::new(std::iter::once(array_to_page_v1(
            &array,
            &options,
            &schema.columns()[0].descriptor,
        ))),
        Compression::Uncompressed,
        vec![],
    ));
    let columns = std::iter::once(Ok(pages));

    let writer = Cursor::new(vec![]);
    let mut writer = FileWriter::new(writer, schema, options, None);

    writer.start()?;
    writer.write(DynIter::new(columns))?;
    writer.end(None)?;

    let data = writer.into_inner().into_inner();
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

async fn test_column_async(column: &str) -> Result<()> {
    let array = alltypes_plain(column);

    let options = WriteOptions {
        write_statistics: true,
        version: Version::V1,
    };

    // prepare schema
    let type_ = match array {
        Array::Int32(_) => PhysicalType::Int32,
        Array::Int64(_) => PhysicalType::Int64,
        Array::Int96(_) => PhysicalType::Int96,
        Array::Float32(_) => PhysicalType::Float,
        Array::Float64(_) => PhysicalType::Double,
        _ => todo!(),
    };

    let schema = SchemaDescriptor::new(
        "schema".to_string(),
        vec![ParquetType::from_physical("col".to_string(), type_)],
    );

    let a = schema.columns();

    let pages = DynStreamingIterator::new(Compressor::new_from_vec(
        DynIter::new(std::iter::once(array_to_page(
            &array,
            &options,
            &a[0].descriptor,
        ))),
        Compression::Uncompressed,
        vec![],
    ));
    let columns = std::iter::once(Ok(pages));

    let writer = futures::io::Cursor::new(vec![]);
    let mut writer = FileStreamer::new(writer, schema, options, None);

    writer.start().await?;
    writer.write(DynIter::new(columns)).await?;
    let writer = writer.end(None).await?.1;

    let data = writer.into_inner();

    let (result, statistics) = read_column_async(&mut futures::io::Cursor::new(data)).await?;
    assert_eq!(array, result);
    let stats = alltypes_statistics(column);
    assert_eq!(
        statistics.as_ref().map(|x| x.as_ref()),
        Some(stats).as_ref().map(|x| x.as_ref())
    );
    Ok(())
}

#[tokio::test]
async fn test_async() -> Result<()> {
    test_column_async("float_col").await
}
