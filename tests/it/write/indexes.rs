use std::io::Cursor;

use parquet2::bloom_filter;
use parquet2::compression::CompressionOptions;
use parquet2::error::Result;
use parquet2::indexes::{
    select_pages, BoundaryOrder, Index, Interval, NativeIndex, PageIndex, PageLocation,
};
use parquet2::metadata::SchemaDescriptor;
use parquet2::read::{
    read_columns_indexes, read_metadata, read_pages_locations, BasicDecompressor, IndexedPageReader,
};
use parquet2::schema::types::{ParquetType, PhysicalType, PrimitiveType};
use parquet2::write::WriteOptions;
use parquet2::write::{Compressor, DynIter, DynStreamingIterator, FileWriter, Version};
#[cfg(feature = "async")]
use {parquet2::read::read_metadata_async, parquet2::write::FileStreamer};

use crate::read::collect;
use crate::Array;

use super::primitive::array_to_page_v1;

const PAGE1: &'static [Option<i32>] = &[Some(0), Some(1), None, Some(3), Some(4), Some(5), Some(6)];
const PAGE2: &'static [Option<i32>] = &[Some(10), Some(11)];

fn create_bloom_filter() -> Vec<u8> {
    let mut bloom_filter_bits = vec![0; 32];

    for num in PAGE1.iter().chain(PAGE2.iter()) {
        let num = match num {
            Some(num) => *num,
            None => continue,
        };

        bloom_filter::insert(&mut bloom_filter_bits, bloom_filter::hash_native(num));
    }

    bloom_filter_bits
}

fn write_file() -> Result<Vec<u8>> {
    let options = WriteOptions {
        write_statistics: true,
        version: Version::V1,
    };

    let schema = SchemaDescriptor::new(
        "schema".to_string(),
        vec![ParquetType::from_physical(
            "col1".to_string(),
            PhysicalType::Int32,
        )],
    );

    let pages = vec![
        array_to_page_v1::<i32>(PAGE1, &options, &schema.columns()[0].descriptor),
        array_to_page_v1::<i32>(PAGE2, &options, &schema.columns()[0].descriptor),
    ];

    let pages = DynStreamingIterator::new(Compressor::new(
        DynIter::new(pages.into_iter()),
        CompressionOptions::Uncompressed,
        vec![],
    ));

    let bloom_filter_bits = create_bloom_filter();

    let columns = std::iter::once(Ok((pages, Some(bloom_filter_bits.as_slice()))));

    let writer = Cursor::new(vec![]);
    let mut writer = FileWriter::new(writer, schema, options, None);

    writer.write(DynIter::new(columns))?;
    writer.end(None)?;

    Ok(writer.into_inner().into_inner())
}

#[test]
fn read_indexed_page() -> Result<()> {
    let data = write_file()?;
    let mut reader = Cursor::new(data);

    let metadata = read_metadata(&mut reader)?;

    let column = 0;
    let columns = &metadata.row_groups[0].columns();

    // selected the rows
    let intervals = &[Interval::new(2, 2)];

    let pages = read_pages_locations(&mut reader, columns)?;

    let pages = select_pages(intervals, &pages[column], metadata.row_groups[0].num_rows())?;

    let pages = IndexedPageReader::new(reader, &columns[column], pages, vec![], vec![]);

    let pages = BasicDecompressor::new(pages, vec![]);

    let arrays = collect(pages, columns[column].physical_type())?;

    // the second item and length 2
    assert_eq!(arrays, vec![Array::Int32(vec![None, Some(3)])]);

    Ok(())
}

#[test]
fn read_indexes_and_locations() -> Result<()> {
    let data = write_file()?;
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
            first_row_index: 7,
        },
    ]];
    let expected_index = vec![Box::new(NativeIndex::<i32> {
        primitive_type: PrimitiveType::from_physical("col1".to_string(), PhysicalType::Int32),
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

#[test]
fn bloom_filter_roundtrip() -> Result<()> {
    let data = write_file()?;
    let mut reader = Cursor::new(data);

    let metadata = read_metadata(&mut reader)?;

    let column_metadata = &metadata.row_groups[0].columns()[0];

    let mut bloom_filter_bits = Vec::new();
    bloom_filter::read(column_metadata, &mut reader, &mut bloom_filter_bits)?;

    assert_eq!(bloom_filter_bits, create_bloom_filter());

    Ok(())
}

async fn write_file_async() -> Result<Vec<u8>> {
    let options = WriteOptions {
        write_statistics: true,
        version: Version::V1,
    };

    let schema = SchemaDescriptor::new(
        "schema".to_string(),
        vec![ParquetType::from_physical(
            "col1".to_string(),
            PhysicalType::Int32,
        )],
    );

    let pages = vec![
        array_to_page_v1::<i32>(PAGE1, &options, &schema.columns()[0].descriptor),
        array_to_page_v1::<i32>(PAGE2, &options, &schema.columns()[0].descriptor),
    ];

    let pages = DynStreamingIterator::new(Compressor::new(
        DynIter::new(pages.into_iter()),
        CompressionOptions::Uncompressed,
        vec![],
    ));

    let bloom_filter_bits = create_bloom_filter();

    let columns = std::iter::once(Ok((pages, Some(bloom_filter_bits.as_slice()))));

    let writer = futures::io::Cursor::new(vec![]);
    let mut writer = FileStreamer::new(writer, schema, options, None);

    writer.write(DynIter::new(columns)).await?;
    writer.end(None).await?;

    Ok(writer.into_inner().into_inner())
}

#[cfg(feature = "async")]
#[tokio::test]
async fn bloom_filter_roundtrip_async() -> Result<()> {
    let data = write_file_async().await?;
    let mut reader = futures::io::Cursor::new(data);

    let metadata = read_metadata_async(&mut reader).await?;

    let column_metadata = &metadata.row_groups[0].columns()[0];

    let mut bloom_filter_bits = Vec::new();
    bloom_filter::read_async(column_metadata, &mut reader, &mut bloom_filter_bits).await?;

    assert_eq!(bloom_filter_bits, create_bloom_filter());

    Ok(())
}
