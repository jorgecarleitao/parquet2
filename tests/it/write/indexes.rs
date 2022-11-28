use std::cell::RefCell;
use std::io::{Cursor, Seek, SeekFrom};
use std::rc::Rc;
use std::sync::Arc;

use parquet2::compression::CompressionOptions;
use parquet2::error::Result;
use parquet2::indexes::{
    select_pages, BoundaryOrder, Index, Interval, NativeIndex, PageIndex, PageLocation,
};
use parquet2::metadata::SchemaDescriptor;
use parquet2::page::Page;
use parquet2::read::{
    read_columns_indexes, read_metadata, read_pages_locations, BasicDecompressor,
    IndexedPageReader, PageReader,
};
use parquet2::schema::types::{ParquetType, PhysicalType, PrimitiveType};
use parquet2::write::WriteOptions;
use parquet2::write::{Compressor, DynIter, DynStreamingIterator, FileWriter, Version};
use parquet2::FallibleStreamingIterator;

use crate::read::{collect, page_to_array};
use crate::Array;

use super::primitive::array_to_page_v1;

fn write_file() -> Result<Vec<u8>> {
    let page1 = vec![Some(0), Some(1), None, Some(3), Some(4), Some(5), Some(6)];
    let page2 = vec![Some(10), Some(11)];

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
        array_to_page_v1::<i32>(&page1, &options, &schema.columns()[0].descriptor),
        array_to_page_v1::<i32>(&page2, &options, &schema.columns()[0].descriptor),
    ];

    let pages = DynStreamingIterator::new(Compressor::new(
        DynIter::new(pages.into_iter()),
        CompressionOptions::Uncompressed,
        vec![],
    ));
    let columns = std::iter::once(Ok(pages));

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
fn select_rows_in_runtime() -> Result<()> {
    let data = write_file()?;
    let mut reader = Cursor::new(data);

    let metadata = read_metadata(&mut reader)?;

    let columns = &metadata.row_groups[0].columns();
    let column = &columns[0];

    let intervals = Rc::new(RefCell::new(vec![Interval::new(2, 2)]));
    let pass_in = intervals.clone();

    reader.seek(SeekFrom::Start(column.metadata().data_page_offset as u64))?;

    let pages = PageReader::new(
        reader,
        &column,
        Arc::new(|_, _| true),
        Arc::new(move |page| {
            let interval = pass_in.borrow();
            page.set_selected_rows(interval.clone());
        }),
        vec![],
        usize::MAX,
    );

    let mut pages = BasicDecompressor::new(pages, vec![]);

    let mut arrays = vec![];
    let dict = None;
    if let Some(page) = pages.next()? {
        match page {
            Page::Data(page) => arrays.push(page_to_array(page, dict.as_ref())?),
            _ => unreachable!(),
        }
    }

    // change intervals
    intervals.replace(vec![Interval::new(1, 1)]);

    if let Some(page) = pages.next()? {
        match page {
            Page::Data(page) => arrays.push(page_to_array(page, dict.as_ref())?),
            _ => unreachable!(),
        }
    }
    assert_eq!(
        arrays,
        vec![
            Array::Int32(vec![None, Some(3)]),
            Array::Int32(vec![Some(11)])
        ]
    );

    Ok(())
}
