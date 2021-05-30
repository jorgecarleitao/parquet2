mod compression;
mod metadata;
mod page;
mod page_dict;
mod page_iterator;
mod statistics;

pub use streaming_iterator;
pub use streaming_iterator::StreamingIterator;

pub use compression::{decompress, Decompressor};

pub use metadata::read_metadata;

use std::io::{Read, Seek, SeekFrom};

use crate::metadata::RowGroupMetaData;
use crate::{error::Result, metadata::FileMetaData};

pub use page::{CompressedPage, Page, PageHeader};
pub use page_dict::{BinaryPageDict, FixedLenByteArrayPageDict, PageDict, PrimitivePageDict};
pub use page_iterator::PageIterator;
pub use statistics::*;

/// Filters row group metadata to only those row groups,
/// for which the predicate function returns true
pub fn filter_row_groups(
    metadata: &FileMetaData,
    predicate: &dyn Fn(&RowGroupMetaData, usize) -> bool,
) -> FileMetaData {
    let mut filtered_row_groups = Vec::<RowGroupMetaData>::new();
    for (i, row_group_metadata) in metadata.row_groups.iter().enumerate() {
        if predicate(row_group_metadata, i) {
            filtered_row_groups.push(row_group_metadata.clone());
        }
    }
    let mut metadata = metadata.clone();
    metadata.row_groups = filtered_row_groups;
    metadata
}

pub fn get_page_iterator<'a, RR: Read + Seek>(
    metadata: &FileMetaData,
    row_group: usize,
    column: usize,
    reader: &'a mut RR,
    buffer: Vec<u8>,
) -> Result<PageIterator<'a, RR>> {
    let column_metadata = metadata.row_groups[row_group].column(column);
    let (col_start, _) = column_metadata.byte_range();
    reader.seek(SeekFrom::Start(col_start))?;
    Ok(PageIterator::new(
        reader,
        column_metadata.num_values(),
        *column_metadata.compression(),
        column_metadata.physical_type(),
        buffer,
    ))
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use super::*;

    use crate::tests::get_path;

    #[test]
    fn basic() -> Result<()> {
        let mut testdata = get_path();
        testdata.push("alltypes_plain.parquet");
        let mut file = File::open(testdata).unwrap();

        let metadata = read_metadata(&mut file)?;

        let row_group = 0;
        let column = 0;
        let buffer = vec![];
        let mut iter = get_page_iterator(&metadata, row_group, column, &mut file, buffer)?;

        let page = iter.next().unwrap().unwrap();
        assert_eq!(page.num_values(), 8);
        Ok(())
    }

    #[test]
    fn reuse_buffer() -> Result<()> {
        let mut testdata = get_path();
        testdata.push("alltypes_plain.parquet");
        let mut file = File::open(testdata).unwrap();

        let metadata = read_metadata(&mut file)?;

        let row_group = 0;
        let column = 0;
        let buffer = vec![0];
        let mut iterator = get_page_iterator(&metadata, row_group, column, &mut file, buffer)?;

        let page = iterator.next().unwrap().unwrap();
        iterator.reuse_buffer(page.buffer);

        assert!(iterator.next().is_none());
        assert!(!iterator.buffer.is_empty());

        Ok(())
    }

    #[test]
    fn reuse_buffer_decompress() -> Result<()> {
        let mut testdata = get_path();
        testdata.push("alltypes_plain.parquet");
        let mut file = File::open(testdata).unwrap();

        let metadata = read_metadata(&mut file)?;

        let row_group = 0;
        let column = 0;
        let buffer = vec![1];
        let iterator = get_page_iterator(&metadata, row_group, column, &mut file, buffer)?;

        let buffer = vec![];
        let mut iterator = Decompressor::new(iterator, buffer);

        iterator.next().unwrap().as_ref().unwrap();

        assert!(iterator.next().is_none());
        let (a, b) = iterator.into_buffers();

        assert_eq!(a.len(), 11);
        assert_eq!(b.len(), 0); // the decompressed buffer is never used because it is always swapped with the other buffer.

        Ok(())
    }
}
