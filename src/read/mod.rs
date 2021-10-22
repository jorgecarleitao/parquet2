mod compression;
pub mod levels;
mod metadata;
mod page_iterator;
#[cfg(feature = "stream")]
mod page_stream;
#[cfg(feature = "stream")]
mod stream;

use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

pub use compression::{decompress, BasicDecompressor, Decompressor};
pub use metadata::read_metadata;
pub use page_iterator::{PageFilter, PageIterator};
#[cfg(feature = "stream")]
pub use page_stream::get_page_stream;
#[cfg(feature = "stream")]
pub use stream::read_metadata as read_metadata_async;

use crate::error::ParquetError;
use crate::metadata::{ColumnChunkMetaData, ColumnDescriptor, RowGroupMetaData};
use crate::{error::Result, metadata::FileMetaData};

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

/// Returns a new [`PageIterator`] by seeking `reader` to the begining of `column_chunk`.
pub fn get_page_iterator<RR: Read + Seek>(
    column_chunk: &ColumnChunkMetaData,
    mut reader: RR,
    pages_filter: Option<PageFilter>,
    buffer: Vec<u8>,
) -> Result<PageIterator<RR>> {
    let pages_filter = pages_filter.unwrap_or_else(|| Arc::new(|_, _| true));

    let (col_start, _) = column_chunk.byte_range();
    reader.seek(SeekFrom::Start(col_start))?;
    Ok(PageIterator::new(
        reader,
        column_chunk.num_values(),
        column_chunk.compression(),
        column_chunk.descriptor().clone(),
        pages_filter,
        buffer,
    ))
}

pub trait MutStreamingIterator: Sized {
    type Item;
    type Error;

    fn advance(self) -> std::result::Result<Option<Self>, Self::Error>;
    fn get(&mut self) -> Option<&mut Self::Item>;
}

pub struct ColumnIterator<R: Read + Seek> {
    reader: Option<R>,
    columns: Vec<ColumnChunkMetaData>,
    filters: Vec<Option<PageFilter>>,
    current: Option<(PageIterator<R>, ColumnDescriptor)>,
}

impl<R: Read + Seek> ColumnIterator<R> {
    pub fn new(
        reader: R,
        mut columns: Vec<ColumnChunkMetaData>,
        mut filters: Vec<Option<PageFilter>>,
    ) -> Self {
        columns.reverse();
        filters.reverse();
        Self {
            reader: Some(reader),
            columns,
            filters,
            current: None,
        }
    }
}

impl<R: Read + Seek> MutStreamingIterator for ColumnIterator<R> {
    type Item = (PageIterator<R>, ColumnDescriptor);
    type Error = ParquetError;

    fn advance(mut self) -> Result<Option<Self>> {
        let (reader, buffer) = if let Some((iter, _)) = self.current {
            iter.into_inner()
        } else {
            (self.reader.unwrap(), vec![])
        };
        if self.columns.is_empty() {
            return Ok(None);
        };
        let column = self.columns.pop().unwrap();
        let filter = self.filters.pop().unwrap();

        let iter = get_page_iterator(&column, reader, filter, buffer)?;
        let current = Some((iter, column.descriptor().clone()));
        Ok(Some(Self {
            reader: None,
            columns: self.columns,
            filters: self.filters,
            current,
        }))
    }

    fn get(&mut self) -> Option<&mut Self::Item> {
        self.current.as_mut()
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use crate::FallibleStreamingIterator;

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
        let column_metadata = metadata.row_groups[row_group].column(column);
        let buffer = vec![];
        let mut iter = get_page_iterator(column_metadata, &mut file, None, buffer)?;

        let page = iter.next().unwrap().unwrap();
        assert_eq!(page.num_values(), 8);
        Ok(())
    }

    #[test]
    fn reuse_buffer() -> Result<()> {
        let mut testdata = get_path();
        testdata.push("alltypes_plain.snappy.parquet");
        let mut file = File::open(testdata).unwrap();

        let metadata = read_metadata(&mut file)?;

        let row_group = 0;
        let column = 0;
        let column_metadata = metadata.row_groups[row_group].column(column);
        let buffer = vec![0];
        let iterator = get_page_iterator(column_metadata, &mut file, None, buffer)?;

        let buffer = vec![];
        let mut iterator = Decompressor::new(iterator, buffer);

        let _ = iterator.next()?.unwrap();

        assert!(iterator.next()?.is_none());
        let (a, b) = iterator.into_buffers();
        assert_eq!(a.len(), 11); // note: compressed is higher in this example.
        assert_eq!(b.len(), 9);

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
        let column_metadata = metadata.row_groups[row_group].column(column);
        let buffer = vec![1];
        let iterator = get_page_iterator(column_metadata, &mut file, None, buffer)?;

        let buffer = vec![];
        let mut iterator = Decompressor::new(iterator, buffer);

        iterator.next()?.unwrap();

        assert!(iterator.next()?.is_none());
        let (a, b) = iterator.into_buffers();

        assert_eq!(a.len(), 11);
        assert_eq!(b.len(), 0); // the decompressed buffer is never used because it is always swapped with the other buffer.

        Ok(())
    }

    #[test]
    fn basics_column_iterator() -> Result<()> {
        let mut testdata = get_path();
        testdata.push("alltypes_plain.parquet");
        let mut file = File::open(testdata).unwrap();

        let metadata = read_metadata(&mut file)?;

        let mut iter = ColumnIterator::new(
            file,
            metadata.row_groups[0].columns().to_vec(),
            vec![None; metadata.row_groups[0].columns().len()],
        );

        while let Some(mut new_iter) = iter.advance()? {
            if let Some((pages, _descriptor)) = new_iter.get() {
                let mut iterator = BasicDecompressor::new(pages, vec![]);
                while let Some(_page) = iterator.next()? {
                    // do something with it
                }
            }
            iter = new_iter;
        }
        Ok(())
    }
}
