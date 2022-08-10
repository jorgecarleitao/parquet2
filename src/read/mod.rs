mod compression;
mod indexes;
pub mod levels;
mod metadata;
mod page;
#[cfg(feature = "async")]
mod stream;

use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;
use std::vec::IntoIter;

pub use compression::{decompress, BasicDecompressor, Decompressor};
pub use metadata::{deserialize_metadata, read_metadata};
#[cfg(feature = "async")]
#[cfg_attr(docsrs, doc(cfg(feature = "async")))]
pub use page::{get_page_stream, get_page_stream_from_column_start};
pub use page::{IndexedPageReader, PageFilter, PageIterator, PageMetaData, PageReader};

#[cfg(feature = "async")]
#[cfg_attr(docsrs, doc(cfg(feature = "async")))]
pub use stream::read_metadata as read_metadata_async;

use crate::error::Error;
use crate::metadata::{ColumnChunkMetaData, RowGroupMetaData};
use crate::page::CompressedPage;
use crate::schema::types::ParquetType;
use crate::{error::Result, metadata::FileMetaData};

pub use indexes::{read_columns_indexes, read_pages_locations};

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

/// Returns a new [`PageReader`] by seeking `reader` to the begining of `column_chunk`.
pub fn get_page_iterator<R: Read + Seek>(
    column_chunk: &ColumnChunkMetaData,
    mut reader: R,
    pages_filter: Option<PageFilter>,
    scratch: Vec<u8>,
    max_header_size: usize,
) -> Result<PageReader<R>> {
    let pages_filter = pages_filter.unwrap_or_else(|| Arc::new(|_, _| true));

    let (col_start, _) = column_chunk.byte_range();
    reader.seek(SeekFrom::Start(col_start))?;
    Ok(PageReader::new(
        reader,
        column_chunk,
        pages_filter,
        scratch,
        max_header_size,
    ))
}

/// Returns an [`Iterator`] of [`ColumnChunkMetaData`] corresponding to the columns
/// from `field` at `row_group`.
/// For primitive fields (e.g. `i64`), the iterator has exactly one item.
pub fn get_field_columns<'a>(
    metadata: &'a FileMetaData,
    row_group: usize,
    field: &'a ParquetType,
) -> impl Iterator<Item = &'a ColumnChunkMetaData> {
    metadata
        .schema()
        .columns()
        .iter()
        .enumerate()
        .filter(move |x| x.1.path_in_schema[0] == field.name())
        .map(move |x| &metadata.row_groups[row_group].columns()[x.0])
}

/// Returns a [`ColumnIterator`] of column chunks corresponding to `field`.
///
/// Contrarily to [`get_page_iterator`] that returns a single iterator of pages, this iterator
/// returns multiple iterators, one per physical column of the `field`.
/// For primitive fields (e.g. `i64`), [`ColumnIterator`] yields exactly one column.
/// For complex fields, it yields multiple columns.
/// `max_header_size` is the maximum number of bytes thrift is allowed to allocate
/// to read a page header.
pub fn get_column_iterator<R: Read + Seek>(
    reader: R,
    metadata: &FileMetaData,
    row_group: usize,
    field: usize,
    page_filter: Option<PageFilter>,
    scratch: Vec<u8>,
    max_header_size: usize,
) -> ColumnIterator<R> {
    let field = metadata.schema().fields()[field].clone();
    let columns = get_field_columns(metadata, row_group, &field)
        .cloned()
        .collect::<Vec<_>>();

    ColumnIterator::new(
        reader,
        field,
        columns,
        page_filter,
        scratch,
        max_header_size,
    )
}

/// State of [`MutStreamingIterator`].
#[derive(Debug)]
pub enum State<T> {
    /// Iterator still has elements
    Some(T),
    /// Iterator finished
    Finished(Vec<u8>),
}

/// A special kind of fallible streaming iterator where `advance` consumes the iterator.
pub trait MutStreamingIterator: Sized {
    type Item;
    type Error;

    fn advance(self) -> std::result::Result<State<Self>, Self::Error>;
    fn get(&mut self) -> Option<&mut Self::Item>;
}

/// Trait describing a [`MutStreamingIterator`] of column chunks.
pub trait ColumnChunkIter<I>:
    MutStreamingIterator<Item = (I, ColumnChunkMetaData), Error = Error>
{
    /// The field associated to the set of column chunks this iterator iterates over.
    fn field(&self) -> &ParquetType;
}

/// A [`MutStreamingIterator`] that reads column chunks one by one,
/// returning a [`PageReader`] per column.
pub struct ColumnIterator<R: Read + Seek> {
    reader: Option<R>,
    field: ParquetType,
    columns: Vec<ColumnChunkMetaData>,
    page_filter: Option<PageFilter>,
    current: Option<(PageReader<R>, ColumnChunkMetaData)>,
    scratch: Vec<u8>,
    max_header_size: usize,
}

impl<R: Read + Seek> ColumnIterator<R> {
    /// Returns a new [`ColumnIterator`]
    /// `max_header_size` is the maximum number of bytes thrift is allowed to allocate
    /// to read a page header.
    pub fn new(
        reader: R,
        field: ParquetType,
        mut columns: Vec<ColumnChunkMetaData>,
        page_filter: Option<PageFilter>,
        scratch: Vec<u8>,
        max_header_size: usize,
    ) -> Self {
        columns.reverse();
        Self {
            reader: Some(reader),
            field,
            scratch,
            columns,
            page_filter,
            current: None,
            max_header_size,
        }
    }
}

impl<R: Read + Seek> MutStreamingIterator for ColumnIterator<R> {
    type Item = (PageReader<R>, ColumnChunkMetaData);
    type Error = Error;

    fn advance(mut self) -> Result<State<Self>> {
        let (reader, scratch) = if let Some((iter, _)) = self.current {
            iter.into_inner()
        } else {
            (self.reader.unwrap(), self.scratch)
        };
        if self.columns.is_empty() {
            return Ok(State::Finished(scratch));
        };
        let column = self.columns.pop().unwrap();

        let iter = get_page_iterator(
            &column,
            reader,
            self.page_filter.clone(),
            scratch,
            self.max_header_size,
        )?;
        let current = Some((iter, column));
        Ok(State::Some(Self {
            reader: None,
            field: self.field,
            columns: self.columns,
            page_filter: self.page_filter,
            current,
            scratch: vec![],
            max_header_size: self.max_header_size,
        }))
    }

    fn get(&mut self) -> Option<&mut Self::Item> {
        self.current.as_mut()
    }
}

impl<R: Read + Seek> ColumnChunkIter<PageReader<R>> for ColumnIterator<R> {
    fn field(&self) -> &ParquetType {
        &self.field
    }
}

/// A [`MutStreamingIterator`] of pre-read column chunks
#[derive(Debug)]
pub struct ReadColumnIterator {
    field: ParquetType,
    chunks: Vec<(Vec<Result<CompressedPage>>, ColumnChunkMetaData)>,
    current: Option<(IntoIter<Result<CompressedPage>>, ColumnChunkMetaData)>,
}

impl ReadColumnIterator {
    /// Returns a new [`ReadColumnIterator`]
    pub fn new(
        field: ParquetType,
        chunks: Vec<(Vec<Result<CompressedPage>>, ColumnChunkMetaData)>,
    ) -> Self {
        Self {
            field,
            chunks,
            current: None,
        }
    }
}

impl MutStreamingIterator for ReadColumnIterator {
    type Item = (IntoIter<Result<CompressedPage>>, ColumnChunkMetaData);
    type Error = Error;

    fn advance(mut self) -> Result<State<Self>> {
        if self.chunks.is_empty() {
            return Ok(State::Finished(vec![]));
        }
        self.current = self
            .chunks
            .pop()
            .map(|(pages, meta)| (pages.into_iter(), meta));
        Ok(State::Some(Self {
            field: self.field,
            chunks: self.chunks,
            current: self.current,
        }))
    }

    fn get(&mut self) -> Option<&mut Self::Item> {
        self.current.as_mut()
    }
}

impl ColumnChunkIter<IntoIter<Result<CompressedPage>>> for ReadColumnIterator {
    fn field(&self) -> &ParquetType {
        &self.field
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
        let column_metadata = &metadata.row_groups[row_group].columns()[column];
        let buffer = vec![];
        let mut iter = get_page_iterator(column_metadata, &mut file, None, buffer, 1024 * 1024)?;

        let dict = iter.next().unwrap().unwrap();
        assert_eq!(dict.num_values(), 0);
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
        let column_metadata = &metadata.row_groups[row_group].columns()[column];
        let buffer = vec![0];
        let iterator = get_page_iterator(column_metadata, &mut file, None, buffer, 1024 * 1024)?;

        let buffer = vec![];
        let mut iterator = Decompressor::new(iterator, buffer);

        let _dict = iterator.next()?.unwrap();
        let _page = iterator.next()?.unwrap();

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
        let column_metadata = &metadata.row_groups[row_group].columns()[column];
        let buffer = vec![1];
        let iterator = get_page_iterator(column_metadata, &mut file, None, buffer, 1024 * 1024)?;

        let buffer = vec![];
        let mut iterator = Decompressor::new(iterator, buffer);

        // dict
        iterator.next()?.unwrap();
        // page
        iterator.next()?.unwrap();

        assert!(iterator.next()?.is_none());
        let (a, b) = iterator.into_buffers();

        assert_eq!(a.len(), 11);
        assert_eq!(b.len(), 0); // the decompressed buffer is never used because it is always swapped with the other buffer.

        Ok(())
    }

    #[test]
    fn column_iter() -> Result<()> {
        let mut testdata = get_path();
        testdata.push("alltypes_plain.parquet");
        let mut file = File::open(testdata).unwrap();

        let metadata = read_metadata(&mut file)?;

        let row_group = 0;
        let column = 0;
        let column_metadata = &metadata.row_groups[row_group].columns()[column];
        let iter: Vec<_> =
            get_page_iterator(column_metadata, &mut file, None, vec![], usize::MAX)?.collect();

        let field = metadata.schema().fields()[0].clone();
        let mut iter = ReadColumnIterator::new(field, vec![(iter, column_metadata.clone())]);

        loop {
            match iter.advance()? {
                State::Some(mut new_iter) => {
                    if let Some((pages, _descriptor)) = new_iter.get() {
                        let mut iterator = BasicDecompressor::new(pages, vec![]);
                        while let Some(_page) = iterator.next()? {
                            // do something with it
                        }
                        let _internal_buffer = iterator.into_inner();
                    }
                    iter = new_iter;
                }
                State::Finished(_buffer) => {
                    assert!(_buffer.is_empty()); // data is uncompressed => buffer is always moved
                    break;
                }
            }
        }
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
            metadata.schema().fields()[0].clone(),
            metadata.row_groups[0].columns().to_vec(),
            None,
            vec![],
            usize::MAX, // we trust the file is correct
        );

        loop {
            match iter.advance()? {
                State::Some(mut new_iter) => {
                    if let Some((pages, _descriptor)) = new_iter.get() {
                        let mut iterator = BasicDecompressor::new(pages, vec![]);
                        while let Some(_page) = iterator.next()? {
                            // do something with it
                        }
                        let _internal_buffer = iterator.into_inner();
                    }
                    iter = new_iter;
                }
                State::Finished(_buffer) => {
                    assert!(_buffer.is_empty()); // data is uncompressed => buffer is always moved
                    break;
                }
            }
        }
        Ok(())
    }
}
