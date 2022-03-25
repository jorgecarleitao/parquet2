use std::{
    collections::VecDeque,
    io::{Cursor, Read, Seek, SeekFrom},
    sync::Arc,
};

use crate::{
    error::ParquetError,
    indexes::FilteredPage,
    metadata::Descriptor,
    page::{CompressedDataPage, DictPage, ParquetPageHeader},
    parquet_bridge::Compression,
};

use super::reader::{finish_page, read_page_header, FinishedPage};

enum LazyDict {
    // The dictionary has been read and deserialized
    Dictionary(Arc<dyn DictPage>),
    // The range of the dictionary page
    Range(u64, usize),
}

/// A fallible [`Iterator`] of [`CompressedDataPage`]. This iterator leverages page indexes
/// to skip pages that are not needed. Consequently, the pages from this
/// iterator always have [`Some`] [`CompressedDataPage::rows()`]
pub struct IndexedPageReader<R: Read + Seek> {
    // The source
    reader: R,

    compression: Compression,

    dictionary: Option<LazyDict>,

    // used to deserialize dictionary pages and attach the descriptor to every read page
    descriptor: Descriptor,

    // buffer to read the whole page [header][data] into memory
    buffer: Vec<u8>,

    // buffer to store the data [data] and re-use across pages
    data_buffer: Vec<u8>,

    pages: VecDeque<FilteredPage>,
}

fn resize_buffer(buffer: &mut Vec<u8>, length: usize) {
    // prepare buffer
    if length > buffer.len() {
        // dealloc and ignore region, replacing it by a new region
        *buffer = vec![0u8; length];
    } else {
        buffer.clear();
        buffer.resize(length, 0);
    }
}

fn read_page<R: Read + Seek>(
    reader: &mut R,
    start: u64,
    length: usize,
    buffer: &mut Vec<u8>,
    data: &mut Vec<u8>,
) -> Result<ParquetPageHeader, ParquetError> {
    // seek to the page
    reader.seek(SeekFrom::Start(start))?;

    // read [header][data] to buffer
    resize_buffer(buffer, length);
    reader.read_exact(buffer)?;

    // deserialize [header]
    let mut reader = Cursor::new(buffer);
    let page_header = read_page_header(&mut reader)?;
    let header_size = reader.seek(SeekFrom::Current(0)).unwrap() as usize;
    let buffer = reader.into_inner();

    // copy [data]
    data.clear();
    data.extend_from_slice(&buffer[header_size..]);
    Ok(page_header)
}

fn read_dict_page<R: Read + Seek>(
    reader: &mut R,
    start: u64,
    length: usize,
    buffer: &mut Vec<u8>,
    data: &mut Vec<u8>,
    compression: Compression,
    descriptor: &Descriptor,
) -> Result<Arc<dyn DictPage>, ParquetError> {
    let page_header = read_page(reader, start, length, buffer, data)?;

    let result = finish_page(page_header, data, compression, &None, descriptor, None)?;
    match result {
        FinishedPage::Data(_) => Err(ParquetError::OutOfSpec(
            "The first page is not a dictionary page but it should".to_string(),
        )),
        FinishedPage::Dict(dict) => Ok(dict),
    }
}

impl<R: Read + Seek> IndexedPageReader<R> {
    pub fn new(
        reader: R,
        compression: Compression,
        descriptor: Descriptor,
        column_start: u64,
        pages: Vec<FilteredPage>,
        buffer: Vec<u8>,
        data_buffer: Vec<u8>,
    ) -> Self {
        // a dictionary page exists iff the first data page is not at the start of
        // the column
        let dictionary = match pages.get(0) {
            Some(page) => {
                let length = (page.start() - column_start) as usize;
                if length > 0 {
                    Some(LazyDict::Range(column_start, length))
                } else {
                    None
                }
            }
            None => None,
        };

        let pages = pages.into_iter().collect();
        Self {
            reader,
            compression,
            descriptor,
            buffer,
            data_buffer,
            pages,
            dictionary,
        }
    }

    /// consumes self into the reader and the two internal buffers
    pub fn into_inner(self) -> (R, Vec<u8>, Vec<u8>) {
        (self.reader, self.buffer, self.data_buffer)
    }

    fn read_page(
        &mut self,
        start: u64,
        length: usize,
        rows: (usize, usize),
    ) -> Result<FinishedPage, ParquetError> {
        // it will be read - take buffer
        let mut data = std::mem::take(&mut self.data_buffer);

        // read the dictionary if needed
        let dict = self
            .dictionary
            .as_mut()
            .map(|dict| match &dict {
                LazyDict::Dictionary(dict) => Ok(dict.clone()),
                LazyDict::Range(start, length) => {
                    let maybe_page = read_dict_page(
                        &mut self.reader,
                        *start,
                        *length,
                        &mut self.buffer,
                        &mut data,
                        self.compression,
                        &self.descriptor,
                    );

                    match maybe_page {
                        Ok(d) => {
                            *dict = LazyDict::Dictionary(d.clone());
                            Ok(d)
                        }
                        Err(e) => Err(e),
                    }
                }
            })
            .transpose()?;

        let page_header = read_page(&mut self.reader, start, length, &mut self.buffer, &mut data)?;

        finish_page(
            page_header,
            &mut data,
            self.compression,
            &dict,
            &self.descriptor,
            Some(rows),
        )
    }
}

impl<R: Read + Seek> Iterator for IndexedPageReader<R> {
    type Item = Result<CompressedDataPage, ParquetError>;

    fn next(&mut self) -> Option<Self::Item> {
        // todo: check if the first page is a dictionary page and read it accordingly so that
        // we can attach it to data pages

        if let Some(page) = self.pages.pop_front() {
            match page {
                FilteredPage::Select {
                    start,
                    length,
                    rows_offset,
                    rows_length,
                } => {
                    let page = match self.read_page(start, length, (rows_offset, rows_length)) {
                        Err(e) => return Some(Err(e)),
                        Ok(header) => header,
                    };
                    match page {
                        FinishedPage::Data(page) => Some(Ok(page)),
                        FinishedPage::Dict(_) => Some(Err(ParquetError::OutOfSpec(
                            "Dictionary pages cannot be selected via indexes".to_string(),
                        ))),
                    }
                }
                FilteredPage::Skip { .. } => self.next(),
            }
        } else {
            None
        }
    }
}
