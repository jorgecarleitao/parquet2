use std::convert::TryInto;
use std::{io::Read, sync::Arc};

use parquet_format_async_temp::thrift::protocol::TCompactInputProtocol;

use crate::compression::Compression;
use crate::error::Result;
use crate::metadata::{ColumnChunkMetaData, Descriptor};

use crate::page::{
    read_dict_page, CompressedDataPage, DataPageHeader, DictPage, EncodedDictPage, PageType,
    ParquetPageHeader,
};

use super::PageIterator;

/// Type declaration for a page filter
pub type PageFilter = Arc<dyn Fn(&Descriptor, &DataPageHeader) -> bool + Send + Sync>;

/// A fallible [`Iterator`] of [`CompressedDataPage`]. This iterator reads pages back
/// to back until all pages have been consumed.
/// The pages from this iterator always have [`None`] [`CompressedDataPage::rows()`] since
/// filter pushdown is not supported without a
/// pre-computed [page index](https://github.com/apache/parquet-format/blob/master/PageIndex.md).
pub struct PageReader<R: Read> {
    // The source
    reader: R,

    compression: Compression,

    // The number of values we have seen so far.
    seen_num_values: i64,

    // The number of total values in this column chunk.
    total_num_values: i64,

    // Arc: it will be shared between multiple pages and pages should be Send + Sync.
    current_dictionary: Option<Arc<dyn DictPage>>,

    pages_filter: PageFilter,

    descriptor: Descriptor,

    // The currently allocated buffer.
    pub(crate) buffer: Vec<u8>,
}

impl<R: Read> PageReader<R> {
    /// Returns a new [`PageReader`].
    ///
    /// It assumes that the reader has been `seeked` to the beginning of `column`.
    pub fn new(
        reader: R,
        column: &ColumnChunkMetaData,
        pages_filter: PageFilter,
        buffer: Vec<u8>,
    ) -> Self {
        Self {
            reader,
            total_num_values: column.num_values(),
            compression: column.compression(),
            seen_num_values: 0,
            current_dictionary: None,
            descriptor: column.descriptor().descriptor.clone(),
            pages_filter,
            buffer,
        }
    }

    pub fn into_inner(self) -> (R, Vec<u8>) {
        (self.reader, self.buffer)
    }
}

impl<R: Read> PageIterator for PageReader<R> {
    fn swap_buffer(&mut self, buffer: &mut Vec<u8>) {
        std::mem::swap(&mut self.buffer, buffer)
    }
}

impl<R: Read> Iterator for PageReader<R> {
    type Item = Result<CompressedDataPage>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut buffer = std::mem::take(&mut self.buffer);
        let maybe_maybe_page = next_page(self, &mut buffer).transpose();
        if let Some(ref maybe_page) = maybe_maybe_page {
            if let Ok(page) = maybe_page {
                // check if we should filter it
                let to_consume = (self.pages_filter)(&self.descriptor, page.header());
                if !to_consume {
                    self.buffer = std::mem::take(&mut buffer);
                    return self.next();
                }
            }
        } else {
            // no page => we take back the buffer
            self.buffer = std::mem::take(&mut buffer);
        }
        maybe_maybe_page
    }
}

/// Reads Page header from Thrift.
pub(super) fn read_page_header<R: Read>(reader: &mut R) -> Result<ParquetPageHeader> {
    let mut prot = TCompactInputProtocol::new(reader);
    let page_header = ParquetPageHeader::read_from_in_protocol(&mut prot)?;
    Ok(page_header)
}

/// This function is lightweight and executes a minimal amount of work so that it is IO bounded.
// Any un-necessary CPU-intensive tasks SHOULD be executed on individual pages.
fn next_page<R: Read>(
    reader: &mut PageReader<R>,
    buffer: &mut Vec<u8>,
) -> Result<Option<CompressedDataPage>> {
    let total_values = reader.total_num_values;
    let mut seen_values = reader.seen_num_values;
    if seen_values >= total_values {
        return Ok(None);
    };

    while seen_values < total_values {
        let page = build_page(reader, buffer)?;
        seen_values = reader.seen_num_values;
        if let Some(page) = page {
            return Ok(Some(page));
        }
    }
    Ok(None)
}

pub(super) fn build_page<R: Read>(
    reader: &mut PageReader<R>,
    buffer: &mut Vec<u8>,
) -> Result<Option<CompressedDataPage>> {
    let page_header = read_page_header(&mut reader.reader)?;
    reader.seen_num_values += get_page_header(&page_header)
        .map(|x| x.num_values() as i64)
        .unwrap_or_default();

    let read_size = page_header.compressed_page_size as usize;
    if read_size > 0 {
        if read_size > buffer.len() {
            // dealloc and ignore region, replacing it by a new region
            *buffer = vec![0; read_size]
        } else {
            buffer.truncate(read_size);
        }
        reader.reader.read_exact(buffer)?;
    }

    let result = finish_page(
        page_header,
        buffer,
        reader.compression,
        &reader.current_dictionary,
        &reader.descriptor,
        None,
    )?;

    match result {
        FinishedPage::Data(page) => Ok(Some(page)),
        FinishedPage::Dict(dict) => {
            reader.current_dictionary = Some(dict);
            Ok(None)
        }
    }
}

pub(super) enum FinishedPage {
    Data(CompressedDataPage),
    Dict(Arc<dyn DictPage>),
}

pub(super) fn finish_page(
    page_header: ParquetPageHeader,
    data: &mut Vec<u8>,
    compression: Compression,
    current_dictionary: &Option<Arc<dyn DictPage>>,
    descriptor: &Descriptor,
    rows: Option<(usize, usize)>,
) -> Result<FinishedPage> {
    let type_ = page_header.type_.try_into()?;
    match type_ {
        PageType::DictionaryPage => {
            let dict_header = page_header.dictionary_page_header.as_ref().unwrap();
            let is_sorted = dict_header.is_sorted.unwrap_or(false);

            // move the buffer to `dict_page`
            let mut dict_page =
                EncodedDictPage::new(std::mem::take(data), dict_header.num_values as usize);

            let page = read_dict_page(
                &dict_page,
                (compression, page_header.uncompressed_page_size as usize),
                is_sorted,
                descriptor.primitive_type.physical_type,
            )?;
            // take the buffer out of the `dict_page` to re-use it
            std::mem::swap(&mut dict_page.buffer, data);

            Ok(FinishedPage::Dict(page))
        }
        PageType::DataPage => {
            let header = page_header.data_page_header.unwrap();

            Ok(FinishedPage::Data(CompressedDataPage::new(
                DataPageHeader::V1(header),
                std::mem::take(data),
                compression,
                page_header.uncompressed_page_size as usize,
                current_dictionary.clone(),
                descriptor.clone(),
                rows,
            )))
        }
        PageType::DataPageV2 => {
            let header = page_header.data_page_header_v2.unwrap();

            Ok(FinishedPage::Data(CompressedDataPage::new(
                DataPageHeader::V2(header),
                std::mem::take(data),
                compression,
                page_header.uncompressed_page_size as usize,
                current_dictionary.clone(),
                descriptor.clone(),
                rows,
            )))
        }
    }
}

pub(super) fn get_page_header(header: &ParquetPageHeader) -> Option<DataPageHeader> {
    let type_ = header.type_.try_into().unwrap();
    match type_ {
        PageType::DataPage => {
            let header = header.data_page_header.clone().unwrap();
            Some(DataPageHeader::V1(header))
        }
        PageType::DataPageV2 => {
            let header = header.data_page_header_v2.clone().unwrap();
            Some(DataPageHeader::V2(header))
        }
        _ => None,
    }
}
