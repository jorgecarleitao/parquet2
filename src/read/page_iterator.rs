use std::convert::TryInto;
use std::{io::Read, sync::Arc};

use parquet_format_async_temp::thrift::protocol::TCompactInputProtocol;

use crate::compression::Compression;
use crate::error::Result;
use crate::metadata::ColumnDescriptor;

use crate::page::{
    read_dict_page, CompressedDataPage, DataPageHeader, DictPage, EncodedDictPage, PageType,
    ParquetPageHeader,
};

/// Type declaration for a page filter
pub type PageFilter = Arc<dyn Fn(&ColumnDescriptor, &DataPageHeader) -> bool + Send + Sync>;

/// A page iterator iterates over row group's pages. In parquet, pages are guaranteed to be
/// contiguously arranged in memory and therefore must be read in sequence.
pub struct PageIterator<'a, R: Read> {
    // The source
    reader: &'a mut R,

    compression: Compression,

    // The number of values we have seen so far.
    seen_num_values: i64,

    // The number of total values in this column chunk.
    total_num_values: i64,

    // Arc: it will be shared between multiple pages and pages should be Send + Sync.
    current_dictionary: Option<Arc<dyn DictPage>>,

    pages_filter: PageFilter,

    descriptor: ColumnDescriptor,

    // The currently allocated buffer.
    pub(crate) buffer: Vec<u8>,
}

impl<'a, R: Read> PageIterator<'a, R> {
    pub fn new(
        reader: &'a mut R,
        total_num_values: i64,
        compression: Compression,
        descriptor: ColumnDescriptor,
        pages_filter: PageFilter,
        buffer: Vec<u8>,
    ) -> Self {
        Self {
            reader,
            total_num_values,
            compression,
            seen_num_values: 0,
            current_dictionary: None,
            descriptor,
            pages_filter,
            buffer,
        }
    }

    /// Reads Page header from Thrift.
    fn read_page_header(&mut self) -> Result<ParquetPageHeader> {
        let mut prot = TCompactInputProtocol::new(&mut self.reader);
        let page_header = ParquetPageHeader::read_from_in_protocol(&mut prot)?;
        Ok(page_header)
    }

    pub fn reuse_buffer(&mut self, buffer: Vec<u8>) {
        self.buffer = buffer;
    }

    pub fn into_buffer(self) -> Vec<u8> {
        self.buffer
    }
}

impl<'a, R: Read> Iterator for PageIterator<'a, R> {
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

/// This function is lightweight and executes a minimal amount of work so that it is IO bounded.
// Any un-necessary CPU-intensive tasks SHOULD be executed on individual pages.
fn next_page<R: Read>(
    reader: &mut PageIterator<R>,
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

fn build_page<R: Read>(
    reader: &mut PageIterator<R>,
    buffer: &mut Vec<u8>,
) -> Result<Option<CompressedDataPage>> {
    let page_header = reader.read_page_header()?;
    reader.seen_num_values += get_page_header(&page_header)
        .map(|x| x.num_values() as i64)
        .unwrap_or_default();

    let read_size = page_header.compressed_page_size as usize;
    if read_size > 0 {
        buffer.clear();
        buffer.resize(read_size, 0);
        reader.reader.read_exact(buffer)?;
    }

    let result = finish_page(
        page_header,
        buffer,
        reader.compression,
        &reader.current_dictionary,
        &reader.descriptor,
    )?;

    match result {
        FinishedPage::Data(page) => Ok(Some(page)),
        FinishedPage::Dict(dict) => {
            reader.current_dictionary = Some(dict);
            Ok(None)
        }
        _ => Ok(None),
    }
}

pub(super) enum FinishedPage {
    Data(CompressedDataPage),
    Dict(Arc<dyn DictPage>),
    Index,
}

pub(super) fn finish_page(
    page_header: ParquetPageHeader,
    buffer: &mut Vec<u8>,
    compression: Compression,
    current_dictionary: &Option<Arc<dyn DictPage>>,
    descriptor: &ColumnDescriptor,
) -> Result<FinishedPage> {
    let type_ = page_header.type_.try_into()?;
    match type_ {
        PageType::DictionaryPage => {
            let dict_header = page_header.dictionary_page_header.as_ref().unwrap();
            let is_sorted = dict_header.is_sorted.unwrap_or(false);

            // move the buffer to `dict_page`
            let mut dict_page =
                EncodedDictPage::new(std::mem::take(buffer), dict_header.num_values as usize);

            let page = read_dict_page(
                &dict_page,
                (compression, page_header.uncompressed_page_size as usize),
                is_sorted,
                descriptor.physical_type(),
            )?;
            // take the buffer out of the `dict_page` to re-use it
            std::mem::swap(&mut dict_page.buffer, buffer);

            Ok(FinishedPage::Dict(page))
        }
        PageType::DataPage => {
            let header = page_header.data_page_header.unwrap();

            Ok(FinishedPage::Data(CompressedDataPage::new(
                DataPageHeader::V1(header),
                std::mem::take(buffer),
                compression,
                page_header.uncompressed_page_size as usize,
                current_dictionary.clone(),
                descriptor.clone(),
            )))
        }
        PageType::DataPageV2 => {
            let header = page_header.data_page_header_v2.unwrap();

            Ok(FinishedPage::Data(CompressedDataPage::new(
                DataPageHeader::V2(header),
                std::mem::take(buffer),
                compression,
                page_header.uncompressed_page_size as usize,
                current_dictionary.clone(),
                descriptor.clone(),
            )))
        }
        PageType::IndexPage => Ok(FinishedPage::Index),
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
