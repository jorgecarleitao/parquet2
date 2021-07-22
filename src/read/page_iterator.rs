use std::{io::Read, sync::Arc};

use parquet_format::{CompressionCodec, PageHeader as ParquetPageHeader, PageType};
use thrift::protocol::TCompactInputProtocol;

use crate::error::Result;
use crate::metadata::ColumnDescriptor;

use super::page::CompressedPage;
use super::page::PageHeader;
use super::page_dict::{read_page_dict, PageDict};

/// A page iterator iterates over row group's pages. In parquet, pages are guaranteed to be
/// contiguously arranged in memory and therefore must be read in sequence.
#[derive(Debug)]
pub struct PageIterator<'a, R: Read> {
    // The source
    reader: &'a mut R,

    compression: CompressionCodec,

    // The number of values we have seen so far.
    seen_num_values: i64,

    // The number of total values in this column chunk.
    total_num_values: i64,

    // Arc: it will be shared between multiple pages and pages should be Send + Sync.
    current_dictionary: Option<Arc<dyn PageDict>>,

    descriptor: ColumnDescriptor,

    // The currently allocated buffer.
    pub(crate) buffer: Vec<u8>,
}

impl<'a, R: Read> PageIterator<'a, R> {
    pub fn new(
        reader: &'a mut R,
        total_num_values: i64,
        compression: CompressionCodec,
        descriptor: ColumnDescriptor,
        buffer: Vec<u8>,
    ) -> Self {
        Self {
            reader,
            total_num_values,
            compression,
            seen_num_values: 0,
            current_dictionary: None,
            descriptor,
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
}

impl<'a, R: Read> Iterator for PageIterator<'a, R> {
    type Item = Result<CompressedPage>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut buffer = std::mem::take(&mut self.buffer);
        let result = next_page(self, &mut buffer).transpose();
        if result.is_none() {
            // if no page, we take back the buffer
            self.buffer = std::mem::take(&mut buffer);
        }
        result
    }
}

/// This function is lightweight and executes a minimal amount of work so that it is IO bounded.
// Any un-necessary CPU-intensive tasks SHOULD be executed on individual pages.
fn next_page<R: Read>(
    reader: &mut PageIterator<R>,
    buffer: &mut Vec<u8>,
) -> Result<Option<CompressedPage>> {
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
) -> Result<Option<CompressedPage>> {
    let page_header = reader.read_page_header()?;

    let read_size = page_header.compressed_page_size as usize;
    if read_size > 0 {
        buffer.resize(read_size, 0);
        reader.reader.read_exact(buffer)?;
    }

    match page_header.type_ {
        PageType::DictionaryPage => {
            let dict_header = page_header.dictionary_page_header.as_ref().unwrap();
            let is_sorted = dict_header.is_sorted.unwrap_or(false);

            let page = read_page_dict(
                buffer,
                dict_header.num_values as u32,
                (
                    reader.compression,
                    page_header.uncompressed_page_size as usize,
                ),
                is_sorted,
                reader.descriptor.physical_type(),
            )?;

            reader.current_dictionary = Some(page);
            Ok(None)
        }
        PageType::DataPage => {
            let header = page_header.data_page_header.unwrap();
            reader.seen_num_values += header.num_values as i64;

            Ok(Some(CompressedPage::new(
                PageHeader::V1(header),
                std::mem::take(buffer),
                reader.compression,
                page_header.uncompressed_page_size as usize,
                reader.current_dictionary.clone(),
                reader.descriptor.clone(),
            )))
        }
        PageType::DataPageV2 => {
            let header = page_header.data_page_header_v2.unwrap();
            reader.seen_num_values += header.num_values as i64;

            Ok(Some(CompressedPage::new(
                PageHeader::V2(header),
                std::mem::take(buffer),
                reader.compression,
                page_header.uncompressed_page_size as usize,
                reader.current_dictionary.clone(),
                reader.descriptor.clone(),
            )))
        }
        PageType::IndexPage => Ok(None),
    }
}
