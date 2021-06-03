use std::sync::Arc;

use parquet_format::Encoding;
use parquet_format::{CompressionCodec, DataPageHeader, DataPageHeaderV2};

use super::page_dict::PageDict;
use super::statistics::Statistics;

/// A [`CompressedPage`] is compressed, encoded representation of a Parquet page. It holds actual data
/// and thus cloning it is expensive. Favor passing this enum by value, as it deallocates it
/// as soon as it is not needed, thereby reducing memory usage.
#[derive(Debug)]
pub struct CompressedPage {
    pub(crate) header: PageHeader,
    pub(crate) buffer: Vec<u8>,
    compression: CompressionCodec,
    uncompressed_page_size: usize,
    pub(crate) dictionary_page: Option<Arc<dyn PageDict>>,
    pub(crate) statistics: Option<Arc<dyn Statistics>>,
}

impl CompressedPage {
    pub fn new(
        header: PageHeader,
        buffer: Vec<u8>,
        compression: CompressionCodec,
        uncompressed_page_size: usize,
        dictionary_page: Option<Arc<dyn PageDict>>,
        statistics: Option<Arc<dyn Statistics>>,
    ) -> Self {
        Self {
            header,
            buffer,
            compression,
            uncompressed_page_size,
            dictionary_page,
            statistics,
        }
    }

    pub fn header(&self) -> &PageHeader {
        &self.header
    }

    pub fn uncompressed_size(&self) -> usize {
        self.uncompressed_page_size
    }

    pub fn statistics(&self) -> Option<&Arc<dyn Statistics>> {
        self.statistics.as_ref()
    }

    pub fn compressed_size(&self) -> usize {
        self.buffer.len()
    }

    pub fn compression(&self) -> CompressionCodec {
        self.compression
    }

    pub fn num_values(&self) -> usize {
        match &self.header {
            PageHeader::V1(d) => d.num_values as usize,
            PageHeader::V2(d) => d.num_values as usize,
        }
    }
}

#[derive(Debug, Clone)]
pub enum PageHeader {
    V1(DataPageHeader),
    V2(DataPageHeaderV2),
}

/// A [`Page`] is an uncompressed, encoded representation of a Parquet page. It holds actual data
/// and thus cloning it is expensive. Favor passing this enum by value, as it deallocates it
/// as soon as it is not needed, thereby reducing memory usage.
#[derive(Debug, Clone)]
pub struct Page {
    header: PageHeader,
    pub(super) buffer: Vec<u8>,
    dictionary_page: Option<Arc<dyn PageDict>>,
    statistics: Option<Arc<dyn Statistics>>,
}

impl Page {
    pub fn new(
        header: PageHeader,
        buffer: Vec<u8>,
        dictionary_page: Option<Arc<dyn PageDict>>,
        statistics: Option<Arc<dyn Statistics>>,
    ) -> Self {
        Self {
            header,
            buffer,
            dictionary_page,
            statistics,
        }
    }

    pub fn statistics(&self) -> Option<&Arc<dyn Statistics>> {
        self.statistics.as_ref()
    }

    pub fn header(&self) -> &PageHeader {
        &self.header
    }

    pub fn dictionary_page(&self) -> Option<&Arc<dyn PageDict>> {
        self.dictionary_page.as_ref()
    }

    pub fn buffer(&self) -> &[u8] {
        &self.buffer
    }

    pub fn num_values(&self) -> usize {
        match &self.header {
            PageHeader::V1(d) => d.num_values as usize,
            PageHeader::V2(d) => d.num_values as usize,
        }
    }

    pub fn encoding(&self) -> Encoding {
        match &self.header {
            PageHeader::V1(d) => d.encoding,
            PageHeader::V2(d) => d.encoding,
        }
    }
}

// read: CompressedPage -> Page
// write: Page -> CompressedPage
