use std::sync::Arc;

use parquet_format::{CompressionCodec, DataPageHeader, DataPageHeaderV2};

use super::page_dict::PageDict;
use super::statistics::Statistics;

#[derive(Debug)]
pub struct PageV1 {
    pub buffer: Vec<u8>,
    pub header: DataPageHeader,
    pub compression: CompressionCodec,
    pub uncompressed_page_size: usize,
    pub dictionary_page: Option<Arc<dyn PageDict>>,
    pub statistics: Option<Arc<dyn Statistics>>,
}

#[derive(Debug)]
pub struct PageV2 {
    pub buffer: Vec<u8>,
    pub header: DataPageHeaderV2,
    pub compression: CompressionCodec,
    pub uncompressed_page_size: usize,
    pub dictionary_page: Option<Arc<dyn PageDict>>,
    pub statistics: Option<Arc<dyn Statistics>>,
}

/// A [`CompressedPage`] is compressed, encoded representation of a Parquet page. It holds actual data
/// and thus cloning it is expensive. Favor passing this enum by value, as it deallocates it
/// as soon as it is not needed, thereby reducing memory usage.
#[derive(Debug)]
pub enum CompressedPage {
    V1(PageV1),
    V2(PageV2),
}

impl CompressedPage {
    pub fn compressed_size(&self) -> usize {
        match self {
            Self::V1(page) => page.buffer.len(),
            Self::V2(page) => page.buffer.len(),
        }
    }

    pub fn uncompressed_size(&self) -> usize {
        match self {
            Self::V1(page) => page.uncompressed_page_size,
            Self::V2(page) => page.uncompressed_page_size,
        }
    }

    pub fn statistics(&self) -> Option<&Arc<dyn Statistics>> {
        match self {
            Self::V1(page) => page.statistics.as_ref(),
            Self::V2(page) => page.statistics.as_ref(),
        }
    }
}

/// A [`Page`] is an uncompressed, encoded representation of a Parquet page. It holds actual data
/// and thus cloning it is expensive. Favor passing this enum by value, as it deallocates it
/// as soon as it is not needed, thereby reducing memory usage.
#[derive(Debug)]
pub enum Page {
    V1(PageV1),
    V2(PageV2),
}

impl Page {
    pub fn dictionary_page(&self) -> Option<&dyn PageDict> {
        match self {
            Self::V1(page) => page.dictionary_page.as_ref().map(|x| x.as_ref()),
            Self::V2(page) => page.dictionary_page.as_ref().map(|x| x.as_ref()),
        }
    }
}

// read: CompressedPage -> Page
// write: Page -> CompressedPage
