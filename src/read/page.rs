use std::sync::Arc;

use parquet_format::{CompressionCodec, DataPageHeader, DataPageHeaderV2};

use super::page_dict::PageDict;

/// a Page in the V1 of the format.
/// `buffer` is compressed.
#[derive(Debug)]
pub struct PageV1 {
    pub buffer: Vec<u8>,
    pub header: DataPageHeader,
    pub compression: CompressionCodec,
    pub uncompressed_page_size: usize,
    pub dictionary_page: Option<Arc<dyn PageDict>>,
    //statistics: Option<Statistics>,
}

#[derive(Debug)]
pub struct PageV2 {
    pub buffer: Vec<u8>,
    pub header: DataPageHeaderV2,
    pub compression: CompressionCodec,
    pub uncompressed_page_size: usize,
    pub dictionary_page: Option<Arc<dyn PageDict>>,
    //statistics: Option<Statistics>,
}

/// A [`Page`] is an uncompressed, encoded representation of a Parquet page. It holds actual data
/// and thus cloning it is expensive.
/// Parquet has two page versions, which this enum accounts for.
#[derive(Debug)]
pub enum Page {
    V1(PageV1),
    V2(PageV2),
}

impl Page {
    pub fn dictionary_page(&self) -> Option<&dyn PageDict> {
        match self {
            Page::V1(page) => page.dictionary_page.as_ref().map(|x| x.as_ref()),
            Page::V2(page) => page.dictionary_page.as_ref().map(|x| x.as_ref()),
        }
    }

    pub fn compression(&self) -> (CompressionCodec, usize) {
        match self {
            Page::V1(page) => (page.compression, page.uncompressed_page_size),
            Page::V2(page) => (page.compression, page.uncompressed_page_size),
        }
    }
}
