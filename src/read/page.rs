use std::sync::Arc;

use parquet_format::{CompressionCodec, DataPageHeaderV2, Encoding};

use super::page_dict::PageDict;

/// a Page in the V1 of the format.
/// `buf` is compressed.
#[derive(Debug)]
pub struct PageV1 {
    pub buf: Vec<u8>,
    pub num_values: u32,
    pub encoding: Encoding,
    pub compression: (CompressionCodec, usize),
    pub def_level_encoding: Encoding,
    pub rep_level_encoding: Encoding,
    pub dictionary_page: Option<Arc<dyn PageDict>>,
    //statistics: Option<Statistics>,
}

impl PageV1 {
    pub fn new(
        buf: Vec<u8>,
        num_values: u32,
        encoding: Encoding,
        compression: (CompressionCodec, usize),
        def_level_encoding: Encoding,
        rep_level_encoding: Encoding,
        dictionary_page: Option<Arc<dyn PageDict>>,
    ) -> Self {
        Self {
            buf,
            num_values,
            encoding,
            compression,
            def_level_encoding,
            rep_level_encoding,
            dictionary_page,
        }
    }
}

#[derive(Debug)]
pub struct PageV2 {
    pub buf: Vec<u8>,
    pub header: DataPageHeaderV2,
    pub compression: (CompressionCodec, usize),
    pub dictionary_page: Option<Arc<dyn PageDict>>,
    //statistics: Option<Statistics>,
}

impl PageV2 {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        buf: Vec<u8>,
        header: DataPageHeaderV2,
        compression: (CompressionCodec, usize),
        dictionary_page: Option<Arc<dyn PageDict>>,
    ) -> Self {
        Self {
            buf,
            header,
            compression,
            dictionary_page,
        }
    }
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

    pub fn compression(&self) -> &(CompressionCodec, usize) {
        match self {
            Page::V1(page) => &page.compression,
            Page::V2(page) => &page.compression,
        }
    }
}
