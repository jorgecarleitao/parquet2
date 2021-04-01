use std::sync::Arc;

use parquet_format::Encoding;

#[derive(Debug)]
pub struct PageV1 {
    pub buf: Vec<u8>,
    pub num_values: u32,
    pub encoding: Encoding,
    pub def_level_encoding: Encoding,
    pub rep_level_encoding: Encoding,
    pub dictionary_page: Option<Arc<PageDict>>,
    //statistics: Option<Statistics>,
}

impl PageV1 {
    pub fn new(
        buf: Vec<u8>,
        num_values: u32,
        encoding: Encoding,
        def_level_encoding: Encoding,
        rep_level_encoding: Encoding,
        dictionary_page: Option<Arc<PageDict>>,
    ) -> Self {
        Self {
            buf,
            num_values,
            encoding,
            def_level_encoding,
            rep_level_encoding,
            dictionary_page,
        }
    }
}

#[derive(Debug)]
pub struct PageV2 {
    pub buf: Vec<u8>,
    pub num_values: u32,
    pub encoding: Encoding,
    pub num_nulls: u32,
    pub num_rows: u32,
    pub def_levels_byte_len: u32,
    pub rep_levels_byte_len: u32,
    pub is_compressed: bool,
    pub dictionary_page: Option<Arc<PageDict>>,
    //statistics: Option<Statistics>,
}

impl PageV2 {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        buf: Vec<u8>,
        num_values: u32,
        encoding: Encoding,
        num_nulls: u32,
        num_rows: u32,
        def_levels_byte_len: u32,
        rep_levels_byte_len: u32,
        is_compressed: bool,
        dictionary_page: Option<Arc<PageDict>>,
    ) -> Self {
        Self {
            buf,
            num_values,
            encoding,
            num_nulls,
            num_rows,
            def_levels_byte_len,
            rep_levels_byte_len,
            is_compressed,
            dictionary_page,
        }
    }
}

// todo: make this decoded on the first read, so that it does not need to be decoded on every
// page. Requires introducing typing to this struct :/
#[derive(Debug)]
pub struct PageDict {
    pub buf: Vec<u8>,
    pub num_values: u32,
    pub encoding: Encoding,
    pub is_sorted: bool,
}

impl PageDict {
    pub fn new(buf: Vec<u8>, num_values: u32, encoding: Encoding, is_sorted: bool) -> Self {
        Self {
            buf,
            num_values,
            encoding,
            is_sorted,
        }
    }
}

/// A page is an uncompressed, encoded representation of a Parquet page. It holds actual data
/// and thus memory operations on it are expensive.
/// Its in-memory representation depends on its type.
#[derive(Debug)]
pub enum Page {
    V1(PageV1),
    V2(PageV2),
}

impl Page {
    pub fn dictionary_page(&self) -> Option<&PageDict> {
        match self {
            Page::V1(page) => page.dictionary_page.as_ref().map(|x| x.as_ref()),
            Page::V2(page) => page.dictionary_page.as_ref().map(|x| x.as_ref()),
        }
    }
}
