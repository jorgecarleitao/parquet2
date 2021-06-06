use std::sync::Arc;

use parquet_format::Encoding;
use parquet_format::{CompressionCodec, DataPageHeader, DataPageHeaderV2};

use crate::error::Result;
use crate::metadata::ColumnDescriptor;

use super::page_dict::PageDict;
use crate::statistics::{deserialize_statistics, Statistics};

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
    pub(crate) descriptor: ColumnDescriptor,
}

impl CompressedPage {
    pub fn new(
        header: PageHeader,
        buffer: Vec<u8>,
        compression: CompressionCodec,
        uncompressed_page_size: usize,
        dictionary_page: Option<Arc<dyn PageDict>>,
        descriptor: ColumnDescriptor,
    ) -> Self {
        Self {
            header,
            buffer,
            compression,
            uncompressed_page_size,
            dictionary_page,
            descriptor,
        }
    }

    pub fn header(&self) -> &PageHeader {
        &self.header
    }

    pub fn uncompressed_size(&self) -> usize {
        self.uncompressed_page_size
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

    /// Decodes the raw statistics into a statistics
    pub fn statistics(&self) -> Option<Result<Arc<dyn Statistics>>> {
        match &self.header {
            PageHeader::V1(d) => d
                .statistics
                .as_ref()
                .map(|x| deserialize_statistics(x, self.descriptor().clone())),
            PageHeader::V2(d) => d
                .statistics
                .as_ref()
                .map(|x| deserialize_statistics(x, self.descriptor().clone())),
        }
    }

    pub fn descriptor(&self) -> &ColumnDescriptor {
        &self.descriptor
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
    descriptor: ColumnDescriptor,
}

impl Page {
    pub fn new(
        header: PageHeader,
        buffer: Vec<u8>,
        dictionary_page: Option<Arc<dyn PageDict>>,
        descriptor: ColumnDescriptor,
    ) -> Self {
        Self {
            header,
            buffer,
            dictionary_page,
            descriptor,
        }
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

    /// Decodes the raw statistics into a statistics
    pub fn statistics(&self) -> Option<Result<Arc<dyn Statistics>>> {
        match &self.header {
            PageHeader::V1(d) => d
                .statistics
                .as_ref()
                .map(|x| deserialize_statistics(x, self.descriptor().clone())),
            PageHeader::V2(d) => d
                .statistics
                .as_ref()
                .map(|x| deserialize_statistics(x, self.descriptor().clone())),
        }
    }

    pub fn descriptor(&self) -> &ColumnDescriptor {
        &self.descriptor
    }
}

// read: CompressedPage -> Page
// write: Page -> CompressedPage
