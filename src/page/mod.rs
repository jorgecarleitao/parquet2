mod page_dict;
pub use page_dict::*;

use std::convert::TryInto;
use std::sync::Arc;

pub use parquet_format_async_temp::{
    DataPageHeader as DataPageHeaderV1, DataPageHeaderV2, PageHeader as ParquetPageHeader,
};

pub use crate::parquet_bridge::{DataPageHeaderExt, PageType};

use crate::compression::Compression;
use crate::encoding::Encoding;
use crate::error::Result;
use crate::metadata::ColumnDescriptor;

use crate::statistics::{deserialize_statistics, Statistics};

/// A [`CompressedDataPage`] is compressed, encoded representation of a Parquet data page.
/// It holds actual data and thus cloning it is expensive.
#[derive(Debug)]
pub struct CompressedDataPage {
    pub(crate) header: DataPageHeader,
    pub(crate) buffer: Vec<u8>,
    compression: Compression,
    uncompressed_page_size: usize,
    pub(crate) dictionary_page: Option<Arc<dyn DictPage>>,
    pub(crate) descriptor: ColumnDescriptor,
}

impl CompressedDataPage {
    pub fn new(
        header: DataPageHeader,
        buffer: Vec<u8>,
        compression: Compression,
        uncompressed_page_size: usize,
        dictionary_page: Option<Arc<dyn DictPage>>,
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

    pub fn header(&self) -> &DataPageHeader {
        &self.header
    }

    pub fn uncompressed_size(&self) -> usize {
        self.uncompressed_page_size
    }

    pub fn compressed_size(&self) -> usize {
        self.buffer.len()
    }

    pub fn compression(&self) -> Compression {
        self.compression
    }

    pub fn num_values(&self) -> usize {
        match &self.header {
            DataPageHeader::V1(d) => d.num_values as usize,
            DataPageHeader::V2(d) => d.num_values as usize,
        }
    }

    /// Decodes the raw statistics into a statistics
    pub fn statistics(&self) -> Option<Result<Arc<dyn Statistics>>> {
        match &self.header {
            DataPageHeader::V1(d) => d
                .statistics
                .as_ref()
                .map(|x| deserialize_statistics(x, self.descriptor().clone())),
            DataPageHeader::V2(d) => d
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
pub enum DataPageHeader {
    V1(DataPageHeaderV1),
    V2(DataPageHeaderV2),
}

/// A [`DataPage`] is an uncompressed, encoded representation of a Parquet data page. It holds actual data
/// and thus cloning it is expensive.
#[derive(Debug, Clone)]
pub struct DataPage {
    header: DataPageHeader,
    pub(super) buffer: Vec<u8>,
    dictionary_page: Option<Arc<dyn DictPage>>,
    descriptor: ColumnDescriptor,
}

impl DataPage {
    pub fn new(
        header: DataPageHeader,
        buffer: Vec<u8>,
        dictionary_page: Option<Arc<dyn DictPage>>,
        descriptor: ColumnDescriptor,
    ) -> Self {
        Self {
            header,
            buffer,
            dictionary_page,
            descriptor,
        }
    }

    pub fn header(&self) -> &DataPageHeader {
        &self.header
    }

    pub fn dictionary_page(&self) -> Option<&Arc<dyn DictPage>> {
        self.dictionary_page.as_ref()
    }

    pub fn buffer(&self) -> &[u8] {
        &self.buffer
    }

    pub fn num_values(&self) -> usize {
        match &self.header {
            DataPageHeader::V1(d) => d.num_values as usize,
            DataPageHeader::V2(d) => d.num_values as usize,
        }
    }

    pub fn encoding(&self) -> Encoding {
        match &self.header {
            DataPageHeader::V1(d) => d.encoding.try_into().unwrap(),
            DataPageHeader::V2(d) => d.encoding.try_into().unwrap(),
        }
    }

    /// Decodes the raw statistics into a statistics
    pub fn statistics(&self) -> Option<Result<Arc<dyn Statistics>>> {
        match &self.header {
            DataPageHeader::V1(d) => d
                .statistics
                .as_ref()
                .map(|x| deserialize_statistics(x, self.descriptor().clone())),
            DataPageHeader::V2(d) => d
                .statistics
                .as_ref()
                .map(|x| deserialize_statistics(x, self.descriptor().clone())),
        }
    }

    pub fn descriptor(&self) -> &ColumnDescriptor {
        &self.descriptor
    }
}

/// A [`Page`] is an uncompressed, encoded representation of a Parquet page. It may hold actual data
/// and thus cloning it may be expensive.
#[derive(Debug)]
pub enum Page {
    Data(DataPage),
    Dict(Arc<dyn DictPage>),
}

/// A [`CompressedPage`] is a compressed, encoded representation of a Parquet page. It holds actual data
/// and thus cloning it is expensive.
#[derive(Debug)]
pub enum CompressedPage {
    Data(CompressedDataPage),
    Dict(CompressedDictPage),
}

// read: CompressedPage -> Page
// write: Page -> CompressedPage
