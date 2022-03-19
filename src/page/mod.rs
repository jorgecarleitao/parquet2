mod page_dict;
pub use page_dict::*;

use std::sync::Arc;

pub use parquet_format_async_temp::{
    DataPageHeader as DataPageHeaderV1, DataPageHeaderV2, PageHeader as ParquetPageHeader,
};

pub use crate::parquet_bridge::{DataPageHeaderExt, PageType};

use crate::compression::Compression;
use crate::encoding::{get_length, Encoding};
use crate::error::Result;
use crate::metadata::Descriptor;

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
    pub(crate) descriptor: Descriptor,

    // The offset and length in rows
    rows: Option<(usize, usize)>,
}

impl CompressedDataPage {
    pub fn new(
        header: DataPageHeader,
        buffer: Vec<u8>,
        compression: Compression,
        uncompressed_page_size: usize,
        dictionary_page: Option<Arc<dyn DictPage>>,
        descriptor: Descriptor,
        rows: Option<(usize, usize)>,
    ) -> Self {
        Self {
            header,
            buffer,
            compression,
            uncompressed_page_size,
            dictionary_page,
            descriptor,
            rows,
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

    /// The compression of the data in this page.
    /// Note that what is compressed in a page depends on its version:
    /// in V1, the whole data (`[repetition levels][definition levels][values]`) is compressed; in V2 only the values are compressed.
    pub fn compression(&self) -> Compression {
        self.compression
    }

    /// the rows to be selected by this page.
    /// When `None`, all rows are to be considered.
    pub fn rows(&self) -> Option<(usize, usize)> {
        self.rows
    }

    pub fn num_values(&self) -> usize {
        self.header.num_values()
    }

    /// Decodes the raw statistics into a statistics
    pub fn statistics(&self) -> Option<Result<Arc<dyn Statistics>>> {
        match &self.header {
            DataPageHeader::V1(d) => d
                .statistics
                .as_ref()
                .map(|x| deserialize_statistics(x, self.descriptor.primitive_type.clone())),
            DataPageHeader::V2(d) => d
                .statistics
                .as_ref()
                .map(|x| deserialize_statistics(x, self.descriptor.primitive_type.clone())),
        }
    }
}

#[derive(Debug, Clone)]
pub enum DataPageHeader {
    V1(DataPageHeaderV1),
    V2(DataPageHeaderV2),
}

impl DataPageHeader {
    pub fn num_values(&self) -> usize {
        match &self {
            DataPageHeader::V1(d) => d.num_values as usize,
            DataPageHeader::V2(d) => d.num_values as usize,
        }
    }
}

/// A [`DataPage`] is an uncompressed, encoded representation of a Parquet data page. It holds actual data
/// and thus cloning it is expensive.
#[derive(Debug, Clone)]
pub struct DataPage {
    pub(super) header: DataPageHeader,
    pub(super) buffer: Vec<u8>,
    pub(super) dictionary_page: Option<Arc<dyn DictPage>>,
    pub descriptor: Descriptor,
}

impl DataPage {
    pub fn new(
        header: DataPageHeader,
        buffer: Vec<u8>,
        dictionary_page: Option<Arc<dyn DictPage>>,
        descriptor: Descriptor,
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

    /// Returns a mutable reference to the internal buffer.
    /// Useful to recover the buffer after the page has been decoded.
    pub fn buffer_mut(&mut self) -> &mut Vec<u8> {
        &mut self.buffer
    }

    pub fn num_values(&self) -> usize {
        self.header.num_values()
    }

    pub fn encoding(&self) -> Encoding {
        match &self.header {
            DataPageHeader::V1(d) => d.encoding(),
            DataPageHeader::V2(d) => d.encoding(),
        }
    }

    pub fn definition_level_encoding(&self) -> Encoding {
        match &self.header {
            DataPageHeader::V1(d) => d.definition_level_encoding(),
            DataPageHeader::V2(_) => Encoding::Rle,
        }
    }

    pub fn repetition_level_encoding(&self) -> Encoding {
        match &self.header {
            DataPageHeader::V1(d) => d.repetition_level_encoding(),
            DataPageHeader::V2(_) => Encoding::Rle,
        }
    }

    /// Decodes the raw statistics into a statistics
    pub fn statistics(&self) -> Option<Result<Arc<dyn Statistics>>> {
        match &self.header {
            DataPageHeader::V1(d) => d
                .statistics
                .as_ref()
                .map(|x| deserialize_statistics(x, self.descriptor.primitive_type.clone())),
            DataPageHeader::V2(d) => d
                .statistics
                .as_ref()
                .map(|x| deserialize_statistics(x, self.descriptor.primitive_type.clone())),
        }
    }
}

/// A [`Page`] is an uncompressed, encoded representation of a Parquet page. It may hold actual data
/// and thus cloning it may be expensive.
#[derive(Debug)]
pub enum Page {
    Data(DataPage),
    Dict(Arc<dyn DictPage>),
}

/// A [`EncodedPage`] is an uncompressed, encoded representation of a Parquet page. It may hold actual data
/// and thus cloning it may be expensive.
#[derive(Debug)]
pub enum EncodedPage {
    Data(DataPage),
    Dict(EncodedDictPage),
}

/// A [`CompressedPage`] is a compressed, encoded representation of a Parquet page. It holds actual data
/// and thus cloning it is expensive.
#[derive(Debug)]
pub enum CompressedPage {
    Data(CompressedDataPage),
    Dict(CompressedDictPage),
}

impl CompressedPage {
    pub(crate) fn buffer(&mut self) -> &mut Vec<u8> {
        match self {
            CompressedPage::Data(page) => &mut page.buffer,
            CompressedPage::Dict(page) => &mut page.buffer,
        }
    }
}

/// Splits the page buffer into 3 slices corresponding to (encoded rep levels, encoded def levels, encoded values) for v1 pages.
#[inline]
pub fn split_buffer_v1(buffer: &[u8], has_rep: bool, has_def: bool) -> (&[u8], &[u8], &[u8]) {
    let (rep, buffer) = if has_rep {
        let level_buffer_length = get_length(buffer) as usize;
        (
            &buffer[4..4 + level_buffer_length],
            &buffer[4 + level_buffer_length..],
        )
    } else {
        (&[] as &[u8], buffer)
    };

    let (def, buffer) = if has_def {
        let level_buffer_length = get_length(buffer) as usize;
        (
            &buffer[4..4 + level_buffer_length],
            &buffer[4 + level_buffer_length..],
        )
    } else {
        (&[] as &[u8], buffer)
    };

    (rep, def, buffer)
}

/// Splits the page buffer into 3 slices corresponding to (encoded rep levels, encoded def levels, encoded values) for v2 pages.
pub fn split_buffer_v2(
    buffer: &[u8],
    rep_level_buffer_length: usize,
    def_level_buffer_length: usize,
) -> (&[u8], &[u8], &[u8]) {
    (
        &buffer[..rep_level_buffer_length],
        &buffer[rep_level_buffer_length..rep_level_buffer_length + def_level_buffer_length],
        &buffer[rep_level_buffer_length + def_level_buffer_length..],
    )
}

/// Splits the page buffer into 3 slices corresponding to (encoded rep levels, encoded def levels, encoded values).
pub fn split_buffer(page: &DataPage) -> (&[u8], &[u8], &[u8]) {
    match page.header() {
        DataPageHeader::V1(_) => split_buffer_v1(
            page.buffer(),
            page.descriptor.max_rep_level > 0,
            page.descriptor.max_def_level > 0,
        ),
        DataPageHeader::V2(header) => {
            let def_level_buffer_length = header.definition_levels_byte_length as usize;
            let rep_level_buffer_length = header.repetition_levels_byte_length as usize;
            split_buffer_v2(
                page.buffer(),
                rep_level_buffer_length,
                def_level_buffer_length,
            )
        }
    }
}
