use std::convert::TryInto;
use std::io::Write;
use std::sync::Arc;

use futures::{AsyncWrite, AsyncWriteExt};
use parquet_format_async_temp::{DictionaryPageHeader, Encoding, PageType};

use crate::error::{ParquetError, Result};
use crate::page::{
    CompressedDataPage, CompressedDictPage, CompressedPage, DataPageHeader, ParquetPageHeader,
};
use crate::statistics::Statistics;
use crate::thrift_io_wrapper::{write_to_thrift, write_to_thrift_async};

fn maybe_bytes(uncompressed: usize, compressed: usize) -> Result<(i32, i32)> {
    let uncompressed_page_size: i32 = uncompressed.try_into().map_err(|_| {
        ParquetError::OutOfSpec(format!(
            "A page can only contain i32::MAX uncompressed bytes. This one contains {}",
            uncompressed
        ))
    })?;

    let compressed_page_size: i32 = compressed.try_into().map_err(|_| {
        ParquetError::OutOfSpec(format!(
            "A page can only contain i32::MAX compressed bytes. This one contains {}",
            compressed
        ))
    })?;

    Ok((uncompressed_page_size, compressed_page_size))
}

/// Contains page write metrics.
pub struct PageWriteSpec {
    pub header: ParquetPageHeader,
    pub header_size: u64,
    pub offset: u64,
    pub bytes_written: u64,
    pub statistics: Option<Arc<dyn Statistics>>,
}

pub fn write_page<W: Write>(
    writer: &mut W,
    offset: u64,
    compressed_page: &CompressedPage,
) -> Result<PageWriteSpec> {
    let header = match &compressed_page {
        CompressedPage::Data(compressed_page) => assemble_data_page_header(compressed_page),
        CompressedPage::Dict(compressed_page) => assemble_dict_page_header(compressed_page),
    }?;

    let header_size = write_to_thrift(&header, writer)? as u64;
    let mut bytes_written = header_size;

    bytes_written += match &compressed_page {
        CompressedPage::Data(compressed_page) => {
            writer.write_all(&compressed_page.buffer)?;
            compressed_page.buffer.len() as u64
        }
        CompressedPage::Dict(compressed_page) => {
            writer.write_all(&compressed_page.buffer)?;
            compressed_page.buffer.len() as u64
        }
    };

    let statistics = match &compressed_page {
        CompressedPage::Data(compressed_page) => compressed_page.statistics().transpose()?,
        CompressedPage::Dict(_) => None,
    };

    Ok(PageWriteSpec {
        header,
        header_size,
        offset,
        bytes_written,
        statistics,
    })
}

pub async fn write_page_async<W: AsyncWrite + Unpin + Send>(
    writer: &mut W,
    offset: u64,
    compressed_page: &CompressedPage,
) -> Result<PageWriteSpec> {
    let header = match &compressed_page {
        CompressedPage::Data(compressed_page) => assemble_data_page_header(compressed_page),
        CompressedPage::Dict(compressed_page) => assemble_dict_page_header(compressed_page),
    }?;

    let header_size = write_to_thrift_async(&header, writer).await? as u64;
    let mut bytes_written = header_size as u64;

    bytes_written += match &compressed_page {
        CompressedPage::Data(compressed_page) => {
            writer.write_all(&compressed_page.buffer).await?;
            compressed_page.buffer.len() as u64
        }
        CompressedPage::Dict(compressed_page) => {
            writer.write_all(&compressed_page.buffer).await?;
            compressed_page.buffer.len() as u64
        }
    };

    let statistics = match &compressed_page {
        CompressedPage::Data(compressed_page) => compressed_page.statistics().transpose()?,
        CompressedPage::Dict(_) => None,
    };

    Ok(PageWriteSpec {
        header,
        header_size,
        offset,
        bytes_written,
        statistics,
    })
}

fn assemble_data_page_header(page: &CompressedDataPage) -> Result<ParquetPageHeader> {
    let (uncompressed_page_size, compressed_page_size) =
        maybe_bytes(page.uncompressed_size(), page.compressed_size())?;

    let mut page_header = ParquetPageHeader {
        type_: match page.header() {
            DataPageHeader::V1(_) => PageType::DATA_PAGE,
            DataPageHeader::V2(_) => PageType::DATA_PAGE_V2,
        },
        uncompressed_page_size,
        compressed_page_size,
        crc: None,
        data_page_header: None,
        index_page_header: None,
        dictionary_page_header: None,
        data_page_header_v2: None,
    };

    match page.header() {
        DataPageHeader::V1(header) => {
            page_header.data_page_header = Some(header.clone());
        }
        DataPageHeader::V2(header) => {
            page_header.data_page_header_v2 = Some(header.clone());
        }
    }
    Ok(page_header)
}

fn assemble_dict_page_header(page: &CompressedDictPage) -> Result<ParquetPageHeader> {
    let (uncompressed_page_size, compressed_page_size) =
        maybe_bytes(page.uncompressed_page_size, page.buffer.len())?;

    let num_values: i32 = page.num_values.try_into().map_err(|_| {
        ParquetError::OutOfSpec(format!(
            "A dictionary page can only contain i32::MAX items. This one contains {}",
            page.num_values
        ))
    })?;

    Ok(ParquetPageHeader {
        type_: PageType::DICTIONARY_PAGE,
        uncompressed_page_size,
        compressed_page_size,
        crc: None,
        data_page_header: None,
        index_page_header: None,
        dictionary_page_header: Some(DictionaryPageHeader {
            num_values,
            encoding: Encoding::PLAIN,
            is_sorted: None,
        }),
        data_page_header_v2: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dict_too_large() {
        let page = CompressedDictPage {
            buffer: vec![],
            uncompressed_page_size: i32::MAX as usize + 1,
            num_values: 100,
        };
        assert!(assemble_dict_page_header(&page).is_err());
    }

    #[test]
    fn dict_too_many_values() {
        let page = CompressedDictPage {
            buffer: vec![],
            uncompressed_page_size: 0,
            num_values: i32::MAX as usize + 1,
        };
        assert!(assemble_dict_page_header(&page).is_err());
    }
}
