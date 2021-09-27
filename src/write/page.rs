use std::io::Write;
use std::sync::Arc;

use futures::{AsyncWrite, AsyncWriteExt};
use parquet_format_async_temp::thrift::protocol::{
    TCompactOutputProtocol, TCompactOutputStreamProtocol,
};
use parquet_format_async_temp::{DictionaryPageHeader, Encoding, PageType};

use crate::error::Result;
use crate::page::{
    CompressedDataPage, CompressedDictPage, CompressedPage, DataPageHeader, ParquetPageHeader,
};
use crate::statistics::Statistics;

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
    };

    let header_size = write_page_header(writer, &header)?;
    let mut bytes_written = header_size as u64;

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
    };

    let header_size = write_page_header_async(writer, &header).await?;
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

fn assemble_data_page_header(compressed_page: &CompressedDataPage) -> ParquetPageHeader {
    let mut page_header = ParquetPageHeader {
        type_: match compressed_page.header() {
            DataPageHeader::V1(_) => PageType::DATA_PAGE,
            DataPageHeader::V2(_) => PageType::DATA_PAGE_V2,
        },
        uncompressed_page_size: compressed_page.uncompressed_size() as i32,
        compressed_page_size: compressed_page.compressed_size() as i32,
        crc: None,
        data_page_header: None,
        index_page_header: None,
        dictionary_page_header: None,
        data_page_header_v2: None,
    };

    match compressed_page.header() {
        DataPageHeader::V1(header) => {
            page_header.data_page_header = Some(header.clone());
        }
        DataPageHeader::V2(header) => {
            page_header.data_page_header_v2 = Some(header.clone());
        }
    }
    page_header
}

fn assemble_dict_page_header(page: &CompressedDictPage) -> ParquetPageHeader {
    ParquetPageHeader {
        type_: PageType::DICTIONARY_PAGE,
        uncompressed_page_size: page.buffer.len() as i32,
        compressed_page_size: page.buffer.len() as i32,
        crc: None,
        data_page_header: None,
        index_page_header: None,
        dictionary_page_header: Some(DictionaryPageHeader {
            num_values: page.num_values as i32,
            encoding: Encoding::PLAIN,
            is_sorted: None,
        }),
        data_page_header_v2: None,
    }
}

/// writes the page header into `writer`, returning the number of bytes used in the process.
fn write_page_header<W: Write>(mut writer: &mut W, header: &ParquetPageHeader) -> Result<u64> {
    let mut protocol = TCompactOutputProtocol::new(&mut writer);
    Ok(header.write_to_out_protocol(&mut protocol)? as u64)
}

/// writes the page header into `writer`, returning the number of bytes used in the process.
async fn write_page_header_async<W: AsyncWrite + Unpin + Send>(
    mut writer: &mut W,
    header: &ParquetPageHeader,
) -> Result<u64> {
    let mut protocol = TCompactOutputStreamProtocol::new(&mut writer);
    Ok(header.write_to_out_stream_protocol(&mut protocol).await? as u64)
}
