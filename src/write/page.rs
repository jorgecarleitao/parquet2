use std::io::{Seek, SeekFrom, Write};

use parquet_format::{PageHeader, PageType};
use thrift::protocol::TCompactOutputProtocol;
use thrift::protocol::TOutputProtocol;

use crate::error::Result;
use crate::read::CompressedPage;

/// Contains page write metrics.
pub struct PageWriteSpec {
    pub header: PageHeader,
    pub header_size: usize,
    pub offset: u64,
    pub bytes_written: u64,
}

pub fn write_page<W: Write + Seek>(
    writer: &mut W,
    compressed_page: CompressedPage,
) -> Result<PageWriteSpec> {
    let header = assemble_page_header(&compressed_page);

    let start_pos = writer.seek(SeekFrom::Current(0))?;

    let header_size = write_page_header(writer, &header)?;

    match compressed_page {
        CompressedPage::V1(page) => writer.write_all(&page.buffer),
        CompressedPage::V2(page) => writer.write_all(&page.buffer),
    }?;
    let end_pos = writer.seek(SeekFrom::Current(0))?;

    Ok(PageWriteSpec {
        header,
        header_size,
        offset: start_pos,
        bytes_written: end_pos - start_pos,
    })
}

fn assemble_page_header(compressed_page: &CompressedPage) -> PageHeader {
    let mut page_header = PageHeader {
        type_: match compressed_page {
            CompressedPage::V1(_) => PageType::DataPage,
            CompressedPage::V2(_) => PageType::DataPageV2,
        },
        uncompressed_page_size: compressed_page.uncompressed_size() as i32,
        compressed_page_size: compressed_page.compressed_size() as i32,
        crc: None,
        data_page_header: None,
        index_page_header: None,
        dictionary_page_header: None,
        data_page_header_v2: None,
    };

    match compressed_page {
        CompressedPage::V1(page) => {
            page_header.data_page_header = Some(page.header.clone());
        }
        CompressedPage::V2(page) => {
            page_header.data_page_header_v2 = Some(page.header.clone());
        }
    }
    page_header
}

/// writes the page header into `writer`, returning the number of bytes used in the process.
fn write_page_header<W: Write + Seek>(mut writer: &mut W, header: &PageHeader) -> Result<usize> {
    let start_pos = writer.seek(SeekFrom::Current(0))?;
    {
        let mut protocol = TCompactOutputProtocol::new(&mut writer);
        header.write_to_out_protocol(&mut protocol)?;
        protocol.flush()?;
    }
    let end_pos = writer.seek(SeekFrom::Current(0))?;
    Ok((end_pos - start_pos) as usize)
}
