use std::io::SeekFrom;

use async_stream::try_stream;
use futures::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, Stream};
use parquet_format_async_temp::thrift::protocol::TCompactInputStreamProtocol;

use crate::compression::Compression;
use crate::error::Result;
use crate::metadata::{ColumnChunkMetaData, Descriptor};
use crate::page::{CompressedDataPage, ParquetPageHeader};

use super::reader::{finish_page, get_page_header, FinishedPage, PageMetaData};
use super::PageFilter;

/// Returns a stream of compressed data pages
pub async fn get_page_stream<'a, RR: AsyncRead + Unpin + Send + AsyncSeek>(
    column_metadata: &'a ColumnChunkMetaData,
    reader: &'a mut RR,
    buffer: Vec<u8>,
    pages_filter: PageFilter,
) -> Result<impl Stream<Item = Result<CompressedDataPage>> + 'a> {
    get_page_stream_with_page_meta(column_metadata.into(), reader, buffer, pages_filter).await
}

/// Returns a stream of compressed data pages with [`PageMetaData`]
pub async fn get_page_stream_with_page_meta<RR: AsyncRead + Unpin + Send + AsyncSeek>(
    page_metadata: PageMetaData,
    reader: &mut RR,
    buffer: Vec<u8>,
    pages_filter: PageFilter,
) -> Result<impl Stream<Item = Result<CompressedDataPage>> + '_> {
    let column_start = page_metadata.column_start;
    reader.seek(SeekFrom::Start(column_start)).await?;
    Ok(_get_page_stream(
        reader,
        page_metadata.num_values,
        page_metadata.compression,
        page_metadata.descriptor,
        buffer,
        pages_filter,
    ))
}

fn _get_page_stream<R: AsyncRead + AsyncSeek + Unpin + Send>(
    reader: &mut R,
    total_num_values: i64,
    compression: Compression,
    descriptor: Descriptor,
    mut buffer: Vec<u8>,
    pages_filter: PageFilter,
) -> impl Stream<Item = Result<CompressedDataPage>> + '_ {
    let mut seen_values = 0i64;
    let mut current_dictionary = None;
    try_stream! {
        while seen_values < total_num_values {
            // the header
            let page_header = read_page_header(reader).await?;

            let data_header = get_page_header(&page_header);
            seen_values += data_header.as_ref().map(|x| x.num_values() as i64).unwrap_or_default();

            let read_size = page_header.compressed_page_size as i64;

            if let Some(data_header) = data_header {
                if !pages_filter(&descriptor, &data_header) {
                    // page to be skipped, we sill need to seek
                    reader.seek(SeekFrom::Current(read_size)).await?;
                    continue
                }
            }

            // followed by the buffer
            let read_size = read_size as usize;
            if read_size > 0 {
                buffer.resize(read_size, 0);
                reader.read_exact(&mut buffer).await?;
            }
            let result = finish_page(
                page_header,
                &mut buffer,
                compression,
                &current_dictionary,
                &descriptor,
                None,
            )?;

            match result {
                FinishedPage::Data(page) => {
                    yield page
                }
                FinishedPage::Dict(dict) => {
                    current_dictionary = Some(dict);
                    continue
                }
            }
        }
    }
}

/// Reads Page header from Thrift.
async fn read_page_header<R: AsyncRead + Unpin + Send>(
    reader: &mut R,
) -> Result<ParquetPageHeader> {
    let mut prot = TCompactInputStreamProtocol::new(reader);
    let page_header = ParquetPageHeader::stream_from_in_protocol(&mut prot).await?;
    Ok(page_header)
}
