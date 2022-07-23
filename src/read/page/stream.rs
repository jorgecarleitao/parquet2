use std::io::SeekFrom;

use async_stream::try_stream;
use futures::io::{copy, sink};
use futures::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, Stream};
use parquet_format_async_temp::thrift::protocol::TCompactInputStreamProtocol;

use crate::compression::Compression;
use crate::error::Result;
use crate::metadata::{ColumnChunkMetaData, Descriptor};
use crate::page::{CompressedPage, ParquetPageHeader};

use super::reader::{finish_page, get_page_header, PageMetaData};
use super::PageFilter;

/// Returns a stream of compressed data pages
pub async fn get_page_stream<'a, RR: AsyncRead + Unpin + Send + AsyncSeek>(
    column_metadata: &'a ColumnChunkMetaData,
    reader: &'a mut RR,
    buffer: Vec<u8>,
    pages_filter: PageFilter,
) -> Result<impl Stream<Item = Result<CompressedPage>> + 'a> {
    get_page_stream_with_page_meta(column_metadata.into(), reader, buffer, pages_filter).await
}

/// Returns a stream of compressed data pages from a reader that begins at the start of the column
pub async fn get_page_stream_from_column_start<'a, R: AsyncRead + Unpin + Send>(
    column_metadata: &'a ColumnChunkMetaData,
    reader: &'a mut R,
    buffer: Vec<u8>,
    pages_filter: PageFilter,
) -> Result<impl Stream<Item = Result<CompressedPage>> + 'a> {
    let page_metadata: PageMetaData = column_metadata.into();
    Ok(_get_page_stream(
        reader,
        page_metadata.num_values,
        page_metadata.compression,
        page_metadata.descriptor,
        buffer,
        pages_filter,
    ))
}

/// Returns a stream of compressed data pages with [`PageMetaData`]
pub async fn get_page_stream_with_page_meta<RR: AsyncRead + Unpin + Send + AsyncSeek>(
    page_metadata: PageMetaData,
    reader: &mut RR,
    buffer: Vec<u8>,
    pages_filter: PageFilter,
) -> Result<impl Stream<Item = Result<CompressedPage>> + '_> {
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

fn _get_page_stream<R: AsyncRead + Unpin + Send>(
    reader: &mut R,
    total_num_values: i64,
    compression: Compression,
    descriptor: Descriptor,
    mut buffer: Vec<u8>,
    pages_filter: PageFilter,
) -> impl Stream<Item = Result<CompressedPage>> + '_ {
    let mut seen_values = 0i64;
    try_stream! {
        while seen_values < total_num_values {
            // the header
            let page_header = read_page_header(reader).await?;

            let data_header = get_page_header(&page_header)?;
            seen_values += data_header.as_ref().map(|x| x.num_values() as i64).unwrap_or_default();

            let read_size: usize = page_header.compressed_page_size.try_into()?;

            if let Some(data_header) = data_header {
                if !pages_filter(&descriptor, &data_header) {
                    // page to be skipped, we sill need to seek
                    copy(reader.take(read_size as u64), &mut sink()).await?;
                    continue
                }
            }

            // followed by the buffer
            buffer.clear();
            buffer.try_reserve(read_size)?;
            reader
                .take(read_size as u64)
                .read_to_end(&mut buffer).await?;

            yield finish_page(
                page_header,
                &mut buffer,
                compression,
                &descriptor,
                None,
            )?;
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
