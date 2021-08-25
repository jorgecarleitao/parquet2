use std::io::SeekFrom;

use async_stream::try_stream;
use futures::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, Stream};
use parquet_format_async_temp::thrift::protocol::TCompactInputStreamProtocol;

use crate::compression::Compression;
use crate::error::Result;
use crate::metadata::{ColumnChunkMetaData, ColumnDescriptor};
use crate::page::{CompressedDataPage, ParquetPageHeader};

use super::page_iterator::{finish_page, get_page_header, FinishedPage};
use super::PageFilter;

/// Returns a stream of compressed data pages
pub async fn get_page_stream<'a, RR: AsyncRead + Unpin + Send + AsyncSeek>(
    column_metadata: &'a ColumnChunkMetaData,
    reader: &'a mut RR,
    buffer: Vec<u8>,
    pages_filter: PageFilter,
) -> Result<impl Stream<Item = Result<CompressedDataPage>> + 'a> {
    let (col_start, _) = column_metadata.byte_range();
    reader.seek(SeekFrom::Start(col_start)).await?;
    Ok(_get_page_stream(
        reader,
        column_metadata.num_values(),
        column_metadata.compression(),
        column_metadata.descriptor(),
        buffer,
        pages_filter,
    ))
}

fn _get_page_stream<'a, R: AsyncRead + AsyncSeek + Unpin + Send>(
    reader: &'a mut R,
    total_num_values: i64,
    compression: Compression,
    descriptor: &'a ColumnDescriptor,
    mut buffer: Vec<u8>,
    pages_filter: PageFilter,
) -> impl Stream<Item = Result<CompressedDataPage>> + 'a {
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
                if !pages_filter(descriptor, &data_header) {
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
                descriptor,
            )?;

            match result {
                FinishedPage::Data(page) => {
                    yield page
                }
                FinishedPage::Dict(dict) => {
                    current_dictionary = Some(dict);
                    continue
                }
                _ => continue,
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
