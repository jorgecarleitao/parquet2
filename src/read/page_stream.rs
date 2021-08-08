use std::io::SeekFrom;

use async_stream::try_stream;
use futures::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, Stream};
use parquet_format_async_temp::thrift::protocol::TCompactInputStreamProtocol;

use crate::compression::Compression;
use crate::error::Result;
use crate::metadata::{ColumnDescriptor, FileMetaData};
use crate::page::{CompressedDataPage, ParquetPageHeader};

use super::page_iterator::{finish_page, FinishedPage};

/// Returns a stream of compressed data pages
pub async fn get_page_stream<'a, RR: AsyncRead + Unpin + Send + AsyncSeek>(
    metadata: &'a FileMetaData,
    row_group: usize,
    column: usize,
    reader: &'a mut RR,
    buffer: Vec<u8>,
) -> Result<impl Stream<Item = Result<CompressedDataPage>> + 'a> {
    let column_metadata = metadata.row_groups[row_group].column(column);
    let (col_start, _) = column_metadata.byte_range();
    reader.seek(SeekFrom::Start(col_start)).await?;
    Ok(_get_page_stream(
        reader,
        column_metadata.num_values(),
        column_metadata.compression(),
        column_metadata.descriptor(),
        buffer,
    ))
}

fn _get_page_stream<'a, R: AsyncRead + Unpin + Send>(
    reader: &'a mut R,
    total_num_values: i64,
    compression: Compression,
    descriptor: &'a ColumnDescriptor,
    mut buffer: Vec<u8>,
) -> impl Stream<Item = Result<CompressedDataPage>> + 'a {
    let mut seen_values = 0i64;
    let mut current_dictionary = None;
    try_stream! {
        while seen_values < total_num_values {
            // the header
            let page_header = read_page_header(reader).await?;

            // followed by the buffer
            let read_size = page_header.compressed_page_size as usize;
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
                FinishedPage::Data(page, new_seen_values) => {
                    seen_values += new_seen_values;
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
