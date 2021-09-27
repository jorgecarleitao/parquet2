use std::collections::HashSet;
use std::convert::TryInto;
use std::io::Write;

use futures::AsyncWrite;
use parquet_format_async_temp::thrift::protocol::{
    TCompactOutputProtocol, TCompactOutputStreamProtocol, TOutputProtocol, TOutputStreamProtocol,
};
use parquet_format_async_temp::{ColumnChunk, ColumnMetaData};

use crate::statistics::serialize_statistics;
use crate::FallibleStreamingIterator;
use crate::{
    compression::Compression,
    encoding::Encoding,
    error::{ParquetError, Result},
    metadata::ColumnDescriptor,
    page::{CompressedPage, PageType},
    schema::types::{physical_type_to_type, ParquetType},
};

use super::page::{write_page, write_page_async, PageWriteSpec};
use super::statistics::reduce;
use super::DynStreamingIterator;

pub fn write_column_chunk<'a, W, E>(
    writer: &mut W,
    mut offset: u64,
    descriptor: &ColumnDescriptor,
    compression: Compression,
    mut compressed_pages: DynStreamingIterator<'a, CompressedPage, E>,
) -> Result<(ColumnChunk, u64)>
where
    W: Write,
    ParquetError: From<E>,
    E: std::error::Error,
{
    // write every page

    let initial = offset;

    let mut specs = vec![];
    while let Some(compressed_page) = compressed_pages.next()? {
        let spec = write_page(writer, offset, compressed_page)?;
        offset += spec.bytes_written;
        specs.push(spec);
    }
    let mut bytes_written = offset - initial;

    let column_chunk = build_column_chunk(&specs, descriptor, compression)?;

    // write metadata
    let mut protocol = TCompactOutputProtocol::new(writer);
    bytes_written += column_chunk.write_to_out_protocol(&mut protocol)? as u64;
    protocol.flush()?;

    Ok((column_chunk, bytes_written))
}

pub async fn write_column_chunk_async<W, E>(
    writer: &mut W,
    mut offset: u64,
    descriptor: &ColumnDescriptor,
    compression: Compression,
    mut compressed_pages: DynStreamingIterator<'_, CompressedPage, E>,
) -> Result<(ColumnChunk, usize)>
where
    W: AsyncWrite + Unpin + Send,
    ParquetError: From<E>,
    E: std::error::Error,
{
    let initial = offset;
    // write every page
    let mut specs = vec![];
    while let Some(compressed_page) = compressed_pages.next()? {
        let spec = write_page_async(writer, offset, compressed_page).await?;
        offset += spec.bytes_written;
        specs.push(spec);
    }
    let mut bytes_written = (offset - initial) as usize;

    let column_chunk = build_column_chunk(&specs, descriptor, compression)?;

    // write metadata
    let mut protocol = TCompactOutputStreamProtocol::new(writer);
    bytes_written += column_chunk
        .write_to_out_stream_protocol(&mut protocol)
        .await?;
    protocol.flush().await?;

    Ok((column_chunk, bytes_written))
}

fn build_column_chunk(
    specs: &[PageWriteSpec],
    descriptor: &ColumnDescriptor,
    compression: Compression,
) -> Result<ColumnChunk> {
    // compute stats to build header at the end of the chunk

    // SPEC: the total compressed size is the total compressed size of each page + the header size
    let total_compressed_size = specs
        .iter()
        .map(|x| x.header_size as i64 + x.header.compressed_page_size as i64)
        .sum();
    // SPEC: the total compressed size is the total compressed size of each page + the header size
    let total_uncompressed_size = specs
        .iter()
        .map(|x| x.header_size as i64 + x.header.uncompressed_page_size as i64)
        .sum();
    let data_page_offset = specs.first().map(|spec| spec.offset).unwrap_or(0) as i64;
    let num_values = specs
        .iter()
        .map(|spec| {
            let type_ = spec.header.type_.try_into().unwrap();
            match type_ {
                PageType::DataPage => {
                    spec.header.data_page_header.as_ref().unwrap().num_values as i64
                }
                PageType::DataPageV2 => {
                    spec.header.data_page_header_v2.as_ref().unwrap().num_values as i64
                }
                _ => 0, // only data pages contribute
            }
        })
        .sum();
    let encodings = specs
        .iter()
        .map(|spec| {
            let type_ = spec.header.type_.try_into().unwrap();
            match type_ {
                PageType::DataPage => vec![
                    spec.header.data_page_header.as_ref().unwrap().encoding,
                    Encoding::Rle.into(),
                ],
                PageType::DataPageV2 => {
                    vec![
                        spec.header.data_page_header_v2.as_ref().unwrap().encoding,
                        Encoding::Rle.into(),
                    ]
                }
                PageType::DictionaryPage => vec![
                    spec.header
                        .dictionary_page_header
                        .as_ref()
                        .unwrap()
                        .encoding,
                ],
                _ => todo!(),
            }
        })
        .flatten()
        .collect::<HashSet<_>>() // unique
        .into_iter() // to vec
        .collect();

    let statistics = specs.iter().map(|x| &x.statistics).collect::<Vec<_>>();
    let statistics = reduce(&statistics)?;
    let statistics = statistics.map(|x| serialize_statistics(x.as_ref()));

    let type_ = match descriptor.type_() {
        ParquetType::PrimitiveType { physical_type, .. } => physical_type_to_type(physical_type).0,
        _ => {
            return Err(general_err!(
                "Trying to write a row group of a non-physical type"
            ))
        }
    };

    let metadata = ColumnMetaData {
        type_,
        encodings,
        path_in_schema: descriptor.path_in_schema().to_vec(),
        codec: compression.into(),
        num_values,
        total_uncompressed_size,
        total_compressed_size,
        key_value_metadata: None,
        data_page_offset,
        index_page_offset: None,
        dictionary_page_offset: None,
        statistics,
        encoding_stats: None,
        bloom_filter_offset: None,
    };

    Ok(ColumnChunk {
        file_path: None, // same file for now.
        file_offset: data_page_offset + total_compressed_size,
        meta_data: Some(metadata),
        offset_index_offset: None,
        offset_index_length: None,
        column_index_offset: None,
        column_index_length: None,
        crypto_metadata: None,
        encrypted_column_metadata: None,
    })
}
