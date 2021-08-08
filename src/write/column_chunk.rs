use std::convert::TryInto;
use std::io::{Seek, Write};
use std::{collections::HashSet, error::Error};

use parquet_format_async_temp::thrift::protocol::TCompactOutputProtocol;
use parquet_format_async_temp::thrift::protocol::TOutputProtocol;
use parquet_format_async_temp::{ColumnChunk, ColumnMetaData};

use crate::statistics::serialize_statistics;
use crate::{
    compression::Compression,
    encoding::Encoding,
    error::{ParquetError, Result},
    metadata::ColumnDescriptor,
    page::{CompressedPage, PageType},
    schema::types::{physical_type_to_type, ParquetType},
};

use super::page::write_page;
use super::statistics::reduce;

pub fn write_column_chunk<
    W: Write + Seek,
    I: Iterator<Item = std::result::Result<CompressedPage, E>>,
    E: Error + Send + Sync + 'static,
>(
    writer: &mut W,
    descriptor: &ColumnDescriptor,
    compression: Compression,
    compressed_pages: I,
) -> Result<ColumnChunk> {
    // write every page
    let specs = compressed_pages
        .map(|compressed_page| {
            write_page(
                writer,
                compressed_page.map_err(ParquetError::from_external_error)?,
            )
        })
        .collect::<Result<Vec<_>>>()?;

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

    let column_chunk = ColumnChunk {
        file_path: None, // same file for now.
        file_offset: data_page_offset + total_compressed_size,
        meta_data: Some(metadata),
        offset_index_offset: None,
        offset_index_length: None,
        column_index_offset: None,
        column_index_length: None,
        crypto_metadata: None,
        encrypted_column_metadata: None,
    };

    // write metadata
    let mut protocol = TCompactOutputProtocol::new(writer);
    column_chunk.write_to_out_protocol(&mut protocol)?;
    protocol.flush()?;

    Ok(column_chunk)
}
