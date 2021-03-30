use std::io::Read;

use parquet_format::{CompressionCodec, PageHeader, PageType};
use thrift::protocol::TCompactInputProtocol;

use crate::compression::Codec;
use crate::{
    compression::create_codec,
    errors::{ParquetError, Result},
};

use super::page::{Page, PageDict, PageV1, PageV2};

/// A page iterator iterates over row group's pages. In parquet, pages are guaranteed to be
/// contiguously arranged in memory and therefore must be read in sequence.
#[derive(Debug)]
pub struct PageIterator<'a, R: Read> {
    // The source
    reader: &'a mut R,

    // The compression codec for this column chunk. None represents PLAIN.
    decompressor: Option<Box<dyn Codec>>,

    // The number of values we have seen so far.
    seen_num_values: i64,

    // The number of total values in this column chunk.
    total_num_values: i64,
}

impl<'a, R: Read> PageIterator<'a, R> {
    pub fn try_new(
        reader: &'a mut R,
        total_num_values: i64,
        compression: &CompressionCodec,
    ) -> Result<Self> {
        let decompressor = create_codec(compression)?;
        Ok(Self {
            reader,
            total_num_values,
            decompressor,
            seen_num_values: 0,
        })
    }

    /// Reads Page header from Thrift.
    fn read_page_header(&mut self) -> Result<PageHeader> {
        let mut prot = TCompactInputProtocol::new(&mut self.reader);
        let page_header = PageHeader::read_from_in_protocol(&mut prot)?;
        Ok(page_header)
    }
}

impl<'a, R: Read> Iterator for PageIterator<'a, R> {
    type Item = Result<Page>;

    fn next(&mut self) -> Option<Self::Item> {
        next_page(self).transpose()
    }
}

fn next_page<R: Read>(reader: &mut PageIterator<R>) -> Result<Option<Page>> {
    while reader.seen_num_values < reader.total_num_values {
        let page_header = reader.read_page_header()?;

        // When processing data page v2, depending on enabled compression for the
        // page, we should account for uncompressed data ('offset') of
        // repetition and definition levels.
        //
        // We always use 0 offset for other pages other than v2, `true` flag means
        // that compression will be applied if decompressor is defined
        let mut offset: usize = 0;
        let mut can_decompress = true;

        if let Some(ref header_v2) = page_header.data_page_header_v2 {
            offset = (header_v2.definition_levels_byte_length
                + header_v2.repetition_levels_byte_length) as usize;
            // When is_compressed flag is missing the page is considered compressed
            can_decompress = header_v2.is_compressed.unwrap_or(true);
        }

        let compressed_len = page_header.compressed_page_size as usize - offset;
        let uncompressed_len = page_header.uncompressed_page_size as usize - offset;
        // We still need to read all bytes from buffered stream
        let mut buffer = vec![0; offset + compressed_len];
        reader.reader.read_exact(&mut buffer)?;

        if let Some(decompressor) = reader.decompressor.as_mut() {
            if can_decompress {
                let mut decompressed_buffer = Vec::with_capacity(uncompressed_len);
                let decompressed_size =
                    decompressor.decompress(&buffer[offset..], &mut decompressed_buffer)?;
                if decompressed_size != uncompressed_len {
                    return Err(general_err!(
                        "Actual decompressed size doesn't match the expected one ({} vs {})",
                        decompressed_size,
                        uncompressed_len
                    ));
                }
                if offset == 0 {
                    buffer = decompressed_buffer;
                } else {
                    // Prepend saved offsets to the buffer
                    buffer.truncate(offset);
                    buffer.append(&mut decompressed_buffer);
                }
            }
        }

        let result = match page_header.type_ {
            PageType::DictionaryPage => {
                assert!(page_header.dictionary_page_header.is_some());
                let dict_header = page_header.dictionary_page_header.as_ref().unwrap();
                let is_sorted = dict_header.is_sorted.unwrap_or(false);
                Page::Dictionary(PageDict::new(
                    buffer,
                    dict_header.num_values as u32,
                    dict_header.encoding,
                    is_sorted,
                ))
            }
            PageType::DataPage => {
                assert!(page_header.data_page_header.is_some());
                let header = page_header.data_page_header.unwrap();
                reader.seen_num_values += header.num_values as i64;
                Page::V1(PageV1::new(
                    buffer,
                    header.num_values as u32,
                    header.encoding,
                    header.definition_level_encoding,
                    header.repetition_level_encoding,
                ))
            }
            PageType::DataPageV2 => {
                assert!(page_header.data_page_header_v2.is_some());
                let header = page_header.data_page_header_v2.unwrap();
                let is_compressed = header.is_compressed.unwrap_or(true);
                reader.seen_num_values += header.num_values as i64;
                Page::V2(PageV2::new(
                    buffer,
                    header.num_values as u32,
                    header.encoding,
                    header.num_nulls as u32,
                    header.num_rows as u32,
                    header.definition_levels_byte_length as u32,
                    header.repetition_levels_byte_length as u32,
                    is_compressed,
                ))
            }
            PageType::IndexPage => {
                continue;
            }
        };
        return Ok(Some(result));
    }
    Ok(None)
}
