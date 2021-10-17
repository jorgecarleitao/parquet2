use crate::error::{ParquetError, Result};
use crate::page::{CompressedDictPage, CompressedPage, DataPageHeader, EncodedDictPage};
use crate::parquet_bridge::Compression;
use crate::FallibleStreamingIterator;
use crate::{
    compression::create_codec,
    page::{CompressedDataPage, DataPage, EncodedPage},
};

/// Compresses a [`DataPage`] into a [`CompressedDataPage`].
fn compress_data(
    page: DataPage,
    mut compressed_buffer: Vec<u8>,
    compression: Compression,
) -> Result<CompressedDataPage> {
    let DataPage {
        mut buffer,
        header,
        dictionary_page,
        descriptor,
    } = page;
    let uncompressed_page_size = buffer.len();
    let codec = create_codec(&compression)?;
    if let Some(mut codec) = codec {
        match &header {
            DataPageHeader::V1(_) => {
                codec.compress(&buffer, &mut compressed_buffer)?;
            }
            DataPageHeader::V2(header) => {
                let levels_byte_length = (header.repetition_levels_byte_length
                    + header.definition_levels_byte_length)
                    as usize;
                compressed_buffer.extend_from_slice(&buffer[..levels_byte_length]);
                codec.compress(&buffer[levels_byte_length..], &mut compressed_buffer)?;
            }
        };
    } else {
        std::mem::swap(&mut buffer, &mut compressed_buffer);
    };
    Ok(CompressedDataPage::new(
        header,
        compressed_buffer,
        compression,
        uncompressed_page_size,
        dictionary_page,
        descriptor,
    ))
}

fn compress_dict(
    page: EncodedDictPage,
    mut compressed_buffer: Vec<u8>,
    compression: Compression,
) -> Result<CompressedDictPage> {
    let EncodedDictPage {
        mut buffer,
        num_values,
    } = page;
    let codec = create_codec(&compression)?;
    if let Some(mut codec) = codec {
        codec.compress(&buffer, &mut compressed_buffer)?;
    } else {
        std::mem::swap(&mut buffer, &mut compressed_buffer);
    }
    Ok(CompressedDictPage::new(compressed_buffer, num_values))
}

pub fn compress(
    page: EncodedPage,
    compressed_buffer: Vec<u8>,
    compression: Compression,
) -> Result<CompressedPage> {
    match page {
        EncodedPage::Data(page) => {
            compress_data(page, compressed_buffer, compression).map(CompressedPage::Data)
        }
        EncodedPage::Dict(page) => {
            compress_dict(page, compressed_buffer, compression).map(CompressedPage::Dict)
        }
    }
}

/// A [`FallibleStreamingIterator`] that consumes [`EncodedPage`] and yields [`CompressedPage`]
/// holding a reusable buffer ([`Vec<u8>`]) for compression.
pub struct Compressor<I: Iterator<Item = Result<EncodedPage>>> {
    iter: I,
    compression: Compression,
    buffer: Vec<u8>,
    current: Option<CompressedPage>,
}

impl<I: Iterator<Item = Result<EncodedPage>>> Compressor<I> {
    pub fn new_from_vec(iter: I, compression: Compression, buffer: Vec<u8>) -> Self {
        Self::new(iter, compression, buffer)
    }

    pub fn new(iter: I, compression: Compression, buffer: Vec<u8>) -> Self {
        Self {
            iter,
            compression,
            buffer,
            current: None,
        }
    }
}

impl<I: Iterator<Item = Result<EncodedPage>>> FallibleStreamingIterator for Compressor<I> {
    type Item = CompressedPage;
    type Error = ParquetError;

    fn advance(&mut self) -> std::result::Result<(), Self::Error> {
        let mut compressed_buffer = if let Some(page) = self.current.as_mut() {
            std::mem::take(page.buffer())
        } else {
            std::mem::take(&mut self.buffer)
        };
        compressed_buffer.clear();

        let next = self
            .iter
            .next()
            .map(|x| x.and_then(|page| compress(page, compressed_buffer, self.compression)))
            .transpose()?;
        self.current = next;
        Ok(())
    }

    fn get(&self) -> Option<&Self::Item> {
        self.current.as_ref()
    }
}
