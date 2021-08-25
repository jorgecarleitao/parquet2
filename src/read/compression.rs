use parquet_format_async_temp::DataPageHeaderV2;

use crate::compression::{create_codec, Codec};
use crate::error::Result;

use super::{PageIterator, StreamingIterator};
use crate::page::{CompressedDataPage, DataPage, DataPageHeader};

fn decompress_v1(compressed: &[u8], decompressor: &mut dyn Codec, buffer: &mut [u8]) -> Result<()> {
    decompressor.decompress(compressed, buffer)
}

fn decompress_v2(
    compressed: &[u8],
    page_header: &DataPageHeaderV2,
    decompressor: &mut dyn Codec,
    buffer: &mut [u8],
) -> Result<()> {
    // When processing data page v2, depending on enabled compression for the
    // page, we should account for uncompressed data ('offset') of
    // repetition and definition levels.
    //
    // We always use 0 offset for other pages other than v2, `true` flag means
    // that compression will be applied if decompressor is defined
    let offset = (page_header.definition_levels_byte_length
        + page_header.repetition_levels_byte_length) as usize;
    // When is_compressed flag is missing the page is considered compressed
    let can_decompress = page_header.is_compressed.unwrap_or(true);

    if can_decompress {
        (&mut buffer[..offset]).copy_from_slice(&compressed[..offset]);

        decompressor.decompress(&compressed[offset..], &mut buffer[offset..])?;
    } else {
        buffer.copy_from_slice(compressed);
    }
    Ok(())
}

/// decompresses a page.
/// If `page.buffer.len() == 0`, there was no decompression and the buffer was moved.
/// Else, decompression took place.
pub fn decompress_buffer(
    compressed_page: &mut CompressedDataPage,
    buffer: &mut Vec<u8>,
) -> Result<bool> {
    let codec = create_codec(&compressed_page.compression())?;

    if let Some(mut codec) = codec {
        // the buffer must be decompressed; do it so now, writing the decompressed data into `buffer`
        let compressed_buffer = &compressed_page.buffer;
        buffer.resize(compressed_page.uncompressed_size(), 0);
        match compressed_page.header() {
            DataPageHeader::V1(_) => decompress_v1(compressed_buffer, codec.as_mut(), buffer)?,
            DataPageHeader::V2(header) => {
                decompress_v2(compressed_buffer, header, codec.as_mut(), buffer)?
            }
        }
        Ok(true)
    } else {
        // page.buffer is already decompressed => swap it with `buffer`, making `page.buffer` the
        // decompression buffer and `buffer` the decompressed buffer
        std::mem::swap(&mut compressed_page.buffer, buffer);
        Ok(false)
    }
}

/// Decompresses the page, using `buffer` for decompression.
/// If `page.buffer.len() == 0`, there was no decompression and the buffer was moved.
/// Else, decompression took place.
pub fn decompress(
    mut compressed_page: CompressedDataPage,
    buffer: &mut Vec<u8>,
) -> Result<DataPage> {
    decompress_buffer(&mut compressed_page, buffer)?;
    Ok(DataPage::new(
        compressed_page.header,
        std::mem::take(buffer),
        compressed_page.dictionary_page,
        compressed_page.descriptor,
    ))
}

fn decompress_reuse<R: std::io::Read>(
    mut compressed_page: CompressedDataPage,
    iterator: &mut PageIterator<R>,
    buffer: &mut Vec<u8>,
    decompressions: &mut usize,
) -> Result<DataPage> {
    let was_decompressed = decompress_buffer(&mut compressed_page, buffer)?;

    let new_page = DataPage::new(
        compressed_page.header,
        std::mem::take(buffer),
        compressed_page.dictionary_page,
        compressed_page.descriptor,
    );

    if was_decompressed {
        *decompressions += 1;
    } else {
        iterator.reuse_buffer(compressed_page.buffer)
    }
    Ok(new_page)
}

/// Decompressor that allows re-using the page buffer of [`PageIterator`]
pub struct Decompressor<'a, R: std::io::Read> {
    iter: PageIterator<'a, R>,
    buffer: Vec<u8>,
    current: Option<Result<DataPage>>,
    decompressions: usize,
}

impl<'a, R: std::io::Read> Decompressor<'a, R> {
    pub fn new(iter: PageIterator<'a, R>, buffer: Vec<u8>) -> Self {
        Self {
            iter,
            buffer,
            current: None,
            decompressions: 0,
        }
    }

    pub fn into_buffers(mut self) -> (Vec<u8>, Vec<u8>) {
        let mut a = self
            .current
            .map(|x| x.map(|x| x.buffer).unwrap_or_else(|_| Vec::new()))
            .unwrap_or(self.iter.buffer);

        if self.decompressions % 2 == 0 {
            std::mem::swap(&mut a, &mut self.buffer)
        };
        (a, self.buffer)
    }
}

impl<'a, R: std::io::Read> StreamingIterator for Decompressor<'a, R> {
    type Item = Result<DataPage>;

    fn advance(&mut self) {
        if let Some(Ok(page)) = self.current.as_mut() {
            self.buffer = std::mem::take(&mut page.buffer);
        }

        let next = self.iter.next().map(|x| {
            x.and_then(|x| {
                decompress_reuse(
                    x,
                    &mut self.iter,
                    &mut self.buffer,
                    &mut self.decompressions,
                )
            })
        });
        self.current = next;
    }

    fn get(&self) -> Option<&Self::Item> {
        self.current.as_ref()
    }
}
