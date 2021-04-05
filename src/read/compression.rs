use crate::compression::{create_codec, Codec};
use crate::error::{ParquetError, Result};

use super::page::{CompressedPage, PageV1, PageV2};
use super::Page;

pub fn decompress(
    buf: &[u8],
    uncompressed_page_size: usize,
    decompressor: &mut dyn Codec,
) -> Result<Vec<u8>> {
    let mut decompressed_buffer = Vec::with_capacity(uncompressed_page_size);
    let decompressed_size = decompressor.decompress(buf, &mut decompressed_buffer)?;
    if decompressed_size != uncompressed_page_size {
        return Err(general_err!(
            "Actual decompressed size doesn't match the expected one ({} vs {})",
            decompressed_size,
            uncompressed_page_size
        ));
    }
    Ok(decompressed_buffer)
}

fn decompress_v1(mut page: PageV1, decompressor: &mut dyn Codec) -> Result<PageV1> {
    page.buffer = decompress(&page.buffer, page.uncompressed_page_size, decompressor)?;
    Ok(page)
}

fn decompress_v2(mut page: PageV2, decompressor: &mut dyn Codec) -> Result<PageV2> {
    let page_header = &page.header;
    let uncompressed_page_size = &page.uncompressed_page_size;

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

    let uncompressed_len = uncompressed_page_size - offset;

    if can_decompress {
        let mut decompressed_buffer = Vec::with_capacity(uncompressed_len);
        let decompressed_size =
            decompressor.decompress(&page.buffer[offset..], &mut decompressed_buffer)?;
        if decompressed_size != uncompressed_len {
            return Err(general_err!(
                "Actual decompressed size doesn't match the expected one ({} vs {})",
                decompressed_size,
                uncompressed_len
            ));
        }
        if offset == 0 {
            page.buffer = decompressed_buffer;
        } else {
            // Prepend saved offsets to the buffer
            page.buffer.truncate(offset);
            page.buffer.append(&mut decompressed_buffer);
        }
    };
    Ok(page)
}

/// decompresses a page in place. This only changes the pages' internal buffer.
pub fn decompress_page(page: CompressedPage) -> Result<Page> {
    match page {
        CompressedPage::V1(page) => {
            let codec = create_codec(&page.compression)?;
            if let Some(mut codec) = codec {
                decompress_v1(page, codec.as_mut()).map(Page::V1)
            } else {
                Ok(Page::V1(page))
            }
        }
        CompressedPage::V2(page) => {
            let codec = create_codec(&page.compression)?;
            if let Some(mut codec) = codec {
                decompress_v2(page, codec.as_mut()).map(Page::V2)
            } else {
                Ok(Page::V2(page))
            }
        }
    }
}
