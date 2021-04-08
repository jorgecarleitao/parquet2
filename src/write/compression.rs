use crate::error::Result;
use crate::{
    compression::{create_codec, Codec},
    read::{CompressedPage, Page, PageV1, PageV2},
};

fn compress_(buffer: &[u8], decompressor: &mut dyn Codec) -> Result<Vec<u8>> {
    let mut compressed_buffer = Vec::new();
    decompressor.compress(buffer, &mut compressed_buffer)?;
    Ok(compressed_buffer)
}

fn compress_v1(mut page: PageV1, codec: &mut dyn Codec) -> Result<PageV1> {
    page.buffer = compress_(&page.buffer, codec)?;
    Ok(page)
}

fn compress_v2(mut page: PageV2, codec: &mut dyn Codec) -> Result<PageV2> {
    // only values are compressed in v2:
    // [<rep data> <def data> <values>] -> [<rep data> <def data> <compressed_values>]
    let prefix = (page.header.repetition_levels_byte_length
        + page.header.definition_levels_byte_length) as usize;
    let compressed_values = compress_(&page.buffer[prefix..], codec)?;
    page.buffer.truncate(prefix);
    page.buffer.extend(compressed_values);
    Ok(page)
}

/// decompresses a page in place. This only changes the pages' internal buffer.
pub fn compress(page: Page) -> Result<CompressedPage> {
    match page {
        Page::V1(page) => {
            let codec = create_codec(&page.compression)?;
            if let Some(mut codec) = codec {
                compress_v1(page, codec.as_mut()).map(CompressedPage::V1)
            } else {
                Ok(CompressedPage::V1(page))
            }
        }
        Page::V2(page) => {
            let codec = create_codec(&page.compression)?;
            if let Some(mut codec) = codec {
                compress_v2(page, codec.as_mut()).map(CompressedPage::V2)
            } else {
                Ok(CompressedPage::V2(page))
            }
        }
    }
}
