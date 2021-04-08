use parquet_format::{CompressionCodec, DataPageHeader, Encoding};

use crate::{compression::create_codec, encoding::hybrid_rle::encode, error::Result, read::PageV1};
use crate::{read::CompressedPage, types::NativeType};

fn unzip_option<T: NativeType>(array: &[Option<T>]) -> Result<(Vec<u8>, Vec<u8>)> {
    // leave the first 4 bytes anouncing the length of the def level
    // this will be overwritten at the end, once the length is known.
    // This is unknown at this point because of the uleb128 encoding,
    // whose length is variable.
    let mut validity = std::io::Cursor::new(vec![0; 4]);
    validity.set_position(4);

    let mut values = vec![];
    let iter = array.iter().map(|value| {
        if let Some(item) = value {
            values.extend_from_slice(item.to_le_bytes().as_ref());
            true
        } else {
            false
        }
    });
    encode(&mut validity, iter)?;

    // write the length, now that it is known
    let mut validity = validity.into_inner();
    let length = validity.len() - 4;
    // todo: pay this small debt (loop?)
    let length = length.to_le_bytes();
    validity[0] = length[0];
    validity[1] = length[1];
    validity[2] = length[2];
    validity[3] = length[3];

    Ok((values, validity))
}

pub fn array_to_page_v1<T: NativeType>(
    array: &[Option<T>],
    compression: CompressionCodec,
) -> Result<CompressedPage> {
    let (values, mut buffer) = unzip_option(array)?;

    buffer.extend_from_slice(&values);
    let uncompressed_page_size = buffer.len();

    let codec = create_codec(&compression)?;
    let buffer = if let Some(mut codec) = codec {
        // todo: remove this allocation by extending `buffer` directly.
        // needs refactoring `compress`'s API.
        let mut tmp = vec![];
        codec.compress(&values, &mut tmp)?;
        tmp
    } else {
        buffer
    };

    let header = DataPageHeader {
        num_values: array.len() as i32,
        encoding: Encoding::Plain,
        definition_level_encoding: Encoding::Rle,
        repetition_level_encoding: Encoding::Rle,
        statistics: None,
    };

    Ok(CompressedPage::V1(PageV1 {
        buffer,
        header,
        compression,
        uncompressed_page_size,
        dictionary_page: None,
    }))
}
