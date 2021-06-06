use parquet_format::{DataPageHeader, Encoding};

use crate::metadata::ColumnDescriptor;
use crate::read::PageHeader;
use crate::statistics::serialize_statistics;
use crate::statistics::PrimitiveStatistics;
use crate::statistics::Statistics;
use crate::write::WriteOptions;
use crate::{compression::create_codec, encoding::hybrid_rle::encode, error::Result};
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
    options: &WriteOptions,
    descriptor: &ColumnDescriptor,
) -> Result<CompressedPage> {
    let (values, mut buffer) = unzip_option(array)?;

    buffer.extend_from_slice(&values);
    let uncompressed_page_size = buffer.len();

    let codec = create_codec(&options.compression)?;
    let buffer = if let Some(mut codec) = codec {
        // todo: remove this allocation by extending `buffer` directly.
        // needs refactoring `compress`'s API.
        let mut tmp = vec![];
        codec.compress(&values, &mut tmp)?;
        tmp
    } else {
        buffer
    };

    let statistics = if options.write_statistics {
        let statistics = &PrimitiveStatistics {
            null_count: Some((array.len() - array.iter().flatten().count()) as i64),
            distinct_count: None,
            max_value: array.iter().flatten().max_by(|x, y| x.ord(y)).copied(),
            min_value: array.iter().flatten().min_by(|x, y| x.ord(y)).copied(),
        } as &dyn Statistics;
        Some(serialize_statistics(statistics))
    } else {
        None
    };

    let header = DataPageHeader {
        num_values: array.len() as i32,
        encoding: Encoding::Plain,
        definition_level_encoding: Encoding::Rle,
        repetition_level_encoding: Encoding::Rle,
        statistics,
    };

    Ok(CompressedPage::new(
        PageHeader::V1(header),
        buffer,
        options.compression,
        uncompressed_page_size,
        None,
        descriptor.clone(),
    ))
}
