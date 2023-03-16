use std::io::{Read, Seek, SeekFrom};

use parquet_format_safe::{
    thrift::protocol::TCompactInputProtocol, BloomFilterAlgorithm, BloomFilterCompression,
    BloomFilterHeader, SplitBlockAlgorithm, Uncompressed,
};

#[cfg(feature = "async")]
use {
    futures::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt},
    parquet_format_safe::thrift::protocol::TCompactInputStreamProtocol,
};

use crate::{error::Error, metadata::ColumnChunkMetaData};

/// Reads the bloom filter associated to [`ColumnChunkMetaData`] into `bitset`.
/// Results in an empty `bitset` if there is no associated bloom filter or the algorithm is not supported.
/// # Error
/// Errors if the column contains no metadata or the filter can't be read or deserialized.
pub fn read<R: Read + Seek>(
    column_metadata: &ColumnChunkMetaData,
    mut reader: &mut R,
    bitset: &mut Vec<u8>,
) -> Result<(), Error> {
    let offset = column_metadata.metadata().bloom_filter_offset;

    let offset = if let Some(offset) = offset {
        offset as u64
    } else {
        bitset.clear();
        return Ok(());
    };
    reader.seek(SeekFrom::Start(offset))?;

    // deserialize header
    let mut prot = TCompactInputProtocol::new(&mut reader, usize::MAX); // max is ok since `BloomFilterHeader` never allocates
    let header = BloomFilterHeader::read_from_in_protocol(&mut prot)?;

    if header.algorithm != BloomFilterAlgorithm::BLOCK(SplitBlockAlgorithm {}) {
        bitset.clear();
        return Ok(());
    }
    if header.compression != BloomFilterCompression::UNCOMPRESSED(Uncompressed {}) {
        bitset.clear();
        return Ok(());
    }

    let length: usize = header.num_bytes.try_into()?;

    bitset.clear();
    bitset.try_reserve(length)?;
    reader.take(length as u64).read_to_end(bitset)?;

    Ok(())
}

/// Reads the bloom filter associated to [`ColumnChunkMetaData`] into `bitset`.
/// Results in an empty `bitset` if there is no associated bloom filter or the algorithm is not supported.
/// # Error
/// Errors if the column contains no metadata or the filter can't be read or deserialized.
#[cfg(feature = "async")]
pub async fn read_async<R: AsyncRead + AsyncSeek + Send + Unpin>(
    column_metadata: &ColumnChunkMetaData,
    mut reader: &mut R,
    bitset: &mut Vec<u8>,
) -> Result<(), Error> {
    let offset = column_metadata.metadata().bloom_filter_offset;

    let offset = if let Some(offset) = offset {
        offset as u64
    } else {
        bitset.clear();
        return Ok(());
    };
    reader.seek(SeekFrom::Start(offset)).await?;

    // deserialize header
    let mut prot = TCompactInputStreamProtocol::new(&mut reader, usize::MAX); // max is ok since `BloomFilterHeader` never allocates
    let header = BloomFilterHeader::stream_from_in_protocol(&mut prot).await?;

    if header.algorithm != BloomFilterAlgorithm::BLOCK(SplitBlockAlgorithm {}) {
        bitset.clear();
        return Ok(());
    }
    if header.compression != BloomFilterCompression::UNCOMPRESSED(Uncompressed {}) {
        bitset.clear();
        return Ok(());
    }

    let length: usize = header.num_bytes.try_into()?;

    bitset.clear();
    bitset.try_reserve(length)?;
    reader.take(length as u64).read_to_end(bitset).await?;

    Ok(())
}
