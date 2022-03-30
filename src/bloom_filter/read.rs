use std::io::{Read, Seek, SeekFrom};

use parquet_format_async_temp::{
    thrift::protocol::TCompactInputProtocol, BloomFilterAlgorithm, BloomFilterCompression,
    BloomFilterHeader, SplitBlockAlgorithm, Uncompressed,
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
    let mut prot = TCompactInputProtocol::new(&mut reader);
    let header = BloomFilterHeader::read_from_in_protocol(&mut prot)?;

    if header.algorithm != BloomFilterAlgorithm::BLOCK(SplitBlockAlgorithm {}) {
        bitset.clear();
        return Ok(());
    }
    if header.compression != BloomFilterCompression::UNCOMPRESSED(Uncompressed {}) {
        bitset.clear();
        return Ok(());
    }
    // read bitset
    if header.num_bytes as usize > bitset.capacity() {
        *bitset = vec![0; header.num_bytes as usize]
    } else {
        bitset.clear();
        bitset.resize(header.num_bytes as usize, 0); // populate with zeros
    }

    reader.read_exact(bitset)?;
    Ok(())
}
