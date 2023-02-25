use std::io::Write;

use parquet_format_safe::{
    thrift::protocol::TCompactOutputProtocol, BloomFilterAlgorithm, BloomFilterCompression,
    BloomFilterHash, BloomFilterHeader, SplitBlockAlgorithm, Uncompressed, XxHash,
};

use crate::error::Error;

pub fn write_to_protocol<W: Write>(
    protocol: &mut TCompactOutputProtocol<W>,
    num_bytes: i32,
) -> Result<(), Error> {
    let header = BloomFilterHeader {
        num_bytes,
        algorithm: BloomFilterAlgorithm::BLOCK(SplitBlockAlgorithm {}),
        hash: BloomFilterHash::XXHASH(XxHash {}),
        compression: BloomFilterCompression::UNCOMPRESSED(Uncompressed {}),
    };

    header.write_to_out_protocol(protocol)?;

    Ok(())
}
