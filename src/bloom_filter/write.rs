use std::io::Write;

use parquet_format_safe::{
    thrift::protocol::TCompactOutputProtocol, BloomFilterAlgorithm, BloomFilterCompression,
    BloomFilterHash, BloomFilterHeader, SplitBlockAlgorithm, Uncompressed, XxHash,
};

#[cfg(feature = "async")]
use futures::AsyncWrite;
#[cfg(feature = "async")]
use parquet_format_safe::thrift::protocol::TCompactOutputStreamProtocol;

use crate::error::Error;

pub(crate) fn write_to_protocol<W: Write>(
    protocol: &mut TCompactOutputProtocol<W>,
    num_bytes: i32,
) -> Result<usize, Error> {
    let header = BloomFilterHeader {
        num_bytes,
        algorithm: BloomFilterAlgorithm::BLOCK(SplitBlockAlgorithm {}),
        hash: BloomFilterHash::XXHASH(XxHash {}),
        compression: BloomFilterCompression::UNCOMPRESSED(Uncompressed {}),
    };

    let written = header.write_to_out_protocol(protocol)?;

    Ok(written)
}

#[cfg(feature = "async")]
pub(crate) async fn write_to_stream_protocol<W: AsyncWrite + Unpin + Send>(
    protocol: &mut TCompactOutputStreamProtocol<W>,
    num_bytes: i32,
) -> Result<usize, Error> {
    let header = BloomFilterHeader {
        num_bytes,
        algorithm: BloomFilterAlgorithm::BLOCK(SplitBlockAlgorithm {}),
        hash: BloomFilterHash::XXHASH(XxHash {}),
        compression: BloomFilterCompression::UNCOMPRESSED(Uncompressed {}),
    };

    let written = header.write_to_out_stream_protocol(protocol).await?;

    Ok(written)
}
