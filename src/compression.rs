use std::io::{Read, Write};

pub use super::parquet_bridge::Compression;

use crate::error::{ParquetError, Result};

/// Compresses data stored in slice `input_buf` and writes the compressed result
/// to `output_buf`.
/// Note that you'll need to call `clear()` before reusing the same `output_buf`
/// across different `compress` calls.
pub fn compress(
    compression: Compression,
    input_buf: &[u8],
    output_buf: &mut Vec<u8>,
) -> Result<()> {
    match compression {
        #[cfg(feature = "brotli")]
        Compression::Brotli => {
            const BROTLI_DEFAULT_BUFFER_SIZE: usize = 4096;
            const BROTLI_DEFAULT_COMPRESSION_QUALITY: u32 = 1; // supported levels 0-9
            const BROTLI_DEFAULT_LG_WINDOW_SIZE: u32 = 22; // recommended between 20-22

            let mut encoder = brotli::CompressorWriter::new(
                output_buf,
                BROTLI_DEFAULT_BUFFER_SIZE,
                BROTLI_DEFAULT_COMPRESSION_QUALITY,
                BROTLI_DEFAULT_LG_WINDOW_SIZE,
            );
            encoder.write_all(input_buf)?;
            encoder.flush().map_err(|e| e.into())
        }
        #[cfg(feature = "gzip")]
        Compression::Gzip => {
            let mut encoder = flate2::write::GzEncoder::new(output_buf, Default::default());
            encoder.write_all(input_buf)?;
            encoder.try_finish().map_err(|e| e.into())
        }
        #[cfg(feature = "snappy")]
        Compression::Snappy => {
            use snap::raw::{max_compress_len, Encoder};

            let output_buf_len = output_buf.len();
            let required_len = max_compress_len(input_buf.len());
            output_buf.resize(output_buf_len + required_len, 0);
            let n = Encoder::new().compress(input_buf, &mut output_buf[output_buf_len..])?;
            output_buf.truncate(output_buf_len + n);
            Ok(())
        }
        #[cfg(feature = "lz4")]
        Compression::Lz4 => {
            const LZ4_BUFFER_SIZE: usize = 4096;
            let mut encoder = lz4::EncoderBuilder::new().build(output_buf)?;
            let mut from = 0;
            loop {
                let to = std::cmp::min(from + LZ4_BUFFER_SIZE, input_buf.len());
                encoder.write_all(&input_buf[from..to])?;
                from += LZ4_BUFFER_SIZE;
                if from >= input_buf.len() {
                    break;
                }
            }
            encoder.finish().1.map_err(|e| e.into())
        }
        #[cfg(feature = "zstd")]
        Compression::Zstd => {
            /// Compression level (1-21) for ZSTD. Choose 1 here for better compression speed.
            const ZSTD_COMPRESSION_LEVEL: i32 = 1;

            let mut encoder = zstd::Encoder::new(output_buf, ZSTD_COMPRESSION_LEVEL)?;
            encoder.write_all(input_buf)?;
            match encoder.finish() {
                Ok(_) => Ok(()),
                Err(e) => Err(e.into()),
            }
        }
        Compression::Uncompressed => {
            Err(general_err!("Compressing without compression is not valid"))
        }
        _ => Err(general_err!(
            "Compression {:?} is not installed",
            compression
        )),
    }
}

/// Decompresses data stored in slice `input_buf` and writes output to `output_buf`.
/// Returns the total number of bytes written.
pub fn decompress(compression: Compression, input_buf: &[u8], output_buf: &mut [u8]) -> Result<()> {
    match compression {
        #[cfg(feature = "brotli")]
        Compression::Brotli => {
            const BROTLI_DEFAULT_BUFFER_SIZE: usize = 4096;
            brotli::Decompressor::new(input_buf, BROTLI_DEFAULT_BUFFER_SIZE)
                .read_exact(output_buf)
                .map_err(|e| e.into())
        }
        #[cfg(feature = "gzip")]
        Compression::Gzip => {
            let mut decoder = flate2::read::GzDecoder::new(input_buf);
            decoder.read_exact(output_buf).map_err(|e| e.into())
        }
        #[cfg(feature = "snappy")]
        Compression::Snappy => {
            use snap::raw::{decompress_len, Decoder};

            let len = decompress_len(input_buf)?;
            assert!(len <= output_buf.len());
            Decoder::new()
                .decompress(input_buf, output_buf)
                .map_err(|e| e.into())
                .map(|_| ())
        }
        #[cfg(feature = "lz4")]
        Compression::Lz4 => {
            let mut decoder = lz4::Decoder::new(input_buf)?;
            decoder.read_exact(output_buf).map_err(|e| e.into())
        }
        #[cfg(feature = "zstd")]
        Compression::Zstd => {
            let mut decoder = zstd::Decoder::new(input_buf)?;
            decoder.read_exact(output_buf).map_err(|e| e.into())
        }
        Compression::Uncompressed => {
            Err(general_err!("Compressing without compression is not valid"))
        }
        _ => Err(general_err!(
            "Compression {:?} is not installed",
            compression
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_roundtrip(c: Compression, data: &[u8]) {
        let offset = 2;

        // Compress to a buffer that already has data is possible
        let mut compressed = vec![2; offset];
        compress(c, data, &mut compressed).expect("Error when compressing");

        // data is compressed...
        assert!(compressed.len() - 2 < data.len());

        let mut decompressed = vec![0; data.len()];
        decompress(c, &compressed[offset..], &mut decompressed).expect("Error when decompressing");
        assert_eq!(data, decompressed.as_slice());
    }

    fn test_codec(c: Compression) {
        let sizes = vec![10000, 100000];
        for size in sizes {
            let data = (0..size).map(|x| (x % 255) as u8).collect::<Vec<_>>();
            test_roundtrip(c, &data);
        }
    }

    #[test]
    fn test_codec_snappy() {
        test_codec(Compression::Snappy);
    }

    #[test]
    fn test_codec_gzip() {
        test_codec(Compression::Gzip);
    }

    #[test]
    fn test_codec_brotli() {
        test_codec(Compression::Brotli);
    }

    #[test]
    fn test_codec_lz4() {
        test_codec(Compression::Lz4);
    }

    #[test]
    fn test_codec_zstd() {
        test_codec(Compression::Zstd);
    }
}
