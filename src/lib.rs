#![forbid(unsafe_code)]
/// Unofficial implementation of parquet IO in Rust.

#[macro_use]
pub mod error;
pub mod compression;
pub mod encoding;
pub mod metadata;
pub mod page;
mod parquet_bridge;
pub mod read;
pub mod schema;
pub mod statistics;
pub mod types;
pub mod write;

pub use fallible_streaming_iterator;
pub use fallible_streaming_iterator::FallibleStreamingIterator;

const FOOTER_SIZE: u64 = 8;
const PARQUET_MAGIC: [u8; 4] = [b'P', b'A', b'R', b'1'];

/// The number of bytes read at the end of the parquet file on first read
const DEFAULT_FOOTER_READ_SIZE: u64 = 64 * 1024;

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    pub fn get_path() -> PathBuf {
        let dir = env!("CARGO_MANIFEST_DIR");

        PathBuf::from(dir).join("testing/parquet-testing/data")
    }
}
