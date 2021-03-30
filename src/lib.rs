#[macro_use]
pub mod errors;

pub mod schema;

mod compression;
mod metadata;
mod types;

pub mod read;

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
