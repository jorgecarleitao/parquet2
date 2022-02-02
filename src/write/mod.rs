mod column_chunk;
mod compression;
mod file;
mod page;
mod row_group;
pub(self) mod statistics;

#[cfg(feature = "stream")]
mod stream;
#[cfg(feature = "stream")]
pub use stream::FileStreamer;

mod dyn_iter;
pub use dyn_iter::{DynIter, DynStreamingIterator};

pub use compression::{compress, Compressor};

pub use file::FileWriter;

use crate::compression::Compression;
use crate::page::CompressedPage;

pub type RowGroupIter<'a, E> =
    DynIter<'a, std::result::Result<DynStreamingIterator<'a, CompressedPage, E>, E>>;

/// Write options of different interfaces on this crate
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct WriteOptions {
    /// Whether to write statistics
    pub write_statistics: bool,
    /// Whether to use compression
    pub compression: Compression,
    /// Which Parquet version to use
    pub version: Version,
}

/// The parquet version to use
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum Version {
    V1,
    V2,
}

impl From<Version> for i32 {
    fn from(version: Version) -> Self {
        match version {
            Version::V1 => 1,
            Version::V2 => 2,
        }
    }
}
