mod column_chunk;
mod file;
mod page;
mod row_group;
pub(self) mod statistics;

#[cfg(feature = "stream")]
pub mod stream;
#[cfg(feature = "stream")]
mod stream_stream;

mod dyn_iter;
pub use dyn_iter::DynIter;

pub use file::write_file;

use crate::compression::Compression;
use crate::page::CompressedPage;

pub type RowGroupIter<'a, E> =
    DynIter<'a, std::result::Result<DynIter<'a, std::result::Result<CompressedPage, E>>, E>>;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct WriteOptions {
    pub write_statistics: bool,
    pub compression: Compression,
    pub version: Version,
}

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
