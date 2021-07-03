mod column_chunk;
mod file;
mod page;
mod row_group;
pub(self) mod statistics;

#[cfg(feature = "stream")]
pub mod stream;

mod dyn_iter;
pub use dyn_iter::DynIter;

pub use file::write_file;
use parquet_format::CompressionCodec;

use crate::read::CompressedPage;

pub type RowGroupIter<'a, E> =
    DynIter<'a, std::result::Result<DynIter<'a, std::result::Result<CompressedPage, E>>, E>>;

#[derive(Debug, Copy, Clone)]
pub struct WriteOptions {
    pub write_statistics: bool,
    pub compression: CompressionCodec,
}
