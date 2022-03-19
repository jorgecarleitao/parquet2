mod indexed_reader;
mod reader;
#[cfg(feature = "stream")]
mod stream;

use crate::{error::ParquetError, page::CompressedDataPage};

pub use indexed_reader::IndexedPageReader;
pub use reader::{PageFilter, PageReader};

pub trait PageIterator: Iterator<Item = Result<CompressedDataPage, ParquetError>> {
    fn swap_buffer(&mut self, buffer: &mut Vec<u8>);
}

#[cfg(feature = "stream")]
pub use stream::get_page_stream;
