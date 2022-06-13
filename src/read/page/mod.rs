mod indexed_reader;
mod reader;
mod stream;

use crate::{error::Error, page::CompressedDataPage};

pub use indexed_reader::IndexedPageReader;
pub use reader::{PageFilter, PageMetaData, PageReader};

pub trait PageIterator: Iterator<Item = Result<CompressedDataPage, Error>> {
    fn swap_buffer(&mut self, buffer: &mut Vec<u8>);
}

pub use stream::get_page_stream;
pub use stream::get_page_stream_from_column_start;
