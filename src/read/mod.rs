mod compression;
pub use compression::decompress_page;
mod metadata;
mod page;
mod page_dict;
mod page_iterator;

pub use metadata::read_metadata;

use std::io::{Read, Seek, SeekFrom};

use crate::errors::Result;
use crate::metadata::{ParquetMetaData, RowGroupMetaData};

pub use page::Page;
pub use page_dict::{BinaryPageDict, PageDict, PrimitivePageDict};
pub use page_iterator::PageIterator;

/// Filters row group metadata to only those row groups,
/// for which the predicate function returns true
pub fn filter_row_groups(
    metadata: &ParquetMetaData,
    predicate: &dyn Fn(&RowGroupMetaData, usize) -> bool,
) -> ParquetMetaData {
    let mut filtered_row_groups = Vec::<RowGroupMetaData>::new();
    for (i, row_group_metadata) in metadata.row_groups().iter().enumerate() {
        if predicate(row_group_metadata, i) {
            filtered_row_groups.push(row_group_metadata.clone());
        }
    }
    ParquetMetaData::new(metadata.file_metadata().clone(), filtered_row_groups)
}

pub fn get_page_iterator<'b, RR: Read + Seek>(
    metadata: &ParquetMetaData,
    row_group: usize,
    column: usize,
    reader: &'b mut RR,
) -> Result<PageIterator<'b, RR>> {
    let column_metadata = metadata.row_group(row_group).column(column);
    let (col_start, _) = column_metadata.byte_range();
    reader.seek(SeekFrom::Start(col_start))?;
    PageIterator::try_new(
        reader,
        column_metadata.num_values(),
        *column_metadata.compression(),
        column_metadata.column_descriptor().clone(),
    )
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use page::Page;

    use super::*;

    use crate::tests::get_path;

    #[test]
    fn basic() -> Result<()> {
        let mut testdata = get_path();
        testdata.push("alltypes_plain.parquet");
        let mut file = File::open(testdata).unwrap();

        let metadata = read_metadata(&mut file)?;

        let row_group = 0;
        let column = 0;
        let mut iter = get_page_iterator(&metadata, row_group, column, &mut file)?;

        let a = iter.next().unwrap().unwrap();
        if let Page::V1(page) = &a {
            assert_eq!(page.num_values, 8)
        } else {
            panic!("Page not a dict");
        }
        Ok(())
    }
}
