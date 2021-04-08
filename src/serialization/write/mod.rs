mod primitive;

use parquet_format::CompressionCodec;

use crate::{error::Result, read::CompressedPage};

use super::read::Array;

pub fn array_to_page(array: &Array) -> Result<CompressedPage> {
    // using plain encoding format
    match array {
        Array::Int32(array) => primitive::array_to_page_v1(&array, CompressionCodec::Uncompressed),
        _ => todo!(),
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Read, Seek};

    use io_message::from_message;

    use crate::read::{get_page_iterator, read_metadata};
    use crate::schema::io_message;
    use crate::tests::alltypes_plain;
    use crate::write::write_file;

    use crate::metadata::SchemaDescriptor;
    use crate::serialization::read::page_to_array;

    use super::*;
    use crate::{error::Result, metadata::ColumnDescriptor, read::CompressedPage};

    fn get_pages<R: Read + Seek>(
        reader: &mut R,
        row_group: usize,
        column: usize,
    ) -> Result<(ColumnDescriptor, Vec<CompressedPage>)> {
        let metadata = read_metadata(reader)?;
        let descriptor = metadata.row_groups[row_group]
            .column(column)
            .column_descriptor()
            .clone();
        Ok((
            descriptor,
            get_page_iterator(&metadata, row_group, column, reader)?.collect::<Result<Vec<_>>>()?,
        ))
    }

    fn get_column<R: Read + Seek>(reader: &mut R) -> Result<Array> {
        let (descriptor, mut pages) = get_pages(reader, 0, 0)?;
        assert_eq!(pages.len(), 1);

        let page = pages.pop().unwrap();

        page_to_array(page, &descriptor)
    }

    #[test]
    fn int32() -> Result<()> {
        let array = alltypes_plain(0);

        let row_groups = std::iter::once(Ok(std::iter::once(Ok(std::iter::once(array_to_page(
            &array,
        ))))));

        let schema = SchemaDescriptor::new(from_message("message schema { OPTIONAL INT32 col; }")?);

        let mut writer = Cursor::new(vec![]);
        write_file(
            &mut writer,
            &schema,
            CompressionCodec::Uncompressed,
            row_groups,
        )?;

        let data = writer.into_inner();

        let a = get_column(&mut Cursor::new(data))?;
        assert_eq!(a, array);

        Ok(())
    }
}
