mod primitive;

use parquet_format::CompressionCodec;

use crate::{error::Result, read::CompressedPage};

use super::read::Array;

pub fn array_to_page(array: &Array) -> Result<CompressedPage> {
    // using plain encoding format
    let compression = CompressionCodec::Uncompressed;
    match array {
        Array::Int32(array) => primitive::array_to_page_v1(&array, compression),
        Array::Int64(array) => primitive::array_to_page_v1(&array, compression),
        Array::Int96(array) => primitive::array_to_page_v1(&array, compression),
        Array::Float32(array) => primitive::array_to_page_v1(&array, compression),
        Array::Float64(array) => primitive::array_to_page_v1(&array, compression),
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

    fn read_column<R: Read + Seek>(reader: &mut R) -> Result<Array> {
        let (descriptor, mut pages) = get_pages(reader, 0, 0)?;
        assert_eq!(pages.len(), 1);

        let page = pages.pop().unwrap();

        page_to_array(page, &descriptor)
    }

    fn test_column(column: usize) -> Result<()> {
        let array = alltypes_plain(column);

        let row_groups = std::iter::once(Ok(std::iter::once(Ok(std::iter::once(array_to_page(
            &array,
        ))))));

        // prepare schema
        let a = match array {
            Array::Int32(_) => "INT32",
            Array::Int64(_) => "INT64",
            Array::Int96(_) => "INT96",
            Array::Float32(_) => "FLOAT",
            Array::Float64(_) => "DOUBLE",
            _ => todo!(),
        };
        let schema = SchemaDescriptor::new(from_message(&format!(
            "message schema {{ OPTIONAL {} col; }}",
            a
        ))?);

        let mut writer = Cursor::new(vec![]);
        write_file(
            &mut writer,
            &schema,
            CompressionCodec::Uncompressed,
            row_groups,
        )?;

        let data = writer.into_inner();

        let result = read_column(&mut Cursor::new(data))?;
        assert_eq!(array, result);
        Ok(())
    }

    #[test]
    fn int32() -> Result<()> {
        test_column(0)
    }

    #[test]
    #[ignore = "Native boolean writer not yet implemented"]
    fn bool() -> Result<()> {
        test_column(1)
    }

    #[test]
    fn tiny_int() -> Result<()> {
        test_column(2)
    }

    #[test]
    fn smallint_col() -> Result<()> {
        test_column(3)
    }

    #[test]
    fn int_col() -> Result<()> {
        test_column(4)
    }

    #[test]
    fn bigint_col() -> Result<()> {
        test_column(5)
    }

    #[test]
    fn float32_col() -> Result<()> {
        test_column(6)
    }

    #[test]
    fn float64_col() -> Result<()> {
        test_column(7)
    }
}
