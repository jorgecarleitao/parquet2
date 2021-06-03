pub(crate) mod primitive;

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

    use crate::tests::alltypes_plain;
    use crate::write::write_file;

    use crate::metadata::SchemaDescriptor;

    use super::*;
    use crate::error::Result;

    fn read_column<R: Read + Seek>(reader: &mut R) -> Result<Array> {
        let (a, _) = super::super::read::tests::read_column(reader, 0, 0)?;
        Ok(a)
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
        let schema = SchemaDescriptor::try_from_message(&format!(
            "message schema {{ OPTIONAL {} col; }}",
            a
        ))?;

        let mut writer = Cursor::new(vec![]);
        write_file(
            &mut writer,
            row_groups,
            schema,
            CompressionCodec::Uncompressed,
            None,
            None,
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
