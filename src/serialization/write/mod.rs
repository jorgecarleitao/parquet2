pub(crate) mod primitive;

use crate::{error::Result, metadata::ColumnDescriptor, read::CompressedPage, write::WriteOptions};

use super::read::Array;

pub fn array_to_page(
    array: &Array,
    options: &WriteOptions,
    descriptor: &ColumnDescriptor,
) -> Result<CompressedPage> {
    // using plain encoding format
    match array {
        Array::Int32(array) => primitive::array_to_page_v1(&array, options, descriptor),
        Array::Int64(array) => primitive::array_to_page_v1(&array, options, descriptor),
        Array::Int96(array) => primitive::array_to_page_v1(&array, options, descriptor),
        Array::Float32(array) => primitive::array_to_page_v1(&array, options, descriptor),
        Array::Float64(array) => primitive::array_to_page_v1(&array, options, descriptor),
        _ => todo!(),
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Read, Seek};
    use std::sync::Arc;

    use crate::tests::{alltypes_plain, alltypes_statistics};
    use crate::write::{write_file, DynIter};

    use crate::compression::CompressionCodec;
    use crate::metadata::SchemaDescriptor;
    use crate::statistics::Statistics;

    use super::*;
    use crate::error::Result;

    fn read_column<R: Read + Seek>(reader: &mut R) -> Result<(Array, Option<Arc<dyn Statistics>>)> {
        let (a, statistics) = super::super::read::tests::read_column(reader, 0, 0)?;
        Ok((a, statistics))
    }

    fn test_column(column: usize) -> Result<()> {
        let array = alltypes_plain(column);

        let options = WriteOptions {
            write_statistics: true,
            compression: CompressionCodec::Uncompressed,
        };

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

        let a = schema.columns();

        let row_groups = std::iter::once(Ok(DynIter::new(std::iter::once(Ok(DynIter::new(
            std::iter::once(array_to_page(&array, &options, &a[0])),
        ))))));

        let mut writer = Cursor::new(vec![]);
        write_file(&mut writer, row_groups, schema, options, None, None)?;

        let data = writer.into_inner();

        let (result, statistics) = read_column(&mut Cursor::new(data))?;
        assert_eq!(array, result);
        let stats = alltypes_statistics(column);
        assert_eq!(
            statistics.as_ref().map(|x| x.as_ref()),
            Some(stats).as_ref().map(|x| x.as_ref())
        );
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
