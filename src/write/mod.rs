mod column_chunk;
//mod compression;
mod file;
mod page;
mod row_group;
pub(self) mod statistics;

pub use file::write_file;
use parquet_format::CompressionCodec;

#[derive(Debug, Copy, Clone)]
pub struct WriteOptions {
    pub write_statistics: bool,
    pub compression: CompressionCodec,
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    use crate::{
        error::Result, metadata::SchemaDescriptor, read::read_metadata,
        serialization::write::primitive::array_to_page_v1,
    };

    #[test]
    fn basic() -> Result<()> {
        let array = vec![
            Some(0),
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(6),
        ];

        let options = WriteOptions {
            write_statistics: false,
            compression: CompressionCodec::Uncompressed,
        };

        let schema = SchemaDescriptor::try_from_message("message schema { OPTIONAL INT32 col; }")?;

        let row_groups = std::iter::once(Ok(std::iter::once(Ok(std::iter::once(
            array_to_page_v1(&array, &options, &schema.columns()[0]),
        )))));

        let mut writer = Cursor::new(vec![]);
        write_file(&mut writer, row_groups, schema, options, None, None)?;

        let data = writer.into_inner();
        let mut reader = Cursor::new(data);

        let metadata = read_metadata(&mut reader)?;

        // validated against an equivalent array produced by pyarrow.
        let expected = 51;
        assert_eq!(
            metadata.row_groups[0].columns()[0].uncompressed_size(),
            expected
        );

        Ok(())
    }
}
