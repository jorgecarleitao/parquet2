#[macro_use]
pub mod errors;

pub mod schema;

mod compression;
mod encoding;
mod metadata;
pub mod serialization;
mod types;
pub use types::int96_to_i64;

pub mod read;

const FOOTER_SIZE: u64 = 8;
const PARQUET_MAGIC: [u8; 4] = [b'P', b'A', b'R', b'1'];

/// The number of bytes read at the end of the parquet file on first read
const DEFAULT_FOOTER_READ_SIZE: u64 = 64 * 1024;

#[cfg(test)]
mod tests {
    use read::{get_page_iterator, read_metadata};
    use serialization::native::{page_to_vec, Array};
    use types::int96_to_i64;

    use super::*;
    use crate::{errors::Result, metadata::ColumnDescriptor, read::Page};
    use std::path::PathBuf;

    pub fn get_path() -> PathBuf {
        let dir = env!("CARGO_MANIFEST_DIR");

        PathBuf::from(dir).join("testing/parquet-testing/data")
    }

    use std::fs::File;

    fn prepare(
        path: &str,
        row_group: usize,
        column: usize,
        mut testdata: PathBuf,
    ) -> Result<(ColumnDescriptor, Vec<Page>)> {
        testdata.push(path);
        let mut file = File::open(testdata).unwrap();

        let metadata = read_metadata(&mut file)?;
        let descriptor = metadata
            .row_group(row_group)
            .column(column)
            .column_descriptor()
            .clone();
        Ok((
            descriptor,
            get_page_iterator(&metadata, row_group, column, &mut file)?
                .collect::<Result<Vec<_>>>()?,
        ))
    }

    fn get_column(column: usize) -> Result<Array> {
        let (descriptor, pages) = prepare("alltypes_plain.parquet", 0, column, get_path())?;
        assert_eq!(pages.len(), 1);

        page_to_vec(&pages[0], &descriptor)
    }

    fn test_column(column: usize, expected: &Array) -> Result<()> {
        let result = get_column(column)?;
        assert_eq!(&result, expected);
        Ok(())
    }

    #[test]
    fn int32() -> Result<()> {
        let expected = vec![4, 5, 6, 7, 2, 3, 0, 1];
        let expected = expected.into_iter().map(Some).collect::<Vec<_>>();
        let expected = Array::Int32(expected);
        test_column(0, &expected)
    }

    #[test]
    fn bool() -> Result<()> {
        let expected = vec![true, false, true, false, true, false, true, false];
        let expected = expected.into_iter().map(Some).collect::<Vec<_>>();
        let expected = Array::Boolean(expected);
        test_column(1, &expected)
    }

    #[test]
    fn tiny_int() -> Result<()> {
        let expected = vec![0, 1, 0, 1, 0, 1, 0, 1];
        let expected = expected.into_iter().map(Some).collect::<Vec<_>>();
        let expected = Array::Int32(expected);
        test_column(2, &expected)
    }

    #[test]
    fn smallint_col() -> Result<()> {
        let expected = vec![0, 1, 0, 1, 0, 1, 0, 1];
        let expected = expected.into_iter().map(Some).collect::<Vec<_>>();
        let expected = Array::Int32(expected);
        test_column(3, &expected)
    }

    #[test]
    fn int_col() -> Result<()> {
        let expected = vec![0, 1, 0, 1, 0, 1, 0, 1];
        let expected = expected.into_iter().map(Some).collect::<Vec<_>>();
        let expected = Array::Int32(expected);
        test_column(4, &expected)
    }

    #[test]
    fn bigint_col() -> Result<()> {
        let expected = vec![0, 10, 0, 10, 0, 10, 0, 10];
        let expected = expected.into_iter().map(Some).collect::<Vec<_>>();
        let expected = Array::Int64(expected);
        test_column(5, &expected)
    }

    #[test]
    fn float32_col() -> Result<()> {
        let expected = vec![0.0, 1.1, 0.0, 1.1, 0.0, 1.1, 0.0, 1.1];
        let expected = expected.into_iter().map(Some).collect::<Vec<_>>();
        let expected = Array::Float32(expected);
        test_column(6, &expected)
    }

    #[test]
    fn float64_col() -> Result<()> {
        let expected = vec![0.0, 10.1, 0.0, 10.1, 0.0, 10.1, 0.0, 10.1];
        let expected = expected.into_iter().map(Some).collect::<Vec<_>>();
        let expected = Array::Float64(expected);
        test_column(7, &expected)
    }

    #[test]
    fn date_string_col() -> Result<()> {
        let expected = vec![
            vec![48, 51, 47, 48, 49, 47, 48, 57],
            vec![48, 51, 47, 48, 49, 47, 48, 57],
            vec![48, 52, 47, 48, 49, 47, 48, 57],
            vec![48, 52, 47, 48, 49, 47, 48, 57],
            vec![48, 50, 47, 48, 49, 47, 48, 57],
            vec![48, 50, 47, 48, 49, 47, 48, 57],
            vec![48, 49, 47, 48, 49, 47, 48, 57],
            vec![48, 49, 47, 48, 49, 47, 48, 57],
        ];
        let expected = expected.into_iter().map(Some).collect::<Vec<_>>();
        let expected = Array::Binary(expected);
        test_column(8, &expected)
    }

    #[test]
    fn string_col() -> Result<()> {
        let expected = vec![
            vec![48],
            vec![49],
            vec![48],
            vec![49],
            vec![48],
            vec![49],
            vec![48],
            vec![49],
        ];
        let expected = expected.into_iter().map(Some).collect::<Vec<_>>();
        let expected = Array::Binary(expected);
        test_column(9, &expected)
    }

    #[test]
    fn timestamp_col() -> Result<()> {
        let expected = vec![
            1235865600000i64,
            1235865660000,
            1238544000000,
            1238544060000,
            1233446400000,
            1233446460000,
            1230768000000,
            1230768060000,
        ];

        let expected = expected.into_iter().map(Some).collect::<Vec<_>>();
        let result = get_column(10)?;
        if let Array::Int96(result) = result {
            let a = result
                .into_iter()
                .map(|x| x.map(int96_to_i64))
                .collect::<Vec<_>>();
            assert_eq!(expected, a);
        } else {
            panic!("Timestamp expected");
        };
        Ok(())
    }

    #[test]
    fn test_pyarrow_integration() -> Result<()> {
        let column = 0;
        let path = "pyarrow3/basic_nulls.parquet";
        let (descriptor, pages) = prepare(path, 0, column, "fixtures".into())?;
        assert_eq!(pages.len(), 1);

        let array = page_to_vec(&pages[0], &descriptor)?;

        let expected = Array::Int64(vec![
            Some(0),
            Some(1),
            None,
            Some(3),
            None,
            Some(5),
            Some(6),
            Some(7),
            None,
            Some(9),
        ]);

        assert_eq!(array, expected,);

        Ok(())
    }
}
