//! Subcommand `dump`. This subcommand shows the parquet metadata information
use parquet2::{
    error::{Error, Result},
    page::Page,
    read::{decompress, get_page_iterator, read_metadata},
    schema::types::PhysicalType,
    types::{decode, NativeType},
};

use std::{fs::File, io::Write, path::Path};

use crate::SEPARATOR;

pub struct PrimitivePageDict<T: NativeType> {
    values: Vec<T>,
}

impl<T: NativeType> PrimitivePageDict<T> {
    pub fn new(values: Vec<T>) -> Self {
        Self { values }
    }

    pub fn values(&self) -> &[T] {
        &self.values
    }

    #[inline]
    pub fn value(&self, index: usize) -> Result<&T> {
        let a = self.values.get(index).ok_or_else(|| {
            Error::OutOfSpec(
                "The data page has an index larger than the dictionary page values".to_string(),
            )
        });

        a
    }
}

// Dumps data from the file.
// The function prints a sample of the data from each of the RowGroups.
// The the columns to be printed and the sample size is controlled using the
// arguments introduced in the command line
pub fn dump_file<T, W>(
    file_name: T,
    sample_size: usize,
    columns: Option<Vec<usize>>,
    writer: &mut W,
) -> Result<()>
where
    T: AsRef<Path> + std::fmt::Display,
    W: Write,
{
    let mut file = File::open(file_name)?;

    let metadata = read_metadata(&mut file)?;

    let columns = match columns {
        Some(cols) => cols,
        None => {
            let num_cols = metadata.schema().fields().len();
            (0..num_cols).collect()
        }
    };

    for (i, group) in metadata.row_groups.iter().enumerate() {
        writeln!(
            writer,
            "Group: {:<10}Rows: {:<15} Bytes: {:}",
            i,
            group.num_rows(),
            group.total_byte_size()
        )?;
        writeln!(writer, "{}", SEPARATOR)?;

        for column in &columns {
            let column_meta = &group.columns()[*column];
            let iter = get_page_iterator(
                column_meta,
                &mut file,
                None,
                Vec::with_capacity(4 * 1024),
                1024 * 1024,
            )?;

            let mut decompress_buffer = vec![];
            for (page_ind, page) in iter.enumerate() {
                let page = page?;

                writeln!(writer, "\nPage: {:<10}Column: {:<15}", page_ind, column,)?;

                let page = decompress(page, &mut decompress_buffer)?;
                match page {
                    Page::Dict(_) => {
                        todo!()
                    }
                    Page::Data(page) => {
                        match page.descriptor.primitive_type.physical_type {
                            PhysicalType::Int32 => {
                                print_page::<i32, W>(page.buffer(), sample_size, true, writer)?
                            }
                            PhysicalType::Int64 => {
                                print_page::<i64, W>(page.buffer(), sample_size, true, writer)?
                            }
                            PhysicalType::Float => {
                                print_page::<f32, W>(page.buffer(), sample_size, true, writer)?
                            }
                            PhysicalType::Double => {
                                print_page::<f64, W>(page.buffer(), sample_size, true, writer)?
                            }
                            _ => continue,
                        };
                    }
                }
            }
        }
    }

    Ok(())
}

pub fn read<T: NativeType>(
    buf: &[u8],
    num_values: usize,
    _is_sorted: bool,
) -> Result<PrimitivePageDict<T>> {
    let size_of = std::mem::size_of::<T>();

    let typed_size = num_values.wrapping_mul(size_of);

    let values = buf.get(..typed_size).ok_or_else(|| {
        Error::OutOfSpec(
            "The number of values declared in the dict page does not match the length of the page"
                .to_string(),
        )
    })?;

    let values = values.chunks_exact(size_of).map(decode::<T>).collect();

    Ok(PrimitivePageDict::new(values))
}

fn print_page<T, W>(buffer: &[u8], sample_size: usize, sorted: bool, writer: &mut W) -> Result<()>
where
    T: NativeType,
    W: Write,
{
    let dict = read::<T>(buffer, sample_size, sorted)?;
    print_iterator(dict.values().iter(), sample_size, writer)
}

fn print_iterator<I, T, W>(iter: I, sample_size: usize, writer: &mut W) -> Result<()>
where
    I: Iterator<Item = T>,
    T: std::fmt::Debug,
    W: Write,
{
    for (i, val) in iter.enumerate().take(sample_size) {
        writeln!(writer, "Value: {:<10}\t{:?}", i, val)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_show_meta() {
        let file_name = "data/sample.parquet";
        let mut buf = Vec::new();

        dump_file(file_name, 1, None, &mut buf).unwrap();

        let string_output = String::from_utf8(buf).unwrap();

        let expected = "Group: 0         Rows: 100            Bytes: 9597
--------------------------------------------------

Page: 0         Column: 0               Bytes: 100
Compressed page: PageV1          Physical type: Int64
Value: 0         \t97007

Page: 0         Column: 1               Bytes: 100
Compressed page: PageV1          Physical type: ByteArray
Value: 0         \t\"Leif Tommy Prim\"

Page: 0         Column: 2               Bytes: 22
Compressed page: PageV1          Physical type: ByteArray
Value: 0         \t\"M\"

Page: 0         Column: 3               Bytes: 83
Compressed page: PageV1          Physical type: Double
Value: 0         \t20.0

Page: 0         Column: 4               Bytes: 83
Compressed page: PageV1          Physical type: Double
Value: 0         \t185.0

Page: 0         Column: 5               Bytes: 83
Compressed page: PageV1          Physical type: Double
Value: 0         \t76.0

Page: 0         Column: 6               Bytes: 87
Compressed page: PageV1          Physical type: ByteArray
Value: 0         \t\"Sweden\"

Page: 0         Column: 7               Bytes: 87
Compressed page: PageV1          Physical type: ByteArray
Value: 0         \t\"SWE\"

Page: 0         Column: 8               Bytes: 87
Compressed page: PageV1          Physical type: ByteArray
Value: 0         \t\"1976 Summer\"

Page: 0         Column: 9               Bytes: 74
Compressed page: PageV1          Physical type: Int64
Value: 0         \t1976

Page: 0         Column: 10              Bytes: 23
Compressed page: PageV1          Physical type: ByteArray
Value: 0         \t\"Summer\"

Page: 0         Column: 11              Bytes: 87
Compressed page: PageV1          Physical type: ByteArray
Value: 0         \t\"Montreal\"

Page: 0         Column: 12              Bytes: 74
Compressed page: PageV1          Physical type: ByteArray
Value: 0         \t\"Cycling\"

Page: 0         Column: 13              Bytes: 100
Compressed page: PageV1          Physical type: ByteArray
Value: 0         \t\"Cycling Men\\\'s 100 kilometres Team Time Trial\"

Page: 0         Column: 14              Bytes: 26
Compressed page: PageV1          Physical type: ByteArray
Value: 0         \t\"Bronze\"

Page: 0         Column: 15              Bytes: 100
Compressed page: PageV1          Physical type: Int64
Value: 0         \t193210\n";

        assert_eq!(expected, string_output);
    }

    #[test]
    fn test_show_meta_cols() {
        let file_name = "data/sample.parquet";
        let mut buf = Vec::new();

        let columns = Some(vec![0, 2, 5]);

        dump_file(file_name, 1, columns, &mut buf).unwrap();

        let string_output = String::from_utf8(buf).unwrap();

        let expected = "Group: 0         Rows: 100            Bytes: 9597
--------------------------------------------------

Page: 0         Column: 0               Bytes: 100
Compressed page: PageV1          Physical type: Int64
Value: 0         \t97007

Page: 0         Column: 2               Bytes: 22
Compressed page: PageV1          Physical type: ByteArray
Value: 0         \t\"M\"

Page: 0         Column: 5               Bytes: 83
Compressed page: PageV1          Physical type: Double
Value: 0         \t76.0\n";

        assert_eq!(expected, string_output);
    }
}
