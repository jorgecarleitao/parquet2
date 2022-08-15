//! Subcommand `dump`. This subcommand shows the parquet metadata information
use parquet2::{
    deserialize::{native_cast, HybridRleBooleanIter, HybridRleIter},
    encoding::{hybrid_rle, Encoding},
    error::{Error, Result},
    page::{split_buffer, DataPage, DictPage, Page},
    read::{decompress, get_page_iterator, read_metadata},
    schema::types::PhysicalType,
    types::{decode, NativeType},
};

use std::{fs::File, io::Write, path::Path};

use crate::SEPARATOR;

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

            let descriptor = column_meta.descriptor();
            let physical_type = descriptor.descriptor.primitive_type.physical_type;

            let mut decompress_buffer = vec![];
            for (page_ind, page) in iter.enumerate() {
                let page = page?;

                writeln!(writer, "\nPage: {:<10}Column: {:<15}", page_ind, column,)?;

                let page = decompress(page, &mut decompress_buffer)?;
                match page {
                    Page::Dict(page) => match physical_type {
                        PhysicalType::Int32 => {
                            print_dict_page::<i32, W>(page, sample_size, writer)?
                        }
                        PhysicalType::Int64 => {
                            print_dict_page::<i64, W>(page, sample_size, writer)?
                        }
                        PhysicalType::Float => {
                            print_dict_page::<f32, W>(page, sample_size, writer)?
                        }
                        PhysicalType::Double => {
                            print_dict_page::<f64, W>(page, sample_size, writer)?
                        }
                        _ => continue,
                    },
                    Page::Data(page) => {
                        match (physical_type, page.encoding()) {
                            (PhysicalType::Int32, Encoding::Plain) => {
                                print_page::<i32, W>(page, sample_size, writer)?
                            }
                            (
                                PhysicalType::Int32,
                                Encoding::RleDictionary | Encoding::PlainDictionary,
                            ) => print_rle_page::<i32, W>(page, sample_size, writer)?,
                            (PhysicalType::Int64, Encoding::Plain) => {
                                print_page::<i64, W>(page, sample_size, writer)?
                            }
                            (PhysicalType::Float, Encoding::Plain) => {
                                print_page::<f32, W>(page, sample_size, writer)?
                            }
                            (PhysicalType::Double, Encoding::Plain) => {
                                print_page::<f64, W>(page, sample_size, writer)?
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

fn print_dict_page<T: NativeType, W: Write>(
    page: DictPage,
    num_values: usize,
    writer: &mut W,
) -> Result<()> {
    let size_of = std::mem::size_of::<T>();

    let typed_size = num_values.wrapping_mul(size_of);

    let values = page.buffer.get(..typed_size).ok_or_else(|| {
        Error::OutOfSpec(
            "The number of values declared in the dict page does not match the length of the page"
                .to_string(),
        )
    })?;

    let iter = values.chunks_exact(size_of).map(decode::<T>);

    print_iterator(iter, num_values, writer)
}

fn print_page<T, W>(page: DataPage, sample_size: usize, writer: &mut W) -> Result<()>
where
    T: NativeType,
    W: Write,
{
    let validity = validity(&page)?;

    let mut non_null_values = native_cast::<T>(&page)?;

    let iter = validity.map(|is_valid| {
        if is_valid {
            non_null_values.next()
        } else {
            None
        }
    });

    print_iterator(iter, sample_size, writer)
}

fn print_rle_page<T, W>(page: DataPage, sample_size: usize, writer: &mut W) -> Result<()>
where
    T: NativeType,
    W: Write,
{
    let validity = validity(&page)?;

    let mut non_null_indices = dict_indices_decoder(&page)?;

    let iter = validity.map(|is_valid| {
        if is_valid {
            non_null_indices.next()
        } else {
            None
        }
    });

    print_iterator(iter, sample_size, writer)
}

fn dict_indices_decoder(page: &DataPage) -> Result<hybrid_rle::HybridRleDecoder> {
    let (_, _, indices_buffer) = split_buffer(page)?;

    // SPEC: Data page format: the bit width used to encode the entry ids stored as 1 byte (max bit width = 32),
    // SPEC: followed by the values encoded using RLE/Bit packed described above (with the given bit width).
    let bit_width = indices_buffer[0];
    if bit_width > 32 {
        return Err(Error::OutOfSpec(
            "Bit width of dictionary pages cannot be larger than 32".to_string(),
        ));
    }
    let indices_buffer = &indices_buffer[1..];

    Ok(hybrid_rle::HybridRleDecoder::new(
        indices_buffer,
        bit_width as u32,
        page.num_values(),
    ))
}

fn validity(page: &DataPage) -> Result<HybridRleBooleanIter<HybridRleIter<hybrid_rle::Decoder>>> {
    let (_, def_levels, _) = split_buffer(page)?;

    // only works for non-nested pages
    let num_bits = (page.descriptor.max_def_level == 1) as usize;

    let validity = hybrid_rle::Decoder::new(def_levels, num_bits);
    let validity = HybridRleIter::new(validity, page.num_values());
    Ok(HybridRleBooleanIter::new(validity))
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
