mod primitive;

use arrow2::{array::Array, datatypes::DataType};

use crate::schema::types::{ParquetType, PhysicalType};
use crate::{errors::Result, metadata::ColumnDescriptor, read::Page};

pub fn page_iter_to_array<I: Iterator<Item = Result<Page>>>(
    iter: I,
    descriptor: &ColumnDescriptor,
) -> Result<Box<dyn Array>> {
    match descriptor.type_() {
        ParquetType::PrimitiveType { physical_type, .. } => match physical_type {
            PhysicalType::Int32 => Ok(Box::new(primitive::iter_to_array::<i32, _>(
                iter,
                descriptor,
                DataType::Int32,
            )?)),
            PhysicalType::Int64 => Ok(Box::new(primitive::iter_to_array::<i64, _>(
                iter,
                descriptor,
                DataType::Int64,
            )?)),
            _ => todo!(),
        },
        _ => todo!(),
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use arrow2::array::Primitive;

    use super::*;
    use crate::{
        errors::Result,
        read::{get_page_iterator, read_metadata},
    };

    fn get_column(path: &str, row_group: usize, column: usize) -> Result<Box<dyn Array>> {
        let mut file = File::open(path).unwrap();

        let metadata = read_metadata(&mut file)?;
        let iter = get_page_iterator(&metadata, row_group, column, &mut file)?;

        let descriptor = &iter.descriptor().clone();

        page_iter_to_array(iter, descriptor)
    }

    #[test]
    fn test_pyarrow_integration() -> Result<()> {
        let column = 0;
        let path = "fixtures/pyarrow3/basic_nulls.parquet";
        let array = get_column(path, 0, column)?;

        let expected = Primitive::<i64>::from(&[
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
        ])
        .to(DataType::Int64);

        assert_eq!(expected, array.as_ref());

        Ok(())
    }
}
