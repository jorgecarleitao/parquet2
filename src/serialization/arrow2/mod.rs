mod primitive;

use arrow2::{array::Array, datatypes::DataType};

use crate::schema::types::{ParquetType, PhysicalType};
use crate::{
    errors::{ParquetError, Result},
    metadata::ColumnDescriptor,
    read::Page,
};

pub fn page_iter_to_array<I: Iterator<Item = Result<Page>>>(
    iter: I,
    descriptor: &ColumnDescriptor,
) -> Result<Box<dyn Array>> {
    match descriptor.type_() {
        ParquetType::PrimitiveType {
            physical_type,
            converted_type,
            logical_type,
            ..
        } => match (physical_type, converted_type, logical_type) {
            // todo: apply conversion rules and the like
            (PhysicalType::Int32, None, None) => Ok(Box::new(primitive::iter_to_array::<i32, _>(
                iter,
                descriptor,
                DataType::Int32,
            )?)),
            (PhysicalType::Int64, None, None) => Ok(Box::new(primitive::iter_to_array::<i64, _>(
                iter,
                descriptor,
                DataType::Int64,
            )?)),
            (PhysicalType::Float, None, None) => Ok(Box::new(primitive::iter_to_array::<f32, _>(
                iter,
                descriptor,
                DataType::Float32,
            )?)),
            (PhysicalType::Double, None, None) => Ok(Box::new(primitive::iter_to_array::<f64, _>(
                iter,
                descriptor,
                DataType::Float64,
            )?)),
            (p, c, l) => Err(general_err!(
                "The conversion of ({:?}, {:?}, {:?}) to arrow still not implemented",
                p,
                c,
                l
            )),
        },
        _ => todo!(),
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use crate::serialization::native::Array as NativeArray;
    use arrow2::array::Primitive;

    fn native_to_arrow(array: NativeArray) -> Box<dyn Array> {
        match array {
            NativeArray::UInt32(v) => Box::new(Primitive::from(&v).to(DataType::UInt32)),
            NativeArray::Int32(v) => Box::new(Primitive::from(&v).to(DataType::Int32)),
            NativeArray::Int64(v) => Box::new(Primitive::from(&v).to(DataType::Int64)),
            NativeArray::Int96(_) => todo!(),
            NativeArray::Float32(v) => Box::new(Primitive::from(&v).to(DataType::Float32)),
            NativeArray::Float64(v) => Box::new(Primitive::from(&v).to(DataType::Float64)),
            NativeArray::Boolean(_) => todo!(),
            NativeArray::Binary(_) => todo!(),
        }
    }

    use super::*;
    use crate::tests::pyarrow_integration;
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
    fn pyarrow_integration_int64() -> Result<()> {
        let column = 0;
        let path = "fixtures/pyarrow3/basic_nulls.parquet";
        let array = get_column(path, 0, column)?;

        let expected = native_to_arrow(pyarrow_integration(column));

        assert_eq!(expected.as_ref(), array.as_ref());

        Ok(())
    }

    #[test]
    fn pyarrow_integration_float64() -> Result<()> {
        let column = 1;
        let path = "fixtures/pyarrow3/basic_nulls.parquet";
        let array = get_column(path, 0, column)?;

        let expected = native_to_arrow(pyarrow_integration(column));

        assert_eq!(expected.as_ref(), array.as_ref());

        Ok(())
    }

    // todo: implement me
    #[test]
    #[ignore]
    fn pyarrow_integration_string() -> Result<()> {
        let column = 2;
        let path = "fixtures/pyarrow3/basic_nulls.parquet";
        let array = get_column(path, 0, column)?;

        let expected = native_to_arrow(pyarrow_integration(column));

        assert_eq!(expected.as_ref(), array.as_ref());

        Ok(())
    }
}
