use super::Type;
use crate::error::{ParquetError, Result};

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum PhysicalType {
    Boolean,
    Int32,
    Int64,
    Int96,
    Float,
    Double,
    ByteArray,
    FixedLenByteArray(i32),
}

pub fn type_to_physical_type(type_: &Type, length: Option<i32>) -> Result<PhysicalType> {
    Ok(match *type_ {
        Type::BOOLEAN => PhysicalType::Boolean,
        Type::INT32 => PhysicalType::Int32,
        Type::INT64 => PhysicalType::Int64,
        Type::INT96 => PhysicalType::Int96,
        Type::FLOAT => PhysicalType::Float,
        Type::DOUBLE => PhysicalType::Double,
        Type::BYTE_ARRAY => PhysicalType::ByteArray,
        Type::FIXED_LEN_BYTE_ARRAY => {
            let length = length
                .ok_or_else(|| general_err!("Length must be defined for FixedLenByteArray"))?;
            PhysicalType::FixedLenByteArray(length)
        }
        _ => unreachable!(),
    })
}

pub fn physical_type_to_type(physical_type: &PhysicalType) -> (Type, Option<i32>) {
    match physical_type {
        PhysicalType::Boolean => (Type::BOOLEAN, None),
        PhysicalType::Int32 => (Type::INT32, None),
        PhysicalType::Int64 => (Type::INT64, None),
        PhysicalType::Int96 => (Type::INT96, None),
        PhysicalType::Float => (Type::FLOAT, None),
        PhysicalType::Double => (Type::DOUBLE, None),
        PhysicalType::ByteArray => (Type::BYTE_ARRAY, None),
        PhysicalType::FixedLenByteArray(length) => (Type::FIXED_LEN_BYTE_ARRAY, Some(*length)),
    }
}
