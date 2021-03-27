use super::Type;
use crate::errors::{ParquetError, Result};

#[derive(Clone, Debug, PartialEq)]
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
    Ok(match type_ {
        Type::Boolean => PhysicalType::Boolean,
        Type::Int32 => PhysicalType::Int32,
        Type::Int64 => PhysicalType::Int64,
        Type::Int96 => PhysicalType::Int96,
        Type::Float => PhysicalType::Float,
        Type::Double => PhysicalType::Double,
        Type::ByteArray => PhysicalType::ByteArray,
        Type::FixedLenByteArray => {
            let length = length
                .ok_or_else(|| general_err!("Length must be defined for FixedLenByteArray"))?;
            PhysicalType::FixedLenByteArray(length)
        }
    })
}

pub fn physical_type_to_type(physical_type: &PhysicalType) -> (Type, Option<i32>) {
    match physical_type {
        PhysicalType::Boolean => (Type::Boolean, None),
        PhysicalType::Int32 => (Type::Int32, None),
        PhysicalType::Int64 => (Type::Int64, None),
        PhysicalType::Int96 => (Type::Int96, None),
        PhysicalType::Float => (Type::Float, None),
        PhysicalType::Double => (Type::Double, None),
        PhysicalType::ByteArray => (Type::ByteArray, None),
        PhysicalType::FixedLenByteArray(length) => (Type::FixedLenByteArray, Some(*length)),
    }
}
