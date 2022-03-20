use std::io::Cursor;

use parquet_format_async_temp::{thrift::protocol::TCompactInputProtocol, ColumnIndex};

use crate::error::ParquetError;
use crate::schema::types::{PhysicalType, PrimitiveType};

use super::{ByteIndex, FixedLenByteIndex, Index, NativeIndex};

pub fn deserialize(
    data: &[u8],
    primitive_type: PrimitiveType,
) -> Result<Option<Box<dyn Index>>, ParquetError> {
    let mut d = Cursor::new(data);
    let mut prot = TCompactInputProtocol::new(&mut d);

    let index = ColumnIndex::read_from_in_protocol(&mut prot)?;

    let index = match primitive_type.physical_type {
        PhysicalType::Boolean => return Ok(None),
        PhysicalType::Int32 => {
            Box::new(NativeIndex::<i32>::try_new(index, primitive_type)?) as Box<dyn Index>
        }
        PhysicalType::Int64 => Box::new(NativeIndex::<i64>::try_new(index, primitive_type)?),
        PhysicalType::Int96 => Box::new(NativeIndex::<[u32; 3]>::try_new(index, primitive_type)?),
        PhysicalType::Float => Box::new(NativeIndex::<f32>::try_new(index, primitive_type)?),
        PhysicalType::Double => Box::new(NativeIndex::<f64>::try_new(index, primitive_type)?),
        PhysicalType::ByteArray => Box::new(ByteIndex::try_new(index, primitive_type)?),
        PhysicalType::FixedLenByteArray(_) => {
            Box::new(FixedLenByteIndex::try_new(index, primitive_type)?)
        }
    };

    Ok(Some(index))
}
