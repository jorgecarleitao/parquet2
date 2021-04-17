mod binary;
mod fixed_len_binary;
mod primitive;

pub use binary::BinaryPageDict;
pub use fixed_len_binary::FixedLenByteArrayPageDict;
pub use primitive::PrimitivePageDict;

use std::{any::Any, sync::Arc};

use parquet_format::CompressionCodec;

use crate::compression::create_codec;
use crate::error::{ParquetError, Result};
use crate::schema::types::PhysicalType;

use super::compression::decompress;

/// A dynamic trait describing a decompressed and decoded Dictionary Page.
pub trait PageDict: std::fmt::Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;

    fn physical_type(&self) -> &PhysicalType;
}

pub fn read_page_dict(
    buf: Vec<u8>,
    num_values: u32,
    compression: (CompressionCodec, usize),
    is_sorted: bool,
    physical_type: PhysicalType,
) -> Result<Arc<dyn PageDict>> {
    let decompressor = create_codec(&compression.0)?;
    let buf = if let Some(mut decompressor) = decompressor {
        decompress(&buf, compression.1, decompressor.as_mut())?
    } else {
        buf
    };

    match physical_type {
        PhysicalType::Boolean => Err(ParquetError::OutOfSpec(
            "Boolean physical type cannot be dictionary-encoded".to_string(),
        )),
        PhysicalType::Int32 => primitive::read::<i32>(&buf, num_values, is_sorted),
        PhysicalType::Int64 => primitive::read::<i64>(&buf, num_values, is_sorted),
        PhysicalType::Int96 => primitive::read::<[u32; 3]>(&buf, num_values, is_sorted),
        PhysicalType::Float => primitive::read::<f32>(&buf, num_values, is_sorted),
        PhysicalType::Double => primitive::read::<f64>(&buf, num_values, is_sorted),
        PhysicalType::ByteArray => binary::read(&buf, num_values),
        PhysicalType::FixedLenByteArray(size) => fixed_len_binary::read(&buf, size, num_values),
    }
}
