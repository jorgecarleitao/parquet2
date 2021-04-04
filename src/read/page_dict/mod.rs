mod binary;
mod primitive;

pub use binary::BinaryPageDict;
pub use primitive::PrimitivePageDict;

use std::{any::Any, sync::Arc};

use parquet_format::CompressionCodec;

use crate::compression::create_codec;
use crate::error::Result;
use crate::schema::types::PhysicalType;

use super::compression::decompress;

/// A dynamic trait describing a decompressed and decoded Dictionary Page.
pub trait PageDict: std::fmt::Debug {
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
        PhysicalType::Boolean => todo!(),
        PhysicalType::Int32 => {
            primitive::read_page_dict::<i32>(&buf, num_values, is_sorted, physical_type)
        }
        PhysicalType::Int64 => {
            primitive::read_page_dict::<i64>(&buf, num_values, is_sorted, physical_type)
        }
        PhysicalType::Int96 => {
            primitive::read_page_dict::<[u32; 3]>(&buf, num_values, is_sorted, physical_type)
        }
        PhysicalType::Float => {
            primitive::read_page_dict::<f32>(&buf, num_values, is_sorted, physical_type)
        }
        PhysicalType::Double => {
            primitive::read_page_dict::<f64>(&buf, num_values, is_sorted, physical_type)
        }
        PhysicalType::ByteArray => binary::read_page_dict(&buf, num_values),
        PhysicalType::FixedLenByteArray(_) => todo!(),
    }
}
