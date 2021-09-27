mod binary;
mod fixed_len_binary;
mod primitive;

pub use binary::BinaryPageDict;
pub use fixed_len_binary::FixedLenByteArrayPageDict;
pub use primitive::PrimitivePageDict;

use std::{any::Any, sync::Arc};

use crate::compression::{create_codec, Compression};
use crate::error::{ParquetError, Result};
use crate::schema::types::PhysicalType;

/// A dynamic trait describing a decompressed and decoded Dictionary Page.
pub trait DictPage: std::fmt::Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;

    fn physical_type(&self) -> &PhysicalType;
}

/// A encoded and uncompressed dictionary page.
#[derive(Debug)]
pub struct EncodedDictPage {
    pub(crate) buffer: Vec<u8>,
    pub(crate) num_values: usize,
}

impl EncodedDictPage {
    pub fn new(buffer: Vec<u8>, num_values: usize) -> Self {
        Self { buffer, num_values }
    }
}

/// An encoded and compressed dictionary page.
#[derive(Debug)]
pub struct CompressedDictPage {
    pub(crate) buffer: Vec<u8>,
    pub(crate) num_values: usize,
}

impl CompressedDictPage {
    pub fn new(buffer: Vec<u8>, num_values: usize) -> Self {
        Self { buffer, num_values }
    }
}

pub fn read_dict_page(
    page: &EncodedDictPage,
    compression: (Compression, usize),
    is_sorted: bool,
    physical_type: &PhysicalType,
) -> Result<Arc<dyn DictPage>> {
    let decompressor = create_codec(&compression.0)?;
    if let Some(mut decompressor) = decompressor {
        let mut decompressed = vec![0; compression.1];
        decompressor.decompress(&page.buffer, &mut decompressed)?;
        deserialize(&decompressed, page.num_values, is_sorted, physical_type)
    } else {
        deserialize(&page.buffer, page.num_values, is_sorted, physical_type)
    }
}

fn deserialize(
    buf: &[u8],
    num_values: usize,
    is_sorted: bool,
    physical_type: &PhysicalType,
) -> Result<Arc<dyn DictPage>> {
    match physical_type {
        PhysicalType::Boolean => Err(ParquetError::OutOfSpec(
            "Boolean physical type cannot be dictionary-encoded".to_string(),
        )),
        PhysicalType::Int32 => primitive::read::<i32>(buf, num_values, is_sorted),
        PhysicalType::Int64 => primitive::read::<i64>(buf, num_values, is_sorted),
        PhysicalType::Int96 => primitive::read::<[u32; 3]>(buf, num_values, is_sorted),
        PhysicalType::Float => primitive::read::<f32>(buf, num_values, is_sorted),
        PhysicalType::Double => primitive::read::<f64>(buf, num_values, is_sorted),
        PhysicalType::ByteArray => binary::read(buf, num_values),
        PhysicalType::FixedLenByteArray(size) => fixed_len_binary::read(buf, *size, num_values),
    }
}
