mod binary;
mod fixed_len_binary;
mod primitive;

pub use binary::BinaryPageDict;
pub use fixed_len_binary::FixedLenByteArrayPageDict;

use parquet2::error::{Error, Result};
use parquet2::page::DictPage;
use parquet2::schema::types::PhysicalType;

pub enum DecodedDictPage {
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    Int96(Vec<[u32; 3]>),
    Float(Vec<f32>),
    Double(Vec<f64>),
    ByteArray(BinaryPageDict),
    FixedLenByteArray(FixedLenByteArrayPageDict),
}

pub fn deserialize(page: &DictPage, physical_type: PhysicalType) -> Result<DecodedDictPage> {
    _deserialize(&page.buffer, page.num_values, page.is_sorted, physical_type)
}

fn _deserialize(
    buf: &[u8],
    num_values: usize,
    is_sorted: bool,
    physical_type: PhysicalType,
) -> Result<DecodedDictPage> {
    match physical_type {
        PhysicalType::Boolean => Err(Error::OutOfSpec(
            "Boolean physical type cannot be dictionary-encoded".to_string(),
        )),
        PhysicalType::Int32 => {
            primitive::read::<i32>(buf, num_values, is_sorted).map(DecodedDictPage::Int32)
        }
        PhysicalType::Int64 => {
            primitive::read::<i64>(buf, num_values, is_sorted).map(DecodedDictPage::Int64)
        }
        PhysicalType::Int96 => {
            primitive::read::<[u32; 3]>(buf, num_values, is_sorted).map(DecodedDictPage::Int96)
        }
        PhysicalType::Float => {
            primitive::read::<f32>(buf, num_values, is_sorted).map(DecodedDictPage::Float)
        }
        PhysicalType::Double => {
            primitive::read::<f64>(buf, num_values, is_sorted).map(DecodedDictPage::Double)
        }
        PhysicalType::ByteArray => binary::read(buf, num_values).map(DecodedDictPage::ByteArray),
        PhysicalType::FixedLenByteArray(size) => {
            fixed_len_binary::read(buf, size, num_values).map(DecodedDictPage::FixedLenByteArray)
        }
    }
}
