mod binary;
mod boolean;
mod primitive;
mod levels;

use crate::errors::Result;
use crate::schema::types::ParquetType;
use crate::schema::types::PhysicalType;
use crate::{metadata::ColumnDescriptor, read::Page};

// The dynamic representation of values in native Rust. This is not exaustive and should not be used for
// large scale analytics; however, it is very useful to understand and use the format.
// todo: maybe refactor this into serde/json?
#[derive(Debug, PartialEq)]
pub enum Array {
    UInt32(Vec<Option<u32>>),
    Int32(Vec<Option<i32>>),
    Int64(Vec<Option<i64>>),
    Int96(Vec<Option<[u32; 3]>>),
    Float32(Vec<Option<f32>>),
    Float64(Vec<Option<f64>>),
    Boolean(Vec<Option<bool>>),
    Binary(Vec<Option<Vec<u8>>>),
}

pub fn page_to_vec(page: &Page, descriptor: &ColumnDescriptor) -> Result<Array> {
    match descriptor.type_() {
        ParquetType::PrimitiveType { physical_type, .. } => match page.dictionary_page() {
            Some(_) => match physical_type {
                PhysicalType::Int32 => {
                    Ok(Array::Int32(primitive::page_dict_to_vec(page, descriptor)?))
                }
                PhysicalType::Int64 => {
                    Ok(Array::Int64(primitive::page_dict_to_vec(page, descriptor)?))
                }
                PhysicalType::Int96 => {
                    Ok(Array::Int96(primitive::page_dict_to_vec(page, descriptor)?))
                }
                PhysicalType::Float => Ok(Array::Float32(primitive::page_dict_to_vec(
                    page, descriptor,
                )?)),
                PhysicalType::Double => Ok(Array::Float64(primitive::page_dict_to_vec(
                    page, descriptor,
                )?)),
                PhysicalType::ByteArray => {
                    Ok(Array::Binary(binary::page_dict_to_vec(page, descriptor)?))
                }
                _ => todo!(),
            },
            None => match physical_type {
                PhysicalType::Boolean => {
                    Ok(Array::Boolean(boolean::page_to_vec(page, descriptor)?))
                }
                PhysicalType::Int32 => Ok(Array::Int32(primitive::page_to_vec(page, descriptor)?)),
                PhysicalType::Int64 => Ok(Array::Int64(primitive::page_to_vec(page, descriptor)?)),
                PhysicalType::Int96 => Ok(Array::Int96(primitive::page_to_vec(page, descriptor)?)),
                PhysicalType::Float => {
                    Ok(Array::Float32(primitive::page_to_vec(page, descriptor)?))
                }
                PhysicalType::Double => {
                    Ok(Array::Float64(primitive::page_to_vec(page, descriptor)?))
                }
                _ => todo!(),
            },
        },
        _ => todo!(),
    }
}
