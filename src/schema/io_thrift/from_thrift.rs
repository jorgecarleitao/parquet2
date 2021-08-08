use std::convert::TryInto;

use parquet_format_async_temp::SchemaElement;

use crate::error::{ParquetError, Result};

use super::super::types::{
    converted_to_group_converted, converted_to_primitive_converted, type_to_physical_type,
    ParquetType,
};

impl ParquetType {
    /// Method to convert from Thrift.
    pub fn try_from_thrift(elements: &[&SchemaElement]) -> Result<ParquetType> {
        let mut index = 0;
        let mut schema_nodes = Vec::new();
        while index < elements.len() {
            let t = from_thrift_helper(elements, index)?;
            index = t.0;
            schema_nodes.push(t.1);
        }
        if schema_nodes.len() != 1 {
            return Err(general_err!(
                "Expected exactly one root node, but found {}",
                schema_nodes.len()
            ));
        }

        Ok(schema_nodes.remove(0))
    }
}

/// Constructs a new Type from the `elements`, starting at index `index`.
/// The first result is the starting index for the next Type after this one. If it is
/// equal to `elements.len()`, then this Type is the last one.
/// The second result is the result Type.
fn from_thrift_helper(elements: &[&SchemaElement], index: usize) -> Result<(usize, ParquetType)> {
    // Whether or not the current node is root (message type).
    // There is only one message type node in the schema tree.
    let is_root_node = index == 0;

    let element = elements[index];
    let name = element.name.clone();
    let converted_type = element.converted_type;
    // LogicalType is only present in v2 Parquet files. ConvertedType is always
    // populated, regardless of the version of the file (v1 or v2).
    let logical_type = &element.logical_type;
    let field_id = element.field_id;
    match element.num_children {
        // From parquet-format:
        //   The children count is used to construct the nested relationship.
        //   This field is not set when the element is a primitive type
        // Sometimes parquet-cpp sets num_children field to 0 for primitive types, so we
        // have to handle this case too.
        None | Some(0) => {
            // primitive type
            let repetition = element
                .repetition_type
                .ok_or_else(|| {
                    general_err!("Repetition level must be defined for a primitive type")
                })?
                .try_into()
                .unwrap();
            let physical_type = element.type_.ok_or_else(|| {
                general_err!("Physical type must be defined for a primitive type")
            })?;
            let length = element.type_length;
            let physical_type = type_to_physical_type(&physical_type, length)?;

            let converted_type = match converted_type {
                Some(converted_type) => Some({
                    let maybe_decimal = match (element.precision, element.scale) {
                        (Some(precision), Some(scale)) => Some((precision, scale)),
                        (None, None) => None,
                        _ => {
                            return Err(general_err!(
                                "When precision or scale are defined, both must be defined"
                            ))
                        }
                    };
                    converted_to_primitive_converted(&converted_type, maybe_decimal)?
                }),
                None => None,
            };

            let tp = ParquetType::try_from_primitive(
                name,
                physical_type,
                repetition,
                converted_type,
                logical_type.clone(),
                field_id,
            )?;

            Ok((index + 1, tp))
        }
        Some(n) => {
            let repetition = element.repetition_type.map(|x| x.try_into().unwrap());
            let mut fields = vec![];
            let mut next_index = index + 1;
            for _ in 0..n {
                let child_result = from_thrift_helper(elements, next_index)?;
                next_index = child_result.0;
                fields.push(child_result.1);
            }

            let tp = if is_root_node {
                ParquetType::new_root(name, fields)
            } else {
                let converted_type = match converted_type {
                    Some(converted_type) => Some(converted_to_group_converted(&converted_type)?),
                    None => None,
                };
                ParquetType::from_converted(name, fields, repetition, converted_type, field_id)
            };
            Ok((next_index, tp))
        }
    }
}
