use parquet_format_async_temp::SchemaElement;

use crate::error::{ParquetError, Result};

use super::super::types::{
    group_converted_converted_to, physical_type_to_type, primitive_converted_to_converted,
    ParquetType,
};

impl ParquetType {
    /// Method to convert to Thrift.
    pub fn to_thrift(&self) -> Result<Vec<SchemaElement>> {
        if !self.is_root() {
            return Err(general_err!("Root schema must be Group type"));
        }
        let mut elements: Vec<SchemaElement> = Vec::new();
        to_thrift_helper(self, &mut elements);
        Ok(elements)
    }
}

/// Constructs list of `SchemaElement` from the schema using depth-first traversal.
/// Here we assume that schema is always valid and starts with group type.
fn to_thrift_helper(schema: &ParquetType, elements: &mut Vec<SchemaElement>) {
    match schema {
        ParquetType::PrimitiveType {
            basic_info,
            logical_type,
            converted_type,
            physical_type,
        } => {
            let (type_, type_length) = physical_type_to_type(physical_type);
            let converted_type = converted_type
                .as_ref()
                .map(|x| primitive_converted_to_converted(x));
            let (converted_type, maybe_decimal) = converted_type
                .map(|x| (Some(x.0), x.1))
                .unwrap_or((None, None));

            let element = SchemaElement {
                type_: Some(type_),
                type_length,
                repetition_type: Some((*basic_info.repetition()).into()),
                name: basic_info.name().to_owned(),
                num_children: None,
                converted_type,
                precision: maybe_decimal.map(|x| x.0),
                scale: maybe_decimal.map(|x| x.1),
                field_id: *basic_info.id(),
                logical_type: logical_type.clone(),
            };

            elements.push(element);
        }
        ParquetType::GroupType {
            basic_info,
            fields,
            logical_type,
            converted_type,
        } => {
            let converted_type = converted_type
                .as_ref()
                .map(|x| group_converted_converted_to(x));

            let repetition_type = if basic_info.is_root() {
                // https://github.com/apache/parquet-format/blob/7f06e838cbd1b7dbd722ff2580b9c2525e37fc46/src/main/thrift/parquet.thrift#L363
                None
            } else {
                Some(*basic_info.repetition())
            };

            let element = SchemaElement {
                type_: None,
                type_length: None,
                repetition_type: repetition_type.map(|x| x.into()),
                name: basic_info.name().to_owned(),
                num_children: Some(fields.len() as i32),
                converted_type,
                scale: None,
                precision: None,
                field_id: *basic_info.id(),
                logical_type: logical_type.clone(),
            };

            elements.push(element);

            // Add child elements for a group
            for field in fields {
                to_thrift_helper(field, elements);
            }
        }
    }
}
