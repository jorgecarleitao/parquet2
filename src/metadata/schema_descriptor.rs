use parquet_format_async_temp::SchemaElement;

use crate::{
    error::ParquetError,
    schema::{io_message::from_message, types::ParquetType, Repetition},
};
use crate::{error::Result, schema::types::BasicTypeInfo};

use super::column_descriptor::ColumnDescriptor;

/// A schema descriptor. This encapsulates the top-level schemas for all the columns,
/// as well as all descriptors for all the primitive columns.
#[derive(Debug, Clone)]
pub struct SchemaDescriptor {
    name: String,
    // The top-level schema (the "message" type).
    fields: Vec<ParquetType>,

    // All the descriptors for primitive columns in this schema, constructed from
    // `schema` in DFS order.
    leaves: Vec<ColumnDescriptor>,
}

impl SchemaDescriptor {
    /// Creates new schema descriptor from Parquet schema.
    pub fn new(name: String, fields: Vec<ParquetType>) -> Self {
        let mut leaves = vec![];
        for f in &fields {
            let mut path = vec![];
            build_tree(f, f, 0, 0, &mut leaves, &mut path);
        }

        Self {
            name,
            fields,
            leaves,
        }
    }

    /// Returns [`ColumnDescriptor`] for a field position.
    pub fn column(&self, i: usize) -> &ColumnDescriptor {
        &self.leaves[i]
    }

    /// Returns slice of [`ColumnDescriptor`].
    pub fn columns(&self) -> &[ColumnDescriptor] {
        &self.leaves
    }

    /// Returns number of leaf-level columns.
    pub fn num_columns(&self) -> usize {
        self.leaves.len()
    }

    /// Returns schema name.
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn fields(&self) -> &[ParquetType] {
        &self.fields
    }

    pub(crate) fn into_thrift(self) -> Result<Vec<SchemaElement>> {
        ParquetType::GroupType {
            basic_info: BasicTypeInfo::new(self.name, Repetition::Optional, None, true),
            logical_type: None,
            converted_type: None,
            fields: self.fields,
        }
        .to_thrift()
    }

    fn try_from_type(type_: ParquetType) -> Result<Self> {
        match type_ {
            ParquetType::GroupType {
                basic_info, fields, ..
            } => Ok(Self::new(basic_info.name().to_string(), fields)),
            _ => Err(ParquetError::OutOfSpec(
                "The parquet schema MUST be a group type".to_string(),
            )),
        }
    }

    pub(crate) fn try_from_thrift(elements: &[&SchemaElement]) -> Result<Self> {
        let schema = ParquetType::try_from_thrift(elements)?;
        Self::try_from_type(schema)
    }

    pub fn try_from_message(message: &str) -> Result<Self> {
        let schema = from_message(message)?;
        Self::try_from_type(schema)
    }
}

fn build_tree<'a>(
    tp: &'a ParquetType,
    base_tp: &ParquetType,
    mut max_rep_level: i16,
    mut max_def_level: i16,
    leaves: &mut Vec<ColumnDescriptor>,
    path_so_far: &mut Vec<&'a str>,
) {
    path_so_far.push(tp.name());
    match *tp.get_basic_info().repetition() {
        Repetition::Optional => {
            max_def_level += 1;
        }
        Repetition::Repeated => {
            max_def_level += 1;
            max_rep_level += 1;
        }
        _ => {}
    }

    match tp {
        ParquetType::PrimitiveType { .. } => {
            let path_in_schema = path_so_far.iter().copied().map(String::from).collect();
            leaves.push(ColumnDescriptor::new(
                tp.clone(),
                max_def_level,
                max_rep_level,
                path_in_schema,
                base_tp.clone(),
            ));
        }
        ParquetType::GroupType { ref fields, .. } => {
            for f in fields {
                build_tree(
                    f,
                    base_tp,
                    max_rep_level,
                    max_def_level,
                    leaves,
                    path_so_far,
                );
                path_so_far.pop();
            }
        }
    }
}
