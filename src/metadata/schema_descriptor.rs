use crate::schema::types::{ParquetType, Repetition};

use super::column_descriptor::ColumnDescriptor;

/// A schema descriptor. This encapsulates the top-level schemas for all the columns,
/// as well as all descriptors for all the primitive columns.
#[derive(Debug, Clone)]
pub struct SchemaDescriptor {
    // The top-level schema (the "message" type).
    // This must be a `GroupType` where each field is a root column type in the schema.
    schema: ParquetType,

    // All the descriptors for primitive columns in this schema, constructed from
    // `schema` in DFS order.
    leaves: Vec<ColumnDescriptor>,

    // Mapping from a leaf column's index to the root column type that it
    // comes from. For instance: the leaf `a.b.c.d` would have a link back to `a`:
    // -- a  <-----+
    // -- -- b     |
    // -- -- -- c  |
    // -- -- -- -- d
    leaf_to_base: Vec<ParquetType>,
}

impl SchemaDescriptor {
    /// Creates new schema descriptor from Parquet schema.
    pub fn new(type_: ParquetType) -> Self {
        assert!(type_.is_root());
        match type_ {
            ParquetType::GroupType { ref fields, .. } => {
                let mut leaves = vec![];
                let mut leaf_to_base = Vec::new();
                for f in fields {
                    let mut path = vec![];
                    build_tree(f, f, 0, 0, &mut leaves, &mut leaf_to_base, &mut path);
                }

                Self {
                    schema: type_,
                    leaves,
                    leaf_to_base,
                }
            }
            ParquetType::PrimitiveType { .. } => unreachable!(),
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

    /// Returns column root [`ParquetType`](crate::schema::types::ParquetType) for a field position.
    pub fn get_column_root(&self, i: usize) -> &ParquetType {
        self.leaf_to_base
            .get(i)
            .unwrap_or_else(|| panic!("Expected a value for index {} but found None", i))
    }

    /// Returns schema as [`Type`](crate::schema::types::Type).
    pub fn root_schema(&self) -> &ParquetType {
        &self.schema
    }

    /// Returns schema name.
    pub fn name(&self) -> &str {
        self.schema.name()
    }
}

fn build_tree<'a>(
    tp: &'a ParquetType,
    base_tp: &ParquetType,
    mut max_rep_level: i16,
    mut max_def_level: i16,
    leaves: &mut Vec<ColumnDescriptor>,
    leaf_to_base: &mut Vec<ParquetType>,
    path_so_far: &mut Vec<&'a str>,
) {
    path_so_far.push(tp.name());
    match tp.get_basic_info().repetition() {
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
            leaf_to_base.push(base_tp.clone());
        }
        ParquetType::GroupType { ref fields, .. } => {
            for f in fields {
                build_tree(
                    f,
                    base_tp,
                    max_rep_level,
                    max_def_level,
                    leaves,
                    leaf_to_base,
                    path_so_far,
                );
                path_so_far.pop();
            }
        }
    }
}
