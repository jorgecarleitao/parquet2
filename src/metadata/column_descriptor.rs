use crate::schema::types::{ParquetType, PhysicalType};

/// A descriptor for leaf-level primitive columns.
/// This encapsulates information such as definition and repetition levels and is used to
/// re-assemble nested data.
#[derive(Debug, PartialEq, Clone)]
pub struct ColumnDescriptor {
    // The "leaf" primitive type of this column
    primitive_type: ParquetType,

    // The maximum definition level for this column
    max_def_level: i16,

    // The maximum repetition level for this column
    max_rep_level: i16,

    // The path of this column. For instance, "a.b.c.d".
    path_in_schema: Vec<String>,

    base_type: ParquetType,
}

impl ColumnDescriptor {
    /// Creates new descriptor for leaf-level column.
    pub fn new(
        primitive_type: ParquetType,
        max_def_level: i16,
        max_rep_level: i16,
        path_in_schema: Vec<String>,
        base_type: ParquetType,
    ) -> Self {
        Self {
            primitive_type,
            max_def_level,
            max_rep_level,
            path_in_schema,
            base_type,
        }
    }

    /// Returns maximum definition level for this column.
    pub fn max_def_level(&self) -> i16 {
        self.max_def_level
    }

    /// Returns maximum repetition level for this column.
    pub fn max_rep_level(&self) -> i16 {
        self.max_rep_level
    }

    pub fn path_in_schema(&self) -> &[String] {
        &self.path_in_schema
    }

    pub fn base_type(&self) -> &ParquetType {
        &self.base_type
    }

    /// Returns self type [`ParquetType`] for this leaf column.
    pub fn type_(&self) -> &ParquetType {
        &self.primitive_type
    }

    /// Returns self type [`PhysicalType`] for this leaf column.
    /// # Panic
    /// This function panics if the corresponding [`ParquetType`] is not a primitive type
    pub fn physical_type(&self) -> &PhysicalType {
        match &self.primitive_type {
            ParquetType::PrimitiveType { physical_type, .. } => physical_type,
            _ => unreachable!(""),
        }
    }

    /// Returns column name.
    pub fn name(&self) -> &str {
        self.primitive_type.name()
    }
}
