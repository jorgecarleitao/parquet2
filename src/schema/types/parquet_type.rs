// see https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
use crate::error::Result;
use std::collections::HashMap;

use super::super::Repetition;
use super::{
    spec, BasicTypeInfo, GroupConvertedType, LogicalType, PhysicalType, PrimitiveConvertedType,
};

/// Representation of a Parquet type.
/// Used to describe primitive leaf fields and structs, including top-level schema.
/// Note that the top-level schema type is represented using `GroupType` whose
/// repetition is `None`.
#[derive(Clone, Debug, PartialEq)]
pub enum ParquetType {
    PrimitiveType {
        basic_info: BasicTypeInfo,
        logical_type: Option<LogicalType>,
        converted_type: Option<PrimitiveConvertedType>,
        physical_type: PhysicalType,
    },
    GroupType {
        basic_info: BasicTypeInfo,
        logical_type: Option<LogicalType>,
        converted_type: Option<GroupConvertedType>,
        fields: Vec<ParquetType>,
    },
}

/// Accessors
impl ParquetType {
    /// Returns [`BasicTypeInfo`] information about the type.
    pub fn get_basic_info(&self) -> &BasicTypeInfo {
        match *self {
            Self::PrimitiveType { ref basic_info, .. } => basic_info,
            Self::GroupType { ref basic_info, .. } => basic_info,
        }
    }

    /// Returns this type's field name.
    pub fn name(&self) -> &str {
        self.get_basic_info().name()
    }

    pub fn is_root(&self) -> bool {
        self.get_basic_info().is_root()
    }

    /// Checks if `sub_type` schema is part of current schema.
    /// This method can be used to check if projected columns are part of the root schema.
    pub fn check_contains(&self, sub_type: &ParquetType) -> bool {
        // Names match, and repetitions match or not set for both
        let basic_match = self.get_basic_info().name() == sub_type.get_basic_info().name()
            && (self.is_root() && sub_type.is_root()
                || !self.is_root()
                    && !sub_type.is_root()
                    && self.get_basic_info().repetition()
                        == sub_type.get_basic_info().repetition());

        match (self, sub_type) {
            (
                Self::PrimitiveType { physical_type, .. },
                Self::PrimitiveType {
                    physical_type: other_physical_type,
                    ..
                },
            ) => basic_match && physical_type == other_physical_type,
            (
                Self::GroupType { fields, .. },
                Self::GroupType {
                    fields: other_fields,
                    ..
                },
            ) => {
                // build hashmap of name -> Type
                let mut field_map = HashMap::new();
                for field in fields {
                    field_map.insert(field.name(), field);
                }

                for field in other_fields {
                    if !field_map
                        .get(field.name())
                        .map(|tpe| tpe.check_contains(field))
                        .unwrap_or(false)
                    {
                        return false;
                    }
                }
                true
            }
            _ => false,
        }
    }
}

/// Constructors
impl ParquetType {
    pub fn new_root(name: String, fields: Vec<ParquetType>) -> Self {
        let basic_info = BasicTypeInfo::new(name, Repetition::Optional, None, true);
        ParquetType::GroupType {
            basic_info,
            fields,
            logical_type: None,
            converted_type: None,
        }
    }

    pub fn from_converted(
        name: String,
        fields: Vec<ParquetType>,
        repetition: Option<Repetition>,
        converted_type: Option<GroupConvertedType>,
        id: Option<i32>,
    ) -> Self {
        let basic_info =
            BasicTypeInfo::new(name, repetition.unwrap_or(Repetition::Optional), id, false);
        ParquetType::GroupType {
            basic_info,
            fields,
            converted_type,
            logical_type: None,
        }
    }

    pub fn try_from_primitive(
        name: String,
        physical_type: PhysicalType,
        repetition: Repetition,
        converted_type: Option<PrimitiveConvertedType>,
        logical_type: Option<LogicalType>,
        id: Option<i32>,
    ) -> Result<Self> {
        spec::check_converted_invariants(&physical_type, &converted_type)?;
        spec::check_logical_invariants(&physical_type, &logical_type)?;

        let basic_info = BasicTypeInfo::new(name, repetition, id, false);

        Ok(ParquetType::PrimitiveType {
            basic_info,
            converted_type,
            logical_type,
            physical_type,
        })
    }

    pub fn from_physical(name: String, physical_type: PhysicalType) -> Self {
        let basic_info = BasicTypeInfo::new(name, Repetition::Optional, None, false);
        ParquetType::PrimitiveType {
            basic_info,
            converted_type: None,
            logical_type: None,
            physical_type,
        }
    }

    pub fn try_from_group(
        name: String,
        repetition: Repetition,
        converted_type: Option<GroupConvertedType>,
        logical_type: Option<LogicalType>,
        fields: Vec<ParquetType>,
        id: Option<i32>,
    ) -> Result<Self> {
        let basic_info = BasicTypeInfo::new(name, repetition, id, false);

        Ok(ParquetType::GroupType {
            basic_info,
            logical_type,
            converted_type,
            fields,
        })
    }
}
