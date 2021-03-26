// see https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
use std::collections::HashMap;
use crate::errors::{ParquetError, Result};

use super::{BasicTypeInfo, ConvertedType, Repetition};

#[derive(Clone, Debug, PartialEq)]
pub enum PhysicalType {
    Boolean,
    Int32,
    Int64,
    Int96,
    Float,
    Double,
    ByteArray,
    FixedLenByteArray(i32),
}

/// Representation of a Parquet type.
/// Used to describe primitive leaf fields and structs, including top-level schema.
/// Note that the top-level schema type is represented using `GroupType` whose
/// repetition is `None`.
#[derive(Clone, Debug, PartialEq)]
pub enum ParquetType {
    PrimitiveType {
        basic_info: BasicTypeInfo,
        physical_type: PhysicalType,
    },
    GroupType {
        basic_info: BasicTypeInfo,
        fields: Vec<ParquetType>,
    },
}

/// Accessors
impl ParquetType {
    /// Returns [`BasicTypeInfo`] information about the type.
    pub fn get_basic_info(&self) -> &BasicTypeInfo {
        match *self {
            Self::PrimitiveType { ref basic_info, .. } => &basic_info,
            Self::GroupType { ref basic_info, .. } => &basic_info,
        }
    }

    /// Returns this type's field name.
    pub fn name(&self) -> &str {
        self.get_basic_info().name()
    }

    /// Checks if `sub_type` schema is part of current schema.
    /// This method can be used to check if projected columns are part of the root schema.
    pub fn check_contains(&self, sub_type: &ParquetType) -> bool {
        // Names match, and repetitions match or not set for both
        let basic_match = self.get_basic_info().name() == sub_type.get_basic_info().name()
            && (self.is_schema() && sub_type.is_schema()
                || !self.is_schema()
                    && !sub_type.is_schema()
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

    /// Returns `true` if this type is the top-level schema type (message type).
    fn is_schema(&self) -> bool {
        match *self {
            Self::GroupType { ref basic_info, .. } => {
                basic_info.repetition() != &Repetition::Optional
            }
            _ => false,
        }
    }

    /// Returns `true` if this type is repeated or optional.
    /// If this type doesn't have repetition defined, we still treat it as optional.
    pub fn is_optional(&self) -> bool {
        self.get_basic_info().repetition() != &Repetition::Required
    }
}

// Not all converted types are valid for a given physical type. Let's check this
#[inline]
fn check_decimal_invariants(physical_type: &PhysicalType, precision: i32, scale: i32) -> Result<()> {
    if precision < 1 {
        return Err(general_err!(
            "DECIMAL precision must be larger than 0; It is {}",
            precision
        ));
    }
    if scale >= precision {
        return Err(general_err!(
            "Invalid DECIMAL: scale ({}) cannot be greater than or equal to precision \
            ({})",
            scale,
            precision
        ));
    }

    match physical_type {
        PhysicalType::Int32 => {
            if precision > 9 {
                return Err(general_err!(
                    "Cannot represent INT32 as DECIMAL with precision {}",
                    precision
                ));
            }
        }
        PhysicalType::Int64 => {
            if precision > 18 {
                return Err(general_err!(
                    "Cannot represent INT64 as DECIMAL with precision {}",
                    precision
                ));
            }
        }
        PhysicalType::FixedLenByteArray(length) => {
            let max_precision = (2f64.powi(8 * length - 1) - 1f64).log10().floor() as i32;

            if precision > max_precision {
                return Err(general_err!(
                    "Cannot represent FIXED_LEN_BYTE_ARRAY as DECIMAL with length {} and \
                    precision {}. The max precision can only be {}",
                    length,
                    precision,
                    max_precision
                ));
            }
        }
        PhysicalType::ByteArray => {},
        _ => return Err(general_err!(
            "DECIMAL can only annotate INT32, INT64, BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY"
        ))
    };
    Ok(())
}

fn check_converted_invariants(physical_type: &PhysicalType, converted_type: &Option<ConvertedType>) -> Result<()> {
    if converted_type.is_none() {
        return Ok(())
    };
    let converted_type = converted_type.as_ref().unwrap();

    match converted_type {
        ConvertedType::Utf8 | ConvertedType::Bson | ConvertedType::Json => {
            if physical_type != &PhysicalType::ByteArray {
                return Err(general_err!(
                    "{:?} can only annotate BYTE_ARRAY fields",
                    converted_type
                ));
            }
        }
        ConvertedType::Decimal(precision, scale) => {
            check_decimal_invariants(physical_type, *precision, *scale)?;
        }
        ConvertedType::Date
        | ConvertedType::TimeMillis
        | ConvertedType::Uint8
        | ConvertedType::Uint16
        | ConvertedType::Uint32
        | ConvertedType::Int8
        | ConvertedType::Int16
        | ConvertedType::Int32 => {
            if physical_type != &PhysicalType::Int32 {
                return Err(general_err!(
                    "{:?} can only annotate INT32",
                    converted_type
                ));
            }
        }
        ConvertedType::TimeMicros
        | ConvertedType::TimestampMillis
        | ConvertedType::TimestampMicros
        | ConvertedType::Uint64
        | ConvertedType::Int64 => {
            if physical_type != &PhysicalType::Int64 {
                return Err(general_err!(
                    "{:?} can only annotate INT64",
                    converted_type
                ));
            }
        }
        ConvertedType::Interval => {
            if physical_type != &PhysicalType::FixedLenByteArray(12) {
                return Err(general_err!(
                    "INTERVAL can only annotate FIXED_LEN_BYTE_ARRAY(12)"
                ));
            }
        }
        ConvertedType::Enum => {
            if physical_type != &PhysicalType::ByteArray {
                return Err(general_err!("ENUM can only annotate BYTE_ARRAY fields"));
            }
        }
        _ => {
            return Err(general_err!(
                "{:?} cannot be applied to a primitive type",
                converted_type
            ));
        }
    };
    Ok(())
}

/// Constructors
impl ParquetType {
    pub fn from_fields(name: String, fields: Vec<ParquetType>) -> Self {
        let basic_info = BasicTypeInfo::from_logical_type(name, None, None, None);
        ParquetType::GroupType { basic_info, fields }
    }

    pub fn from_converted(
        name: String,
        fields: Vec<ParquetType>,
        repetition: Option<Repetition>,
        converted_type: Option<ConvertedType>,
        id: Option<i32>,
    ) -> Self {
        let basic_info = BasicTypeInfo::from_converted_type(name, repetition, converted_type, id);
        ParquetType::GroupType { basic_info, fields }
    }

    pub fn try_from_primitive(
        name: String,
        physical_type: PhysicalType,
        repetition: Repetition,
        converted_type: Option<ConvertedType>,
        id: Option<i32>,
    ) -> Result<Self> {
        check_converted_invariants(&physical_type, &converted_type)?;

        let basic_info =
            BasicTypeInfo::from_converted_type(name, Some(repetition), converted_type, id);

        Ok(ParquetType::PrimitiveType {
            basic_info,
            physical_type,
        })
    }

    pub fn from_physical(name: String, physical_type: PhysicalType) -> Self {
        let basic_info = BasicTypeInfo::from_converted_type(name, None, None, None);
        ParquetType::PrimitiveType {
            basic_info,
            physical_type,
        }
    }
}
