use super::{ConvertedType, LogicalType, Repetition};

/// Basic type info. This contains information such as the name of the type,
/// the repetition level, the logical type and the kind of the type (group, primitive).
#[derive(Clone, Debug, PartialEq)]
pub struct BasicTypeInfo {
    name: String,
    repetition: Repetition,
    converted_type: Option<ConvertedType>,
    logical_type: Option<LogicalType>,
    id: Option<i32>,
}

// Accessors
impl BasicTypeInfo {
    /// Returns field name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns [`Repetition`](crate::basic::Repetition) value for the type.
    pub fn repetition(&self) -> &Repetition {
        &self.repetition
    }

    /// Returns [`ConvertedType`](crate::basic::ConvertedType) value for the type.
    pub fn converted_type(&self) -> &Option<ConvertedType> {
        &self.converted_type
    }

    /// Returns [`LogicalType`](crate::basic::LogicalType) value for the type.
    pub fn logical_type(&self) -> &Option<LogicalType> {
        // Unlike ConvertedType, LogicalType cannot implement Copy, thus we clone it
        &self.logical_type
    }

    /// Returns `true` if id is set, `false` otherwise.
    pub fn has_id(&self) -> bool {
        self.id.is_some()
    }

    /// Returns id value for the type.
    pub fn id(&self) -> i32 {
        assert!(self.id.is_some());
        self.id.unwrap()
    }
}

// Constructors
impl BasicTypeInfo {
    pub fn from_logical_type(
        name: String,
        repetition: Option<Repetition>,
        logical_type: Option<LogicalType>,
        id: Option<i32>,
    ) -> Self {
        Self {
            name,
            repetition: repetition.unwrap_or(Repetition::Optional),
            logical_type,
            converted_type: None,
            id,
        }
    }

    pub fn from_converted_type(
        name: String,
        repetition: Option<Repetition>,
        converted_type: Option<ConvertedType>,
        id: Option<i32>,
    ) -> Self {
        Self {
            name,
            repetition: repetition.unwrap_or(Repetition::Optional),
            logical_type: None,
            converted_type,
            id,
        }
    }
}
