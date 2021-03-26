use super::Repetition;

/// Basic type info. This contains information such as the name of the type,
/// the repetition level, the logical type and the kind of the type (group, primitive).
#[derive(Clone, Debug, PartialEq)]
pub struct BasicTypeInfo {
    name: String,
    repetition: Repetition,
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
    pub fn new(name: String, repetition: Option<Repetition>, id: Option<i32>) -> Self {
        Self {
            name,
            repetition: repetition.unwrap_or(Repetition::Optional),
            id,
        }
    }
}
