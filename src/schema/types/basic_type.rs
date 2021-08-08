use super::super::Repetition;

/// Common type information.
#[derive(Clone, Debug, PartialEq)]
pub struct BasicTypeInfo {
    name: String,
    // Parquet Spec:
    //   Root of the schema does not have a repetition.
    //   All other types must have one.
    repetition: Repetition,
    is_root: bool,
    id: Option<i32>,
}

// Accessors
impl BasicTypeInfo {
    /// Returns field name.
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn is_root(&self) -> bool {
        self.is_root
    }

    /// Returns [`Repetition`](crate::basic::Repetition) value for the type.
    /// Returns `Optional` if the repetition is not defined
    pub fn repetition(&self) -> &Repetition {
        &self.repetition
    }

    /// Returns `true` if id is set, `false` otherwise.
    pub fn id(&self) -> &Option<i32> {
        &self.id
    }
}

// Constructors
impl BasicTypeInfo {
    pub fn new(name: String, repetition: Repetition, id: Option<i32>, is_root: bool) -> Self {
        Self {
            name,
            repetition,
            is_root,
            id,
        }
    }
}
