use super::super::Repetition;

/// Common type information.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct FieldInfo {
    pub name: String,
    pub repetition: Repetition,
    pub is_root: bool,
    pub id: Option<i32>,
}

// Constructors
impl FieldInfo {
    pub fn new(name: String, repetition: Repetition, id: Option<i32>, is_root: bool) -> Self {
        Self {
            name,
            repetition,
            is_root,
            id,
        }
    }
}
