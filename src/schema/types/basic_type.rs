use super::super::Repetition;

/// Common type information.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct FieldInfo {
    pub name: String,
    pub repetition: Repetition,
    pub id: Option<i32>,
}
