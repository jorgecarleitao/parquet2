#[macro_use]
pub mod errors;

pub mod schema;

/// The in-memory representation of a parquet primitive type
trait Type {
    type Bytes: AsRef<[u8]>;

    fn to_le_bytes(&self) -> Self::Bytes;

    fn to_be_bytes(&self) -> Self::Bytes;

    fn from_be_bytes(bytes: Self::Bytes) -> Self;

    fn from_le_bytes(bytes: Self::Bytes) -> Self;
}
