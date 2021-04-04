#[derive(Debug, PartialEq)]
pub enum ParquetError {
    /// General Parquet error.
    General(String),
    /// When the parquet file is known to be out of spec.
    OutOfSpec(String),
}

impl std::error::Error for ParquetError {}

impl std::fmt::Display for ParquetError {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ParquetError::General(message) => {
                write!(fmt, "{}", message)
            }
            ParquetError::OutOfSpec(message) => {
                write!(fmt, "{}", message)
            }
        }
    }
}

#[cfg(feature = "snappy")]
impl From<snap::Error> for ParquetError {
    fn from(e: snap::Error) -> ParquetError {
        ParquetError::General(format!("underlying snap error: {}", e))
    }
}

impl From<thrift::Error> for ParquetError {
    fn from(e: thrift::Error) -> ParquetError {
        ParquetError::General(format!("underlying thrift error: {}", e))
    }
}

impl From<std::io::Error> for ParquetError {
    fn from(e: std::io::Error) -> ParquetError {
        ParquetError::General(format!("underlying IO error: {}", e))
    }
}

/// A specialized `Result` for Parquet errors.
pub type Result<T> = std::result::Result<T, ParquetError>;

macro_rules! general_err {
    ($fmt:expr) => (ParquetError::General($fmt.to_owned()));
    ($fmt:expr, $($args:expr),*) => (ParquetError::General(format!($fmt, $($args),*)));
    ($e:expr, $fmt:expr) => (ParquetError::General($fmt.to_owned(), $e));
    ($e:ident, $fmt:expr, $($args:tt),*) => (
        ParquetError::General(&format!($fmt, $($args),*), $e));
}
