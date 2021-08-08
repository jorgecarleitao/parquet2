use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum ParquetError {
    /// General Parquet error.
    General(String),
    /// When the parquet file is known to be out of spec.
    OutOfSpec(String),
    // An error originating from a consumer or dependency
    External(String, Arc<dyn std::error::Error + Send + Sync>),
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
            ParquetError::External(message, err) => {
                write!(fmt, "{}: {}", message, err)
            }
        }
    }
}

impl ParquetError {
    /// Wraps an external error in an `ParquetError`.
    pub fn from_external_error(error: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::External("".to_string(), Arc::new(error))
    }
}

#[cfg(feature = "snappy")]
impl From<snap::Error> for ParquetError {
    fn from(e: snap::Error) -> ParquetError {
        ParquetError::General(format!("underlying snap error: {}", e))
    }
}

impl From<parquet_format_async_temp::thrift::Error> for ParquetError {
    fn from(e: parquet_format_async_temp::thrift::Error) -> ParquetError {
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
