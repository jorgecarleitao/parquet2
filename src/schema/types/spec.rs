// see https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
use crate::error::{ParquetError, Result};

use super::{LogicalType, PhysicalType, PrimitiveConvertedType, TimeType, TimeUnit};

fn check_decimal_invariants(
    physical_type: &PhysicalType,
    precision: i32,
    scale: i32,
) -> Result<()> {
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
            if !(1..=9).contains(&precision) {
                return Err(general_err!(
                    "Cannot represent INT32 as DECIMAL with precision {}",
                    precision
                ));
            }
        }
        PhysicalType::Int64 => {
            if !(1..=18).contains(&precision) {
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
        PhysicalType::ByteArray => {}
        _ => {
            return Err(general_err!(
                "DECIMAL can only annotate INT32, INT64, BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY"
            ))
        }
    };
    Ok(())
}

pub fn check_converted_invariants(
    physical_type: &PhysicalType,
    converted_type: &Option<PrimitiveConvertedType>,
) -> Result<()> {
    if converted_type.is_none() {
        return Ok(());
    };
    let converted_type = converted_type.as_ref().unwrap();

    use PrimitiveConvertedType::*;
    match converted_type {
        Utf8 | Bson | Json => {
            if physical_type != &PhysicalType::ByteArray {
                return Err(general_err!(
                    "{:?} can only annotate BYTE_ARRAY fields",
                    converted_type
                ));
            }
        }
        Decimal(precision, scale) => {
            check_decimal_invariants(physical_type, *precision, *scale)?;
        }
        Date | TimeMillis | Uint8 | Uint16 | Uint32 | Int8 | Int16 | Int32 => {
            if physical_type != &PhysicalType::Int32 {
                return Err(general_err!("{:?} can only annotate INT32", converted_type));
            }
        }
        TimeMicros | TimestampMillis | TimestampMicros | Uint64 | Int64 => {
            if physical_type != &PhysicalType::Int64 {
                return Err(general_err!("{:?} can only annotate INT64", converted_type));
            }
        }
        Interval => {
            if physical_type != &PhysicalType::FixedLenByteArray(12) {
                return Err(general_err!(
                    "INTERVAL can only annotate FIXED_LEN_BYTE_ARRAY(12)"
                ));
            }
        }
        Enum => {
            if physical_type != &PhysicalType::ByteArray {
                return Err(general_err!("ENUM can only annotate BYTE_ARRAY fields"));
            }
        }
    };
    Ok(())
}

pub fn check_logical_invariants(
    physical_type: &PhysicalType,
    logical_type: &Option<LogicalType>,
) -> Result<()> {
    use parquet_format_async_temp::LogicalType::*;
    if logical_type.is_none() {
        return Ok(());
    };
    let logical_type = logical_type.as_ref().unwrap();

    // Check that logical type and physical type are compatible
    match (logical_type, physical_type) {
        (MAP(_), _) | (LIST(_), _) => {
            return Err(general_err!(
                "{:?} cannot be applied to a primitive type",
                logical_type
            ));
        }
        (ENUM(_), PhysicalType::ByteArray) => {}
        (DECIMAL(t), _) => {
            check_decimal_invariants(physical_type, t.precision, t.scale)?;
        }
        (DATE(_), PhysicalType::Int32) => {}
        (
            TIME(TimeType {
                unit: TimeUnit::MILLIS(_),
                ..
            }),
            PhysicalType::Int32,
        ) => {}
        (TIME(t), PhysicalType::Int64) => {
            if t.unit == TimeUnit::MILLIS(Default::default()) {
                return Err(general_err!("Cannot use millisecond unit on INT64 type"));
            }
        }
        (TIMESTAMP(_), PhysicalType::Int64) => {}
        (INTEGER(t), PhysicalType::Int32) if t.bit_width <= 32 => {}
        (INTEGER(t), PhysicalType::Int64) if t.bit_width == 64 => {}
        // Null type
        (UNKNOWN(_), PhysicalType::Int32) => {}
        (STRING(_), PhysicalType::ByteArray) => {}
        (JSON(_), PhysicalType::ByteArray) => {}
        (BSON(_), PhysicalType::ByteArray) => {}
        // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#uuid
        (UUID(_), PhysicalType::FixedLenByteArray(16)) => {}
        (a, b) => return Err(general_err!("Cannot annotate {:?} from {:?} fields", a, b)),
    };
    Ok(())
}
