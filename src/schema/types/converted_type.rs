use crate::errors::{ParquetError, Result};
use parquet_format::ConvertedType;

#[derive(Clone, Debug, PartialEq)]
pub enum PrimitiveConvertedType {
    Utf8,
    /// an enum is converted into a binary field
    Enum,
    /// A decimal value.
    ///
    /// This may be used to annotate binary or fixed primitive types. The
    /// underlying byte array stores the unscaled value encoded as two's
    /// complement using big-endian byte order (the most significant byte is the
    /// zeroth element). The value of the decimal is the value * 10^{-scale}.
    ///
    /// This must be accompanied by a (maximum) precision and a scale in the
    /// SchemaElement. The precision specifies the number of digits in the decimal
    /// and the scale stores the location of the decimal point. For example 1.23
    /// would have precision 3 (3 total digits) and scale 2 (the decimal point is
    /// 2 digits over).
    // (precision, scale)
    Decimal(i32, i32),
    /// A Date
    ///
    /// Stored as days since Unix epoch, encoded as the INT32 physical type.
    ///
    Date,
    /// A time
    ///
    /// The total number of milliseconds since midnight.  The value is stored
    /// as an INT32 physical type.
    TimeMillis,
    /// A time.
    ///
    /// The total number of microseconds since midnight.  The value is stored as
    /// an INT64 physical type.
    TimeMicros,
    /// A date/time combination
    ///
    /// Date and time recorded as milliseconds since the Unix epoch.  Recorded as
    /// a physical type of INT64.
    TimestampMillis,
    /// A date/time combination
    ///
    /// Date and time recorded as microseconds since the Unix epoch.  The value is
    /// stored as an INT64 physical type.
    TimestampMicros,
    /// An unsigned integer value.
    ///
    /// The number describes the maximum number of meainful data bits in
    /// the stored value. 8, 16 and 32 bit values are stored using the
    /// INT32 physical type.  64 bit values are stored using the INT64
    /// physical type.
    ///
    Uint8,
    Uint16,
    Uint32,
    Uint64,
    /// A signed integer value.
    ///
    /// The number describes the maximum number of meainful data bits in
    /// the stored value. 8, 16 and 32 bit values are stored using the
    /// INT32 physical type.  64 bit values are stored using the INT64
    /// physical type.
    ///
    Int8,
    Int16,
    Int32,
    Int64,
    /// An embedded JSON document
    ///
    /// A JSON document embedded within a single UTF8 column.
    Json,
    /// An embedded BSON document
    ///
    /// A BSON document embedded within a single BINARY column.
    Bson,
    /// An interval of time
    ///
    /// This type annotates data stored as a FIXED_LEN_BYTE_ARRAY of length 12
    /// This data is composed of three separate little endian unsigned
    /// integers.  Each stores a component of a duration of time.  The first
    /// integer identifies the number of months associated with the duration,
    /// the second identifies the number of days associated with the duration
    /// and the third identifies the number of milliseconds associated with
    /// the provided duration.  This duration of time is independent of any
    /// particular timezone or date.
    Interval,
}

#[derive(Clone, Debug, PartialEq)]
pub enum GroupConvertedType {
    /// a map is converted as an optional field containing a repeated key/value pair
    Map,
    /// a key/value pair is converted into a group of two fields
    MapKeyValue,
    /// a list is converted into an optional field containing a repeated field for its
    /// values
    List,
}

pub fn converted_to_primitive_converted(
    ty: &ConvertedType,
    maybe_decimal: Option<(i32, i32)>,
) -> Result<PrimitiveConvertedType> {
    use PrimitiveConvertedType::*;
    Ok(match ty {
        ConvertedType::Utf8 => Utf8,
        ConvertedType::Enum => Enum,
        ConvertedType::Decimal => {
            if let Some(maybe_decimal) = maybe_decimal {
                Decimal(maybe_decimal.0, maybe_decimal.1)
            } else {
                return Err(general_err!("Decimal requires a precision and scale"));
            }
        }
        ConvertedType::Date => Date,
        ConvertedType::TimeMillis => TimeMillis,
        ConvertedType::TimeMicros => TimeMicros,
        ConvertedType::TimestampMillis => TimestampMillis,
        ConvertedType::TimestampMicros => TimestampMicros,
        ConvertedType::Uint8 => Uint8,
        ConvertedType::Uint16 => Uint16,
        ConvertedType::Uint32 => Uint32,
        ConvertedType::Uint64 => Uint64,
        ConvertedType::Int8 => Int8,
        ConvertedType::Int16 => Int16,
        ConvertedType::Int32 => Int32,
        ConvertedType::Int64 => Int64,
        ConvertedType::Json => Json,
        ConvertedType::Bson => Bson,
        ConvertedType::Interval => Interval,
        _ => {
            return Err(general_err!(
                "Converted type \"{:?}\" cannot be applied to a primitive type",
                ty
            ))
        }
    })
}

pub fn converted_to_group_converted(ty: &ConvertedType) -> Result<GroupConvertedType> {
    use GroupConvertedType::*;
    Ok(match ty {
        ConvertedType::Map => Map,
        ConvertedType::List => List,
        ConvertedType::MapKeyValue => MapKeyValue,
        _ => {
            return Err(general_err!(
                "Converted type \"{:?}\" cannot be applied to a primitive type",
                ty
            ))
        }
    })
}

pub fn primitive_converted_to_converted(
    ty: &PrimitiveConvertedType,
) -> (ConvertedType, Option<(i32, i32)>) {
    use PrimitiveConvertedType::*;
    match ty {
        Utf8 => (ConvertedType::Utf8, None),
        Enum => (ConvertedType::Enum, None),
        Decimal(precision, scale) => (ConvertedType::Decimal, Some((*precision, *scale))),
        Date => (ConvertedType::Date, None),
        TimeMillis => (ConvertedType::TimeMillis, None),
        TimeMicros => (ConvertedType::TimeMicros, None),
        TimestampMillis => (ConvertedType::TimestampMillis, None),
        TimestampMicros => (ConvertedType::TimestampMicros, None),
        Uint8 => (ConvertedType::Uint8, None),
        Uint16 => (ConvertedType::Uint16, None),
        Uint32 => (ConvertedType::Uint32, None),
        Uint64 => (ConvertedType::Uint64, None),
        Int8 => (ConvertedType::Int8, None),
        Int16 => (ConvertedType::Int16, None),
        Int32 => (ConvertedType::Int32, None),
        Int64 => (ConvertedType::Int64, None),
        Json => (ConvertedType::Json, None),
        Bson => (ConvertedType::Bson, None),
        Interval => (ConvertedType::Interval, None),
    }
}

pub fn group_converted_converted_to(ty: &GroupConvertedType) -> ConvertedType {
    use GroupConvertedType::*;
    match ty {
        Map => ConvertedType::Map,
        List => ConvertedType::List,
        MapKeyValue => ConvertedType::MapKeyValue,
    }
}
