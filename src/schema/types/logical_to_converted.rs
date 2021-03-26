use super::{ConvertedType, LogicalType, TimeUnit};

// Note: To prevent type loss when converting from ConvertedType to LogicalType,
// the conversion from ConvertedType -> LogicalType is not implemented.
// Such type loss includes:
// - Not knowing the decimal scale and precision of ConvertedType
// - Time and timestamp nanosecond precision, that is not supported in ConvertedType.
pub fn to_converted(logical_type: &LogicalType) -> Option<ConvertedType> {
    match logical_type {
        LogicalType::STRING(_) => Some(ConvertedType::Utf8),
        LogicalType::MAP(_) => Some(ConvertedType::Map),
        LogicalType::LIST(_) => Some(ConvertedType::List),
        LogicalType::ENUM(_) => Some(ConvertedType::Enum),
        LogicalType::DECIMAL(v) => Some(ConvertedType::Decimal(v.precision, v.scale)),
        LogicalType::DATE(_) => Some(ConvertedType::Date),
        LogicalType::TIME(t) => match t.unit {
            TimeUnit::MILLIS(_) => Some(ConvertedType::TimeMillis),
            TimeUnit::MICROS(_) => Some(ConvertedType::TimeMicros),
            TimeUnit::NANOS(_) => None,
        },
        LogicalType::TIMESTAMP(t) => match t.unit {
            TimeUnit::MILLIS(_) => Some(ConvertedType::TimestampMillis),
            TimeUnit::MICROS(_) => Some(ConvertedType::TimestampMicros),
            TimeUnit::NANOS(_) => None,
        },
        LogicalType::INTEGER(t) => Some(match (t.bit_width, t.is_signed) {
            (8, true) => ConvertedType::Int8,
            (16, true) => ConvertedType::Int16,
            (32, true) => ConvertedType::Int32,
            (64, true) => ConvertedType::Int64,
            (8, false) => ConvertedType::Uint8,
            (16, false) => ConvertedType::Uint16,
            (32, false) => ConvertedType::Uint32,
            (64, false) => ConvertedType::Uint64,
            t => panic!("Integer type {:?} is not supported", t),
        }),
        LogicalType::UNKNOWN(_) => None,
        LogicalType::JSON(_) => Some(ConvertedType::Json),
        LogicalType::BSON(_) => Some(ConvertedType::Bson),
        LogicalType::UUID(_) => None,
    }
}
