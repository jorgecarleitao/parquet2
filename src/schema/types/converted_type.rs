#[derive(Clone, Debug, PartialEq)]
pub enum ConvertedType {
    Utf8,
    /// a map is converted as an optional field containing a repeated key/value pair
    Map,
    /// a key/value pair is converted into a group of two fields
    MapKeyValue,
    /// a list is converted into an optional field containing a repeated field for its
    /// values
    List,
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


pub fn converted_to_converted(ty: &parquet_format::ConvertedType) -> ConvertedType {
    match ty {
        parquet_format::ConvertedType::Utf8 => ConvertedType::Utf8,
        parquet_format::ConvertedType::Map => ConvertedType::Map,
        parquet_format::ConvertedType::MapKeyValue => ConvertedType::MapKeyValue,
        parquet_format::ConvertedType::List => ConvertedType::List,
        parquet_format::ConvertedType::Enum => ConvertedType::Enum,
        parquet_format::ConvertedType::Decimal => unreachable!(),
        parquet_format::ConvertedType::Date => ConvertedType::Date,
        parquet_format::ConvertedType::TimeMillis => ConvertedType::TimeMillis,
        parquet_format::ConvertedType::TimeMicros => ConvertedType::TimeMicros,
        parquet_format::ConvertedType::TimestampMillis => ConvertedType::TimestampMillis,
        parquet_format::ConvertedType::TimestampMicros => ConvertedType::TimestampMicros,
        parquet_format::ConvertedType::Uint8 => ConvertedType::Uint8,
        parquet_format::ConvertedType::Uint16 => ConvertedType::Uint16,
        parquet_format::ConvertedType::Uint32 => ConvertedType::Uint32,
        parquet_format::ConvertedType::Uint64 => ConvertedType::Uint64,
        parquet_format::ConvertedType::Int8 => ConvertedType::Int8,
        parquet_format::ConvertedType::Int16 => ConvertedType::Int16,
        parquet_format::ConvertedType::Int32 => ConvertedType::Int32,
        parquet_format::ConvertedType::Int64 => ConvertedType::Int64,
        parquet_format::ConvertedType::Json => ConvertedType::Json,
        parquet_format::ConvertedType::Bson => ConvertedType::Bson,
        parquet_format::ConvertedType::Interval => ConvertedType::Interval,
    }
}
