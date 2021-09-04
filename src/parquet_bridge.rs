// Bridges structs from thrift-generated code to rust enums.
use std::convert::TryFrom;
use std::convert::TryInto;

use crate::error::ParquetError;
use parquet_format_async_temp::CompressionCodec;
use parquet_format_async_temp::DataPageHeader;
use parquet_format_async_temp::DataPageHeaderV2;
use parquet_format_async_temp::Encoding as ParquetEncoding;
use parquet_format_async_temp::FieldRepetitionType;
use parquet_format_async_temp::PageType as ParquetPageType;

#[derive(Debug, Eq, PartialEq, Hash, Clone, Copy)]
pub enum Repetition {
    Required,
    Optional,
    Repeated,
}

impl TryFrom<FieldRepetitionType> for Repetition {
    type Error = ParquetError;

    fn try_from(repetition: FieldRepetitionType) -> Result<Self, Self::Error> {
        Ok(match repetition {
            FieldRepetitionType::REQUIRED => Repetition::Required,
            FieldRepetitionType::OPTIONAL => Repetition::Optional,
            FieldRepetitionType::REPEATED => Repetition::Repeated,
            _ => return Err(ParquetError::OutOfSpec("Thrift out of range".to_string())),
        })
    }
}

impl From<Repetition> for FieldRepetitionType {
    fn from(repetition: Repetition) -> Self {
        match repetition {
            Repetition::Required => FieldRepetitionType::REQUIRED,
            Repetition::Optional => FieldRepetitionType::OPTIONAL,
            Repetition::Repeated => FieldRepetitionType::REPEATED,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Hash, Clone, Copy)]
pub enum Compression {
    Uncompressed,
    Snappy,
    Gzip,
    Lzo,
    Brotli,
    Lz4,
    Zstd,
}

impl TryFrom<CompressionCodec> for Compression {
    type Error = ParquetError;

    fn try_from(codec: CompressionCodec) -> Result<Self, Self::Error> {
        Ok(match codec {
            CompressionCodec::UNCOMPRESSED => Compression::Uncompressed,
            CompressionCodec::SNAPPY => Compression::Snappy,
            CompressionCodec::GZIP => Compression::Gzip,
            CompressionCodec::LZO => Compression::Lzo,
            CompressionCodec::BROTLI => Compression::Brotli,
            CompressionCodec::LZ4 => Compression::Lz4,
            CompressionCodec::ZSTD => Compression::Zstd,
            _ => return Err(ParquetError::OutOfSpec("Thrift out of range".to_string())),
        })
    }
}

impl From<Compression> for CompressionCodec {
    fn from(codec: Compression) -> Self {
        match codec {
            Compression::Uncompressed => CompressionCodec::UNCOMPRESSED,
            Compression::Snappy => CompressionCodec::SNAPPY,
            Compression::Gzip => CompressionCodec::GZIP,
            Compression::Lzo => CompressionCodec::LZO,
            Compression::Brotli => CompressionCodec::BROTLI,
            Compression::Lz4 => CompressionCodec::LZ4,
            Compression::Zstd => CompressionCodec::ZSTD,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Hash, Clone, Copy)]
pub enum PageType {
    DataPage,
    DataPageV2,
    DictionaryPage,
    IndexPage,
}

impl TryFrom<ParquetPageType> for PageType {
    type Error = ParquetError;

    fn try_from(type_: ParquetPageType) -> Result<Self, Self::Error> {
        Ok(match type_ {
            ParquetPageType::DATA_PAGE => PageType::DataPage,
            ParquetPageType::DATA_PAGE_V2 => PageType::DataPageV2,
            ParquetPageType::DICTIONARY_PAGE => PageType::DictionaryPage,
            ParquetPageType::INDEX_PAGE => PageType::IndexPage,
            _ => return Err(ParquetError::OutOfSpec("Thrift out of range".to_string())),
        })
    }
}

impl From<PageType> for ParquetPageType {
    fn from(type_: PageType) -> Self {
        match type_ {
            PageType::DataPage => ParquetPageType::DATA_PAGE,
            PageType::DataPageV2 => ParquetPageType::DATA_PAGE_V2,
            PageType::DictionaryPage => ParquetPageType::DICTIONARY_PAGE,
            PageType::IndexPage => ParquetPageType::INDEX_PAGE,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Hash, Clone, Copy)]
pub enum Encoding {
    /// Default encoding.
    /// BOOLEAN - 1 bit per value. 0 is false; 1 is true.
    /// INT32 - 4 bytes per value.  Stored as little-endian.
    /// INT64 - 8 bytes per value.  Stored as little-endian.
    /// FLOAT - 4 bytes per value.  IEEE. Stored as little-endian.
    /// DOUBLE - 8 bytes per value.  IEEE. Stored as little-endian.
    /// BYTE_ARRAY - 4 byte length stored as little endian, followed by bytes.
    /// FIXED_LEN_BYTE_ARRAY - Just the bytes.
    Plain,
    /// Deprecated: Dictionary encoding. The values in the dictionary are encoded in the
    /// plain type.
    /// in a data page use RLE_DICTIONARY instead.
    /// in a Dictionary page use PLAIN instead
    PlainDictionary,
    /// Group packed run length encoding. Usable for definition/repetition levels
    /// encoding and Booleans (on one bit: 0 is false; 1 is true.)
    Rle,
    /// Bit packed encoding.  This can only be used if the data has a known max
    /// width.  Usable for definition/repetition levels encoding.
    BitPacked,
    /// Delta encoding for integers. This can be used for int columns and works best
    /// on sorted data
    DeltaBinaryPacked,
    /// Encoding for byte arrays to separate the length values and the data. The lengths
    /// are encoded using DELTA_BINARY_PACKED
    DeltaLengthByteArray,
    /// Incremental-encoded byte array. Prefix lengths are encoded using DELTA_BINARY_PACKED.
    /// Suffixes are stored as delta length byte arrays.
    DeltaByteArray,
    /// Dictionary encoding: the ids are encoded using the RLE encoding
    RleDictionary,
    /// Encoding for floating-point data.
    /// K byte-streams are created where K is the size in bytes of the data type.
    /// The individual bytes of an FP value are scattered to the corresponding stream and
    /// the streams are concatenated.
    /// This itself does not reduce the size of the data but can lead to better compression
    /// afterwards.
    ByteStreamSplit,
}

impl TryFrom<ParquetEncoding> for Encoding {
    type Error = ParquetError;

    fn try_from(encoding: ParquetEncoding) -> Result<Self, Self::Error> {
        Ok(match encoding {
            ParquetEncoding::PLAIN => Encoding::Plain,
            ParquetEncoding::PLAIN_DICTIONARY => Encoding::PlainDictionary,
            ParquetEncoding::RLE => Encoding::Rle,
            ParquetEncoding::BIT_PACKED => Encoding::BitPacked,
            ParquetEncoding::DELTA_BINARY_PACKED => Encoding::DeltaBinaryPacked,
            ParquetEncoding::DELTA_LENGTH_BYTE_ARRAY => Encoding::DeltaLengthByteArray,
            ParquetEncoding::DELTA_BYTE_ARRAY => Encoding::DeltaByteArray,
            ParquetEncoding::RLE_DICTIONARY => Encoding::RleDictionary,
            ParquetEncoding::BYTE_STREAM_SPLIT => Encoding::ByteStreamSplit,
            _ => return Err(ParquetError::OutOfSpec("Thrift out of range".to_string())),
        })
    }
}

impl From<Encoding> for ParquetEncoding {
    fn from(encoding: Encoding) -> Self {
        match encoding {
            Encoding::Plain => ParquetEncoding::PLAIN,
            Encoding::PlainDictionary => ParquetEncoding::PLAIN_DICTIONARY,
            Encoding::Rle => ParquetEncoding::RLE,
            Encoding::BitPacked => ParquetEncoding::BIT_PACKED,
            Encoding::DeltaBinaryPacked => ParquetEncoding::DELTA_BINARY_PACKED,
            Encoding::DeltaLengthByteArray => ParquetEncoding::DELTA_LENGTH_BYTE_ARRAY,
            Encoding::DeltaByteArray => ParquetEncoding::DELTA_BYTE_ARRAY,
            Encoding::RleDictionary => ParquetEncoding::RLE_DICTIONARY,
            Encoding::ByteStreamSplit => ParquetEncoding::BYTE_STREAM_SPLIT,
        }
    }
}

pub trait DataPageHeaderExt {
    fn encoding(&self) -> Encoding;
    fn repetition_level_encoding(&self) -> Encoding;
    fn definition_level_encoding(&self) -> Encoding;
}

impl DataPageHeaderExt for DataPageHeader {
    fn encoding(&self) -> Encoding {
        self.encoding.try_into().unwrap()
    }

    fn repetition_level_encoding(&self) -> Encoding {
        self.repetition_level_encoding.try_into().unwrap()
    }

    fn definition_level_encoding(&self) -> Encoding {
        self.definition_level_encoding.try_into().unwrap()
    }
}

impl DataPageHeaderExt for DataPageHeaderV2 {
    fn encoding(&self) -> Encoding {
        self.encoding.try_into().unwrap()
    }

    fn repetition_level_encoding(&self) -> Encoding {
        Encoding::Rle
    }

    fn definition_level_encoding(&self) -> Encoding {
        Encoding::Rle
    }
}
