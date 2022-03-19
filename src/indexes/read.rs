use std::convert::TryInto;
use std::io::{Cursor, Read, Seek, SeekFrom};

use parquet_format_async_temp::{
    thrift::protocol::TCompactInputProtocol, ColumnChunk, ColumnIndex, OffsetIndex, PageLocation,
};

use crate::error::ParquetError;
use crate::metadata::ColumnChunkMetaData;
use crate::schema::types::PhysicalType;

use super::{ByteIndex, FixedLenByteIndex, Index, NativeIndex};

/// Read the [`ColumnIndex`] from the [`ColumnChunk`], if available.
pub fn read_column<R: Read + Seek>(
    reader: &mut R,
    chunk: &ColumnChunkMetaData,
) -> Result<Option<Box<dyn Index>>, ParquetError> {
    let metadata = chunk.column_chunk();
    let (offset, length): (u64, usize) = if let Some(offset) = metadata.column_index_offset {
        let length = metadata.column_index_length.ok_or_else(|| {
            ParquetError::OutOfSpec(
                "The column length must exist if column offset exists".to_string(),
            )
        })?;
        (offset.try_into()?, length.try_into()?)
    } else {
        return Ok(None);
    };

    reader.seek(SeekFrom::Start(offset))?;
    let mut data = vec![0; length];
    reader.read_exact(&mut data)?;

    let mut d = Cursor::new(&data);
    let mut prot = TCompactInputProtocol::new(&mut d);

    let index = ColumnIndex::read_from_in_protocol(&mut prot)?;
    let index = match chunk.descriptor().descriptor.primitive_type.physical_type {
        PhysicalType::Boolean => return Ok(None),
        PhysicalType::Int32 => Box::new(NativeIndex::<i32>::try_from(index)?) as Box<dyn Index>,
        PhysicalType::Int64 => Box::new(NativeIndex::<i64>::try_from(index)?) as _,
        PhysicalType::Int96 => Box::new(NativeIndex::<[u32; 3]>::try_from(index)?) as _,
        PhysicalType::Float => Box::new(NativeIndex::<f32>::try_from(index)?),
        PhysicalType::Double => Box::new(NativeIndex::<f64>::try_from(index)?),
        PhysicalType::ByteArray => Box::new(ByteIndex::try_from(index)?),
        PhysicalType::FixedLenByteArray(size) => {
            Box::new(FixedLenByteIndex::try_from((index, size))?)
        }
    };

    Ok(Some(index))
}

/// Read [`PageLocation`]s from the [`ColumnChunk`], if available.
pub fn read_page_locations<R: Read + Seek>(
    reader: &mut R,
    chunk: &ColumnChunk,
) -> Result<Option<Vec<PageLocation>>, ParquetError> {
    let (offset, length): (u64, usize) = if let Some(offset) = chunk.offset_index_offset {
        let length = chunk.offset_index_length.ok_or_else(|| {
            ParquetError::OutOfSpec(
                "The column length must exist if column offset exists".to_string(),
            )
        })?;
        (offset.try_into()?, length.try_into()?)
    } else {
        return Ok(None);
    };

    reader.seek(SeekFrom::Start(offset))?;
    let mut data = vec![0; length];
    reader.read_exact(&mut data)?;

    let mut d = Cursor::new(&data);
    let mut prot = TCompactInputProtocol::new(&mut d);
    let offset = OffsetIndex::read_from_in_protocol(&mut prot)?;

    Ok(Some(offset.page_locations))
}
