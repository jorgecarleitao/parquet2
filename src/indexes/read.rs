use std::convert::TryInto;
use std::io::{Cursor, Read, Seek, SeekFrom};

use parquet_format_async_temp::{
    thrift::protocol::TCompactInputProtocol, OffsetIndex, PageLocation,
};

use crate::error::ParquetError;
use crate::metadata::ColumnChunkMetaData;

use super::deserialize::deserialize;
use super::Index;

/// Read the column index from the [`ColumnChunkMetaData`] if available and deserializes it into [`Index`].
pub fn read_column_index<R: Read + Seek>(
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

    let primitive_type = chunk.descriptor().descriptor.primitive_type.clone();
    deserialize(&data, primitive_type)
}

/// Read [`PageLocation`]s from the [`ColumnChunk`], if available.
pub fn read_page_locations<R: Read + Seek>(
    reader: &mut R,
    chunk: &ColumnChunkMetaData,
) -> Result<Option<Vec<PageLocation>>, ParquetError> {
    let (offset, length): (u64, usize) =
        if let Some(offset) = chunk.column_chunk().offset_index_offset {
            let length = chunk.column_chunk().offset_index_length.ok_or_else(|| {
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
