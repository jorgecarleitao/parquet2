use std::convert::TryInto;
use std::io::{Cursor, Read, Seek, SeekFrom};

use parquet_format_async_temp::{
    thrift::protocol::TCompactInputProtocol, ColumnChunk, ColumnIndex, OffsetIndex, PageLocation,
};

use crate::error::ParquetError;

/// Read the [`ColumnIndex`] from the [`ColumnChunk`], if available.
pub fn read_column<R: Read + Seek>(
    reader: &mut R,
    chunk: &ColumnChunk,
) -> Result<Option<ColumnIndex>, ParquetError> {
    let (offset, length): (u64, usize) = if let Some(offset) = chunk.column_index_offset {
        let length = chunk.column_index_length.ok_or_else(|| {
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
    Ok(Some(ColumnIndex::read_from_in_protocol(&mut prot)?))
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
