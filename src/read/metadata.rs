use std::convert::TryInto;
use std::{
    cmp::min,
    io::{Cursor, Read, Seek, SeekFrom},
};

use parquet_format_async_temp::thrift::protocol::TCompactInputProtocol;
use parquet_format_async_temp::FileMetaData as TFileMetaData;

use super::super::{
    metadata::FileMetaData, DEFAULT_FOOTER_READ_SIZE, FOOTER_SIZE, HEADER_SIZE, PARQUET_MAGIC,
};

use crate::error::{Error, Result};

pub(super) fn metadata_len(buffer: &[u8], len: usize) -> i32 {
    i32::from_le_bytes(buffer[len - 8..len - 4].try_into().unwrap())
}

// see (unstable) Seek::stream_len
fn stream_len(seek: &mut impl Seek) -> std::result::Result<u64, std::io::Error> {
    let old_pos = seek.seek(SeekFrom::Current(0))?;
    let len = seek.seek(SeekFrom::End(0))?;

    // Avoid seeking a third time when we were already at the end of the
    // stream. The branch is usually way cheaper than a seek operation.
    if old_pos != len {
        seek.seek(SeekFrom::Start(old_pos))?;
    }

    Ok(len)
}

/// Reads a file's metadata.
// Layout of Parquet file
// +---------------------------+-----+---+
// |      Rest of file         |  B  | A |
// +---------------------------+-----+---+
// where A: parquet footer, B: parquet metadata.
//
// The reader first reads DEFAULT_FOOTER_SIZE bytes from the end of the file.
// If it is not enough according to the length indicated in the footer, it reads more bytes.
pub fn read_metadata<R: Read + Seek>(reader: &mut R) -> Result<FileMetaData> {
    // check file is large enough to hold footer
    let file_size = stream_len(reader)?;
    if file_size < HEADER_SIZE + FOOTER_SIZE {
        return Err(general_err!(
            "Invalid Parquet file. Size is smaller than header + footer"
        ));
    }

    // read and cache up to DEFAULT_FOOTER_READ_SIZE bytes from the end and process the footer
    let default_end_len = min(DEFAULT_FOOTER_READ_SIZE, file_size) as usize;
    reader.seek(SeekFrom::End(-(default_end_len as i64)))?;
    let mut buffer = vec![0; default_end_len];
    reader.read_exact(&mut buffer)?;

    // check this is indeed a parquet file
    if buffer[default_end_len - 4..] != PARQUET_MAGIC {
        return Err(general_err!("Invalid Parquet file. Corrupt footer"));
    }

    let metadata = metadata_len(&buffer, default_end_len);

    let metadata_len: u64 = metadata.try_into().map_err(|_| {
        general_err!(
            "Invalid Parquet file. Metadata length is less than zero ({})",
            metadata
        )
    })?;

    let footer_len = FOOTER_SIZE + metadata_len;
    if footer_len > file_size {
        return Err(general_err!(
            "Invalid Parquet file. Metadata start is less than zero ({})",
            file_size as i64 - footer_len as i64
        ));
    }

    let reader = if (footer_len as usize) < buffer.len() {
        // the whole metadata is in the bytes we already read
        // build up the reader covering the entire metadata
        let mut reader = Cursor::new(buffer);
        reader.seek(SeekFrom::End(-(footer_len as i64)))?;

        reader
    } else {
        // the end of file read by default is not long enough, read again including all metadata.
        reader.seek(SeekFrom::End(-(footer_len as i64)))?;
        let mut buffer = vec![0; footer_len as usize];
        reader.read_exact(&mut buffer)?;

        Cursor::new(buffer)
    };

    let mut prot = TCompactInputProtocol::new(reader);
    let metadata = TFileMetaData::read_from_in_protocol(&mut prot)
        .map_err(|e| Error::General(format!("Could not parse metadata: {}", e)))?;

    FileMetaData::try_from_thrift(metadata)
}
