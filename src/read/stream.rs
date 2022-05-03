use std::io::{Cursor, Seek, SeekFrom};

use futures::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};
use parquet_format_async_temp::thrift::protocol::TCompactInputProtocol;
use parquet_format_async_temp::FileMetaData as TFileMetaData;

use super::super::{metadata::FileMetaData, DEFAULT_FOOTER_READ_SIZE, FOOTER_SIZE, PARQUET_MAGIC};
use super::metadata::metadata_len;
use crate::error::{Error, Result};

async fn stream_len(
    seek: &mut (impl AsyncSeek + std::marker::Unpin),
) -> std::result::Result<u64, std::io::Error> {
    let old_pos = seek.seek(SeekFrom::Current(0)).await?;
    let len = seek.seek(SeekFrom::End(0)).await?;

    // Avoid seeking a third time when we were already at the end of the
    // stream. The branch is usually way cheaper than a seek operation.
    if old_pos != len {
        seek.seek(SeekFrom::Start(old_pos)).await?;
    }

    Ok(len)
}

/// Asynchronously reads the files' metadata
pub async fn read_metadata<R: AsyncRead + AsyncSeek + Send + std::marker::Unpin>(
    reader: &mut R,
) -> Result<FileMetaData> {
    let file_size = stream_len(reader).await?;

    // check file is large enough to hold footer
    if file_size < FOOTER_SIZE {
        return Err(general_err!(
            "Invalid Parquet file. Size is smaller than footer"
        ));
    }

    // read and cache up to DEFAULT_FOOTER_READ_SIZE bytes from the end and process the footer
    let default_end_len = std::cmp::min(DEFAULT_FOOTER_READ_SIZE, file_size) as usize;
    reader
        .seek(SeekFrom::End(-(default_end_len as i64)))
        .await?;
    let mut buffer = vec![0; default_end_len];
    reader.read_exact(&mut buffer).await?;

    // check this is indeed a parquet file
    if buffer[default_end_len - 4..] != PARQUET_MAGIC {
        return Err(general_err!(
            "Invalid file. The footer does not contain Parquet's magic numbers"
        ));
    }

    let metadata_len = metadata_len(&buffer, default_end_len);

    if metadata_len < 0 {
        return Err(general_err!(
            "Invalid file. Metadata length is less than zero ({})",
            metadata_len
        ));
    }
    let footer_len = FOOTER_SIZE + metadata_len as u64;
    if footer_len > file_size {
        return Err(general_err!(
            "Invalid Parquet file. Metadata start is less than zero ({})",
            file_size as i64 - footer_len as i64
        ));
    }

    let metadata = if footer_len < DEFAULT_FOOTER_READ_SIZE {
        // the whole metadata is in the bytes we already read
        // build up the reader covering the entire metadata
        let mut reader = Cursor::new(buffer);
        reader.seek(SeekFrom::End(-(footer_len as i64)))?;

        let mut prot = TCompactInputProtocol::new(reader);
        TFileMetaData::read_from_in_protocol(&mut prot)?
    } else {
        // the end of file read by default is not long enough, read again including all metadata.
        reader.seek(SeekFrom::End(-(footer_len as i64))).await?;
        let mut buffer = vec![0; footer_len as usize];
        reader.read_exact(&mut buffer).await?;

        let reader = Cursor::new(buffer);

        let mut prot = TCompactInputProtocol::new(reader);
        TFileMetaData::read_from_in_protocol(&mut prot)?
    };

    FileMetaData::try_from_thrift(metadata)
}
