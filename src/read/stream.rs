use std::io::SeekFrom;

use futures::{io::Cursor, AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};
use parquet_format_async_temp::thrift::protocol::TCompactInputStreamProtocol;
use parquet_format_async_temp::FileMetaData as TFileMetaData;

use super::super::{metadata::*, DEFAULT_FOOTER_READ_SIZE, FOOTER_SIZE, PARQUET_MAGIC};
use super::metadata::{metadata_len, parse_column_orders};
use crate::error::{ParquetError, Result};

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

pub async fn read_metadata<R: AsyncRead + AsyncSeek + Send + std::marker::Unpin>(
    reader: &mut R,
) -> Result<FileMetaData> {
    // check file is large enough to hold footer
    let file_size = stream_len(reader).await?;
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
    let mut default_len_end_buf = vec![0; default_end_len];
    reader.read_exact(&mut default_len_end_buf).await?;

    // check this is indeed a parquet file
    if default_len_end_buf[default_end_len - 4..] != PARQUET_MAGIC {
        return Err(general_err!("Invalid Parquet file. Corrupt footer"));
    }

    let metadata_len = metadata_len(&default_len_end_buf, default_end_len);

    if metadata_len < 0 {
        return Err(general_err!(
            "Invalid Parquet file. Metadata length is less than zero ({})",
            metadata_len
        ));
    }
    let footer_metadata_len = FOOTER_SIZE + metadata_len as u64;

    let t_file_metadata = if footer_metadata_len > file_size {
        return Err(general_err!(
            "Invalid Parquet file. Metadata start is less than zero ({})",
            file_size as i64 - footer_metadata_len as i64
        ));
    } else if footer_metadata_len < DEFAULT_FOOTER_READ_SIZE {
        // the whole metadata is in the bytes we already read
        // build up the reader covering the entire metadata
        let mut reader = Cursor::new(default_len_end_buf);
        reader
            .seek(SeekFrom::End(-(footer_metadata_len as i64)))
            .await?;

        let mut prot = TCompactInputStreamProtocol::new(reader);
        TFileMetaData::stream_from_in_protocol(&mut prot).await?
    } else {
        // the end of file read by default is not long enough, read again including all metadata.
        reader
            .seek(SeekFrom::End(-(footer_metadata_len as i64)))
            .await?;

        let mut prot = TCompactInputStreamProtocol::new(reader);
        TFileMetaData::stream_from_in_protocol(&mut prot).await?
    };

    let schema = t_file_metadata.schema.iter().collect::<Vec<_>>();
    let schema_descr = SchemaDescriptor::try_from_thrift(&schema)?;

    let row_groups = t_file_metadata
        .row_groups
        .into_iter()
        .map(|rg| RowGroupMetaData::try_from_thrift(&schema_descr, rg))
        .collect::<Result<Vec<_>>>()?;

    // compute and cache column orders
    let column_orders = t_file_metadata
        .column_orders
        .map(|orders| parse_column_orders(&orders, &schema_descr));

    Ok(FileMetaData::new(
        t_file_metadata.version,
        t_file_metadata.num_rows,
        t_file_metadata.created_by,
        row_groups,
        t_file_metadata.key_value_metadata,
        schema_descr,
        column_orders,
    ))
}
