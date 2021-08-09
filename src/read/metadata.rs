// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::convert::TryInto;
use std::{
    cmp::min,
    io::{Cursor, Read, Seek, SeekFrom},
};

use parquet_format_async_temp::thrift::protocol::TCompactInputProtocol;
use parquet_format_async_temp::{ColumnOrder as TColumnOrder, FileMetaData as TFileMetaData};

use super::super::metadata::get_sort_order;
use super::super::metadata::ColumnOrder;
use super::super::metadata::SchemaDescriptor;
use super::super::{metadata::*, DEFAULT_FOOTER_READ_SIZE, FOOTER_SIZE, PARQUET_MAGIC};

use crate::error::{ParquetError, Result};
use crate::schema::types::ParquetType;

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
    if file_size < FOOTER_SIZE {
        return Err(general_err!(
            "Invalid Parquet file. Size is smaller than footer"
        ));
    }

    // read and cache up to DEFAULT_FOOTER_READ_SIZE bytes from the end and process the footer
    let default_end_len = min(DEFAULT_FOOTER_READ_SIZE, file_size) as usize;
    reader.seek(SeekFrom::End(-(default_end_len as i64)))?;
    let mut default_len_end_buf = vec![0; default_end_len];
    reader.read_exact(&mut default_len_end_buf)?;

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
        reader.seek(SeekFrom::End(-(footer_metadata_len as i64)))?;

        let mut prot = TCompactInputProtocol::new(reader);
        TFileMetaData::read_from_in_protocol(&mut prot)
    } else {
        // the end of file read by default is not long enough, read again including all metadata.
        reader.seek(SeekFrom::End(-(footer_metadata_len as i64)))?;

        let mut prot = TCompactInputProtocol::new(reader);
        TFileMetaData::read_from_in_protocol(&mut prot)
    }
    .map_err(|e| ParquetError::General(format!("Could not parse metadata: {}", e)))?;

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

/// Parses column orders from Thrift definition.
/// If no column orders are defined, returns `None`.
pub(super) fn parse_column_orders(
    orders: &[TColumnOrder],
    schema_descr: &SchemaDescriptor,
) -> Vec<ColumnOrder> {
    // Should always be the case
    assert_eq!(
        orders.len(),
        schema_descr.num_columns(),
        "Column order length mismatch"
    );

    schema_descr
        .columns()
        .iter()
        .enumerate()
        .map(|(i, column)| match orders[i] {
            TColumnOrder::TYPEORDER(_) => match column.type_() {
                ParquetType::GroupType { .. } => unreachable!(),
                ParquetType::PrimitiveType {
                    logical_type,
                    converted_type,
                    physical_type,
                    ..
                } => {
                    let sort_order = get_sort_order(logical_type, converted_type, physical_type);
                    ColumnOrder::TypeDefinedOrder(sort_order)
                }
            },
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use super::*;

    use crate::schema::{types::PhysicalType, Repetition};
    use crate::tests::get_path;

    #[test]
    fn test_basics() {
        let mut testdata = get_path();
        testdata.push("alltypes_plain.parquet");
        let mut file = File::open(testdata).unwrap();

        let metadata = read_metadata(&mut file).unwrap();

        let columns = metadata.schema_descr.columns();

        /*
        from pyarrow:
        required group field_id=0 schema {
            optional int32 field_id=1 id;
            optional boolean field_id=2 bool_col;
            optional int32 field_id=3 tinyint_col;
            optional int32 field_id=4 smallint_col;
            optional int32 field_id=5 int_col;
            optional int64 field_id=6 bigint_col;
            optional float field_id=7 float_col;
            optional double field_id=8 double_col;
            optional binary field_id=9 date_string_col;
            optional binary field_id=10 string_col;
            optional int96 field_id=11 timestamp_col;
        }
        */
        let expected = vec![
            PhysicalType::Int32,
            PhysicalType::Boolean,
            PhysicalType::Int32,
            PhysicalType::Int32,
            PhysicalType::Int32,
            PhysicalType::Int64,
            PhysicalType::Float,
            PhysicalType::Double,
            PhysicalType::ByteArray,
            PhysicalType::ByteArray,
            PhysicalType::Int96,
        ];

        let result = columns
            .iter()
            .map(|column| match column.type_() {
                ParquetType::PrimitiveType {
                    physical_type,
                    basic_info,
                    ..
                } => {
                    assert_eq!(basic_info.repetition(), &Repetition::Optional);
                    *physical_type
                }
                _ => {
                    panic!("All types should be primitives");
                }
            })
            .collect::<Vec<_>>();

        assert_eq!(expected, result);
    }
}
