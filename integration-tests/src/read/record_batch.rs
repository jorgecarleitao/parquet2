use std::io::{Read, Seek};

use streaming_iterator::StreamingIterator;

use super::{page_to_array, Array};
use crate::read::{get_page_iterator, read_metadata, Decompressor};
use crate::{error::Result, metadata::FileMetaData};

/// Single threaded iterator of `RecordBatch`.
pub struct RecordReader<R: Read + Seek> {
    reader: R,
    buffer: Vec<u8>,
    decompress_buffer: Vec<u8>,
    metadata: FileMetaData,
    current_group: usize,
}

impl<R: Read + Seek> RecordReader<R> {
    pub fn try_new(mut reader: R) -> Result<Self> {
        let metadata = read_metadata(&mut reader)?;
        Ok(Self {
            reader,
            metadata,
            current_group: 0,
            buffer: vec![],
            decompress_buffer: vec![],
        })
    }
}

impl<R: Read + Seek> Iterator for RecordReader<R> {
    type Item = Result<Vec<Vec<Array>>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_group == self.metadata.row_groups.len() {
            return None;
        };
        let row_group = self.current_group;
        let metadata = self.metadata.clone();
        let group = &metadata.row_groups[row_group];

        let b1 = std::mem::take(&mut self.buffer);
        let b2 = std::mem::take(&mut self.decompress_buffer);

        let a = group.columns().iter().enumerate().try_fold(
            (b1, b2, vec![]),
            |(b1, b2, mut columns), (column, column_meta)| {
                let pages = get_page_iterator(&metadata, row_group, column, &mut self.reader, b1)?;
                let mut pages = Decompressor::new(pages, b2);

                let mut arrays = vec![];
                while let Some(page) = pages.next() {
                    let array = page_to_array(page.as_ref().unwrap(), column_meta.descriptor())?;
                    arrays.push(array);
                }
                columns.push(arrays);
                let (b1, b2) = pages.into_buffers();
                Result::Ok((b1, b2, columns))
            },
        );

        self.current_group += 1;
        Some(a.and_then(|(b1, b2, columns)| {
            self.buffer = b1;
            self.decompress_buffer = b2;
            Ok(columns)
        }))
    }
}
