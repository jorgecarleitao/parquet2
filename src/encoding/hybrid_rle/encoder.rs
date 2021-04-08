use crate::encoding::ceil8;

use std::io::{Seek, Write};

use super::bitmap::{set, Bitmap, BitmapIter};

pub struct BitmapEncoder<'a, W: Write + Seek> {
    writer: &'a mut W,
    length: usize,
    buffer: u8,
}

#[inline]
fn extend<W: Write, I: Iterator<Item = bool>>(
    writer: &mut W,
    length: usize,
    mut iterator: I,
) -> std::io::Result<u8> {
    let chunks = length / 8;
    let reminder = length % 8;

    (0..chunks).try_for_each(|_| {
        let mut byte = 0u8;
        (0..8).for_each(|i| {
            if iterator.next().unwrap() {
                byte = set(byte, i)
            }
        });
        writer.write_all(&[byte])
    })?;

    if reminder != 0 {
        let mut last = 0u8;
        iterator.enumerate().for_each(|(i, value)| {
            if value {
                last = set(last, i)
            }
        });
        Ok(last)
    } else {
        Ok(0)
    }
}

impl<'a, W: Write + Seek> BitmapEncoder<'a, W> {
    /// Errors if the writer has no capacity to hold 4 bytes, which will contain the encoded length.
    pub fn try_new(writer: &'a mut W) -> std::io::Result<Self> {
        writer.write_all(&[0; 4])?;
        Ok(Self {
            writer,
            length: 0,
            buffer: 0,
        })
    }

    pub fn extend_from_trusted_len_iter<I: IntoIterator<Item = bool>>(
        &mut self,
        iterator: I,
    ) -> std::io::Result<()> {
        let mut iterator = iterator.into_iter();

        // the length of the iterator throughout this function.
        let mut length = iterator.size_hint().1.unwrap();

        let bit_offset = self.length % 8;

        if length < 8 - bit_offset {
            if bit_offset == 0 {
                self.buffer = 0;
            }
            let mut i = bit_offset;
            for value in iterator {
                if value {
                    self.buffer = set(self.buffer, i);
                }
                i += 1;
            }
            self.length += length;
            return Ok(());
        }

        // at this point we know that length will hit a byte boundary and thus
        // increase the buffer.

        if bit_offset != 0 {
            // we are in the middle of a byte; lets finish it
            (bit_offset..8).for_each(|i| {
                let value = iterator.next().unwrap();
                if value {
                    self.buffer = set(self.buffer, i);
                }
            });
            self.writer.write_all(&[self.buffer])?;
            self.length += 8 - bit_offset;
            length -= 8 - bit_offset;
        }

        // everything is aligned; proceed with the bulk operation
        debug_assert_eq!(self.length % 8, 0);

        self.buffer = extend(&mut self.writer, length, iterator)?;
        self.length += length;
        Ok(())
    }

    // this executes a bitmap run
    pub fn extend_from_bitmap(&mut self, bitmap: &Bitmap<'a>) -> std::io::Result<()> {
        if bitmap.len() == 0 {
            return Ok(());
        }
        // todo: split bitmap.
        assert!(bitmap.len() < u32::MAX as usize); // bit-packed-run-len and rle-run-len must be in the range [1, 231 - 1]

        let iterator = BitmapIter::from_bitmap(bitmap);

        self.extend_from_trusted_len_iter(iterator)
    }

    pub fn finish(self) -> std::io::Result<()> {
        if self.length % 8 != 0 {
            self.writer.write_all(&[self.buffer])?;
        }
        self.writer.seek(std::io::SeekFrom::Start(0))?;
        let len = self.length;
        self.writer.write_all(&(ceil8(len) as u32).to_le_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basics_1() -> std::io::Result<()> {
        let bitmap = Bitmap::new(&[0b10011101u8, 0b10011101], 0, 14);

        let mut container = std::io::Cursor::new(vec![]);

        let mut encoder = BitmapEncoder::try_new(&mut container)?;

        encoder.extend_from_bitmap(&bitmap)?;
        encoder.finish()?;

        let vec = container.into_inner();
        assert_eq!(vec, vec![2, 0, 0, 0, 0b10011101u8, 0b00011101]);

        Ok(())
    }

    #[test]
    fn multiple() -> std::io::Result<()> {
        let bitmap1 = Bitmap::new(&[0b10011101u8, 0b10011101], 0, 14);
        let bitmap2 = Bitmap::new(&[0b10011101], 0, 3);

        let mut container = std::io::Cursor::new(vec![]);

        let mut encoder = BitmapEncoder::try_new(&mut container)?;

        encoder.extend_from_bitmap(&bitmap1)?;
        encoder.extend_from_bitmap(&bitmap2)?;
        encoder.finish()?;

        let vec = container.into_inner();
        assert_eq!(vec, vec![3, 0, 0, 0, 0b10011101u8, 0b01011101, 0b00000001]);
        Ok(())
    }

    #[test]
    fn from_iter() -> std::io::Result<()> {
        let mut container = std::io::Cursor::new(vec![]);

        let mut encoder = BitmapEncoder::try_new(&mut container)?;

        encoder.extend_from_trusted_len_iter(vec![true, true, true, true, true, true, true, true].into_iter())?;
        encoder.finish()?;

        let vec = container.into_inner();
        assert_eq!(vec, vec![1, 0, 0, 0, 0b11111111]);
        Ok(())
    }
}
