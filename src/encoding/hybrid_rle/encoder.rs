use crate::encoding::{ceil8, uleb128};

use std::io::Write;

use super::bitmap::set;

/// the bitpacked part of the encoder.
pub fn encode<W: Write, I: Iterator<Item = bool>>(
    writer: &mut W,
    mut iterator: I,
) -> std::io::Result<()> {
    // the length of the iterator.
    let length = iterator.size_hint().1.unwrap();

    let chunks = length / 8;
    let reminder = length % 8;

    // write the length + indicator
    let mut header = ceil8(length) as u64;
    header <<= 1;
    header |= 1; // it is bitpacked => first bit is set
    let mut container = [0; 10];
    let used = uleb128::encode(header, &mut container);

    writer.write_all(&container[..used])?;

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
        writer.write_all(&[last])
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::bitmap::BitmapIter;
    use super::*;

    #[test]
    fn basics_1() -> std::io::Result<()> {
        let iter = BitmapIter::new(&[0b10011101u8, 0b10011101], 0, 14);

        let mut container = std::io::Cursor::new(vec![]);

        encode(&mut container, iter)?;

        let vec = container.into_inner();
        assert_eq!(vec, vec![(2 << 1 | 1), 0b10011101u8, 0b00011101]);

        Ok(())
    }

    #[test]
    fn from_iter() -> std::io::Result<()> {
        let mut container = std::io::Cursor::new(vec![]);

        encode(
            &mut container,
            vec![true, true, true, true, true, true, true, true].into_iter(),
        )?;

        let vec = container.into_inner();
        assert_eq!(vec, vec![(1 << 1 | 1), 0b11111111]);
        Ok(())
    }
}
