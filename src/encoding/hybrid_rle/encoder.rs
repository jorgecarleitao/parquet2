use crate::encoding::bitpacking;
use crate::encoding::{ceil8, uleb128};

use std::io::Write;

use super::bitpacked_encode;

/// RLE-hybrid encoding of `u32`. This currently only yields bitpacked values.
pub fn encode_u32<W: Write, I: Iterator<Item = u32>>(
    writer: &mut W,
    iterator: I,
    num_bits: u8,
) -> std::io::Result<()> {
    // the length of the iterator.
    let length = iterator.size_hint().1.unwrap();

    // write the length + indicator
    let mut header = ceil8(length as usize) as u64;
    header <<= 1;
    header |= 1; // it is bitpacked => first bit is set
    let mut container = [0; 10];
    let used = uleb128::encode(header, &mut container);
    writer.write_all(&container[..used])?;

    bitpacked_encode_u32(writer, iterator, num_bits)?;

    Ok(())
}

fn bitpacked_encode_u32<W: Write, I: Iterator<Item = u32>>(
    writer: &mut W,
    mut iterator: I,
    num_bits: u8,
) -> std::io::Result<()> {
    // the length of the iterator.
    let length = iterator.size_hint().1.unwrap();

    let chunks = length / bitpacking::BLOCK_LEN;
    let remainder = length - chunks * bitpacking::BLOCK_LEN;
    let mut buffer = [0u32; bitpacking::BLOCK_LEN];

    let compressed_chunk_size = ceil8(bitpacking::BLOCK_LEN * num_bits as usize);
    // this is the upper bound: we do not know `num_bits` at compile time and thus can't allocate (on the stack)
    // the exact length.
    let mut compressed_chunk = [0u8; 4 * bitpacking::BLOCK_LEN];

    for _ in 0..chunks {
        (0..bitpacking::BLOCK_LEN).for_each(|i| {
            // infalible by construction
            buffer[i] = iterator.next().unwrap()
        });
        bitpacking::encode_pack(buffer, num_bits, compressed_chunk.as_mut());
        writer.write_all(&compressed_chunk[..compressed_chunk_size])?;
    }

    if remainder != 0 {
        iterator.enumerate().for_each(|(i, x)| {
            buffer[i] = x;
        });
        let compressed_remainder_size = ceil8(remainder * num_bits as usize);
        bitpacking::encode_pack(buffer, num_bits, compressed_chunk.as_mut());
        writer.write_all(&compressed_chunk[..compressed_remainder_size])?;
    };
    Ok(())
}

/// the bitpacked part of the encoder.
pub fn encode_bool<W: Write, I: Iterator<Item = bool>>(
    writer: &mut W,
    iterator: I,
) -> std::io::Result<()> {
    // the length of the iterator.
    let length = iterator.size_hint().1.unwrap();

    // write the length + indicator
    let mut header = ceil8(length) as u64;
    header <<= 1;
    header |= 1; // it is bitpacked => first bit is set
    let mut container = [0; 10];
    let used = uleb128::encode(header, &mut container);

    writer.write_all(&container[..used])?;

    // encode the iterator
    bitpacked_encode(writer, iterator)
}

#[cfg(test)]
mod tests {
    use super::super::bitmap::BitmapIter;
    use super::*;

    #[test]
    fn bool_basics_1() -> std::io::Result<()> {
        let iter = BitmapIter::new(&[0b10011101u8, 0b10011101], 0, 14);

        let mut vec = vec![];

        encode_bool(&mut vec, iter)?;

        assert_eq!(vec, vec![(2 << 1 | 1), 0b10011101u8, 0b00011101]);

        Ok(())
    }

    #[test]
    fn bool_from_iter() -> std::io::Result<()> {
        let mut vec = vec![];

        encode_bool(
            &mut vec,
            vec![true, true, true, true, true, true, true, true].into_iter(),
        )?;

        assert_eq!(vec, vec![(1 << 1 | 1), 0b11111111]);
        Ok(())
    }

    #[test]
    fn test_encode_u32() -> std::io::Result<()> {
        let mut vec = vec![];

        encode_u32(&mut vec, vec![0, 1, 2, 1, 2, 1, 1, 0, 3].into_iter(), 2)?;

        assert_eq!(
            vec,
            vec![(2 << 1 | 1), 0b01_10_01_00, 0b00_01_01_10, 0b_00_00_00_11]
        );
        Ok(())
    }

    #[test]
    fn test_encode_u32_large() -> std::io::Result<()> {
        let mut vec = vec![];

        let values = (0..128).map(|x| x % 4);

        encode_u32(&mut vec, values, 2)?;

        let length = 128;
        let expected = 0b11_10_01_00u8;

        let mut expected = vec![expected; length / 4];
        expected.insert(0, ((length / 8) as u8) << 1 | 1);

        assert_eq!(vec, expected);
        Ok(())
    }

    #[test]
    fn test_u32_other() -> std::io::Result<()> {
        let values = vec![3, 3, 0, 3, 2, 3, 3, 3, 3, 1, 3, 3, 3, 0, 3].into_iter();

        let mut vec = vec![];
        encode_u32(&mut vec, values, 2)?;

        let expected = vec![5, 207, 254, 247, 51];
        assert_eq!(expected, vec);
        Ok(())
    }
}
