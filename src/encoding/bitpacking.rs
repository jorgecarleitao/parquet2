use std::convert::TryInto;
use std::mem;

/// Usual bitpacking
use bitpacking::BitPacker;
use bitpacking::BitPacker1x;

use super::ceil8;

pub const BLOCK_LEN: usize = bitpacking::BitPacker1x::BLOCK_LEN;

/// Encodes `u32` values into a buffer using `num_bits`.
pub fn encode(decompressed: &[u32], num_bits: u8, compressed: &mut [u8]) -> usize {
    let chunks = decompressed.chunks_exact(BitPacker1x::BLOCK_LEN);

    let remainder = chunks.remainder();

    let size = ceil8(BitPacker1x::BLOCK_LEN * num_bits as usize);
    if !remainder.is_empty() {
        let mut last_chunk = remainder.to_vec();
        let trailing = BitPacker1x::BLOCK_LEN - remainder.len();
        last_chunk.extend(std::iter::repeat(0).take(trailing));

        let mut compressed_len = 0;
        chunks
            .chain(std::iter::once(last_chunk.as_ref()))
            .for_each(|chunk| {
                let chunk_compressed = &mut compressed[compressed_len..compressed_len + size];
                compressed_len +=
                    encode_pack(chunk.try_into().unwrap(), num_bits, chunk_compressed);
            });
    } else {
        let mut compressed_len = 0;
        chunks.for_each(|chunk| {
            let chunk_compressed = &mut compressed[compressed_len..compressed_len + size];
            compressed_len += encode_pack(chunk.try_into().unwrap(), num_bits, chunk_compressed);
        });
    }

    decompressed.len() * num_bits as usize / 8
}

/// Encodes `u32` values into a buffer using `num_bits`.
#[inline]
pub fn encode_pack(decompressed: [u32; BLOCK_LEN], num_bits: u8, compressed: &mut [u8]) -> usize {
    BitPacker1x::new().compress(&decompressed, compressed, num_bits)
}

#[derive(Debug, Clone)]
pub struct Decoder<'a> {
    compressed_chunks: std::slice::Chunks<'a, u8>,
    num_bits: u8,
    remaining: usize,
    current_pack_index: usize, // invariant: <BitPacker1x::BLOCK_LEN
    current_pack: [u32; BitPacker1x::BLOCK_LEN],
}

#[inline]
fn decode_pack(compressed: &[u8], num_bits: u8, pack: &mut [u32; BitPacker1x::BLOCK_LEN]) {
    let compressed_block_size = BitPacker1x::BLOCK_LEN * num_bits as usize / 8;

    if compressed.len() < compressed_block_size {
        let mut buf = [0u8; BitPacker1x::BLOCK_LEN * mem::size_of::<u32>()];
        buf[..compressed.len()].copy_from_slice(compressed);
        BitPacker1x::new().decompress(&buf, pack, num_bits);
    } else {
        BitPacker1x::new().decompress(compressed, pack, num_bits);
    }
}

impl<'a> Decoder<'a> {
    pub fn new(compressed: &'a [u8], num_bits: u8, mut length: usize) -> Self {
        let compressed_block_size = BitPacker1x::BLOCK_LEN * num_bits as usize / 8;

        let mut compressed_chunks = compressed.chunks(compressed_block_size);
        let mut current_pack = [0; BitPacker1x::BLOCK_LEN];
        if let Some(chunk) = compressed_chunks.next() {
            decode_pack(chunk, num_bits, &mut current_pack);
        } else {
            length = 0
        };

        Self {
            remaining: length,
            compressed_chunks,
            num_bits,
            current_pack,
            current_pack_index: 0,
        }
    }
}

impl<'a> Iterator for Decoder<'a> {
    type Item = u32;

    #[inline] // -71% improvement in bench
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let result = self.current_pack[self.current_pack_index];
        self.current_pack_index += 1;
        if self.current_pack_index == BitPacker1x::BLOCK_LEN {
            if let Some(chunk) = self.compressed_chunks.next() {
                decode_pack(chunk, self.num_bits, &mut self.current_pack);
                self.current_pack_index = 0;
            }
        }
        self.remaining -= 1;
        Some(result)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_rle() {
        // Test data: 0-7 with bit width 3
        // 0: 000
        // 1: 001
        // 2: 010
        // 3: 011
        // 4: 100
        // 5: 101
        // 6: 110
        // 7: 111
        let num_bits = 3;
        let length = 8;
        // encoded: 0b10001000u8, 0b11000110, 0b11111010
        let data = vec![0b10001000u8, 0b11000110, 0b11111010];

        let decoded = Decoder::new(&data, num_bits, length).collect::<Vec<_>>();
        assert_eq!(decoded, vec![0, 1, 2, 3, 4, 5, 6, 7]);
    }

    fn case1() -> (u8, Vec<u32>, Vec<u8>) {
        let num_bits = 3;
        let compressed = vec![
            0b10001000u8,
            0b11000110,
            0b11111010,
            0b10001000u8,
            0b11000110,
            0b11111010,
            0b10001000u8,
            0b11000110,
            0b11111010,
            0b10001000u8,
            0b11000110,
            0b11111010,
            0b10001000u8,
            0b11000110,
            0b11111010,
        ];
        let decompressed = vec![
            0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4,
            5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7,
        ];
        (num_bits, decompressed, compressed)
    }

    #[test]
    fn decode_large() {
        let (num_bits, expected, data) = case1();

        let decoded = Decoder::new(&data, num_bits, expected.len()).collect::<Vec<_>>();
        assert_eq!(decoded, expected);
    }

    #[test]
    fn encode_large() {
        let (num_bits, data, expected) = case1();
        let mut compressed = vec![0u8; 4 * BitPacker1x::BLOCK_LEN];

        let compressed_len = encode(&data, num_bits, &mut compressed);
        compressed.truncate(compressed_len);
        assert_eq!(compressed, expected);
    }

    #[test]
    fn test_encode() {
        let num_bits = 3;
        let data = vec![0, 1, 2, 3, 4, 5, 6, 7];

        let mut compressed = vec![0u8; 4 * BitPacker1x::BLOCK_LEN];

        let compressed_len = encode(&data, num_bits, &mut compressed);
        compressed.truncate(compressed_len);

        let expected = vec![0b10001000u8, 0b11000110, 0b11111010];

        assert_eq!(compressed, expected);
    }

    #[test]
    fn test_decode_bool() {
        let num_bits = 1;
        let length = 8;
        let data = vec![0b10101010];

        let decoded = Decoder::new(&data, num_bits, length).collect::<Vec<_>>();
        assert_eq!(decoded, vec![0, 1, 0, 1, 0, 1, 0, 1]);
    }

    #[test]
    fn even_case() {
        // [0, 1, 2, 3, 4, 5, 6, 0]x99
        let data = &[0b10001000u8, 0b11000110, 0b00011010];
        let num_bits = 3;
        let copies = 99; // 8 * 99 % 32 != 0
        let expected = std::iter::repeat(&[0u32, 1, 2, 3, 4, 5, 6, 0])
            .take(copies)
            .flatten()
            .copied()
            .collect::<Vec<_>>();
        let data = std::iter::repeat(data)
            .take(copies)
            .flatten()
            .copied()
            .collect::<Vec<_>>();
        let length = expected.len();

        let decoded = Decoder::new(&data, num_bits, length).collect::<Vec<_>>();
        assert_eq!(decoded, expected);
    }

    #[test]
    fn odd_case() {
        // [0, 1, 2, 3, 4, 5, 6, 0]x4 + [2]
        let data = &[0b10001000u8, 0b11000110, 0b00011010];
        let num_bits = 3;
        let copies = 4;
        let expected = std::iter::repeat(&[0u32, 1, 2, 3, 4, 5, 6, 0])
            .take(copies)
            .flatten()
            .copied()
            .chain(std::iter::once(2))
            .collect::<Vec<_>>();
        let data = std::iter::repeat(data)
            .take(copies)
            .flatten()
            .copied()
            .chain(std::iter::once(0b00000010u8))
            .collect::<Vec<_>>();
        let length = expected.len();

        let decoded = Decoder::new(&data, num_bits, length).collect::<Vec<_>>();
        assert_eq!(decoded, expected);
    }
}
