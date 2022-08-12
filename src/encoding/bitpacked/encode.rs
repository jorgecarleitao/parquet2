use std::convert::TryInto;

/// Usual bitpacking
use bitpacking::BitPacker;
use bitpacking::BitPacker1x;

use super::super::ceil8;

pub const BLOCK_LEN: usize = bitpacking::BitPacker1x::BLOCK_LEN;

/// Encodes `u32` values into a buffer using `num_bits`.
pub fn encode(decompressed: &[u32], num_bits: usize, compressed: &mut [u8]) -> usize {
    let chunks = decompressed.chunks_exact(BitPacker1x::BLOCK_LEN);

    let remainder = chunks.remainder();

    let size = ceil8(BitPacker1x::BLOCK_LEN * num_bits);
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
                    encode_pack(chunk.try_into().unwrap(), num_bits as u8, chunk_compressed);
            });
    } else {
        let mut compressed_len = 0;
        chunks.for_each(|chunk| {
            let chunk_compressed = &mut compressed[compressed_len..compressed_len + size];
            compressed_len +=
                encode_pack(chunk.try_into().unwrap(), num_bits as u8, chunk_compressed);
        });
    }

    decompressed.len() * num_bits / 8
}

/// Encodes `u32` values into a buffer using `num_bits`.
#[inline]
pub fn encode_pack(decompressed: [u32; BLOCK_LEN], num_bits: u8, compressed: &mut [u8]) -> usize {
    BitPacker1x::new().compress(&decompressed, compressed, num_bits)
}
