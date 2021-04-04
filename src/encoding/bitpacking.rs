/// Usual bitpacking
use bitpacking::BitPacker;
use bitpacking::BitPacker1x;

pub fn required_capacity(length: u32) -> usize {
    let chunks_n = (length as usize + BitPacker1x::BLOCK_LEN - 1) / BitPacker1x::BLOCK_LEN;
    chunks_n * BitPacker1x::BLOCK_LEN
}

/// Decodes bitpacked bytes of `num_bits` into `u32`.
/// Returns the number of decoded values.
/// # Panics
/// Panics if `decompressed` has less than [`required_capacity`]
pub fn decode(compressed: &[u8], num_bits: u8, decompressed: &mut [u32]) -> usize {
    let bitpacker = BitPacker1x::new();

    let compressed_block_size = BitPacker1x::BLOCK_LEN * num_bits as usize / 8;

    let compressed_chunks = compressed.chunks_exact(compressed_block_size);
    let decompressed_chunks = decompressed.chunks_exact_mut(BitPacker1x::BLOCK_LEN);

    let remainder = compressed_chunks.remainder();
    let last_compressed_chunk = if remainder.is_empty() {
        vec![]
    } else {
        let mut last_compressed_chunk = compressed.to_vec();
        last_compressed_chunk
            .extend(std::iter::repeat(0).take(compressed_block_size - remainder.len()));
        last_compressed_chunk
    };
    let last_compressed_chunks = last_compressed_chunk.chunks_exact(compressed_block_size);
    debug_assert_eq!(last_compressed_chunks.remainder().len(), 0);

    compressed_chunks
        .chain(last_compressed_chunk.chunks_exact(compressed_block_size))
        .zip(decompressed_chunks)
        .for_each(|(c_chunk, d_chunk)| {
            bitpacker.decompress(&c_chunk, d_chunk, num_bits);
        });
    compressed.len() / num_bits as usize * 8
}

/// Decodes bitpacked bytes into `u32` values in `num_bits`.
/// Returns the number of decoded values.
pub fn encode(decompressed: &[u32], num_bits: u8, compressed: &mut [u8]) -> usize {
    let bitpacker = BitPacker1x::new();

    let chunks = decompressed.chunks_exact(BitPacker1x::BLOCK_LEN);

    let remainder = chunks.remainder();

    let mut last_chunk = remainder.to_vec();
    let trailing = BitPacker1x::BLOCK_LEN - remainder.len();
    last_chunk.extend(std::iter::repeat(0).take(trailing));

    let mut compressed_len = 0;
    chunks
        .chain(std::iter::once(last_chunk.as_ref()))
        .for_each(|chunk| {
            let chunk_compressed =
                &mut compressed[compressed_len..compressed_len + BitPacker1x::BLOCK_LEN];
            compressed_len += bitpacker.compress(&chunk, chunk_compressed, num_bits);
        });
    decompressed.len() * num_bits as usize / 8
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

        let mut decoded = vec![0; required_capacity(length)];

        decode(&data, num_bits, &mut decoded);
        decoded.truncate(length as usize);
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

        let mut decoded = vec![0; 2 * BitPacker1x::BLOCK_LEN];
        let decoded_len = decode(&data, num_bits, &mut decoded);
        decoded.truncate(decoded_len);
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

        let mut decoded = vec![0; required_capacity(length)];

        decode(&data, num_bits, &mut decoded);
        decoded.truncate(length as usize);
        assert_eq!(decoded, vec![0, 1, 0, 1, 0, 1, 0, 1]);
    }
}
