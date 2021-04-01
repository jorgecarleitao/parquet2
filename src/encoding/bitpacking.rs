/// Usual bitpacking
use bitpacking::BitPacker;
use bitpacking::BitPacker1x;

pub fn required_capacity(length: u32) -> usize {
    let chunks_n = (length as usize + BitPacker1x::BLOCK_LEN - 1) / BitPacker1x::BLOCK_LEN;
    chunks_n * BitPacker1x::BLOCK_LEN
}

/// Decodes bitpacked bytes into `u32` values in `num_bits`.
pub fn decode(compressed: &[u8], num_bits: u8, decompressed: &mut [u32]) {
    let bitpacker = BitPacker1x::new();

    let chunks = compressed.chunks_exact(BitPacker1x::BLOCK_LEN);

    let remainder = chunks.remainder();

    let mut last_chunk = remainder.to_vec();
    last_chunk.extend(std::iter::repeat(0).take(BitPacker1x::BLOCK_LEN - remainder.len()));

    chunks
        .chain(std::iter::once(last_chunk.as_ref()))
        .enumerate()
        .for_each(|(i, chunk)| {
            bitpacker.decompress(
                &chunk,
                &mut decompressed[i * BitPacker1x::BLOCK_LEN..(i + 1) * BitPacker1x::BLOCK_LEN],
                num_bits,
            );
        });
}

fn encode(decompressed: &[u32], num_bits: u8, compressed: &mut [u8]) -> usize {
    let bitpacker = BitPacker1x::new();

    let chunks = decompressed.chunks_exact(BitPacker1x::BLOCK_LEN);

    let remainder = chunks.remainder();

    let mut last_chunk = remainder.to_vec();
    last_chunk.extend(std::iter::repeat(0).take(BitPacker1x::BLOCK_LEN - remainder.len()));

    let mut compressed_len = 0;
    chunks
        .chain(std::iter::once(last_chunk.as_ref()))
        .enumerate()
        .for_each(|(i, chunk)| {
            compressed_len += bitpacker.compress(
                &chunk,
                &mut compressed[i * BitPacker1x::BLOCK_LEN..(i + 1) * BitPacker1x::BLOCK_LEN],
                num_bits,
            );
        });
    compressed_len
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

    #[test]
    fn test_encode_rle() {
        let num_bits = 3;

        let data = vec![0, 1, 2, 3, 4, 5, 6, 7];

        let mut compressed = vec![0u8; 4 * BitPacker1x::BLOCK_LEN];

        let compressed_len = encode(data.as_ref(), num_bits, &mut compressed);
        compressed.truncate(compressed_len);

        let expected = vec![
            0b10001000u8,
            0b11000110,
            0b11111010,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
        ];

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
