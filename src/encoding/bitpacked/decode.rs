use super::{Packed, Unpackable, Unpacked};

#[derive(Debug, Clone)]
pub struct Decoder<'a, T: Unpackable> {
    compressed_chunks: std::slice::Chunks<'a, u8>,
    num_bits: usize,
    remaining: usize,
    current_pack_index: usize, // invariant: < T::PACK_LENGTH
    current_pack: T::Unpacked,
}

#[inline]
fn decode_pack<T: Unpackable>(compressed: &[u8], num_bits: usize, pack: &mut T::Unpacked) {
    let compressed_block_size = T::Unpacked::LENGTH * num_bits / 8;

    if compressed.len() < compressed_block_size {
        let mut buf = T::Packed::zero();
        buf.as_mut()[..compressed.len()].copy_from_slice(compressed);
        T::unpack(buf.as_ref(), num_bits, pack)
    } else {
        T::unpack(compressed, num_bits, pack)
    }
}

impl<'a, T: Unpackable> Decoder<'a, T> {
    pub fn new(compressed: &'a [u8], num_bits: usize, mut length: usize) -> Self {
        let compressed_block_size = 32 * num_bits / 8;

        let mut compressed_chunks = compressed.chunks(compressed_block_size);
        let mut current_pack = T::Unpacked::zero();
        if let Some(chunk) = compressed_chunks.next() {
            decode_pack::<T>(chunk, num_bits, &mut current_pack);
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

impl<'a, T: Unpackable> Iterator for Decoder<'a, T> {
    type Item = T;

    #[inline] // -71% improvement in bench
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let result = self.current_pack[self.current_pack_index];
        self.current_pack_index += 1;
        if self.current_pack_index == T::Unpacked::LENGTH {
            if let Some(chunk) = self.compressed_chunks.next() {
                decode_pack::<T>(chunk, self.num_bits, &mut self.current_pack);
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
