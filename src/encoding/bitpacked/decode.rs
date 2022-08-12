use super::unpack;

pub trait Packed: Copy + Sized + AsRef<[u8]> + AsMut<[u8]> {
    fn zero() -> Self;
}

impl Packed for [u8; 8] {
    #[inline]
    fn zero() -> Self {
        [0; 8]
    }
}

impl Packed for [u8; 16 * 2] {
    #[inline]
    fn zero() -> Self {
        [0; 16 * 2]
    }
}

impl Packed for [u8; 32 * 4] {
    #[inline]
    fn zero() -> Self {
        [0; 32 * 4]
    }
}

impl Packed for [u8; 64 * 64] {
    #[inline]
    fn zero() -> Self {
        [0; 64 * 64]
    }
}

pub trait Unpacked<T>: Copy + Sized + AsMut<[T]> + std::ops::Index<usize, Output = T> {
    const LENGTH: usize;
    fn zero() -> Self;
}

impl Unpacked<u8> for [u8; 8] {
    const LENGTH: usize = 8;
    #[inline]
    fn zero() -> Self {
        [0; 8]
    }
}

impl Unpacked<u16> for [u16; 16] {
    const LENGTH: usize = 16;
    #[inline]
    fn zero() -> Self {
        [0; 16]
    }
}

impl Unpacked<u32> for [u32; 32] {
    const LENGTH: usize = 32;
    #[inline]
    fn zero() -> Self {
        [0; 32]
    }
}

impl Unpacked<u64> for [u64; 64] {
    const LENGTH: usize = 64;
    #[inline]
    fn zero() -> Self {
        [0; 64]
    }
}

pub trait Unpackable: Copy + Sized + Default {
    type Packed: Packed;
    type Unpacked: Unpacked<Self>;
    fn unpack(packed: &[u8], num_bits: usize, unpacked: &mut Self::Unpacked);
}

impl Unpackable for u8 {
    type Packed = [u8; 8];
    type Unpacked = [u8; 8];

    #[inline]
    fn unpack(packed: &[u8], num_bits: usize, unpacked: &mut Self::Unpacked) {
        unpack::unpack8(packed, unpacked, num_bits)
    }
}

impl Unpackable for u16 {
    type Packed = [u8; 16 * 2];
    type Unpacked = [u16; 16];

    #[inline]
    fn unpack(packed: &[u8], num_bits: usize, unpacked: &mut Self::Unpacked) {
        unpack::unpack16(packed, unpacked, num_bits)
    }
}

impl Unpackable for u32 {
    type Packed = [u8; 32 * 4];
    type Unpacked = [u32; 32];

    #[inline]
    fn unpack(packed: &[u8], num_bits: usize, unpacked: &mut Self::Unpacked) {
        unpack::unpack32(packed, unpacked, num_bits)
    }
}

impl Unpackable for u64 {
    type Packed = [u8; 64 * 64];
    type Unpacked = [u64; 64];

    #[inline]
    fn unpack(packed: &[u8], num_bits: usize, unpacked: &mut Self::Unpacked) {
        unpack::unpack64(packed, unpacked, num_bits)
    }
}

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
