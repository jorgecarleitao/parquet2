use std::convert::TryInto;

use super::{Packed, Unpackable, Unpacked};

/// Encodes `u32` values into a buffer using `num_bits`.
pub fn encode<T: Unpackable>(unpacked: &[T], num_bits: usize, packed: &mut [u8]) {
    let chunks = unpacked.chunks_exact(T::Unpacked::LENGTH);

    let remainder = chunks.remainder();

    let packed_size = (T::Unpacked::LENGTH * num_bits + 7) / 8;
    if !remainder.is_empty() {
        let packed_chunks = packed.chunks_mut(packed_size);
        let mut last_chunk = T::Unpacked::zero();
        for i in 0..remainder.len() {
            last_chunk[i] = remainder[i]
        }

        chunks
            .chain(std::iter::once(last_chunk.as_ref()))
            .zip(packed_chunks)
            .for_each(|(unpacked, packed)| {
                T::pack(&unpacked.try_into().unwrap(), num_bits, packed);
            });
    } else {
        let packed_chunks = packed.chunks_exact_mut(packed_size);
        chunks.zip(packed_chunks).for_each(|(unpacked, packed)| {
            T::pack(&unpacked.try_into().unwrap(), num_bits, packed);
        });
    }
}

#[inline]
pub fn encode_pack<T: Unpackable>(unpacked: &[T], num_bits: usize, packed: &mut [u8]) {
    if unpacked.len() < T::Packed::LENGTH {
        let mut buf = T::Unpacked::zero();
        buf.as_mut()[..unpacked.len()].copy_from_slice(unpacked);
        T::pack(&buf, num_bits, packed)
    } else {
        T::pack(&unpacked.try_into().unwrap(), num_bits, packed)
    }
}
