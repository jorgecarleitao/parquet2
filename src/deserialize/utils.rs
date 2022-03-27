use crate::{
    encoding::hybrid_rle,
    error::Error,
    page::{split_buffer, DataPage},
};

use super::hybrid_rle::HybridBitmapIter;

pub(super) fn dict_indices_decoder(page: &DataPage) -> hybrid_rle::HybridRleDecoder {
    let (_, _, indices_buffer) = split_buffer(page);

    // SPEC: Data page format: the bit width used to encode the entry ids stored as 1 byte (max bit width = 32),
    // SPEC: followed by the values encoded using RLE/Bit packed described above (with the given bit width).
    let bit_width = indices_buffer[0];
    let indices_buffer = &indices_buffer[1..];

    hybrid_rle::HybridRleDecoder::new(indices_buffer, bit_width as u32, page.num_values())
}

/// Type definition for a [`HybridBitmapIter`]
pub type HybridDecoderBitmapIter<'a> = HybridBitmapIter<'a, hybrid_rle::Decoder<'a>>;

pub(super) fn try_new_iter(
    page: &DataPage,
) -> Result<HybridBitmapIter<'_, hybrid_rle::Decoder<'_>>, Error> {
    let (_, validity, _) = split_buffer(page);

    if page.descriptor.max_def_level != 1 {
        return Err(Error::General(
            "HybridBitmapIter can only be initialized from pages with a maximum definition of 1"
                .to_string(),
        ));
    }

    let iter = hybrid_rle::Decoder::new(validity, 1);
    let iter = HybridBitmapIter::new(iter, page.num_values());

    Ok(iter)
}
