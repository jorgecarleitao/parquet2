use crate::{
    encoding::hybrid_rle::{self, HybridRleDecoder},
    page::{split_buffer, DataPage},
    read::levels::get_bit_width,
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

/// Decoder of definition levels.
#[derive(Debug)]
pub enum DefLevelsDecoder<'a> {
    /// When the maximum definition level is 1, the definition levels are RLE-encoded and
    /// the bitpacked runs are bitmaps. This variant contains [`HybridDecoderBitmapIter`]
    /// that decodes the runs, but not the individual values
    Bitmap(HybridDecoderBitmapIter<'a>),
    /// When the maximum definition level is 1,
    Levels(HybridRleDecoder<'a>, u32),
}

impl<'a> DefLevelsDecoder<'a> {
    pub fn new(page: &'a DataPage) -> Self {
        let (_, def_levels, _) = split_buffer(page);

        let max_def_level = page.descriptor.max_def_level;
        if max_def_level == 1 {
            let iter = hybrid_rle::Decoder::new(def_levels, 1);
            let iter = HybridBitmapIter::new(iter, page.num_values());
            Self::Bitmap(iter)
        } else {
            let iter =
                HybridRleDecoder::new(def_levels, get_bit_width(max_def_level), page.num_values());
            Self::Levels(iter, max_def_level as u32)
        }
    }
}
