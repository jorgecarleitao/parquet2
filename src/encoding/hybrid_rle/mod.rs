// See https://github.com/apache/parquet-format/blob/master/Encodings.md#run-length-encoding--bit-packing-hybrid-rle--3
mod bitmap;
mod decoder;
mod encoder;
pub use bitmap::{encode_bool as bitpacked_encode, BitmapIter};
pub use decoder::Decoder;
pub use encoder::{encode_bool, encode_u32};

#[derive(Debug, PartialEq, Eq)]
pub enum HybridEncoded<'a> {
    /// A bitpacked slice. The consumer must know its bit-width to unpack it.
    Bitpacked(&'a [u8]),
    /// A RLE-encoded slice. The first attribute corresponds to the slice (that can be interpreted)
    /// the second attribute corresponds to the number of repetitions.
    Rle(&'a [u8], usize),
}
