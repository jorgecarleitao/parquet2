// See https://github.com/apache/parquet-format/blob/master/Encodings.md#run-length-encoding--bit-packing-hybrid-rle--3
mod bitmap;
mod decoder;
mod encoder;
pub use bitmap::encode as bitpacked_encode;
pub use decoder::Decoder;
pub use encoder::encode;

#[derive(Debug, PartialEq, Eq)]
pub enum HybridEncoded<'a> {
    /// A bitpacked slice.
    Bitpacked {
        compressed: &'a [u8],
        num_bits: usize,
        run_length: usize,
    },
    /// A RLE-encoded slice.
    Rle { value: u32, run_length: usize },
}

impl<'a> IntoIterator for HybridEncoded<'a> {
    type Item = u32;
    type IntoIter = RunIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            HybridEncoded::Bitpacked {
                compressed,
                num_bits,
                run_length,
            } => RunIterator::Bitpacked(super::bitpacking::Decoder::new(
                compressed,
                num_bits as u8,
                run_length,
            )),
            HybridEncoded::Rle {
                value,
                run_length: len,
            } => RunIterator::Rle(std::iter::repeat(value).take(len)),
        }
    }
}

pub enum RunIterator<'a> {
    Bitpacked(super::bitpacking::Decoder<'a>),
    Rle(std::iter::Take<std::iter::Repeat<u32>>),
}

impl<'a> Iterator for RunIterator<'a> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            RunIterator::Bitpacked(delegate) => delegate.next(),
            RunIterator::Rle(delegate) => delegate.next(),
        }
    }
}
