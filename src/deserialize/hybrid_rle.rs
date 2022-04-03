use crate::encoding::hybrid_rle;

#[derive(Debug, PartialEq, Eq)]
pub enum HybridEncoded<'a> {
    /// a bitmap
    Bitmap(&'a [u8], usize, usize),
    /// A repeated item. The first attribute corresponds to whether the value is set
    /// the second attribute corresponds to the number of repetitions.
    Repeated(bool, usize),
}

/// An iterator of [`HybridEncoded`], adapter over [`hybrid_rle::HybridEncoded`],
/// specialized to be consumed into bitmaps.
#[derive(Debug, Clone)]
pub struct HybridBitmapIter<'a, I: Iterator<Item = hybrid_rle::HybridEncoded<'a>>> {
    iter: I,
    current: Option<hybrid_rle::HybridEncoded<'a>>,
    // invariants:
    // * run_offset < length
    // * consumed < length
    // how many items have been taken on the current encoded run.
    // 0 implies we need to advance the decoder
    run_offset: usize,
    // how many items have been consumed from the encoder
    consumed: usize,
    // how many items the page has
    length: usize,
}

impl<'a, I: Iterator<Item = hybrid_rle::HybridEncoded<'a>>> HybridBitmapIter<'a, I> {
    /// Returns a new [`HybridBitmapIter`]
    #[inline]
    pub fn new(mut iter: I, length: usize) -> Self {
        let current = iter.next();
        Self {
            iter,
            current,
            run_offset: 0,
            consumed: 0,
            length,
        }
    }

    /// the number of elements in the iterator
    #[inline]
    pub fn len(&self) -> usize {
        self.length - self.consumed
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// fetches the next bitmap, optionally limited.
    /// When limited, a run of the hybrid may return an offsetted bitmap
    pub fn limited_next(&mut self, limit: Option<usize>) -> Option<HybridEncoded<'a>> {
        if self.consumed == self.length {
            return None;
        };
        let run = if let Some(run) = &self.current {
            run
        } else {
            // case where the decoder has no more items, likely because the parquet file is wrong
            return None;
        };
        let result = match run {
            hybrid_rle::HybridEncoded::Bitpacked(pack) => {
                // a pack has at most `pack.len() * 8` bits
                // during execution, we may end in the middle of a pack (run_offset != 0)
                // the remaining items in the pack is given by a combination
                // of the page length, the offset in the pack, and where we are in the page
                let pack_size = pack.len() * 8 - self.run_offset;
                let remaining = self.len();
                let length = pack_size.min(remaining);

                let additional = if let Some(limit) = limit {
                    length.min(limit)
                } else {
                    length
                };

                let result = HybridEncoded::Bitmap(pack, self.run_offset, additional);

                if additional == length {
                    self.run_offset = 0;
                    self.current = self.iter.next();
                } else {
                    self.run_offset += additional;
                };
                self.consumed += additional;
                result
            }
            hybrid_rle::HybridEncoded::Rle(value, length) => {
                let is_set = value[0] == 1;
                let length = length - self.run_offset;

                // the number of elements that will be consumed in this (run, iteration)
                let additional = if let Some(limit) = limit {
                    length.min(limit)
                } else {
                    length
                };

                let result = HybridEncoded::Repeated(is_set, additional);

                if additional == length {
                    self.run_offset = 0;
                    self.current = self.iter.next();
                } else {
                    self.run_offset += additional;
                };
                self.consumed += additional;
                result
            }
        };
        Some(result)
    }
}

impl<'a, I: Iterator<Item = hybrid_rle::HybridEncoded<'a>>> Iterator for HybridBitmapIter<'a, I> {
    type Item = HybridEncoded<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.limited_next(None)
    }
}
