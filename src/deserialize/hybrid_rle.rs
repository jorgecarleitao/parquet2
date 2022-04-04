use crate::encoding::hybrid_rle::{self, BitmapIter};

/// The decoding state of the hybrid-RLE decoder with a maximum definition level of 1
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HybridEncoded<'a> {
    /// a bitmap
    Bitmap(&'a [u8], usize, usize),
    /// A repeated item. The first attribute corresponds to whether the value is set
    /// the second attribute corresponds to the number of repetitions.
    Repeated(bool, usize),
}

pub trait HybridRleRunsIterator<'a>: Iterator<Item = HybridEncoded<'a>> {
    /// Number of elements remaining. This may not be the items of the iterator - an item
    /// of the iterator may contain more than one element.
    fn number_of_elements(&self) -> usize;
}

/// An iterator of [`HybridEncoded`], adapter over [`hybrid_rle::HybridEncoded`].
#[derive(Debug, Clone)]
pub struct HybridRleIter<'a, I: Iterator<Item = hybrid_rle::HybridEncoded<'a>>> {
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

impl<'a, I: Iterator<Item = hybrid_rle::HybridEncoded<'a>>> HybridRleIter<'a, I> {
    /// Returns a new [`HybridRleIter`]
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

    /// the number of elements in the iterator. Note that this _is not_ the number of runs.
    #[inline]
    pub fn len(&self) -> usize {
        self.length - self.consumed
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// fetches the next bitmap, optionally limited.
    /// When limited, a run may return an offsetted bitmap
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

impl<'a, I: Iterator<Item = hybrid_rle::HybridEncoded<'a>>> HybridRleRunsIterator<'a>
    for HybridRleIter<'a, I>
{
    fn number_of_elements(&self) -> usize {
        self.len()
    }
}

impl<'a, I: Iterator<Item = hybrid_rle::HybridEncoded<'a>>> Iterator for HybridRleIter<'a, I> {
    type Item = HybridEncoded<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.limited_next(None)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

/// Type definition for a [`HybridRleIter`] using [`hybrid_rle::Decoder`].
pub type HybridDecoderBitmapIter<'a> = HybridRleIter<'a, hybrid_rle::Decoder<'a>>;

#[derive(Debug)]
enum HybridBooleanState<'a> {
    /// a bitmap
    Bitmap(BitmapIter<'a>),
    /// A repeated item. The first attribute corresponds to whether the value is set
    /// the second attribute corresponds to the number of repetitions.
    Repeated(bool, usize),
}

/// An iterator adapter that maps an iterator of [`HybridEncoded`] into an iterator
/// over [`bool`].
#[derive(Debug)]
pub struct HybridRleBooleanIter<'a, I: Iterator<Item = HybridEncoded<'a>>> {
    iter: I,
    current_run: Option<HybridBooleanState<'a>>,
}

impl<'a, I: Iterator<Item = HybridEncoded<'a>>> HybridRleBooleanIter<'a, I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            current_run: None,
        }
    }
}

impl<'a, I: HybridRleRunsIterator<'a>> Iterator for HybridRleBooleanIter<'a, I> {
    type Item = bool;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(run) = &mut self.current_run {
            match run {
                HybridBooleanState::Bitmap(bitmap) => bitmap.next(),
                HybridBooleanState::Repeated(value, remaining) => {
                    if *remaining == 0 {
                        None
                    } else {
                        *remaining -= 1;
                        Some(*value)
                    }
                }
            }
        } else if let Some(run) = self.iter.next() {
            self.current_run = Some(match run {
                HybridEncoded::Bitmap(bitmap, offset, length) => {
                    HybridBooleanState::Bitmap(BitmapIter::new(bitmap, offset, length))
                }
                HybridEncoded::Repeated(value, length) => {
                    HybridBooleanState::Repeated(value, length)
                }
            });
            self.next()
        } else {
            None
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let exact = self.iter.number_of_elements();
        (exact, Some(exact))
    }
}

/// Type definition for a [`HybridRleBooleanIter`] using [`hybrid_rle::Decoder`].
pub type HybridRleDecoderIter<'a> = HybridRleBooleanIter<'a, HybridDecoderBitmapIter<'a>>;
