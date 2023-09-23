use std::collections::VecDeque;

use crate::{
    error::Error,
    indexes::Interval,
    page::{split_buffer, DataPage},
    parquet_bridge::{Encoding, Repetition},
};

use super::values::{Decoder, ValuesDecoder};

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum BooleanValuesDecoder<'a> {
    Plain(Bitmap<'a>),
}

impl<'a> BooleanValuesDecoder<'a> {
    pub fn try_new(page: &'a DataPage) -> Result<Self, Error> {
        match page.encoding() {
            Encoding::Plain => Bitmap::try_new(page).map(Self::Plain),
            _ => Err(Error::InvalidParameter(format!(
                "Viewing page for encoding {:?} for boolean type not supported",
                page.encoding(),
            ))),
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::Plain(validity) => validity.length,
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<'a> ValuesDecoder for BooleanValuesDecoder<'a> {
    fn len(&self) -> usize {
        self.len()
    }
}

#[derive(Debug, Clone)]
pub struct Bitmap<'a> {
    pub values: &'a [u8],
    // invariant: offset <= length;
    pub offset: usize,
    // invariant: length <= values.len() * 8;
    pub length: usize,
}

impl<'a> Bitmap<'a> {
    pub fn slice(self, offset: usize, length: usize) -> Self {
        Self {
            values: self.values,
            offset,
            length,
        }
    }
}

/// An iterator adapter that converts an iterator over items into an iterator over slices of
/// those N items.
///
/// This iterator is best used with iterators that implement `nth` since skipping items
/// allows this iterator to skip sequences of items without having to call each of them.
#[derive(Debug, Clone)]
pub struct FilteredBitmap<'a> {
    bitmap: Bitmap<'a>,
    intervals: VecDeque<Interval>,
    total_length: usize,
}

impl<'a> FilteredBitmap<'a> {
    /// Return a new [`SliceFilteredIter`]
    pub fn new(bitmap: Bitmap<'a>, intervals: VecDeque<Interval>) -> Self {
        let total_length = intervals.iter().map(|i| i.length).sum();
        Self {
            bitmap,
            intervals,
            total_length,
        }
    }
}

impl<'a> Iterator for FilteredBitmap<'a> {
    type Item = Bitmap<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.intervals.pop_front().map(|interval| {
            let item = self.bitmap.clone().slice(interval.start, interval.length);
            self.total_length -= 1;
            item
        })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.total_length, Some(self.total_length))
    }
}

impl<'a> Bitmap<'a> {
    pub fn try_new(page: &'a DataPage) -> Result<Self, Error> {
        let (_, _, values) = split_buffer(page)?;

        let is_optional =
            page.descriptor.primitive_type.field_info.repetition == Repetition::Optional;
        let length = (!is_optional)
            .then(|| page.num_values())
            .unwrap_or(values.len() * 8);

        if length > values.len() * 8 {
            return Err(Error::oos(
                "The boolean page has less items than declared in num_values",
            ));
        }
        Ok(Self {
            values,
            offset: 0,
            length,
        })
    }
}

#[derive(Debug)]
pub enum BooleanFilteredValuesDecoder<'a> {
    Plain(FilteredBitmap<'a>),
}

impl<'a> From<(BooleanValuesDecoder<'a>, VecDeque<Interval>)> for BooleanFilteredValuesDecoder<'a> {
    fn from((decoder, intervals): (BooleanValuesDecoder<'a>, VecDeque<Interval>)) -> Self {
        match decoder {
            BooleanValuesDecoder::Plain(bitmap) => {
                Self::Plain(FilteredBitmap::new(bitmap, intervals))
            }
        }
    }
}

pub type BooleanDecoder<'a> =
    Decoder<'a, BooleanValuesDecoder<'a>, BooleanFilteredValuesDecoder<'a>>;
