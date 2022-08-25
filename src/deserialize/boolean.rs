use std::collections::VecDeque;

use crate::{
    encoding::hybrid_rle::HybridRleDecoder,
    error::Error,
    indexes::Interval,
    page::{split_buffer, DataPage},
    parquet_bridge::{Encoding, Repetition},
    read::levels::get_bit_width,
};

use super::{
    utils::FilteredOptionalPageValidity, FilteredHybridRleDecoderIter, OptionalPageValidity,
};

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum BooleanPageValues<'a> {
    Plain(Bitmap<'a>),
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

impl<'a> BooleanPageValues<'a> {
    pub fn try_new(page: &'a DataPage) -> Result<Self, Error> {
        match page.encoding() {
            Encoding::Plain => Bitmap::try_new(page).map(Self::Plain),
            _ => Err(Error::InvalidParameter(format!(
                "Viewing page for encoding {:?} for boolean type not supported",
                page.encoding(),
            ))),
        }
    }
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum NominalBooleanPage<'a> {
    Optional(OptionalPageValidity<'a>, BooleanPageValues<'a>),
    Required(BooleanPageValues<'a>),
    Levels(HybridRleDecoder<'a>, u32, BooleanPageValues<'a>),
}

impl<'a> NominalBooleanPage<'a> {
    pub fn try_new(page: &'a DataPage) -> Result<Self, Error> {
        let values = BooleanPageValues::try_new(page)?;

        if page.descriptor.max_def_level > 1 {
            let (_, def_levels, _) = split_buffer(page)?;
            let max = page.descriptor.max_def_level as u32;
            let validity = HybridRleDecoder::try_new(
                def_levels,
                get_bit_width(max as i16),
                page.num_values(),
            )?;
            return Ok(Self::Levels(validity, max, values));
        }

        let is_optional =
            page.descriptor.primitive_type.field_info.repetition == Repetition::Optional;

        match (page.encoding(), is_optional) {
            (Encoding::Plain, true) => Ok(Self::Optional(
                OptionalPageValidity::try_new(page)?,
                BooleanPageValues::try_new(page)?,
            )),
            (Encoding::Plain, false) => BooleanPageValues::try_new(page).map(Self::Required),
            (other, _) => Err(Error::OutOfSpec(format!(
                "boolean-encoded non-nested pages cannot be encoded as {other:?}"
            ))),
        }
    }
}

#[derive(Debug)]
pub enum FilteredBooleanPageValues<'a> {
    Plain(FilteredBitmap<'a>),
}

impl<'a> FilteredBooleanPageValues<'a> {
    pub fn new(page: BooleanPageValues<'a>, intervals: VecDeque<Interval>) -> Self {
        match page {
            BooleanPageValues::Plain(bitmap) => Self::Plain(FilteredBitmap::new(bitmap, intervals)),
        }
    }
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum FilteredBooleanPage<'a> {
    Optional(FilteredOptionalPageValidity<'a>, BooleanPageValues<'a>),
    Required(FilteredBooleanPageValues<'a>),
}

impl<'a> FilteredBooleanPage<'a> {
    pub fn try_new(
        page: NominalBooleanPage<'a>,
        intervals: VecDeque<Interval>,
    ) -> Result<Self, Error> {
        Ok(match page {
            NominalBooleanPage::Optional(iter, values) => Self::Optional(
                FilteredOptionalPageValidity::new(FilteredHybridRleDecoderIter::new(
                    iter.iter, intervals,
                )),
                values,
            ),
            NominalBooleanPage::Required(values) => {
                Self::Required(FilteredBooleanPageValues::new(values, intervals))
            }
            NominalBooleanPage::Levels(_, _, _) => {
                return Err(Error::FeatureNotSupported("Filtered levels".to_string()))
            }
        })
    }
}

/// The deserialization state of a [`DataPage`] of a parquet binary type
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum BooleanPage<'a> {
    Nominal(NominalBooleanPage<'a>),
    Filtered(FilteredBooleanPage<'a>),
}

impl<'a> BooleanPage<'a> {
    /// Tries to create [`BooleanPage`]
    /// # Error
    /// Errors iff the page is not a `BooleanPage`
    pub fn try_new(page: &'a DataPage) -> Result<Self, Error> {
        let native_page = NominalBooleanPage::try_new(page)?;

        if let Some(selected_rows) = page.selected_rows() {
            FilteredBooleanPage::try_new(native_page, selected_rows.iter().copied().collect())
                .map(Self::Filtered)
        } else {
            Ok(Self::Nominal(native_page))
        }
    }
}
