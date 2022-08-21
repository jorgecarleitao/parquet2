use std::collections::VecDeque;

use crate::{
    encoding::{
        delta_length_byte_array,
        hybrid_rle::{self, HybridRleDecoder},
        plain_byte_array::BinaryIter,
    },
    error::Error,
    indexes::Interval,
    page::{split_buffer, DataPage},
    parquet_bridge::{Encoding, Repetition},
    read::levels::get_bit_width,
};

use super::SliceFilteredIter;
use super::{
    utils::{
        dict_indices_decoder, get_selected_rows, FilteredOptionalPageValidity, OptionalPageValidity,
    },
    FilteredHybridRleDecoderIter,
};

#[derive(Debug)]
pub struct Dictionary<'a, P> {
    pub indexes: hybrid_rle::HybridRleDecoder<'a>,
    pub dict: &'a P,
}

impl<'a, P> Dictionary<'a, P> {
    pub fn try_new(page: &'a DataPage, dict: &'a P) -> Result<Self, Error> {
        let indexes = dict_indices_decoder(page)?;

        Ok(Self { indexes, dict })
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.indexes.size_hint().0
    }
}

#[derive(Debug)]
pub struct Delta<'a> {
    pub lengths: std::vec::IntoIter<usize>,
    pub values: &'a [u8],
}

impl<'a> Delta<'a> {
    pub fn try_new(page: &'a DataPage) -> Result<Self, Error> {
        let (_, _, values) = split_buffer(page)?;

        let mut lengths_iter = delta_length_byte_array::Decoder::try_new(values)?;

        #[allow(clippy::needless_collect)] // we need to consume it to get the values
        let lengths = lengths_iter
            .by_ref()
            .map(|x| x.map(|x| x as usize))
            .collect::<Result<Vec<_>, _>>()?;

        let values = lengths_iter.into_values();
        Ok(Self {
            lengths: lengths.into_iter(),
            values,
        })
    }

    pub fn len(&self) -> usize {
        self.lengths.size_hint().0
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<'a> Iterator for Delta<'a> {
    type Item = &'a [u8];

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let length = self.lengths.next()?;
        let (item, remaining) = self.values.split_at(length);
        self.values = remaining;
        Some(item)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.lengths.size_hint()
    }
}

#[derive(Debug)]
pub struct FilteredRequired<'a> {
    pub values: SliceFilteredIter<BinaryIter<'a>>,
}

impl<'a> FilteredRequired<'a> {
    pub fn new(page: &'a DataPage) -> Self {
        let values = BinaryIter::new(page.buffer(), Some(page.num_values()));

        let rows = get_selected_rows(page);
        let values = SliceFilteredIter::new(values, rows);

        Self { values }
    }

    /// Returns the length of this [`FilteredRequired`].
    pub fn len(&self) -> usize {
        self.values.size_hint().0
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Debug)]
pub struct FilteredDelta<'a> {
    pub values: SliceFilteredIter<Delta<'a>>,
}

impl<'a> FilteredDelta<'a> {
    pub fn try_new(page: &'a DataPage) -> Result<Self, Error> {
        let values = Delta::try_new(page)?;

        let rows = get_selected_rows(page);
        let values = SliceFilteredIter::new(values, rows);

        Ok(Self { values })
    }

    /// Returns the length of this [`FilteredDelta`].
    pub fn len(&self) -> usize {
        self.values.size_hint().0
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Debug)]
pub struct FilteredDictionary<'a, P> {
    pub indexes: SliceFilteredIter<hybrid_rle::HybridRleDecoder<'a>>,
    pub dict: &'a P,
}

impl<'a, P> FilteredDictionary<'a, P> {
    pub fn try_new(page: &'a DataPage, dict: &'a P) -> Result<Self, Error> {
        let indexes = dict_indices_decoder(page)?;

        let rows = get_selected_rows(page);
        let indexes = SliceFilteredIter::new(indexes, rows);

        Ok(Self { indexes, dict })
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.indexes.size_hint().0
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub enum BinaryPageValues<'a, P> {
    Plain(BinaryIter<'a>),
    Dictionary(Dictionary<'a, P>),
    Delta(Delta<'a>),
}

impl<'a, P> BinaryPageValues<'a, P> {
    pub fn try_new(page: &'a DataPage, dict: Option<&'a P>) -> Result<Self, Error> {
        let is_optional =
            page.descriptor.primitive_type.field_info.repetition == Repetition::Optional;
        let length = (!is_optional).then(|| page.num_values());

        match (page.encoding(), dict) {
            (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict)) => {
                Dictionary::try_new(page, dict).map(Self::Dictionary)
            }
            (Encoding::Plain, _) => {
                let (_, _, values) = split_buffer(page)?;
                Ok(Self::Plain(BinaryIter::new(values, length)))
            }
            (Encoding::DeltaLengthByteArray, _) => Delta::try_new(page).map(Self::Delta),
            (other, _) => Err(Error::OutOfSpec(format!(
                "Binary-encoded non-nested pages cannot be encoded as {other:?}"
            ))),
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::Plain(validity) => validity.size_hint().0,
            Self::Dictionary(state) => state.len(),
            Self::Delta(state) => state.len(),
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub enum BinaryPage<'a, P> {
    Optional(OptionalPageValidity<'a>, BinaryPageValues<'a, P>),
    Required(BinaryPageValues<'a, P>),
    Levels(HybridRleDecoder<'a>, u32, BinaryPageValues<'a, P>),
}

impl<'a, P> BinaryPage<'a, P> {
    pub fn try_new(page: &'a DataPage, dict: Option<&'a P>) -> Result<Self, Error> {
        let values = BinaryPageValues::try_new(page, dict)?;

        if page.descriptor.max_def_level > 1 {
            let (_, def_levels, _) = split_buffer(page)?;
            let max = page.descriptor.max_def_level as u32;
            let validity = HybridRleDecoder::try_new(
                def_levels,
                get_bit_width(max as i16),
                page.num_values(),
            )?;
            return Ok(Self::Levels(
                validity,
                page.descriptor.max_def_level as u32,
                values,
            ));
        }

        let is_optional =
            page.descriptor.primitive_type.field_info.repetition == Repetition::Optional;
        Ok(if is_optional {
            Self::Optional(OptionalPageValidity::try_new(page)?, values)
        } else {
            Self::Required(values)
        })
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Optional(validity, _) => validity.len(),
            Self::Required(state) => state.len(),
            Self::Levels(state, _, _) => state.len(),
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub enum FilteredBinaryPageValues<'a, P> {
    Plain(SliceFilteredIter<BinaryIter<'a>>),
    Dictionary(FilteredDictionary<'a, P>),
    Delta(SliceFilteredIter<Delta<'a>>),
}

impl<'a, P> FilteredBinaryPageValues<'a, P> {
    pub fn new(page: BinaryPageValues<'a, P>, intervals: VecDeque<Interval>) -> Self {
        match page {
            BinaryPageValues::Plain(values) => {
                Self::Plain(SliceFilteredIter::new(values, intervals))
            }
            BinaryPageValues::Dictionary(values) => Self::Dictionary(FilteredDictionary {
                indexes: SliceFilteredIter::new(values.indexes, intervals),
                dict: values.dict,
            }),
            BinaryPageValues::Delta(values) => {
                Self::Delta(SliceFilteredIter::new(values, intervals))
            }
        }
    }
}

pub enum FilteredBinaryPage<'a, P> {
    Optional(FilteredOptionalPageValidity<'a>, BinaryPageValues<'a, P>),
    Required(FilteredBinaryPageValues<'a, P>),
    // todo: levels
}

impl<'a, P> FilteredBinaryPage<'a, P> {
    pub fn try_new(page: BinaryPage<'a, P>, intervals: VecDeque<Interval>) -> Result<Self, Error> {
        Ok(match page {
            BinaryPage::Optional(iter, values) => Self::Optional(
                FilteredOptionalPageValidity::new(FilteredHybridRleDecoderIter::new(
                    iter.iter, intervals,
                )),
                values,
            ),
            BinaryPage::Required(values) => {
                Self::Required(FilteredBinaryPageValues::new(values, intervals))
            }
            BinaryPage::Levels(_, _, _) => {
                return Err(Error::FeatureNotSupported("Filtered levels".to_string()))
            }
        })
    }
}
