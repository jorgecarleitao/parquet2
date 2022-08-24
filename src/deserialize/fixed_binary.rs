use std::collections::VecDeque;

use crate::{
    encoding::hybrid_rle::{self, HybridRleDecoder},
    error::Error,
    indexes::Interval,
    page::{split_buffer, DataPage},
    parquet_bridge::{Encoding, Repetition},
    read::levels::get_bit_width,
    schema::types::PhysicalType,
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

#[derive(Debug)]
pub enum FixedBinaryPageValues<'a, P> {
    Plain(std::slice::ChunksExact<'a, u8>),
    Dictionary(Dictionary<'a, P>),
}

impl<'a, P> FixedBinaryPageValues<'a, P> {
    pub fn try_new(page: &'a DataPage, dict: Option<&'a P>) -> Result<Self, Error> {
        match (page.encoding(), dict) {
            (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict)) => {
                Dictionary::try_new(page, dict).map(Self::Dictionary)
            }
            (Encoding::Plain, _) => {
                let size: usize = if let PhysicalType::FixedLenByteArray(size) =
                    page.descriptor.primitive_type.physical_type
                {
                    size
                } else {
                    return Err(Error::InvalidParameter(
                        "FixedLenFixedBinaryPage must be initialized by pages of FixedLenByteArray"
                            .to_string(),
                    ));
                };
                let (_, _, values) = split_buffer(page)?;
                Ok(Self::Plain(values.chunks_exact(size)))
            }
            (other, _) => Err(Error::OutOfSpec(format!(
                "FixedBinary-encoded non-nested pages cannot be encoded as {other:?}"
            ))),
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::Plain(validity) => validity.size_hint().0,
            Self::Dictionary(state) => state.len(),
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Debug)]
pub enum NominalFixedBinaryPage<'a, P> {
    Optional(OptionalPageValidity<'a>, FixedBinaryPageValues<'a, P>),
    Required(FixedBinaryPageValues<'a, P>),
    Levels(HybridRleDecoder<'a>, u32, FixedBinaryPageValues<'a, P>),
}

impl<'a, P> NominalFixedBinaryPage<'a, P> {
    pub fn try_new(page: &'a DataPage, dict: Option<&'a P>) -> Result<Self, Error> {
        let values = FixedBinaryPageValues::try_new(page, dict)?;

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

#[derive(Debug)]
pub enum FilteredFixedBinaryPageValues<'a, P> {
    Plain(SliceFilteredIter<std::slice::ChunksExact<'a, u8>>),
    Dictionary(FilteredDictionary<'a, P>),
}

impl<'a, P> FilteredFixedBinaryPageValues<'a, P> {
    pub fn new(page: FixedBinaryPageValues<'a, P>, intervals: VecDeque<Interval>) -> Self {
        match page {
            FixedBinaryPageValues::Plain(values) => {
                Self::Plain(SliceFilteredIter::new(values, intervals))
            }
            FixedBinaryPageValues::Dictionary(values) => Self::Dictionary(FilteredDictionary {
                indexes: SliceFilteredIter::new(values.indexes, intervals),
                dict: values.dict,
            }),
        }
    }
}

#[derive(Debug)]
pub enum FilteredFixedBinaryPage<'a, P> {
    Optional(
        FilteredOptionalPageValidity<'a>,
        FixedBinaryPageValues<'a, P>,
    ),
    Required(FilteredFixedBinaryPageValues<'a, P>),
    // todo: levels
}

impl<'a, P> FilteredFixedBinaryPage<'a, P> {
    pub fn try_new(
        page: NominalFixedBinaryPage<'a, P>,
        intervals: VecDeque<Interval>,
    ) -> Result<Self, Error> {
        Ok(match page {
            NominalFixedBinaryPage::Optional(iter, values) => Self::Optional(
                FilteredOptionalPageValidity::new(FilteredHybridRleDecoderIter::new(
                    iter.iter, intervals,
                )),
                values,
            ),
            NominalFixedBinaryPage::Required(values) => {
                Self::Required(FilteredFixedBinaryPageValues::new(values, intervals))
            }
            NominalFixedBinaryPage::Levels(_, _, _) => {
                return Err(Error::FeatureNotSupported("Filtered levels".to_string()))
            }
        })
    }
}

/// The deserialization state of a [`DataPage`] of a parquet FixedBinary type
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum FixedBinaryPage<'a, P> {
    Nominal(NominalFixedBinaryPage<'a, P>),
    Filtered(FilteredFixedBinaryPage<'a, P>),
}

impl<'a, P> FixedBinaryPage<'a, P> {
    /// Tries to create [`FixedBinaryPage`]
    /// # Error
    /// Errors iff the page is not a `FixedBinaryPage`
    pub fn try_new(page: &'a DataPage, dict: Option<&'a P>) -> Result<Self, Error> {
        let native_page = NominalFixedBinaryPage::try_new(page, dict)?;

        if let Some(selected_rows) = page.selected_rows() {
            FilteredFixedBinaryPage::try_new(native_page, selected_rows.iter().copied().collect())
                .map(Self::Filtered)
        } else {
            Ok(Self::Nominal(native_page))
        }
    }
}
