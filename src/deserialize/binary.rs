use crate::{
    encoding::{delta_length_byte_array, hybrid_rle, plain_byte_array::BinaryIter},
    error::Error,
    page::{split_buffer, DataPage},
    parquet_bridge::{Encoding, Repetition},
};

use super::utils::{self, get_selected_rows, FilteredOptionalPageValidity, OptionalPageValidity};
use super::SliceFilteredIter;

/// The state of a binary-encoded, non-nested [`DataPage`]
#[derive(Debug)]
pub enum BinaryPageState<'a, P> {
    Optional(OptionalPageValidity<'a>, BinaryIter<'a>),
    Required(BinaryIter<'a>),
    RequiredDictionary(Dictionary<'a, P>),
    OptionalDictionary(OptionalPageValidity<'a>, Dictionary<'a, P>),
    Delta(Delta<'a>),
    OptionalDelta(OptionalPageValidity<'a>, Delta<'a>),
    FilteredRequired(FilteredRequired<'a>),
    FilteredDelta(FilteredDelta<'a>),
    FilteredOptionalDelta(FilteredOptionalPageValidity<'a>, Delta<'a>),
    FilteredOptional(FilteredOptionalPageValidity<'a>, BinaryIter<'a>),
    FilteredRequiredDictionary(FilteredRequiredDictionary<'a, P>),
    FilteredOptionalDictionary(FilteredOptionalPageValidity<'a>, Dictionary<'a, P>),
}

impl<'a, P> BinaryPageState<'a, P> {
    pub fn try_new(page: &'a DataPage, dict: Option<&'a P>) -> Result<Self, Error> {
        let is_optional =
            page.descriptor.primitive_type.field_info.repetition == Repetition::Optional;
        let is_filtered = page.selected_rows().is_some();

        match (page.encoding(), dict, is_optional, is_filtered) {
            (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), false, false) => {
                Ok(Self::RequiredDictionary(Dictionary::try_new(page, dict)?))
            }
            (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), true, false) => {
                Ok(Self::OptionalDictionary(
                    OptionalPageValidity::try_new(page)?,
                    Dictionary::try_new(page, dict)?,
                ))
            }
            (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), false, true) => {
                FilteredRequiredDictionary::try_new(page, dict)
                    .map(Self::FilteredRequiredDictionary)
            }
            (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), true, true) => {
                Ok(Self::FilteredOptionalDictionary(
                    FilteredOptionalPageValidity::try_new(page)?,
                    Dictionary::try_new(page, dict)?,
                ))
            }
            (Encoding::Plain, _, true, false) => {
                let (_, _, values) = split_buffer(page)?;

                let values = BinaryIter::new(values, None);

                Ok(Self::Optional(OptionalPageValidity::try_new(page)?, values))
            }
            (Encoding::Plain, _, false, false) => {
                let (_, _, values) = split_buffer(page)?;

                Ok(Self::Required(BinaryIter::new(
                    values,
                    Some(page.num_values()),
                )))
            }
            (Encoding::Plain, _, false, true) => {
                Ok(Self::FilteredRequired(FilteredRequired::new(page)))
            }
            (Encoding::Plain, _, true, true) => {
                let (_, _, values) = split_buffer(page)?;

                Ok(Self::FilteredOptional(
                    FilteredOptionalPageValidity::try_new(page)?,
                    BinaryIter::new(values, None),
                ))
            }
            (Encoding::DeltaLengthByteArray, _, false, false) => {
                Delta::try_new(page).map(Self::Delta)
            }
            (Encoding::DeltaLengthByteArray, _, true, false) => Ok(Self::OptionalDelta(
                OptionalPageValidity::try_new(page)?,
                Delta::try_new(page)?,
            )),
            (Encoding::DeltaLengthByteArray, _, false, true) => {
                FilteredDelta::try_new(page).map(Self::FilteredDelta)
            }
            (Encoding::DeltaLengthByteArray, _, true, true) => Ok(Self::FilteredOptionalDelta(
                FilteredOptionalPageValidity::try_new(page)?,
                Delta::try_new(page)?,
            )),
            (other, _, _, _) => Err(Error::OutOfSpec(format!(
                "Binary-encoded non-nested pages cannot be encoded as {other:?}"
            ))),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Optional(validity, _) => validity.len(),
            Self::Required(state) => state.size_hint().0,
            Self::Delta(state) => state.len(),
            Self::OptionalDelta(state, _) => state.len(),
            Self::RequiredDictionary(values) => values.len(),
            Self::OptionalDictionary(optional, _) => optional.len(),
            Self::FilteredRequired(state) => state.len(),
            Self::FilteredOptional(validity, _) => validity.len(),
            Self::FilteredDelta(state) => state.len(),
            Self::FilteredOptionalDelta(state, _) => state.len(),
            Self::FilteredRequiredDictionary(values) => values.len(),
            Self::FilteredOptionalDictionary(optional, _) => optional.len(),
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Debug)]
pub struct Dictionary<'a, P> {
    pub indexes: hybrid_rle::HybridRleDecoder<'a>,
    pub dict: &'a P,
}

impl<'a, P> Dictionary<'a, P> {
    pub fn try_new(page: &'a DataPage, dict: &'a P) -> Result<Self, Error> {
        let indexes = utils::dict_indices_decoder(page)?;

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
pub struct FilteredRequiredDictionary<'a, P> {
    pub values: SliceFilteredIter<hybrid_rle::HybridRleDecoder<'a>>,
    pub dict: &'a P,
}

impl<'a, P> FilteredRequiredDictionary<'a, P> {
    pub fn try_new(page: &'a DataPage, dict: &'a P) -> Result<Self, Error> {
        let values = utils::dict_indices_decoder(page)?;

        let rows = get_selected_rows(page);
        let values = SliceFilteredIter::new(values, rows);

        Ok(Self { values, dict })
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.values.size_hint().0
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
