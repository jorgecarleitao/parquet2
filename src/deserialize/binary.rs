use std::collections::VecDeque;

use crate::{
    encoding::{delta_length_byte_array, hybrid_rle, plain_byte_array::BinaryIter},
    error::Error,
    indexes::Interval,
    page::{split_buffer, DataPage},
    parquet_bridge::{Encoding, Repetition},
};

use super::{
    utils::{dict_indices_decoder, get_selected_rows},
    values::Decoder,
};
use super::{values::ValuesDecoder, SliceFilteredIter};

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
    type Item = Result<&'a [u8], Error>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let length = self.lengths.next()?;
        if length > self.values.len() {
            return Some(Err(Error::oos(
                "Delta contains a length larger than the values",
            )));
        }
        let (item, remaining) = self.values.split_at(length);
        self.values = remaining;
        Some(Ok(item))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.lengths.size_hint()
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

#[derive(Debug)]
pub enum BinaryValuesDecoder<'a, P> {
    Plain(BinaryIter<'a>),
    Dictionary(Dictionary<'a, P>),
    Delta(Delta<'a>),
}

impl<'a, P> BinaryValuesDecoder<'a, P> {
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

impl<'a, P> ValuesDecoder for BinaryValuesDecoder<'a, P> {
    fn len(&self) -> usize {
        self.len()
    }
}

#[derive(Debug)]
pub enum BinaryFilteredValuesDecoder<'a, P> {
    Plain(SliceFilteredIter<BinaryIter<'a>>),
    Dictionary(FilteredDictionary<'a, P>),
    Delta(SliceFilteredIter<Delta<'a>>),
}

impl<'a, P> From<(BinaryValuesDecoder<'a, P>, VecDeque<Interval>)>
    for BinaryFilteredValuesDecoder<'a, P>
{
    fn from((decoder, intervals): (BinaryValuesDecoder<'a, P>, VecDeque<Interval>)) -> Self {
        match decoder {
            BinaryValuesDecoder::Plain(values) => {
                Self::Plain(SliceFilteredIter::new(values, intervals))
            }
            BinaryValuesDecoder::Dictionary(values) => Self::Dictionary(FilteredDictionary {
                indexes: SliceFilteredIter::new(values.indexes, intervals),
                dict: values.dict,
            }),
            BinaryValuesDecoder::Delta(values) => {
                Self::Delta(SliceFilteredIter::new(values, intervals))
            }
        }
    }
}

pub type BinaryDecoder<'a, P> =
    Decoder<'a, BinaryValuesDecoder<'a, P>, BinaryFilteredValuesDecoder<'a, P>>;
