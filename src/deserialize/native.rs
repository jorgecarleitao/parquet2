use std::collections::VecDeque;

use crate::{
    encoding::hybrid_rle,
    error::Error,
    indexes::Interval,
    page::{split_buffer, DataPage},
    parquet_bridge::Encoding,
    types::{decode, NativeType},
};

use super::{utils, values::*, SliceFilteredIter};

/// Typedef of an iterator over PLAIN page values
pub type Casted<'a, T> = std::iter::Map<std::slice::ChunksExact<'a, u8>, fn(&'a [u8]) -> T>;

/// Views the values of the data page as [`Casted`] to [`NativeType`].
pub fn native_cast<T: NativeType>(page: &DataPage) -> Result<Casted<T>, Error> {
    let (_, _, values) = split_buffer(page)?;
    if values.len() % std::mem::size_of::<T>() != 0 {
        return Err(Error::oos(
            "A primitive page data's len must be a multiple of the type",
        ));
    }

    Ok(values
        .chunks_exact(std::mem::size_of::<T>())
        .map(decode::<T>))
}

#[derive(Debug)]
pub struct Dictionary<'a, P> {
    pub indexes: hybrid_rle::HybridRleDecoder<'a>,
    pub dict: &'a P,
}

impl<'a, P> Dictionary<'a, P> {
    fn try_new(page: &'a DataPage, dict: &'a P) -> Result<Self, Error> {
        let indexes = utils::dict_indices_decoder(page)?;

        Ok(Self { dict, indexes })
    }

    pub fn len(&self) -> usize {
        self.indexes.size_hint().0
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Debug)]
pub enum NativeValuesDecoder<'a, T: NativeType, P> {
    Plain(Casted<'a, T>),
    Dictionary(Dictionary<'a, P>),
}

impl<'a, T: NativeType, P> NativeValuesDecoder<'a, T, P> {
    pub fn try_new(page: &'a DataPage, dict: Option<&'a P>) -> Result<Self, Error> {
        match (page.encoding(), dict) {
            (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict)) => {
                Dictionary::try_new(page, dict).map(Self::Dictionary)
            }
            (Encoding::Plain, _) => native_cast(page).map(Self::Plain),
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
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<'a, T: NativeType, P> ValuesDecoder for NativeValuesDecoder<'a, T, P> {
    fn len(&self) -> usize {
        self.len()
    }
}

#[derive(Debug)]
pub struct FilteredDictionary<'a, P> {
    pub indexes: SliceFilteredIter<hybrid_rle::HybridRleDecoder<'a>>,
    pub dict: &'a P,
}

#[derive(Debug)]
pub enum NativeFilteredValuesDecoder<'a, T: NativeType, P> {
    Plain(SliceFilteredIter<Casted<'a, T>>),
    Dictionary(FilteredDictionary<'a, P>),
}

impl<'a, T: NativeType, P> From<(NativeValuesDecoder<'a, T, P>, VecDeque<Interval>)>
    for NativeFilteredValuesDecoder<'a, T, P>
{
    fn from((page, intervals): (NativeValuesDecoder<'a, T, P>, VecDeque<Interval>)) -> Self {
        match page {
            NativeValuesDecoder::Plain(values) => {
                Self::Plain(SliceFilteredIter::new(values, intervals))
            }
            NativeValuesDecoder::Dictionary(values) => Self::Dictionary(FilteredDictionary {
                indexes: SliceFilteredIter::new(values.indexes, intervals),
                dict: values.dict,
            }),
        }
    }
}

pub type NativeDecoder<'a, T, P> =
    Decoder<'a, NativeValuesDecoder<'a, T, P>, NativeFilteredValuesDecoder<'a, T, P>>;
