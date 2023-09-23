use std::collections::VecDeque;

use crate::{
    encoding::hybrid_rle,
    error::Error,
    indexes::Interval,
    page::{split_buffer, DataPage},
    parquet_bridge::Encoding,
    schema::types::PhysicalType,
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
pub enum FixedBinaryValuesDecoder<'a, P> {
    Plain(std::slice::ChunksExact<'a, u8>),
    Dictionary(Dictionary<'a, P>),
}

impl<'a, P> FixedBinaryValuesDecoder<'a, P> {
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

impl<'a, P> ValuesDecoder for FixedBinaryValuesDecoder<'a, P> {
    fn len(&self) -> usize {
        self.len()
    }
}

#[derive(Debug)]
pub enum FixedBinaryFilteredValuesDecoder<'a, P> {
    Plain(SliceFilteredIter<std::slice::ChunksExact<'a, u8>>),
    Dictionary(FilteredDictionary<'a, P>),
}

impl<'a, P> From<(FixedBinaryValuesDecoder<'a, P>, VecDeque<Interval>)>
    for FixedBinaryFilteredValuesDecoder<'a, P>
{
    fn from((decoder, intervals): (FixedBinaryValuesDecoder<'a, P>, VecDeque<Interval>)) -> Self {
        match decoder {
            FixedBinaryValuesDecoder::Plain(values) => {
                Self::Plain(SliceFilteredIter::new(values, intervals))
            }
            FixedBinaryValuesDecoder::Dictionary(values) => Self::Dictionary(FilteredDictionary {
                indexes: SliceFilteredIter::new(values.indexes, intervals),
                dict: values.dict,
            }),
        }
    }
}

pub type FixedBinaryDecoder<'a, P> =
    Decoder<'a, FixedBinaryValuesDecoder<'a, P>, FixedBinaryFilteredValuesDecoder<'a, P>>;
