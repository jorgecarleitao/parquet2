use std::collections::VecDeque;

use crate::{
    encoding::delta_bitpacked,
    error::Error,
    indexes::Interval,
    page::{split_buffer, DataPage},
    parquet_bridge::Encoding,
    types::NativeType,
};

use super::{
    values::{Decoder, ValuesDecoder},
    NativeFilteredValuesDecoder, NativeValuesDecoder, SliceFilteredIter,
};

pub trait AsNative<T: NativeType> {
    fn as_(self) -> T;
}

impl AsNative<i32> for i32 {
    #[inline]
    fn as_(self) -> i32 {
        self
    }
}

impl AsNative<i64> for i64 {
    #[inline]
    fn as_(self) -> i64 {
        self
    }
}

impl AsNative<i64> for i32 {
    #[inline]
    fn as_(self) -> i64 {
        self as i64
    }
}

/// The state of a [`DataPage`] of an integer parquet type (i32 or i64)
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum IntegerValuesDecoder<'a, T, P>
where
    T: NativeType,
    i64: AsNative<T>,
{
    Common(NativeValuesDecoder<'a, T, P>),
    DeltaBinaryPacked(delta_bitpacked::Decoder<'a>),
}

impl<'a, T, P> IntegerValuesDecoder<'a, T, P>
where
    T: NativeType,
    i64: AsNative<T>,
{
    pub fn try_new(page: &'a DataPage, dict: Option<&'a P>) -> Result<Self, Error> {
        match (page.encoding(), dict) {
            (Encoding::DeltaBinaryPacked, _) => {
                let (_, _, values) = split_buffer(page)?;
                delta_bitpacked::Decoder::try_new(values).map(Self::DeltaBinaryPacked)
            }
            _ => NativeValuesDecoder::try_new(page, dict).map(Self::Common),
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::Common(validity) => validity.len(),
            Self::DeltaBinaryPacked(state) => state.size_hint().0,
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<'a, T, P> ValuesDecoder for IntegerValuesDecoder<'a, T, P>
where
    T: NativeType,
    i64: AsNative<T>,
{
    fn len(&self) -> usize {
        self.len()
    }
}

/// The state of a [`DataPage`] of an integer parquet type (i32 or i64)
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum IntegerFilteredValuesDecoder<'a, T, P>
where
    T: NativeType,
    i64: AsNative<T>,
{
    Common(NativeFilteredValuesDecoder<'a, T, P>),
    DeltaBinaryPacked(SliceFilteredIter<delta_bitpacked::Decoder<'a>>),
}

impl<'a, T, P> From<(IntegerValuesDecoder<'a, T, P>, VecDeque<Interval>)>
    for IntegerFilteredValuesDecoder<'a, T, P>
where
    T: NativeType,
    i64: AsNative<T>,
{
    fn from((page, intervals): (IntegerValuesDecoder<'a, T, P>, VecDeque<Interval>)) -> Self {
        match page {
            IntegerValuesDecoder::Common(values) => Self::Common((values, intervals).into()),
            IntegerValuesDecoder::DeltaBinaryPacked(values) => {
                Self::DeltaBinaryPacked(SliceFilteredIter::new(values, intervals))
            }
        }
    }
}

pub type IntegerDecoder<'a, T, P> =
    Decoder<'a, IntegerValuesDecoder<'a, T, P>, IntegerFilteredValuesDecoder<'a, T, P>>;
