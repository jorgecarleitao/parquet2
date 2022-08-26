use std::collections::VecDeque;

use crate::{
    encoding::hybrid_rle::HybridRleDecoder,
    error::Error,
    indexes::Interval,
    page::{split_buffer, DataPage},
    parquet_bridge::Repetition,
    read::levels::get_bit_width,
};

use super::{FilteredHybridRleDecoderIter, FilteredOptionalPageValidity, OptionalPageValidity};

pub trait ValuesDecoder: Sized {
    fn len(&self) -> usize;
}

#[derive(Debug)]
pub enum FullDecoder<'a, P: ValuesDecoder> {
    Optional(OptionalPageValidity<'a>, P),
    Required(P),
    Levels(HybridRleDecoder<'a>, u32, P),
}

impl<'a, P: ValuesDecoder> FullDecoder<'a, P> {
    pub fn try_new(page: &'a DataPage, values: P) -> Result<Self, Error> {
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

/// The [`DataPage`]
#[derive(Debug)]
pub enum FilteredDecoder<'a, V, F> {
    /// A page of optional values
    Optional(FilteredOptionalPageValidity<'a>, V),
    /// A page of required values
    Required(F),
}

impl<'a, V: ValuesDecoder, F: From<(V, VecDeque<Interval>)>> FilteredDecoder<'a, V, F> {
    fn try_new(decoder: FullDecoder<'a, V>, intervals: VecDeque<Interval>) -> Result<Self, Error> {
        Ok(match decoder {
            FullDecoder::Optional(iter, values) => Self::Optional(
                FilteredOptionalPageValidity::new(FilteredHybridRleDecoderIter::new(
                    iter.iter, intervals,
                )),
                values,
            ),
            FullDecoder::Required(values) => Self::Required((values, intervals).into()),
            FullDecoder::Levels(_, _, _) => {
                return Err(Error::FeatureNotSupported("Filtered levels".to_string()))
            }
        })
    }
}

/// The deserialization state of a [`DataPage`] of a parquet primitive type
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum Decoder<'a, V: ValuesDecoder, F: From<(V, VecDeque<Interval>)>> {
    Full(FullDecoder<'a, V>),
    Filtered(FilteredDecoder<'a, V, F>),
}

impl<'a, V: ValuesDecoder, F: From<(V, VecDeque<Interval>)>> Decoder<'a, V, F> {
    /// Tries to create [`NativePage`]
    /// # Error
    /// Errors when:
    /// * The page does not contain a valid encoding
    pub fn try_new(page: &'a DataPage, decoder: FullDecoder<'a, V>) -> Result<Self, Error> {
        if let Some(selected_rows) = page.selected_rows() {
            FilteredDecoder::try_new(decoder, selected_rows.iter().copied().collect())
                .map(Self::Filtered)
        } else {
            Ok(Self::Full(decoder))
        }
    }
}
