use crate::{
    encoding::hybrid_rle::{BitmapIter, HybridRleDecoder},
    error::Error,
    page::{split_buffer, DataPage},
    parquet_bridge::{Encoding, Repetition},
    read::levels::get_bit_width,
};

use super::{utils::FilteredOptionalPageValidity, OptionalPageValidity, SliceFilteredIter};

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum BooleanValuesPageState<'a> {
    Plain(Plain<'a>),
}

/// The state of a required [`DataPage`] with a boolean physical type
#[derive(Debug)]
pub struct Plain<'a> {
    pub values: &'a [u8],
    // invariant: offset <= length;
    pub offset: usize,
    // invariant: length <= values.len() * 8;
    pub length: usize,
}

impl<'a> Plain<'a> {
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

impl<'a> BooleanValuesPageState<'a> {
    pub fn try_new(page: &'a DataPage) -> Result<Self, Error> {
        match page.encoding() {
            Encoding::Plain => Plain::try_new(page).map(Self::Plain),
            _ => Err(Error::InvalidParameter(format!(
                "Viewing page for encoding {:?} for boolean type not supported",
                page.encoding(),
            ))),
        }
    }
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum BooleanPageState<'a> {
    Optional(OptionalPageValidity<'a>, BooleanValuesPageState<'a>),
    Required(BooleanValuesPageState<'a>),
    Levels(HybridRleDecoder<'a>, u32, BooleanValuesPageState<'a>),
}

impl<'a> BooleanPageState<'a> {
    pub fn try_new(page: &'a DataPage) -> Result<Self, Error> {
        let values = BooleanValuesPageState::try_new(page)?;

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

        match (page.encoding(), is_optional) {
            (Encoding::Plain, true) => Ok(Self::Optional(
                OptionalPageValidity::try_new(page)?,
                BooleanValuesPageState::try_new(page)?,
            )),
            (Encoding::Plain, false) => BooleanValuesPageState::try_new(page).map(Self::Required),
            (other, _) => Err(Error::OutOfSpec(format!(
                "boolean-encoded non-nested pages cannot be encoded as {other:?}"
            ))),
        }
    }
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum FilteredBooleanPageState<'a> {
    Optional(FilteredOptionalPageValidity<'a>, BitmapIter<'a>),
    Required(SliceFilteredIter<BitmapIter<'a>>),
    Levels(HybridRleDecoder<'a>, u32, BitmapIter<'a>),
}
