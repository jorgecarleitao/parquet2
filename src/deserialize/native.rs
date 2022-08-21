use crate::{
    encoding::hybrid_rle::{self, HybridRleDecoder},
    error::Error,
    page::{split_buffer, DataPage},
    parquet_bridge::{Encoding, Repetition},
    read::levels::get_bit_width,
    types::{decode, NativeType},
};

use super::{utils, OptionalPageValidity};

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
    pub fn try_new(page: &'a DataPage, dict: &'a P) -> Result<Self, Error> {
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
pub enum NativePageValues<'a, T: NativeType, P> {
    Plain(Casted<'a, T>),
    Dictionary(Dictionary<'a, P>),
}

impl<'a, T: NativeType, P> NativePageValues<'a, T, P> {
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

#[derive(Debug)]
pub enum NativePage<'a, T: NativeType, P> {
    Optional(OptionalPageValidity<'a>, NativePageValues<'a, T, P>),
    Required(NativePageValues<'a, T, P>),
    Levels(HybridRleDecoder<'a>, u32, NativePageValues<'a, T, P>),
}

impl<'a, T: NativeType, P> NativePage<'a, T, P> {
    pub fn try_new(page: &'a DataPage, dict: Option<&'a P>) -> Result<Self, Error> {
        let values = NativePageValues::try_new(page, dict)?;

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
