use crate::{
    encoding::hybrid_rle,
    error::Error,
    page::{split_buffer, DataPage, PrimitivePageDict},
    parquet_bridge::{Encoding, Repetition},
    types::{decode, NativeType},
};

use super::utils;

/// Typedef of an iterator over PLAIN page values
pub type Casted<'a, T> = std::iter::Map<std::slice::ChunksExact<'a, u8>, fn(&'a [u8]) -> T>;

/// Views the values of the data page as [`Casted`] to [`NativeType`].
pub fn native_cast<T: NativeType>(page: &DataPage) -> Result<Casted<T>, Error> {
    let (_, _, values) = split_buffer(page)?;
    if values.len() % std::mem::size_of::<T>() != 0 {
        return Err(Error::OutOfSpec(
            "A primitive page data's len must be a multiple of the type".to_string(),
        ));
    }

    Ok(values
        .chunks_exact(std::mem::size_of::<T>())
        .map(decode::<T>))
}

#[derive(Debug)]
pub struct Dictionary<'a, T>
where
    T: NativeType,
{
    pub indexes: hybrid_rle::HybridRleDecoder<'a>,
    pub dict: &'a PrimitivePageDict<T>,
}

impl<'a, T> Dictionary<'a, T>
where
    T: NativeType,
{
    pub fn try_new(page: &'a DataPage, dict: &'a PrimitivePageDict<T>) -> Result<Self, Error> {
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

/// The deserialization state of a `DataPage` of `Primitive` parquet primitive type
#[derive(Debug)]
pub enum NativePageState<'a, T>
where
    T: NativeType,
{
    /// A page of optional values
    Optional(utils::DefLevelsDecoder<'a>, Casted<'a, T>),
    /// A page of required values
    Required(Casted<'a, T>),
    /// A page of required, dictionary-encoded values
    RequiredDictionary(Dictionary<'a, T>),
    /// A page of optional, dictionary-encoded values
    OptionalDictionary(utils::DefLevelsDecoder<'a>, Dictionary<'a, T>),
}

impl<'a, T: NativeType> NativePageState<'a, T> {
    /// Tries to create [`NativePageState`]
    /// # Error
    /// Errors iff the page is not a `NativePageState`
    pub fn try_new(page: &'a DataPage) -> Result<Self, Error> {
        let is_optional =
            page.descriptor.primitive_type.field_info.repetition == Repetition::Optional;

        match (page.encoding(), page.dictionary_page(), is_optional) {
            (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), false) => {
                let dict = dict.as_any().downcast_ref().unwrap();
                Ok(Self::RequiredDictionary(Dictionary::try_new(page, dict)?))
            }
            (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), true) => {
                let dict = dict.as_any().downcast_ref().unwrap();

                Ok(Self::OptionalDictionary(
                    utils::DefLevelsDecoder::try_new(page)?,
                    Dictionary::try_new(page, dict)?,
                ))
            }
            (Encoding::Plain, _, true) => {
                let validity = utils::DefLevelsDecoder::try_new(page)?;
                let values = native_cast(page)?;

                Ok(Self::Optional(validity, values))
            }
            (Encoding::Plain, _, false) => Ok(Self::Required(native_cast(page)?)),
            _ => Err(Error::General(format!(
                "Viewing page for encoding {:?} for native type {} not supported",
                page.encoding(),
                std::any::type_name::<T>()
            ))),
        }
    }
}
