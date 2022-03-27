use crate::{
    encoding::hybrid_rle,
    error::Error,
    page::{split_buffer, DataPage, PrimitivePageDict},
    parquet_bridge::{Encoding, Repetition},
    types::NativeType,
};

use super::utils;

#[inline]
pub fn cast_unaligned<T: NativeType>(chunk: &[u8]) -> T {
    let chunk: <T as NativeType>::Bytes = match chunk.try_into() {
        Ok(v) => v,
        Err(_) => panic!(),
    };
    T::from_le_bytes(chunk)
}

pub type Casted<'a, T> = std::iter::Map<std::slice::ChunksExact<'a, u8>, for<'r> fn(&'r [u8]) -> T>;

pub fn native_cast<T: NativeType>(page: &DataPage) -> Result<Casted<T>, Error> {
    let (_, _, values) = split_buffer(page);
    if values.len() % std::mem::size_of::<T>() != 0 {
        return Err(Error::OutOfSpec(
            "A primitive page data's len must be a multiple of the type".to_string(),
        ));
    }

    Ok(values
        .chunks_exact(std::mem::size_of::<T>())
        .map(cast_unaligned::<T>))
}

#[derive(Debug)]
pub struct Dictionary<'a, T>
where
    T: NativeType,
{
    pub indexes: hybrid_rle::HybridRleDecoder<'a>,
    pub values: &'a [T],
}

impl<'a, T> Dictionary<'a, T>
where
    T: NativeType,
{
    pub fn new(page: &'a DataPage, dict: &'a PrimitivePageDict<T>) -> Self {
        let indexes = utils::dict_indices_decoder(page);

        Self {
            values: dict.values(),
            indexes,
        }
    }

    pub fn len(&self) -> usize {
        self.indexes.size_hint().0
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// The deserialization state of a `DataPage` of `Primitive` parquet primitive type
#[derive(Debug)]
pub enum NativePageState<'a, T>
where
    T: NativeType,
{
    Optional(utils::DefLevelsDecoder<'a>, Casted<'a, T>),
    Required(Casted<'a, T>),
    RequiredDictionary(Dictionary<'a, T>),
    OptionalDictionary(utils::DefLevelsDecoder<'a>, Dictionary<'a, T>),
}

impl<'a, T: NativeType> NativePageState<'a, T> {
    pub fn try_new(page: &'a DataPage) -> Result<Self, Error> {
        let is_optional =
            page.descriptor.primitive_type.field_info.repetition == Repetition::Optional;

        match (page.encoding(), page.dictionary_page(), is_optional) {
            (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), false) => {
                let dict = dict.as_any().downcast_ref().unwrap();
                Ok(Self::RequiredDictionary(Dictionary::new(page, dict)))
            }
            (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), true) => {
                let dict = dict.as_any().downcast_ref().unwrap();

                Ok(Self::OptionalDictionary(
                    utils::DefLevelsDecoder::new(page),
                    Dictionary::new(page, dict),
                ))
            }
            (Encoding::Plain, _, true) => {
                let validity = utils::DefLevelsDecoder::new(page);
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
