use crate::{
    encoding::hybrid_rle,
    error::Error,
    page::{split_buffer, DataPage, FixedLenByteArrayPageDict},
    parquet_bridge::{Encoding, Repetition},
    schema::types::PhysicalType,
};

use super::utils;

#[derive(Debug)]
pub struct FixexBinaryIter<'a> {
    values: std::slice::ChunksExact<'a, u8>,
}

impl<'a> FixexBinaryIter<'a> {
    pub fn new(values: &'a [u8], size: usize) -> Self {
        let values = values.chunks_exact(size);
        Self { values }
    }
}

impl<'a> Iterator for FixexBinaryIter<'a> {
    type Item = &'a [u8];

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.values.next()
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.values.size_hint()
    }
}

#[derive(Debug)]
pub struct Dictionary<'a> {
    pub indexes: hybrid_rle::HybridRleDecoder<'a>,
    pub dict: &'a FixedLenByteArrayPageDict,
}

impl<'a> Dictionary<'a> {
    pub fn new(page: &'a DataPage, dict: &'a FixedLenByteArrayPageDict) -> Self {
        let indexes = utils::dict_indices_decoder(page);

        Self { indexes, dict }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.indexes.size_hint().0
    }
}

pub enum FixedLenBinaryPageState<'a> {
    Optional(utils::DefLevelsDecoder<'a>, FixexBinaryIter<'a>),
    Required(FixexBinaryIter<'a>),
    RequiredDictionary(Dictionary<'a>),
    OptionalDictionary(utils::DefLevelsDecoder<'a>, Dictionary<'a>),
}

impl<'a> FixedLenBinaryPageState<'a> {
    pub fn try_new(page: &'a DataPage) -> Result<Self, Error> {
        let is_optional =
            page.descriptor.primitive_type.field_info.repetition == Repetition::Optional;

        let size: usize = if let PhysicalType::FixedLenByteArray(size) =
            page.descriptor.primitive_type.physical_type
        {
            size
        } else {
            return Err(Error::General(
                "FixedLenBinaryPageState must be initialized by pages of FixedLenByteArray"
                    .to_string(),
            ));
        };

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
                let (_, _, values) = split_buffer(page);

                let validity = utils::DefLevelsDecoder::new(page);
                let values = FixexBinaryIter::new(values, size);

                Ok(Self::Optional(validity, values))
            }
            (Encoding::Plain, _, false) => {
                let (_, _, values) = split_buffer(page);
                let values = FixexBinaryIter::new(values, size);

                Ok(Self::Required(values))
            }
            _ => Err(Error::General(format!(
                "Viewing page for encoding {:?} for binary type not supported",
                page.encoding(),
            ))),
        }
    }
}
