use crate::{
    encoding::hybrid_rle::BitmapIter,
    error::Error,
    page::{split_buffer, DataPage},
    parquet_bridge::{Encoding, Repetition},
};

use super::utils;

// The state of a `DataPage` of `Boolean` parquet boolean type
#[derive(Debug)]
pub enum BooleanPageState<'a> {
    Optional(utils::HybridDecoderBitmapIter<'a>, BitmapIter<'a>),
    Required(utils::HybridDecoderBitmapIter<'a>),
}

impl<'a> BooleanPageState<'a> {
    pub fn try_new(page: &'a DataPage) -> Result<Self, Error> {
        let is_optional =
            page.descriptor.primitive_type.field_info.repetition == Repetition::Optional;

        match (page.encoding(), page.dictionary_page(), is_optional) {
            (Encoding::Plain, _, true) => {
                let validity = utils::try_new_iter(page)?;

                let (_, _, values) = split_buffer(page);
                let values = BitmapIter::new(values, 0, values.len() * 8);

                Ok(Self::Optional(validity, values))
            }
            (Encoding::Plain, _, false) => Ok(Self::Required(utils::try_new_iter(page)?)),
            _ => Err(Error::General(format!(
                "Viewing page for encoding {:?} for boolean type not supported",
                page.encoding(),
            ))),
        }
    }
}
