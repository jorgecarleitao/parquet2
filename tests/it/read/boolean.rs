use parquet2::deserialize::{BooleanPageState, HybridEncoded};
use parquet2::encoding::hybrid_rle::BitmapIter;
use parquet2::error::Result;
use parquet2::page::DataPage;

use crate::read::utils::deserialize_optional;

pub fn page_to_vec(page: &DataPage) -> Result<Vec<Option<bool>>> {
    assert_eq!(page.descriptor.max_rep_level, 0);
    let state = BooleanPageState::try_new(page)?;

    match state {
        BooleanPageState::Optional(validity, values) => deserialize_optional(validity, values),
        BooleanPageState::Required(values) => {
            let mut deserialized = Vec::with_capacity(values.len());

            values.for_each(|run| match run {
                HybridEncoded::Bitmap(bitmap, offset, length) => {
                    BitmapIter::new(bitmap, offset, length)
                        .into_iter()
                        .for_each(|x| deserialized.push(Some(x)));
                }
                HybridEncoded::Repeated(is_set, length) => {
                    deserialized.extend(std::iter::repeat(Some(is_set)).take(length))
                }
            });
            Ok(deserialized)
        }
    }
}
