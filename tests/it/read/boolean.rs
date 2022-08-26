use parquet2::deserialize::{BooleanDecoder, BooleanValuesDecoder, FullDecoder};
use parquet2::encoding::hybrid_rle::BitmapIter;
use parquet2::error::Result;
use parquet2::page::DataPage;

use crate::read::utils::{deserialize_levels, deserialize_optional};

pub fn page_to_vec(page: &DataPage) -> Result<Vec<Option<bool>>> {
    assert_eq!(page.descriptor.max_rep_level, 0);

    let values = BooleanValuesDecoder::try_new(page)?;
    let decoder = FullDecoder::try_new(page, values)?;
    let decoder = BooleanDecoder::try_new(page, decoder)?;

    match decoder {
        BooleanDecoder::Full(values) => match values {
            FullDecoder::Optional(validity, values) => match values {
                BooleanValuesDecoder::Plain(values) => {
                    let values = BitmapIter::new(values.values, values.offset, values.length);
                    deserialize_optional(validity, values.map(Ok))
                }
            },
            FullDecoder::Required(values) => match values {
                BooleanValuesDecoder::Plain(values) => {
                    Ok(BitmapIter::new(values.values, values.offset, values.length)
                        .into_iter()
                        .map(Some)
                        .collect())
                }
            },
            FullDecoder::Levels(validity, max, values) => {
                let validity = validity.map(|def| Ok(def? == max));
                match values {
                    BooleanValuesDecoder::Plain(values) => {
                        let values = BitmapIter::new(values.values, values.offset, values.length);
                        deserialize_levels(validity, values.map(Ok))
                    }
                }
            }
        },
        BooleanDecoder::Filtered(..) => todo!(),
    }
}
