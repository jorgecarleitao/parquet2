use parquet2::deserialize::{BooleanPageState, BooleanValuesPageState};
use parquet2::encoding::hybrid_rle::BitmapIter;
use parquet2::error::Result;
use parquet2::page::DataPage;

use crate::read::utils::{deserialize_levels, deserialize_optional1};

pub fn page_to_vec(page: &DataPage) -> Result<Vec<Option<bool>>> {
    assert_eq!(page.descriptor.max_rep_level, 0);
    let state = BooleanPageState::try_new(page)?;

    match state {
        BooleanPageState::Optional(validity, values) => match values {
            BooleanValuesPageState::Plain(values) => {
                let values = BitmapIter::new(values.values, values.offset, values.length);
                deserialize_optional1(validity, values.map(Ok))
            }
        },
        BooleanPageState::Required(values) => match values {
            BooleanValuesPageState::Plain(values) => {
                Ok(BitmapIter::new(values.values, values.offset, values.length)
                    .into_iter()
                    .map(Some)
                    .collect())
            }
        },
        BooleanPageState::Levels(validity, max, values) => {
            let validity = validity.map(|def| Ok(def? == max));
            match values {
                BooleanValuesPageState::Plain(values) => {
                    let values = BitmapIter::new(values.values, values.offset, values.length);
                    deserialize_levels(validity, values.map(Ok))
                }
            }
        }
    }
}
