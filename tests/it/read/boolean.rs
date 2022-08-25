use parquet2::deserialize::{BooleanPage, BooleanPageValues, NominalBooleanPage};
use parquet2::encoding::hybrid_rle::BitmapIter;
use parquet2::error::Result;
use parquet2::page::DataPage;

use crate::read::utils::{deserialize_levels, deserialize_optional};

pub fn page_to_vec(page: &DataPage) -> Result<Vec<Option<bool>>> {
    assert_eq!(page.descriptor.max_rep_level, 0);
    let state = BooleanPage::try_new(page)?;

    match state {
        BooleanPage::Nominal(values) => match values {
            NominalBooleanPage::Optional(validity, values) => match values {
                BooleanPageValues::Plain(values) => {
                    let values = BitmapIter::new(values.values, values.offset, values.length);
                    deserialize_optional(validity, values.map(Ok))
                }
            },
            NominalBooleanPage::Required(values) => match values {
                BooleanPageValues::Plain(values) => {
                    Ok(BitmapIter::new(values.values, values.offset, values.length)
                        .into_iter()
                        .map(Some)
                        .collect())
                }
            },
            NominalBooleanPage::Levels(validity, max, values) => {
                let validity = validity.map(|def| Ok(def? == max));
                match values {
                    BooleanPageValues::Plain(values) => {
                        let values = BitmapIter::new(values.values, values.offset, values.length);
                        deserialize_levels(validity, values.map(Ok))
                    }
                }
            }
        },
        BooleanPage::Filtered(..) => todo!(),
    }
}
