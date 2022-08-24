use parquet2::deserialize::{FixedBinaryPage, FixedBinaryPageValues, NominalFixedBinaryPage};
use parquet2::{error::Result, page::DataPage};

use super::dictionary::FixedLenByteArrayPageDict;
use super::utils::deserialize_optional;

pub fn page_to_vec(
    page: &DataPage,
    dict: Option<&FixedLenByteArrayPageDict>,
) -> Result<Vec<Option<Vec<u8>>>> {
    assert_eq!(page.descriptor.max_rep_level, 0);

    let state = FixedBinaryPage::try_new(page, dict)?;

    match state {
        FixedBinaryPage::Nominal(state) => match state {
            NominalFixedBinaryPage::Optional(validity, values) => match values {
                FixedBinaryPageValues::Plain(values) => {
                    deserialize_optional(validity, values.map(|x| Ok(x.to_vec())))
                }
                FixedBinaryPageValues::Dictionary(dict) => deserialize_optional(
                    validity,
                    dict.indexes
                        .map(|x| x.and_then(|x| dict.dict.value(x as usize).map(|x| x.to_vec()))),
                ),
            },
            NominalFixedBinaryPage::Required(values) => match values {
                FixedBinaryPageValues::Plain(values) => {
                    Ok(values.map(|x| x.to_vec()).map(Some).collect())
                }
                FixedBinaryPageValues::Dictionary(dict) => dict
                    .indexes
                    .map(|x| {
                        x.and_then(|x| dict.dict.value(x as usize).map(|x| x.to_vec()).map(Some))
                    })
                    .collect(),
            },
            NominalFixedBinaryPage::Levels(..) => {
                unreachable!()
            }
        },
        FixedBinaryPage::Filtered(_) => todo!(),
    }
}
