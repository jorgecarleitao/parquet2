use parquet2::{
    deserialize::{BinaryPage, BinaryPageValues, NominalBinaryPage},
    error::Result,
    page::DataPage,
};

use super::dictionary::BinaryPageDict;
use super::utils::{deserialize_levels, deserialize_optional1};

pub fn page_to_vec(page: &DataPage, dict: Option<&BinaryPageDict>) -> Result<Vec<Option<Vec<u8>>>> {
    assert_eq!(page.descriptor.max_rep_level, 0);

    let state = BinaryPage::try_new(page, dict)?;
    match state {
        BinaryPage::Nominal(state) => match state {
            NominalBinaryPage::Optional(validity, values) => match values {
                BinaryPageValues::Plain(values) => {
                    deserialize_optional1(validity, values.map(|x| x.map(|x| x.to_vec())))
                }
                BinaryPageValues::Dictionary(dict) => deserialize_optional1(
                    validity,
                    dict.indexes
                        .map(|x| x.and_then(|x| dict.dict.value(x as usize).map(|x| x.to_vec()))),
                ),
                BinaryPageValues::Delta(_) => todo!(),
            },
            NominalBinaryPage::Required(values) => match values {
                BinaryPageValues::Plain(values) => values
                    .map(|x| x.map(|x| x.to_vec()))
                    .map(Some)
                    .map(|x| x.transpose())
                    .collect(),
                BinaryPageValues::Dictionary(dict) => dict
                    .indexes
                    .map(|x| {
                        x.and_then(|x| dict.dict.value(x as usize).map(|x| x.to_vec()).map(Some))
                    })
                    .collect(),
                BinaryPageValues::Delta(_) => todo!(),
            },
            NominalBinaryPage::Levels(validity, max, values) => {
                let validity = validity.map(|def| Ok(def? == max));
                match values {
                    BinaryPageValues::Plain(values) => {
                        deserialize_levels(validity, values.map(|x| x.map(|x| x.to_vec())))
                    }
                    BinaryPageValues::Dictionary(dict) => {
                        let values = dict.indexes.map(|x| {
                            x.and_then(|x| dict.dict.value(x as usize).map(|x| x.to_vec()))
                        });
                        deserialize_levels(validity, values)
                    }
                    _ => todo!(),
                }
            }
        },
        BinaryPage::Filtered(..) => todo!(),
    }
}
