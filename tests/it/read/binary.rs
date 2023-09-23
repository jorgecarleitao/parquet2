use parquet2::{
    deserialize::{BinaryDecoder, BinaryValuesDecoder, FullDecoder},
    error::Result,
    page::DataPage,
};

use super::dictionary::BinaryPageDict;
use super::utils::{deserialize_levels, deserialize_optional};

pub fn page_to_vec(page: &DataPage, dict: Option<&BinaryPageDict>) -> Result<Vec<Option<Vec<u8>>>> {
    assert_eq!(page.descriptor.max_rep_level, 0);

    let values = BinaryValuesDecoder::try_new(page, dict)?;
    let decoder = FullDecoder::try_new(page, values)?;
    let decoder = BinaryDecoder::try_new(page, decoder)?;

    match decoder {
        BinaryDecoder::Full(state) => match state {
            FullDecoder::Optional(validity, values) => match values {
                BinaryValuesDecoder::Plain(values) => {
                    deserialize_optional(validity, values.map(|x| x.map(|x| x.to_vec())))
                }
                BinaryValuesDecoder::Dictionary(dict) => deserialize_optional(
                    validity,
                    dict.indexes
                        .map(|x| x.and_then(|x| dict.dict.value(x as usize).map(|x| x.to_vec()))),
                ),
                BinaryValuesDecoder::Delta(_) => todo!(),
            },
            FullDecoder::Required(values) => match values {
                BinaryValuesDecoder::Plain(values) => values
                    .map(|x| x.map(|x| x.to_vec()))
                    .map(Some)
                    .map(|x| x.transpose())
                    .collect(),
                BinaryValuesDecoder::Dictionary(dict) => dict
                    .indexes
                    .map(|x| {
                        x.and_then(|x| dict.dict.value(x as usize).map(|x| x.to_vec()).map(Some))
                    })
                    .collect(),
                BinaryValuesDecoder::Delta(_) => todo!(),
            },
            FullDecoder::Levels(validity, max, values) => {
                let validity = validity.map(|def| Ok(def? == max));
                match values {
                    BinaryValuesDecoder::Plain(values) => {
                        deserialize_levels(validity, values.map(|x| x.map(|x| x.to_vec())))
                    }
                    BinaryValuesDecoder::Dictionary(dict) => {
                        let values = dict.indexes.map(|x| {
                            x.and_then(|x| dict.dict.value(x as usize).map(|x| x.to_vec()))
                        });
                        deserialize_levels(validity, values)
                    }
                    BinaryValuesDecoder::Delta(_) => todo!(),
                }
            }
        },
        BinaryDecoder::Filtered(..) => todo!(),
    }
}
