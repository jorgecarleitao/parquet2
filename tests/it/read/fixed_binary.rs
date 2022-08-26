use parquet2::{
    deserialize::{FixedBinaryDecoder, FixedBinaryValuesDecoder, FullDecoder},
    error::Result,
    page::DataPage,
};

use super::dictionary::FixedLenByteArrayPageDict;
use super::utils::deserialize_optional;

pub fn page_to_vec(
    page: &DataPage,
    dict: Option<&FixedLenByteArrayPageDict>,
) -> Result<Vec<Option<Vec<u8>>>> {
    assert_eq!(page.descriptor.max_rep_level, 0);

    let values = FixedBinaryValuesDecoder::try_new(page, dict)?;
    let decoder = FullDecoder::try_new(page, values)?;
    let decoder = FixedBinaryDecoder::try_new(page, decoder)?;

    match decoder {
        FixedBinaryDecoder::Full(state) => match state {
            FullDecoder::Optional(validity, values) => match values {
                FixedBinaryValuesDecoder::Plain(values) => {
                    deserialize_optional(validity, values.map(|x| Ok(x.to_vec())))
                }
                FixedBinaryValuesDecoder::Dictionary(dict) => deserialize_optional(
                    validity,
                    dict.indexes
                        .map(|x| x.and_then(|x| dict.dict.value(x as usize).map(|x| x.to_vec()))),
                ),
            },
            FullDecoder::Required(values) => match values {
                FixedBinaryValuesDecoder::Plain(values) => {
                    Ok(values.map(|x| x.to_vec()).map(Some).collect())
                }
                FixedBinaryValuesDecoder::Dictionary(dict) => dict
                    .indexes
                    .map(|x| {
                        x.and_then(|x| dict.dict.value(x as usize).map(|x| x.to_vec()).map(Some))
                    })
                    .collect(),
            },
            FullDecoder::Levels(..) => {
                unreachable!()
            }
        },
        FixedBinaryDecoder::Filtered(_) => todo!(),
    }
}
