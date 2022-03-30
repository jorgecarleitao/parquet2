use parquet2::{deserialize::FixedLenBinaryPageState, error::Result, page::DataPage};

use crate::read::utils::deserialize_optional;

pub fn page_to_vec(page: &DataPage) -> Result<Vec<Option<Vec<u8>>>> {
    assert_eq!(page.descriptor.max_rep_level, 0);

    let state = FixedLenBinaryPageState::try_new(page)?;

    match state {
        FixedLenBinaryPageState::Optional(validity, values) => {
            deserialize_optional(validity, values.map(|x| x.to_vec()))
        }
        FixedLenBinaryPageState::Required(values) => {
            Ok(values.map(|x| x.to_vec()).map(Some).collect())
        }
        FixedLenBinaryPageState::RequiredDictionary(dict) => Ok(dict
            .indexes
            .map(|x| x as usize)
            .map(|x| dict.dict.value(x).to_vec())
            .map(Some)
            .collect()),
        FixedLenBinaryPageState::OptionalDictionary(validity, dict) => {
            let values = dict
                .indexes
                .map(|x| x as usize)
                .map(|x| dict.dict.value(x).to_vec());
            deserialize_optional(validity, values)
        }
    }
}
