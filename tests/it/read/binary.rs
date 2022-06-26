use parquet2::{deserialize::BinaryPageState, error::Result, page::DataPage};

use crate::read::utils::deserialize_optional;

pub fn page_to_vec(page: &DataPage) -> Result<Vec<Option<Vec<u8>>>> {
    assert_eq!(page.descriptor.max_rep_level, 0);

    let state = BinaryPageState::try_new(page)?;

    match state {
        BinaryPageState::Optional(validity, values) => {
            deserialize_optional(validity, values.map(|x| x.map(|x| x.to_vec())))
        }
        BinaryPageState::Required(values) => values
            .map(|x| x.map(|x| x.to_vec()))
            .map(Some)
            .map(|x| x.transpose())
            .collect(),
        BinaryPageState::RequiredDictionary(dict) => dict
            .indexes
            .map(|x| x as usize)
            .map(|x| dict.dict.value(x).map(|x| x.to_vec()).map(Some))
            .collect(),
        BinaryPageState::OptionalDictionary(validity, dict) => {
            let values = dict
                .indexes
                .map(|x| x as usize)
                .map(|x| dict.dict.value(x).map(|x| x.to_vec()));
            deserialize_optional(validity, values)
        }
    }
}
