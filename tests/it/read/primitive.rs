use parquet2::{deserialize::NativePageState, error::Error, page::DataPage, types::NativeType};

use super::utils::deserialize_optional;

pub fn page_to_vec<T: NativeType>(page: &DataPage) -> Result<Vec<Option<T>>, Error> {
    assert_eq!(page.descriptor.max_rep_level, 0);
    let state = NativePageState::<T>::try_new(page)?;

    match state {
        NativePageState::Optional(validity, values) => deserialize_optional(validity, values),
        NativePageState::Required(values) => Ok(values.map(Some).collect()),
        NativePageState::RequiredDictionary(dict) => Ok(dict
            .indexes
            .map(|x| x as usize)
            .map(|x| dict.values[x])
            .map(Some)
            .collect()),
        NativePageState::OptionalDictionary(validity, dict) => {
            let values = dict.indexes.map(|x| x as usize).map(|x| dict.values[x]);
            deserialize_optional(validity, values)
        }
    }
}
