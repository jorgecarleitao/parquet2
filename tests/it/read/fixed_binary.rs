use parquet2::{deserialize::FixedLenBinaryPage, error::Result, page::DataPage};

use super::dictionary::FixedLenByteArrayPageDict;
use super::utils::deserialize_optional;

pub fn page_to_vec(
    page: &DataPage,
    dict: Option<&FixedLenByteArrayPageDict>,
) -> Result<Vec<Option<Vec<u8>>>> {
    assert_eq!(page.descriptor.max_rep_level, 0);

    let state = FixedLenBinaryPage::try_new(page, dict)?;

    match state {
        FixedLenBinaryPage::Optional(validity, values) => {
            deserialize_optional(validity, values.map(|x| Ok(x.to_vec())))
        }
        FixedLenBinaryPage::Required(values) => Ok(values.map(|x| x.to_vec()).map(Some).collect()),
        FixedLenBinaryPage::RequiredDictionary(dict) => dict
            .indexes
            .map(|x| x.and_then(|x| dict.dict.value(x as usize).map(|x| x.to_vec()).map(Some)))
            .collect(),
        FixedLenBinaryPage::OptionalDictionary(validity, dict) => {
            let values = dict
                .indexes
                .map(|x| x.and_then(|x| dict.dict.value(x as usize).map(|x| x.to_vec())));
            deserialize_optional(validity, values)
        }
    }
}
