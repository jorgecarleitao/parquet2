use parquet2::encoding::hybrid_rle::HybridRleDecoder;
use parquet2::page::split_buffer;
use parquet2::read::levels::get_bit_width;
use parquet2::{deserialize::BinaryPageState, error::Result, page::DataPage};

use super::dictionary::BinaryPageDict;
use super::utils::{deserialize_levels, deserialize_optional1};

pub fn page_to_vec(page: &DataPage, dict: Option<&BinaryPageDict>) -> Result<Vec<Option<Vec<u8>>>> {
    assert_eq!(page.descriptor.max_rep_level, 0);

    let state = BinaryPageState::try_new(page, dict)?;

    if page.descriptor.max_def_level <= 1 {
        let state = BinaryPageState::try_new(page, dict)?;
        match state {
            BinaryPageState::Optional(validity, values) => {
                deserialize_optional1(validity, values.map(|x| x.map(|x| x.to_vec())))
            }
            BinaryPageState::Required(values) => values
                .map(|x| x.map(|x| x.to_vec()))
                .map(Some)
                .map(|x| x.transpose())
                .collect(),
            BinaryPageState::RequiredDictionary(dict) => dict
                .indexes
                .map(|x| x.and_then(|x| dict.dict.value(x as usize).map(|x| x.to_vec()).map(Some)))
                .collect(),
            BinaryPageState::OptionalDictionary(validity, dict) => {
                let values = dict
                    .indexes
                    .map(|x| x.and_then(|x| dict.dict.value(x as usize).map(|x| x.to_vec())));
                deserialize_optional1(validity, values)
            }
            _ => todo!(),
        }
    } else {
        let (_, def_levels, _) = split_buffer(page)?;
        let max = page.descriptor.max_def_level as u32;
        let validity =
            HybridRleDecoder::try_new(def_levels, get_bit_width(max as i16), page.num_values())?;
        let validity = validity.map(|def| Ok(def? == max));

        match state {
            BinaryPageState::Optional(_, values) => {
                deserialize_levels(validity, values.map(|x| x.map(|x| x.to_vec())))
            }
            BinaryPageState::Required(values) => values
                .map(|x| x.map(|x| x.to_vec()))
                .map(Some)
                .map(|x| x.transpose())
                .collect(),
            BinaryPageState::RequiredDictionary(dict) => dict
                .indexes
                .map(|x| x.and_then(|x| dict.dict.value(x as usize).map(|x| x.to_vec()).map(Some)))
                .collect(),
            BinaryPageState::OptionalDictionary(_, dict) => {
                let values = dict
                    .indexes
                    .map(|x| x.and_then(|x| dict.dict.value(x as usize).map(|x| x.to_vec())));
                deserialize_levels(validity, values)
            }
            _ => todo!(),
        }
    }
}
