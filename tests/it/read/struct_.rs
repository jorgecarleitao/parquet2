use parquet2::encoding::hybrid_rle::HybridRleDecoder;
use parquet2::error::Error;
use parquet2::page::{split_buffer, DataPage};
use parquet2::read::levels::get_bit_width;

pub fn extend_validity(val: &mut Vec<bool>, page: &DataPage) -> Result<(), Error> {
    let (_, def_levels, _) = split_buffer(page)?;
    let length = page.num_values();

    if page.descriptor.max_def_level == 0 {
        return Ok(());
    }

    let def_level_encoding = (
        &page.definition_level_encoding(),
        page.descriptor.max_def_level,
    );

    let def_levels = HybridRleDecoder::new(def_levels, get_bit_width(def_level_encoding.1), length);

    val.extend(def_levels.map(|x| x != 0));
    Ok(())
}
