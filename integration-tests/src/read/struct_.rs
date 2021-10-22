use parquet::encoding::hybrid_rle::HybridRleDecoder;
use parquet::metadata::ColumnDescriptor;
use parquet::page::{split_buffer, DataPage};
use parquet::read::levels::get_bit_width;

pub fn extend_validity(val: &mut Vec<bool>, page: &DataPage, descriptor: &ColumnDescriptor) {
    let (_, def_levels, _) = split_buffer(page, descriptor);
    let length = page.num_values();

    if descriptor.max_def_level() == 0 {
        return;
    }

    let def_level_encoding = (
        &page.definition_level_encoding(),
        descriptor.max_def_level(),
    );

    let def_levels = HybridRleDecoder::new(def_levels, get_bit_width(def_level_encoding.1), length);

    val.extend(def_levels.map(|x| x != 0));
}
