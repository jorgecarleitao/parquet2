use parquet2::bloom_filter;
use parquet2::error::Error;

// ANCHOR: deserialize
use parquet2::deserialize::{FullDecoder, NativeDecoder, NativeValuesDecoder};
use parquet2::page::{DataPage, DictPage, Page};
use parquet2::schema::types::PhysicalType;
use parquet2::types::decode;

enum Dict {
    I32(Vec<i32>),
    _I64(Vec<i64>),
}

fn deserialize_dict_i32(page: &DictPage) -> Vec<i32> {
    page.buffer
        .chunks_exact(std::mem::size_of::<i32>())
        .map(decode::<i32>)
        .collect()
}

fn deserialize_dict(page: &DictPage, physical_type: PhysicalType) -> Result<Dict, Error> {
    match physical_type {
        PhysicalType::Int32 => Ok(Dict::I32(deserialize_dict_i32(page))),
        _ => todo!(),
    }
}

/// Deserializes the [`DataPage`] and optional [`Dict`] into `Vec<Option<i32>>` by handling
/// single
fn deserialize(page: &DataPage, dict: Option<&Dict>) -> Result<Vec<Option<i32>>, Error> {
    match page.descriptor.primitive_type.physical_type {
        PhysicalType::Int32 => {
            let dict = dict.map(|dict| {
                if let Dict::I32(dict) = dict {
                    dict
                } else {
                    unreachable!()
                }
            });
            let values = NativeValuesDecoder::<i32, Vec<i32>>::try_new(page, dict)?;
            let decoder = FullDecoder::try_new(page, values)?;
            let decoder = NativeDecoder::try_new(page, decoder)?;
            // page is an enum comprising of the different possible encodings:
            match decoder {
                NativeDecoder::Full(values) => match values {
                    FullDecoder::Optional(_, _) => todo!("optional pages"),
                    FullDecoder::Required(values) => match values {
                        NativeValuesDecoder::Plain(values) => Ok(values.map(Some).collect()),
                        NativeValuesDecoder::Dictionary(dict) => dict
                            .indexes
                            .map(|x| x.map(|x| Some(dict.dict[x as usize])))
                            .collect(),
                    },
                    FullDecoder::Levels(_, _, _) => todo!("nested pages"),
                },
                NativeDecoder::Filtered(_) => todo!("Filtered page"),
            }
        }
        PhysicalType::Int64 => todo!("int64"),
        _ => todo!("Other physical types"),
    }
}
// ANCHOR_END: deserialize

fn main() -> Result<(), Error> {
    // ANCHOR: metadata
    use std::env;
    let args: Vec<String> = env::args().collect();

    let path = &args[1];

    use parquet2::read::read_metadata;
    let mut reader = std::fs::File::open(path)?;
    let metadata = read_metadata(&mut reader)?;

    println!("{:#?}", metadata);
    // ANCHOR_END: metadata

    // ANCHOR: column_metadata
    let row_group = 0;
    let column = 0;
    let columns = metadata.row_groups[row_group].columns();
    let column_metadata = &columns[column];
    // ANCHOR_END: column_metadata

    // ANCHOR: column_index
    // read the column indexes of every column
    use parquet2::read;
    let index = read::read_columns_indexes(&mut reader, columns)?;
    // these are the minimum and maximum within each page, which can be used
    // to skip pages.
    println!("{index:?}");

    // read the offset indexes containing page locations of every column
    let pages = read::read_pages_locations(&mut reader, columns)?;
    println!("{pages:?}");
    // ANCHOR_END: column_index

    // ANCHOR: statistics
    if let Some(maybe_stats) = column_metadata.statistics() {
        let stats = maybe_stats?;
        use parquet2::statistics::PrimitiveStatistics;
        match stats.physical_type() {
            PhysicalType::Int32 => {
                let stats = stats
                    .as_any()
                    .downcast_ref::<PrimitiveStatistics<i32>>()
                    .unwrap();
                let _min: i32 = stats.min_value.unwrap();
                let _max: i32 = stats.max_value.unwrap();
                let _null_count: i64 = stats.null_count.unwrap();
            }
            PhysicalType::Int64 => {
                let stats = stats
                    .as_any()
                    .downcast_ref::<PrimitiveStatistics<i64>>()
                    .unwrap();
                let _min: i64 = stats.min_value.unwrap();
                let _max: i64 = stats.max_value.unwrap();
                let _null_count: i64 = stats.null_count.unwrap();
            }
            _ => todo!(),
        }
    }
    // ANCHOR_END: statistics

    // ANCHOR: bloom_filter
    let mut bitset = vec![];
    bloom_filter::read(column_metadata, &mut reader, &mut bitset)?;
    if !bitset.is_empty() {
        // there is a bitset, we can use it to check if elements are in the column chunk

        // assume that our query engine had resulted in the filter `"column 0" == 100i64` (it also verified that column 0 is i64 in parquet)
        let value = 100i64;

        // we hash this value
        let hash = bloom_filter::hash_native(value);

        // and check if the hash is in the bitset.
        let _in_set = bloom_filter::is_in_set(&bitset, hash);
        // if not (false), we could skip this entire row group, because no item hits the filter
        // this can naturally be applied over multiple columns.
        // if yes (true), the item _may_ be in the row group, and we usually can't skip it.
    }
    // ANCHOR_END: bloom_filter

    // ANCHOR: pages
    use parquet2::read::get_page_iterator;
    let pages = get_page_iterator(column_metadata, &mut reader, None, vec![], 1024 * 1024)?;
    let type_ = column_metadata
        .descriptor()
        .descriptor
        .primitive_type
        .physical_type;
    // ANCHOR_END: pages

    // ANCHOR: decompress
    let mut decompress_buffer = vec![];
    let mut dict = None;
    for maybe_page in pages {
        let page = maybe_page?;
        let page = parquet2::read::decompress(page, &mut decompress_buffer)?;

        match page {
            Page::Dict(page) => {
                // the first page may be a dictionary page, which needs to be deserialized
                dict = Some(deserialize_dict(&page, type_)?);
            }
            Page::Data(page) => {
                let _array = deserialize(&page, dict.as_ref())?;
            }
        }
    }
    // ANCHOR_END: decompress

    Ok(())
}
