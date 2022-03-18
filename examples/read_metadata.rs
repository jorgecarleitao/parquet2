use parquet2::bloom_filter;
use parquet2::error::Result;
use parquet2::indexes;

// ANCHOR: deserialize
use parquet2::encoding::Encoding;
use parquet2::page::{split_buffer, DataPage};
use parquet2::schema::types::PhysicalType;

fn deserialize(page: &DataPage) {
    // split the data buffer in repetition levels, definition levels and values
    let (_rep_levels, _def_levels, _values_buffer) = split_buffer(page);

    // decode and deserialize.
    match (
        page.descriptor.primitive_type.physical_type,
        page.encoding(),
        page.dictionary_page(),
    ) {
        (
            PhysicalType::Int32,
            Encoding::PlainDictionary | Encoding::RleDictionary,
            Some(_dict_page),
        ) => {
            // plain encoded page with a dictionary
            // _dict_page can be downcasted based on the descriptor's physical type
            todo!()
        }
        (PhysicalType::Int32, Encoding::Plain, None) => {
            // plain encoded page
            todo!()
        }
        _ => todo!(),
    }
}
// ANCHOR_END: deserialize

fn main() -> Result<()> {
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
    let column_metadata = metadata.row_groups[row_group].column(column);
    // ANCHOR_END: column_metadata

    // ANCHOR: column_index
    // read the column index
    let index = indexes::read_column(&mut reader, column_metadata)?;
    if let Some(index) = index {
        // these are the minimum and maximum within each page, which can be used
        // to skip pages.
        println!("{index:?}");
    }

    // read the offset index containing page locations
    let maybe_pages = indexes::read_page_locations(&mut reader, column_metadata.column_chunk())?;
    if let Some(pages) = maybe_pages {
        // there are page locations in the file
        println!("{pages:?}");
    }
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
    let pages = get_page_iterator(column_metadata, &mut reader, None, vec![])?;
    // ANCHOR_END: pages

    // ANCHOR: decompress
    let mut decompress_buffer = vec![];
    for maybe_page in pages {
        let page = maybe_page?;
        let page = parquet2::read::decompress(page, &mut decompress_buffer)?;

        let _array = deserialize(&page);
    }
    // ANCHOR_END: decompress

    Ok(())
}
