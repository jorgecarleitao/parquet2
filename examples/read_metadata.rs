use parquet2::error::Result;

// ANCHOR: deserialize
use parquet2::encoding::Encoding;
use parquet2::metadata::ColumnDescriptor;
use parquet2::page::{split_buffer, DataPage};
use parquet2::schema::types::ParquetType;

fn deserialize(page: &DataPage, descriptor: &ColumnDescriptor) {
    let (_rep_levels, _def_levels, _values_buffer) = split_buffer(page, descriptor);

    if let ParquetType::PrimitiveType {
        physical_type,
        converted_type,
        logical_type,
        ..
    } = descriptor.type_()
    {
        // map the types to your physical typing system (e.g. this usually adds
        // casting, tz conversions, int96 to timestamp)
    } else {
        // column chunks are always primitive types
        unreachable!()
    }

    // finally, decode and deserialize.
    match (&page.encoding(), page.dictionary_page()) {
        (Encoding::PlainDictionary | Encoding::RleDictionary, Some(_dict_page)) => {
            // plain encoded page with a dictionary
            // _dict_page can be downcasted based on the descriptor's physical type
            todo!()
        }
        (Encoding::Plain, None) => {
            // plain encoded page
            todo!()
        }
        _ => todo!(),
    }
}
// ANCHOR_END: deserialize

fn main() -> Result<()> {
    // ANCHOR: metadata
    use parquet2::read::read_metadata;
    let mut reader = std::fs::File::open("path")?;
    let metadata = read_metadata(&mut reader)?;

    println!("{:#?}", metadata);
    // ANCHOR_END: metadata

    // ANCHOR: column_metadata
    let row_group = 0;
    let column = 0;
    let column_metadata = metadata.row_groups[row_group].column(column);
    // ANCHOR_END: column_metadata

    // ANCHOR: statistics
    if let Some(maybe_stats) = column_metadata.statistics() {
        let stats = maybe_stats?;
        use parquet2::schema::types::PhysicalType;
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
            _ => todo!(),
        }
    }
    // ANCHOR_END: statistics

    // ANCHOR: pages
    use parquet2::read::get_page_iterator;
    let pages = get_page_iterator(column_metadata, &mut reader, None, vec![])?;
    // ANCHOR_END: pages

    // ANCHOR: decompress
    let mut decompress_buffer = vec![];
    for maybe_page in pages {
        let page = maybe_page?;
        let page = parquet2::read::decompress(page, &mut decompress_buffer)?;

        let _array = deserialize(&page, column_metadata.descriptor());
    }
    // ANCHOR_END: decompress
    Ok(())
}
