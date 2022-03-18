use parquet_format_async_temp::PageLocation;

use crate::error::ParquetError;

use super::index::PageIndex;

/// Returns the set of (row) intervals of the pages.
fn compute_row_page_intervals(
    locations: &[PageLocation],
    num_rows: u64,
) -> Result<Vec<(u64, u64)>, ParquetError> {
    if locations.is_empty() {
        return Ok(vec![]);
    };

    let last = (|| {
        let first = locations.last().unwrap().first_row_index;
        let start = u64::try_from(first)?;
        let length = num_rows - start;
        Result::<_, ParquetError>::Ok((start, length))
    })();

    let pages_lengths = locations
        .windows(2)
        .map(|x| {
            let start = u64::try_from(x[0].first_row_index)?;
            let length = u64::try_from(x[1].first_row_index - x[0].first_row_index)?;
            Ok((start, length))
        })
        .chain(std::iter::once(last));
    pages_lengths.collect()
}

/// Returns the set of intervals `(start, len)` containing all the
/// selected rows (for a given column)
pub fn compute_rows<'a, T>(
    index: &'a [PageIndex<T>],
    locations: &[PageLocation],
    num_rows: u64,
    selector: &dyn Fn(&'a PageIndex<T>) -> bool,
) -> Result<Vec<(u64, u64)>, ParquetError> {
    let page_intervals = compute_row_page_intervals(locations, num_rows)?;

    Ok(index
        .iter()
        .zip(page_intervals.iter().copied())
        .filter_map(|(index, page)| {
            let is_selected = selector(index);
            if is_selected {
                Some(page)
            } else {
                None
            }
        })
        .collect())
}
