use std::collections::VecDeque;

use parquet_format_async_temp::PageLocation;

use crate::error::Error;

/// An interval
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Interval {
    /// Its start
    pub start: usize,
    /// Its length
    pub length: usize,
}

impl Interval {
    /// Create a new interal
    pub fn new(start: usize, length: usize) -> Self {
        Self { start, length }
    }
}

/// Returns the set of (row) intervals of the pages.
fn compute_page_row_intervals(
    locations: &[PageLocation],
    num_rows: usize,
) -> Result<Vec<Interval>, Error> {
    if locations.is_empty() {
        return Ok(vec![]);
    };

    let last = (|| {
        let start: usize = locations.last().unwrap().first_row_index.try_into()?;
        let length = num_rows - start;
        Result::<_, Error>::Ok(Interval::new(start, length))
    })();

    let pages_lengths = locations
        .windows(2)
        .map(|x| {
            let start = usize::try_from(x[0].first_row_index)?;
            let length = usize::try_from(x[1].first_row_index - x[0].first_row_index)?;
            Ok(Interval::new(start, length))
        })
        .chain(std::iter::once(last));
    pages_lengths.collect()
}

/// Returns the set of intervals `(start, len)` containing all the
/// selected rows (for a given column)
pub fn compute_rows(
    selected: &[bool],
    locations: &[PageLocation],
    num_rows: usize,
) -> Result<Vec<Interval>, Error> {
    let page_intervals = compute_page_row_intervals(locations, num_rows)?;

    Ok(selected
        .iter()
        .zip(page_intervals.iter().copied())
        .filter_map(
            |(&is_selected, page)| {
                if is_selected {
                    Some(page)
                } else {
                    None
                }
            },
        )
        .collect())
}

/// An enum describing a page that was either selected in a filter pushdown or skipped
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FilteredPage {
    /// Location of the page in the file
    pub start: u64,
    pub length: usize,
    /// rows to select from the page
    pub selected_rows: Vec<Interval>,
    pub num_rows: usize,
}

fn is_in(probe: Interval, intervals: &[Interval]) -> Vec<Interval> {
    intervals
        .iter()
        .filter_map(|interval| {
            let interval_end = interval.start + interval.length;
            let probe_end = probe.start + probe.length;
            let overlaps = (probe.start < interval_end) && (probe_end > interval.start);
            if overlaps {
                let start = interval.start.max(probe.start);
                let end = interval_end.min(probe_end);
                Some(Interval::new(start - probe.start, end - start))
            } else {
                None
            }
        })
        .collect()
}

/// Given a set of selected [Interval]s of rows and the set of [`PageLocation`], returns the
/// a set of [`FilteredPage`] with the same number of items as `locations`.
pub fn select_pages(
    intervals: &[Interval],
    locations: &[PageLocation],
    num_rows: usize,
) -> Result<VecDeque<FilteredPage>, Error> {
    let page_intervals = compute_page_row_intervals(locations, num_rows)?;

    page_intervals
        .into_iter()
        .zip(locations.iter())
        .map(|(interval, location)| {
            let selected_rows = is_in(interval, intervals);
            Ok(FilteredPage {
                start: location.offset.try_into()?,
                length: location.compressed_page_size.try_into()?,
                selected_rows,
                num_rows: interval.length,
            })
        })
        .collect()
}
