use parquet_format_async_temp::PageLocation;

use crate::error::Error;

/// An interval
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Interval {
    /// Its start
    pub start: u64,
    /// Its length
    pub length: u64,
}

impl Interval {
    /// Create a new interal
    pub fn new(start: u64, length: u64) -> Self {
        Self { start, length }
    }
}

/// Returns the set of (row) intervals of the pages.
fn compute_page_row_intervals(
    locations: &[PageLocation],
    num_rows: u64,
) -> Result<Vec<Interval>, Error> {
    if locations.is_empty() {
        return Ok(vec![]);
    };

    let last = (|| {
        let first = locations.last().unwrap().first_row_index;
        let start = u64::try_from(first)?;
        let length = num_rows - start;
        Result::<_, Error>::Ok(Interval::new(start, length))
    })();

    let pages_lengths = locations
        .windows(2)
        .map(|x| {
            let start = u64::try_from(x[0].first_row_index)?;
            let length = u64::try_from(x[1].first_row_index - x[0].first_row_index)?;
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
    num_rows: u64,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FilteredPage {
    Select {
        /// Location of the page in the file
        start: u64,
        length: usize,
        /// Location of rows to select in the page
        rows_offset: usize,
        rows_length: usize,
    },
    Skip {
        /// Location of the page in the file
        start: u64,
        length: usize,
        /// number of rows that are skip by skipping this page
        num_rows: usize,
    },
}

impl FilteredPage {
    pub fn start(&self) -> u64 {
        match self {
            Self::Select { start, .. } => *start,
            Self::Skip { start, .. } => *start,
        }
    }
}

fn is_in(probe: Interval, intervals: &[Interval]) -> Option<Interval> {
    intervals.iter().find_map(|interval| {
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
}

/// Given a set of selected [Interval]s of rows and the set of page locations, returns the
pub fn select_pages(
    intervals: &[Interval],
    locations: &[PageLocation],
    num_rows: u64,
) -> Result<Vec<FilteredPage>, Error> {
    let page_intervals = compute_page_row_intervals(locations, num_rows)?;

    page_intervals
        .into_iter()
        .zip(locations.iter())
        .map(|(interval, location)| {
            Ok(if let Some(overlap) = is_in(interval, intervals) {
                FilteredPage::Select {
                    start: location.offset.try_into()?,
                    length: location.compressed_page_size.try_into()?,
                    rows_offset: overlap.start.try_into()?,
                    rows_length: overlap.length.try_into()?,
                }
            } else {
                FilteredPage::Skip {
                    start: location.offset.try_into()?,
                    length: location.compressed_page_size.try_into()?,
                    num_rows: interval.length.try_into()?,
                }
            })
        })
        .collect()
}
