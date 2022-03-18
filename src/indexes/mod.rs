mod index;
mod intervals;
mod read;

pub use self::index::{ByteIndex, FixedLenByteIndex, Index, NativeIndex, PageIndex};
pub use intervals::compute_rows;
pub use read::*;

#[cfg(test)]
mod tests {
    use parquet_format_async_temp::PageLocation;

    use super::*;

    #[test]
    fn test_basic() {
        let index = NativeIndex {
            indexes: vec![PageIndex {
                min: Some(0i32),
                max: Some(10),
                null_count: Some(0),
            }],
            boundary_order: Default::default(),
        };
        let locations = &[PageLocation {
            offset: 100,
            compressed_page_size: 10,
            first_row_index: 0,
        }];
        let num_rows = 10;

        let selector = |_| true;

        let row_intervals = compute_rows(&index.indexes, locations, num_rows, &selector).unwrap();
        assert_eq!(row_intervals, vec![(0, 10)])
    }

    #[test]
    fn test_multiple() {
        // two pages
        let index = ByteIndex {
            indexes: vec![
                PageIndex {
                    min: Some(vec![0]),
                    max: Some(vec![8, 9]),
                    null_count: Some(0),
                },
                PageIndex {
                    min: Some(vec![20]),
                    max: Some(vec![98, 99]),
                    null_count: Some(0),
                },
            ],
            boundary_order: Default::default(),
        };
        let locations = &[
            PageLocation {
                offset: 100,
                compressed_page_size: 10,
                first_row_index: 0,
            },
            PageLocation {
                offset: 110,
                compressed_page_size: 20,
                first_row_index: 5,
            },
        ];
        let num_rows = 10;

        // filter of the form `x > "a"`
        let selector = |page: &PageIndex<Vec<u8>>| {
            page.max
                .as_ref()
                .map(|x| x.as_slice() > &[97])
                .unwrap_or(false) // no max is present => all nulls => not selected
        };

        let row_intervals = compute_rows(&index.indexes, locations, num_rows, &selector).unwrap();
        assert_eq!(row_intervals, vec![(5, 5)])
    }
}
