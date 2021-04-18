use super::uleb128;

pub fn decode(values: &[u8]) -> (i64, usize) {
    let (u, consumed) = uleb128::decode(values);
    ((u >> 1) as i64 ^ -((u & 1) as i64), consumed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic() {
        // see e.g. https://stackoverflow.com/a/2211086/931303
        let cases = vec![
            (0u8, 0i64),
            (1, -1),
            (2, 1),
            (3, -2),
            (4, 2),
            (5, -3),
            (6, 3),
            (7, -4),
            (8, 4),
            (9, -5),
        ];
        for (data, expected) in cases {
            let (result, _) = decode(&[data]);
            assert_eq!(result, expected)
        }
    }
}
