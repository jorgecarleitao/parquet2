mod decoder;
mod encoder;

pub use decoder::Decoder;
pub use encoder::encode;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic() {
        let data = vec![1i32, 3, 1, 2, 3];

        let mut buffer = vec![];
        encode(data.clone().into_iter(), &mut buffer);
        let iter = Decoder::new(&buffer);

        let result = iter.collect::<Vec<_>>();
        assert_eq!(result, data);
    }

    #[test]
    fn negative_value() {
        let data = vec![1i32, 3, -1, 2, 3];

        let mut buffer = vec![];
        encode(data.clone().into_iter(), &mut buffer);
        let iter = Decoder::new(&buffer);

        let result = iter.collect::<Vec<_>>();
        assert_eq!(result, data);
    }

    #[test]
    fn more_than_one_block() {
        let mut data = vec![1i32, 3, -1, 2, 3, 10, 1];
        for x in 0..128 {
            data.push(x - 10)
        }

        let mut buffer = vec![];
        encode(data.clone().into_iter(), &mut buffer);
        let iter = Decoder::new(&buffer);

        let result = iter.collect::<Vec<_>>();
        assert_eq!(result, data);
    }

    #[test]
    fn test_another() {
        let data = vec![2, 3, 1, 2, 1];

        let mut buffer = vec![];
        encode(data.clone().into_iter(), &mut buffer);
        let len = buffer.len();
        let mut iter = Decoder::new(&buffer);

        let result = iter.by_ref().collect::<Vec<_>>();
        assert_eq!(result, data);

        assert_eq!(iter.consumed_bytes(), len);
    }
}
