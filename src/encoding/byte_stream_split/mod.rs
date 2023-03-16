mod decoder;
mod encoder;

pub use decoder::Decoder;
pub use encoder::encode;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;

    #[test]
    fn basic() -> Result<(), Error> {
        let data = vec![1.0_f32, 2.0_f32, 3.0_f32];
        let mut buffer = vec![];
        encode(&data, &mut buffer);

        let mut decoder = Decoder::<f32>::try_new(&buffer).unwrap();
        let values = decoder.by_ref().collect::<Result<Vec<_>, _>>()?;

        assert_eq!(data, values);

        Ok(())
    }

    #[test]
    fn from_pyarrow_page() -> Result<(), Error> {
        let buffer = vec![0, 205, 0, 205, 0, 0, 204, 0, 204, 0, 128, 140, 0, 140, 128, 255, 191, 0, 63, 127];

        let mut decoder = Decoder::<f32>::try_new(&buffer).unwrap();
        let values = decoder.by_ref().collect::<Result<Vec<_>, _>>()?;

        assert_eq!(values, vec![-f32::INFINITY, -1.1, 0.0, 1.1, f32::INFINITY]);

        Ok(())
    }

    #[test]
    fn fails_for_bad_size() -> Result<(), Error> {
        let buffer = vec![0; 12];

        let result = Decoder::<f64>::try_new(&buffer);
        assert!(result.is_err());

        Ok(())
    }
}
