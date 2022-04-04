use parquet2::{
    deserialize::{DefLevelsDecoder, HybridDecoderBitmapIter, HybridEncoded},
    encoding::hybrid_rle::{BitmapIter, HybridRleDecoder},
    error::Error,
};

pub fn deserialize_optional<C: Clone, I: Iterator<Item = C>>(
    validity: DefLevelsDecoder,
    values: I,
) -> Result<Vec<Option<C>>, Error> {
    match validity {
        DefLevelsDecoder::Bitmap(bitmap) => deserialize_bitmap(bitmap, values),
        DefLevelsDecoder::Levels(levels, max_level) => {
            deserialize_levels(levels, max_level, values)
        }
    }
}

fn deserialize_bitmap<C: Clone, I: Iterator<Item = C>>(
    validity: HybridDecoderBitmapIter,
    mut values: I,
) -> Result<Vec<Option<C>>, Error> {
    let mut deserialized = Vec::with_capacity(validity.len());

    validity.for_each(|run| match run {
        HybridEncoded::Bitmap(bitmap, length) => {
            BitmapIter::new(bitmap, 0, length)
                .into_iter()
                .for_each(|x| {
                    if x {
                        deserialized.push(values.next())
                    } else {
                        deserialized.push(None)
                    }
                });
        }
        HybridEncoded::Repeated(is_set, length) => {
            if is_set {
                deserialized.extend(values.by_ref().take(length).map(Some))
            } else {
                deserialized.extend(std::iter::repeat(None).take(length))
            }
        }
    });
    Ok(deserialized)
}

fn deserialize_levels<C: Clone, I: Iterator<Item = C>>(
    levels: HybridRleDecoder,
    max: u32,
    mut values: I,
) -> Result<Vec<Option<C>>, Error> {
    Ok(levels
        .into_iter()
        .map(|x| if x == max { values.next() } else { None })
        .collect())
}
