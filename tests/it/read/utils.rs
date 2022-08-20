use parquet2::{
    deserialize::{
        DefLevelsDecoder, FilteredHybridEncoded, HybridDecoderBitmapIter, HybridEncoded,
        OptionalPageValidity,
    },
    encoding::hybrid_rle::BitmapIter,
    error::Error,
};

pub fn deserialize_optional1<C: Clone, I: Iterator<Item = Result<C, Error>>>(
    mut validity: OptionalPageValidity,
    mut values_iter: I,
) -> Result<Vec<Option<C>>, Error> {
    let mut deserialized = Vec::with_capacity(validity.len());

    while let Some(run) = validity.next_limited(usize::MAX) {
        match run {
            FilteredHybridEncoded::Bitmap {
                values,
                offset,
                length,
            } => BitmapIter::new(values, offset, length)
                .into_iter()
                .try_for_each(|x| {
                    if x {
                        deserialized.push(values_iter.next().transpose()?);
                    } else {
                        deserialized.push(None);
                    }
                    Result::<_, Error>::Ok(())
                })?,
            FilteredHybridEncoded::Repeated { is_set, length } => {
                if is_set {
                    deserialized.reserve(length);
                    for x in values_iter.by_ref().take(length) {
                        deserialized.push(Some(x?))
                    }
                } else {
                    deserialized.extend(std::iter::repeat(None).take(length))
                }
            }
            FilteredHybridEncoded::Skipped(_) => continue,
        }
    }
    Ok(deserialized)
}

pub fn deserialize_optional<C: Clone, I: Iterator<Item = Result<C, Error>>>(
    validity: DefLevelsDecoder,
    values: I,
) -> Result<Vec<Option<C>>, Error> {
    match validity {
        DefLevelsDecoder::Bitmap(bitmap) => deserialize_bitmap(bitmap, values),
        DefLevelsDecoder::Levels(levels, max_level) => {
            let levels = levels.map(|def| Ok(def? == max_level));
            deserialize_levels(levels, values)
        }
    }
}

fn deserialize_bitmap<C: Clone, I: Iterator<Item = Result<C, Error>>>(
    mut validity: HybridDecoderBitmapIter,
    mut values: I,
) -> Result<Vec<Option<C>>, Error> {
    let mut deserialized = Vec::with_capacity(validity.len());

    validity.try_for_each(|run| match run? {
        HybridEncoded::Bitmap(bitmap, length) => BitmapIter::new(bitmap, 0, length)
            .into_iter()
            .try_for_each(|x| {
                if x {
                    deserialized.push(values.next().transpose()?);
                } else {
                    deserialized.push(None);
                }
                Result::<_, Error>::Ok(())
            }),
        HybridEncoded::Repeated(is_set, length) => {
            if is_set {
                deserialized.reserve(length);
                for x in values.by_ref().take(length) {
                    deserialized.push(Some(x?))
                }
            } else {
                deserialized.extend(std::iter::repeat(None).take(length))
            }
            Ok(())
        }
    })?;
    Ok(deserialized)
}

pub fn deserialize_levels<
    C: Clone,
    L: Iterator<Item = Result<bool, Error>>,
    I: Iterator<Item = Result<C, Error>>,
>(
    levels: L,
    mut values: I,
) -> Result<Vec<Option<C>>, Error> {
    levels
        .into_iter()
        .map(|maybe_max| {
            maybe_max.and_then(|is_max| {
                if is_max {
                    values.next().transpose()
                } else {
                    Ok(None)
                }
            })
        })
        .collect()
}
