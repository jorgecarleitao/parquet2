use parquet2::{
    deserialize::{FilteredHybridEncoded, OptionalPageValidity},
    encoding::hybrid_rle::BitmapIter,
    error::Error,
};

pub fn deserialize_optional<C: Clone, I: Iterator<Item = Result<C, Error>>>(
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
