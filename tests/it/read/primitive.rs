use parquet2::{
    deserialize::{
        FilteredDecoder, FilteredHybridEncoded, FilteredOptionalPageValidity, FullDecoder,
        NativeDecoder, NativeFilteredValuesDecoder, NativeValuesDecoder,
    },
    encoding::hybrid_rle::BitmapIter,
    error::Error,
    page::DataPage,
    types::NativeType,
};

use super::utils::deserialize_optional;

pub fn page_to_vec<T: NativeType>(
    page: &DataPage,
    dict: Option<&Vec<T>>,
) -> Result<Vec<Option<T>>, Error> {
    assert_eq!(page.descriptor.max_rep_level, 0);

    let values = NativeValuesDecoder::<T, _>::try_new(page, dict)?;
    let decoder = FullDecoder::try_new(page, values)?;
    let state = NativeDecoder::try_new(page, decoder)?;

    match state {
        NativeDecoder::Full(state) => match state {
            FullDecoder::Optional(validity, values) => match values {
                NativeValuesDecoder::Plain(mut values) => {
                    deserialize_optional(validity, values.by_ref().map(Ok))
                }
                NativeValuesDecoder::Dictionary(dict) => {
                    let values = dict.indexes.map(|x| x.map(|x| dict.dict[x as usize]));
                    deserialize_optional(validity, values)
                }
            },
            FullDecoder::Required(values) => match values {
                NativeValuesDecoder::Plain(values) => Ok(values.map(Some).collect()),
                NativeValuesDecoder::Dictionary(dict) => dict
                    .indexes
                    .map(|x| x.map(|x| Some(dict.dict[x as usize])))
                    .collect(),
            },
            FullDecoder::Levels(..) => {
                unreachable!()
            }
        },
        NativeDecoder::Filtered(state) => match state {
            FilteredDecoder::Optional(mut validity, values) => match values {
                NativeValuesDecoder::Plain(mut values) => {
                    extend_filtered(&mut validity, &mut values)
                }
                NativeValuesDecoder::Dictionary(dict) => {
                    let mut values = dict
                        .indexes
                        .map(|x| x.map(|x| dict.dict[x as usize]).unwrap());
                    extend_filtered(&mut validity, &mut values)
                }
            },
            FilteredDecoder::Required(values) => match values {
                NativeFilteredValuesDecoder::Plain(values) => Ok(values.map(Some).collect()),
                NativeFilteredValuesDecoder::Dictionary(dict) => dict
                    .indexes
                    .map(|x| x.map(|x| Some(dict.dict[x as usize])))
                    .collect(),
            },
        },
    }
}

pub(super) fn extend_filtered<T: NativeType, I: Iterator<Item = T>>(
    validity: &mut FilteredOptionalPageValidity,
    mut values_iter: I,
) -> Result<Vec<Option<T>>, Error> {
    let mut pushable = Vec::with_capacity(validity.len());

    let mut remaining = validity.len();
    while remaining > 0 {
        let run = validity.next_limited(remaining);
        let run = if let Some(run) = run { run } else { break }?;

        match run {
            FilteredHybridEncoded::Bitmap {
                values,
                offset,
                length,
            } => {
                // consume `length` items
                let iter = BitmapIter::new(values, offset, length);

                iter.for_each(|x| {
                    if x {
                        pushable.push(values_iter.next());
                    } else {
                        pushable.push(None);
                    }
                });

                remaining -= length;
            }
            FilteredHybridEncoded::Repeated { is_set, length } => {
                if is_set {
                    pushable.extend((0..length).map(|_| values_iter.next()));
                } else {
                    pushable.extend(std::iter::repeat(None).take(length));
                }

                remaining -= length;
            }
            FilteredHybridEncoded::Skipped(valids) => for _ in values_iter.by_ref().take(valids) {},
        };
    }

    Ok(pushable)
}
