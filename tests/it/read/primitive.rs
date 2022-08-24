use parquet2::{
    deserialize::{
        FilteredHybridEncoded, FilteredNativePage, FilteredNativePageValues,
        FilteredOptionalPageValidity, NativePage, NativePageValues, NominalNativePage,
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
    let state = NativePage::<T, _>::try_new(page, dict)?;

    match state {
        NativePage::Nominal(state) => match state {
            NominalNativePage::Optional(validity, values) => match values {
                NativePageValues::Plain(mut values) => {
                    deserialize_optional(validity, values.by_ref().map(Ok))
                }
                parquet2::deserialize::NativePageValues::Dictionary(dict) => {
                    let values = dict.indexes.map(|x| x.map(|x| dict.dict[x as usize]));
                    deserialize_optional(validity, values)
                }
            },
            NominalNativePage::Required(values) => match values {
                NativePageValues::Plain(values) => Ok(values.map(Some).collect()),
                NativePageValues::Dictionary(dict) => dict
                    .indexes
                    .map(|x| x.map(|x| Some(dict.dict[x as usize])))
                    .collect(),
            },
            NominalNativePage::Levels(..) => {
                unreachable!()
            }
        },
        NativePage::Filtered(state) => match state {
            FilteredNativePage::Optional(mut validity, values) => match values {
                NativePageValues::Plain(mut values) => extend_filtered(&mut validity, &mut values),
                NativePageValues::Dictionary(dict) => {
                    let mut values = dict
                        .indexes
                        .map(|x| x.map(|x| dict.dict[x as usize]).unwrap());
                    extend_filtered(&mut validity, &mut values)
                }
            },
            FilteredNativePage::Required(values) => match values {
                FilteredNativePageValues::Plain(values) => Ok(values.map(Some).collect()),
                FilteredNativePageValues::Dictionary(dict) => dict
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
