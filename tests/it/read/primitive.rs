use parquet2::{
    deserialize::{
        native_cast, Casted, HybridRleDecoderIter, HybridRleIter, NativePageState, OptionalValues,
        SliceFilteredIter,
    },
    encoding::{hybrid_rle::Decoder, Encoding},
    error::Error,
    page::{split_buffer, DataPage},
    schema::Repetition,
    types::NativeType,
};

use super::utils::deserialize_optional;

/// The deserialization state of a `DataPage` of `Primitive` parquet primitive type
#[derive(Debug)]
pub enum FilteredPageState<'a, T>
where
    T: NativeType,
{
    /// A page of optional values
    Optional(SliceFilteredIter<OptionalValues<T, HybridRleDecoderIter<'a>, Casted<'a, T>>>),
    /// A page of required values
    Required(SliceFilteredIter<Casted<'a, T>>),
}

/// The deserialization state of a `DataPage` of `Primitive` parquet primitive type
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum PageState<'a, T>
where
    T: NativeType,
{
    Nominal(NativePageState<'a, T>),
    Filtered(FilteredPageState<'a, T>),
}

impl<'a, T: NativeType> PageState<'a, T> {
    /// Tries to create [`NativePageState`]
    /// # Error
    /// Errors iff the page is not a `NativePageState`
    pub fn try_new(page: &'a DataPage) -> Result<Self, Error> {
        if let Some(selected_rows) = page.selected_rows() {
            let is_optional =
                page.descriptor.primitive_type.field_info.repetition == Repetition::Optional;

            match (page.encoding(), page.dictionary_page(), is_optional) {
                (Encoding::Plain, _, true) => {
                    let (_, def_levels, _) = split_buffer(page);

                    let validity = HybridRleDecoderIter::new(HybridRleIter::new(
                        Decoder::new(def_levels, 1),
                        page.num_values(),
                    ));
                    let values = native_cast(page)?;

                    // validity and values interleaved.
                    let values = OptionalValues::new(validity, values);

                    let values =
                        SliceFilteredIter::new(values, selected_rows.iter().copied().collect());

                    Ok(Self::Filtered(FilteredPageState::Optional(values)))
                }
                (Encoding::Plain, _, false) => {
                    let values = SliceFilteredIter::new(
                        native_cast(page)?,
                        selected_rows.iter().copied().collect(),
                    );
                    Ok(Self::Filtered(FilteredPageState::Required(values)))
                }
                _ => Err(Error::General(format!(
                    "Viewing page for encoding {:?} for native type {} not supported",
                    page.encoding(),
                    std::any::type_name::<T>()
                ))),
            }
        } else {
            NativePageState::try_new(page).map(Self::Nominal)
        }
    }
}

pub fn page_to_vec<T: NativeType>(page: &DataPage) -> Result<Vec<Option<T>>, Error> {
    assert_eq!(page.descriptor.max_rep_level, 0);
    let state = PageState::<T>::try_new(page)?;

    match state {
        PageState::Nominal(state) => match state {
            NativePageState::Optional(validity, values) => deserialize_optional(validity, values),
            NativePageState::Required(values) => Ok(values.map(Some).collect()),
            NativePageState::RequiredDictionary(dict) => Ok(dict
                .indexes
                .map(|x| x as usize)
                .map(|x| dict.values[x])
                .map(Some)
                .collect()),
            NativePageState::OptionalDictionary(validity, dict) => {
                let values = dict.indexes.map(|x| x as usize).map(|x| dict.values[x]);
                deserialize_optional(validity, values)
            }
        },
        PageState::Filtered(state) => match state {
            FilteredPageState::Optional(values) => Ok(values.collect()),
            FilteredPageState::Required(values) => Ok(values.map(Some).collect()),
        },
    }
}
