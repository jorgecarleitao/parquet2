use std::convert::TryInto;

use arrow2::{
    array::PrimitiveArray, bitmap::MutableBitmap, buffer::MutableBuffer, datatypes::DataType,
    types::NativeType as ArrowNativeType,
};
use parquet_format::Encoding;

use crate::encoding::{bitpacking, uleb128};
use crate::types::NativeType;
use crate::{
    errors::{ParquetError, Result},
    metadata::ColumnDescriptor,
    read::{decompress_page, Page, PrimitivePageDict},
};

use super::super::levels;
use super::super::utils::ValuesDef;

fn read_dict_buffer<'a, T: NativeType + ArrowNativeType>(
    buffer: &'a [u8],
    length: u32,
    dict: &'a PrimitivePageDict<T>,
    rep_level_encoding: (&Encoding, i16),
    def_level_encoding: (&Encoding, i16),
    values: &mut MutableBuffer<T>,
    validity: &mut MutableBitmap,
) {
    let dict_values = dict.values();

    // skip bytes from levels
    let offset = levels::needed_bytes(buffer, length, def_level_encoding);
    let def_levels = levels::decode(buffer, length, def_level_encoding);
    let buffer = &buffer[offset..];
    let offset = levels::needed_bytes(buffer, length, rep_level_encoding);
    let buffer = &buffer[offset..];

    let bit_width = buffer[0];
    let buffer = &buffer[1..];

    let (_, consumed) = uleb128::decode(&buffer);
    let buffer = &buffer[consumed..];

    let mut indices = vec![0; bitpacking::required_capacity(length)];
    bitpacking::decode(&buffer, bit_width, &mut indices);
    indices.truncate(length as usize);

    let iterator = ValuesDef::new(
        indices.into_iter(),
        def_levels.into_iter(),
        def_level_encoding.1 as u32,
    );

    let iterator = iterator.map(|maybe_id| {
        validity.push(maybe_id.is_some());
        match maybe_id {
            Some(id) => dict_values[id as usize],
            None => T::default(),
        }
    });

    unsafe { values.extend_from_trusted_len_iter(iterator) }
}

fn read_buffer<T: NativeType + ArrowNativeType>(
    buffer: &[u8],
    length: u32,
    rep_level_encoding: (&Encoding, i16),
    def_level_encoding: (&Encoding, i16),
    values: &mut MutableBuffer<T>,
    validity: &mut MutableBitmap,
) {
    // skip bytes from levels
    let offset = levels::needed_bytes(buffer, length, def_level_encoding);
    let def_levels = levels::decode(buffer, length, def_level_encoding);
    let buffer = &buffer[offset..];
    let offset = levels::needed_bytes(buffer, length, rep_level_encoding);
    let buffer = &buffer[offset..];

    let chunks =
        buffer[..length as usize * std::mem::size_of::<T>()].chunks_exact(std::mem::size_of::<T>());
    assert_eq!(chunks.remainder().len(), 0);

    let iterator = ValuesDef::new(chunks, def_levels.into_iter(), def_level_encoding.1 as u32);

    let iterator = iterator.map(|maybe_chunk| {
        validity.push(maybe_chunk.is_some());
        match maybe_chunk {
            Some(chunk) => {
                let chunk: <T as NativeType>::Bytes = match chunk.try_into() {
                    Ok(v) => v,
                    Err(_) => panic!(),
                };
                T::from_le_bytes(chunk)
            }
            None => T::default(),
        }
    });
    unsafe { values.extend_from_trusted_len_iter(iterator) }
}

pub fn iter_to_array<T, I>(
    mut iter: I,
    descriptor: &ColumnDescriptor,
    data_type: DataType,
) -> Result<PrimitiveArray<T>>
where
    T: NativeType + ArrowNativeType,
    I: Iterator<Item = Result<Page>>,
{
    // todo: push metadata from the file to get this capacity
    let capacity = 0;
    let mut values = MutableBuffer::<T>::with_capacity(capacity);
    let mut validity = MutableBitmap::with_capacity(capacity);
    iter.try_for_each(|page| extend_from_page(page?, &descriptor, &mut values, &mut validity))?;

    Ok(PrimitiveArray::from_data(
        data_type,
        values.into(),
        validity.into(),
    ))
}

fn extend_from_page<T: NativeType + ArrowNativeType>(
    page: Page,
    descriptor: &ColumnDescriptor,
    values: &mut MutableBuffer<T>,
    validity: &mut MutableBitmap,
) -> Result<()> {
    let page = decompress_page(page)?;
    match page {
        Page::V1(page) => match (&page.encoding, &page.dictionary_page) {
            (Encoding::PlainDictionary, Some(dict)) => read_dict_buffer::<T>(
                &page.buf,
                page.num_values,
                dict.as_any().downcast_ref().unwrap(),
                (&page.rep_level_encoding, descriptor.max_rep_level()),
                (&page.def_level_encoding, descriptor.max_def_level()),
                values,
                validity,
            ),
            (Encoding::Plain, None) => read_buffer::<T>(
                &page.buf,
                page.num_values,
                (&page.rep_level_encoding, descriptor.max_rep_level()),
                (&page.def_level_encoding, descriptor.max_def_level()),
                values,
                validity,
            ),
            (encoding, None) => {
                return Err(general_err!("Encoding {:?} not yet implemented", encoding))
            }
            _ => todo!(),
        },
        Page::V2(_) => todo!(),
    };
    Ok(())
}
