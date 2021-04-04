use arrow2::{
    array::PrimitiveArray,
    bitmap::{BitmapIter, MutableBitmap},
    buffer::MutableBuffer,
    datatypes::DataType,
    types::NativeType as ArrowNativeType,
};
use parquet_format::Encoding;

use crate::{
    encoding::get_length,
    errors::{ParquetError, Result},
    metadata::ColumnDescriptor,
    read::{decompress_page, Page, PrimitivePageDict},
};
use crate::{encoding::hybrid_rle, types, types::NativeType};

use super::super::levels;

/// Assumptions: No rep levels
fn read_dict_buffer<'a, T: NativeType + ArrowNativeType>(
    buffer: &'a [u8],
    length: u32,
    dict: &'a PrimitivePageDict<T>,
    _has_validity: bool,
    values: &mut MutableBuffer<T>,
    validity: &mut MutableBitmap,
) {
    let length = length as usize;
    let dict_values = dict.values();

    // split in two buffers: def_levels and data
    let def_level_buffer_length = get_length(buffer) as usize;
    let validity_buffer = &buffer[4..4 + def_level_buffer_length];
    let indices_buffer = &buffer[4 + def_level_buffer_length..];

    // SPEC: Data page format: the bit width used to encode the entry ids stored as 1 byte (max bit width = 32),
    // SPEC: followed by the values encoded using RLE/Bit packed described above (with the given bit width).
    let bit_width = indices_buffer[0];
    let indices_buffer = &indices_buffer[1..];

    let non_null_indices_len = (buffer.len() * 8 / bit_width as usize) as u32;
    let indices = levels::rle_decode(&indices_buffer, bit_width as u32, non_null_indices_len);

    let validity_iterator = hybrid_rle::Decoder::new(&validity_buffer, 1);

    validity.reserve(length);
    values.reserve(length);
    let mut current_len = 0;
    for run in validity_iterator {
        match run {
            hybrid_rle::HybridEncoded::Bitpacked(packed) => {
                // bitpacked has the same representation as arrow's bitmaps and thus we can just copy
                // them. Arrow2 has no API to extend from a slice, so we do it bit by bit
                let len = std::cmp::min(packed.len() * 8, length);
                for is_valid in BitmapIter::new(packed, 0, len) {
                    validity.push(is_valid);
                    let value = if is_valid {
                        current_len += 1;
                        dict_values[indices[current_len - 1] as usize]
                    } else {
                        T::default()
                    };
                    values.push(value);
                }
            }
            _ => todo!(),
        }
    }
}

fn read_buffer<T: NativeType + ArrowNativeType>(
    buffer: &[u8],
    length: u32,
    _has_validity: bool,
    values: &mut MutableBuffer<T>,
    validity: &mut MutableBitmap,
) {
    let length = length as usize;

    // split in two buffers: def_levels and data
    let def_level_buffer_length = get_length(buffer) as usize;
    let validity_buffer = &buffer[4..4 + def_level_buffer_length];
    let values_buffer = &buffer[4 + def_level_buffer_length..];

    let mut chunks = values_buffer[..length as usize * std::mem::size_of::<T>()]
        .chunks_exact(std::mem::size_of::<T>());

    let validity_iterator = hybrid_rle::Decoder::new(&validity_buffer, 1);

    validity.reserve(length);
    values.reserve(length);
    for run in validity_iterator {
        match run {
            hybrid_rle::HybridEncoded::Bitpacked(packed) => {
                // bitpacked has the same representation as arrow's bitmaps and thus we can just copy
                // them. Arrow2 has no API to extend from a slice, so we do it bit by bit
                let len = std::cmp::min(packed.len() * 8, length);
                for is_valid in BitmapIter::new(packed, 0, len) {
                    validity.push(is_valid);
                    let value = if is_valid {
                        types::decode(chunks.next().unwrap())
                    } else {
                        T::default()
                    };
                    values.push(value);
                }
            }
            _ => todo!(),
        }
    }
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
    assert_eq!(descriptor.max_rep_level(), 0);
    assert_eq!(descriptor.max_def_level(), 1);
    let has_validity = descriptor.max_def_level() == 1;
    match page {
        Page::V1(page) => {
            assert_eq!(page.def_level_encoding, Encoding::Rle);
            match (&page.encoding, &page.dictionary_page) {
                (Encoding::PlainDictionary, Some(dict)) => read_dict_buffer::<T>(
                    &page.buf,
                    page.num_values,
                    dict.as_any().downcast_ref().unwrap(),
                    has_validity,
                    values,
                    validity,
                ),
                (Encoding::Plain, None) => {
                    read_buffer::<T>(&page.buf, page.num_values, has_validity, values, validity)
                }
                (encoding, None) => {
                    return Err(general_err!("Encoding {:?} not yet implemented", encoding))
                }
                _ => todo!(),
            }
        }
        Page::V2(_) => todo!(),
    };
    Ok(())
}
