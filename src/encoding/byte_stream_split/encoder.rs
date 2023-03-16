use crate::types::NativeType;

/// Encodes an array of NativeType according to BYTE_STREAM_SPLIT
pub fn encode<T: NativeType>(data: &[T], buffer: &mut Vec<u8>) {
    let element_size = std::mem::size_of::<T>();
    let num_elements = data.len();
    let total_length = element_size * num_elements;
    buffer.resize(total_length, 0);

    for (i, v) in data.iter().enumerate() {
        let value_bytes = v.to_le_bytes();
        let value_bytes_ref = value_bytes.as_ref();
        for n in 0..element_size {
            buffer[(num_elements * n) + i] = value_bytes_ref[n];
        }
    }
}
