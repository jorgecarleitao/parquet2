pub fn decode(values: &[u8]) -> (u64, usize) {
    let mut result = 0;
    let mut shift = 0;

    let mut consumed = 0;
    for byte in values {
        consumed += 1;
        if shift == 63 && *byte > 1 {
            panic!()
        };

        result |= u64::from(byte & 0x7f) << shift;

        if byte & 0x80 == 0 {
            break;
        }

        shift += 7;
    }
    (result, consumed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_1() {
        let data = vec![0xe5, 0x8e, 0x26, 0xDE, 0xAD, 0xBE, 0xEF];
        let (value, len) = decode(&data);
        assert_eq!(value, 624_485);
        assert_eq!(len, 3);
    }

    #[test]
    fn decode_2() {
        let data = vec![0b00010000, 0b00000001, 0b00000011, 0b00000011];
        let (value, len) = decode(&data);
        assert_eq!(value, 16);
        assert_eq!(len, 1);
    }
}
