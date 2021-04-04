use std::convert::{TryFrom, TryInto};

/// A physical native representation of a Parquet fixed-sized type.
pub trait NativeType: Sized + Copy + std::fmt::Debug + 'static {
    type Bytes: AsRef<[u8]> + for<'a> TryFrom<&'a [u8]>;

    fn to_le_bytes(&self) -> Self::Bytes;

    fn from_le_bytes(bytes: Self::Bytes) -> Self;

    fn to_be_bytes(&self) -> Self::Bytes;

    fn from_be_bytes(bytes: Self::Bytes) -> Self;
}

macro_rules! native {
    ($type:ty) => {
        impl NativeType for $type {
            type Bytes = [u8; std::mem::size_of::<Self>()];
            #[inline]
            fn to_le_bytes(&self) -> Self::Bytes {
                Self::to_le_bytes(*self)
            }

            #[inline]
            fn to_be_bytes(&self) -> Self::Bytes {
                Self::to_be_bytes(*self)
            }

            #[inline]
            fn from_le_bytes(bytes: Self::Bytes) -> Self {
                Self::from_le_bytes(bytes)
            }

            #[inline]
            fn from_be_bytes(bytes: Self::Bytes) -> Self {
                Self::from_be_bytes(bytes)
            }
        }
    };
}

native!(i32);
native!(i64);
native!(f32);
native!(f64);

impl NativeType for [u32; 3] {
    type Bytes = [u8; std::mem::size_of::<Self>()];
    #[inline]
    fn to_le_bytes(&self) -> Self::Bytes {
        todo!()
    }

    #[inline]
    fn to_be_bytes(&self) -> Self::Bytes {
        todo!()
    }

    #[inline]
    fn from_le_bytes(bytes: Self::Bytes) -> Self {
        let mut first = [0; 4];
        first[0] = bytes[0];
        first[1] = bytes[1];
        first[2] = bytes[2];
        first[3] = bytes[3];
        let mut second = [0; 4];
        second[0] = bytes[4];
        second[1] = bytes[5];
        second[2] = bytes[6];
        second[3] = bytes[7];
        let mut third = [0; 4];
        third[0] = bytes[8];
        third[1] = bytes[9];
        third[2] = bytes[10];
        third[3] = bytes[11];
        [
            u32::from_le_bytes(first),
            u32::from_le_bytes(second),
            u32::from_le_bytes(third),
        ]
    }

    #[inline]
    fn from_be_bytes(_: Self::Bytes) -> Self {
        todo!()
    }
}

pub fn int96_to_i64(value: [u32; 3]) -> i64 {
    const JULIAN_DAY_OF_EPOCH: i64 = 2_440_588;
    const SECONDS_PER_DAY: i64 = 86_400;
    const MILLIS_PER_SECOND: i64 = 1_000;

    let day = value[2] as i64;
    let nanoseconds = ((value[1] as i64) << 32) + value[0] as i64;
    let seconds = (day - JULIAN_DAY_OF_EPOCH) * SECONDS_PER_DAY;

    seconds * MILLIS_PER_SECOND + nanoseconds / 1_000_000
}

#[inline]
pub fn decode<T: NativeType>(chunk: &[u8]) -> T {
    let chunk: <T as NativeType>::Bytes = match chunk.try_into() {
        Ok(v) => v,
        Err(_) => panic!(),
    };
    T::from_le_bytes(chunk)
}
