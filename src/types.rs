/// A physical native representation of a Parquet fixed-sized type.
pub trait NativeType: Sized + Copy + std::fmt::Debug {
    type Bytes: AsRef<[u8]>;

    fn to_le_bytes(&self) -> Self::Bytes;

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
// todo: add int96 = [u32; 3]
