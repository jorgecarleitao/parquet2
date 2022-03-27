use parquet2::{
    deserialize::{HybridDecoderBitmapIter, HybridEncoded},
    encoding::hybrid_rle::BitmapIter,
    error::Error,
};

pub struct ValuesDef<T, V, D>
where
    V: Iterator<Item = T>,
    D: Iterator<Item = u32>,
{
    values: V,
    def_levels: D,
    max_def_level: u32,
}

impl<T, V, D> ValuesDef<T, V, D>
where
    V: Iterator<Item = T>,
    D: Iterator<Item = u32>,
{
    pub fn new(values: V, def_levels: D, max_def_level: u32) -> Self {
        Self {
            values,
            def_levels,
            max_def_level,
        }
    }
}

impl<T, V, D> Iterator for ValuesDef<T, V, D>
where
    V: Iterator<Item = T>,
    D: Iterator<Item = u32>,
{
    type Item = Option<T>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.def_levels.next() {
            Some(def) => {
                if def == self.max_def_level {
                    Some(self.values.next())
                } else {
                    Some(None)
                }
            }
            None => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.def_levels.size_hint()
    }
}

pub fn deserialize_optional<C: Copy + Clone, I: Iterator<Item = C>>(
    validity: HybridDecoderBitmapIter,
    mut values: I,
) -> Result<Vec<Option<C>>, Error> {
    let mut deserialized = Vec::with_capacity(validity.len());

    validity.for_each(|run| match run {
        HybridEncoded::Bitmap(bitmap, offset, length) => {
            BitmapIter::new(bitmap, offset, length)
                .into_iter()
                .for_each(|x| {
                    if x {
                        deserialized.push(values.next())
                    } else {
                        deserialized.push(None)
                    }
                });
        }
        HybridEncoded::Repeated(is_set, length) => {
            if is_set {
                deserialized.extend(values.by_ref().take(length).map(Some))
            } else {
                deserialized.extend(std::iter::repeat(None).take(length))
            }
        }
    });
    Ok(deserialized)
}
