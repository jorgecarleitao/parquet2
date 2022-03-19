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
