/// [`DynIter`] is an implementation of a single-threaded, dynamically-typed iterator.
pub struct DynIter<V> {
    iter: Box<dyn Iterator<Item = V>>,
}

impl<V> Iterator for DynIter<V> {
    type Item = V;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl<V> DynIter<V> {
    pub fn new<I>(iter: I) -> Self
    where
        I: Iterator<Item = V> + 'static,
    {
        Self {
            iter: Box::new(iter),
        }
    }
}
