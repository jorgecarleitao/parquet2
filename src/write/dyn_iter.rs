/// [`DynIter`] is an implementation of a single-threaded, dynamically-typed iterator.
pub struct DynIter<'a, V> {
    iter: Box<dyn Iterator<Item = V> + 'a>,
}

impl<'iter, V> Iterator for DynIter<'iter, V> {
    type Item = V;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl<'iter, V> DynIter<'iter, V> {
    pub fn new<I>(iter: I) -> Self
    where
        I: Iterator<Item = V> + 'iter,
    {
        Self {
            iter: Box::new(iter),
        }
    }
}
