use crate::FallibleStreamingIterator;

/// [`DynIter`] is an implementation of a single-threaded, dynamically-typed iterator.
pub struct DynIter<'a, V> {
    iter: Box<dyn Iterator<Item = V> + 'a>,
}

impl<'a, V> Iterator for DynIter<'a, V> {
    type Item = V;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl<'a, V> DynIter<'a, V> {
    pub fn new<I>(iter: I) -> Self
    where
        I: Iterator<Item = V> + 'a,
    {
        Self {
            iter: Box::new(iter),
        }
    }
}

/// Dynamically-typed [`FallibleStreamingIterator`].
pub struct DynStreamingIterator<'a, V, E> {
    iter: Box<dyn FallibleStreamingIterator<Item = V, Error = E> + 'a>,
}

impl<'a, V, E> FallibleStreamingIterator for DynStreamingIterator<'a, V, E> {
    type Item = V;
    type Error = E;

    fn advance(&mut self) -> Result<(), Self::Error> {
        self.iter.advance()
    }

    fn get(&self) -> Option<&Self::Item> {
        self.iter.get()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl<'a, V, E> DynStreamingIterator<'a, V, E> {
    pub fn new<I>(iter: I) -> Self
    where
        I: FallibleStreamingIterator<Item = V, Error = E> + 'a,
    {
        Self {
            iter: Box::new(iter),
        }
    }
}
