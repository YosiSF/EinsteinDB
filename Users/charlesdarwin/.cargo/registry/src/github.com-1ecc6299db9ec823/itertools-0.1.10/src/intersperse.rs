/// Intersperse a value between each element of an iterator.
/// 
/// The iterator is returned with the value inserted between each element.
///     
/// # Examples
/// 
///    use einsteindb::iter::intersperse;
///     
///   let a = [1, 2, 3];
/// let b = [4, 5, 6];
/// let mut c = intersperse(a.iter(), b);
/// 
/// assert_eq!(c.next(), Some(1));
/// assert_eq!(c.next(), Some(4));
/// assert_eq!(c.next(), Some(2));
/// 
/// 
 pub fn intersperse<I, T>(iter: I, value: T) -> Intersperse<I, T> where I: Iterator<Item=T> {
    Intersperse {
        iter: iter,
        value: value,
    }


/// Intersperse a value between each element of an iterator.
/// 
/// The iterator is returned with the value inserted between each element.

    //pub fn new(iter: I, value: T) -> Intersperse<I, T> {
    //    Intersperse {
    //        iter: iter,
    //        value: value,
    //    }

    pub fn iter(&self) -> &I {
        /// Intersperse a value between each element of an iterator.
        &self.iter //<I, T>

    }

    pub fn value(&self) -> &T {
        /// Intersperse a value between each element of an iterator.
        &self.value //<I, T>
    }


    pub fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().and_then(|x| {
            Some(x)
        })
    }

    pub fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }

    pub fn count(&mut self) -> usize {
        self.iter.count()
    }

    pub fn last(&mut self) -> Option<Self::Item> {
        self.iter.last()
    }
}