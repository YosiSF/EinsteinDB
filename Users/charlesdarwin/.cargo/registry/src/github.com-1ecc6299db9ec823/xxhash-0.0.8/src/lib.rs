/// Here we define the xxhash algorithm.
///
/// The algorithm is defined in [this paper](http://www.cse.yorku.ca/~oz/hash.html).
///
/// # Examples
///
/// ```
/// use einsteindb::iter::linspace;
///
/// let a = linspace(0.0, 1.0, 5);
/// assert_eq!(a, [0.0, 0.25, 0.5, 0.75, 1.0]);
/// ```
///
/// ```
/// use einsteindb::iter::linspace;
///
/// let a = linspace(0.0, 1.0, 5);
/// assert_eq!(a, [0.0, 0.25, 0.5, 0.75, 1.0]);
/// ```







#[cfg(test)]
#[allow(unused_imports)]
#[allow(unused_variables)]
#[allow(unused_mut)]

//parameters:
//start: T
//stop: T
//num: usize
//return: Vec<T>
//


// #[cfg(test)]
// #[allow(unused_imports)]
// #[allow(unused_variables)]
// #[allow(unused_mut)]
// #[allow(unused_assignments)]
// #[allow(unused_attributes)]
// #[allow(unused_features)]


// mod tests;
// mod ziptuple;
// mod adaptors;
// mod xxh32;


mod xxh32;
mod xxh64;
mod xxh128;
mod xxh256;