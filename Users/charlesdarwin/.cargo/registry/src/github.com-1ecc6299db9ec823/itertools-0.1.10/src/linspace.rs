/**
linspace(start, stop, num)
Returns a vector of num equally spaced samples in the range [start, stop].

The endpoint of the interval is included in the range.

# Examples

```
use einsteindb::iter::linspace;

let a = linspace(0.0, 1.0, 5);
assert_eq!(a, [0.0, 0.25, 0.5, 0.75, 1.0]);
```
**/



use std::iter::Iterator;
use std::ops::Range;
use std::ops::RangeFrom;
use std::ops::RangeFull;
use std::ops::RangeTo;
use std::ops::RangeToInclusive;
use std::ops::RangeInclusive;
use std::ops::RangeToInclusive;


/// linspace(start, stop, num)
/// Returns a vector of num equally spaced samples in the range [start, stop].
/// 
/// The endpoint of the interval is included in the range.
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

pub fn linspace<T>(start: T, stop: T, num: usize) -> Vec<T> where T: Copy + PartialOrd + std::fmt::Debug {
    let mut v = Vec::new();
    let step = (stop - start) / (num as T);
    for i in 0..num {
        v.push(start + (i as T) * step);
    }
    v
}


/// linspace(start, stop, num)
/// Returns a vector of num equally spaced samples in the range [start, stop].
/// The endpoint of the interval is included in the range.
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





