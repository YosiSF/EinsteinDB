use std::iter::Iterator;
use std::iter::IntoIterator;
pub(crate) use std::iter::FromIterator;
use std::iter::Map;
use std::iter::MapInto;
use std::iter::Zip;
use std::iter::ZipLongest;


pub(crate) struct ZipTuple<I1, I2> {
    iter1: I1,
    iter2: I2,
}


pub(crate) struct PackageRegistry {
    packages: HashMap<String, Package>,
}


// Compare this snippet from EinsteinDB/Users/charlesdarwin/.cargo/registry/src/github.com-1ecc6299db9ec823/itertools-0.1.10/src/ziptuple.rs:
// //ziptuple
//
//
// use std::iter::Iterator;
// use std::iter::FromIterator;
// use std::iter::IntoIterator;
// use std::iter::Map;
// use std::iter::MapInto;


impl PackageRegistry {
    pub fn new() -> Self {
        PackageRegistry {
            packages: HashMap::new(),
        }
    }
}


// Compare this snippet from EinsteinDB/Users/charlesdarwin/.cargo/registry/src/github.com-1ecc6299db9ec823/itertools-0.1.10/src/ziptuple.rs:
// //ziptuple
//
//
// use std::iter::Iterator;
// use std::iter::FromIterator;
// use std::iter::IntoIterator;
// use std::iter::Map;
// use std::iter::MapInto;
// use std::iter::Zip;
// use std::iter::ZipLongest;


impl<'a> IntoIterator for &'a PackageRegistry {
    type Item = &'a Package;
    type IntoIter = Iter<'a, Package>;
    fn into_iter(self) -> Self::IntoIter {
        self.packages.values().into_iter()
    }
}


// Compare this snippet from EinsteinDB/Users/charlesdarwin/.cargo/registry/src/github.com-1ecc6299db9ec823/itertools-0.1.10/src/ziptuple.rs:
// //ziptuple
//
//
// use std::iter::Iterator;
// use std::iter::FromIterator;
// use std::iter::IntoIterator;
// use std::iter::Map;




impl<'a> IntoIterator for &'a PackageRegistry {
    type Item = &'a Package;
    type IntoIter = Iter<'a, Package>;
    fn into_iter(self) -> Self::IntoIter {
        self.packages.values().into_iter()
    }
}



/* 
https://doc.rust-lang.org/std/iter/trait.Iterator.html#method.zip_longest
*/






//Compare this snippet from EinsteinDB/Users/charlesdarwin/.cargo/registry/src/github.com-1ecc6299db9ec823/itertools-0.1.10/src/adaptors.rs:
// //adaptors



// 
//   pub struct ZipTuple<I1, I2> {
//     iter1: I1,
//     iter2: I2,
//   }
//
//   impl<I1, I2> Iterator for ZipTuple<I1, I2>
//       where I1: Iterator,
//             I2: Iterator


#[allow(unused_variables)]
#[allow(unused_mut)]
#[allow(unused_assignments)]
#[allow(unused_attributes)]
#[allow(unused_features)]
#[allow(unused_must_use)]




// #[cfg(test)]
// #[allow(unused_imports)]
// #[allow(unused_variables)]
// #[allow(unused_mut)]
// #[allow(unused_assignments)]
// #[allow(unused_attributes)]
// #[allow(unused_features)]
// #[allow(unused_must_use)]
//




// #[cfg(test)]
// #[allow(unused_imports)]
// #[allow(unused_variables)]


/// ziptuple is a zip iterator that zips two iterators together.
/// Causal sets and FoundationDB Records are both iterators.
/// 
/// # Examples
///     
///    use einsteindb::iter::ziptuple;
///   use einsteindb::iter::ziptuple::ZipTuple;
/// 
///   let a = [1, 2, 3];
///  let b = [4, 5, 6];
/// let mut c = ziptuple(a.iter(), b.iter());
/// 
/// assert_eq!(c.next(), Some((1, 4)));
/// assert_eq!(c.next(), Some((2, 5)));
/// assert_eq!(c.next(), Some((3, 6)));
/// assert_eq!(c.next(), None);
/// 
/// 
pub struct ZipTuple<I1, I2> {

    iter1: I1,
    iter2: I2,

    //pub fn new(iter1: I1, iter2: I2) -> ZipTuple<I1, I2> {
    //    ZipTuple {
    //        iter1: iter1,
    //        iter2: iter2,
    //    }
    //}

    //pub fn iter1(&self) -> &I1 {
    //    &self.iter1
    //}
    //
    //pub fn iter2(&self) -> &I2 {
}


impl<I1, I2> Iterator for ZipTuple<I1, I2>
    where I1: Iterator,
          I2: Iterator
{
    type Item = (I1::Item, I2::Item);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter1.next().and_then(|a| {
            self.iter2.next().and_then(|b| {
                Some((a, b))
            })
        })
    }
}



///MongoDB is a NoSQL database. We use adapters to convert between iterators and collections.
/// adaptors is a collection of adapters that convert between iterators and collections.
/// EinsteinDB is compatible with MySQL, PostgreSQL, and MongoDB. Uses FoundationDB.
/// 
/// # Examples
/// 
/// ```
/// use einsteindb::iter::adaptors;
/// use einsteindb::iter::adaptors::{
///    Adaptor,
///   AdaptorMut,
///  AdaptorMutRef,
/// AdaptorRef,
/// };
/// 
/// 




///MongoDB is a NoSQL database. We use adapters to convert between iterators and collections.
/// adaptors is a collection of adapters that convert between iterators and collections.
/// EinsteinDB is compatible with MySQL, PostgreSQL, and MongoDB. Uses FoundationDB.
/// 
/// # Examples
/// 
/// ```
/// use einsteindb::iter::adaptors;
/// use einsteindb::iter::adaptors::{
///   Adaptor,
/// AdaptorMut,
/// AdaptorMutRef,
/// AdaptorRef,
/// };




