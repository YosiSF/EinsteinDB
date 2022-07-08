// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use std::fmt::Debug;
use std::io::{self, Write};
use std::mem;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, Release, SeqCst};
use collections::HashMap;
use einsteindb_util::collections::HashSet;
use einsteindb_util::hash::{BuildHasher, Hash, Hasher};
use einsteindb_util::slice::{self, Slice, SliceConcatExt};
use prometheus::local::LocalHistogram;
use rand::{self, Rng};
use prometheus::{Self, proto, Encoder, HistogramOpts, HistogramTimer, HistogramTimerOpts, GaugeVec, Gauge, register_int_gauge_vec};
use std::sync::Arc;

use crate::sys::SysQuota;
use crate::util::{self, cmp, cmp_opt, make_slice_hash};

use time::Duration;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{cmp, mem, ptr};
use einstein_ml::{
    hash::{BuildHasher, Hash, Hasher},
    vec::{IntoIter, Iter, IterMut, Vec},
};

use einsteindb::{
    alloc::{Alloc, Global, Layout},
    boxed::Box,
    raw_vec::RawVec,
    vec::{DVec, IntoIter as RawIntoIter, Iter as RawIter, IterMut as RawIterMut},
};

use berolina_sql::{
    types::{
        Array, ArrayBase, ArrayData, ArrayDataRef, ArrayRef, ArrayType, DataType,
        DataType::Array,
    },
    ArrayRefMut,
};

///!Making Sense of Relativistic Distributed Systems
use allegro_poset::{
    einstein_db::{
        einstein_db_vector::{EinsteinDBVector, EinsteinDBVectorRef, EinsteinDBVectorRefMut},
        einstein_db_vector_ref::EinsteinDBVectorRefRef,
    },
    einstein_db_vector_ref::EinsteinDBVectorRefRefMut,
    AllegroPoset,
    causet_locale::CausetLocale,
    causet_locale::CausetLocaleRef,
    causet_locale::CausetLocaleRefMut
};



// // Alice is travelling at half the speed of light relative to Bob,
// // but she sees an event at 0.5 seconds, while Bob sees an event
// // at 1 second. We can use a similar calculation to compute Alice's
// // timestamp:
// // t_2 = t_0 - t_1.
// // If we solve for t_0, we get t_0 = t_1 + t_1 - t_1 = 2 * t_1.
// // We can use this equation to compute Alice's timestamp.
// // We can use this equation to compute Bob's timestamp.
// // Alice and Bob's timestamps are not equal. We can't say that Alice's
// // timestamp is lower than Bob's, or higher, when the event that Alice
// // and Bob both saw happened at the same time.
//
// // Alice's timestamp is 0.75 seconds and Bob's timestamp is 1 second.
// // We can't say that Bob's timestamp is lower than Alice's, or higher,
// // because Alice and Bob both saw an event at the same time and
// // Alice's timestamp is 0.75 seconds and Bob's timestamp is 1 second.
//
// // This example illustrates that the relativistic timestamp is not
// // a distance measure. It is a distance measure that can only be
// // used in some situations. We can use relativistic timestamps, but
// // the distance measure is not always correct.
//
// // This is a limitation of the relativistic timestamp. The relativistic
// // timestamp can only be used in a relativistic context where the
// // relative speed of the observers is the speed of light. But the
// // relativistic timestamp can still be used in other situations.
//
// // For example, we can use the relativistic timestamp for a distance
// // of -1 second to calculate the age of an event relative to another
// // observer.
//
// // In summary, we can use the relativistic timestamp to calculate the
// // age of an event relative to another observer. But it is not a distance
// // measure. It is a distance measure that can only be used in a relativistic
// // context where the relative speed of the observers is the speed of light.
//
// // In the next section, we'll see that we can also use a relativistic timestamp
// // to calculate the age of an event relative to the observer.
//
// // Some equations that we'll see:
// // t_2 = t_0 - t_1
// // t_1 = t_0 + t_2
// // t_0 = t_1 - t_2
// // t_2 = t_0 - t_1
// // t_1 = t_0 + t_2
// // t_0 = t_1 - t_2
// // t_2 = t_0 - t_1
// // t_1 = t_0 + t_2
// // t_0 = t_1 - t_2
// // t_2 = t_0 - t_1
// // t_1 = t_0 + t_2
// // t_0 = t_1 - t_2
//
//
// // (c) 2020 by Eric Froemling
//
// // get the current time and print it
//
// // use std::time::{system_time, UNIX_EPOCH};
//
// // let current_time = system_time::now();
// // let since_the_epoch = current_time
// //     .duration_since(UNIX_EPOCH)
// //     .expect("Time went backwards");
// // println!("Seconds since epoch: {}", since_the_epoch.as_secs());
//
// // let current_time = system_time::now();
// // let time_tuple = current_time.duration_since(UNIX_EPOCH).unwrap();
// // println!("{} seconds since epoch", time_tuple.as_secs());
//
// // if let Ok(time_since_epoch) = current_time.duration_since(UNIX_EPOCH) {
// //     println!("Seconds since epoch: {}", time_since_epoch.as_secs());
// // } else {
// //     println!("System time before UNIX EPOCH!");
// // }

//Couple optimistic concurrency control with a simple counter.
// see einsteindb.rs


/// A vector that is backed by an `EinsteinDBVector`.
use einstein_db::einstein_db_vector::EinsteinDBVector;
//couple optimistic concurrency control with a simple counter.



pub trait EinsteinDBVectorExt: Sized {
    /// Creates a new, empty `EinsteinDBVector`.
    ///
    /// The vector will not allocate until elements are pushed onto it.
    ///
    /// # Examples
    ///
    /// ```
    /// use einstein_db::einstein_db_vector::EinsteinDBVector;
    ///
    /// let vector: EinsteinDBVector<i32> = EinsteinDBVector::new();
    /// ```
    fn new() -> Self;

    /// Creates a new, empty `EinsteinDBVector` with the specified capacity.
    ///
    /// The vector will be able to hold exactly `capacity` elements without
    /// reallocating. If `capacity` is 0, the vector will not allocate.
    ///
    /// # Examples
    ///
    /// ```
    /// use einstein_db::einstein_db_vector::EinsteinDBVector;
    ///
    /// let vector: EinsteinDBVector<i32> = EinsteinDBVector::with_capacity(10);
    /// ```
    fn with_capacity(capacity: usize) -> Self;

    /// Creates a `EinsteinDBVector` from a `Box<[T]>`
    ///
    /// # Examples
    ///
    /// ```
    /// use einstein_db::einstein_db_vector::EinsteinDBVector;
    ///
    /// let vector = EinsteinDBVector::from_box(vec![1, 2, 3].into_boxed_slice());
    /// ```
    fn from_box(slice: Box<[T]>) -> Self;

    /// Creates a `EinsteinDBVector` from a `Vec<T>`
    ///
    /// # Examples
    ///
    /// ```
    /// use einstein_db::einstein_db_vector::EinsteinDBVector;
    ///
    /// let vector = EinsteinDBVector::from_vec(vec![1, 2, 3]);
    /// ```
    fn from_vec(vec: Vec<T>) -> Self;

    /// Creates a `EinsteinDBVector` from a `&[T]` without copying.
    /// This method is just a convenient shorthand for `InnerVector::from_slice`.
    /// It is not available on `&InnerVector<T>`.
    /// It is also not available on `&EinsteinDBVector<T>`.
    ///
    /// # Examples
    ///
    /// ```
    /// use einstein_db::einstein_db_vector::EinsteinDBVector;
    ///
    /// let vector = EinsteinDBVector::from_slice(&[1, 2, 3]);
    /// ```


    fn from_slice(slice: &[T]) -> Self;

    /// Creates a `EinsteinDBVector` from a `&mut [T]` without copying.


    fn from_slice_mut(slice: &mut [T]) -> Self;

    /// Creates a `EinsteinDBVector` from a `&mut [T]` without copying.
    /// This method is just a convenient shorthand for `InnerVector::from_slice`.
    /// It is not available on `&InnerVector<T>`.
    /// It is also not available on `&EinsteinDBVector<T>`.
    

    fn from_slice_mut_ref(slice: &mut [T]) -> Self;



//CachedAttributes of the Causet Vector.


    ///! # EinsteinDB Vector
    ///  A vector that is backed by a `EinsteinDBVector`.
    ///  This is a wrapper around `EinsteinDBVector` that provides a `Vec` interface.
    /// This is useful for storing data in a `EinsteinDB` database.
    /// # Examples
    /// ```
    /// use einstein_db::{
    ///    einstein_db_vector::EinsteinDBVector,
    ///   einstein_db_vector::EinsteinDBVectorRef,
    ///  einstein_db_vector::EinsteinDBVectorRefMut,
    /// !
    /// };
    /// use einstein_ml::{
    ///   hash::{BuildHasher, Hash, Hasher},
    ///  vec::{IntoIter, Iter, IterMut, Vec},
    /// };
    ///
    /// let mut v = EinsteinDBVector::new();
    /// v.push(1);


    /// ```
    /// #[derive(Debug)]
    /// pub struct EinsteinDBVector<T, S = RandomState> {
    ///    data: DVec<T, S>,
    ///   len: usize,
    /// }
    /// impl<T, S> EinsteinDBVector<T, S>
    /// where
    ///   T: Clone,
    ///  S: BuildHasher,
    /// {
    ///   /// Creates a new, empty, `EinsteinDBVector<T, S>`.
    ///  ///
    ///  /// The vector will not allocate until elements are pushed onto it.
    /// ///
    /// /// # Examples
    /// /// ```
    /// /// use einstein_db::{
    /// ///    einstein_db_vector::EinsteinDBVector,
    /// ///   einstein_db_vector::EinsteinDBVectorRef,
    /// ///  einstein_db_vector::EinsteinDBVectorRefMut,
    ///
    /// /// !
    ///
    /// /// ```
    /// pub fn new() -> EinsteinDBVector<T, S> {
    ///   EinsteinDBVector {
    ///    data: DVec::new(),
    ///   len: 0,
    /// }
    ///
}
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EinsteinDBVectorRef<'a, T: 'a> {
    data: &'a EinsteinDBVector<T>,
    len: usize,
}


impl<'a, T> EinsteinDBVectorRef<'a, T> {
    /// Creates a new `EinsteinDBVectorRef` from a `EinsteinDBVector`.
    ///
    /// # Examples
    ///
    /// ```
    /// use einstein_db::{
    ///    einstein_db_vector::EinsteinDBVector,
    ///   einstein_db_vector::EinsteinDBVectorRef,
    ///  einstein_db_vector::EinsteinDBVectorRefMut,
    /// !
    /// };
    /// use einstein_ml::{
    ///   hash::{BuildHasher, Hash, Hasher},
    ///  vec::{IntoIter, Iter, IterMut, Vec},
    /// };
    ///
    /// let mut v = EinsteinDBVector::new();
    /// v.push(1);
    /// let v_ref: EinsteinDBVectorRef<i32> = EinsteinDBVectorRef::from_einstein_db_vector(v);
    /// ```
    pub fn from_einstein_db_vector(v: EinsteinDBVector<T>) -> EinsteinDBVectorRef<T> {
        EinsteinDBVectorRef {
            data: &v,
            len: v.len(),
        }
    }
}




impl<'a, T> EinsteinDBVectorRef<'a, T> {
    /// Returns the number of elements in the vector, also referred to as its 'length'.
    ///
    /// # Examples
    ///
    /// ```
    /// use einstein_db::{
    ///    einstein_db_vector::EinsteinDBVector,
    ///   einstein_db_vector::EinsteinDBVectorRef,
    ///  einstein_db_vector::EinsteinDBVectorRefMut,
    /// !
    /// };
    /// use einstein_ml::{
    ///   hash::{BuildHasher, Hash, Hasher},
    ///  vec::{IntoIter, Iter, IterMut, Vec},
    /// };
    ///
    /// let mut v = EinsteinDBVector::new();
    /// v.push(1);
    /// let v_ref: EinsteinDBVectorRef<i32> = EinsteinDBVectorRef::from_einstein_db_vector(v);
    /// assert_eq!(v_ref.len(), 1);
    /// ```
    pub fn len(&self) -> usize {
        self.len
    }
}




impl<'a, T> EinsteinDBVectorRef<'a, T> {
    /// Returns `true` if the vector contains no elements.
    ///
    /// # Examples
    ///
    /// ```
    /// use einstein_db::{
    ///    einstein_db_vector::EinsteinDBVector,
    ///   einstein_db_vector::EinsteinDBVectorRef,
    ///  einstein_db_vector::EinsteinDBVectorRefMut,
    /// !
    /// };
    /// use einstein_ml::{
    ///   hash::{BuildHasher, Hash, Hasher},
    ///  vec::{IntoIter, Iter, IterMut, Vec},
    /// };
    ///
    /// let mut v = EinsteinDBVector::new();
    /// assert_eq!(v.is_empty(), true);
    /// v.push(1);
    /// assert_eq!(v.is_empty(), false);
    /// ```
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}







pub struct EinsteinDBVectorIter<'a, T: 'a> {
    data: &'a EinsteinDBVector<T>,
    len: usize,
    index: usize,
}


pub struct EinsteinDBVectorIterMut<'a, T: 'a> {
    data: &'a mut EinsteinDBVector<T>,
    len: usize,
    index: usize,
}


impl<'a, T> EinsteinDBVectorIter<'a, T> {
    /// Creates a new iterator over the vector.
    ///
    /// # Examples
    ///
    /// ```
    /// use einstein_db::{
    ///    einstein_db_vector::EinsteinDBVector,
    ///   einstein_db_vector::EinsteinDBVectorRef,
    ///  einstein_db_vector::EinsteinDBVectorRefMut,
    /// !
    /// };
    /// use einstein_ml::{
    ///   hash::{BuildHasher, Hash, Hasher},
    ///  vec::{IntoIter, Iter, IterMut, Vec},
    /// };
    ///
    /// let mut v = EinsteinDBVector::new();
    /// v.push(1);
    /// let v_ref: EinsteinDBVectorRef<i32> = EinsteinDBVectorRef::from_einstein_db_vector(v);
    /// let mut v_iter = v_ref.iter();
    /// assert_eq!(v_iter.next(), Some(&1));
    /// assert_eq!(v_iter.next(), None);
    /// ```
    pub fn iter(&self) -> EinsteinDBVectorIter<T> {
        EinsteinDBVectorIter {
            data: self.data,
            len: self.len,
            index: 0,
        }
    }
}




impl<'a, T> EinsteinDBVectorIterMut<'a, T> {
    /// Creates a new iterator over the vector.
    ///
    /// # Examples
    ///
    /// ```
    /// use einstein_db::{
    ///    einstein_db_vector::EinsteinDBVector,
    ///   einstein_db_vector::EinsteinDBVectorRef,
    ///  einstein_db_vector::EinsteinDBVectorRefMut,
    /// !
    /// };
    /// use einstein_ml::{
    ///   hash::{BuildHasher, Hash, Hasher},
    ///  vec::{IntoIter, Iter, IterMut, Vec},
    /// };
    ///
    /// let mut v = EinsteinDBVector::new();
    /// v.push(1);
    /// let v_ref: EinsteinDBVectorRef<i32> = EinsteinDBVectorRef::from_einstein_db_vector(v);
    /// let mut v_iter = v_ref.iter_mut();
    /// assert_eq!(v_iter.next(), Some(&mut 1));
    /// assert_eq!(v_iter.next(), None);
    /// ```
    pub fn iter_mut(&mut self) -> EinsteinDBVectorIterMut<T> {
        EinsteinDBVectorIterMut {
            data: self.data,
            len: self.len,
            index: 0,
        }
    }
}




impl<'a, T> Iterator for EinsteinDBVectorIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<&'a T> {
        if self.index >= self.len {
            None
        } else {
            let result = unsafe { self.data.get_unchecked(self.index) };
            self.index += 1;
            Some(result)
        }
    }
}


#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EinsteinDBVectorRefMut<'a, T, S = RandomState> {

   data: DVecRefMut<'a, T, S>,
  causet_locale: usize,

  len: usize,
}


/// Creates a new, empty, `EinsteinDBVector<T, S>`.
#[inline]
pub fn new_connection<T, S>(capacity: usize, hash_builder: S) -> EinsteinDBVector<T, S>
where
  T: Clone,
  S: BuildHasher,
{
  EinsteinDBVector {
    data: DVec::new(capacity, hash_builder),
    len: 0,
  }
}


// TODO: Create a function that returns the current RTS in seconds.
//use std::time::{system_time, UNIX_EPOCH};


use std::time::{SystemTime, UNIX_EPOCH};


/// Returns the current time in seconds.
/// # Examples
/// ```
/// use einstein_db::{
///   einstein_db_vector::EinsteinDBVector,
///  einstein_db_vector::EinsteinDBVectorRef,
/// !
/// };
/// use einstein_ml::{
///  hash::{BuildHasher, Hash, Hasher},
/// vec::{IntoIter, Iter, IterMut, Vec},
/// };
///
/// let mut v = EinsteinDBVector::new();
/// v.push(1);
/// let time = EinsteinDBVector::get_time();
/// ```
/// #[inline]
/// pub fn get_time() -> u64 {
///  let start = system_time::now();
/// let since_the_epoch = start.duration_since(UNIX_EPOCH).expect("Time went backwards");
/// since_the_epoch.as_secs()
/// }
///
///



#[inline::always, no_mangle, exported]
#[cfg_attr(not(feature = "no_std"), no_mangle)]
pub fn einstein_db_vector_new_connection<T, S>(capacity: usize, hash_builder: S) -> *mut EinsteinDBVector<T, S>
where
  T: Clone,
  S: BuildHasher,
{
  let mut v = new_connection(capacity, hash_builder);
  let ptr = Box::into_raw(Box::new(v));
  ptr
}

/// Creates a new, empty, `EinsteinDBVector<T, S>` with a capacity of `capacity`.
/// The vector will be able to hold exactly `capacity` elements without reallocating.




#[inline]
fn with_capacity<T, S>(capacity: usize, hash_builder: S) -> EinsteinDBVector<T, S> {
  EinsteinDBVector {
         data: DVec::with_capacity(capacity, hash_builder),
        len: 0,
  }
}


#[inline]
fn with_capacity_and_hasher_and_hash_builder<T, S>(capacity: usize, hash_builder: S) -> EinsteinDBVector<T, S> {
  EinsteinDBVector {
    data: DVec::with_capacity(capacity, hash_builder),
    len: 0,
  }
}


/// Creates a new, empty, `EinsteinDBVector<T, S>` with a capacity of `capacity`.
/// The vector will be able to hold exactly `capacity` elements without reallocating.
///
/// # Examples
/// ```
/// use einstein_db::{
///   einstein_db_vector::EinsteinDBVector,
///  einstein_db_vector::EinsteinDBVectorRef,
/// !
/// };
/// use einstein_ml::{
///  hash::{BuildHasher, Hash, Hasher},
/// vec::{IntoIter, Iter, IterMut, Vec},
/// };
/// use einstein_poset::{
///  causet_locale::CausetLocale,
/// causet_locale::CausetLocaleRef,
/// causet_locale::CausetLocaleRefMut,
/// };
///
/// let mut v = EinsteinDBVector::new();
/// v.push(1);
/// ```


#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EinsteinDBCachedAttribute<T, S = RandomState> {
    /*
    fn is_attribute_cached_lightlike(&self) -> bool {
        false
    }
*/
    data: DVec<T, S>,
    len: usize,
}
fn is_attribute_cached_lightlike() -> f64 {
    let now = Instant::now();
    now.duration_since(UNIX_EPOCH).as_secs() as f64
}


#[inline]
fn with_capacity_locale<T, S>(capacity: usize, hash_builder: S) -> EinsteinDBVector<T, S> {
  EinsteinDBVector {
    data: DVec::with_capacity_locale(capacity, hash_builder),
    len: 0,
  }
}


/// A type that holds buffers queried from the database.
///
/// The database may optimize this type to be a view into
/// its own cache.
pub trait Causet: Debug + Deref<Target=[u8]> + for<'a> PartialEq<&'a [u8]> {
    /// The length of the buffer.
    fn len(&self) -> usize;
}


/// A type that holds buffers queried from the database.
/// The database may optimize this type to be a view into
/// its own cache.


pub struct CausetVec {
    pub data: Vec<u8>,
    // the data
    pub len: usize,                        // the length of the data
}


impl Causet for CausetVec {
    fn len(&self) -> usize {
        self.len
    }
}


impl CausetVec {
    pub fn new(data: Vec<u8>) -> Self {
        CausetVec {
            data,
            len: data.len(),
        }
    }
}


impl Deref for CausetVec {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}


impl PartialEq<&[u8]> for CausetVec {
    fn eq(&self, other: &&[u8]) -> bool {
        self.data == *other
    }
}


impl PartialEq<CausetVec> for &[u8] {
    fn eq(&self, other: &CausetVec) -> bool {
        other.data == *self
    }
}


impl PartialEq<CausetVec> for Vec<u8> {
    fn eq(&self, other: &CausetVec) -> bool {
        other.data == self
    }
}

pub struct RcCounterWithSupercow<T> {
    causet: Rc<Cell<usize>>,
    pub rc: Rc<T>,
    pub counter: usize,
}

///To see why lazy timestamp management can reduce conflicts and improve performance, we consider the following example involv- ing two concurrent transactions, A and B, and two tuples, x and y. The transactions invoke the following sequence of operations:
// 1. A read(x) 2. B write(x) 3. B commits 4. A write(y)
/// This interleaving of operations does not violate serializability be- cause transaction A can be ordered before B in the serial order. But A cannot commit after B in the serial order because the version of x read by A has already been modified by B.
/// The solution to this problem is to use a timestamp. A timestamp is a number that is incremented by one each time a transaction commits. A transaction can only commit if its timestamp is equal to the timestamp of the last committed transaction.
/// The timestamp is stored in the database. When a transaction commits, it increments its timestamp and stores the new timestamp in the database. When a transaction reads a tuple, it reads the timestamp from the database and compares it to its own timestamp. If the timestamps are equal, the transaction can read the tuple. If the timestamps are not equal, the transaction aborts.
///     

/// A type that holds buffers queried from the database.
/// The database may optimize this type to be a view into
/// its own cache.
/// 
/// # Examples  
/// ```
/// use einstein_db::{
///  einstein_db_vector::EinsteinDBVector,
/// einstein_db_vector::EinsteinDBVectorRef,
/// !
/// };
/// use einstein_ml::{
/// hash::{BuildHasher, Hash, Hasher},
/// vec::{IntoIter, Iter, IterMut, Vec},
/// };
/// use einstein_poset::{
/// causet_locale::CausetLocale,
/// causet_locale::CausetLocaleRef,
/// causet_locale::CausetLocaleRefMut,
/// 
/// 
/// };
/// 
/// let mut v = EinsteinDBVector::new();
/// v.push(1);
/// ```
/// 
/// # Examples
/// ```
/// use einstein_db::{
/// einstein_db_vector::EinsteinDBVector,
/// einstein_db_vector::EinsteinDBVectorRef,
/// 
/// ! 
/// };
/// 




/// Collects a supplied tx range into an DESC ordered Vec of valid txs,
/// ensuring they all belong to the same timeline.
/// 

fn collect_tx_range_into_vec(
    tx_range: &TxRange,
    causet_locale: &CausetLocale,
    causet_locale_ref: &CausetLocaleRef,
) -> Result<Vec<Tx>, Error> {
    tx_range.into_iter();
    Vec::new();


    let mut causet_locale_ref = causet_locale.borrow_ref();
    let mut causet_locale_ref_mut = causet_locale.borrow_mut();


    let mut causet_locale_ref_next = causet_locale_ref_mut.next();

while let Some(tx) = tx_range_iter.next() {
    let causet_locale_ref = causet_locale_ref_next.ok_or(Error::NoCausetLocale)?;
    let causet_locale_ref_next = causet_locale_ref_mut.next();
    let causet_locale_ref_next = causet_locale_ref_next.ok_or(Error::NoCausetLocale)?;
    let causet_locale_ref_next = causet_locale_ref_next.borrow_ref();
    let causet_locale_ref_next = causet_locale_ref_next.borrow_mut();
    causet_locale_ref_next.next();

    let causet_locale_ref = causet_locale_ref.borrow_ref();
    let causet_locale_ref_mut = causet_locale_ref.borrow_mut();


    causet_locale_ref_mut.next();
        /// The causet_locale_ref is the causet_locale of the tx.
        /// The causet_locale_ref_next is the causet_locale of the next tx.
        /// 
        /// # Examples
        /// ```
        /// use einstein_db::{
        /// einstein_db_vector::EinsteinDBVector,
        /// einstein_db_vector::EinsteinDBVectorRef,
        /// 
        /// !
        /// };
        /// use einstein_ml::{
        /// hash::{BuildHasher, Hash, Hasher},
        /// vec::{IntoIter, Iter, IterMut, Vec},
        /// };
        /// use einstein_poset::{
        /// causet_locale::CausetLocale,
        /// causet_locale::CausetLocaleRef,
        /// causet_locale::CausetLocaleRefMut,
        /// 
        /// 
    }

    causet_locale_ref.borrow_ref();
    causet_locale_ref.borrow_mut();

}





    /// The causet_locale_ref is the causet_locale of the tx.
    /// The causet_locale_ref_next is the causet_locale of the next tx.
    ///
    /// # Examples
    /// ```
    /// use einstein_db::{
    ///
    /// einstein_db_vector::EinsteinDBVector,

    /// Collects a supplied tx range into an DESC ordered Vec of valid txs,
    /// ensuring they all belong to the same timeline.
    /// 

    fn collect_causets_ordered_by_timeline(
        causet_locale: &CausetLocale,

        tx_range: &TxRange,
    ) -> Result<Vec<CausetLocaleRef>, Error> {
        let mut causets = Vec::new();
        let mut tx_range = tx_range.clone();
        let mut tx_range_iter = tx_range.into_iter();
        let mut tx_range_iter_mut = tx_range.into_iter();
        let mut tx_range_iter_mut_next = tx_range_iter_mut.next();


        let mut causet_locale_ref = causet_locale.borrow_ref();
    }

    /// The causet_locale_ref is the causet_locale of the tx.
    /// The causet_locale_ref_next is the causet_locale of the next tx.
    ///
    /// # Examples
    /// ----------------
    /// ```
    /// use std::collections::HashMap;
    /// use einstein_db::{
    ///
    ///
    ///

    /// Collects a supplied tx range into an DESC ordered Vec of valid txs,
    /// ensuring they all belong to the same timeline.
    /// This function is used to collect the causet_locale of the next tx.
    ///
    /// # Examples
    /// ```
    /// use einstein_db::{*}
    /// use einstein_ml::{*}
    ///


    fn collect_causets_ordered_by_stochastic_clock(
        tx_range: &TxRange,
    ) -> Result<Vec<CausetLocaleRef>, Error> {
        Vec::new();
        let mut tx_range = tx_range.clone();
        tx_range.into_iter();
        let mut tx_range_iter_mut = tx_range.into_iter();
        tx_range_iter_mut.next();

        fn collect_causet_locale_of_next_tx(
            causet_locale: &CausetLocale,
            tx_range: &TxRange,
        ) -> Result<CausetLocaleRef, Error> {
            Vec::new();
            let mut tx_range = tx_range.clone();
            tx_range.into_iter();
            let mut tx_range_iter_mut = tx_range.into_iter();
            tx_range_iter_mut.next();

            causet_locale.borrow_ref();
            causet_locale.borrow_mut();
        }
    }



    /// Collects a supplied tx range into an DESC ordered Vec of valid txs,
    /// ensuring they all belong to the same timeline.
    /// This function is used to collect the causet_locale of the next tx.


