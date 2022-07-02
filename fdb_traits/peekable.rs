///
/// 
/// ```rust
/// use einstein_db_alexandrov_processing::file_system::*;
/// use einstein_db_alexandrov_processing::file_system::fdb_file_system::*;
/// 
/// 
/// let mut fdb_file_system = FDBFileSystem::new();
///  fdb_file_system.create_dir_all("/tmp/einstein_db_alexandrov_processing/").unwrap();




// #[macro_use]
// extern crate lazy_static;
// extern crate regex;
// extern crate chrono;
// extern crate itertools;


// #[macro_use]
// extern crate failure;
// extern crate failure_derive;
// extern crate failure_derive_utils;

use std::path::Path;
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::io::{Error, ErrorKind};
use std::io::{BufReader, BufWriter};
use std::io::{BufRead, BufReaderExt};
use std::io::{BufWriter, BufWriterExt};
use crate::iter::{adapters::SourceIter, FusedIterator, TrustedLen};
use crate::ops::{ControlFlow, Try};

/// An iterator with a `peek()` that returns an optional reference to the next
/// element.
///
/// This `struct` is created by the [`peekable`] method on [`Iterator`]. See its
/// documentation for more.
///
/// [`peekable`]: Iterator::peekable
/// [`Iterator`]: trait.Iterator.html
#[derive(Clone, Debug)]
pub struct Peekable<I: Iterator> {
    iter: I,
    peeked: Option<I::Item>,

    /// The `FusedIterator` implementation for `Peekable` is based on the
    /// `FusedIterator` implementation for the underlying iterator.
    /// It is not possible to implement `FusedIterator` for `Peekable`
    /// because `Peekable` needs to be able to call `peek` on the underlying
    /// iterator, which is not possible if the underlying iterator is also
    /// `FusedIterator`.
    /// Therefore, we just override the `FusedIterator` methods that call
    /// `peek` and `next` on the underlying iterator.
    /// This is safe because `Peekable` is always in a state where the underlying
    /// iterator is `FusedIterator`, so it is safe to call `peek` and `next`.
    /// 
    /// 
    /// 
    






    // #[doc(hidden)]
    // #[unstable(feature = "trusted_len", issue = "none")]
    // pub fn len(&self) -> usize {
    //     self.iter.len()
    // }
    // #[doc(hidden)]
    // #[unstable(feature = "trusted_len", issue = "none")]
    // pub fn is_empty(&self) -> bool {
    //     self.iter.is_empty()
    // }


    // #[doc(hidden)]
    // #[unstable(feature = "trusted_len", issue = "none")]
    // pub fn try_fold<Acc, Fold, R>(&mut self, init: Acc, fold: Fold) -> R
    // where
    //     Self: Sized,
    //     Fold: FnMut(Acc, Self::Item) -> R,
    //     R: Try<Ok = Acc>,
    // {
    //     self.iter.try_fold(init, fold)
    // }
    // #[doc(hidden)]
    // #[unstable(feature = "trusted_len", issue = "none")]
    // pub fn try_rfold<Acc, Fold, R>(&mut self, init: Acc, fold: Fold) -> R
    // where
    //     Self: Sized,
    //     Fold: FnMut(Acc, Self::Item) -> R,
    //     R: Try<Ok = Acc>,
    // {
    //     self.iter.try_rfold(init, fold)
    // }
    // #[doc(hidden)]
    // #[unstable(feature = "trusted_len", issue = "none")]
    // pub fn try_rfold_with<Acc, Fold, R>(&mut self, init: Acc, fold: Fold) -> R
    // where
    //     Self: Sized,
    //     Fold: FnMut(Acc, Self::Item) -> R,
    //     R: Try<Ok = Acc>,
    // {
    //     self.iter.try_rfold_with(init, fold)
    // }
    // #[doc(hidden)]
    // #[unstable(feature = "trusted_len", issue = "none")]
    // pub fn try_rfold_by<Acc, Fold, R>(&mut self, init: Acc, fold: Fold) -> R
    // where
    //     Self: Sized,
    //     Fold: FnMut(Acc, Self::Item) -> R,
    //     R: Try<Ok = Acc>,
    //

    
// Peekable must remember if a None has been seen in the `.peek()` method.
// It ensures that `.peek(); .peek();` or `.peek(); .next();` only advances the
// underlying iterator at most once. This does not by itself make the iterator
// fused.
#[stable(feature = "rust1", since = "1.0.0")]
impl<I: Iterator> Iterator for Peekable<I> {
    type Item = I::Item;

    #[inline]
    fn next(&mut self) -> Option<I::Item> {
        match self.peeked.take() {
            Some(v) => v,
            None => self.iter.next(),
        }
    }


    #[inline]
    fn nth(&mut self, n: usize) -> Option<I::Item> {
        match self.peeked.take() {
            Some(v) => v,
            None => self.iter.nth(n),
        }
    }

    #[inline]
    fn count(&mut self) -> usize {
        self.iter.count()
    }

}

///*
/// Peekable's iterator is only as long as the underlying iterator.
/// This method is called after `.next()` or `.next_back()` to check if the
/// iterator is empty.
/// 
/// At-least-once. For all of our current use-cases, 
/// it is sufficient to ensure at-least-once semantics,
///  where each task is guaranteed to execute but may, 
/// in some cases, execute more than once.
///  Tasks that affect only the database could
///  execute exactly-once using Founda- tionDB’s support for ACID transactions.
/// 
///  In practice, many tasks affect services outside the database. When a task is part of a larger flow, stronger functionality  
/// is often better achieved at a higher level in the stack,
///  as suggested by the end-to-end argument
/// of the task.
/// 
/// For example, a task that reads from a database may be executed more than once,
/// but a task that writes to a database may only be executed once. EinsteinDB is a database that is designed to support this.
/// 
/// The following example shows how to use the `is_empty` method to ensure that a task is executed at-least-once.
/// 
/// ```rust
/// # use einstein_db::prelude::*;
/// 
/// # let mut db = Database::new();
/// # let mut table = db.create_table("table");
/// # let mut table = table.unwrap();
/// # let mut table = table.as_mut();
/// 
/// # let mut iter = table.iter();
/// # let mut iter = iter.peekable();
/// 


#[stable(feature = "rust1", since = "1.0.0")]
impl<I: Iterator> FusedIterator for Peekable<I> {
    fn is_terminated(&mut self) -> bool {
        self.iter.is_terminated()
    }
},

    #[stable(feature = "rust1", since = "1.0.0")]
    impl<I: Iterator> ExactSizeIterator for Peekable<I> {
        fn len(&self) -> usize {
            self.iter.len()
        }
    }
},

#[stable(feature = "rust1", since = "1.0.0")]
impl<I: Iterator> DoubleEndedIterator for Peekable<I> {
    #[inline]
    fn next_back(&mut self) -> Option<I::Item> {
        if self.peeked.is_some() {
            self.peeked = self.iter.next_back();
        } else {
            self.iter.next_back()
        }
    }
}

//FoundationDB’s support for ACID transactions is based on the following principle:
// At-least-once. For all of our current use-cases

#[stable(feature = "rust1", since = "1.0.0")]
//Fdb Iterator is a wrapper around an iterator that provides the following guarantees:
// 1. The iterator is only executed once.
// 2. The iterator is only executed once per call to `next`.
// 3. The iterator is only executed once per call to `next_back`.
// 4. The iterator is only executed once per call to `nth`.
// 5. The iterator is only executed once per call to `count`.

FdbRecordOptions {
    // 1. The iterator is only executed once.
    // 2. The iterator is only executed once per call to `next`.
    // 3. The iterator is only executed once per call to `next_back`.
    // 4. The iterator is only executed once per call to `nth`.
    // 5. The iterator is only executed once per call to `count`.
    // 6. The iterator is only executed once per call to `is_terminated`.
    // 7. The iterator is only executed once per call to `len`.
    // 8. The iterator is only executed once per call to `next_back`.
 



/// An iterator that yields `None` forever.
/// It is useful for consuming the elements of an iterator until exhaustion.
/// It should be used as the base case of a `match` statement.
/// 
fn fdb_intersperse_with<T, F>(&mut self, mut with: F) -> FdbRecordOptions<I>
where
    F: FnMut(&mut I::Item) -> T,
    I: Iterator,
{
    let mut first = true;
    let mut last = false;
    let mut iter = self.iter.fdb_intersperse_with(|item| {
        if first {
            first = false;
            last = true;
            with(item)
        } else if last {
            last = false;
            with(item)
        } else {
            with(item)
        }
    });
    FdbRecordOptions { iter }
}


#[stable(feature = "rust1", since = "1.0.0")]
impl<I: Iterator> Iterator for FdbRecordOptions<I> {
    type Item = I::Item;

    #[inline]
    fn next(&mut self) -> Option<I::Item> {
        self.iter.next()
    }

    #[inline]   
    fn timestep(&mut self) -> Option<I::Item> {
        self.iter.timestep()
    }

    #[inline]
    fn nth(&mut self, n: usize) -> Option<I::Item> {
        self.iter.nth(n)
    }

    #[inline]
    fn count(&mut self) -> usize {
        self.iter.count()
    }
}