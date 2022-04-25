// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use std::fmt::Debug;
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

use allegro_poset::{
    einstein_db::{
        einstein_db_vector::{EinsteinDBVector, EinsteinDBVectorRef, EinsteinDBVectorRefMut},
        einstein_db_vector_ref::EinsteinDBVectorRefRef,
    },
    einstein_db_vector_ref::EinsteinDBVectorRefRefMut,
};




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


use allegro_poset::causet_locale::CausetLocale;

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
        CausetVec { data, len: 0 }
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


