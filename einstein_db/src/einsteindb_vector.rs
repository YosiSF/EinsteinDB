// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;


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


