// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use fdb_traits::DBVector;
use foundationdb::DBVector as RawDBVector;
use std::fmt::{self, Debug, Formatter};
use std::ops::Deref;

pub struct FdbDBVector(RawDBVector);

impl FdbDBVector {
    pub fn from_raw(raw: RawDBVector) -> FdbDBVector {
        FdbDBVector(raw)
    }
}

impl DBVector for FdbDBVector {}

impl Deref for FdbDBVector {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl Debug for FdbDBVector {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}", &**self)
    }
}

impl<'a> PartialEq<&'a [u8]> for FdbDBVector {
    fn eq(&self, rhs: &&[u8]) -> bool {
        **rhs == **self
    }
}
