// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use fdb_traits::Causet;
use std::ops::Deref;

#[derive(Debug)]
pub struct PanicCauset;

impl Causet for PanicCauset {}

impl Deref for PanicCauset {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        panic!()
    }
}

impl<'a> PartialEq<&'a [u8]> for PanicCauset {
    fn eq(&self, rhs: &&[u8]) -> bool {
        **rhs == **self
    }
}
