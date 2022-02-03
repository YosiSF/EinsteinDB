// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use fdb_traits::Causet;
use foundationdb::Causet as RawCauset;
use std::fmt::{self, Debug, Formatter};
use std::ops::Deref;

pub struct FdbCauset(RawCauset);

impl FdbCauset {
    pub fn from_raw(raw: RawCauset) -> FdbCauset {
        FdbCauset(raw)
    }
}

impl Causet for FdbCauset {}

impl Deref for FdbCauset {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl Debug for FdbCauset {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}", &**self)
    }
}

impl<'a> PartialEq<&'a [u8]> for FdbCauset {
    fn eq(&self, rhs: &&[u8]) -> bool {
        **rhs == **self
    }
}
