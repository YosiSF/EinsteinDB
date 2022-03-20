// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use fdb_traits::Causet;
use foundationdb::Causet as Primitive_CausetCauset;
use std::fmt::{self, Debug, Formatter};
use std::ops::Deref;

pub struct FdbCauset(Primitive_CausetCauset);

impl FdbCauset {
    pub fn from_primitive_causet(primitive_causet: Primitive_CausetCauset) -> FdbCauset {
        FdbCauset(primitive_causet)
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
