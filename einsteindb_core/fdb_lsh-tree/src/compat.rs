// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use crate::fdb_lsh_treeFdbEngine;
use crate::raw::DB;

/// A trait to enter the world of engine traits from a raw `Arc<DB>`
/// with as little syntax as possible.
///
/// This will be used during the transition from FdbDB to the
/// `KvEngine` abstraction and then discarded.
pub trait Compat {
    type Other;

    fn c(&self) -> &Self::Other;
}

impl Compat for Arc<DB> {
    type Other = FdbEngine;

    #[inline]
    fn c(&self) -> &FdbEngine {
        FdbEngine::from_ref(self)
    }
}
