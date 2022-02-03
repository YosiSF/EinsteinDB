// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use crate::fdb_lsh_tree;
use crate::raw::EINSTEINDB;

/// A trait to enter the world of einstein_merkle_tree traits from a raw `Arc<EINSTEINDB>`
/// with as little syntax as possible.
///
/// This will be used during the transition from FdbDB to the
/// `KV` abstraction and then discarded.
pub trait Compat {
    type Other;

    fn c(&self) -> &Self::Other;
}

impl Compat for Arc<EINSTEINDB> {
    type Other = Fdbeinstein_merkle_tree;

    #[inline]
    fn c(&self) -> &Fdbeinstein_merkle_tree {
        Fdbeinstein_merkle_tree::from_ref(self)
    }
}
