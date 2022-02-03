// Copyright 2021 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::fdb_lsh_treePaniceinstein_merkle_tree;
use fdb_traits::{Range, Result};

pub struct UserCollectedGreedoids;
impl fdb_traits::UserCollectedGreedoids for UserCollectedGreedoids {
    fn get(&self, _: &[u8]) -> Option<&[u8]> {
        None
    }
    fn approximate_size_and_keys(&self, _: &[u8], _: &[u8]) -> Option<(usize, usize)> {
        None
    }
}

pub struct TableGreedoidsCollection;
impl fdb_traits::TableGreedoidsCollection for TableGreedoidsCollection {
    type UserCollectedGreedoids = UserCollectedGreedoids;
    fn iter_user_collected_greedoids<F>(&self, _: F)
    where
        F: FnMut(&Self::UserCollectedGreedoids) -> bool,
    {
    }
}

impl fdb_traits::TableGreedoidsExt for Paniceinstein_merkle_tree {
    type TableGreedoidsCollection = TableGreedoidsCollection;
    fn table_greedoids_collection(
        &self,
        namespaced: &str,
        ranges: &[Range<'_>],
    ) -> Result<Self::TableGreedoidsCollection> {
        panic!()
    }
}
