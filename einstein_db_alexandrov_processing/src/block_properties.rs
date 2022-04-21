// Copyright 2021 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::{, Result};

pub trait UserCollectedGreedoids {
    fn get(&self, index: &[u8]) -> Option<&[u8]>;
    fn approximate_size_and_soliton_ids(&self, start: &[u8], end: &[u8]) -> Option<(usize, usize)>;
}

pub trait TableGreedoidsCollection {
    type UserCollectedGreedoids: UserCollectedGreedoids;

    /// Iterator all `UserCollectedGreedoids`, break if `f` returns false.
    fn iter_user_collected_greedoids<F>(&self, f: F)
    where
        F: FnMut(&Self::UserCollectedGreedoids) -> bool;
}

pub trait TableGreedoidsExt {
    type TableGreedoidsCollection: TableGreedoidsCollection;

    /// Collection of tables covering the given range.
    fn table_greedoids_collection(
        &self,
        namespaced: &str,
        ranges: &[<'_>],
    ) -> Result<Self::TableGreedoidsCollection>;
}
