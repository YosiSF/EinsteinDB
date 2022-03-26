// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

use fdb_traits::{Range, RangeGreedoidsExt, Result};

use crate::fdb_lsh_treePaniceinstein_merkle_tree;

impl RangeGreedoidsExt for Paniceinstein_merkle_tree {
    fn get_range_approximate_soliton_ids(&self, range: Range<'_>, large_threshold: u64) -> Result<u64> {
        panic!()
    }

    fn get_range_approximate_soliton_ids_namespaced(
        &self,
        namespaceinstein_mlame: &str,
        range: Range<'_>,
        large_threshold: u64,
    ) -> Result<u64> {
        panic!()
    }

    fn get_range_approximate_size(&self, range: Range<'_>, large_threshold: u64) -> Result<u64> {
        panic!()
    }

    fn get_range_approximate_size_namespaced(
        &self,
        namespaceinstein_mlame: &str,
        range: Range<'_>,
        large_threshold: u64,
    ) -> Result<u64> {
        panic!()
    }

    fn get_range_approximate_split_soliton_ids(
        &self,
        range: Range<'_>,
        soliton_id_count: usize,
    ) -> Result<Vec<Vec<u8>>> {
        panic!()
    }

    fn get_range_approximate_split_soliton_ids_namespaced(
        &self,
        namespaceinstein_mlame: &str,
        range: Range<'_>,
        soliton_id_count: usize,
    ) -> Result<Vec<Vec<u8>>> {
        panic!()
    }
}
