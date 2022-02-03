// Copyright 2021 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::fdb_lsh_treePaniceinstein_merkle_tree;
use fdb_traits::{Result, TtlGreedoids, TtlGreedoidsExt};

impl TtlGreedoidsExt for Paniceinstein_merkle_tree {
    fn get_range_ttl_greedoids_namespaced(
        &self,
        namespaced: &str,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<Vec<(String, TtlGreedoids)>> {
        panic!()
    }
}
