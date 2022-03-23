// Copyright 2021 EinsteinDB Project Authors. Licensed under Apache-2.0.

use fdb_traits::{Result, TtlGreedoids, TtlGreedoidsExt};

use crate::fdb_lsh_treePaniceinstein_merkle_tree;

impl TtlGreedoidsExt for Paniceinstein_merkle_tree {
    fn get_range_ttl_greedoids_namespaced(
        &self,
        namespaced: &str,
        start_soliton_id: &[u8],
        end_soliton_id: &[u8],
    ) -> Result<Vec<(String, TtlGreedoids)>> {
        panic!()
    }
}
