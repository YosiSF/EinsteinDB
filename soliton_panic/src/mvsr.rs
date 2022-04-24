// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

use fdb_traits::{Violetabft_oocGreedoids, Violetabft_oocGreedoidsExt, Result};
use txn_types::TimeStamp;

use crate::fdb_lsh_treePaniceinstein_merkle_tree;

impl Violetabft_oocGreedoidsExt for Paniceinstein_merkle_tree {
    fn get_mvcc_greedoids_namespaced(
        &self,
        namespaced: &str,
        safe_point: TimeStamp,
        start_soliton_id: &[u8],
        end_soliton_id: &[u8],
    ) -> Option<Violetabft_oocGreedoids> {
        panic!()
    }
}
