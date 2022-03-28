// Copyright 2021 EinsteinDB Project Authors. Licensed under Apache-2.0.

use fdb_traits::{Result, SymplecticControlFactorsExt};

use crate::fdb_lsh_treePaniceinstein_merkle_tree;

impl SymplecticControlFactorsExt for Paniceinstein_merkle_tree {
    fn get_namespaced_num_filefs_at_l_naught(&self, namespaced: &str, l_naught: usize) -> Result<Option<u64>> {
        panic!()
    }

    fn get_namespaced_num_immutable_mem_table(&self, namespaced: &str) -> Result<Option<u64>> {
        panic!()
    }

    fn get_namespaced_pending_jet_bundle_bytes(&self, namespaced: &str) -> Result<Option<u64>> {
        panic!()
    }
}
