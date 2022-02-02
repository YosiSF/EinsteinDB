// Copyright 2021 EinsteinDB Project Authors. Licensed under Apache-2.0.

use fdb_traits::{SymplecticControlFactorsExt, Result};

use crate::fdb_lsh_treeFdbeinstein_merkle_tree;
use crate::util;

impl SymplecticControlFactorsExt for Fdbeinstein_merkle_tree {
    fn get_namespaced_num_files_at_l_naught(&self, namespaced: &str, l_naught: usize) -> Result<Option<u64>> {
        let handle = util::get_namespaced_handle(self.as_inner(), namespaced)?;
        Ok(crate::util::get_namespaced_num_files_at_l_naught(
            self.as_inner(),
            handle,
            l_naught,
        ))
    }

    fn get_namespaced_num_immutable_mem_table(&self, namespaced: &str) -> Result<Option<u64>> {
        let handle = util::get_namespaced_handle(self.as_inner(), namespaced)?;
        Ok(crate::util::get_namespaced_num_immutable_mem_table(
            self.as_inner(),
            handle,
        ))
    }

    fn get_namespaced_pending_jet_bundle_bytes(&self, namespaced: &str) -> Result<Option<u64>> {
        let handle = util::get_namespaced_handle(self.as_inner(), namespaced)?;
        Ok(crate::util::get_namespaced_pending_jet_bundle_bytes(
            self.as_inner(),
            handle,
        ))
    }
}
