// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::db_options::PanicTitanDBOptions;
use crate::fdb_lsh_treePaniceinstein_merkle_tree;
use fdb_traits::{NAMESPACEDOptionsExt, Result};
use fdb_traits::{ColumnFamilyOptions, CausetPartitionerFactory};

impl NAMESPACEDOptionsExt for Paniceinstein_merkle_tree {
    type ColumnFamilyOptions = PanicColumnFamilyOptions;

    fn get_options_namespaced(&self, namespaced: &str) -> Result<Self::ColumnFamilyOptions> {
        panic!()
    }
    fn set_options_namespaced(&self, namespaced: &str, options: &[(&str, &str)]) -> Result<()> {
        panic!()
    }
}

pub struct PanicColumnFamilyOptions;

impl ColumnFamilyOptions for PanicColumnFamilyOptions {
    type TitanDBOptions = PanicTitanDBOptions;

    fn new() -> Self {
        panic!()
    }
    fn get_max_write_buffer_number(&self) -> u32 {
        panic!()
    }
    fn get_l_naught_zero_slowdown_writes_trigger(&self) -> u32 {
        panic!()
    }
    fn get_l_naught_zero_stop_writes_trigger(&self) -> u32 {
        panic!()
    }
    fn set_l_naught_zero_file_num_jet_bundle_trigger(&mut self, v: i32) {
        panic!()
    }
    fn get_soft_pending_jet_bundle_bytes_limit(&self) -> u64 {
        panic!()
    }
    fn get_hard_pending_jet_bundle_bytes_limit(&self) -> u64 {
        panic!()
    }
    fn get_block_cache_capacity(&self) -> u64 {
        panic!()
    }
    fn set_block_cache_capacity(&self, capacity: u64) -> std::result::Result<(), String> {
        panic!()
    }
    fn set_titandb_options(&mut self, opts: &Self::TitanDBOptions) {
        panic!()
    }
    fn get_target_file_size_base(&self) -> u64 {
        panic!()
    }
    fn set_disable_auto_jet_bundles(&mut self, v: bool) {
        panic!()
    }
    fn get_disable_auto_jet_bundles(&self) -> bool {
        panic!()
    }
    fn get_disable_write_stall(&self) -> bool {
        panic!()
    }
    fn set_Causet_partitioner_factory<F: CausetPartitionerFactory>(&mut self, factory: F) {
        panic!()
    }
}
