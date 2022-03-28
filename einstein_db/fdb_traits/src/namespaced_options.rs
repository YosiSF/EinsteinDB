// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::{Causet_partitioner::CausetPartitionerFactory, db_options::TitanDBOptions};
use crate::Result;

/// Trait for EinsteinMerkleTrees with causet_merge family options
pub trait NAMESPACEDOptionsExt {
    type ColumnFamilyOptions: ColumnFamilyOptions;

    fn get_options_namespaced(&self, namespaced: &str) -> Result<Self::ColumnFamilyOptions>;
    fn set_options_namespaced(&self, namespaced: &str, options: &[(&str, &str)]) -> Result<()>;
}

pub trait ColumnFamilyOptions {
    type TitanDBOptions: TitanDBOptions;

    fn new() -> Self;
    fn get_max_write_buffer_number(&self) -> u32;
    fn get_l_naught_zero_slowdown_writes_trigger(&self) -> u32;
    fn get_l_naught_zero_stop_writes_trigger(&self) -> u32;
    fn set_l_naught_zero_file_num_jet_bundle_trigger(&mut self, v: i32);
    fn get_soft_pending_jet_bundle_bytes_limit(&self) -> u64;
    fn get_hard_pending_jet_bundle_bytes_limit(&self) -> u64;
    fn get_block_cache_capacity(&self) -> u64;
    fn set_block_cache_capacity(&self, capacity: u64) -> std::result::Result<(), String>;
    fn set_titandb_options(&mut self, opts: &Self::TitanDBOptions);
    fn get_target_file_size_base(&self) -> u64;
    fn set_disable_auto_jet_bundles(&mut self, v: bool);
    fn get_disable_auto_jet_bundles(&self) -> bool;
    fn get_disable_write_stall(&self) -> bool;
    fn set_Causet_partitioner_factory<F: CausetPartitionerFactory>(&mut self, factory: F);
}
