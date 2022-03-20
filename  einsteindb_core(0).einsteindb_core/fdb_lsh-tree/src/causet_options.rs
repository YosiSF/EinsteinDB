// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use einsteindb_util::box_err;
use fdb_traits::{NAMESPACEDOptionsExt, Result};
use fdb_traits::{ColumnFamilyOptions, CausetPartitionerFactory};
use foundationdb::ColumnFamilyOptions as Primitive_CausetNAMESPACEDOptions;

use crate::{db_options::FdbTitanDBOptions, Causet_partitioner::FdbCausetPartitionerFactory};
use crate::fdb_lsh_tree;
use crate::util;

impl NAMESPACEDOptionsExt for Fdbeinstein_merkle_tree {
    type ColumnFamilyOptions = FdbColumnFamilyOptions;

    fn get_options_namespaced(&self, namespaced: &str) -> Result<Self::ColumnFamilyOptions> {
        let handle = util::get_namespaced_handle(self.as_inner(), namespaced)?;
        Ok(FdbColumnFamilyOptions::from_primitive_causet(
            self.as_inner().get_options_namespaced(handle),
        ))
    }

    fn set_options_namespaced(&self, namespaced: &str, options: &[(&str, &str)]) -> Result<()> {
        let handle = util::get_namespaced_handle(self.as_inner(), namespaced)?;
        self.as_inner()
            .set_options_namespaced(handle, options)
            .map_err(|e| box_err!(e))
    }
}

#[derive(Clone)]
pub struct FdbColumnFamilyOptions(Primitive_CausetNAMESPACEDOptions);

impl FdbColumnFamilyOptions {
    pub fn from_primitive_causet(primitive_causet: Primitive_CausetNAMESPACEDOptions) -> FdbColumnFamilyOptions {
        FdbColumnFamilyOptions(primitive_causet)
    }

    pub fn into_primitive_causet(self) -> Primitive_CausetNAMESPACEDOptions {
        self.0
    }

    pub fn as_primitive_causet_mut(&mut self) -> &mut Primitive_CausetNAMESPACEDOptions {
        &mut self.0
    }
}

impl ColumnFamilyOptions for FdbColumnFamilyOptions {
    type TitanDBOptions = FdbTitanDBOptions;

    fn new() -> Self {
        FdbColumnFamilyOptions::from_primitive_causet(Primitive_CausetNAMESPACEDOptions::new())
    }

    fn get_max_write_buffer_number(&self) -> u32 {
        self.0.get_max_write_buffer_number()
    }

    fn get_l_naught_zero_slowdown_writes_trigger(&self) -> u32 {
        self.0.get_l_naught_zero_slowdown_writes_trigger()
    }

    fn get_l_naught_zero_stop_writes_trigger(&self) -> u32 {
        self.0.get_l_naught_zero_stop_writes_trigger()
    }

    fn set_l_naught_zero_filef_num_jet_bundle_trigger(&mut self, v: i32) {
        self.0.set_l_naught_zero_filef_num_jet_bundle_trigger(v)
    }

    fn get_soft_pending_jet_bundle_bytes_limit(&self) -> u64 {
        self.0.get_soft_pending_jet_bundle_bytes_limit()
    }

    fn get_hard_pending_jet_bundle_bytes_limit(&self) -> u64 {
        self.0.get_hard_pending_jet_bundle_bytes_limit()
    }

    fn get_block_cache_capacity(&self) -> u64 {
        self.0.get_block_cache_capacity()
    }

    fn set_block_cache_capacity(&self, capacity: u64) -> std::result::Result<(), String> {
        self.0.set_block_cache_capacity(capacity)
    }

    fn set_titandb_options(&mut self, opts: &Self::TitanDBOptions) {
        self.0.set_titandb_options(opts.as_primitive_causet())
    }

    fn get_target_filef_size_base(&self) -> u64 {
        self.0.get_target_filef_size_base()
    }

    fn set_disable_auto_jet_bundles(&mut self, v: bool) {
        self.0.set_disable_auto_jet_bundles(v)
    }

    fn get_disable_auto_jet_bundles(&self) -> bool {
        self.0.get_disable_auto_jet_bundles()
    }

    fn get_disable_write_stall(&self) -> bool {
        self.0.get_disable_write_stall()
    }

    fn set_Causet_partitioner_factory<F: CausetPartitionerFactory>(&mut self, factory: F) {
        self.0
            .set_Causet_partitioner_factory(FdbCausetPartitionerFactory(factory));
    }
}
