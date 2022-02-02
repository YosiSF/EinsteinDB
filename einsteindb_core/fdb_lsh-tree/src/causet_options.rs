// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use einsteindb_util::box_err;
use fdb_traits::{NAMESPACEDOptionsExt, Result};
use fdb_traits::{ColumnFamilyOptions, SstPartitionerFactory};
use foundationdb::ColumnFamilyOptions as RawNAMESPACEDOptions;

use crate::{db_options::FdbTitanDBOptions, sst_partitioner::FdbSstPartitionerFactory};
use crate::fdb_lsh_treeFdbEngine;
use crate::util;

impl NAMESPACEDOptionsExt for FdbEngine {
    type ColumnFamilyOptions = FdbColumnFamilyOptions;

    fn get_options_namespaced(&self, namespaced: &str) -> Result<Self::ColumnFamilyOptions> {
        let handle = util::get_namespaced_handle(self.as_inner(), namespaced)?;
        Ok(FdbColumnFamilyOptions::from_raw(
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
pub struct FdbColumnFamilyOptions(RawNAMESPACEDOptions);

impl FdbColumnFamilyOptions {
    pub fn from_raw(raw: RawNAMESPACEDOptions) -> FdbColumnFamilyOptions {
        FdbColumnFamilyOptions(raw)
    }

    pub fn into_raw(self) -> RawNAMESPACEDOptions {
        self.0
    }

    pub fn as_raw_mut(&mut self) -> &mut RawNAMESPACEDOptions {
        &mut self.0
    }
}

impl ColumnFamilyOptions for FdbColumnFamilyOptions {
    type TitanDBOptions = FdbTitanDBOptions;

    fn new() -> Self {
        FdbColumnFamilyOptions::from_raw(RawNAMESPACEDOptions::new())
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

    fn set_l_naught_zero_file_num_jet_bundle_trigger(&mut self, v: i32) {
        self.0.set_l_naught_zero_file_num_jet_bundle_trigger(v)
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
        self.0.set_titandb_options(opts.as_raw())
    }

    fn get_target_file_size_base(&self) -> u64 {
        self.0.get_target_file_size_base()
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

    fn set_sst_partitioner_factory<F: SstPartitionerFactory>(&mut self, factory: F) {
        self.0
            .set_sst_partitioner_factory(FdbSstPartitionerFactory(factory));
    }
}
