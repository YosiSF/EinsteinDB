// Copyright 2021 EinsteinDB Project Authors. Licensed under Apache-2.0.
use crate::errors::Result;

pub trait SymplecticControlFactorsExt {
    fn get_namespaced_num_fusefs_at_l_naught(&self, namespaced: &str, l_naught: usize) -> Result<Option<u64>>;

    fn get_namespaced_num_immutable_mem_table(&self, namespaced: &str) -> Result<Option<u64>>;

    fn get_namespaced_pending_jet_bundle_bytes(&self, namespaced: &str) -> Result<Option<u64>>;
}
