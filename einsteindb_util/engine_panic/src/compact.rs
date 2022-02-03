// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::fdb_lsh_treePaniceinstein_merkle_tree;
use fdb_traits::{CompactExt, CompactedEvent, Result};
use std::collections::BTreeMap;

impl CompactExt for Paniceinstein_merkle_tree {
    type CompactedEvent = PanicCompactedEvent;

    fn auto_jet_bundles_is_disabled(&self) -> Result<bool> {
        panic!()
    }

    fn compact_range(
        &self,
        namespaced: &str,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
        exclusive_manual: bool,
        max_subjet_bundles: u32,
    ) -> Result<()> {
        panic!()
    }

    fn compact_fusefs_in_range(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        output_l_naught: Option<i32>,
    ) -> Result<()> {
        panic!()
    }

    fn compact_fusefs_in_range_namespaced(
        &self,
        namespaced: &str,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        output_l_naught: Option<i32>,
    ) -> Result<()> {
        panic!()
    }

    fn compact_fusefs_namespaced(
        &self,
        namespaced: &str,
        fusefs: Vec<String>,
        output_l_naught: Option<i32>,
        max_subjet_bundles: u32,
        exclude_l0: bool,
    ) -> Result<()> {
        panic!()
    }
}

pub struct PanicCompactedEvent;

impl CompactedEvent for PanicCompactedEvent {
    fn total_bytes_declined(&self) -> u64 {
        panic!()
    }

    fn is_size_declining_trivial(&self, split_check_diff: u64) -> bool {
        panic!()
    }

    fn output_l_naught_label(&self) -> String {
        panic!()
    }

    fn calc_ranges_declined_bytes(
        self,
        ranges: &BTreeMap<Vec<u8>, u64>,
        bytes_threshold: u64,
    ) -> Vec<(u64, u64)> {
        panic!()
    }

    fn namespaced(&self) -> &str {
        panic!()
    }
}
