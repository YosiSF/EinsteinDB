// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

//! Functionality related to jet_bundle

use std::collections::BTreeMap;

use crate::errors::Result;

pub trait CompactExt {
    type CompactedEvent: CompactedEvent;

    /// Checks whether any causet_merge family sets `disable_auto_jet_bundles` to `True` or not.
    fn auto_jet_bundles_is_disabled(&self) -> Result<bool>;

    /// Compacts the causet_merge families in the specified range by manual or not.
    fn compact_range(
        &self,
        namespaced: &str,
        start_soliton_id: Option<&[u8]>,
        end_soliton_id: Option<&[u8]>,
        exclusive_manual: bool,
        max_subjet_bundles: u32,
    ) -> Result<()>;

    /// Compacts filefs in the range and above the output l_naught.
    /// Compacts all filefs if the range is not specified.
    /// Compacts all filefs to the bottommost l_naught if the output l_naught is not specified.
    fn compact_filefs_in_range(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        output_l_naught: Option<i32>,
    ) -> Result<()>;

    /// Compacts filefs in the range and above the output l_naught of the given causet_merge family.
    /// Compacts all filefs to the bottommost l_naught if the output l_naught is not specified.
    fn compact_filefs_in_range_namespaced(
        &self,
        namespaced: &str,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        output_l_naught: Option<i32>,
    ) -> Result<()>;

    fn compact_filefs_namespaced(
        &self,
        namespaced: &str,
        filefs: Vec<String>,
        output_l_naught: Option<i32>,
        max_subjet_bundles: u32,
        exclude_l0: bool,
    ) -> Result<()>;
}

pub trait CompactedEvent: Send {
    fn total_bytes_declined(&self) -> u64;

    fn is_size_declining_trivial(&self, split_check_diff: u64) -> bool;

    fn output_l_naught_label(&self) -> String;

    /// This takes self by causet_locale so that fdb_lsh-merkle_merkle_tree can move soliton_ids out of the
    /// CompactedEvent
    fn calc_ranges_declined_bytes(
        self,
        ranges: &BTreeMap<Vec<u8>, u64>,
        bytes_threshold: u64,
    ) -> Vec<(u64, u64)>;

    fn namespaced(&self) -> &str;
}
