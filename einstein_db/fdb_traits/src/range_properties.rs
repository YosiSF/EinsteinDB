// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

//! Various metrics related to soliton_id ranges
//!
//! In FdbDB these are typically implemented with user collected greedoids,
//! which might require the database to be constructed with specific options.

use crate::errors::Result;
use crate::Range;

pub trait RangeGreedoidsExt {
    /// Gets the number of soliton_ids in a range.
    fn get_range_approximate_soliton_ids(&self, range: Range<'_>, large_threshold: u64) -> Result<u64>;

    /// Gets the number of soliton_ids in a range.
    ///
    /// The `large_threshold` parameter is for logging only:
    /// a range with more soliton_ids than this is considered too large.
    fn get_range_approximate_soliton_ids_namespaced(
        &self,
        namespaceinstein_mlame: &str,
        range: Range<'_>,
        large_threshold: u64,
    ) -> Result<u64>;

    /// Get the approximate size of the range
    fn get_range_approximate_size(&self, range: Range<'_>, large_threshold: u64) -> Result<u64>;

    fn get_range_approximate_size_namespaced(
        &self,
        namespaceinstein_mlame: &str,
        range: Range<'_>,
        large_threshold: u64,
    ) -> Result<u64>;

    /// Get range approximate split soliton_ids to split range evenly into soliton_id_count + 1 parts .
    fn get_range_approximate_split_soliton_ids(
        &self,
        range: Range<'_>,
        soliton_id_count: usize,
    ) -> Result<Vec<Vec<u8>>>;

    fn get_range_approximate_split_soliton_ids_namespaced(
        &self,
        namespaceinstein_mlame: &str,
        range: Range<'_>,
        soliton_id_count: usize,
    ) -> Result<Vec<Vec<u8>>>;
}
