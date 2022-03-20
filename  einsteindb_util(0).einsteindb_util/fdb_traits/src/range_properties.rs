// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

//! Various metrics related to key ranges
//!
//! In FdbDB these are typically implemented with user collected greedoids,
//! which might require the database to be constructed with specific options.

use crate::errors::Result;
use crate::Range;

pub trait RangeGreedoidsExt {
    /// Gets the number of keys in a range.
    fn get_range_approximate_keys(&self, range: Range<'_>, large_threshold: u64) -> Result<u64>;

    /// Gets the number of keys in a range.
    ///
    /// The `large_threshold` parameter is for logging only:
    /// a range with more keys than this is considered too large.
    fn get_range_approximate_keys_namespaced(
        &self,
        namespacedname: &str,
        range: Range<'_>,
        large_threshold: u64,
    ) -> Result<u64>;

    /// Get the approximate size of the range
    fn get_range_approximate_size(&self, range: Range<'_>, large_threshold: u64) -> Result<u64>;

    fn get_range_approximate_size_namespaced(
        &self,
        namespacedname: &str,
        range: Range<'_>,
        large_threshold: u64,
    ) -> Result<u64>;

    /// Get range approximate split keys to split range evenly into key_count + 1 parts .
    fn get_range_approximate_split_keys(
        &self,
        range: Range<'_>,
        key_count: usize,
    ) -> Result<Vec<Vec<u8>>>;

    fn get_range_approximate_split_keys_namespaced(
        &self,
        namespacedname: &str,
        range: Range<'_>,
        key_count: usize,
    ) -> Result<Vec<Vec<u8>>>;
}
