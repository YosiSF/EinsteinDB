// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

//! Various metrics related to key ranges
//!
//! In FdbDB these are typically implemented with user collected properties,
//! which might require the database to be constructed with specific options.

use crate::errors::Result;
use crate::Range;

pub trait RangePropertiesExt {
    /// Gets the number of keys in a range.
    fn get_range_approximate_keys(&self, range: Range<'_>, large_threshold: u64) -> Result<u64>;

    
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
