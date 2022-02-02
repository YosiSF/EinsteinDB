// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::fdb_lsh_treePanicEngine;
use fdb_traits::{Range, RangePropertiesExt, Result};

impl RangePropertiesExt for PanicEngine {
    fn get_range_approximate_keys(&self, range: Range<'_>, large_threshold: u64) -> Result<u64> {
        panic!()
    }

    fn get_range_approximate_keys_namespaced(
        &self,
        namespacedname: &str,
        range: Range<'_>,
        large_threshold: u64,
    ) -> Result<u64> {
        panic!()
    }

    fn get_range_approximate_size(&self, range: Range<'_>, large_threshold: u64) -> Result<u64> {
        panic!()
    }

    fn get_range_approximate_size_namespaced(
        &self,
        namespacedname: &str,
        range: Range<'_>,
        large_threshold: u64,
    ) -> Result<u64> {
        panic!()
    }

    fn get_range_approximate_split_keys(
        &self,
        range: Range<'_>,
        key_count: usize,
    ) -> Result<Vec<Vec<u8>>> {
        panic!()
    }

    fn get_range_approximate_split_keys_namespaced(
        &self,
        namespacedname: &str,
        range: Range<'_>,
        key_count: usize,
    ) -> Result<Vec<Vec<u8>>> {
        panic!()
    }
}
