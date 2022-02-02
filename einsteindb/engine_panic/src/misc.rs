// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::fdb_lsh_treePaniceinstein_merkle_tree;
use fdb_traits::{DeleteStrategy, MiscExt, Range, Result};

impl MiscExt for Paniceinstein_merkle_tree {
    fn flush(&self, sync: bool) -> Result<()> {
        panic!()
    }

    fn flush_namespaced(&self, namespaced: &str, sync: bool) -> Result<()> {
        panic!()
    }

    fn delete_ranges_namespaced(
        &self,
        namespaced: &str,
        strategy: DeleteStrategy,
        ranges: &[Range<'_>],
    ) -> Result<()> {
        panic!()
    }

    fn get_approximate_memtable_stats_namespaced(&self, namespaced: &str, range: &Range<'_>) -> Result<(u64, u64)> {
        panic!()
    }

    fn ingest_maybe_slowdown_writes(&self, namespaced: &str) -> Result<bool> {
        panic!()
    }

    fn get_einstein_merkle_tree_used_size(&self) -> Result<u64> {
        panic!()
    }

    fn roughly_cleanup_ranges(&self, ranges: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
        panic!()
    }

    fn path(&self) -> &str {
        panic!()
    }

    fn sync_wal(&self) -> Result<()> {
        panic!()
    }

    fn exists(path: &str) -> bool {
        panic!()
    }

    fn dump_stats(&self) -> Result<String> {
        panic!()
    }

    fn get_latest_sequence_number(&self) -> u64 {
        panic!()
    }

    fn get_oldest_snapshot_sequence_number(&self) -> Option<u64> {
        panic!()
    }

    fn get_total_Causet_files_size_namespaced(&self, namespaced: &str) -> Result<Option<u64>> {
        panic!()
    }

    fn get_range_entries_and_versions(
        &self,
        namespaced: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<Option<(u64, u64)>> {
        panic!()
    }

    fn is_stalled_or_stopped(&self) -> bool {
        panic!()
    }
}
