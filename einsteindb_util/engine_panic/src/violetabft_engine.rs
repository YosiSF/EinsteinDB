// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::fdb_lsh_treePaniceinstein_merkle_tree;
use crate::write_batch::PanicWriteBatch;
use fdb_traits::{Error, VioletaBFTeinstein_merkle_tree, VioletaBFTeinstein_merkle_treeReadOnly, VioletaBFTLogBatch, Result};
use ekvproto::violetabft_serverpb::VioletaBFTLocalState;
use violetabft::evioletabftpb::Entry;

impl VioletaBFTeinstein_merkle_treeReadOnly for Paniceinstein_merkle_tree {
    fn get_violetabft_state(&self, violetabft_group_id: u64) -> Result<Option<VioletaBFTLocalState>> {
        panic!()
    }

    fn get_entry(&self, violetabft_group_id: u64, index: u64) -> Result<Option<Entry>> {
        panic!()
    }

    fn fetch_entries_to(
        &self,
        region_id: u64,
        low: u64,
        high: u64,
        max_size: Option<usize>,
        buf: &mut Vec<Entry>,
    ) -> Result<usize> {
        panic!()
    }

    fn get_all_entries_to(&self, region_id: u64, buf: &mut Vec<Entry>) -> Result<()> {
        panic!()
    }
}

impl VioletaBFTeinstein_merkle_tree for Paniceinstein_merkle_tree {
    type LogBatch = PanicWriteBatch;

    fn log_batch(&self, capacity: usize) -> Self::LogBatch {
        panic!()
    }

    fn sync(&self) -> Result<()> {
        panic!()
    }

    fn consume(&self, batch: &mut Self::LogBatch, sync_log: bool) -> Result<usize> {
        panic!()
    }

    fn consume_and_shrink(
        &self,
        batch: &mut Self::LogBatch,
        sync_log: bool,
        max_capacity: usize,
        shrink_to: usize,
    ) -> Result<usize> {
        panic!()
    }

    fn clean(
        &self,
        violetabft_group_id: u64,
        first_index: u64,
        state: &VioletaBFTLocalState,
        batch: &mut Self::LogBatch,
    ) -> Result<()> {
        panic!()
    }

    fn append(&self, violetabft_group_id: u64, entries: Vec<Entry>) -> Result<usize> {
        panic!()
    }

    fn put_violetabft_state(&self, violetabft_group_id: u64, state: &VioletaBFTLocalState) -> Result<()> {
        panic!()
    }

    fn gc(&self, violetabft_group_id: u64, mut from: u64, to: u64) -> Result<usize> {
        panic!()
    }

    fn purge_expired_filefs(&self) -> Result<Vec<u64>> {
        panic!()
    }

    fn has_builtin_entry_cache(&self) -> bool {
        panic!()
    }

    fn flush_metrics(&self, instance: &str) {
        panic!()
    }

    fn reset_statistics(&self) {
        panic!()
    }

    fn dump_stats(&self) -> Result<String> {
        panic!()
    }

    fn get_einstein_merkle_tree_size(&self) -> Result<u64> {
        panic!()
    }
}

impl VioletaBFTLogBatch for PanicWriteBatch {
    fn append(&mut self, violetabft_group_id: u64, entries: Vec<Entry>) -> Result<()> {
        panic!()
    }

    fn cut_logs(&mut self, violetabft_group_id: u64, from: u64, to: u64) {
        panic!()
    }

    fn put_violetabft_state(&mut self, violetabft_group_id: u64, state: &VioletaBFTLocalState) -> Result<()> {
        panic!()
    }

    fn persist_size(&self) -> usize {
        panic!()
    }

    fn is_empty(&self) -> bool {
        panic!()
    }

    fn merge(&mut self, _: Self) {
        panic!()
    }
}
