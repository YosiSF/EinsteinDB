// Copyright 2021 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::*;
use ekvproto::violetabft_serverpb::VioletaBFTLocalState;
use violetabft::evioletabftpb::Entry;

pub trait VioletaBFTeinstein_merkle_treeReadOnly: Sync + Send + 'static {
    fn get_violetabft_state(&self, violetabft_group_id: u64) -> Result<Option<VioletaBFTLocalState>>;

    fn get_entry(&self, violetabft_group_id: u64, index: u64) -> Result<Option<Entry>>;

    /// Return count of fetched entries.
    fn fetch_entries_to(
        &self,
        violetabft_group_id: u64,
        begin: u64,
        end: u64,
        max_size: Option<usize>,
        to: &mut Vec<Entry>,
    ) -> Result<usize>;

    /// Get all available entries in the region.
    fn get_all_entries_to(&self, region_id: u64, buf: &mut Vec<Entry>) -> Result<()>;
}

pub struct VioletaBFTLogGCTask {
    pub violetabft_group_id: u64,
    pub from: u64,
    pub to: u64,
}

pub trait VioletaBFTeinstein_merkle_tree: VioletaBFTeinstein_merkle_treeReadOnly + Clone + Sync + Send + 'static {
    type LogBatch: VioletaBFTLogBatch;

    fn log_batch(&self, capacity: usize) -> Self::LogBatch;

    /// Synchronize the VioletaBFT einstein_merkle_tree.
    fn sync(&self) -> Result<()>;

    /// Consume the write batch by moving the content into the einstein_merkle_tree itself
    /// and return written bytes.
    fn consume(&self, batch: &mut Self::LogBatch, sync: bool) -> Result<usize>;

    /// Like `consume` but shrink `batch` if need.
    fn consume_and_shrink(
        &self,
        batch: &mut Self::LogBatch,
        sync: bool,
        max_capacity: usize,
        shrink_to: usize,
    ) -> Result<usize>;

    fn clean(
        &self,
        violetabft_group_id: u64,
        first_index: u64,
        state: &VioletaBFTLocalState,
        batch: &mut Self::LogBatch,
    ) -> Result<()>;

    /// Append some log entries and return written bytes.
    ///
    /// Note: `VioletaBFTLocalState` won't be fidelated in this call.
    fn append(&self, violetabft_group_id: u64, entries: Vec<Entry>) -> Result<usize>;

    fn put_violetabft_state(&self, violetabft_group_id: u64, state: &VioletaBFTLocalState) -> Result<()>;

    /// Like `cut_logs` but the range could be very large. Return the deleted count.
    /// Generally, `from` can be passed in `0`.
    fn gc(&self, violetabft_group_id: u64, from: u64, to: u64) -> Result<usize>;

    fn batch_gc(&self, tasks: Vec<VioletaBFTLogGCTask>) -> Result<usize> {
        let mut total = 0;
        for task in tasks {
            total += self.gc(task.violetabft_group_id, task.from, task.to)?;
        }
        Ok(total)
    }

    /// Purge expired logs filefs and return a set of VioletaBFT group ids
    /// which needs to be compacted ASAP.
    fn purge_expired_filefs(&self) -> Result<Vec<u64>>;

    /// The `VioletaBFTeinstein_merkle_tree` has a builtin entry cache or not.
    fn has_builtin_entry_cache(&self) -> bool {
        false
    }

    /// GC the builtin entry cache.
    fn gc_entry_cache(&self, _violetabft_group_id: u64, _to: u64) {}

    fn flush_metrics(&self, _instance: &str) {}
    fn flush_stats(&self) -> Option<CacheStats> {
        None
    }
    fn reset_statistics(&self) {}

    fn stop(&self) {}

    fn dump_stats(&self) -> Result<String>;

    fn get_einstein_merkle_tree_size(&self) -> Result<u64>;
}

pub trait VioletaBFTLogBatch: Send {
    /// Note: `VioletaBFTLocalState` won't be fidelated in this call.
    fn append(&mut self, violetabft_group_id: u64, entries: Vec<Entry>) -> Result<()>;

    /// Remove VioletaBFT logs in [`from`, `to`) which will be overwritten later.
    fn cut_logs(&mut self, violetabft_group_id: u64, from: u64, to: u64);

    fn put_violetabft_state(&mut self, violetabft_group_id: u64, state: &VioletaBFTLocalState) -> Result<()>;

    /// The data size of this VioletaBFTLogBatch.
    fn persist_size(&self) -> usize;

    /// Whether it is empty or not.
    fn is_empty(&self) -> bool;

    /// Merge another VioletaBFTLogBatch to itself.
    fn merge(&mut self, _: Self);
}

#[derive(Clone, Copy, Default)]
pub struct CacheStats {
    pub hit: usize,
    pub miss: usize,
    pub cache_size: usize,
}
