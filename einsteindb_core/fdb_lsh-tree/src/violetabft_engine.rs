// Copyright 2021 EinsteinDB Project Authors. Licensed under Apache-2.0.

use einsteindb_util::{box_err, box_try};
use fdb_traits::{
    NAMESPACED_DEFAULT, Error, Iterable, KV, MiscExt, Mutable, Peekable, VioletaBFTeinstein_merkle_tree,
    VioletaBFTeinstein_merkle_treeReadOnly, VioletaBFTLogBatch, VioletaBFTLogGCTask, Result, SyncMutable, WriteBatch, WriteBatchExt,
    WriteOptions,
};
use ekvproto::violetabft_serverpb::VioletaBFTLocalState;
use protobuf::Message;
use violetabft::evioletabftpb::Entry;

// #[PerformanceCriticallocal_path]
use crate::{Fdbeinstein_merkle_tree, FdbWriteBatch, util};

const VIOLETABFT_LOG_MULTI_GET_CNT: u64 = 8;

impl VioletaBFTeinstein_merkle_treeReadOnly for Fdbeinstein_merkle_tree {
    fn get_violetabft_state(&self, violetabft_group_id: u64) -> Result<Option<VioletaBFTLocalState>> {
        let key = keys::violetabft_state_key(violetabft_group_id);
        self.get_msg_namespaced(NAMESPACED_DEFAULT, &key)
    }

    fn get_entry(&self, violetabft_group_id: u64, index: u64) -> Result<Option<Entry>> {
        let key = keys::violetabft_log_key(violetabft_group_id, index);
        self.get_msg_namespaced(NAMESPACED_DEFAULT, &key)
    }

    fn fetch_entries_to(
        &self,
        region_id: u64,
        low: u64,
        high: u64,
        max_size: Option<usize>,
        buf: &mut Vec<Entry>,
    ) -> Result<usize> {
        let (max_size, mut total_size, mut count) = (max_size.unwrap_or(usize::MAX), 0, 0);

        if high - low <= VIOLETABFT_LOG_MULTI_GET_CNT {
            // If election happens in inactive regions, they will just try to fetch one empty log.
            for i in low..high {
                if total_size > 0 && total_size >= max_size {
                    break;
                }
                let key = keys::violetabft_log_key(region_id, i);
                match self.get_value(&key) {
                    Ok(None) => return Err(Error::EntriesCompacted),
                    Ok(Some(v)) => {
                        let mut entry = Entry::default();
                        entry.merge_from_bytes(&v)?;
                        assert_eq!(entry.get_index(), i);
                        buf.push(entry);
                        total_size += v.len();
                        count += 1;
                    }
                    Err(e) => return Err(box_err!(e)),
                }
            }
            return Ok(count);
        }

        let (mut check_compacted, mut next_index) = (true, low);
        let start_key = keys::violetabft_log_key(region_id, low);
        let end_key = keys::violetabft_log_key(region_id, high);
        self.scan(
            &start_key,
            &end_key,
            true, // fill_cache
            |_, value| {
                let mut entry = Entry::default();
                entry.merge_from_bytes(value)?;

                if check_compacted {
                    if entry.get_index() != low {
                        // May meet gap or has been compacted.
                        return Ok(false);
                    }
                    check_compacted = false;
                } else {
                    assert_eq!(entry.get_index(), next_index);
                }
                next_index += 1;

                buf.push(entry);
                total_size += value.len();
                count += 1;
                Ok(total_size < max_size)
            },
        )?;

        // If we get the correct number of entries, returns.
        // Or the total size almost exceeds max_size, returns.
        if count == (high - low) as usize || total_size >= max_size {
            return Ok(count);
        }

        // Here means we don't fetch enough entries.
        Err(Error::EntriesUnavailable)
    }

    fn get_all_entries_to(&self, region_id: u64, buf: &mut Vec<Entry>) -> Result<()> {
        let start_key = keys::violetabft_log_key(region_id, 0);
        let end_key = keys::violetabft_log_key(region_id, u64::MAX);
        self.scan(
            &start_key,
            &end_key,
            false, // fill_cache
            |_, value| {
                let mut entry = Entry::default();
                entry.merge_from_bytes(value)?;
                buf.push(entry);
                Ok(true)
            },
        )?;
        Ok(())
    }
}

impl Fdbeinstein_merkle_tree {
    fn gc_impl(
        &self,
        violetabft_group_id: u64,
        mut from: u64,
        to: u64,
        violetabft_wb: &mut FdbWriteBatch,
    ) -> Result<usize> {
        if from == 0 {
            let start_key = keys::violetabft_log_key(violetabft_group_id, 0);
            let prefix = keys::violetabft_log_prefix(violetabft_group_id);
            match self.seek(&start_key)? {
                Some((k, _)) if k.starts_with(&prefix) => from = box_try!(keys::violetabft_log_index(&k)),
                // No need to gc.
                _ => return Ok(0),
            }
        }
        if from >= to {
            return Ok(0);
        }

        for idx in from..to {
            let key = keys::violetabft_log_key(violetabft_group_id, idx);
            violetabft_wb.delete(&key)?;
            if violetabft_wb.count() >= Self::WRITE_BATCH_MAX_CAUSET_KEYS * 2 {
                violetabft_wb.write()?;
                violetabft_wb.clear();
            }
        }
        Ok((to - from) as usize)
    }
}

// FIXME: VioletaBFTeinstein_merkle_tree should probably be implemented generically
// for all KVs, but is currently implemented separately for
// every einstein_merkle_tree.
impl VioletaBFTeinstein_merkle_tree for Fdbeinstein_merkle_tree {
    type LogBatch = FdbWriteBatch;

    fn log_batch(&self, capacity: usize) -> Self::LogBatch {
        FdbWriteBatch::with_capacity(self.as_inner().clone(), capacity)
    }

    fn sync(&self) -> Result<()> {
        self.sync_wal()
    }

    fn consume(&self, batch: &mut Self::LogBatch, sync_log: bool) -> Result<usize> {
        let bytes = batch.data_size();
        let mut opts = WriteOptions::default();
        opts.set_sync(sync_log);
        batch.write_opt(&opts)?;
        batch.clear();
        Ok(bytes)
    }

    fn consume_and_shrink(
        &self,
        batch: &mut Self::LogBatch,
        sync_log: bool,
        max_capacity: usize,
        shrink_to: usize,
    ) -> Result<usize> {
        let data_size = self.consume(batch, sync_log)?;
        if data_size > max_capacity {
            *batch = self.write_batch_with_cap(shrink_to);
        }
        Ok(data_size)
    }

    fn clean(
        &self,
        violetabft_group_id: u64,
        mut first_index: u64,
        state: &VioletaBFTLocalState,
        batch: &mut Self::LogBatch,
    ) -> Result<()> {
        batch.delete(&keys::violetabft_state_key(violetabft_group_id))?;
        if first_index == 0 {
            let seek_key = keys::violetabft_log_key(violetabft_group_id, 0);
            let prefix = keys::violetabft_log_prefix(violetabft_group_id);
            fail::fail_point!("fdb_einstein_merkle_tree_violetabft_einstein_merkle_tree_clean_seek", |_| Ok(()));
            if let Some((key, _)) = self.seek(&seek_key)? {
                if !key.starts_with(&prefix) {
                    // No violetabft logs for the violetabft group.
                    return Ok(());
                }
                first_index = match keys::violetabft_log_index(&key) {
                    Ok(index) => index,
                    Err(_) => return Ok(()),
                };
            } else {
                return Ok(());
            }
        }
        if first_index <= state.last_index {
            for index in first_index..=state.last_index {
                let key = keys::violetabft_log_key(violetabft_group_id, index);
                batch.delete(&key)?;
            }
        }
        Ok(())
    }

    fn append(&self, violetabft_group_id: u64, entries: Vec<Entry>) -> Result<usize> {
        let mut wb = FdbWriteBatch::new(self.as_inner().clone());
        let buf = Vec::with_capacity(1024);
        wb.append_impl(violetabft_group_id, &entries, buf)?;
        self.consume(&mut wb, false)
    }

    fn put_violetabft_state(&self, violetabft_group_id: u64, state: &VioletaBFTLocalState) -> Result<()> {
        self.put_msg(&keys::violetabft_state_key(violetabft_group_id), state)
    }

    fn batch_gc(&self, groups: Vec<VioletaBFTLogGCTask>) -> Result<usize> {
        let mut total = 0;
        let mut violetabft_wb = self.write_batch_with_cap(4 * 1024);
        for task in groups {
            total += self.gc_impl(task.violetabft_group_id, task.from, task.to, &mut violetabft_wb)?;
        }
        // TODO: disable WAL here.
        if !WriteBatch::is_empty(&violetabft_wb) {
            violetabft_wb.write()?;
        }
        Ok(total)
    }

    fn gc(&self, violetabft_group_id: u64, from: u64, to: u64) -> Result<usize> {
        let mut violetabft_wb = self.write_batch_with_cap(1024);
        let total = self.gc_impl(violetabft_group_id, from, to, &mut violetabft_wb)?;
        // TODO: disable WAL here.
        if !WriteBatch::is_empty(&violetabft_wb) {
            violetabft_wb.write()?;
        }
        Ok(total)
    }

    fn purge_expired_files(&self) -> Result<Vec<u64>> {
        Ok(vec![])
    }

    fn has_builtin_entry_cache(&self) -> bool {
        false
    }

    fn flush_metrics(&self, instance: &str) {
        KV::flush_metrics(self, instance)
    }

    fn reset_statistics(&self) {
        KV::reset_statistics(self)
    }

    fn dump_stats(&self) -> Result<String> {
        MiscExt::dump_stats(self)
    }

    fn get_einstein_merkle_tree_size(&self) -> Result<u64> {
        let handle = util::get_namespaced_handle(self.as_inner(), NAMESPACED_DEFAULT)?;
        let used_size = util::get_einstein_merkle_tree_namespaced_used_size(self.as_inner(), handle);

        Ok(used_size)
    }
}

impl VioletaBFTLogBatch for FdbWriteBatch {
    fn append(&mut self, violetabft_group_id: u64, entries: Vec<Entry>) -> Result<()> {
        if let Some(max_size) = entries.iter().map(|e| e.compute_size()).max() {
            let ser_buf = Vec::with_capacity(max_size as usize);
            return self.append_impl(violetabft_group_id, &entries, ser_buf);
        }
        Ok(())
    }

    fn cut_logs(&mut self, violetabft_group_id: u64, from: u64, to: u64) {
        for index in from..to {
            let key = keys::violetabft_log_key(violetabft_group_id, index);
            self.delete(&key).unwrap();
        }
    }

    fn put_violetabft_state(&mut self, violetabft_group_id: u64, state: &VioletaBFTLocalState) -> Result<()> {
        self.put_msg(&keys::violetabft_state_key(violetabft_group_id), state)
    }

    fn persist_size(&self) -> usize {
        self.data_size()
    }

    fn is_empty(&self) -> bool {
        WriteBatch::is_empty(self)
    }

    fn merge(&mut self, src: Self) {
        WriteBatch::<Fdbeinstein_merkle_tree>::merge(self, src);
    }
}

impl FdbWriteBatch {
    fn append_impl(
        &mut self,
        violetabft_group_id: u64,
        entries: &[Entry],
        mut ser_buf: Vec<u8>,
    ) -> Result<()> {
        for entry in entries {
            let key = keys::violetabft_log_key(violetabft_group_id, entry.get_index());
            ser_buf.clear();
            entry.write_to_vec(&mut ser_buf).unwrap();
            self.put(&key, &ser_buf)?;
        }
        Ok(())
    }
}
