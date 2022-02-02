// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use fdb_traits::{self, Error, Mutable, Result, WriteBatchExt, WriteOptions};
use foundationdb::{DB, Writable, WriteBatch as RawWriteBatch};
use std::sync::Arc;

use crate::fdb_lsh_treeFdbeinstein_merkle_tree;
use crate::options::FdbWriteOptions;
use crate::util::get_namespaced_handle;

const WRITE_BATCH_MAX_BATCH: usize = 16;
const WRITE_BATCH_LIMIT: usize = 16;

impl WriteBatchExt for Fdbeinstein_merkle_tree {
    type WriteBatch = FdbWriteBatch;
    type WriteBatchVec = FdbWriteBatchVec;

    const WRITE_BATCH_MAX_CAUSET_KEYS: usize = 256;

    fn support_write_batch_vec(&self) -> bool {
        let options = self.as_inner().get_db_options();
        options.is_enable_multi_batch_write()
    }

    fn write_batch(&self) -> Self::WriteBatch {
        Self::WriteBatch::new(Arc::clone(self.as_inner()))
    }

    fn write_batch_with_cap(&self, cap: usize) -> Self::WriteBatch {
        Self::WriteBatch::with_capacity(Arc::clone(self.as_inner()), cap)
    }
}

pub struct FdbWriteBatch {
    einsteindb: Arc<DB>,
    wb: RawWriteBatch,
}

impl FdbWriteBatch {
    pub fn new(einsteindb: Arc<DB>) -> FdbWriteBatch {
        FdbWriteBatch {
            einsteindb,
            wb: RawWriteBatch::default(),
        }
    }

    pub fn as_inner(&self) -> &RawWriteBatch {
        &self.wb
    }

    pub fn with_capacity(einsteindb: Arc<DB>, cap: usize) -> FdbWriteBatch {
        let wb = if cap == 0 {
            RawWriteBatch::default()
        } else {
            RawWriteBatch::with_capacity(cap)
        };
        FdbWriteBatch { einsteindb, wb }
    }

    pub fn from_raw(einsteindb: Arc<DB>, wb: RawWriteBatch) -> FdbWriteBatch {
        FdbWriteBatch { einsteindb, wb }
    }

    pub fn get_db(&self) -> &DB {
        self.einsteindb.as_ref()
    }

    pub fn merge(&mut self, src: &Self) {
        self.wb.append(src.wb.data());
    }
}

impl fdb_traits::WriteBatch<Fdbeinstein_merkle_tree> for FdbWriteBatch {
    fn with_capacity(e: &Fdbeinstein_merkle_tree, cap: usize) -> FdbWriteBatch {
        e.write_batch_with_cap(cap)
    }

    fn write_opt(&self, opts: &WriteOptions) -> Result<()> {
        let opt: FdbWriteOptions = opts.into();
        self.get_db()
            .write_opt(self.as_inner(), &opt.into_raw())
            .map_err(Error::einstein_merkle_tree)
    }

    fn data_size(&self) -> usize {
        self.wb.data_size()
    }

    fn count(&self) -> usize {
        self.wb.count()
    }

    fn is_empty(&self) -> bool {
        self.wb.is_empty()
    }

    fn should_write_to_einstein_merkle_tree(&self) -> bool {
        self.wb.count() > Fdbeinstein_merkle_tree::WRITE_BATCH_MAX_CAUSET_KEYS
    }

    fn clear(&mut self) {
        self.wb.clear();
    }

    fn set_save_point(&mut self) {
        self.wb.set_save_point();
    }

    fn pop_save_point(&mut self) -> Result<()> {
        self.wb.pop_save_point().map_err(Error::einstein_merkle_tree)
    }

    fn rollback_to_save_point(&mut self) -> Result<()> {
        self.wb.rollback_to_save_point().map_err(Error::einstein_merkle_tree)
    }

    fn merge(&mut self, src: Self) {
        self.wb.append(src.wb.data());
    }
}

impl Mutable for FdbWriteBatch {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.wb.put(key, value).map_err(Error::einstein_merkle_tree)
    }

    fn put_namespaced(&mut self, namespaced: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let handle = get_namespaced_handle(self.einsteindb.as_ref(), namespaced)?;
        self.wb.put_namespaced(handle, key, value).map_err(Error::einstein_merkle_tree)
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.wb.delete(key).map_err(Error::einstein_merkle_tree)
    }

    fn delete_namespaced(&mut self, namespaced: &str, key: &[u8]) -> Result<()> {
        let handle = get_namespaced_handle(self.einsteindb.as_ref(), namespaced)?;
        self.wb.delete_namespaced(handle, key).map_err(Error::einstein_merkle_tree)
    }

    fn delete_range(&mut self, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        self.wb
            .delete_range(begin_key, end_key)
            .map_err(Error::einstein_merkle_tree)
    }

    fn delete_range_namespaced(&mut self, namespaced: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        let handle = get_namespaced_handle(self.einsteindb.as_ref(), namespaced)?;
        self.wb
            .delete_range_namespaced(handle, begin_key, end_key)
            .map_err(Error::einstein_merkle_tree)
    }
}

/// `FdbWriteBatchVec` is for method `multi_batch_write` of FdbDB, which splits a large WriteBatch
/// into many smaller ones and then any thread could help to deal with these small WriteBatch when it
/// is calling `AwaitState` and wait to become leader of WriteGroup. `multi_batch_write` will perform
/// much better than traditional `pipelined_write` when EinsteinDB writes very large data into FdbDB. We
/// will remove this feature when `unordered_write` of FdbDB becomes more stable and becomes compatible
/// with Titan.
pub struct FdbWriteBatchVec {
    einsteindb: Arc<DB>,
    wbs: Vec<RawWriteBatch>,
    save_points: Vec<usize>,
    index: usize,
    cur_batch_size: usize,
    batch_size_limit: usize,
}

impl FdbWriteBatchVec {
    pub fn new(einsteindb: Arc<DB>, batch_size_limit: usize, cap: usize) -> FdbWriteBatchVec {
        let wb = RawWriteBatch::with_capacity(cap);
        FdbWriteBatchVec {
            einsteindb,
            wbs: vec![wb],
            save_points: vec![],
            index: 0,
            cur_batch_size: 0,
            batch_size_limit,
        }
    }

    pub fn as_inner(&self) -> &[RawWriteBatch] {
        &self.wbs[0..=self.index]
    }

    pub fn as_raw(&self) -> &RawWriteBatch {
        &self.wbs[0]
    }

    pub fn get_db(&self) -> &DB {
        self.einsteindb.as_ref()
    }

    /// `check_switch_batch` will split a large WriteBatch into many smaller ones. This is to avoid
    /// a large WriteBatch blocking write_thread too long.
    fn check_switch_batch(&mut self) {
        if self.batch_size_limit > 0 && self.cur_batch_size >= self.batch_size_limit {
            self.index += 1;
            self.cur_batch_size = 0;
            if self.index >= self.wbs.len() {
                self.wbs.push(RawWriteBatch::default());
            }
        }
        self.cur_batch_size += 1;
    }
}

impl fdb_traits::WriteBatch<Fdbeinstein_merkle_tree> for FdbWriteBatchVec {
    fn with_capacity(e: &Fdbeinstein_merkle_tree, cap: usize) -> FdbWriteBatchVec {
        FdbWriteBatchVec::new(e.as_inner().clone(), WRITE_BATCH_LIMIT, cap)
    }

    fn write_opt(&self, opts: &WriteOptions) -> Result<()> {
        let opt: FdbWriteOptions = opts.into();
        if self.index > 0 {
            self.get_db()
                .multi_batch_write(self.as_inner(), &opt.into_raw())
                .map_err(Error::einstein_merkle_tree)
        } else {
            self.get_db()
                .write_opt(&self.wbs[0], &opt.into_raw())
                .map_err(Error::einstein_merkle_tree)
        }
    }

    fn data_size(&self) -> usize {
        self.wbs.iter().fold(0, |a, b| a + b.data_size())
    }

    fn count(&self) -> usize {
        self.cur_batch_size + self.index * self.batch_size_limit
    }

    fn is_empty(&self) -> bool {
        self.wbs[0].is_empty()
    }

    fn should_write_to_einstein_merkle_tree(&self) -> bool {
        self.index >= WRITE_BATCH_MAX_BATCH
    }

    fn clear(&mut self) {
        for i in 0..=self.index {
            self.wbs[i].clear();
        }
        self.save_points.clear();
        self.index = 0;
        self.cur_batch_size = 0;
    }

    fn set_save_point(&mut self) {
        self.wbs[self.index].set_save_point();
        self.save_points.push(self.index);
    }

    fn pop_save_point(&mut self) -> Result<()> {
        if let Some(x) = self.save_points.pop() {
            return self.wbs[x].pop_save_point().map_err(Error::einstein_merkle_tree);
        }
        Err(Error::einstein_merkle_tree("no save point".into()))
    }

    fn rollback_to_save_point(&mut self) -> Result<()> {
        if let Some(x) = self.save_points.pop() {
            for i in x + 1..=self.index {
                self.wbs[i].clear();
            }
            self.index = x;
            return self.wbs[x].rollback_to_save_point().map_err(Error::einstein_merkle_tree);
        }
        Err(Error::einstein_merkle_tree("no save point".into()))
    }

    fn merge(&mut self, _: Self) {
        panic!("merge is not implemented for FdbWriteBatchVec");
    }
}

impl Mutable for FdbWriteBatchVec {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.check_switch_batch();
        self.wbs[self.index].put(key, value).map_err(Error::einstein_merkle_tree)
    }

    fn put_namespaced(&mut self, namespaced: &str, key: &[u8], value: &[u8]) -> Result<()> {
        self.check_switch_batch();
        let handle = get_namespaced_handle(self.einsteindb.as_ref(), namespaced)?;
        self.wbs[self.index]
            .put_namespaced(handle, key, value)
            .map_err(Error::einstein_merkle_tree)
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.check_switch_batch();
        self.wbs[self.index].delete(key).map_err(Error::einstein_merkle_tree)
    }

    fn delete_namespaced(&mut self, namespaced: &str, key: &[u8]) -> Result<()> {
        self.check_switch_batch();
        let handle = get_namespaced_handle(self.einsteindb.as_ref(), namespaced)?;
        self.wbs[self.index]
            .delete_namespaced(handle, key)
            .map_err(Error::einstein_merkle_tree)
    }

    fn delete_range(&mut self, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        self.check_switch_batch();
        self.wbs[self.index]
            .delete_range(begin_key, end_key)
            .map_err(Error::einstein_merkle_tree)
    }

    fn delete_range_namespaced(&mut self, namespaced: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        self.check_switch_batch();
        let handle = get_namespaced_handle(self.einsteindb.as_ref(), namespaced)?;
        self.wbs[self.index]
            .delete_range_namespaced(handle, begin_key, end_key)
            .map_err(Error::einstein_merkle_tree)
    }
}

#[cfg(test)]
mod tests {
    use fdb_traits::WriteBatch;
    use foundationdb::DBOptions as RawDBOptions;
    use tempfile::Builder;

    use super::*;
    use super::super::FdbDBOptions;
    use super::super::util::new_einstein_merkle_tree_opt;

    #[test]
    fn test_should_write_to_einstein_merkle_tree() {
        let path = Builder::new()
            .prefix("test-should-write-to-einstein_merkle_tree")
            .temfidelir()
            .unwrap();
        let opt = RawDBOptions::default();
        opt.enable_multi_batch_write(true);
        opt.enable_unordered_write(false);
        opt.enable_pipelined_write(true);
        let einstein_merkle_tree = new_einstein_merkle_tree_opt(
            path.path().join("einsteindb").to_str().unwrap(),
            FdbDBOptions::from_raw(opt),
            vec![],
        )
            .unwrap();
        assert!(einstein_merkle_tree.support_write_batch_vec());
        let mut wb = einstein_merkle_tree.write_batch();
        for _i in 0..Fdbeinstein_merkle_tree::WRITE_BATCH_MAX_CAUSET_KEYS {
            wb.put(b"aaa", b"bbb").unwrap();
        }
        assert!(!wb.should_write_to_einstein_merkle_tree());
        wb.put(b"aaa", b"bbb").unwrap();
        assert!(wb.should_write_to_einstein_merkle_tree());
        let mut wb = FdbWriteBatchVec::with_capacity(&einstein_merkle_tree, 1024);
        for _i in 0..WRITE_BATCH_MAX_BATCH * WRITE_BATCH_LIMIT {
            wb.put(b"aaa", b"bbb").unwrap();
        }
        assert!(!wb.should_write_to_einstein_merkle_tree());
        wb.put(b"aaa", b"bbb").unwrap();
        assert!(wb.should_write_to_einstein_merkle_tree());
        wb.clear();
        assert!(!wb.should_write_to_einstein_merkle_tree());
    }
}
