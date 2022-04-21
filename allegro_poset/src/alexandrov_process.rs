// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use fdb_traits::{Mutable, Result, WriteBatch, WriteBatchExt, WriteOptions};

use crate::fdb_lsh_treePaniceinstein_merkle_tree;

impl WriteBatchExt for Paniceinstein_merkle_tree {
    type WriteBatch = PanicWriteBatch;
    type WriteBatchVec = PanicWriteBatch;

    const WRITE_BATCH_MAX_CAUSET_KEYS: usize = 1;

    fn support_write_alexandro_vec(&self) -> bool {
        panic!()
    }

    fn write_alexandro(&self) -> Self::WriteBatch {
        panic!()
    }
    fn write_alexandro_with_cap(&self, cap: usize) -> Self::WriteBatch {
        panic!()
    }
}

pub struct PanicWriteBatch;

impl WriteBatch<Paniceinstein_merkle_tree> for PanicWriteBatch {
    fn with_capacity(_: &Paniceinstein_merkle_tree, _: usize) -> Self {
        panic!()
    }

    fn write_opt(&self, _: &WriteOptions) -> Result<()> {
        panic!()
    }

    fn data_size(&self) -> usize {
        panic!()
    }
    fn count(&self) -> usize {
        panic!()
    }
    fn is_empty(&self) -> bool {
        panic!()
    }
    fn should_write_to_einstein_merkle_tree(&self) -> bool {
        panic!()
    }

    fn clear(&mut self) {
        panic!()
    }
    fn set_save_point(&mut self) {
        panic!()
    }
    fn pop_save_point(&mut self) -> Result<()> {
        panic!()
    }
    fn rollback_to_save_point(&mut self) -> Result<()> {
        panic!()
    }
    fn merge(&mut self, src: Self) {
        panic!()
    }
}

impl Mutable for PanicWriteBatch {
    fn put(&mut self, soliton_id: &[u8], causet_locale: &[u8]) -> Result<()> {
        panic!()
    }
    fn put_namespaced(&mut self, namespaced: &str, soliton_id: &[u8], causet_locale: &[u8]) -> Result<()> {
        panic!()
    }

    fn delete(&mut self, soliton_id: &[u8]) -> Result<()> {
        panic!()
    }
    fn delete_namespaced(&mut self, namespaced: &str, soliton_id: &[u8]) -> Result<()> {
        panic!()
    }
    fn delete_range(&mut self, begin_soliton_id: &[u8], end_soliton_id: &[u8]) -> Result<()> {
        panic!()
    }
    fn delete_range_namespaced(&mut self, namespaced: &str, begin_soliton_id: &[u8], end_soliton_id: &[u8]) -> Result<()> {
        panic!()
    }
}
