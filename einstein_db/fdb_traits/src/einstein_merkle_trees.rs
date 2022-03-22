// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::fdb_lsh_treeKV;
use crate::errors::Result;
use crate::options::WriteOptions;
use crate::violetabft_einstein_merkle_tree::VioletaBFTeinstein_merkle_tree;
use crate::write_alexandro::WriteBatch;

#[derive(Clone, Debug)]
pub struct einstein_merkle_trees<K, R> {
    pub kv: K,
    pub violetabft: R,
}

impl<K: KV, R: VioletaBFTeinstein_merkle_tree> einstein_merkle_trees<K, R> {
    pub fn new(kv_einstein_merkle_tree: K, violetabft_einstein_merkle_tree: R) -> Self {
        einstein_merkle_trees {
            kv: kv_einstein_merkle_tree,
            violetabft: violetabft_einstein_merkle_tree,
        }
    }

    pub fn write_kv(&self, wb: &K::WriteBatch) -> Result<()> {
        wb.write()
    }

    pub fn write_kv_opt(&self, wb: &K::WriteBatch, opts: &WriteOptions) -> Result<()> {
        wb.write_opt(opts)
    }

    pub fn sync_kv(&self) -> Result<()> {
        self.kv.sync()
    }
}
