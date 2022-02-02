// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::fdb_lsh_treeKvEngine;
use crate::errors::Result;
use crate::options::WriteOptions;
use crate::violetabft_engine::VioletaBFTEngine;
use crate::write_batch::WriteBatch;

#[derive(Clone, Debug)]
pub struct Engines<K, R> {
    pub kv: K,
    pub violetabft: R,
}

impl<K: KvEngine, R: VioletaBFTEngine> Engines<K, R> {
    pub fn new(kv_engine: K, violetabft_engine: R) -> Self {
        Engines {
            kv: kv_engine,
            violetabft: violetabft_engine,
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
