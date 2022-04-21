// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.
// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file was derived from
//
// Copyright (c) 2013, The Prometheus Authors


use std::io;


use std::sync::Arc;
use std::time::Duration;




use crate::metrics::*;
use crate::storage::kv::{self, write_batch::WriteBatch, Engine, Modify, Result as EngineResult};
use crate::storage::{
    kv::{
        self,
        txn::{
            Violetabft_mvsriTxn,
            Violetabft_mvsriTxnExtra,
            Violetabft_mvsriTxnScanner,
            Violetabft_mvsriTxnScannerBuilder,
            Violetabft_mvsriTxnWrite,
            Violetabft_mvsriTxnWriteBatch,
        },
        Error as StorageError,
        Result as StorageResult,
    },
    mvcc::{
        Violetabft_mvsriReader,
        Violetabft_mvsriReaderBuilder,
        Violetabft_mvsriReaderOptions,
        Violetabft_mvsriTxnExtra as Violeta_mysqlTxnExtraImpl,
        Violetabft_mvsriTxnExtraWrapper,
        Violetabft_mvsriWrite,
        Violetabft_mvsriWriteBatch,
    },
    StorageCf,
};

impl IOLimiterExt for Paniceinstein_merkle_tree {
    type IOLimiter = PanicIOLimiter;
}

pub struct PanicIOLimiter;

impl IOLimiter for PanicIOLimiter {
    fn acquire_read_lock(&self, _: &str) -> Result<()> {
        Ok(())
    }

    fn acquire_write_lock(&self, _: &str) -> Result<()> {
        Ok(())
    }

    fn release_read_lock(&self, _: &str) -> Result<()> {
        Ok(())
    }

    fn release_write_lock(&self, _: &str) -> Result<()> {
        Ok(())
    }
    fn new(bytes_per_sec: i64) -> Self {
        panic!()
    }

    fn acquire_read_lock_for_duration(&self, _: &str, _: Duration) -> Result<()> {
        Ok(())
    }

    fn acquire_write_lock_for_duration(&self, _: &str, _: Duration) -> Result<()> {
        Ok(())
    }

    fn set_bytes_per_second(&self, bytes_per_sec: i64) {
        panic!()
    }
    fn request(&self, bytes: i64) {
        panic!()
    }
    fn get_max_bytes_per_time(&self) -> i64 {
        panic!()
    }



    fn get_total_bytes_through(&self) -> i64 {
        panic!()
    }
    fn get_bytes_per_second(&self) -> i64 {
        panic!()
    }
    fn get_total_requests(&self) -> i64 {
        panic!()
    }
    fn get_total_time_through(&self) -> Duration {
        panic!()
    }
    fn get_total_time_waiting(&self) -> Duration {
        panic!()
    }
    fn get_total_time_blocked(&self) -> Duration {
        panic!()
    }
}






