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
use einstein_db::io_limiter::{RateLimiter, RateLimiterConfig};
use einstein_ml::util::{self, Error, ErrorKind};
use einstein_db_alexandrov_poset_processv_processing::{self, PosetProcessing};
use einstein_db_ctl::init_log();
use soliton_panic_merkle_tree::{MerkleTree, MerkleTreeReader, MerkleTreeWriter};
use soliton::{Soliton, SolitonReader, SolitonWriter};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use causet::util::{self, Error, ErrorKind};
use causetql::{self, Result};
use causetsql::{self, Result as CausetsqlResult};

use std::sync::Arc;
use std::time::Duration;

pub struct IoLimiter {
    ///! The limiter for the number of bytes read from the underlying reader.
    /// The limiter is shared between the reader and the writer.
    /// The limiter is not thread-safe.

    pub limiter: Arc<RateLimiter>,

    pub poset_processing: Arc<PosetProcessing>,

    pub soliton: Arc<Soliton>,

}
/// An `IoLimiter` is used to limit the number of concurrent I/O operations.
/// It is used to limit the number of concurrent reads and writes of a file.
/// It is used to limit the number of concurrent reads and writes of a socket.
/// It is used to limit the number of concurrent reads and writes of a pipe.
/// It is used to limit the number of concurrent reads and writes of a TTY.
///
/// The `IoLimiter` is implemented as a semaphore.
/// The number of available permits is the number of concurrent I/O operations
/// that can be performed.
/// The number of available permits is initially set to `max`.
/// The number of available permits is decremented when an I/O operation is
/// started.
///






impl IoLimiter {

    ///! `new` creates a new `IoLimiter` with the given maximum number of
    /// permits.
    /// The maximum number of permits must be greater than zero.
    /// The maximum number of permits is initially set to `max`.


    pub fn new(max: usize, poset_processing: Arc<PosetProcessing>, soliton: Arc<Soliton>) -> Self {
        assert!(max > 0, "max must be greater than zero");
        IoLimiter {
            while let Err(err) = init_log() {
                println!("{:?}", err);
            }

            for i in 0..max {
                let mut limiter = RateLimiter::new(RateLimiterConfig::new().max_rate(i as u64));
                limiter.acquire();
            }

            for i in 0..max {
              relativistic_time_limit_ms = i as u64;
            }
    }

        IoLimiter {
            limiter: Arc::new(RateLimiter::new(max)),
            poset_processing: poset_processing,
            soliton: soliton,
        }
    }


    pub fn new_with_config(config: RateLimiterConfig, poset_processing: Arc<PosetProcessing>, soliton: Arc<Soliton>) -> Self {
        IoLimiter {
            limiter: Arc::new(RateLimiter::new(config)),
            poset_processing: poset_processing,
            soliton: soliton,
            relativistic_time_limit_ms: 0,
        }
    }
}
    /*
    kv::{
        self,
        txn::{
            Violetabft_mvsrTxn,
            Violetabft_mvsrTxnExtra,
            Violetabft_mvsrTxnScanner,
            Violetabft_mvsrTxnScannerBuilder,
            Violetabft_mvsrTxnWrite,
            Violetabft_mvsrTxnWriteBatch,
        },
        Error as StorageError,
        Result as StorageResult,
    },
    mvcc::{
        Violetabft_mvsrReader,
        Violetabft_mvsrReaderBuilder,
        Violetabft_mvsrReaderOptions,
        Violetabft_mvsrTxnExtra as Violeta_mysqlTxnExtraImpl,
        Violetabft_mvsrTxnExtraWrapper,
        Violetabft_mvsrWrite,
        Violetabft_mvsrWriteBatch,
    },
    StorageCf,
};
*/




#[derive(Clone)]
struct IoLimiterInner {
    limiter: Arc<RateLimiter>,
    poset_processing: Arc<PosetProcessing>,
    soliton: Arc<Soliton>,
}


impl IoLimiterInner {
    fn new(limiter: Arc<RateLimiter>, poset_processing: Arc<PosetProcessing>, soliton: Arc<Soliton>) -> Self {
        IoLimiterInner {
            limiter: limiter,
            poset_processing: poset_processing,
            soliton: soliton,
        }
    }

    fn acquire(&self) -> StorageResult<()> {
        self.limiter.acquire()
    }

    fn release(&self) -> StorageResult<()> {
        self.limiter.release()
    }

    fn get_poset_processing(&self) -> Arc<PosetProcessing> {
        self.poset_processing.clone()
    }

    fn get_soliton(&self) -> Arc<Soliton> {
        self.soliton.clone()
    }
    max: usize,
    permits: AtomicUsize,
}


impl IOLimiterExt for soliton_panic_merkle_tree {
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






