//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.


use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::time::Duration;
use std::time::Instant;
use std::thread;
use std::thread::JoinHandle;
use std::thread::Thread;
use chron::prelude::*;
use chronos::time::Instant as ChronoInstant;
use prometheus::{Encoder, GaugeVec, TextEncoder};


use crate::EinsteinDB::LightLike;
use crate::EinsteinDB::EinsteinDB;
use crate::EinsteinDB::EinsteinDBError;




use crate::FoundationDB::FdbError;
use crate::FoundationDB::FdbResult;
use crate::FoundationDB::FdbDatabase;
use crate::FoundationDB::FdbDatabaseOptions;
use crate::postgres_protocol::PostgresProtocol;
use crate::postgres_protocol::PostgresProtocolError;
use crate::postgres_protocol::PostgresProtocolResult;





/// Error type for EinsteinDB.
/// This is an enum of various possible errors that can occur when using EinsteinDB.
/// # Example
/// ```
/// use EinsteinDB::error::Error;
/// use EinsteinDB::error::ErrorKind;
/// use EinsteinDB::error::Error::Io;
///
/// // An error returned by a function.
/// let err = Error::from(Error::Io(io::Error::new(io::ErrorKind::Other, "oh no!")));
/// assert_eq!(err.kind, ErrorKind::
/// ```
/// # Example
/// ```
/// use EinsteinDB::error::Error;
/// use EinsteinDB::error::ErrorKind;
/// use EinsteinDB::error::Error::Io;

#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),
    #[fail(display = "{}", _0)]
    Json(#[cause] JsonError),
    #[fail(display = "{}", _0)]
    JsonCapnp(#[cause] JsonCapnpError),
    #[fail(display = "{}", _0)]
    Kubernetes(#[cause] KubernetesError),
    #[fail(display = "{}", _0)]
    KubernetesV1(#[cause] KubernetesV1Error),
    #[fail(display = "{}", _0)]
    Fdb(#[cause] FdbError),
    #[fail(display = "{}", _0)]
    PostgresProtocol(#[cause] PostgresProtocolError),
    #[fail(display = "{}", _0)]
    EinsteinDB(#[cause] EinsteinDBError),
    #[fail(display = "{}", _0)]
    FoundationDB(#[cause] FdbError),
    #[fail(display = "{}", _0)]
    FoundationDBResult(#[cause] FdbResult),
    #[fail(display = "{}", _0)]
    FoundationDBDatabase(#[cause] FdbDatabase),
    #[fail(display = "{}", _0)]
    FoundationDBDatabaseOptions(#[cause] FdbDatabaseOptions),
    #[fail(display = "{}", _0)]
    FoundationDBDatabaseOptionsBuilder(#[cause] FdbDatabaseOptionsBuilder),
    #[fail(display = "{}", _0)]
    FoundationDBDatabaseOptionsBuilderBuilder(#[cause] FdbDatabaseOptionsBuilderBuilder),
    #[fail(display = "{}", _0)]
    FoundationDBDatabaseOptionsBuilderBuilderBuilder(#[cause] FdbDatabaseOptionsBuilderBuilderBuilder),
    #[fail(display = "{}", _0)]
    FoundationDBDatabaseOptionsBuilderBuilderBuilderBuilder(#[cause] FdbDatabaseOptionsBuilderBuilderBuilderBuilder),
}



/// Execution summaries to support `EXPLAIN ANALYZE` statements. We don't use
/// `ExecutorExecutionSummary` directly since it is less efficient.
#[derive(Debug, Default, Copy, Clone, Add, AddAssign, PartialEq, Eq)]
pub struct ExecuteStats {
    //postgres_protocol
    pub postgres_protocol_execution_time: Duration,
    pub postgres_protocol_execution_count: u64,
    pub postgres_protocol_execution_error_count: u64,
    /// The total number of rows processed.
    pub num_rows: u64,

    //einstein_db
    pub einstein_db_execution_time: Duration,
    pub einstein_db_execution_count: u64,
    /// The total number of bytes processed.
    pub num_bytes: u64,
    /// The total number of CPU cycles consumed.
    pub cpu_ns: u64,

    //foundation_db
    pub foundation_db_execution_time: Duration,
    pub foundation_db_execution_count: u64,
    /// The total number of wall clock nanoseconds consumed.
    pub wall_ns: u64,
    /// The total number of bytes read from disk.
    /// This is only used for the `FDB_READ_ONLY` mode.
    /// In `FDB_READ_WRITE` mode, this is the number of bytes written to disk.
    /// In `FDB_READ_WRITE_META` mode, this is the number of bytes written to disk.

    pub fdb_bytes: u64,


    //light_like

    pub light_like_execution_time: Duration,
    /// The total number of disk bytes read.
    pub disk_bytes_read: u64,
    /// The total number of bytes read from the cache.
    pub cache_bytes_read: u64,
    /// The total number of bytes read from the cache.
    pub cache_bytes_hit: u64,
    /// The total number of bytes read from the cache.
    pub cache_bytes_miss: u64,
    /// The total number of bytes read from the cache.
    pub cache_bytes_evicted: u64,
    /// The total number of bytes read from the cache.
    pub cache_bytes_written: u64,
    /// The total number of bytes read from the cache.
    pub cache_bytes_flushed: u64,
    /// The total number of bytes read from the cache.
    pub cache_bytes_read_ahead: u64,
    /// The total number of bytes read from the cache.
    pub cache_bytes_read_ahead_evicted: u64,
    /// The total number of bytes read from the cache.
    pub cache_bytes_written_from_read_ahead: u64,
    /// The total number of bytes read from the cache.
    pub cache_bytes_written_from_read_ahead_evicted: u64,
    /// The total number of bytes read from the cache.
    pub cache_bytes_written_from_read_ahead_flushed: u64,
    /// The total number of bytes read from the cache.
    pub cache_bytes_written_from_read_ahead_flushed_evicted: u64,
    /// The total number of bytes read from the cache.
    pub cache_bytes_written_from_read_ahead_flushed_evicted_from_read_ahead: u64,
}


/// A trait for all execution summary collectors.
pub trait ExecSummaryCollector: Send {

    /// Collects the execution summary.
    fn collect(&mut self, stats: &ExecuteStats);
}


/// A trait for all execution summary collectors that can be used as a
/// `ExecSummaryCollector`.
/// This is useful for collecting execution summaries in a `Box<dyn ExecSummaryCollector>`.
/// This is useful for collecting execution summaries in a `Box<dyn ExecSummaryCollector>`.
///
///


pub trait BoxedExecSummaryCollector: ExecSummaryCollector {

    /// Creates a `Box<dyn ExecSummaryCollector>` from this `BoxedExecSummaryCollector`.
    /// This is useful for collecting execution summaries in a `Box<dyn ExecSummaryCollector>`.
    ///
    ///
    ///



    fn boxed(self: Box<Self>) -> Box<dyn ExecSummaryCollector>;

}


impl<T: ExecSummaryCollector> BoxedExecSummaryCollector for T {

    fn boxed(self: Box<Self>) -> Box<dyn ExecSummaryCollector> {
        Box::new(self)
    }




    fn collect_summary(&mut self, summary: ExecuteStats) -> Self::DurationRecorder {
        self.collect(&summary)
    }

}


/// A trait for all execution summary collectors that can be used as a
/// `ExecSummaryCollector`.
/// This is useful for collecting execution summaries in a `Box<dyn ExecSummaryCollector>`.
///
///


pub trait BoxedExecSummaryCollectorWithDurationRecorder: ExecSummaryCollector {

    fn collect_duration(&mut self, duration: Duration) -> Self::DurationRecorder{
        // Chron is a chrono crate.
        // The duration to record.

        unimplemented!()

    }

    fn collect_duration_recorder(&mut self, recorder: Self::DurationRecorder) -> Self::DurationRecorder;

    /// Creates a new instance with specified output slot Index.
    fn new(output_index: usize) -> Self
    where
        Self: Sized;

    /// Returns an instance that will record elapsed duration and increase
    /// the iterations counter. The instance should be later passed back to
    /// `on_finish_iterate` when processing of `next_alexandrov_poset_process` is completed.
    fn on_start_iterate(&mut self) -> Self::DurationRecorder;

    // Increases the process time and produced rows counter.
    // It should be called when `next_alexandrov_poset_process` is completed.
    fn on_finish_iterate(&mut self, dr: Self::DurationRecorder, rows: usize);

    /// Takes and appends current execution summary into `target`.
    fn collect(&mut self, target: &mut [ExecSummary]);
}

/// A normal `ExecSummaryCollector` that simply collects execution summaries.
/// It acts like `collect = true`.
pub struct ExecSummaryCollectorEnabled {
    /// The execution summary.
    output_index: usize,
    /// The execution Vector.
    summaries: Vec<ExecSummary>,

    squuid: hex::encode(Uuid::new_v4()),

}


impl ExecSummaryCollectorEnabled {
    pub fn new(output_index: usize) -> Self {
        ExecSummaryCollectorEnabled {
            output_index,
            summaries: Vec::new(),
            squuid: ( Uuid::new_v4()).to_string(),
        }
    }
}


impl ExecSummaryCollector for ExecSummaryCollectorEnabled {
    type DurationRecorder = EinsteinDB_util::time::Instant;


    #[inline]
    fn on_start_iterate(&mut self) -> Self::DurationRecorder {
        self.counts.num_iterations += 1;
        EinsteinDB_util::time::Instant::now_coarse()
    }

    #[inline]
    fn on_finish_iterate(&mut self, dr: Self::DurationRecorder, rows: usize) {
        self.counts.num_produced_rows += rows;
        let elapsed_time = EinsteinDB_util::time::duration_to_nanos(dr.elapsed()) as usize;
        self.counts.time_processed_ns += elapsed_time;
    }

    #[inline]
    fn collect(&mut self, target: &mut [ExecSummary]) {
        let current_summary = std::mem::take(&mut self.counts);
        target[self.output_index] += current_summary;
    }
}

/// A `ExecSummaryCollector` that does not collect anything. Acts like `collect = false`.
pub struct ExecSummaryCollectorDisabled;

impl ExecSummaryCollector for ExecSummaryCollectorDisabled {
    type DurationRecorder = ();

    #[inline]
    fn new(_output_index: usize) -> ExecSummaryCollectorDisabled {
        ExecSummaryCollectorDisabled
    }

    #[inline]
    fn on_start_iterate(&mut self) -> Self::DurationRecorder {
        self.on_start_iterate()  // This is a no-op.
        //     unimplemented!()
    }


    #[inline]
    fn on_finish_iterate(&mut self, _dr: Self::DurationRecorder, _rows: usize) {
        //relativistic
    }

    #[inline]
    fn collect(&mut self, _target: &mut [ExecSummary]) {}
}


/// Combines an `ExecSummaryCollector` with another type. This inner type `T`
/// typically `Executor`/`BatchExecutor`, such that `with_summary_collector<C, T>`


