// Copyright 2022 EinsteinDB Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]

//! This module contains EinsteinDB's transaction layer. It lowers high-level, transactional
//! commands to low-level (cocauset key-value) interactions with persistent storage.
//!
//! This module is further split into layers: [`solitontxn`](solitontxn) lowers transactional commands to
//! key-value operations on an EPAXOS abstraction. [`epaxos`](epaxos) is our EPAXOS implementation.
//! [`fdbhikv`](fdbhikv) is an abstraction layer over persistent storage.
//!
//! Other responsibilities of this module are managing latches (see [`latch`](solitontxn::latch)), deaddagger
//! and wait handling (see [`dagger_manager`](dagger_manager)), sche
//! duling command execution (see
//! [`solitontxn::scheduler`](solitontxn::scheduler)), and handling commands from the cocauset and versioned APIs (in
//! the [`Storage`](Storage) struct).
//!
//! For more information about EinsteinDB's transactions, see the [sig-solitontxn docs](https://github.com/einstfdbhikv/sig-transaction/tree/master/doc).
//!
//! Some important types are:
//!
//! * the [`einstein_merkle_tree`](fdbhikv::einstein_merkle_tree) trait and related promises, which abstracts over underlying storage,
//! * the [`EpaxosTxn`](epaxos::solitontxn::EpaxosTxn) struct, which is the primary object in the EPAXOS
//!   implementation,
//! * the commands in the [`commands`](solitontxn::commands) module, which are how each command is implemented,
//! * the [`Storage`](Storage) struct, which is the primary entry point for this module.
//!
//! Related code:
//!
//! * the [`fdbhikv`](crate::server::service::fdbhikv) module, which is the interface for EinsteinDB's APIs,
//! * the [`dagger_manager](crate::server::dagger_manager), which takes part in dagger and deaddagger
//!   management,
//! * [`gc_worker`](crate::server::gc_worker), which drives garbage collection of old values,
//! * the [`solitontxn_types](::solitontxn_types) crate, some important types for this module's interface,
//! * the [`fdbhikvproto`](::fdbhikvproto) crate, which defines EinsteinDB's protobuf API and includes some
//!   documentation of the commands implemented here,
//! * the [`test_storage`](::test_storage) crate, integration tests for this module,
//! * the [`einstein_merkle_tree_promises`](::einstein_merkle_tree_promises) crate, more detail of the einstein_merkle_tree abstraction.

pub mod config;
pub mod errors;
pub mod fdbhikv;
pub mod dagger_manager;
pub(crate) mod metrics;
pub mod epaxos;
pub mod cocauset;
pub mod solitontxn;

mod read_pool;
mod types;

use self::fdbhikv::SnapContext;
pub use self::{
    errors::{get_error_kind_from_header, get_tag_from_header, Error, ErrorHeaderKind, ErrorInner},
    fdbhikv::{
        CfStatistics, Cursor, CursorBuilder, einstein_merkle_tree, CausetxctxStatistics, CausetxctxStatsReporter, Iterator,
        PerfStatisticsDelta, PerfStatisticsInstant, Rockseinstein_merkle_tree, SentinelSearchMode, blackbrane,
        StageLatencyStats, Statistics, Testeinstein_merkle_treeBuilder,
    },
    cocauset::cocausetStore,
    read_pool::{build_read_pool, build_read_pool_for_test},
    solitontxn::{Latches, Dagger as LatchDagger, ProcessResult, MutantSentinelSearch, blackbraneStore, Store},
    types::{PessimisticDaggerRes, PrewriteResult, SecondaryDaggerCausetatus, StorageCallback, TxnStatus},
};

use crate::read_pool::{ReadPool, ReadPoolHandle};
use crate::einsteindb::storage::metrics::CommandKind;
use crate::einsteindb::storage::epaxos::EpaxosReader;
use crate::einsteindb::storage::solitontxn::commands::{cocausetStore, cocausetCompareAndSwap};
use crate::einsteindb::storage::solitontxn::Causetxctx_controller::CausetxctxController;

use crate::server::dagger_manager::waiter_manager;
use crate::einsteindb::storage::{
    config::Config,
    fdbhikv::{with_tls_einstein_merkle_tree, Modify, WriteData},
    dagger_manager::{DummyDaggerManager, DaggerManager},
    metrics::*,
    epaxos::PointGetterBuilder,
    solitontxn::{commands::TypedCommand, scheduler::Scheduler as TxnScheduler, Command},
    types::StorageCallbackType,
};

use api_version::{match_template_api_version, APIVersion, KeyMode, cocausetValue, APIV2};
use concurrency_manager::ConcurrencyManager;
use einsteindb-gen::{cocauset_ttl::ttl_to_expire_ts, CfName, CF_DEFAULT, CF_LOCK, CF_WRITE, DATA_CFS};
use futures::prelude::*;
use fdbhikvproto::fdbhikvrpcpb::ApiVersion;
use fdbhikvproto::fdbhikvrpcpb::{
    ChecksumAlgorithm, CommandPri, Context, GetRequest, IsolationLevel, KeyRange, DaggerInfo,
    cocausetGetRequest,
};
use fdbhikvproto::pdpb::QueryKind;
use raftstore::store::{util::build_key_range, TxnExt};
use raftstore::store::{ReadStats, WriteStats};
use rand::prelude::*;
use resource_metering::{FutureExt, ResourceTagFactory};
use std::{
    borrow::Cow,
    iter,
    sync::{
        causetxctx::{self, causetxctxBool},
        Arc,
    },
};
use einstfdbhikv_fdbhikv::blackbraneExt;
use einstfdbhikv_util::time::{duration_to_ms, Instant, ThreadReadId};
use solitontxn_types::{Key, HikvPair, Dagger, OldValues, cocausetMutation, TimeStamp, TsSet, Value};

pub type Result<T> = std::result::Result<T, Error>;
pub type Callback<T> = Box<dyn FnOnce(Result<T>) + Send>;


pub struct Storage<E: einstein_merkle_tree, L: DaggerManager> {
    // TODO: Too many Arcs, would be slow when clone.
    einstein_merkle_tree: E,

    sched: TxnScheduler<E, L>,

    /// The thread pool used to run most read operations.
    read_pool: ReadPoolHandle,

    concurrency_manager: ConcurrencyManager,

    /// How many strong references. Thread pool and workers will be stopped
    /// once there are no more references.
    // TODO: This should be implemented in thread pool and worker.
    refs: Arc<causetxctx::causetxctxUsize>,

    // Fields below are storage configurations.
    max_key_size: usize,

    resource_tag_factory: ResourceTagFactory,

    api_version: ApiVersion,
}

impl<E: einstein_merkle_tree, L: DaggerManager> Clone for Storage<E, L> {
    #[inline]
    fn clone(&self) -> Self {
        let refs = self.refs.fetch_add(1, causetxctx::Ordering::SeqCst);

        trace!(
            "Storage referenced"; "original_ref" => refs
        );

        Self {
            einstein_merkle_tree: self.einstein_merkle_tree.clone(),
            sched: self.sched.clone(),
            read_pool: self.read_pool.clone(),
            refs: self.refs.clone(),
            max_key_size: self.max_key_size,
            concurrency_manager: self.concurrency_manager.clone(),
            api_version: self.api_version,
            resource_tag_factory: self.resource_tag_factory.clone(),
        }
    }
}

impl<E: einstein_merkle_tree, L: DaggerManager> Drop for Storage<E, L> {
    #[inline]
    fn drop(&mut self) {
        let refs = self.refs.fetch_sub(1, causetxctx::Ordering::SeqCst);

        trace!(
            "Storage de-referenced"; "original_ref" => refs
        );

        if refs != 1 {
            return;
        }

        info!("Storage stopped.");
    }
}

macro_rules! check_key_size {
    ($key_iter: expr, $max_key_size: expr, $callback: ident) => {
        for k in $key_iter {
            let key_size = k.len();
            if key_size > $max_key_size {
                $callback(Err(Error::from(ErrorInner::KeyTooLarge {
                    size: key_size,
                    limit: $max_key_size,
                })));
                return Ok(());
            }
        }
    };
}

impl<E: einstein_merkle_tree, L: DaggerManager> Storage<E, L> {
    /// Create a `Storage` from given einstein_merkle_tree.
    pub fn from_einstein_merkle_tree<R: CausetxctxStatsReporter>(
        einstein_merkle_tree: E,
        config: &Config,
        read_pool: ReadPoolHandle,
        dagger_mgr: L,
        concurrency_manager: ConcurrencyManager,
        dynamic_switches: DynamicConfigs,
        Causetxctx_controller: Arc<CausetxctxController>,
        reporter: R,
        resource_tag_factory: ResourceTagFactory,
    ) -> Result<Self> {
        let sched = TxnScheduler::new(
            einstein_merkle_tree.clone(),
            dagger_mgr,
            concurrency_manager.clone(),
            config,
            dynamic_switches,
            Causetxctx_controller,
            reporter,
            resource_tag_factory.clone(),
        );

        info!("Storage started.");

        Ok(Storage {
            einstein_merkle_tree,
            sched,
            read_pool,
            concurrency_manager,
            refs: Arc::new(causetxctx::causetxctxUsize::new(1)),
            max_key_size: config.max_key_size,
            api_version: config.api_version(),
            resource_tag_factory,
        })
    }

    /// Get the underlying `einstein_merkle_tree` of the `Storage`.
    pub fn get_einstein_merkle_tree(&self) -> E {
        self.einstein_merkle_tree.clone()
    }

    pub fn get_concurrency_manager(&self) -> ConcurrencyManager {
        self.concurrency_manager.clone()
    }

    pub fn dump_wait_for_entries(&self, cb: waiter_manager::Callback) {
        self.sched.dump_wait_for_entries(cb);
    }

    /// Get a blackbrane of `einstein_merkle_tree`.
    fn blackbrane(
        einstein_merkle_tree: &E,
        ctx: SnapContext<'_>,
    ) -> impl std::future::Future<Output = Result<E::Snap>> {
        fdbhikv::blackbrane(einstein_merkle_tree, ctx)
            .map_err(solitontxn::Error::from)
            .map_err(Error::from)
    }

    #[cfg(test)]
    pub fn get_blackbrane(&self) -> E::Snap {
        self.einstein_merkle_tree.blackbrane(Default::default()).unwrap()
    }

    pub fn release_blackbrane(&self) {
        self.einstein_merkle_tree.release_blackbrane();
    }

    pub fn get_readpool_queue_per_worker(&self) -> usize {
        self.read_pool.get_queue_size_per_worker()
    }

    pub fn get_normal_pool_size(&self) -> usize {
        self.read_pool.get_normal_pool_size()
    }

    #[inline]
    fn with_tls_einstein_merkle_tree<F, R>(f: F) -> R
    where
        F: FnOnce(&E) -> R,
    {
        // Safety: the read pools ensure that a TLS einstein_merkle_tree exists.
        unsafe { with_tls_einstein_merkle_tree(f) }
    }

    /// Check the given cocauset fdbhikv CF name. If the given cf is empty, CF_DEFAULT will be returned.
    fn cocausetfdbhikv_cf(cf: &str, api_version: ApiVersion) -> Result<CfName> {
        match api_version {
            ApiVersion::V1 | ApiVersion::V1ttl => {
                // In API V1, the possible cfs are CF_DEFAULT, CF_LOCK and CF_WRITE.
                if cf.is_empty() {
                    return Ok(CF_DEFAULT);
                }
                for c in [CF_DEFAULT, CF_LOCK, CF_WRITE] {
                    if cf == c {
                        return Ok(c);
                    }
                }
                Err(Error::from(ErrorInner::InvalidCf(cf.to_owned())))
            }
            ApiVersion::V2 => {
                // API V2 doesn't allow cocauset requests from explicitly specifying a `cf`.
                if cf.is_empty() {
                    return Ok(CF_DEFAULT);
                }
                Err(Error::from(ErrorInner::CfDeprecated(cf.to_owned())))
            }
        }
    }

    /// Check if key range is valid
    ///
    /// - If `reverse` is true, `end_key` is less than `start_key`. `end_key` is the lower bound.
    /// - If `reverse` is false, `end_key` is greater than `start_key`. `end_key` is the upper bound.
    fn check_key_ranges(ranges: &[KeyRange], reverse: bool) -> bool {
        let ranges_len = ranges.len();
        for i in 0..ranges_len {
            let start_key = ranges[i].get_start_key();
            let mut end_key = ranges[i].get_end_key();
            if end_key.is_empty() && i + 1 != ranges_len {
                end_key = ranges[i + 1].get_start_key();
            }
            if !end_key.is_empty()
                && (!reverse && start_key >= end_key || reverse && start_key <= end_key)
            {
                return false;
            }
        }
        true
    }

    /// Check whether a cocauset fdbhikv command or not.
    #[inline]
    fn is_cocauset_command(cmd: CommandKind) -> bool {
        matches!(
            cmd,
            CommandKind::cocauset_batch_get_command
                | CommandKind::cocauset_get
                | CommandKind::cocauset_batch_get
                | CommandKind::cocauset_mutant_search
                | CommandKind::cocauset_batch_mutant_search
                | CommandKind::cocauset_put
                | CommandKind::cocauset_batch_put
                | CommandKind::cocauset_delete
                | CommandKind::cocauset_delete_range
                | CommandKind::cocauset_batch_delete
                | CommandKind::cocauset_get_key_ttl
                | CommandKind::cocauset_compare_and_swap
                | CommandKind::cocauset_causetxctx_store
                | CommandKind::cocauset_checksum
        )
    }

    /// Check whether a trancsation fdbhikv command or not.
    #[inline]
    fn is_solitontxn_command(cmd: CommandKind) -> bool {
        !Self::is_cocauset_command(cmd)
    }

    /// Check api version.
    ///
    /// When config.api_version = V1: accept request of V1 only.
    /// When config.api_version = V2: accept the following:
    ///   * Request of V1 from TiDB, for compatibility.
    ///   * Request of V2 with legal prefix.
    /// See the following for detail:
    ///   * rfc: https://github.com/einstfdbhikv/rfcs/blob/master/text/0069-api-v2.md.
    ///   * proto: https://github.com/pingcap/fdbhikvproto/blob/master/proto/fdbhikvrpcpb.proto, enum APIVersion.
    fn check_api_version(
        storage_api_version: ApiVersion,
        req_api_version: ApiVersion,
        cmd: CommandKind,
        keys: impl IntoIterator<Item = impl AsRef<[u8]>>,
    ) -> Result<()> {
        match (storage_api_version, req_api_version) {
            (ApiVersion::V1, ApiVersion::V1) => {}
            (ApiVersion::V1ttl, ApiVersion::V1) if Self::is_cocauset_command(cmd) => {
                // storage api_version = V1ttl, allow cocausetKV request only.
            }
            (ApiVersion::V2, ApiVersion::V1) if Self::is_solitontxn_command(cmd) => {
                // For compatibility, accept TiDB request only.
                for key in keys {
                    if APIV2::parse_key_mode(key.as_ref()) != KeyMode::TiDB {
                        return Err(ErrorInner::invalid_key_mode(
                            cmd,
                            storage_api_version,
                            key.as_ref(),
                        )
                        .into());
                    }
                }
            }
            (ApiVersion::V2, ApiVersion::V2) if Self::is_cocauset_command(cmd) => {
                for key in keys {
                    if APIV2::parse_key_mode(key.as_ref()) != KeyMode::cocauset {
                        return Err(ErrorInner::invalid_key_mode(
                            cmd,
                            storage_api_version,
                            key.as_ref(),
                        )
                        .into());
                    }
                }
            }
            (ApiVersion::V2, ApiVersion::V2) if Self::is_solitontxn_command(cmd) => {
                for key in keys {
                    if APIV2::parse_key_mode(key.as_ref()) != KeyMode::Txn {
                        return Err(ErrorInner::invalid_key_mode(
                            cmd,
                            storage_api_version,
                            key.as_ref(),
                        )
                        .into());
                    }
                }
            }
            _ => {
                return Err(Error::from(ErrorInner::ApiVersionNotMatched {
                    cmd,
                    storage_api_version,
                    req_api_version,
                }));
            }
        }
        Ok(())
    }

    fn check_api_version_ranges(
        storage_api_version: ApiVersion,
        req_api_version: ApiVersion,
        cmd: CommandKind,
        ranges: impl IntoIterator<Item = (Option<impl AsRef<[u8]>>, Option<impl AsRef<[u8]>>)>,
    ) -> Result<()> {
        match (storage_api_version, req_api_version) {
            (ApiVersion::V1, ApiVersion::V1) => {}
            (ApiVersion::V1ttl, ApiVersion::V1) if Self::is_cocauset_command(cmd) => {
                // storage api_version = V1ttl, allow cocausetKV request only.
            }
            (ApiVersion::V2, ApiVersion::V1) if Self::is_solitontxn_command(cmd) => {
                // For compatibility, accept TiDB request only.
                for range in ranges {
                    let range = (
                        range.0.as_ref().map(AsRef::as_ref),
                        range.1.as_ref().map(AsRef::as_ref),
                    );
                    if APIV2::parse_range_mode(range) != KeyMode::TiDB {
                        return Err(ErrorInner::invalid_key_range_mode(
                            cmd,
                            storage_api_version,
                            range,
                        )
                        .into());
                    }
                }
            }
            (ApiVersion::V2, ApiVersion::V2) if Self::is_cocauset_command(cmd) => {
                for range in ranges {
                    let range = (
                        range.0.as_ref().map(AsRef::as_ref),
                        range.1.as_ref().map(AsRef::as_ref),
                    );
                    if APIV2::parse_range_mode(range) != KeyMode::cocauset {
                        return Err(ErrorInner::invalid_key_range_mode(
                            cmd,
                            storage_api_version,
                            range,
                        )
                        .into());
                    }
                }
            }
            (ApiVersion::V2, ApiVersion::V2) if Self::is_solitontxn_command(cmd) => {
                for range in ranges {
                    let range = (
                        range.0.as_ref().map(AsRef::as_ref),
                        range.1.as_ref().map(AsRef::as_ref),
                    );
                    if APIV2::parse_range_mode(range) != KeyMode::Txn {
                        return Err(ErrorInner::invalid_key_range_mode(
                            cmd,
                            storage_api_version,
                            range,
                        )
                        .into());
                    }
                }
            }
            _ => {
                return Err(Error::from(ErrorInner::ApiVersionNotMatched {
                    cmd,
                    storage_api_version,
                    req_api_version,
                }));
            }
        }
        Ok(())
    }

    /// Get value of the given key from a blackbrane.
    ///
    /// Only writes that are committed before `start_ts` are visible.
    pub fn get(
        &self,
        mut ctx: Context,
        key: Key,
        start_ts: TimeStamp,
    ) -> impl Future<Output = Result<(Option<Value>, HikvGetStatistics)>> {
        let stage_begin_ts = Instant::now_coarse();
        const CMD: CommandKind = CommandKind::get;
        let priority = ctx.get_priority();
        let priority_tag = get_priority_tag(priority);
        let resource_tag = self.resource_tag_factory.new_tag(&ctx);
        let concurrency_manager = self.concurrency_manager.clone();
        let api_version = self.api_version;

        let res = self.read_pool.spawn_handle(
            async move {
                let stage_scheduled_ts = Instant::now_coarse();
                tls_collect_query(
                    ctx.get_region_id(),
                    ctx.get_peer(),
                    key.as_encoded(),
                    key.as_encoded(),
                    false,
                    QueryKind::Get,
                );

                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();

                Self::check_api_version(api_version, ctx.api_version, CMD, [key.as_encoded()])?;

                let command_duration = einstfdbhikv_util::time::Instant::now_coarse();

                // The bypass_daggers and access_daggers set will be checked at most once.
                // `TsSet::vec` is more efficient here.
                let bypass_daggers = TsSet::vec_from_u64s(ctx.take_resolved_daggers());
                let access_daggers = TsSet::vec_from_u64s(ctx.take_committed_daggers());

                let snap_ctx = prepare_snap_ctx(
                    &ctx,
                    iter::once(&key),
                    start_ts,
                    &bypass_daggers,
                    &concurrency_manager,
                    CMD,
                )?;
                let blackbrane =
                    Self::with_tls_einstein_merkle_tree(|einstein_merkle_tree| Self::blackbrane(einstein_merkle_tree, snap_ctx)).await?;
                {
                    let begin_instant = Instant::now_coarse();
                    let stage_snap_recv_ts = begin_instant;
                    let mut statistics = Statistics::default();
                    let perf_statistics = PerfStatisticsInstant::new();
                    let snap_store = blackbraneStore::new(
                        blackbrane,
                        start_ts,
                        ctx.get_isolation_level(),
                        !ctx.get_not_fill_cache(),
                        bypass_daggers,
                        access_daggers,
                        false,
                    );
                    let result = snap_store
                        .get(&key, &mut statistics)
                        // map storage::solitontxn::Error -> storage::Error
                        .map_err(Error::from)
                        .map(|r| {
                            KV_COMMAND_KEYREAD_HISTOGRAM_STATIC.get(CMD).observe(1_f64);
                            r
                        });

                    let delta = perf_statistics.delta();
                    metrics::tls_collect_mutant_search_details(CMD, &statistics);
                    metrics::tls_collect_read_Causetxctx(ctx.get_region_id(), &statistics);
                    metrics::tls_collect_perf_stats(CMD, &delta);
                    SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(begin_instant.saturating_elapsed_secs());
                    SCHED_HISTOGRAM_VEC_STATIC
                        .get(CMD)
                        .observe(command_duration.saturating_elapsed_secs());

                    let stage_finished_ts = Instant::now_coarse();
                    let schedule_wait_time =
                        stage_scheduled_ts.saturating_duration_since(stage_begin_ts);
                    let blackbrane_wait_time =
                        stage_snap_recv_ts.saturating_duration_since(stage_scheduled_ts);
                    let wait_wall_time =
                        stage_snap_recv_ts.saturating_duration_since(stage_begin_ts);
                    let process_wall_time =
                        stage_finished_ts.saturating_duration_since(stage_snap_recv_ts);
                    let latency_stats = StageLatencyStats {
                        schedule_wait_time_ms: duration_to_ms(schedule_wait_time),
                        blackbrane_wait_time_ms: duration_to_ms(blackbrane_wait_time),
                        wait_wall_time_ms: duration_to_ms(wait_wall_time),
                        process_wall_time_ms: duration_to_ms(process_wall_time),
                    };
                    Ok((
                        result?,
                        HikvGetStatistics {
                            stats: statistics,
                            perf_stats: delta,
                            latency_stats,
                        },
                    ))
                }
            }
            .in_resource_metering_tag(resource_tag),
            priority,
            thread_rng().next_u64(),
        );
        async move {
            res.map_err(|_| Error::from(ErrorInner::SchedTooBusy))
                .await?
        }
    }

    /// Get values of a set of keys with separate context from a blackbrane, return a list of `Result`s.
    ///
    /// Only writes that are committed before their respective `start_ts` are visible.
    pub fn batch_get_command<
        P: 'static + ResponseBatchConsumer<(Option<Vec<u8>>, Statistics, PerfStatisticsDelta)>,
    >(
        &self,
        requests: Vec<GetRequest>,
        ids: Vec<u64>,
        consumer: P,
        begin_instant: einstfdbhikv_util::time::Instant,
    ) -> impl Future<Output = Result<()>> {
        const CMD: CommandKind = CommandKind::batch_get_command;
        // all requests in a batch have the same region, epoch, term, replica_read
        let priority = requests[0].get_context().get_priority();
        let concurrency_manager = self.concurrency_manager.clone();
        let api_version = self.api_version;

        // The resource tags of these batched requests are not the same, and it is quite expensive
        // to distinguish them, so we can find random one of them as a representative.
        let rand_index = rand::thread_rng().gen_range(0, requests.len());
        let resource_tag = self
            .resource_tag_factory
            .new_tag(requests[rand_index].get_context());

        let res = self.read_pool.spawn_handle(
            async move {
                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                KV_COMMAND_KEYREAD_HISTOGRAM_STATIC
                    .get(CMD)
                    .observe(requests.len() as f64);
                let command_duration = einstfdbhikv_util::time::Instant::now_coarse();
                let read_id = Some(ThreadReadId::new());
                let mut statistics = Statistics::default();
                let mut req_snaps = vec![];

                for (mut req, id) in requests.into_iter().zip(ids) {
                    let mut ctx = req.take_context();
                    let region_id = ctx.get_region_id();
                    let peer = ctx.get_peer();
                    let key = Key::from_cocauset(req.get_key());
                    tls_collect_query(
                        region_id,
                        peer,
                        key.as_encoded(),
                        key.as_encoded(),
                        false,
                        QueryKind::Get,
                    );

                    Self::check_api_version(api_version, ctx.api_version, CMD, [key.as_encoded()])?;

                    let start_ts = req.get_version().into();
                    let isolation_level = ctx.get_isolation_level();
                    let fill_cache = !ctx.get_not_fill_cache();
                    let bypass_daggers = TsSet::vec_from_u64s(ctx.take_resolved_daggers());
                    let access_daggers = TsSet::vec_from_u64s(ctx.take_committed_daggers());
                    let region_id = ctx.get_region_id();

                    let snap_ctx = match prepare_snap_ctx(
                        &ctx,
                        iter::once(&key),
                        start_ts,
                        &bypass_daggers,
                        &concurrency_manager,
                        CMD,
                    ) {
                        Ok(mut snap_ctx) => {
                            snap_ctx.read_id = if ctx.get_stale_read() {
                                None
                            } else {
                                read_id.clone()
                            };
                            snap_ctx
                        }
                        Err(e) => {
                            consumer.consume(id, Err(e), begin_instant);
                            continue;
                        }
                    };

                    let snap = Self::with_tls_einstein_merkle_tree(|einstein_merkle_tree| Self::blackbrane(einstein_merkle_tree, snap_ctx));
                    req_snaps.push((
                        snap,
                        key,
                        start_ts,
                        isolation_level,
                        fill_cache,
                        bypass_daggers,
                        access_daggers,
                        region_id,
                        id,
                    ));
                }
                Self::with_tls_einstein_merkle_tree(|einstein_merkle_tree| einstein_merkle_tree.release_blackbrane());
                for req_snap in req_snaps {
                    let (
                        snap,
                        key,
                        start_ts,
                        isolation_level,
                        fill_cache,
                        bypass_daggers,
                        access_daggers,
                        region_id,
                        id,
                    ) = req_snap;
                    match snap.await {
                        Ok(blackbrane) => {
                            match PointGetterBuilder::new(blackbrane, start_ts)
                                .fill_cache(fill_cache)
                                .isolation_level(isolation_level)
                                .multi(false)
                                .bypass_daggers(bypass_daggers)
                                .access_daggers(access_daggers)
                                .build()
                            {
                                Ok(mut point_getter) => {
                                    let perf_statistics = PerfStatisticsInstant::new();
                                    let v = point_getter.get(&key);
                                    let stat = point_getter.take_statistics();
                                    let delta = perf_statistics.delta();
                                    metrics::tls_collect_read_Causetxctx(region_id, &stat);
                                    metrics::tls_collect_perf_stats(CMD, &delta);
                                    statistics.add(&stat);
                                    consumer.consume(
                                        id,
                                        v.map_err(|e| Error::from(solitontxn::Error::from(e)))
                                            .map(|v| (v, stat, delta)),
                                        begin_instant,
                                    );
                                }
                                Err(e) => {
                                    consumer.consume(
                                        id,
                                        Err(Error::from(solitontxn::Error::from(e))),
                                        begin_instant,
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            consumer.consume(id, Err(e), begin_instant);
                        }
                    }
                }
                metrics::tls_collect_mutant_search_details(CMD, &statistics);
                SCHED_HISTOGRAM_VEC_STATIC
                    .get(CMD)
                    .observe(command_duration.saturating_elapsed_secs());

                Ok(())
            }
            .in_resource_metering_tag(resource_tag),
            priority,
            thread_rng().next_u64(),
        );
        async move {
            res.map_err(|_| Error::from(ErrorInner::SchedTooBusy))
                .await?
        }
    }

    /// Get values of a set of keys in a batch from the blackbrane.
    ///
    /// Only writes that are committed before `start_ts` are visible.
    pub fn batch_get(
        &self,
        mut ctx: Context,
        keys: Vec<Key>,
        start_ts: TimeStamp,
    ) -> impl Future<Output = Result<(Vec<Result<HikvPair>>, HikvGetStatistics)>> {
        let stage_begin_ts = Instant::now_coarse();
        const CMD: CommandKind = CommandKind::batch_get;
        let priority = ctx.get_priority();
        let priority_tag = get_priority_tag(priority);
        let resource_tag = self.resource_tag_factory.new_tag(&ctx);
        let concurrency_manager = self.concurrency_manager.clone();
        let api_version = self.api_version;

        let res = self.read_pool.spawn_handle(
            async move {
                let stage_scheduled_ts = Instant::now_coarse();
                let mut key_ranges = vec![];
                for key in &keys {
                    key_ranges.push(build_key_range(key.as_encoded(), key.as_encoded(), false));
                }
                tls_collect_query_batch(
                    ctx.get_region_id(),
                    ctx.get_peer(),
                    key_ranges,
                    QueryKind::Get,
                );

                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();

                Self::check_api_version(
                    api_version,
                    ctx.api_version,
                    CMD,
                    keys.iter().map(Key::as_encoded),
                )?;

                let command_duration = einstfdbhikv_util::time::Instant::now_coarse();

                let bypass_daggers = TsSet::from_u64s(ctx.take_resolved_daggers());
                let access_daggers = TsSet::from_u64s(ctx.take_committed_daggers());

                let snap_ctx = prepare_snap_ctx(
                    &ctx,
                    &keys,
                    start_ts,
                    &bypass_daggers,
                    &concurrency_manager,
                    CMD,
                )?;
                let blackbrane =
                    Self::with_tls_einstein_merkle_tree(|einstein_merkle_tree| Self::blackbrane(einstein_merkle_tree, snap_ctx)).await?;
                {
                    let begin_instant = Instant::now_coarse();
                    let stage_snap_recv_ts = begin_instant;
                    let mut statistics = Statistics::default();
                    let perf_statistics = PerfStatisticsInstant::new();
                    let snap_store = blackbraneStore::new(
                        blackbrane,
                        start_ts,
                        ctx.get_isolation_level(),
                        !ctx.get_not_fill_cache(),
                        bypass_daggers,
                        access_daggers,
                        false,
                    );
                    let result = snap_store
                        .batch_get(&keys, &mut statistics)
                        .map_err(Error::from)
                        .map(|v| {
                            let fdbhikv_pairs: Vec<_> = v
                                .into_iter()
                                .zip(keys)
                                .filter(|&(ref v, ref _k)| {
                                    !(v.is_ok() && v.as_ref().unwrap().is_none())
                                })
                                .map(|(v, k)| match v {
                                    Ok(Some(x)) => Ok((k.into_cocauset().unwrap(), x)),
                                    Err(e) => Err(Error::from(e)),
                                    _ => unreachable!(),
                                })
                                .collect();
                            KV_COMMAND_KEYREAD_HISTOGRAM_STATIC
                                .get(CMD)
                                .observe(fdbhikv_pairs.len() as f64);
                            fdbhikv_pairs
                        });

                    let delta = perf_statistics.delta();
                    metrics::tls_collect_mutant_search_details(CMD, &statistics);
                    metrics::tls_collect_read_Causetxctx(ctx.get_region_id(), &statistics);
                    metrics::tls_collect_perf_stats(CMD, &delta);
                    SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(begin_instant.saturating_elapsed_secs());
                    SCHED_HISTOGRAM_VEC_STATIC
                        .get(CMD)
                        .observe(command_duration.saturating_elapsed_secs());

                    let stage_finished_ts = Instant::now_coarse();
                    let schedule_wait_time =
                        stage_scheduled_ts.saturating_duration_since(stage_begin_ts);
                    let blackbrane_wait_time =
                        stage_snap_recv_ts.saturating_duration_since(stage_scheduled_ts);
                    let wait_wall_time =
                        stage_snap_recv_ts.saturating_duration_since(stage_begin_ts);
                    let process_wall_time =
                        stage_finished_ts.saturating_duration_since(stage_snap_recv_ts);
                    let latency_stats = StageLatencyStats {
                        schedule_wait_time_ms: duration_to_ms(schedule_wait_time),
                        blackbrane_wait_time_ms: duration_to_ms(blackbrane_wait_time),
                        wait_wall_time_ms: duration_to_ms(wait_wall_time),
                        process_wall_time_ms: duration_to_ms(process_wall_time),
                    };
                    Ok((
                        result?,
                        HikvGetStatistics {
                            stats: statistics,
                            perf_stats: delta,
                            latency_stats,
                        },
                    ))
                }
            }
            .in_resource_metering_tag(resource_tag),
            priority,
            thread_rng().next_u64(),
        );

        async move {
            res.map_err(|_| Error::from(ErrorInner::SchedTooBusy))
                .await?
        }
    }

    /// SentinelSearch keys in [`start_key`, `end_key`) up to `limit` keys from the blackbrane.
    /// If `reverse_mutant_search` is true, it mutant_searchs [`end_key`, `start_key`) in descending order.
    /// If `end_key` is `None`, it means the upper bound or the lower bound if reverse mutant_search is unbounded.
    ///
    /// Only writes committed before `start_ts` are visible.
    pub fn mutant_search(
        &self,
        mut ctx: Context,
        start_key: Key,
        end_key: Option<Key>,
        limit: usize,
        sample_step: usize,
        start_ts: TimeStamp,
        key_only: bool,
        reverse_mutant_search: bool,
    ) -> impl Future<Output = Result<Vec<Result<HikvPair>>>> {
        const CMD: CommandKind = CommandKind::mutant_search;
        let priority = ctx.get_priority();
        let priority_tag = get_priority_tag(priority);
        let resource_tag = self.resource_tag_factory.new_tag(&ctx);
        let concurrency_manager = self.concurrency_manager.clone();
        let api_version = self.api_version;

        let res = self.read_pool.spawn_handle(
            async move {
                {
                    let end_key = match &end_key {
                        Some(k) => k.as_encoded().as_slice(),
                        None => &[],
                    };
                    tls_collect_query(
                        ctx.get_region_id(),
                        ctx.get_peer(),
                        start_key.as_encoded(),
                        end_key,
                        reverse_mutant_search,
                        QueryKind::SentinelSearch,
                    );
                }
                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();

                Self::check_api_version_ranges(
                    api_version,
                    ctx.api_version,
                    CMD,
                    [(
                        Some(start_key.as_encoded()),
                        end_key.as_ref().map(Key::as_encoded),
                    )],
                )?;

                let (mut start_key, mut end_key) = (Some(start_key), end_key);
                if reverse_mutant_search {
                    std::mem::swap(&mut start_key, &mut end_key);
                }
                let command_duration = einstfdbhikv_util::time::Instant::now_coarse();

                let bypass_daggers = TsSet::from_u64s(ctx.take_resolved_daggers());
                let access_daggers = TsSet::from_u64s(ctx.take_committed_daggers());

                // Update max_ts and check the in-memory dagger table before getting the blackbrane
                if !ctx.get_stale_read() {
                    concurrency_manager.update_max_ts(start_ts);
                }
                if ctx.get_isolation_level() == IsolationLevel::Si {
                    let begin_instant = Instant::now();
                    concurrency_manager
                        .read_range_check(start_key.as_ref(), end_key.as_ref(), |key, dagger| {
                            Dagger::check_ts_conflict(
                                Cow::Borrowed(dagger),
                                key,
                                start_ts,
                                &bypass_daggers,
                            )
                        })
                        .map_err(|e| {
                            CHECK_MEM_LOCK_DURATION_HISTOGRAM_VEC
                                .get(CMD)
                                .daggered
                                .observe(begin_instant.saturating_elapsed().as_secs_f64());
                            solitontxn::Error::from_epaxos(e)
                        })?;
                    CHECK_MEM_LOCK_DURATION_HISTOGRAM_VEC
                        .get(CMD)
                        .undaggered
                        .observe(begin_instant.saturating_elapsed().as_secs_f64());
                }

                let mut snap_ctx = SnapContext {
                    pb_ctx: &ctx,
                    start_ts,
                    ..Default::default()
                };
                if need_check_daggers_in_replica_read(&ctx) {
                    let mut key_range = KeyRange::default();
                    if let Some(start_key) = &start_key {
                        key_range.set_start_key(start_key.as_encoded().to_vec());
                    }
                    if let Some(end_key) = &end_key {
                        key_range.set_end_key(end_key.as_encoded().to_vec());
                    }
                    snap_ctx.key_ranges = vec![key_range];
                }

                let blackbrane =
                    Self::with_tls_einstein_merkle_tree(|einstein_merkle_tree| Self::blackbrane(einstein_merkle_tree, snap_ctx)).await?;
                {
                    let begin_instant = Instant::now_coarse();
                    let perf_statistics = PerfStatisticsInstant::new();

                    let snap_store = blackbraneStore::new(
                        blackbrane,
                        start_ts,
                        ctx.get_isolation_level(),
                        !ctx.get_not_fill_cache(),
                        bypass_daggers,
                        access_daggers,
                        false,
                    );

                    let mut mutant_searchner =
                        snap_store.mutant_searchner(reverse_mutant_search, key_only, false, start_key, end_key)?;
                    let res = mutant_searchner.mutant_search(limit, sample_step);

                    let statistics = mutant_searchner.take_statistics();
                    let delta = perf_statistics.delta();
                    metrics::tls_collect_mutant_search_details(CMD, &statistics);
                    metrics::tls_collect_read_Causetxctx(ctx.get_region_id(), &statistics);
                    metrics::tls_collect_perf_stats(CMD, &delta);
                    SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(begin_instant.saturating_elapsed_secs());
                    SCHED_HISTOGRAM_VEC_STATIC
                        .get(CMD)
                        .observe(command_duration.saturating_elapsed_secs());

                    res.map_err(Error::from).map(|results| {
                        KV_COMMAND_KEYREAD_HISTOGRAM_STATIC
                            .get(CMD)
                            .observe(results.len() as f64);
                        results
                            .into_iter()
                            .map(|x| x.map_err(Error::from))
                            .collect()
                    })
                }
            }
            .in_resource_metering_tag(resource_tag),
            priority,
            thread_rng().next_u64(),
        );

        async move {
            res.map_err(|_| Error::from(ErrorInner::SchedTooBusy))
                .await?
        }
    }

    pub fn mutant_search_dagger(
        &self,
        mut ctx: Context,
        max_ts: TimeStamp,
        start_key: Option<Key>,
        end_key: Option<Key>,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<DaggerInfo>>> {
        const CMD: CommandKind = CommandKind::mutant_search_dagger;
        let priority = ctx.get_priority();
        let priority_tag = get_priority_tag(priority);
        let resource_tag = self.resource_tag_factory.new_tag(&ctx);
        let concurrency_manager = self.concurrency_manager.clone();
        let api_version = self.api_version;
        // Do not allow replica read for mutant_search_dagger.
        ctx.set_replica_read(false);

        let res = self.read_pool.spawn_handle(
            async move {
                if let Some(start_key) = &start_key {
                    let end_key = match &end_key {
                        Some(k) => k.as_encoded().as_slice(),
                        None => &[],
                    };
                    tls_collect_query(
                        ctx.get_region_id(),
                        ctx.get_peer(),
                        start_key.as_encoded(),
                        end_key,
                        false,
                        QueryKind::SentinelSearch,
                    );
                }

                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();

                Self::check_api_version_ranges(
                    api_version,
                    ctx.api_version,
                    CMD,
                    [(
                        start_key.as_ref().map(Key::as_encoded),
                        end_key.as_ref().map(Key::as_encoded),
                    )],
                )?;

                let command_duration = einstfdbhikv_util::time::Instant::now_coarse();

                concurrency_manager.update_max_ts(max_ts);
                let begin_instant = Instant::now();
                // TODO: Though it's very unlikely to find a conflicting memory dagger here, it's not
                // a good idea to return an error to the client, making the GC fail. A better
                // approach is to wait for these daggers to be undaggered.
                concurrency_manager.read_range_check(
                    start_key.as_ref(),
                    end_key.as_ref(),
                    |key, dagger| {
                        // `Dagger::check_ts_conflict` can't be used here, because DaggerType::Dagger
                        // can't be ignored in this case.
                        if dagger.ts <= max_ts {
                            CHECK_MEM_LOCK_DURATION_HISTOGRAM_VEC
                                .get(CMD)
                                .daggered
                                .observe(begin_instant.saturating_elapsed().as_secs_f64());
                            Err(solitontxn::Error::from_epaxos(epaxos::ErrorInner::KeyIsDaggered(
                                dagger.clone().into_dagger_info(key.to_cocauset()?),
                            )))
                        } else {
                            Ok(())
                        }
                    },
                )?;
                CHECK_MEM_LOCK_DURATION_HISTOGRAM_VEC
                    .get(CMD)
                    .undaggered
                    .observe(begin_instant.saturating_elapsed().as_secs_f64());

                let snap_ctx = SnapContext {
                    pb_ctx: &ctx,
                    ..Default::default()
                };

                let blackbrane =
                    Self::with_tls_einstein_merkle_tree(|einstein_merkle_tree| Self::blackbrane(einstein_merkle_tree, snap_ctx)).await?;
                {
                    let begin_instant = Instant::now_coarse();
                    let mut statistics = Statistics::default();
                    let perf_statistics = PerfStatisticsInstant::new();
                    let mut reader = EpaxosReader::new(
                        blackbrane,
                        Some(SentinelSearchMode::Forward),
                        !ctx.get_not_fill_cache(),
                    );
                    let result = reader
                        .mutant_search_daggers(
                            start_key.as_ref(),
                            end_key.as_ref(),
                            |dagger| dagger.ts <= max_ts,
                            limit,
                        )
                        .map_err(solitontxn::Error::from);
                    statistics.add(&reader.statistics);
                    let (fdbhikv_pairs, _) = result?;
                    let mut daggers = Vec::with_capacity(fdbhikv_pairs.len());
                    for (key, dagger) in fdbhikv_pairs {
                        let dagger_info =
                            dagger.into_dagger_info(key.into_cocauset().map_err(solitontxn::Error::from)?);
                        daggers.push(dagger_info);
                    }

                    let delta = perf_statistics.delta();
                    metrics::tls_collect_mutant_search_details(CMD, &statistics);
                    metrics::tls_collect_read_Causetxctx(ctx.get_region_id(), &statistics);
                    metrics::tls_collect_perf_stats(CMD, &delta);
                    SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(begin_instant.saturating_elapsed_secs());
                    SCHED_HISTOGRAM_VEC_STATIC
                        .get(CMD)
                        .observe(command_duration.saturating_elapsed_secs());

                    Ok(daggers)
                }
            }
            .in_resource_metering_tag(resource_tag),
            priority,
            thread_rng().next_u64(),
        );
        async move {
            res.map_err(|_| Error::from(ErrorInner::SchedTooBusy))
                .await?
        }
    }

    // The entry point of the storage scheduler. Not only transaction commands need to access keys serially.
    pub fn sched_solitontxn_command<T: StorageCallbackType>(
        &self,
        cmd: TypedCommand<T>,
        callback: Callback<T>,
    ) -> Result<()> {
        use crate::einsteindb::storage::solitontxn::commands::{
            AcquirePessimisticDagger, Prewrite, PrewritePessimistic,
        };

        let cmd: Command = cmd.into();

        match &cmd {
            Command::Prewrite(Prewrite { mutations, .. }) => {
                let keys = mutations.iter().map(|m| m.key().as_encoded());
                Self::check_api_version(
                    self.api_version,
                    cmd.ctx().api_version,
                    CommandKind::prewrite,
                    keys.clone(),
                )?;
                check_key_size!(keys, self.max_key_size, callback);
            }
            Command::PrewritePessimistic(PrewritePessimistic { mutations, .. }) => {
                let keys = mutations.iter().map(|(m, _)| m.key().as_encoded());
                Self::check_api_version(
                    self.api_version,
                    cmd.ctx().api_version,
                    CommandKind::prewrite,
                    keys.clone(),
                )?;
                check_key_size!(keys, self.max_key_size, callback);
            }
            Command::AcquirePessimisticDagger(AcquirePessimisticDagger { keys, .. }) => {
                let keys = keys.iter().map(|k| k.0.as_encoded());
                Self::check_api_version(
                    self.api_version,
                    cmd.ctx().api_version,
                    CommandKind::prewrite,
                    keys.clone(),
                )?;
                check_key_size!(keys, self.max_key_size, callback);
            }
            _ => {}
        }

        fail_point!("storage_drop_message", |_| Ok(()));
        cmd.incr_cmd_metric();
        self.sched.run_cmd(cmd, T::callback(callback));

        Ok(())
    }

    /// Delete all keys in the range [`start_key`, `end_key`).
    ///
    /// All keys in the range will be deleted permanently regardless of their timestamps.
    /// This means that deleted keys will not be retrievable by specifying an older timestamp.
    /// If `notify_only` is set, the data will not be immediately deleted, but the operation will
    /// still be replicated via Raft. This is used to notify that the data will be deleted by
    /// [`unsafe_destroy_range`](crate::server::gc_worker::GcTask::UnsafeDestroyRange) soon.
    pub fn delete_range(
        &self,
        ctx: Context,
        start_key: Key,
        end_key: Key,
        notify_only: bool,
        callback: Callback<()>,
    ) -> Result<()> {
        Self::check_api_version_ranges(
            self.api_version,
            ctx.api_version,
            CommandKind::delete_range,
            [(Some(start_key.as_encoded()), Some(end_key.as_encoded()))],
        )?;

        let mut modifies = Vec::with_capacity(DATA_CFS.len());
        for cf in DATA_CFS {
            modifies.push(Modify::DeleteRange(
                cf,
                start_key.clone(),
                end_key.clone(),
                notify_only,
            ));
        }

        let mut batch = WriteData::from_modifies(modifies);
        batch.set_allowed_on_disk_almost_full();
        self.einstein_merkle_tree.async_write(
            &ctx,
            batch,
            Box::new(|res| callback(res.map_err(Error::from))),
        )?;
        KV_COMMAND_COUNTER_VEC_STATIC.delete_range.inc();
        Ok(())
    }

    /// Get the value of a cocauset key.
    pub fn cocauset_get(
        &self,
        ctx: Context,
        cf: String,
        key: Vec<u8>,
    ) -> impl Future<Output = Result<Option<Vec<u8>>>> {
        const CMD: CommandKind = CommandKind::cocauset_get;
        let priority = ctx.get_priority();
        let priority_tag = get_priority_tag(priority);
        let resource_tag = self.resource_tag_factory.new_tag(&ctx);
        let api_version = self.api_version;

        let res = self.read_pool.spawn_handle(
            async move {
                tls_collect_query(
                    ctx.get_region_id(),
                    ctx.get_peer(),
                    &key,
                    &key,
                    false,
                    QueryKind::Get,
                );

                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();

                Self::check_api_version(api_version, ctx.api_version, CMD, [&key])?;

                let command_duration = einstfdbhikv_util::time::Instant::now_coarse();
                let snap_ctx = SnapContext {
                    pb_ctx: &ctx,
                    ..Default::default()
                };
                let blackbrane =
                    Self::with_tls_einstein_merkle_tree(|einstein_merkle_tree| Self::blackbrane(einstein_merkle_tree, snap_ctx)).await?;
                let store = cocausetStore::new(blackbrane, api_version);
                let cf = Self::cocausetfdbhikv_cf(&cf, api_version)?;
                {
                    let begin_instant = Instant::now_coarse();
                    let mut stats = Statistics::default();
                    let r = store
                        .cocauset_get_key_value(cf, &Key::from_encoded(key), &mut stats)
                        .map_err(Error::from);
                    KV_COMMAND_KEYREAD_HISTOGRAM_STATIC.get(CMD).observe(1_f64);
                    tls_collect_read_Causetxctx(ctx.get_region_id(), &stats);
                    SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(begin_instant.saturating_elapsed_secs());
                    SCHED_HISTOGRAM_VEC_STATIC
                        .get(CMD)
                        .observe(command_duration.saturating_elapsed_secs());
                    r
                }
            }
            .in_resource_metering_tag(resource_tag),
            priority,
            thread_rng().next_u64(),
        );

        async move {
            res.map_err(|_| Error::from(ErrorInner::SchedTooBusy))
                .await?
        }
    }

    /// Get the values of a set of cocauset keys, return a list of `Result`s.
    pub fn cocauset_batch_get_command<P: 'static + ResponseBatchConsumer<Option<Vec<u8>>>>(
        &self,
        gets: Vec<cocausetGetRequest>,
        ids: Vec<u64>,
        consumer: P,
    ) -> impl Future<Output = Result<()>> {
        const CMD: CommandKind = CommandKind::cocauset_batch_get_command;
        // all requests in a batch have the same region, epoch, term, replica_read
        let priority = gets[0].get_context().get_priority();
        let priority_tag = get_priority_tag(priority);
        let api_version = self.api_version;

        // The resource tags of these batched requests are not the same, and it is quite expensive
        // to distinguish them, so we can find random one of them as a representative.
        let rand_index = rand::thread_rng().gen_range(0, gets.len());
        let resource_tag = self
            .resource_tag_factory
            .new_tag(gets[rand_index].get_context());

        let res = self.read_pool.spawn_handle(
            async move {
                for get in &gets {
                    let key = get.key.to_owned();
                    let region_id = get.get_context().get_region_id();
                    let peer = get.get_context().get_peer();
                    tls_collect_query(region_id, peer, &key, &key, false, QueryKind::Get);
                }
                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();
                KV_COMMAND_KEYREAD_HISTOGRAM_STATIC
                    .get(CMD)
                    .observe(gets.len() as f64);

                for get in &gets {
                    Self::check_api_version(
                        api_version,
                        get.get_context().api_version,
                        CMD,
                        [get.get_key()],
                    )
                    .map_err(Error::from)?;
                }

                let command_duration = einstfdbhikv_util::time::Instant::now_coarse();
                let read_id = Some(ThreadReadId::new());
                let mut snaps = vec![];
                for (req, id) in gets.into_iter().zip(ids) {
                    let snap_ctx = SnapContext {
                        pb_ctx: req.get_context(),
                        read_id: read_id.clone(),
                        ..Default::default()
                    };
                    let snap = Self::with_tls_einstein_merkle_tree(|einstein_merkle_tree| Self::blackbrane(einstein_merkle_tree, snap_ctx));
                    snaps.push((id, req, snap));
                }
                Self::with_tls_einstein_merkle_tree(|einstein_merkle_tree| einstein_merkle_tree.release_blackbrane());
                let begin_instant = Instant::now_coarse();
                for (id, mut req, snap) in snaps {
                    let ctx = req.take_context();
                    let cf = req.take_cf();
                    let key = req.take_key();
                    match snap.await {
                        Ok(blackbrane) => {
                            let mut stats = Statistics::default();
                            let store = cocausetStore::new(blackbrane, api_version);
                            match Self::cocausetfdbhikv_cf(&cf, api_version) {
                                Ok(cf) => {
                                    consumer.consume(
                                        id,
                                        store
                                            .cocauset_get_key_value(
                                                cf,
                                                &Key::from_encoded(key),
                                                &mut stats,
                                            )
                                            .map_err(Error::from),
                                        begin_instant,
                                    );
                                    tls_collect_read_Causetxctx(ctx.get_region_id(), &stats);
                                }
                                Err(e) => {
                                    consumer.consume(id, Err(e), begin_instant);
                                }
                            }
                        }
                        Err(e) => {
                            consumer.consume(id, Err(e), begin_instant);
                        }
                    }
                }

                SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                    .get(CMD)
                    .observe(begin_instant.saturating_elapsed_secs());
                SCHED_HISTOGRAM_VEC_STATIC
                    .get(CMD)
                    .observe(command_duration.saturating_elapsed_secs());
                Ok(())
            }
            .in_resource_metering_tag(resource_tag),
            priority,
            thread_rng().next_u64(),
        );
        async move {
            res.map_err(|_| Error::from(ErrorInner::SchedTooBusy))
                .await?
        }
    }

    /// Get the values of some cocauset keys in a batch.
    pub fn cocauset_batch_get(
        &self,
        ctx: Context,
        cf: String,
        keys: Vec<Vec<u8>>,
    ) -> impl Future<Output = Result<Vec<Result<HikvPair>>>> {
        const CMD: CommandKind = CommandKind::cocauset_batch_get;
        let priority = ctx.get_priority();
        let priority_tag = get_priority_tag(priority);
        let resource_tag = self.resource_tag_factory.new_tag(&ctx);
        let api_version = self.api_version;

        let res = self.read_pool.spawn_handle(
            async move {
                let mut key_ranges = vec![];
                for key in &keys {
                    key_ranges.push(build_key_range(key, key, false));
                }
                tls_collect_query_batch(
                    ctx.get_region_id(),
                    ctx.get_peer(),
                    key_ranges,
                    QueryKind::Get,
                );

                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();

                Self::check_api_version(api_version, ctx.api_version, CMD, &keys)?;

                let command_duration = einstfdbhikv_util::time::Instant::now_coarse();
                let snap_ctx = SnapContext {
                    pb_ctx: &ctx,
                    ..Default::default()
                };
                let blackbrane =
                    Self::with_tls_einstein_merkle_tree(|einstein_merkle_tree| Self::blackbrane(einstein_merkle_tree, snap_ctx)).await?;
                let store = cocausetStore::new(blackbrane, api_version);
                {
                    let begin_instant = Instant::now_coarse();

                    let cf = Self::cocausetfdbhikv_cf(&cf, api_version)?;
                    // no mutant_search_count for this kind of op.
                    let mut stats = Statistics::default();
                    let result: Vec<Result<HikvPair>> = keys
                        .into_iter()
                        .map(Key::from_encoded)
                        .map(|k| {
                            let v = store
                                .cocauset_get_key_value(cf, &k, &mut stats)
                                .map_err(Error::from);
                            (k, v)
                        })
                        .filter(|&(_, ref v)| !(v.is_ok() && v.as_ref().unwrap().is_none()))
                        .map(|(k, v)| match v {
                            Ok(v) => Ok((k.into_encoded(), v.unwrap())),
                            Err(v) => Err(v),
                        })
                        .collect();

                    KV_COMMAND_KEYREAD_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(stats.data.Causetxctx_stats.read_keys as f64);
                    tls_collect_read_Causetxctx(ctx.get_region_id(), &stats);
                    SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(begin_instant.saturating_elapsed_secs());
                    SCHED_HISTOGRAM_VEC_STATIC
                        .get(CMD)
                        .observe(command_duration.saturating_elapsed_secs());
                    Ok(result)
                }
            }
            .in_resource_metering_tag(resource_tag),
            priority,
            thread_rng().next_u64(),
        );

        async move {
            res.map_err(|_| Error::from(ErrorInner::SchedTooBusy))
                .await?
        }
    }

    /// Write a cocauset key to the storage.
    pub fn cocauset_put(
        &self,
        ctx: Context,
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl: u64,
        callback: Callback<()>,
    ) -> Result<()> {
        const CMD: CommandKind = CommandKind::cocauset_put;
        let api_version = self.api_version;

        Self::check_api_version(api_version, ctx.api_version, CMD, [&key])?;

        check_key_size!(Some(&key).into_iter(), self.max_key_size, callback);

        let m = match_template_api_version!(
            API,
            match self.api_version {
                ApiVersion::API => {
                    if !API::IS_TTL_ENABLED && ttl != 0 {
                        return Err(Error::from(ErrorInner::TTLNotEnabled));
                    }

                    let cocauset_value = cocausetValue {
                        user_value: value,
                        expire_ts: ttl_to_expire_ts(ttl),
                    };
                    Modify::Put(
                        Self::cocausetfdbhikv_cf(&cf, self.api_version)?,
                        Key::from_encoded(key),
                        API::encode_cocauset_value_owned(cocauset_value),
                    )
                }
            }
        );

        let mut batch = WriteData::from_modifies(vec![m]);
        batch.set_allowed_on_disk_almost_full();

        self.einstein_merkle_tree.async_write(
            &ctx,
            batch,
            Box::new(|res| callback(res.map_err(Error::from))),
        )?;
        KV_COMMAND_COUNTER_VEC_STATIC.cocauset_put.inc();
        Ok(())
    }

    /// Write some keys to the storage in a batch.
    pub fn cocauset_batch_put(
        &self,
        ctx: Context,
        cf: String,
        pairs: Vec<HikvPair>,
        ttls: Vec<u64>,
        callback: Callback<()>,
    ) -> Result<()> {
        Self::check_api_version(
            self.api_version,
            ctx.api_version,
            CommandKind::cocauset_batch_put,
            pairs.iter().map(|(ref k, _)| k),
        )?;

        let cf = Self::cocausetfdbhikv_cf(&cf, self.api_version)?;

        check_key_size!(
            pairs.iter().map(|(ref k, _)| k),
            self.max_key_size,
            callback
        );

        let modifies = match_template_api_version!(
            API,
            match self.api_version {
                ApiVersion::API => {
                    if !API::IS_TTL_ENABLED {
                        if ttls.iter().any(|&x| x != 0) {
                            return Err(Error::from(ErrorInner::TTLNotEnabled));
                        }
                    } else if ttls.len() != pairs.len() {
                        return Err(Error::from(ErrorInner::TTLsLenNotEqualsToPairs));
                    }

                    pairs
                        .into_iter()
                        .zip(ttls)
                        .map(|((k, v), ttl)| {
                            let cocauset_value = cocausetValue {
                                user_value: v,
                                expire_ts: ttl_to_expire_ts(ttl),
                            };
                            Modify::Put(
                                cf,
                                Key::from_encoded(k),
                                API::encode_cocauset_value_owned(cocauset_value),
                            )
                        })
                        .collect()
                }
            }
        );

        let mut batch = WriteData::from_modifies(modifies);
        batch.set_allowed_on_disk_almost_full();

        self.einstein_merkle_tree.async_write(
            &ctx,
            batch,
            Box::new(|res| callback(res.map_err(Error::from))),
        )?;
        KV_COMMAND_COUNTER_VEC_STATIC.cocauset_batch_put.inc();
        Ok(())
    }

    /// Delete a cocauset key from the storage.
    pub fn cocauset_delete(
        &self,
        ctx: Context,
        cf: String,
        key: Vec<u8>,
        callback: Callback<()>,
    ) -> Result<()> {
        Self::check_api_version(
            self.api_version,
            ctx.api_version,
            CommandKind::cocauset_delete,
            [&key],
        )?;

        check_key_size!(Some(&key).into_iter(), self.max_key_size, callback);

        let mut batch = WriteData::from_modifies(vec![Modify::Delete(
            Self::cocausetfdbhikv_cf(&cf, self.api_version)?,
            Key::from_encoded(key),
        )]);
        batch.set_allowed_on_disk_almost_full();

        self.einstein_merkle_tree.async_write(
            &ctx,
            batch,
            Box::new(|res| callback(res.map_err(Error::from))),
        )?;
        KV_COMMAND_COUNTER_VEC_STATIC.cocauset_delete.inc();
        Ok(())
    }

    /// Delete all cocauset keys in [`start_key`, `end_key`).
    pub fn cocauset_delete_range(
        &self,
        ctx: Context,
        cf: String,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        callback: Callback<()>,
    ) -> Result<()> {
        check_key_size!([&start_key, &end_key], self.max_key_size, callback);
        Self::check_api_version_ranges(
            self.api_version,
            ctx.api_version,
            CommandKind::cocauset_delete_range,
            [(Some(&start_key), Some(&end_key))],
        )?;

        let cf = Self::cocausetfdbhikv_cf(&cf, self.api_version)?;
        let start_key = Key::from_encoded(start_key);
        let end_key = Key::from_encoded(end_key);

        let mut batch =
            WriteData::from_modifies(vec![Modify::DeleteRange(cf, start_key, end_key, false)]);
        batch.set_allowed_on_disk_almost_full();

        self.einstein_merkle_tree.async_write(
            &ctx,
            batch,
            Box::new(|res| callback(res.map_err(Error::from))),
        )?;
        KV_COMMAND_COUNTER_VEC_STATIC.cocauset_delete_range.inc();
        Ok(())
    }

    /// Delete some cocauset keys in a batch.
    pub fn cocauset_batch_delete(
        &self,
        ctx: Context,
        cf: String,
        keys: Vec<Vec<u8>>,
        callback: Callback<()>,
    ) -> Result<()> {
        Self::check_api_version(
            self.api_version,
            ctx.api_version,
            CommandKind::cocauset_batch_delete,
            &keys,
        )?;

        let cf = Self::cocausetfdbhikv_cf(&cf, self.api_version)?;
        check_key_size!(keys.iter(), self.max_key_size, callback);

        let modifies = keys
            .into_iter()
            .map(|k| Modify::Delete(cf, Key::from_encoded(k)))
            .collect();

        let mut batch = WriteData::from_modifies(modifies);
        batch.set_allowed_on_disk_almost_full();

        self.einstein_merkle_tree.async_write(
            &ctx,
            batch,
            Box::new(|res| callback(res.map_err(Error::from))),
        )?;
        KV_COMMAND_COUNTER_VEC_STATIC.cocauset_batch_delete.inc();
        Ok(())
    }

    /// SentinelSearch cocauset keys in a range.
    ///
    /// If `reverse_mutant_search` is false, the range is [`start_key`, `end_key`); otherwise, the range is
    /// [`end_key`, `start_key`) and it mutant_searchs from `start_key` and goes timelike_curvatures. If `end_key` is `None`, it
    /// means unbounded.
    ///
    /// This function mutant_searchs at most `limit` keys.
    ///
    /// If `key_only` is true, the value
    /// corresponding to the key will not be read out. Only mutant_searchned keys will be returned.
    pub fn cocauset_mutant_search(
        &self,
        ctx: Context,
        cf: String,
        start_key: Vec<u8>,
        end_key: Option<Vec<u8>>,
        limit: usize,
        key_only: bool,
        reverse_mutant_search: bool,
    ) -> impl Future<Output = Result<Vec<Result<HikvPair>>>> {
        const CMD: CommandKind = CommandKind::cocauset_mutant_search;
        let priority = ctx.get_priority();
        let priority_tag = get_priority_tag(priority);
        let resource_tag = self.resource_tag_factory.new_tag(&ctx);
        let api_version = self.api_version;

        let res = self.read_pool.spawn_handle(
            async move {
                {
                    tls_collect_query(
                        ctx.get_region_id(),
                        ctx.get_peer(),
                        &start_key,
                        end_key.as_ref().unwrap_or(&vec![]),
                        reverse_mutant_search,
                        QueryKind::SentinelSearch,
                    );
                }

                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();

                Self::check_api_version_ranges(
                    api_version,
                    ctx.api_version,
                    CMD,
                    [(Some(&start_key), end_key.as_ref())],
                )?;

                let command_duration = einstfdbhikv_util::time::Instant::now_coarse();
                let snap_ctx = SnapContext {
                    pb_ctx: &ctx,
                    ..Default::default()
                };
                let blackbrane =
                    Self::with_tls_einstein_merkle_tree(|einstein_merkle_tree| Self::blackbrane(einstein_merkle_tree, snap_ctx)).await?;
                let cf = Self::cocausetfdbhikv_cf(&cf, api_version)?;
                {
                    let store = cocausetStore::new(blackbrane, api_version);
                    let begin_instant = Instant::now_coarse();

                    let start_key = Key::from_encoded(start_key);
                    let end_key = end_key.map(Key::from_encoded);

                    let mut statistics = Statistics::default();
                    let result = if reverse_mutant_search {
                        store
                            .reverse_cocauset_mutant_search(
                                cf,
                                &start_key,
                                end_key.as_ref(),
                                limit,
                                &mut statistics,
                                key_only,
                            )
                            .await
                    } else {
                        store
                            .lightlike_completion_cocauset_mutant_search(
                                cf,
                                &start_key,
                                end_key.as_ref(),
                                limit,
                                &mut statistics,
                                key_only,
                            )
                            .await
                    }
                    .map(|pairs| {
                        pairs
                            .into_iter()
                            .map(|pair| pair.map_err(Error::from))
                            .collect()
                    })
                    .map_err(Error::from);

                    metrics::tls_collect_read_Causetxctx(ctx.get_region_id(), &statistics);
                    KV_COMMAND_KEYREAD_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(statistics.data.Causetxctx_stats.read_keys as f64);
                    metrics::tls_collect_mutant_search_details(CMD, &statistics);
                    SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(begin_instant.saturating_elapsed_secs());
                    SCHED_HISTOGRAM_VEC_STATIC
                        .get(CMD)
                        .observe(command_duration.saturating_elapsed_secs());

                    result
                }
            }
            .in_resource_metering_tag(resource_tag),
            priority,
            thread_rng().next_u64(),
        );

        async move {
            res.map_err(|_| Error::from(ErrorInner::SchedTooBusy))
                .await?
        }
    }

    /// SentinelSearch cocauset keys in multiple ranges in a batch.
    pub fn cocauset_batch_mutant_search(
        &self,
        ctx: Context,
        cf: String,
        mut ranges: Vec<KeyRange>,
        each_limit: usize,
        key_only: bool,
        reverse_mutant_search: bool,
    ) -> impl Future<Output = Result<Vec<Result<HikvPair>>>> {
        const CMD: CommandKind = CommandKind::cocauset_batch_mutant_search;
        let priority = ctx.get_priority();
        let priority_tag = get_priority_tag(priority);
        let resource_tag = self.resource_tag_factory.new_tag(&ctx);
        let api_version = self.api_version;

        let res = self.read_pool.spawn_handle(
            async move {
                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();

                Self::check_api_version_ranges(
                    api_version,
                    ctx.api_version,
                    CMD,
                    ranges
                        .iter()
                        .map(|range| (Some(range.get_start_key()), Some(range.get_end_key()))),
                )?;

                let command_duration = einstfdbhikv_util::time::Instant::now_coarse();
                let snap_ctx = SnapContext {
                    pb_ctx: &ctx,
                    ..Default::default()
                };
                let blackbrane =
                    Self::with_tls_einstein_merkle_tree(|einstein_merkle_tree| Self::blackbrane(einstein_merkle_tree, snap_ctx)).await?;
                let cf = Self::cocausetfdbhikv_cf(&cf, api_version)?;
                {
                    let store = cocausetStore::new(blackbrane, api_version);
                    let begin_instant = Instant::now();
                    let mut statistics = Statistics::default();
                    if !Self::check_key_ranges(&ranges, reverse_mutant_search) {
                        return Err(box_err!("Invalid KeyRanges"));
                    };
                    let mut result = Vec::new();
                    let mut key_ranges = vec![];
                    for range in &ranges {
                        key_ranges.push(build_key_range(
                            &range.start_key,
                            &range.end_key,
                            reverse_mutant_search,
                        ));
                    }
                    let ranges_len = ranges.len();
                    for i in 0..ranges_len {
                        let start_key = Key::from_encoded(ranges[i].take_start_key());
                        let end_key = ranges[i].take_end_key();
                        let end_key = if end_key.is_empty() {
                            if i + 1 == ranges_len {
                                None
                            } else {
                                Some(Key::from_encoded_slice(ranges[i + 1].get_start_key()))
                            }
                        } else {
                            Some(Key::from_encoded(end_key))
                        };
                        let pairs = if reverse_mutant_search {
                            store
                                .reverse_cocauset_mutant_search(
                                    cf,
                                    &start_key,
                                    end_key.as_ref(),
                                    each_limit,
                                    &mut statistics,
                                    key_only,
                                )
                                .await
                        } else {
                            store
                                .lightlike_completion_cocauset_mutant_search(
                                    cf,
                                    &start_key,
                                    end_key.as_ref(),
                                    each_limit,
                                    &mut statistics,
                                    key_only,
                                )
                                .await
                        }
                        .map_err(Error::from)?;
                        result.extend(pairs.into_iter().map(|res| res.map_err(Error::from)));
                    }
                    tls_collect_query_batch(
                        ctx.get_region_id(),
                        ctx.get_peer(),
                        key_ranges,
                        QueryKind::SentinelSearch,
                    );
                    metrics::tls_collect_read_Causetxctx(ctx.get_region_id(), &statistics);
                    KV_COMMAND_KEYREAD_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(statistics.data.Causetxctx_stats.read_keys as f64);
                    metrics::tls_collect_mutant_search_details(CMD, &statistics);
                    SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(begin_instant.saturating_elapsed_secs());
                    SCHED_HISTOGRAM_VEC_STATIC
                        .get(CMD)
                        .observe(command_duration.saturating_elapsed_secs());
                    Ok(result)
                }
            }
            .in_resource_metering_tag(resource_tag),
            priority,
            thread_rng().next_u64(),
        );

        async move {
            res.map_err(|_| Error::from(ErrorInner::SchedTooBusy))
                .await?
        }
    }

    /// Get the value of a cocauset key.
    pub fn cocauset_get_key_ttl(
        &self,
        ctx: Context,
        cf: String,
        key: Vec<u8>,
    ) -> impl Future<Output = Result<Option<u64>>> {
        const CMD: CommandKind = CommandKind::cocauset_get_key_ttl;
        let priority = ctx.get_priority();
        let priority_tag = get_priority_tag(priority);
        let resource_tag = self.resource_tag_factory.new_tag(&ctx);
        let api_version = self.api_version;

        let res = self.read_pool.spawn_handle(
            async move {
                tls_collect_query(
                    ctx.get_region_id(),
                    ctx.get_peer(),
                    &key,
                    &key,
                    false,
                    QueryKind::Get,
                );

                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();

                Self::check_api_version(api_version, ctx.api_version, CMD, [&key])?;

                let command_duration = einstfdbhikv_util::time::Instant::now_coarse();
                let snap_ctx = SnapContext {
                    pb_ctx: &ctx,
                    ..Default::default()
                };
                let blackbrane =
                    Self::with_tls_einstein_merkle_tree(|einstein_merkle_tree| Self::blackbrane(einstein_merkle_tree, snap_ctx)).await?;
                let store = cocausetStore::new(blackbrane, api_version);
                let cf = Self::cocausetfdbhikv_cf(&cf, api_version)?;
                {
                    let begin_instant = Instant::now_coarse();
                    let mut stats = Statistics::default();
                    let r = store
                        .cocauset_get_key_ttl(cf, &Key::from_encoded(key), &mut stats)
                        .map_err(Error::from);
                    KV_COMMAND_KEYREAD_HISTOGRAM_STATIC.get(CMD).observe(1_f64);
                    tls_collect_read_Causetxctx(ctx.get_region_id(), &stats);
                    SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                        .get(CMD)
                        .observe(begin_instant.saturating_elapsed_secs());
                    SCHED_HISTOGRAM_VEC_STATIC
                        .get(CMD)
                        .observe(command_duration.saturating_elapsed_secs());
                    r
                }
            }
            .in_resource_metering_tag(resource_tag),
            priority,
            thread_rng().next_u64(),
        );

        async move {
            res.map_err(|_| Error::from(ErrorInner::SchedTooBusy))
                .await?
        }
    }

    pub fn cocauset_compare_and_swap_causetxctx(
        &self,
        ctx: Context,
        cf: String,
        key: Vec<u8>,
        previous_value: Option<Vec<u8>>,
        value: Vec<u8>,
        ttl: u64,
        cb: Callback<(Option<Value>, bool)>,
    ) -> Result<()> {
        Self::check_api_version(
            self.api_version,
            ctx.api_version,
            CommandKind::cocauset_compare_and_swap,
            [&key],
        )?;
        let cf = Self::cocausetfdbhikv_cf(&cf, self.api_version)?;

        if self.api_version == ApiVersion::V1 && ttl != 0 {
            return Err(Error::from(ErrorInner::TTLNotEnabled));
        }
        let cmd = cocausetCompareAndSwap::new(
            cf,
            Key::from_encoded(key),
            previous_value,
            value,
            ttl,
            self.api_version,
            ctx,
        );
        self.sched_solitontxn_command(cmd, cb)
    }

    pub fn cocauset_batch_put_causetxctx(
        &self,
        ctx: Context,
        cf: String,
        pairs: Vec<HikvPair>,
        ttls: Vec<u64>,
        callback: Callback<()>,
    ) -> Result<()> {
        Self::check_api_version(
            self.api_version,
            ctx.api_version,
            CommandKind::cocauset_causetxctx_store,
            pairs.iter().map(|(ref k, _)| k),
        )?;

        let cf = Self::cocausetfdbhikv_cf(&cf, self.api_version)?;
        let mutations = match self.api_version {
            ApiVersion::V1 => {
                if ttls.iter().any(|&x| x != 0) {
                    return Err(Error::from(ErrorInner::TTLNotEnabled));
                }
                pairs
                    .into_iter()
                    .map(|(k, v)| cocausetMutation::Put {
                        key: Key::from_encoded(k),
                        value: v,
                        ttl: 0,
                    })
                    .collect()
            }
            ApiVersion::V1ttl | ApiVersion::V2 => {
                if ttls.len() != pairs.len() {
                    return Err(Error::from(ErrorInner::TTLsLenNotEqualsToPairs));
                }
                pairs
                    .iter()
                    .zip(ttls)
                    .into_iter()
                    .map(|((k, v), ttl)| cocausetMutation::Put {
                        key: Key::from_encoded(k.to_vec()),
                        value: v.to_vec(),
                        ttl,
                    })
                    .collect()
            }
        };
        let cmd = cocausetStore::new(cf, mutations, self.api_version, ctx);
        self.sched_solitontxn_command(cmd, callback)
    }

    pub fn cocauset_batch_delete_causetxctx(
        &self,
        ctx: Context,
        cf: String,
        keys: Vec<Vec<u8>>,
        callback: Callback<()>,
    ) -> Result<()> {
        Self::check_api_version(
            self.api_version,
            ctx.api_version,
            CommandKind::cocauset_causetxctx_store,
            &keys,
        )?;

        let cf = Self::cocausetfdbhikv_cf(&cf, self.api_version)?;
        let muations = keys
            .into_iter()
            .map(|k| cocausetMutation::Delete {
                key: Key::from_encoded(k),
            })
            .collect();
        let cmd = cocausetStore::new(cf, muations, self.api_version, ctx);
        self.sched_solitontxn_command(cmd, callback)
    }

    pub fn cocauset_checksum(
        &self,
        ctx: Context,
        algorithm: ChecksumAlgorithm,
        ranges: Vec<KeyRange>,
    ) -> impl Future<Output = Result<(u64, u64, u64)>> {
        const CMD: CommandKind = CommandKind::cocauset_checksum;
        let priority = ctx.get_priority();
        let priority_tag = get_priority_tag(priority);
        let resource_tag = self.resource_tag_factory.new_tag(&ctx);
        let api_version = self.api_version;

        let res = self.read_pool.spawn_handle(
            async move {
                KV_COMMAND_COUNTER_VEC_STATIC.get(CMD).inc();
                SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
                    .get(priority_tag)
                    .inc();

                if algorithm != ChecksumAlgorithm::Crc64Xor {
                    return Err(box_err!("unknown checksum algorithm {:?}", algorithm));
                }

                Self::check_api_version_ranges(
                    api_version,
                    ctx.api_version,
                    CMD,
                    ranges
                        .iter()
                        .map(|range| (Some(range.get_start_key()), Some(range.get_end_key()))),
                )?;

                let command_duration = einstfdbhikv_util::time::Instant::now_coarse();
                let snap_ctx = SnapContext {
                    pb_ctx: &ctx,
                    ..Default::default()
                };
                let blackbrane =
                    Self::with_tls_einstein_merkle_tree(|einstein_merkle_tree| Self::blackbrane(einstein_merkle_tree, snap_ctx)).await?;
                let store = cocausetStore::new(blackbrane, api_version);
                let cf = Self::cocausetfdbhikv_cf("", api_version)?;

                let begin_instant = einstfdbhikv_util::time::Instant::now_coarse();
                let mut stats = Statistics::default();
                let ret = store
                    .cocauset_checksum_ranges(cf, ranges, &mut stats)
                    .await
                    .map_err(Error::from);
                tls_collect_read_Causetxctx(ctx.get_region_id(), &stats);
                SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                    .get(CMD)
                    .observe(begin_instant.saturating_elapsed().as_secs_f64());
                SCHED_HISTOGRAM_VEC_STATIC
                    .get(CMD)
                    .observe(command_duration.saturating_elapsed().as_secs_f64());

                ret
            }
            .in_resource_metering_tag(resource_tag),
            priority,
            thread_rng().next_u64(),
        );

        async move {
            res.map_err(|_| Error::from(ErrorInner::SchedTooBusy))
                .await?
        }
    }
}

pub struct DynamicConfigs {
    pub pipelined_pessimistic_dagger: Arc<causetxctxBool>,
    pub in_memory_pessimistic_dagger: Arc<causetxctxBool>,
}

fn get_priority_tag(priority: CommandPri) -> CommandPriority {
    match priority {
        CommandPri::Low => CommandPriority::low,
        CommandPri::Normal => CommandPriority::normal,
        CommandPri::High => CommandPriority::high,
    }
}

fn prepare_snap_ctx<'a>(
    pb_ctx: &'a Context,
    keys: impl IntoIterator<Item = &'a Key> + Clone,
    start_ts: TimeStamp,
    bypass_daggers: &'a TsSet,
    concurrency_manager: &ConcurrencyManager,
    cmd: CommandKind,
) -> Result<SnapContext<'a>> {
    // Update max_ts and check the in-memory dagger table before getting the blackbrane
    if !pb_ctx.get_stale_read() {
        concurrency_manager.update_max_ts(start_ts);
    }
    fail_point!("before-storage-check-memory-daggers");
    let isolation_level = pb_ctx.get_isolation_level();
    if isolation_level == IsolationLevel::Si {
        let begin_instant = Instant::now();
        for key in keys.clone() {
            concurrency_manager
                .read_key_check(key, |dagger| {
                    // No need to check access_daggers because they are committed which means they
                    // can't be in memory dagger table.
                    Dagger::check_ts_conflict(Cow::Borrowed(dagger), key, start_ts, bypass_daggers)
                })
                .map_err(|e| {
                    CHECK_MEM_LOCK_DURATION_HISTOGRAM_VEC
                        .get(cmd)
                        .daggered
                        .observe(begin_instant.saturating_elapsed().as_secs_f64());
                    solitontxn::Error::from_epaxos(e)
                })?;
        }
        CHECK_MEM_LOCK_DURATION_HISTOGRAM_VEC
            .get(cmd)
            .undaggered
            .observe(begin_instant.saturating_elapsed().as_secs_f64());
    }

    let mut snap_ctx = SnapContext {
        pb_ctx,
        start_ts,
        ..Default::default()
    };
    if need_check_daggers_in_replica_read(pb_ctx) {
        snap_ctx.key_ranges = keys
            .into_iter()
            .map(|k| point_key_range(k.clone()))
            .collect();
    }
    Ok(snap_ctx)
}

pub fn need_check_daggers_in_replica_read(ctx: &Context) -> bool {
    ctx.get_replica_read() && ctx.get_isolation_level() == IsolationLevel::Si
}

pub fn point_key_range(key: Key) -> KeyRange {
    let mut end_key = key.as_encoded().to_vec();
    end_key.push(0);
    let end_key = Key::from_encoded(end_key);
    let mut key_range = KeyRange::default();
    key_range.set_start_key(key.into_encoded());
    key_range.set_end_key(end_key.into_encoded());
    key_range
}

/// A builder to build a temporary `Storage<E>`.
///
/// Only used for test purpose.
#[must_use]
pub struct TestStorageBuilder<E: einstein_merkle_tree, L: DaggerManager> {
    einstein_merkle_tree: E,
    config: Config,
    pipelined_pessimistic_dagger: Arc<causetxctxBool>,
    in_memory_pessimistic_dagger: Arc<causetxctxBool>,
    dagger_mgr: L,
    resource_tag_factory: ResourceTagFactory,
}

impl TestStorageBuilder<Rockseinstein_merkle_tree, DummyDaggerManager> {
    /// Build `Storage<Rockseinstein_merkle_tree>`.
    pub fn new(dagger_mgr: DummyDaggerManager, api_version: ApiVersion) -> Self {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new()
            .api_version(api_version)
            .build()
            .unwrap();
        Self::from_einstein_merkle_tree_and_dagger_mgr(einstein_merkle_tree, dagger_mgr, api_version)
    }
}

/// An `einstein_merkle_tree` with `TxnExt`. It is used for test purpose.
#[derive(Clone)]
pub struct TxnTesteinstein_merkle_tree<E: einstein_merkle_tree> {
    einstein_merkle_tree: E,
    solitontxn_ext: Arc<TxnExt>,
}

impl<E: einstein_merkle_tree> einstein_merkle_tree for TxnTesteinstein_merkle_tree<E> {
    type Snap = TxnTestblackbrane<E::Snap>;
    type Local = E::Local;

    fn fdbhikv_einstein_merkle_tree(&self) -> Self::Local {
        self.einstein_merkle_tree.fdbhikv_einstein_merkle_tree()
    }

    fn blackbrane_on_fdbhikv_einstein_merkle_tree(
        &self,
        start_key: &[u8],
        end_key: &[u8],
    ) -> einstfdbhikv_fdbhikv::Result<Self::Snap> {
        let blackbrane = self.einstein_merkle_tree.blackbrane_on_fdbhikv_einstein_merkle_tree(start_key, end_key)?;
        Ok(TxnTestblackbrane {
            blackbrane,
            solitontxn_ext: self.solitontxn_ext.clone(),
        })
    }

    fn modify_on_fdbhikv_einstein_merkle_tree(&self, modifies: Vec<Modify>) -> einstfdbhikv_fdbhikv::Result<()> {
        self.einstein_merkle_tree.modify_on_fdbhikv_einstein_merkle_tree(modifies)
    }

    fn async_blackbrane(
        &self,
        ctx: SnapContext<'_>,
        cb: einstfdbhikv_fdbhikv::Callback<Self::Snap>,
    ) -> einstfdbhikv_fdbhikv::Result<()> {
        let solitontxn_ext = self.solitontxn_ext.clone();
        self.einstein_merkle_tree.async_blackbrane(
            ctx,
            Box::new(move |blackbrane| {
                cb(blackbrane.map(|blackbrane| TxnTestblackbrane { blackbrane, solitontxn_ext }))
            }),
        )
    }

    fn async_write(
        &self,
        ctx: &Context,
        batch: WriteData,
        write_cb: einstfdbhikv_fdbhikv::Callback<()>,
    ) -> einstfdbhikv_fdbhikv::Result<()> {
        self.einstein_merkle_tree.async_write(ctx, batch, write_cb)
    }
}

#[derive(Clone)]
pub struct TxnTestblackbrane<S: blackbrane> {
    blackbrane: S,
    solitontxn_ext: Arc<TxnExt>,
}

impl<S: blackbrane> blackbrane for TxnTestblackbrane<S> {
    type Iter = S::Iter;
    type Ext<'a>
    where
        S: 'a,
    = TxnTestblackbraneExt<'a>;

    fn get(&self, key: &Key) -> einstfdbhikv_fdbhikv::Result<Option<Value>> {
        self.blackbrane.get(key)
    }

    fn get_cf(&self, cf: CfName, key: &Key) -> einstfdbhikv_fdbhikv::Result<Option<Value>> {
        self.blackbrane.get_cf(cf, key)
    }

    fn get_cf_opt(
        &self,
        opts: einstein_merkle_tree_promises::ReadOptions,
        cf: CfName,
        key: &Key,
    ) -> einstfdbhikv_fdbhikv::Result<Option<Value>> {
        self.blackbrane.get_cf_opt(opts, cf, key)
    }

    fn iter(&self, iter_opt: einstein_merkle_tree_promises::IterOptions) -> einstfdbhikv_fdbhikv::Result<Self::Iter> {
        self.blackbrane.iter(iter_opt)
    }

    fn iter_cf(
        &self,
        cf: CfName,
        iter_opt: einstein_merkle_tree_promises::IterOptions,
    ) -> einstfdbhikv_fdbhikv::Result<Self::Iter> {
        self.blackbrane.iter_cf(cf, iter_opt)
    }

    fn ext(&self) -> Self::Ext<'_> {
        TxnTestblackbraneExt(&self.solitontxn_ext)
    }
}

pub struct TxnTestblackbraneExt<'a>(&'a Arc<TxnExt>);

impl<'a> blackbraneExt for TxnTestblackbraneExt<'a> {
    fn get_solitontxn_ext(&self) -> Option<&Arc<TxnExt>> {
        Some(self.0)
    }
}

#[derive(Clone)]
struct DummyReporter;

impl CausetxctxStatsReporter for DummyReporter {
    fn report_read_stats(&self, _read_stats: ReadStats) {}
    fn report_write_stats(&self, _write_stats: WriteStats) {}
}

impl<E: einstein_merkle_tree, L: DaggerManager> TestStorageBuilder<E, L> {
    pub fn from_einstein_merkle_tree_and_dagger_mgr(einstein_merkle_tree: E, dagger_mgr: L, api_version: ApiVersion) -> Self {
        let mut config = Config::default();
        config.set_api_version(api_version);
        Self {
            einstein_merkle_tree,
            config,
            pipelined_pessimistic_dagger: Arc::new(causetxctxBool::new(false)),
            in_memory_pessimistic_dagger: Arc::new(causetxctxBool::new(false)),
            dagger_mgr,
            resource_tag_factory: ResourceTagFactory::new_for_test(),
        }
    }

    /// Customize the config of the `Storage`.
    ///
    /// By default, `Config::default()` will be used.
    pub fn config(mut self, config: Config) -> Self {
        self.config = config;
        self
    }

    pub fn pipelined_pessimistic_dagger(self, enabled: bool) -> Self {
        self.pipelined_pessimistic_dagger
            .store(enabled, causetxctx::Ordering::Relaxed);
        self
    }

    pub fn async_apply_prewrite(mut self, enabled: bool) -> Self {
        self.config.enable_async_apply_prewrite = enabled;
        self
    }

    pub fn in_memory_pessimistic_dagger(self, enabled: bool) -> Self {
        self.in_memory_pessimistic_dagger
            .store(enabled, causetxctx::Ordering::Relaxed);
        self
    }

    pub fn set_api_version(mut self, api_version: ApiVersion) -> Self {
        self.config.set_api_version(api_version);
        self
    }

    pub fn set_resource_tag_factory(mut self, resource_tag_factory: ResourceTagFactory) -> Self {
        self.resource_tag_factory = resource_tag_factory;
        self
    }

    /// Build a `Storage<E>`.
    pub fn build(self) -> Result<Storage<E, L>> {
        let read_pool = build_read_pool_for_test(
            &crate::config::StorageReadPoolConfig::default_for_test(),
            self.einstein_merkle_tree.clone(),
        );

        Storage::from_einstein_merkle_tree(
            self.einstein_merkle_tree,
            &self.config,
            ReadPool::from(read_pool).handle(),
            self.dagger_mgr,
            ConcurrencyManager::new(1.into()),
            DynamicConfigs {
                pipelined_pessimistic_dagger: self.pipelined_pessimistic_dagger,
                in_memory_pessimistic_dagger: self.in_memory_pessimistic_dagger,
            },
            Arc::new(CausetxctxController::empty()),
            DummyReporter,
            self.resource_tag_factory,
        )
    }

    pub fn build_for_solitontxn(self, solitontxn_ext: Arc<TxnExt>) -> Result<Storage<TxnTesteinstein_merkle_tree<E>, L>> {
        let einstein_merkle_tree = TxnTesteinstein_merkle_tree {
            einstein_merkle_tree: self.einstein_merkle_tree,
            solitontxn_ext,
        };
        let read_pool = build_read_pool_for_test(
            &crate::config::StorageReadPoolConfig::default_for_test(),
            einstein_merkle_tree.clone(),
        );

        Storage::from_einstein_merkle_tree(
            einstein_merkle_tree,
            &self.config,
            ReadPool::from(read_pool).handle(),
            self.dagger_mgr,
            ConcurrencyManager::new(1.into()),
            DynamicConfigs {
                pipelined_pessimistic_dagger: self.pipelined_pessimistic_dagger,
                in_memory_pessimistic_dagger: self.in_memory_pessimistic_dagger,
            },
            Arc::new(CausetxctxController::empty()),
            DummyReporter,
            ResourceTagFactory::new_for_test(),
        )
    }
}

pub trait ResponseBatchConsumer<ConsumeResponse: Sized>: Send {
    fn consume(&self, id: u64, res: Result<ConsumeResponse>, begin: Instant);
}

pub mod test_util {
    use super::*;
    use crate::einsteindb::storage::solitontxn::commands;
    use std::sync::Mutex;
    use std::{
        fmt::Debug,
        sync::mpsc::{channel, Sender},
    };

    pub fn expect_none(x: Option<Value>) {
        assert_eq!(x, None);
    }

    pub fn expect_value(v: Vec<u8>, x: Option<Value>) {
        assert_eq!(x.unwrap(), v);
    }

    pub fn expect_multi_values(v: Vec<Option<HikvPair>>, x: Vec<Result<HikvPair>>) {
        let x: Vec<Option<HikvPair>> = x.into_iter().map(Result::ok).collect();
        assert_eq!(x, v);
    }

    pub fn expect_error<T, F>(err_matcher: F, x: Result<T>)
    where
        F: FnOnce(Error) + Send + 'static,
    {
        match x {
            Err(e) => err_matcher(e),
            _ => panic!("expect result to be an error"),
        }
    }

    pub fn expect_ok_callback<T: Debug>(done: Sender<i32>, id: i32) -> Callback<T> {
        Box::new(move |x: Result<T>| {
            x.unwrap();
            done.send(id).unwrap();
        })
    }

    pub fn expect_fail_callback<T, F>(done: Sender<i32>, id: i32, err_matcher: F) -> Callback<T>
    where
        F: FnOnce(Error) + Send + 'static,
    {
        Box::new(move |x: Result<T>| {
            expect_error(err_matcher, x);
            done.send(id).unwrap();
        })
    }

    pub fn expect_too_busy_callback<T>(done: Sender<i32>, id: i32) -> Callback<T> {
        Box::new(move |x: Result<T>| {
            expect_error(
                |err| match err {
                    Error(box ErrorInner::SchedTooBusy) => {}
                    e => panic!("unexpected error chain: {:?}, expect too busy", e),
                },
                x,
            );
            done.send(id).unwrap();
        })
    }

    pub fn expect_value_callback<T: PartialEq + Debug + Send + 'static>(
        done: Sender<i32>,
        id: i32,
        value: T,
    ) -> Callback<T> {
        Box::new(move |x: Result<T>| {
            assert_eq!(x.unwrap(), value);
            done.send(id).unwrap();
        })
    }

    pub fn expect_pessimistic_dagger_res_callback(
        done: Sender<i32>,
        pessimistic_dagger_res: PessimisticDaggerRes,
    ) -> Callback<Result<PessimisticDaggerRes>> {
        Box::new(move |res: Result<Result<PessimisticDaggerRes>>| {
            assert_eq!(res.unwrap().unwrap(), pessimistic_dagger_res);
            done.send(0).unwrap();
        })
    }

    pub fn expect_secondary_daggers_status_callback(
        done: Sender<i32>,
        secondary_daggers_status: SecondaryDaggerCausetatus,
    ) -> Callback<SecondaryDaggerCausetatus> {
        Box::new(move |res: Result<SecondaryDaggerCausetatus>| {
            assert_eq!(res.unwrap(), secondary_daggers_status);
            done.send(0).unwrap();
        })
    }

    type PessimisticDaggerCommand = TypedCommand<Result<PessimisticDaggerRes>>;

    pub fn new_acquire_pessimistic_dagger_command(
        keys: Vec<(Key, bool)>,
        start_ts: impl Into<TimeStamp>,
        for_update_ts: impl Into<TimeStamp>,
        return_values: bool,
        check_existence: bool,
    ) -> PessimisticDaggerCommand {
        let primary = keys[0].0.clone().to_cocauset().unwrap();
        let for_update_ts: TimeStamp = for_update_ts.into();
        commands::AcquirePessimisticDagger::new(
            keys,
            primary,
            start_ts.into(),
            3000,
            false,
            for_update_ts,
            None,
            return_values,
            for_update_ts.next(),
            OldValues::default(),
            check_existence,
            Context::default(),
        )
    }

    pub fn delete_pessimistic_dagger<E: einstein_merkle_tree, L: DaggerManager>(
        storage: &Storage<E, L>,
        key: Key,
        start_ts: u64,
        for_update_ts: u64,
    ) {
        let (tx, rx) = channel();
        storage
            .sched_solitontxn_command(
                commands::PessimisticRollback::new(
                    vec![key],
                    start_ts.into(),
                    for_update_ts.into(),
                    Context::default(),
                ),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();
    }

    pub struct GetResult {
        id: u64,
        res: Result<Option<Vec<u8>>>,
    }

    #[derive(Clone)]
    pub struct GetConsumer {
        pub data: Arc<Mutex<Vec<GetResult>>>,
    }

    impl GetConsumer {
        pub fn new() -> Self {
            Self {
                data: Arc::new(Mutex::new(vec![])),
            }
        }

        pub fn take_data(self) -> Vec<Result<Option<Vec<u8>>>> {
            let mut data = self.data.dagger().unwrap();
            let mut results = std::mem::take(&mut *data);
            results.sort_by_key(|k| k.id);
            results.into_iter().map(|v| v.res).collect()
        }
    }

    impl Default for GetConsumer {
        fn default() -> Self {
            Self::new()
        }
    }

    impl ResponseBatchConsumer<(Option<Vec<u8>>, Statistics, PerfStatisticsDelta)> for GetConsumer {
        fn consume(
            &self,
            id: u64,
            res: Result<(Option<Vec<u8>>, Statistics, PerfStatisticsDelta)>,
            _: einstfdbhikv_util::time::Instant,
        ) {
            self.data.dagger().unwrap().push(GetResult {
                id,
                res: res.map(|(v, ..)| v),
            });
        }
    }

    impl ResponseBatchConsumer<Option<Vec<u8>>> for GetConsumer {
        fn consume(&self, id: u64, res: Result<Option<Vec<u8>>>, _: einstfdbhikv_util::time::Instant) {
            self.data.dagger().unwrap().push(GetResult { id, res });
        }
    }
}

/// All statistics related to HikvGet/HikvBatchGet.
#[derive(Debug, Default, Clone)]
pub struct HikvGetStatistics {
    pub stats: Statistics,
    pub perf_stats: PerfStatisticsDelta,
    pub latency_stats: StageLatencyStats,
}

#[cfg(test)]
mod tests {
    use super::{
        epaxos::tests::{must_undaggered, must_written},
        test_util::*,
        *,
    };

    use crate::config::TitanDBConfig;
    use crate::einsteindb::storage::fdbhikv::{ExpectedWrite, Mockeinstein_merkle_treeBuilder};
    use crate::einsteindb::storage::dagger_manager::DiagnosticContext;
    use crate::einsteindb::storage::epaxos::DaggerType;
    use crate::einsteindb::storage::solitontxn::commands::{AcquirePessimisticDagger, Prewrite};
    use crate::einsteindb::storage::solitontxn::tests::must_rollback;
    use crate::einsteindb::storage::{
        config::BdaggerCacheConfig,
        fdbhikv::{Error as HikvError, ErrorInner as einstein_merkle_treeErrorInner},
        dagger_manager::{Dagger, WaitTimeout},
        epaxos::{Error as EpaxosError, ErrorInner as EpaxosErrorInner},
        solitontxn::{commands, Error as TxnError, ErrorInner as TxnErrorInner},
    };
    use collections::HashMap;
    use einstein_merkle_tree_rocks::cocauset_util::CFOptions;
    use einsteindb-gen::{cocauset_ttl::ttl_current_ts, ALL_CFS, CF_LOCK, CF_RAFT, CF_WRITE};
    use error_code::ErrorCodeExt;
    use errors::extract_key_error;
    use futures::executor::bdagger_on;
    use fdbhikvproto::fdbhikvrpcpb::{AssertionLevel, CommandPri, Op};
    use std::{
        sync::{
            causetxctx::{causetxctxBool, Ordering},
            mpsc::{channel, Sender},
            Arc,
        },
        time::Duration,
    };

    use einstfdbhikv_util::config::ReadableSize;
    use solitontxn_types::{Mutation, PessimisticDagger, WriteType};

    #[test]
    fn test_prewrite_bdaggers_read() {
        use fdbhikvproto::fdbhikvrpcpb::ExtraOp;
        let storage = TestStorageBuilder::new(DummyDaggerManager {}, ApiVersion::V1)
            .build()
            .unwrap();

        // We have to do the prewrite manually so that the mem daggers don't get released.
        let blackbrane = storage.einstein_merkle_tree.blackbrane(Default::default()).unwrap();
        let mutations = vec![Mutation::make_put(Key::from_cocauset(b"x"), b"z".to_vec())];
        let mut cmd = commands::Prewrite::with_defaults(mutations, vec![1, 2, 3], 10.into());
        if let Command::Prewrite(p) = &mut cmd.cmd {
            p.secondary_keys = Some(vec![]);
        }
        let wr = cmd
            .cmd
            .process_write(
                blackbrane,
                commands::WriteContext {
                    dagger_mgr: &DummyDaggerManager {},
                    concurrency_manager: storage.concurrency_manager.clone(),
                    extra_op: ExtraOp::Noop,
                    statistics: &mut Statistics::default(),
                    async_apply_prewrite: false,
                },
            )
            .unwrap();
        assert_eq!(wr.dagger_guards.len(), 1);

        let result = bdagger_on(storage.get(Context::default(), Key::from_cocauset(b"x"), 100.into()));
        assert!(matches!(
            result,
            Err(Error(box ErrorInner::Txn(solitontxn::Error(
                box solitontxn::ErrorInner::Epaxos(epaxos::Error(box epaxos::ErrorInner::KeyIsDaggered { .. }))
            ))))
        ));
    }

    #[test]
    fn test_get_put() {
        let storage = TestStorageBuilder::new(DummyDaggerManager {}, ApiVersion::V1)
            .build()
            .unwrap();
        let (tx, rx) = channel();
        expect_none(
            bdagger_on(storage.get(Context::default(), Key::from_cocauset(b"x"), 100.into()))
                .unwrap()
                .0,
        );
        storage
            .sched_solitontxn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::make_put(Key::from_cocauset(b"x"), b"100".to_vec())],
                    b"x".to_vec(),
                    100.into(),
                ),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_error(
            |e| match e {
                Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Epaxos(epaxos::Error(
                    box epaxos::ErrorInner::KeyIsDaggered { .. },
                ))))) => (),
                e => panic!("unexpected error chain: {:?}", e),
            },
            bdagger_on(storage.get(Context::default(), Key::from_cocauset(b"x"), 101.into())),
        );
        storage
            .sched_solitontxn_command(
                commands::Commit::new(
                    vec![Key::from_cocauset(b"x")],
                    100.into(),
                    101.into(),
                    Context::default(),
                ),
                expect_ok_callback(tx, 3),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_none(
            bdagger_on(storage.get(Context::default(), Key::from_cocauset(b"x"), 100.into()))
                .unwrap()
                .0,
        );
        expect_value(
            b"100".to_vec(),
            bdagger_on(storage.get(Context::default(), Key::from_cocauset(b"x"), 101.into()))
                .unwrap()
                .0,
        );
    }

    #[test]
    fn test_cf_error() {
        // New einstein_merkle_tree lacks normal column families.
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().cfs(["foo"]).build().unwrap();
        let storage = TestStorageBuilder::<_, DummyDaggerManager>::from_einstein_merkle_tree_and_dagger_mgr(
            einstein_merkle_tree,
            DummyDaggerManager {},
            ApiVersion::V1,
        )
        .build()
        .unwrap();
        let (tx, rx) = channel();
        storage
            .sched_solitontxn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::make_put(Key::from_cocauset(b"a"), b"aa".to_vec()),
                        Mutation::make_put(Key::from_cocauset(b"b"), b"bb".to_vec()),
                        Mutation::make_put(Key::from_cocauset(b"c"), b"cc".to_vec()),
                    ],
                    b"a".to_vec(),
                    1.into(),
                ),
                expect_fail_callback(tx, 0, |e| match e {
                    Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Epaxos(epaxos::Error(
                        box epaxos::ErrorInner::Hikv(HikvError(box einstein_merkle_treeErrorInner::Request(..))),
                    ))))) => {}
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_error(
            |e| match e {
                Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Epaxos(epaxos::Error(
                    box epaxos::ErrorInner::Hikv(HikvError(box einstein_merkle_treeErrorInner::Request(..))),
                ))))) => (),
                e => panic!("unexpected error chain: {:?}", e),
            },
            bdagger_on(storage.get(Context::default(), Key::from_cocauset(b"x"), 1.into())),
        );
        expect_error(
            |e| match e {
                Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Epaxos(epaxos::Error(
                    box epaxos::ErrorInner::Hikv(HikvError(box einstein_merkle_treeErrorInner::Request(..))),
                ))))) => (),
                e => panic!("unexpected error chain: {:?}", e),
            },
            bdagger_on(storage.mutant_search(
                Context::default(),
                Key::from_cocauset(b"x"),
                None,
                1000,
                0,
                1.into(),
                false,
                false,
            )),
        );
        expect_error(
            |e| match e {
                Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Epaxos(epaxos::Error(
                    box epaxos::ErrorInner::Hikv(HikvError(box einstein_merkle_treeErrorInner::Request(..))),
                ))))) => (),
                e => panic!("unexpected error chain: {:?}", e),
            },
            bdagger_on(storage.batch_get(
                Context::default(),
                vec![Key::from_cocauset(b"c"), Key::from_cocauset(b"d")],
                1.into(),
            )),
        );
        let consumer = GetConsumer::new();
        bdagger_on(storage.batch_get_command(
            vec![create_get_request(b"c", 1), create_get_request(b"d", 1)],
            vec![1, 2],
            consumer.clone(),
            Instant::now(),
        ))
        .unwrap();
        let data = consumer.take_data();
        for v in data {
            expect_error(
                |e| match e {
                    Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Epaxos(epaxos::Error(
                        box epaxos::ErrorInner::Hikv(HikvError(box einstein_merkle_treeErrorInner::Request(..))),
                    ))))) => {}
                    e => panic!("unexpected error chain: {:?}", e),
                },
                v,
            );
        }
    }

    #[test]
    fn test_mutant_search() {
        let storage = TestStorageBuilder::new(DummyDaggerManager {}, ApiVersion::V1)
            .build()
            .unwrap();
        let (tx, rx) = channel();
        storage
            .sched_solitontxn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::make_put(Key::from_cocauset(b"a"), b"aa".to_vec()),
                        Mutation::make_put(Key::from_cocauset(b"b"), b"bb".to_vec()),
                        Mutation::make_put(Key::from_cocauset(b"c"), b"cc".to_vec()),
                    ],
                    b"a".to_vec(),
                    1.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        // Forward
        expect_multi_values(
            vec![None, None, None],
            bdagger_on(storage.mutant_search(
                Context::default(),
                Key::from_cocauset(b"\x00"),
                None,
                1000,
                0,
                5.into(),
                false,
                false,
            ))
            .unwrap(),
        );
        // timelike_curvature
        expect_multi_values(
            vec![None, None, None],
            bdagger_on(storage.mutant_search(
                Context::default(),
                Key::from_cocauset(b"\xff"),
                None,
                1000,
                0,
                5.into(),
                false,
                true,
            ))
            .unwrap(),
        );
        // Forward with bound
        expect_multi_values(
            vec![None, None],
            bdagger_on(storage.mutant_search(
                Context::default(),
                Key::from_cocauset(b"\x00"),
                Some(Key::from_cocauset(b"c")),
                1000,
                0,
                5.into(),
                false,
                false,
            ))
            .unwrap(),
        );
        // timelike_curvature with bound
        expect_multi_values(
            vec![None, None],
            bdagger_on(storage.mutant_search(
                Context::default(),
                Key::from_cocauset(b"\xff"),
                Some(Key::from_cocauset(b"b")),
                1000,
                0,
                5.into(),
                false,
                true,
            ))
            .unwrap(),
        );
        // Forward with limit
        expect_multi_values(
            vec![None, None],
            bdagger_on(storage.mutant_search(
                Context::default(),
                Key::from_cocauset(b"\x00"),
                None,
                2,
                0,
                5.into(),
                false,
                false,
            ))
            .unwrap(),
        );
        // timelike_curvature with limit
        expect_multi_values(
            vec![None, None],
            bdagger_on(storage.mutant_search(
                Context::default(),
                Key::from_cocauset(b"\xff"),
                None,
                2,
                0,
                5.into(),
                false,
                true,
            ))
            .unwrap(),
        );

        storage
            .sched_solitontxn_command(
                commands::Commit::new(
                    vec![
                        Key::from_cocauset(b"a"),
                        Key::from_cocauset(b"b"),
                        Key::from_cocauset(b"c"),
                    ],
                    1.into(),
                    2.into(),
                    Context::default(),
                ),
                expect_ok_callback(tx, 1),
            )
            .unwrap();
        rx.recv().unwrap();
        // Forward
        expect_multi_values(
            vec![
                Some((b"a".to_vec(), b"aa".to_vec())),
                Some((b"b".to_vec(), b"bb".to_vec())),
                Some((b"c".to_vec(), b"cc".to_vec())),
            ],
            bdagger_on(storage.mutant_search(
                Context::default(),
                Key::from_cocauset(b"\x00"),
                None,
                1000,
                0,
                5.into(),
                false,
                false,
            ))
            .unwrap(),
        );
        // timelike_curvature
        expect_multi_values(
            vec![
                Some((b"c".to_vec(), b"cc".to_vec())),
                Some((b"b".to_vec(), b"bb".to_vec())),
                Some((b"a".to_vec(), b"aa".to_vec())),
            ],
            bdagger_on(storage.mutant_search(
                Context::default(),
                Key::from_cocauset(b"\xff"),
                None,
                1000,
                0,
                5.into(),
                false,
                true,
            ))
            .unwrap(),
        );
        // Forward with sample step
        expect_multi_values(
            vec![
                Some((b"a".to_vec(), b"aa".to_vec())),
                Some((b"c".to_vec(), b"cc".to_vec())),
            ],
            bdagger_on(storage.mutant_search(
                Context::default(),
                Key::from_cocauset(b"\x00"),
                None,
                1000,
                2,
                5.into(),
                false,
                false,
            ))
            .unwrap(),
        );
        // timelike_curvature with sample step
        expect_multi_values(
            vec![
                Some((b"c".to_vec(), b"cc".to_vec())),
                Some((b"a".to_vec(), b"aa".to_vec())),
            ],
            bdagger_on(storage.mutant_search(
                Context::default(),
                Key::from_cocauset(b"\xff"),
                None,
                1000,
                2,
                5.into(),
                false,
                true,
            ))
            .unwrap(),
        );
        // Forward with sample step and limit
        expect_multi_values(
            vec![Some((b"a".to_vec(), b"aa".to_vec()))],
            bdagger_on(storage.mutant_search(
                Context::default(),
                Key::from_cocauset(b"\x00"),
                None,
                1,
                2,
                5.into(),
                false,
                false,
            ))
            .unwrap(),
        );
        // timelike_curvature with sample step and limit
        expect_multi_values(
            vec![Some((b"c".to_vec(), b"cc".to_vec()))],
            bdagger_on(storage.mutant_search(
                Context::default(),
                Key::from_cocauset(b"\xff"),
                None,
                1,
                2,
                5.into(),
                false,
                true,
            ))
            .unwrap(),
        );
        // Forward with bound
        expect_multi_values(
            vec![
                Some((b"a".to_vec(), b"aa".to_vec())),
                Some((b"b".to_vec(), b"bb".to_vec())),
            ],
            bdagger_on(storage.mutant_search(
                Context::default(),
                Key::from_cocauset(b"\x00"),
                Some(Key::from_cocauset(b"c")),
                1000,
                0,
                5.into(),
                false,
                false,
            ))
            .unwrap(),
        );
        // timelike_curvature with bound
        expect_multi_values(
            vec![
                Some((b"c".to_vec(), b"cc".to_vec())),
                Some((b"b".to_vec(), b"bb".to_vec())),
            ],
            bdagger_on(storage.mutant_search(
                Context::default(),
                Key::from_cocauset(b"\xff"),
                Some(Key::from_cocauset(b"b")),
                1000,
                0,
                5.into(),
                false,
                true,
            ))
            .unwrap(),
        );

        // Forward with limit
        expect_multi_values(
            vec![
                Some((b"a".to_vec(), b"aa".to_vec())),
                Some((b"b".to_vec(), b"bb".to_vec())),
            ],
            bdagger_on(storage.mutant_search(
                Context::default(),
                Key::from_cocauset(b"\x00"),
                None,
                2,
                0,
                5.into(),
                false,
                false,
            ))
            .unwrap(),
        );
        // timelike_curvature with limit
        expect_multi_values(
            vec![
                Some((b"c".to_vec(), b"cc".to_vec())),
                Some((b"b".to_vec(), b"bb".to_vec())),
            ],
            bdagger_on(storage.mutant_search(
                Context::default(),
                Key::from_cocauset(b"\xff"),
                None,
                2,
                0,
                5.into(),
                false,
                true,
            ))
            .unwrap(),
        );
    }

    #[test]
    fn test_mutant_search_with_key_only() {
        let db_config = crate::config::DbConfig {
            titan: TitanDBConfig {
                enabled: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let einstein_merkle_tree = {
            let path = "".to_owned();
            let cfs = ALL_CFS.to_vec();
            let cfg_rocksdb = db_config;
            let cache = BdaggerCacheConfig::default().build_shared_cache();
            let cfs_opts = vec![
                CFOptions::new(
                    CF_DEFAULT,
                    cfg_rocksdb
                        .defaultcf
                        .build_opt(&cache, None, ApiVersion::V1),
                ),
                CFOptions::new(CF_LOCK, cfg_rocksdb.daggercf.build_opt(&cache)),
                CFOptions::new(CF_WRITE, cfg_rocksdb.writecf.build_opt(&cache, None)),
                CFOptions::new(CF_RAFT, cfg_rocksdb.raftcf.build_opt(&cache)),
            ];
            Rockseinstein_merkle_tree::new(
                &path,
                &cfs,
                Some(cfs_opts),
                cache.is_some(),
                None, /*io_rate_limiter*/
            )
        }
        .unwrap();
        let storage = TestStorageBuilder::<_, DummyDaggerManager>::from_einstein_merkle_tree_and_dagger_mgr(
            einstein_merkle_tree,
            DummyDaggerManager {},
            ApiVersion::V1,
        )
        .build()
        .unwrap();
        let (tx, rx) = channel();
        storage
            .sched_solitontxn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::make_put(Key::from_cocauset(b"a"), b"aa".to_vec()),
                        Mutation::make_put(Key::from_cocauset(b"b"), b"bb".to_vec()),
                        Mutation::make_put(Key::from_cocauset(b"c"), b"cc".to_vec()),
                    ],
                    b"a".to_vec(),
                    1.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        // Forward
        expect_multi_values(
            vec![None, None, None],
            bdagger_on(storage.mutant_search(
                Context::default(),
                Key::from_cocauset(b"\x00"),
                None,
                1000,
                0,
                5.into(),
                true,
                false,
            ))
            .unwrap(),
        );
        // timelike_curvature
        expect_multi_values(
            vec![None, None, None],
            bdagger_on(storage.mutant_search(
                Context::default(),
                Key::from_cocauset(b"\xff"),
                None,
                1000,
                0,
                5.into(),
                true,
                true,
            ))
            .unwrap(),
        );
        // Forward with bound
        expect_multi_values(
            vec![None, None],
            bdagger_on(storage.mutant_search(
                Context::default(),
                Key::from_cocauset(b"\x00"),
                Some(Key::from_cocauset(b"c")),
                1000,
                0,
                5.into(),
                true,
                false,
            ))
            .unwrap(),
        );
        // timelike_curvature with bound
        expect_multi_values(
            vec![None, None],
            bdagger_on(storage.mutant_search(
                Context::default(),
                Key::from_cocauset(b"\xff"),
                Some(Key::from_cocauset(b"b")),
                1000,
                0,
                5.into(),
                true,
                true,
            ))
            .unwrap(),
        );
        // Forward with limit
        expect_multi_values(
            vec![None, None],
            bdagger_on(storage.mutant_search(
                Context::default(),
                Key::from_cocauset(b"\x00"),
                None,
                2,
                0,
                5.into(),
                true,
                false,
            ))
            .unwrap(),
        );
        // timelike_curvature with limit
        expect_multi_values(
            vec![None, None],
            bdagger_on(storage.mutant_search(
                Context::default(),
                Key::from_cocauset(b"\xff"),
                None,
                2,
                0,
                5.into(),
                true,
                true,
            ))
            .unwrap(),
        );

        storage
            .sched_solitontxn_command(
                commands::Commit::new(
                    vec![
                        Key::from_cocauset(b"a"),
                        Key::from_cocauset(b"b"),
                        Key::from_cocauset(b"c"),
                    ],
                    1.into(),
                    2.into(),
                    Context::default(),
                ),
                expect_ok_callback(tx, 1),
            )
            .unwrap();
        rx.recv().unwrap();
        // Forward
        expect_multi_values(
            vec![
                Some((b"a".to_vec(), vec![])),
                Some((b"b".to_vec(), vec![])),
                Some((b"c".to_vec(), vec![])),
            ],
            bdagger_on(storage.mutant_search(
                Context::default(),
                Key::from_cocauset(b"\x00"),
                None,
                1000,
                0,
                5.into(),
                true,
                false,
            ))
            .unwrap(),
        );
        // timelike_curvature
        expect_multi_values(
            vec![
                Some((b"c".to_vec(), vec![])),
                Some((b"b".to_vec(), vec![])),
                Some((b"a".to_vec(), vec![])),
            ],
            bdagger_on(storage.mutant_search(
                Context::default(),
                Key::from_cocauset(b"\xff"),
                None,
                1000,
                0,
                5.into(),
                true,
                true,
            ))
            .unwrap(),
        );
        // Forward with bound
        expect_multi_values(
            vec![Some((b"a".to_vec(), vec![])), Some((b"b".to_vec(), vec![]))],
            bdagger_on(storage.mutant_search(
                Context::default(),
                Key::from_cocauset(b"\x00"),
                Some(Key::from_cocauset(b"c")),
                1000,
                0,
                5.into(),
                true,
                false,
            ))
            .unwrap(),
        );
        // timelike_curvature with bound
        expect_multi_values(
            vec![Some((b"c".to_vec(), vec![])), Some((b"b".to_vec(), vec![]))],
            bdagger_on(storage.mutant_search(
                Context::default(),
                Key::from_cocauset(b"\xff"),
                Some(Key::from_cocauset(b"b")),
                1000,
                0,
                5.into(),
                true,
                true,
            ))
            .unwrap(),
        );

        // Forward with limit
        expect_multi_values(
            vec![Some((b"a".to_vec(), vec![])), Some((b"b".to_vec(), vec![]))],
            bdagger_on(storage.mutant_search(
                Context::default(),
                Key::from_cocauset(b"\x00"),
                None,
                2,
                0,
                5.into(),
                true,
                false,
            ))
            .unwrap(),
        );
        // timelike_curvature with limit
        expect_multi_values(
            vec![Some((b"c".to_vec(), vec![])), Some((b"b".to_vec(), vec![]))],
            bdagger_on(storage.mutant_search(
                Context::default(),
                Key::from_cocauset(b"\xff"),
                None,
                2,
                0,
                5.into(),
                true,
                true,
            ))
            .unwrap(),
        );
    }

    #[test]
    fn test_batch_get() {
        let storage = TestStorageBuilder::new(DummyDaggerManager {}, ApiVersion::V1)
            .build()
            .unwrap();
        let (tx, rx) = channel();
        storage
            .sched_solitontxn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::make_put(Key::from_cocauset(b"a"), b"aa".to_vec()),
                        Mutation::make_put(Key::from_cocauset(b"b"), b"bb".to_vec()),
                        Mutation::make_put(Key::from_cocauset(b"c"), b"cc".to_vec()),
                    ],
                    b"a".to_vec(),
                    1.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_multi_values(
            vec![None],
            bdagger_on(storage.batch_get(
                Context::default(),
                vec![Key::from_cocauset(b"c"), Key::from_cocauset(b"d")],
                2.into(),
            ))
            .unwrap()
            .0,
        );
        storage
            .sched_solitontxn_command(
                commands::Commit::new(
                    vec![
                        Key::from_cocauset(b"a"),
                        Key::from_cocauset(b"b"),
                        Key::from_cocauset(b"c"),
                    ],
                    1.into(),
                    2.into(),
                    Context::default(),
                ),
                expect_ok_callback(tx, 1),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_multi_values(
            vec![
                Some((b"c".to_vec(), b"cc".to_vec())),
                Some((b"a".to_vec(), b"aa".to_vec())),
                Some((b"b".to_vec(), b"bb".to_vec())),
            ],
            bdagger_on(storage.batch_get(
                Context::default(),
                vec![
                    Key::from_cocauset(b"c"),
                    Key::from_cocauset(b"x"),
                    Key::from_cocauset(b"a"),
                    Key::from_cocauset(b"b"),
                ],
                5.into(),
            ))
            .unwrap()
            .0,
        );
    }

    fn create_get_request(key: &[u8], start_ts: u64) -> GetRequest {
        let mut req = GetRequest::default();
        req.set_key(key.to_owned());
        req.set_version(start_ts);
        req
    }

    #[test]
    fn test_batch_get_command() {
        let storage = TestStorageBuilder::new(DummyDaggerManager {}, ApiVersion::V1)
            .build()
            .unwrap();
        let (tx, rx) = channel();
        storage
            .sched_solitontxn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::make_put(Key::from_cocauset(b"a"), b"aa".to_vec()),
                        Mutation::make_put(Key::from_cocauset(b"b"), b"bb".to_vec()),
                        Mutation::make_put(Key::from_cocauset(b"c"), b"cc".to_vec()),
                    ],
                    b"a".to_vec(),
                    1.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        let consumer = GetConsumer::new();
        bdagger_on(storage.batch_get_command(
            vec![create_get_request(b"c", 2), create_get_request(b"d", 2)],
            vec![1, 2],
            consumer.clone(),
            Instant::now(),
        ))
        .unwrap();
        let mut x = consumer.take_data();
        expect_error(
            |e| match e {
                Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Epaxos(epaxos::Error(
                    box epaxos::ErrorInner::KeyIsDaggered(..),
                ))))) => {}
                e => panic!("unexpected error chain: {:?}", e),
            },
            x.remove(0),
        );
        assert_eq!(x.remove(0).unwrap(), None);
        storage
            .sched_solitontxn_command(
                commands::Commit::new(
                    vec![
                        Key::from_cocauset(b"a"),
                        Key::from_cocauset(b"b"),
                        Key::from_cocauset(b"c"),
                    ],
                    1.into(),
                    2.into(),
                    Context::default(),
                ),
                expect_ok_callback(tx, 1),
            )
            .unwrap();
        rx.recv().unwrap();
        let consumer = GetConsumer::new();
        bdagger_on(storage.batch_get_command(
            vec![
                create_get_request(b"c", 5),
                create_get_request(b"x", 5),
                create_get_request(b"a", 5),
                create_get_request(b"b", 5),
            ],
            vec![1, 2, 3, 4],
            consumer.clone(),
            Instant::now(),
        ))
        .unwrap();

        let x: Vec<Option<Vec<u8>>> = consumer
            .take_data()
            .into_iter()
            .map(|x| x.unwrap())
            .collect();
        assert_eq!(
            x,
            vec![
                Some(b"cc".to_vec()),
                None,
                Some(b"aa".to_vec()),
                Some(b"bb".to_vec())
            ]
        );
    }

    #[test]
    fn test_solitontxn() {
        let storage = TestStorageBuilder::new(DummyDaggerManager {}, ApiVersion::V1)
            .build()
            .unwrap();
        let (tx, rx) = channel();
        storage
            .sched_solitontxn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::make_put(Key::from_cocauset(b"x"), b"100".to_vec())],
                    b"x".to_vec(),
                    100.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        storage
            .sched_solitontxn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::make_put(Key::from_cocauset(b"y"), b"101".to_vec())],
                    b"y".to_vec(),
                    101.into(),
                ),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        rx.recv().unwrap();
        storage
            .sched_solitontxn_command(
                commands::Commit::new(
                    vec![Key::from_cocauset(b"x")],
                    100.into(),
                    110.into(),
                    Context::default(),
                ),
                expect_value_callback(tx.clone(), 2, TxnStatus::committed(110.into())),
            )
            .unwrap();
        storage
            .sched_solitontxn_command(
                commands::Commit::new(
                    vec![Key::from_cocauset(b"y")],
                    101.into(),
                    111.into(),
                    Context::default(),
                ),
                expect_value_callback(tx.clone(), 3, TxnStatus::committed(111.into())),
            )
            .unwrap();
        rx.recv().unwrap();
        rx.recv().unwrap();
        expect_value(
            b"100".to_vec(),
            bdagger_on(storage.get(Context::default(), Key::from_cocauset(b"x"), 120.into()))
                .unwrap()
                .0,
        );
        expect_value(
            b"101".to_vec(),
            bdagger_on(storage.get(Context::default(), Key::from_cocauset(b"y"), 120.into()))
                .unwrap()
                .0,
        );
        storage
            .sched_solitontxn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::make_put(Key::from_cocauset(b"x"), b"105".to_vec())],
                    b"x".to_vec(),
                    105.into(),
                ),
                expect_fail_callback(tx, 6, |e| match e {
                    Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Epaxos(epaxos::Error(
                        box epaxos::ErrorInner::WriteConflict { .. },
                    ))))) => (),
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();
    }

    #[test]
    fn test_sched_too_busy() {
        let config = Config {
            scheduler_pending_write_threshold: ReadableSize(1),
            ..Default::default()
        };
        let storage = TestStorageBuilder::new(DummyDaggerManager {}, ApiVersion::V1)
            .config(config)
            .build()
            .unwrap();
        let (tx, rx) = channel();
        expect_none(
            bdagger_on(storage.get(Context::default(), Key::from_cocauset(b"x"), 100.into()))
                .unwrap()
                .0,
        );
        storage
            .sched_solitontxn_command::<()>(
                commands::Pause::new(vec![Key::from_cocauset(b"x")], 1000, Context::default()),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        storage
            .sched_solitontxn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::make_put(Key::from_cocauset(b"y"), b"101".to_vec())],
                    b"y".to_vec(),
                    101.into(),
                ),
                expect_too_busy_callback(tx.clone(), 2),
            )
            .unwrap();
        rx.recv().unwrap();
        rx.recv().unwrap();
        storage
            .sched_solitontxn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::make_put(Key::from_cocauset(b"z"), b"102".to_vec())],
                    b"y".to_vec(),
                    102.into(),
                ),
                expect_ok_callback(tx, 3),
            )
            .unwrap();
        rx.recv().unwrap();
    }

    #[test]
    fn test_cleanup() {
        let storage = TestStorageBuilder::new(DummyDaggerManager {}, ApiVersion::V1)
            .build()
            .unwrap();
        let cm = storage.concurrency_manager.clone();
        let (tx, rx) = channel();
        storage
            .sched_solitontxn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::make_put(Key::from_cocauset(b"x"), b"100".to_vec())],
                    b"x".to_vec(),
                    100.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .sched_solitontxn_command(
                commands::Cleanup::new(
                    Key::from_cocauset(b"x"),
                    100.into(),
                    TimeStamp::zero(),
                    Context::default(),
                ),
                expect_ok_callback(tx, 1),
            )
            .unwrap();
        rx.recv().unwrap();
        assert_eq!(cm.max_ts(), 100.into());
        expect_none(
            bdagger_on(storage.get(Context::default(), Key::from_cocauset(b"x"), 105.into()))
                .unwrap()
                .0,
        );
    }

    #[test]
    fn test_cleanup_check_ttl() {
        let storage = TestStorageBuilder::new(DummyDaggerManager {}, ApiVersion::V1)
            .build()
            .unwrap();
        let (tx, rx) = channel();

        let ts = TimeStamp::compose;
        storage
            .sched_solitontxn_command(
                commands::Prewrite::with_dagger_ttl(
                    vec![Mutation::make_put(Key::from_cocauset(b"x"), b"110".to_vec())],
                    b"x".to_vec(),
                    ts(110, 0),
                    100,
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_solitontxn_command(
                commands::Cleanup::new(
                    Key::from_cocauset(b"x"),
                    ts(110, 0),
                    ts(120, 0),
                    Context::default(),
                ),
                expect_fail_callback(tx.clone(), 0, |e| match e {
                    Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Epaxos(epaxos::Error(
                        box epaxos::ErrorInner::KeyIsDaggered(info),
                    ))))) => assert_eq!(info.get_dagger_ttl(), 100),
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_solitontxn_command(
                commands::Cleanup::new(
                    Key::from_cocauset(b"x"),
                    ts(110, 0),
                    ts(220, 0),
                    Context::default(),
                ),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_none(
            bdagger_on(storage.get(Context::default(), Key::from_cocauset(b"x"), ts(230, 0)))
                .unwrap()
                .0,
        );
    }

    #[test]
    fn test_high_priority_get_put() {
        let storage = TestStorageBuilder::new(DummyDaggerManager {}, ApiVersion::V1)
            .build()
            .unwrap();
        let (tx, rx) = channel();
        let mut ctx = Context::default();
        ctx.set_priority(CommandPri::High);
        expect_none(
            bdagger_on(storage.get(ctx, Key::from_cocauset(b"x"), 100.into()))
                .unwrap()
                .0,
        );
        let mut ctx = Context::default();
        ctx.set_priority(CommandPri::High);
        storage
            .sched_solitontxn_command(
                commands::Prewrite::with_context(
                    vec![Mutation::make_put(Key::from_cocauset(b"x"), b"100".to_vec())],
                    b"x".to_vec(),
                    100.into(),
                    ctx,
                ),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        let mut ctx = Context::default();
        ctx.set_priority(CommandPri::High);
        storage
            .sched_solitontxn_command(
                commands::Commit::new(vec![Key::from_cocauset(b"x")], 100.into(), 101.into(), ctx),
                expect_ok_callback(tx, 2),
            )
            .unwrap();
        rx.recv().unwrap();
        let mut ctx = Context::default();
        ctx.set_priority(CommandPri::High);
        expect_none(
            bdagger_on(storage.get(ctx, Key::from_cocauset(b"x"), 100.into()))
                .unwrap()
                .0,
        );
        let mut ctx = Context::default();
        ctx.set_priority(CommandPri::High);
        expect_value(
            b"100".to_vec(),
            bdagger_on(storage.get(ctx, Key::from_cocauset(b"x"), 101.into()))
                .unwrap()
                .0,
        );
    }

    #[test]
    fn test_high_priority_no_bdagger() {
        let config = Config {
            scheduler_worker_pool_size: 1,
            ..Default::default()
        };
        let storage = TestStorageBuilder::new(DummyDaggerManager {}, ApiVersion::V1)
            .config(config)
            .build()
            .unwrap();
        let (tx, rx) = channel();
        expect_none(
            bdagger_on(storage.get(Context::default(), Key::from_cocauset(b"x"), 100.into()))
                .unwrap()
                .0,
        );
        storage
            .sched_solitontxn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::make_put(Key::from_cocauset(b"x"), b"100".to_vec())],
                    b"x".to_vec(),
                    100.into(),
                ),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .sched_solitontxn_command(
                commands::Commit::new(
                    vec![Key::from_cocauset(b"x")],
                    100.into(),
                    101.into(),
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 2),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_solitontxn_command(
                commands::Pause::new(vec![Key::from_cocauset(b"y")], 1000, Context::default()),
                expect_ok_callback(tx, 3),
            )
            .unwrap();
        let mut ctx = Context::default();
        ctx.set_priority(CommandPri::High);
        expect_value(
            b"100".to_vec(),
            bdagger_on(storage.get(ctx, Key::from_cocauset(b"x"), 101.into()))
                .unwrap()
                .0,
        );
        // Command Get with high priority not bdagger by command Pause.
        assert_eq!(rx.recv().unwrap(), 3);
    }

    #[test]
    fn test_delete_range() {
        let storage = TestStorageBuilder::new(DummyDaggerManager {}, ApiVersion::V1)
            .build()
            .unwrap();
        let (tx, rx) = channel();
        // Write x and y.
        storage
            .sched_solitontxn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::make_put(Key::from_cocauset(b"x"), b"100".to_vec()),
                        Mutation::make_put(Key::from_cocauset(b"y"), b"100".to_vec()),
                        Mutation::make_put(Key::from_cocauset(b"z"), b"100".to_vec()),
                    ],
                    b"x".to_vec(),
                    100.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .sched_solitontxn_command(
                commands::Commit::new(
                    vec![
                        Key::from_cocauset(b"x"),
                        Key::from_cocauset(b"y"),
                        Key::from_cocauset(b"z"),
                    ],
                    100.into(),
                    101.into(),
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_value(
            b"100".to_vec(),
            bdagger_on(storage.get(Context::default(), Key::from_cocauset(b"x"), 101.into()))
                .unwrap()
                .0,
        );
        expect_value(
            b"100".to_vec(),
            bdagger_on(storage.get(Context::default(), Key::from_cocauset(b"y"), 101.into()))
                .unwrap()
                .0,
        );
        expect_value(
            b"100".to_vec(),
            bdagger_on(storage.get(Context::default(), Key::from_cocauset(b"z"), 101.into()))
                .unwrap()
                .0,
        );

        // Delete range [x, z)
        storage
            .delete_range(
                Context::default(),
                Key::from_cocauset(b"x"),
                Key::from_cocauset(b"z"),
                false,
                expect_ok_callback(tx.clone(), 5),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_none(
            bdagger_on(storage.get(Context::default(), Key::from_cocauset(b"x"), 101.into()))
                .unwrap()
                .0,
        );
        expect_none(
            bdagger_on(storage.get(Context::default(), Key::from_cocauset(b"y"), 101.into()))
                .unwrap()
                .0,
        );
        expect_value(
            b"100".to_vec(),
            bdagger_on(storage.get(Context::default(), Key::from_cocauset(b"z"), 101.into()))
                .unwrap()
                .0,
        );

        storage
            .delete_range(
                Context::default(),
                Key::from_cocauset(b""),
                Key::from_cocauset(&[255]),
                false,
                expect_ok_callback(tx, 9),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_none(
            bdagger_on(storage.get(Context::default(), Key::from_cocauset(b"z"), 101.into()))
                .unwrap()
                .0,
        );
    }

    #[test]
    fn test_cocauset_delete_range() {
        test_cocauset_delete_range_impl(ApiVersion::V1);
        test_cocauset_delete_range_impl(ApiVersion::V1ttl);
        test_cocauset_delete_range_impl(ApiVersion::V2);
    }

    fn test_cocauset_delete_range_impl(api_version: ApiVersion) {
        let storage = TestStorageBuilder::new(DummyDaggerManager {}, api_version)
            .build()
            .unwrap();
        let (tx, rx) = channel();
        let req_api_version = if api_version == ApiVersion::V1ttl {
            ApiVersion::V1
        } else {
            api_version
        };
        let ctx = Context {
            api_version: req_api_version,
            ..Default::default()
        };

        let test_data = [
            (b"r\0a", b"001"),
            (b"r\0b", b"002"),
            (b"r\0c", b"003"),
            (b"r\0d", b"004"),
            (b"r\0e", b"005"),
        ];

        // Write some key-value pairs to the db
        for fdbhikv in &test_data {
            storage
                .cocauset_put(
                    ctx.clone(),
                    "".to_string(),
                    fdbhikv.0.to_vec(),
                    fdbhikv.1.to_vec(),
                    0,
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
        }

        expect_value(
            b"004".to_vec(),
            bdagger_on(storage.cocauset_get(ctx.clone(), "".to_string(), b"r\0d".to_vec())).unwrap(),
        );

        // Delete ["d", "e")
        storage
            .cocauset_delete_range(
                ctx.clone(),
                "".to_string(),
                b"r\0d".to_vec(),
                b"r\0e".to_vec(),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();

        // Assert key "d" has gone
        expect_value(
            b"003".to_vec(),
            bdagger_on(storage.cocauset_get(ctx.clone(), "".to_string(), b"r\0c".to_vec())).unwrap(),
        );
        expect_none(
            bdagger_on(storage.cocauset_get(ctx.clone(), "".to_string(), b"r\0d".to_vec())).unwrap(),
        );
        expect_value(
            b"005".to_vec(),
            bdagger_on(storage.cocauset_get(ctx.clone(), "".to_string(), b"r\0e".to_vec())).unwrap(),
        );

        // Delete ["aa", "ab")
        storage
            .cocauset_delete_range(
                ctx.clone(),
                "".to_string(),
                b"r\0aa".to_vec(),
                b"r\0ab".to_vec(),
                expect_ok_callback(tx.clone(), 2),
            )
            .unwrap();
        rx.recv().unwrap();

        // Assert nothing happened
        expect_value(
            b"001".to_vec(),
            bdagger_on(storage.cocauset_get(ctx.clone(), "".to_string(), b"r\0a".to_vec())).unwrap(),
        );
        expect_value(
            b"002".to_vec(),
            bdagger_on(storage.cocauset_get(ctx.clone(), "".to_string(), b"r\0b".to_vec())).unwrap(),
        );

        // Delete all
        storage
            .cocauset_delete_range(
                ctx.clone(),
                "".to_string(),
                b"r\0a".to_vec(),
                b"r\0z".to_vec(),
                expect_ok_callback(tx, 3),
            )
            .unwrap();
        rx.recv().unwrap();

        // Assert now no key remains
        for fdbhikv in &test_data {
            expect_none(
                bdagger_on(storage.cocauset_get(ctx.clone(), "".to_string(), fdbhikv.0.to_vec())).unwrap(),
            );
        }

        rx.recv().unwrap();
    }

    #[test]
    fn test_cocauset_batch_put() {
        test_cocauset_batch_put_impl(ApiVersion::V1);
        test_cocauset_batch_put_impl(ApiVersion::V1ttl);
        test_cocauset_batch_put_impl(ApiVersion::V2);
    }

    fn test_cocauset_batch_put_impl(api_version: ApiVersion) {
        let storage = TestStorageBuilder::new(DummyDaggerManager {}, api_version)
            .build()
            .unwrap();
        let (tx, rx) = channel();
        let req_api_version = if api_version == ApiVersion::V1ttl {
            ApiVersion::V1
        } else {
            api_version
        };
        let ctx = Context {
            api_version: req_api_version,
            ..Default::default()
        };

        let test_data = vec![
            (b"r\0a".to_vec(), b"aa".to_vec(), 10),
            (b"r\0b".to_vec(), b"bb".to_vec(), 20),
            (b"r\0c".to_vec(), b"cc".to_vec(), 30),
            (b"r\0d".to_vec(), b"dd".to_vec(), 0),
            (b"r\0e".to_vec(), b"ee".to_vec(), 40),
        ];

        let fdbhikvpairs = test_data
            .clone()
            .into_iter()
            .map(|(key, value, _)| (key, value))
            .collect();
        let ttls = if let ApiVersion::V1ttl | ApiVersion::V2 = api_version {
            test_data
                .clone()
                .into_iter()
                .map(|(_, _, ttl)| ttl)
                .collect()
        } else {
            vec![0; test_data.len()]
        };
        // Write key-value pairs in a batch
        storage
            .cocauset_batch_put(
                ctx.clone(),
                "".to_string(),
                fdbhikvpairs,
                ttls,
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Verify pairs one by one
        for (key, val, _) in &test_data {
            expect_value(
                val.to_vec(),
                bdagger_on(storage.cocauset_get(ctx.clone(), "".to_string(), key.to_vec())).unwrap(),
            );
        }
    }

    #[test]
    fn test_cocauset_batch_get() {
        test_cocauset_batch_get_impl(ApiVersion::V1);
        test_cocauset_batch_get_impl(ApiVersion::V1ttl);
        test_cocauset_batch_get_impl(ApiVersion::V2);
    }

    fn test_cocauset_batch_get_impl(api_version: ApiVersion) {
        let storage = TestStorageBuilder::new(DummyDaggerManager {}, api_version)
            .build()
            .unwrap();
        let (tx, rx) = channel();
        let req_api_version = if api_version == ApiVersion::V1ttl {
            ApiVersion::V1
        } else {
            api_version
        };
        let ctx = Context {
            api_version: req_api_version,
            ..Default::default()
        };

        let test_data = vec![
            (b"r\0a".to_vec(), b"aa".to_vec()),
            (b"r\0b".to_vec(), b"bb".to_vec()),
            (b"r\0c".to_vec(), b"cc".to_vec()),
            (b"r\0d".to_vec(), b"dd".to_vec()),
            (b"r\0e".to_vec(), b"ee".to_vec()),
        ];

        // Write key-value pairs one by one
        for &(ref key, ref value) in &test_data {
            storage
                .cocauset_put(
                    ctx.clone(),
                    "".to_string(),
                    key.clone(),
                    value.clone(),
                    0,
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
        }
        rx.recv().unwrap();

        // Verify pairs in a batch
        let keys = test_data.iter().map(|&(ref k, _)| k.clone()).collect();
        let results = test_data.into_iter().map(|(k, v)| Some((k, v))).collect();
        expect_multi_values(
            results,
            bdagger_on(storage.cocauset_batch_get(ctx, "".to_string(), keys)).unwrap(),
        );
    }

    #[test]
    fn test_batch_cocauset_get() {
        test_batch_cocauset_get_impl(ApiVersion::V1);
        test_batch_cocauset_get_impl(ApiVersion::V1ttl);
        test_batch_cocauset_get_impl(ApiVersion::V2);
    }

    fn test_batch_cocauset_get_impl(api_version: ApiVersion) {
        let storage = TestStorageBuilder::new(DummyDaggerManager {}, api_version)
            .build()
            .unwrap();
        let (tx, rx) = channel();
        let req_api_version = if api_version == ApiVersion::V1ttl {
            ApiVersion::V1
        } else {
            api_version
        };
        let ctx = Context {
            api_version: req_api_version,
            ..Default::default()
        };

        let test_data = vec![
            (b"r\0a".to_vec(), b"aa".to_vec()),
            (b"r\0b".to_vec(), b"bb".to_vec()),
            (b"r\0c".to_vec(), b"cc".to_vec()),
            (b"r\0d".to_vec(), b"dd".to_vec()),
            (b"r\0e".to_vec(), b"ee".to_vec()),
        ];

        // Write key-value pairs one by one
        for &(ref key, ref value) in &test_data {
            storage
                .cocauset_put(
                    ctx.clone(),
                    "".to_string(),
                    key.clone(),
                    value.clone(),
                    0,
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
        }
        rx.recv().unwrap();

        // Verify pairs in a batch
        let mut ids = vec![];
        let cmds = test_data
            .iter()
            .map(|&(ref k, _)| {
                let mut req = cocausetGetRequest::default();
                req.set_context(ctx.clone());
                req.set_key(k.clone());
                ids.push(ids.len() as u64);
                req
            })
            .collect();
        let results: Vec<Option<Vec<u8>>> = test_data.into_iter().map(|(_, v)| Some(v)).collect();
        let consumer = GetConsumer::new();
        bdagger_on(storage.cocauset_batch_get_command(cmds, ids, consumer.clone())).unwrap();
        let x: Vec<Option<Vec<u8>>> = consumer
            .take_data()
            .into_iter()
            .map(|x| x.unwrap())
            .collect();
        assert_eq!(x, results);
    }

    #[test]
    fn test_cocauset_batch_delete() {
        test_cocauset_batch_delete_impl(ApiVersion::V1);
        test_cocauset_batch_delete_impl(ApiVersion::V1ttl);
        test_cocauset_batch_delete_impl(ApiVersion::V2);
    }

    fn test_cocauset_batch_delete_impl(api_version: ApiVersion) {
        let storage = TestStorageBuilder::new(DummyDaggerManager {}, api_version)
            .build()
            .unwrap();
        let (tx, rx) = channel();
        let req_api_version = if api_version == ApiVersion::V1ttl {
            ApiVersion::V1
        } else {
            api_version
        };
        let ctx = Context {
            api_version: req_api_version,
            ..Default::default()
        };

        let test_data = vec![
            (b"r\0a".to_vec(), b"aa".to_vec()),
            (b"r\0b".to_vec(), b"bb".to_vec()),
            (b"r\0c".to_vec(), b"cc".to_vec()),
            (b"r\0d".to_vec(), b"dd".to_vec()),
            (b"r\0e".to_vec(), b"ee".to_vec()),
        ];

        // Write key-value pairs in batch
        storage
            .cocauset_batch_put(
                ctx.clone(),
                "".to_string(),
                test_data.clone(),
                vec![0; test_data.len()],
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Verify pairs exist
        let keys = test_data.iter().map(|&(ref k, _)| k.clone()).collect();
        let results = test_data
            .iter()
            .map(|&(ref k, ref v)| Some((k.clone(), v.clone())))
            .collect();
        expect_multi_values(
            results,
            bdagger_on(storage.cocauset_batch_get(ctx.clone(), "".to_string(), keys)).unwrap(),
        );

        // Delete ["b", "d"]
        storage
            .cocauset_batch_delete(
                ctx.clone(),
                "".to_string(),
                vec![b"r\0b".to_vec(), b"r\0d".to_vec()],
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();

        // Assert "b" and "d" are gone
        expect_value(
            b"aa".to_vec(),
            bdagger_on(storage.cocauset_get(ctx.clone(), "".to_string(), b"r\0a".to_vec())).unwrap(),
        );
        expect_none(
            bdagger_on(storage.cocauset_get(ctx.clone(), "".to_string(), b"r\0b".to_vec())).unwrap(),
        );
        expect_value(
            b"cc".to_vec(),
            bdagger_on(storage.cocauset_get(ctx.clone(), "".to_string(), b"r\0c".to_vec())).unwrap(),
        );
        expect_none(
            bdagger_on(storage.cocauset_get(ctx.clone(), "".to_string(), b"r\0d".to_vec())).unwrap(),
        );
        expect_value(
            b"ee".to_vec(),
            bdagger_on(storage.cocauset_get(ctx.clone(), "".to_string(), b"r\0e".to_vec())).unwrap(),
        );

        // Delete ["a", "c", "e"]
        storage
            .cocauset_batch_delete(
                ctx.clone(),
                "".to_string(),
                vec![b"r\0a".to_vec(), b"r\0c".to_vec(), b"r\0e".to_vec()],
                expect_ok_callback(tx, 2),
            )
            .unwrap();
        rx.recv().unwrap();

        // Assert no key remains
        for (k, _) in test_data {
            expect_none(bdagger_on(storage.cocauset_get(ctx.clone(), "".to_string(), k)).unwrap());
        }
    }

    #[test]
    fn test_cocauset_mutant_search() {
        test_cocauset_mutant_search_impl(ApiVersion::V1);
        test_cocauset_mutant_search_impl(ApiVersion::V1ttl);
        test_cocauset_mutant_search_impl(ApiVersion::V2);
    }

    fn test_cocauset_mutant_search_impl(api_version: ApiVersion) {
        let (end_key, end_key_reverse_mutant_search) = if let ApiVersion::V2 = api_version {
            (Some(b"r\0z".to_vec()), Some(b"r\0\0".to_vec()))
        } else {
            (None, None)
        };

        let storage = TestStorageBuilder::new(DummyDaggerManager {}, api_version)
            .build()
            .unwrap();
        let (tx, rx) = channel();
        let req_api_version = if api_version == ApiVersion::V1ttl {
            ApiVersion::V1
        } else {
            api_version
        };
        let ctx = Context {
            api_version: req_api_version,
            ..Default::default()
        };

        let test_data = vec![
            (b"r\0a".to_vec(), b"aa".to_vec()),
            (b"r\0a1".to_vec(), b"aa11".to_vec()),
            (b"r\0a2".to_vec(), b"aa22".to_vec()),
            (b"r\0a3".to_vec(), b"aa33".to_vec()),
            (b"r\0b".to_vec(), b"bb".to_vec()),
            (b"r\0b1".to_vec(), b"bb11".to_vec()),
            (b"r\0b2".to_vec(), b"bb22".to_vec()),
            (b"r\0b3".to_vec(), b"bb33".to_vec()),
            (b"r\0c".to_vec(), b"cc".to_vec()),
            (b"r\0c1".to_vec(), b"cc11".to_vec()),
            (b"r\0c2".to_vec(), b"cc22".to_vec()),
            (b"r\0c3".to_vec(), b"cc33".to_vec()),
            (b"r\0d".to_vec(), b"dd".to_vec()),
            (b"r\0d1".to_vec(), b"dd11".to_vec()),
            (b"r\0d2".to_vec(), b"dd22".to_vec()),
            (b"r\0d3".to_vec(), b"dd33".to_vec()),
            (b"r\0e".to_vec(), b"ee".to_vec()),
            (b"r\0e1".to_vec(), b"ee11".to_vec()),
            (b"r\0e2".to_vec(), b"ee22".to_vec()),
            (b"r\0e3".to_vec(), b"ee33".to_vec()),
        ];

        // Write key-value pairs in batch
        storage
            .cocauset_batch_put(
                ctx.clone(),
                "".to_string(),
                test_data.clone(),
                vec![0; test_data.len()],
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // SentinelSearch pairs with key only
        let mut results: Vec<Option<HikvPair>> = test_data
            .iter()
            .map(|&(ref k, _)| Some((k.clone(), vec![])))
            .collect();
        expect_multi_values(
            results.clone(),
            bdagger_on(storage.cocauset_mutant_search(
                ctx.clone(),
                "".to_string(),
                b"r\0".to_vec(),
                end_key.clone(),
                20,
                true,
                false,
            ))
            .unwrap(),
        );
        results = results.split_off(10);
        expect_multi_values(
            results,
            bdagger_on(storage.cocauset_mutant_search(
                ctx.clone(),
                "".to_string(),
                b"r\0c2".to_vec(),
                end_key.clone(),
                20,
                true,
                false,
            ))
            .unwrap(),
        );
        let mut results: Vec<Option<HikvPair>> = test_data
            .clone()
            .into_iter()
            .map(|(k, v)| Some((k, v)))
            .collect();
        expect_multi_values(
            results.clone(),
            bdagger_on(storage.cocauset_mutant_search(
                ctx.clone(),
                "".to_string(),
                b"r\0".to_vec(),
                end_key.clone(),
                20,
                false,
                false,
            ))
            .unwrap(),
        );
        results = results.split_off(10);
        expect_multi_values(
            results,
            bdagger_on(storage.cocauset_mutant_search(
                ctx.clone(),
                "".to_string(),
                b"r\0c2".to_vec(),
                end_key,
                20,
                false,
                false,
            ))
            .unwrap(),
        );
        let results: Vec<Option<HikvPair>> = test_data
            .clone()
            .into_iter()
            .map(|(k, v)| Some((k, v)))
            .rev()
            .collect();
        expect_multi_values(
            results,
            bdagger_on(storage.cocauset_mutant_search(
                ctx.clone(),
                "".to_string(),
                b"r\0z".to_vec(),
                end_key_reverse_mutant_search.clone(),
                20,
                false,
                true,
            ))
            .unwrap(),
        );
        let results: Vec<Option<HikvPair>> = test_data
            .clone()
            .into_iter()
            .map(|(k, v)| Some((k, v)))
            .rev()
            .take(5)
            .collect();
        expect_multi_values(
            results,
            bdagger_on(storage.cocauset_mutant_search(
                ctx.clone(),
                "".to_string(),
                b"r\0z".to_vec(),
                end_key_reverse_mutant_search,
                5,
                false,
                true,
            ))
            .unwrap(),
        );

        // SentinelSearch with end_key
        let results: Vec<Option<HikvPair>> = test_data
            .clone()
            .into_iter()
            .skip(6)
            .take(4)
            .map(|(k, v)| Some((k, v)))
            .collect();
        expect_multi_values(
            results,
            bdagger_on(storage.cocauset_mutant_search(
                ctx.clone(),
                "".to_string(),
                b"r\0b2".to_vec(),
                Some(b"r\0c2".to_vec()),
                20,
                false,
                false,
            ))
            .unwrap(),
        );
        let results: Vec<Option<HikvPair>> = test_data
            .clone()
            .into_iter()
            .skip(6)
            .take(1)
            .map(|(k, v)| Some((k, v)))
            .collect();
        expect_multi_values(
            results,
            bdagger_on(storage.cocauset_mutant_search(
                ctx.clone(),
                "".to_string(),
                b"r\0b2".to_vec(),
                Some(b"r\0b2\x00".to_vec()),
                20,
                false,
                false,
            ))
            .unwrap(),
        );

        // Reverse mutant_search with end_key
        let results: Vec<Option<HikvPair>> = test_data
            .clone()
            .into_iter()
            .rev()
            .skip(10)
            .take(4)
            .map(|(k, v)| Some((k, v)))
            .collect();
        expect_multi_values(
            results,
            bdagger_on(storage.cocauset_mutant_search(
                ctx.clone(),
                "".to_string(),
                b"r\0c2".to_vec(),
                Some(b"r\0b2".to_vec()),
                20,
                false,
                true,
            ))
            .unwrap(),
        );
        let results: Vec<Option<HikvPair>> = test_data
            .into_iter()
            .skip(6)
            .take(1)
            .map(|(k, v)| Some((k, v)))
            .collect();
        expect_multi_values(
            results,
            bdagger_on(storage.cocauset_mutant_search(
                ctx.clone(),
                "".to_string(),
                b"r\0b2\x00".to_vec(),
                Some(b"r\0b2".to_vec()),
                20,
                false,
                true,
            ))
            .unwrap(),
        );

        // End key tests. Confirm that lower/upper bound works correctly.
        let results = vec![
            (b"r\0c1".to_vec(), b"cc11".to_vec()),
            (b"r\0c2".to_vec(), b"cc22".to_vec()),
            (b"r\0c3".to_vec(), b"cc33".to_vec()),
            (b"r\0d".to_vec(), b"dd".to_vec()),
            (b"r\0d1".to_vec(), b"dd11".to_vec()),
            (b"r\0d2".to_vec(), b"dd22".to_vec()),
        ]
        .into_iter()
        .map(|(k, v)| Some((k, v)));
        expect_multi_values(
            results.clone().collect(),
            bdagger_on(async {
                storage
                    .cocauset_mutant_search(
                        ctx.clone(),
                        "".to_string(),
                        b"r\0c1".to_vec(),
                        Some(b"r\0d3".to_vec()),
                        20,
                        false,
                        false,
                    )
                    .await
            })
            .unwrap(),
        );
        expect_multi_values(
            results.rev().collect(),
            bdagger_on(async {
                storage
                    .cocauset_mutant_search(
                        ctx.clone(),
                        "".to_string(),
                        b"r\0d3".to_vec(),
                        Some(b"r\0c1".to_vec()),
                        20,
                        false,
                        true,
                    )
                    .await
            })
            .unwrap(),
        );
    }

    #[test]
    fn test_check_key_ranges() {
        fn make_ranges(ranges: Vec<(Vec<u8>, Vec<u8>)>) -> Vec<KeyRange> {
            ranges
                .into_iter()
                .map(|(s, e)| {
                    let mut range = KeyRange::default();
                    range.set_start_key(s);
                    if !e.is_empty() {
                        range.set_end_key(e);
                    }
                    range
                })
                .collect()
        }

        let ranges = make_ranges(vec![
            (b"a".to_vec(), b"a3".to_vec()),
            (b"b".to_vec(), b"b3".to_vec()),
            (b"c".to_vec(), b"c3".to_vec()),
        ]);
        assert_eq!(
            <Storage<Rockseinstein_merkle_tree, DummyDaggerManager>>::check_key_ranges(&ranges, false,),
            true
        );

        let ranges = make_ranges(vec![
            (b"a".to_vec(), vec![]),
            (b"b".to_vec(), vec![]),
            (b"c".to_vec(), vec![]),
        ]);
        assert_eq!(
            <Storage<Rockseinstein_merkle_tree, DummyDaggerManager>>::check_key_ranges(&ranges, false,),
            true
        );

        let ranges = make_ranges(vec![
            (b"a3".to_vec(), b"a".to_vec()),
            (b"b3".to_vec(), b"b".to_vec()),
            (b"c3".to_vec(), b"c".to_vec()),
        ]);
        assert_eq!(
            <Storage<Rockseinstein_merkle_tree, DummyDaggerManager>>::check_key_ranges(&ranges, false,),
            false
        );

        // if end_key is omitted, the next start_key is used instead. so, false is returned.
        let ranges = make_ranges(vec![
            (b"c".to_vec(), vec![]),
            (b"b".to_vec(), vec![]),
            (b"a".to_vec(), vec![]),
        ]);
        assert_eq!(
            <Storage<Rockseinstein_merkle_tree, DummyDaggerManager>>::check_key_ranges(&ranges, false,),
            false
        );

        let ranges = make_ranges(vec![
            (b"a3".to_vec(), b"a".to_vec()),
            (b"b3".to_vec(), b"b".to_vec()),
            (b"c3".to_vec(), b"c".to_vec()),
        ]);
        assert_eq!(
            <Storage<Rockseinstein_merkle_tree, DummyDaggerManager>>::check_key_ranges(&ranges, true,),
            true
        );

        let ranges = make_ranges(vec![
            (b"c3".to_vec(), vec![]),
            (b"b3".to_vec(), vec![]),
            (b"a3".to_vec(), vec![]),
        ]);
        assert_eq!(
            <Storage<Rockseinstein_merkle_tree, DummyDaggerManager>>::check_key_ranges(&ranges, true,),
            true
        );

        let ranges = make_ranges(vec![
            (b"a".to_vec(), b"a3".to_vec()),
            (b"b".to_vec(), b"b3".to_vec()),
            (b"c".to_vec(), b"c3".to_vec()),
        ]);
        assert_eq!(
            <Storage<Rockseinstein_merkle_tree, DummyDaggerManager>>::check_key_ranges(&ranges, true,),
            false
        );

        let ranges = make_ranges(vec![
            (b"a3".to_vec(), vec![]),
            (b"b3".to_vec(), vec![]),
            (b"c3".to_vec(), vec![]),
        ]);
        assert_eq!(
            <Storage<Rockseinstein_merkle_tree, DummyDaggerManager>>::check_key_ranges(&ranges, true,),
            false
        );
    }

    #[test]
    fn test_cocauset_batch_mutant_search() {
        test_cocauset_batch_mutant_search_impl(ApiVersion::V1);
        test_cocauset_batch_mutant_search_impl(ApiVersion::V1ttl);
        test_cocauset_batch_mutant_search_impl(ApiVersion::V2);
    }

    fn test_cocauset_batch_mutant_search_impl(api_version: ApiVersion) {
        let make_ranges = |delimiters: Vec<Vec<u8>>| -> Vec<KeyRange> {
            delimiters
                .windows(2)
                .map(|key_pair| {
                    let mut range = KeyRange::default();
                    range.set_start_key(key_pair[0].clone());
                    if let ApiVersion::V2 = api_version {
                        range.set_end_key(key_pair[1].clone());
                    };
                    range
                })
                .collect()
        };

        let storage = TestStorageBuilder::new(DummyDaggerManager {}, api_version)
            .build()
            .unwrap();
        let (tx, rx) = channel();
        let req_api_version = if api_version == ApiVersion::V1ttl {
            ApiVersion::V1
        } else {
            api_version
        };
        let ctx = Context {
            api_version: req_api_version,
            ..Default::default()
        };

        let test_data = vec![
            (b"r\0a".to_vec(), b"aa".to_vec()),
            (b"r\0a1".to_vec(), b"aa11".to_vec()),
            (b"r\0a2".to_vec(), b"aa22".to_vec()),
            (b"r\0a3".to_vec(), b"aa33".to_vec()),
            (b"r\0b".to_vec(), b"bb".to_vec()),
            (b"r\0b1".to_vec(), b"bb11".to_vec()),
            (b"r\0b2".to_vec(), b"bb22".to_vec()),
            (b"r\0b3".to_vec(), b"bb33".to_vec()),
            (b"r\0c".to_vec(), b"cc".to_vec()),
            (b"r\0c1".to_vec(), b"cc11".to_vec()),
            (b"r\0c2".to_vec(), b"cc22".to_vec()),
            (b"r\0c3".to_vec(), b"cc33".to_vec()),
            (b"r\0d".to_vec(), b"dd".to_vec()),
            (b"r\0d1".to_vec(), b"dd11".to_vec()),
            (b"r\0d2".to_vec(), b"dd22".to_vec()),
            (b"r\0d3".to_vec(), b"dd33".to_vec()),
            (b"r\0e".to_vec(), b"ee".to_vec()),
            (b"r\0e1".to_vec(), b"ee11".to_vec()),
            (b"r\0e2".to_vec(), b"ee22".to_vec()),
            (b"r\0e3".to_vec(), b"ee33".to_vec()),
        ];

        // Write key-value pairs in batch
        storage
            .cocauset_batch_put(
                ctx.clone(),
                "".to_string(),
                test_data.clone(),
                vec![0; test_data.len()],
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Verify pairs exist
        let keys = test_data.iter().map(|&(ref k, _)| k.clone()).collect();
        let results = test_data.into_iter().map(|(k, v)| Some((k, v))).collect();
        expect_multi_values(
            results,
            bdagger_on(storage.cocauset_batch_get(ctx.clone(), "".to_string(), keys)).unwrap(),
        );

        let results = vec![
            Some((b"r\0a".to_vec(), b"aa".to_vec())),
            Some((b"r\0a1".to_vec(), b"aa11".to_vec())),
            Some((b"r\0a2".to_vec(), b"aa22".to_vec())),
            Some((b"r\0a3".to_vec(), b"aa33".to_vec())),
            Some((b"r\0b".to_vec(), b"bb".to_vec())),
            Some((b"r\0b1".to_vec(), b"bb11".to_vec())),
            Some((b"r\0b2".to_vec(), b"bb22".to_vec())),
            Some((b"r\0b3".to_vec(), b"bb33".to_vec())),
            Some((b"r\0c".to_vec(), b"cc".to_vec())),
            Some((b"r\0c1".to_vec(), b"cc11".to_vec())),
            Some((b"r\0c2".to_vec(), b"cc22".to_vec())),
            Some((b"r\0c3".to_vec(), b"cc33".to_vec())),
            Some((b"r\0d".to_vec(), b"dd".to_vec())),
        ];
        let ranges: Vec<KeyRange> = make_ranges(vec![
            b"r\0a".to_vec(),
            b"r\0b".to_vec(),
            b"r\0c".to_vec(),
            b"r\0z".to_vec(),
        ]);
        expect_multi_values(
            results,
            bdagger_on(storage.cocauset_batch_mutant_search(
                ctx.clone(),
                "".to_string(),
                ranges.clone(),
                5,
                false,
                false,
            ))
            .unwrap(),
        );

        let results = vec![
            Some((b"r\0a".to_vec(), vec![])),
            Some((b"r\0a1".to_vec(), vec![])),
            Some((b"r\0a2".to_vec(), vec![])),
            Some((b"r\0a3".to_vec(), vec![])),
            Some((b"r\0b".to_vec(), vec![])),
            Some((b"r\0b1".to_vec(), vec![])),
            Some((b"r\0b2".to_vec(), vec![])),
            Some((b"r\0b3".to_vec(), vec![])),
            Some((b"r\0c".to_vec(), vec![])),
            Some((b"r\0c1".to_vec(), vec![])),
            Some((b"r\0c2".to_vec(), vec![])),
            Some((b"r\0c3".to_vec(), vec![])),
            Some((b"r\0d".to_vec(), vec![])),
        ];
        expect_multi_values(
            results,
            bdagger_on(storage.cocauset_batch_mutant_search(
                ctx.clone(),
                "".to_string(),
                ranges.clone(),
                5,
                true,
                false,
            ))
            .unwrap(),
        );

        let results = vec![
            Some((b"r\0a".to_vec(), b"aa".to_vec())),
            Some((b"r\0a1".to_vec(), b"aa11".to_vec())),
            Some((b"r\0a2".to_vec(), b"aa22".to_vec())),
            Some((b"r\0b".to_vec(), b"bb".to_vec())),
            Some((b"r\0b1".to_vec(), b"bb11".to_vec())),
            Some((b"r\0b2".to_vec(), b"bb22".to_vec())),
            Some((b"r\0c".to_vec(), b"cc".to_vec())),
            Some((b"r\0c1".to_vec(), b"cc11".to_vec())),
            Some((b"r\0c2".to_vec(), b"cc22".to_vec())),
        ];
        expect_multi_values(
            results,
            bdagger_on(storage.cocauset_batch_mutant_search(
                ctx.clone(),
                "".to_string(),
                ranges.clone(),
                3,
                false,
                false,
            ))
            .unwrap(),
        );

        let results = vec![
            Some((b"r\0a".to_vec(), vec![])),
            Some((b"r\0a1".to_vec(), vec![])),
            Some((b"r\0a2".to_vec(), vec![])),
            Some((b"r\0b".to_vec(), vec![])),
            Some((b"r\0b1".to_vec(), vec![])),
            Some((b"r\0b2".to_vec(), vec![])),
            Some((b"r\0c".to_vec(), vec![])),
            Some((b"r\0c1".to_vec(), vec![])),
            Some((b"r\0c2".to_vec(), vec![])),
        ];
        expect_multi_values(
            results,
            bdagger_on(storage.cocauset_batch_mutant_search(ctx.clone(), "".to_string(), ranges, 3, true, false))
                .unwrap(),
        );

        let results = vec![
            Some((b"r\0a2".to_vec(), b"aa22".to_vec())),
            Some((b"r\0a1".to_vec(), b"aa11".to_vec())),
            Some((b"r\0a".to_vec(), b"aa".to_vec())),
            Some((b"r\0b2".to_vec(), b"bb22".to_vec())),
            Some((b"r\0b1".to_vec(), b"bb11".to_vec())),
            Some((b"r\0b".to_vec(), b"bb".to_vec())),
            Some((b"r\0c2".to_vec(), b"cc22".to_vec())),
            Some((b"r\0c1".to_vec(), b"cc11".to_vec())),
            Some((b"r\0c".to_vec(), b"cc".to_vec())),
        ];
        let ranges: Vec<KeyRange> = vec![
            (b"r\0a3".to_vec(), b"r\0a".to_vec()),
            (b"r\0b3".to_vec(), b"r\0b".to_vec()),
            (b"r\0c3".to_vec(), b"r\0c".to_vec()),
        ]
        .into_iter()
        .map(|(s, e)| {
            let mut range = KeyRange::default();
            range.set_start_key(s);
            range.set_end_key(e);
            range
        })
        .collect();
        expect_multi_values(
            results,
            bdagger_on(storage.cocauset_batch_mutant_search(ctx.clone(), "".to_string(), ranges, 5, false, true))
                .unwrap(),
        );

        let results = vec![
            Some((b"r\0c2".to_vec(), b"cc22".to_vec())),
            Some((b"r\0c1".to_vec(), b"cc11".to_vec())),
            Some((b"r\0b2".to_vec(), b"bb22".to_vec())),
            Some((b"r\0b1".to_vec(), b"bb11".to_vec())),
            Some((b"r\0a2".to_vec(), b"aa22".to_vec())),
            Some((b"r\0a1".to_vec(), b"aa11".to_vec())),
        ];
        let ranges: Vec<KeyRange> = make_ranges(vec![
            b"r\0c3".to_vec(),
            b"r\0b3".to_vec(),
            b"r\0a3".to_vec(),
            b"r\0".to_vec(),
        ]);
        expect_multi_values(
            results,
            bdagger_on(storage.cocauset_batch_mutant_search(ctx.clone(), "".to_string(), ranges, 2, false, true))
                .unwrap(),
        );

        let results = vec![
            Some((b"r\0a2".to_vec(), vec![])),
            Some((b"r\0a1".to_vec(), vec![])),
            Some((b"r\0a".to_vec(), vec![])),
            Some((b"r\0b2".to_vec(), vec![])),
            Some((b"r\0b1".to_vec(), vec![])),
            Some((b"r\0b".to_vec(), vec![])),
            Some((b"r\0c2".to_vec(), vec![])),
            Some((b"r\0c1".to_vec(), vec![])),
            Some((b"r\0c".to_vec(), vec![])),
        ];
        let ranges: Vec<KeyRange> = vec![
            (b"r\0a3".to_vec(), b"r\0a".to_vec()),
            (b"r\0b3".to_vec(), b"r\0b".to_vec()),
            (b"r\0c3".to_vec(), b"r\0c".to_vec()),
        ]
        .into_iter()
        .map(|(s, e)| {
            let mut range = KeyRange::default();
            range.set_start_key(s);
            range.set_end_key(e);
            range
        })
        .collect();
        expect_multi_values(
            results,
            bdagger_on(storage.cocauset_batch_mutant_search(ctx, "".to_string(), ranges, 5, true, true)).unwrap(),
        );
    }

    #[test]
    fn test_cocauset_get_key_ttl() {
        test_cocauset_get_key_ttl_impl(ApiVersion::V1ttl);
        test_cocauset_get_key_ttl_impl(ApiVersion::V2);
    }

    fn test_cocauset_get_key_ttl_impl(api_version: ApiVersion) {
        let storage = TestStorageBuilder::new(DummyDaggerManager {}, api_version)
            .build()
            .unwrap();
        let (tx, rx) = channel();
        let req_api_version = if api_version == ApiVersion::V1ttl {
            ApiVersion::V1
        } else {
            api_version
        };
        let ctx = Context {
            api_version: req_api_version,
            ..Default::default()
        };

        let test_data = vec![
            (b"r\0a".to_vec(), b"aa".to_vec(), 10),
            (b"r\0b".to_vec(), b"bb".to_vec(), 20),
            (b"r\0c".to_vec(), b"cc".to_vec(), 0),
            (b"r\0d".to_vec(), b"dd".to_vec(), 10),
            (b"r\0e".to_vec(), b"ee".to_vec(), 20),
            (b"r\0f".to_vec(), b"ff".to_vec(), u64::MAX),
        ];

        let before_written = ttl_current_ts();
        // Write key-value pairs one by one
        for &(ref key, ref value, ttl) in &test_data {
            storage
                .cocauset_put(
                    ctx.clone(),
                    "".to_string(),
                    key.clone(),
                    value.clone(),
                    ttl,
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
        }
        rx.recv().unwrap();

        for &(ref key, _, ttl) in &test_data {
            let res = bdagger_on(storage.cocauset_get_key_ttl(ctx.clone(), "".to_string(), key.clone()))
                .unwrap()
                .unwrap();
            if ttl != 0 {
                let lower_bound = before_written.saturating_add(ttl) - ttl_current_ts();
                assert!(
                    res >= lower_bound && res <= ttl,
                    "{} < {} < {}",
                    lower_bound,
                    res,
                    ttl
                );
            } else {
                assert_eq!(res, 0);
            }
        }
    }

    #[test]
    fn test_mutant_search_dagger() {
        let storage = TestStorageBuilder::new(DummyDaggerManager {}, ApiVersion::V1)
            .build()
            .unwrap();
        let (tx, rx) = channel();
        storage
            .sched_solitontxn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::make_put(Key::from_cocauset(b"x"), b"foo".to_vec()),
                        Mutation::make_put(Key::from_cocauset(b"y"), b"foo".to_vec()),
                        Mutation::make_put(Key::from_cocauset(b"z"), b"foo".to_vec()),
                    ],
                    b"x".to_vec(),
                    100.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_solitontxn_command(
                commands::Prewrite::new(
                    vec![
                        Mutation::make_put(Key::from_cocauset(b"a"), b"foo".to_vec()),
                        Mutation::make_put(Key::from_cocauset(b"b"), b"foo".to_vec()),
                        Mutation::make_put(Key::from_cocauset(b"c"), b"foo".to_vec()),
                    ],
                    b"c".to_vec(),
                    101.into(),
                    123,
                    false,
                    3,
                    TimeStamp::default(),
                    TimeStamp::default(),
                    None,
                    false,
                    AssertionLevel::Off,
                    Context::default(),
                ),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();

        let (dagger_a, dagger_b, dagger_c, dagger_x, dagger_y, dagger_z) = (
            {
                let mut dagger = DaggerInfo::default();
                dagger.set_primary_dagger(b"c".to_vec());
                dagger.set_dagger_version(101);
                dagger.set_key(b"a".to_vec());
                dagger.set_dagger_ttl(123);
                dagger.set_solitontxn_size(3);
                dagger
            },
            {
                let mut dagger = DaggerInfo::default();
                dagger.set_primary_dagger(b"c".to_vec());
                dagger.set_dagger_version(101);
                dagger.set_key(b"b".to_vec());
                dagger.set_dagger_ttl(123);
                dagger.set_solitontxn_size(3);
                dagger
            },
            {
                let mut dagger = DaggerInfo::default();
                dagger.set_primary_dagger(b"c".to_vec());
                dagger.set_dagger_version(101);
                dagger.set_key(b"c".to_vec());
                dagger.set_dagger_ttl(123);
                dagger.set_solitontxn_size(3);
                dagger
            },
            {
                let mut dagger = DaggerInfo::default();
                dagger.set_primary_dagger(b"x".to_vec());
                dagger.set_dagger_version(100);
                dagger.set_key(b"x".to_vec());
                dagger
            },
            {
                let mut dagger = DaggerInfo::default();
                dagger.set_primary_dagger(b"x".to_vec());
                dagger.set_dagger_version(100);
                dagger.set_key(b"y".to_vec());
                dagger
            },
            {
                let mut dagger = DaggerInfo::default();
                dagger.set_primary_dagger(b"x".to_vec());
                dagger.set_dagger_version(100);
                dagger.set_key(b"z".to_vec());
                dagger
            },
        );

        let cm = storage.concurrency_manager.clone();

        let res =
            bdagger_on(storage.mutant_search_dagger(Context::default(), 99.into(), None, None, 10)).unwrap();
        assert_eq!(res, vec![]);
        assert_eq!(cm.max_ts(), 99.into());

        let res =
            bdagger_on(storage.mutant_search_dagger(Context::default(), 100.into(), None, None, 10)).unwrap();
        assert_eq!(res, vec![dagger_x.clone(), dagger_y.clone(), dagger_z.clone()]);
        assert_eq!(cm.max_ts(), 100.into());

        let res = bdagger_on(storage.mutant_search_dagger(
            Context::default(),
            100.into(),
            Some(Key::from_cocauset(b"a")),
            None,
            10,
        ))
        .unwrap();
        assert_eq!(res, vec![dagger_x.clone(), dagger_y.clone(), dagger_z.clone()]);

        let res = bdagger_on(storage.mutant_search_dagger(
            Context::default(),
            100.into(),
            Some(Key::from_cocauset(b"y")),
            None,
            10,
        ))
        .unwrap();
        assert_eq!(res, vec![dagger_y.clone(), dagger_z.clone()]);

        let res =
            bdagger_on(storage.mutant_search_dagger(Context::default(), 101.into(), None, None, 10)).unwrap();
        assert_eq!(
            res,
            vec![
                dagger_a.clone(),
                dagger_b.clone(),
                dagger_c.clone(),
                dagger_x.clone(),
                dagger_y.clone(),
                dagger_z.clone(),
            ]
        );
        assert_eq!(cm.max_ts(), 101.into());

        let res =
            bdagger_on(storage.mutant_search_dagger(Context::default(), 101.into(), None, None, 4)).unwrap();
        assert_eq!(
            res,
            vec![dagger_a, dagger_b.clone(), dagger_c.clone(), dagger_x.clone()]
        );

        let res = bdagger_on(storage.mutant_search_dagger(
            Context::default(),
            101.into(),
            Some(Key::from_cocauset(b"b")),
            None,
            4,
        ))
        .unwrap();
        assert_eq!(
            res,
            vec![
                dagger_b.clone(),
                dagger_c.clone(),
                dagger_x.clone(),
                dagger_y.clone(),
            ]
        );

        let res = bdagger_on(storage.mutant_search_dagger(
            Context::default(),
            101.into(),
            Some(Key::from_cocauset(b"b")),
            None,
            0,
        ))
        .unwrap();
        assert_eq!(
            res,
            vec![
                dagger_b.clone(),
                dagger_c.clone(),
                dagger_x.clone(),
                dagger_y.clone(),
                dagger_z
            ]
        );

        let res = bdagger_on(storage.mutant_search_dagger(
            Context::default(),
            101.into(),
            Some(Key::from_cocauset(b"b")),
            Some(Key::from_cocauset(b"c")),
            0,
        ))
        .unwrap();
        assert_eq!(res, vec![dagger_b.clone()]);

        let res = bdagger_on(storage.mutant_search_dagger(
            Context::default(),
            101.into(),
            Some(Key::from_cocauset(b"b")),
            Some(Key::from_cocauset(b"z")),
            4,
        ))
        .unwrap();
        assert_eq!(
            res,
            vec![
                dagger_b.clone(),
                dagger_c.clone(),
                dagger_x.clone(),
                dagger_y.clone()
            ]
        );

        let res = bdagger_on(storage.mutant_search_dagger(
            Context::default(),
            101.into(),
            Some(Key::from_cocauset(b"b")),
            Some(Key::from_cocauset(b"z")),
            3,
        ))
        .unwrap();
        assert_eq!(res, vec![dagger_b.clone(), dagger_c.clone(), dagger_x.clone()]);

        let mem_dagger = |k: &[u8], ts: u64, dagger_type| {
            let key = Key::from_cocauset(k);
            let guard = bdagger_on(cm.dagger_key(&key));
            guard.with_dagger(|dagger| {
                *dagger = Some(solitontxn_types::Dagger::new(
                    dagger_type,
                    k.to_vec(),
                    ts.into(),
                    100,
                    None,
                    0.into(),
                    1,
                    20.into(),
                ));
            });
            guard
        };

        let guard = mem_dagger(b"z", 80, DaggerType::Put);
        bdagger_on(storage.mutant_search_dagger(Context::default(), 101.into(), None, None, 1)).unwrap_err();

        let guard2 = mem_dagger(b"a", 80, DaggerType::Put);
        let res = bdagger_on(storage.mutant_search_dagger(
            Context::default(),
            101.into(),
            Some(Key::from_cocauset(b"b")),
            Some(Key::from_cocauset(b"z")),
            0,
        ))
        .unwrap();
        assert_eq!(
            res,
            vec![
                dagger_b.clone(),
                dagger_c.clone(),
                dagger_x.clone(),
                dagger_y.clone()
            ]
        );
        drop(guard);
        drop(guard2);

        // DaggerType::Dagger can't be ignored by mutant_search_dagger
        let guard = mem_dagger(b"c", 80, DaggerType::Dagger);
        bdagger_on(storage.mutant_search_dagger(
            Context::default(),
            101.into(),
            Some(Key::from_cocauset(b"b")),
            Some(Key::from_cocauset(b"z")),
            1,
        ))
        .unwrap_err();
        drop(guard);

        let guard = mem_dagger(b"c", 102, DaggerType::Put);
        let res = bdagger_on(storage.mutant_search_dagger(
            Context::default(),
            101.into(),
            Some(Key::from_cocauset(b"b")),
            Some(Key::from_cocauset(b"z")),
            0,
        ))
        .unwrap();
        assert_eq!(res, vec![dagger_b, dagger_c, dagger_x, dagger_y]);
        drop(guard);
    }

    #[test]
    fn test_resolve_dagger() {
        use crate::einsteindb::storage::solitontxn::RESOLVE_LOCK_BATCH_SIZE;

        let storage = TestStorageBuilder::new(DummyDaggerManager {}, ApiVersion::V1)
            .build()
            .unwrap();
        let (tx, rx) = channel();

        // These daggers (transaction ts=99) are not going to be resolved.
        storage
            .sched_solitontxn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::make_put(Key::from_cocauset(b"a"), b"foo".to_vec()),
                        Mutation::make_put(Key::from_cocauset(b"b"), b"foo".to_vec()),
                        Mutation::make_put(Key::from_cocauset(b"c"), b"foo".to_vec()),
                    ],
                    b"c".to_vec(),
                    99.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        let (dagger_a, dagger_b, dagger_c) = (
            {
                let mut dagger = DaggerInfo::default();
                dagger.set_primary_dagger(b"c".to_vec());
                dagger.set_dagger_version(99);
                dagger.set_key(b"a".to_vec());
                dagger
            },
            {
                let mut dagger = DaggerInfo::default();
                dagger.set_primary_dagger(b"c".to_vec());
                dagger.set_dagger_version(99);
                dagger.set_key(b"b".to_vec());
                dagger
            },
            {
                let mut dagger = DaggerInfo::default();
                dagger.set_primary_dagger(b"c".to_vec());
                dagger.set_dagger_version(99);
                dagger.set_key(b"c".to_vec());
                dagger
            },
        );

        // We should be able to resolve all daggers for transaction ts=100 when there are this
        // many daggers.
        let mutant_searchned_daggers_coll = vec![
            1,
            RESOLVE_LOCK_BATCH_SIZE,
            RESOLVE_LOCK_BATCH_SIZE - 1,
            RESOLVE_LOCK_BATCH_SIZE + 1,
            RESOLVE_LOCK_BATCH_SIZE * 2,
            RESOLVE_LOCK_BATCH_SIZE * 2 - 1,
            RESOLVE_LOCK_BATCH_SIZE * 2 + 1,
        ];

        let is_rollback_coll = vec![
            false, // commit
            true,  // rollback
        ];
        let mut ts = 100.into();

        for mutant_searchned_daggers in mutant_searchned_daggers_coll {
            for is_rollback in &is_rollback_coll {
                let mut mutations = vec![];
                for i in 0..mutant_searchned_daggers {
                    mutations.push(Mutation::make_put(
                        Key::from_cocauset(format!("x{:08}", i).as_bytes()),
                        b"foo".to_vec(),
                    ));
                }

                storage
                    .sched_solitontxn_command(
                        commands::Prewrite::with_defaults(mutations, b"x".to_vec(), ts),
                        expect_ok_callback(tx.clone(), 0),
                    )
                    .unwrap();
                rx.recv().unwrap();

                let mut solitontxn_status = HashMap::default();
                solitontxn_status.insert(
                    ts,
                    if *is_rollback {
                        TimeStamp::zero() // rollback
                    } else {
                        (ts.into_inner() + 5).into() // commit, commit_ts = start_ts + 5
                    },
                );
                storage
                    .sched_solitontxn_command(
                        commands::ResolveDaggerReadPhase::new(solitontxn_status, None, Context::default()),
                        expect_ok_callback(tx.clone(), 0),
                    )
                    .unwrap();
                rx.recv().unwrap();

                // All daggers should be resolved except for a, b and c.
                let res =
                    bdagger_on(storage.mutant_search_dagger(Context::default(), ts, None, None, 0)).unwrap();
                assert_eq!(res, vec![dagger_a.clone(), dagger_b.clone(), dagger_c.clone()]);

                ts = (ts.into_inner() + 10).into();
            }
        }
    }

    #[test]
    fn test_resolve_dagger_lite() {
        let storage = TestStorageBuilder::new(DummyDaggerManager {}, ApiVersion::V1)
            .build()
            .unwrap();
        let (tx, rx) = channel();

        storage
            .sched_solitontxn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::make_put(Key::from_cocauset(b"a"), b"foo".to_vec()),
                        Mutation::make_put(Key::from_cocauset(b"b"), b"foo".to_vec()),
                        Mutation::make_put(Key::from_cocauset(b"c"), b"foo".to_vec()),
                    ],
                    b"c".to_vec(),
                    99.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Rollback key 'b' and key 'c' and left key 'a' still daggered.
        let resolve_keys = vec![Key::from_cocauset(b"b"), Key::from_cocauset(b"c")];
        storage
            .sched_solitontxn_command(
                commands::ResolveDaggerLite::new(
                    99.into(),
                    TimeStamp::zero(),
                    resolve_keys,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Check dagger for key 'a'.
        let dagger_a = {
            let mut dagger = DaggerInfo::default();
            dagger.set_primary_dagger(b"c".to_vec());
            dagger.set_dagger_version(99);
            dagger.set_key(b"a".to_vec());
            dagger
        };
        let res =
            bdagger_on(storage.mutant_search_dagger(Context::default(), 99.into(), None, None, 0)).unwrap();
        assert_eq!(res, vec![dagger_a]);

        // Resolve dagger for key 'a'.
        storage
            .sched_solitontxn_command(
                commands::ResolveDaggerLite::new(
                    99.into(),
                    TimeStamp::zero(),
                    vec![Key::from_cocauset(b"a")],
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_solitontxn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::make_put(Key::from_cocauset(b"a"), b"foo".to_vec()),
                        Mutation::make_put(Key::from_cocauset(b"b"), b"foo".to_vec()),
                        Mutation::make_put(Key::from_cocauset(b"c"), b"foo".to_vec()),
                    ],
                    b"c".to_vec(),
                    101.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Commit key 'b' and key 'c' and left key 'a' still daggered.
        let resolve_keys = vec![Key::from_cocauset(b"b"), Key::from_cocauset(b"c")];
        storage
            .sched_solitontxn_command(
                commands::ResolveDaggerLite::new(
                    101.into(),
                    102.into(),
                    resolve_keys,
                    Context::default(),
                ),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Check dagger for key 'a'.
        let dagger_a = {
            let mut dagger = DaggerInfo::default();
            dagger.set_primary_dagger(b"c".to_vec());
            dagger.set_dagger_version(101);
            dagger.set_key(b"a".to_vec());
            dagger
        };
        let res =
            bdagger_on(storage.mutant_search_dagger(Context::default(), 101.into(), None, None, 0)).unwrap();
        assert_eq!(res, vec![dagger_a]);
    }

    #[test]
    fn test_solitontxn_heart_beat() {
        let storage = TestStorageBuilder::new(DummyDaggerManager {}, ApiVersion::V1)
            .build()
            .unwrap();
        let (tx, rx) = channel();

        let k = Key::from_cocauset(b"k");
        let v = b"v".to_vec();

        let uncommitted = TxnStatus::uncommitted;

        // No dagger.
        storage
            .sched_solitontxn_command(
                commands::TxnHeartBeat::new(k.clone(), 10.into(), 100, Context::default()),
                expect_fail_callback(tx.clone(), 0, |e| match e {
                    Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Epaxos(epaxos::Error(
                        box epaxos::ErrorInner::TxnNotFound { .. },
                    ))))) => (),
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_solitontxn_command(
                commands::Prewrite::with_dagger_ttl(
                    vec![Mutation::make_put(k.clone(), v.clone())],
                    b"k".to_vec(),
                    10.into(),
                    100,
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        let dagger_with_ttl = |ttl| {
            solitontxn_types::Dagger::new(
                DaggerType::Put,
                b"k".to_vec(),
                10.into(),
                ttl,
                Some(v.clone()),
                0.into(),
                0,
                0.into(),
            )
        };

        // `advise_ttl` = 90, which is less than current ttl 100. The dagger's ttl will remains 100.
        storage
            .sched_solitontxn_command(
                commands::TxnHeartBeat::new(k.clone(), 10.into(), 90, Context::default()),
                expect_value_callback(tx.clone(), 0, uncommitted(dagger_with_ttl(100), false)),
            )
            .unwrap();
        rx.recv().unwrap();

        // `advise_ttl` = 110, which is greater than current ttl. The dagger's ttl will be updated to
        // 110.
        storage
            .sched_solitontxn_command(
                commands::TxnHeartBeat::new(k.clone(), 10.into(), 110, Context::default()),
                expect_value_callback(tx.clone(), 0, uncommitted(dagger_with_ttl(110), false)),
            )
            .unwrap();
        rx.recv().unwrap();

        // Dagger not match. Nothing happens except throwing an error.
        storage
            .sched_solitontxn_command(
                commands::TxnHeartBeat::new(k, 11.into(), 150, Context::default()),
                expect_fail_callback(tx, 0, |e| match e {
                    Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Epaxos(epaxos::Error(
                        box epaxos::ErrorInner::TxnNotFound { .. },
                    ))))) => (),
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();
    }

    #[test]
    fn test_check_solitontxn_status() {
        let storage = TestStorageBuilder::new(DummyDaggerManager {}, ApiVersion::V1)
            .build()
            .unwrap();
        let cm = storage.concurrency_manager.clone();
        let (tx, rx) = channel();

        let k = Key::from_cocauset(b"k");
        let v = b"b".to_vec();

        let ts = TimeStamp::compose;
        use TxnStatus::*;
        let uncommitted = TxnStatus::uncommitted;
        let committed = TxnStatus::committed;

        // No dagger and no commit info. Gets an error.
        storage
            .sched_solitontxn_command(
                commands::CheckTxnStatus::new(
                    k.clone(),
                    ts(9, 0),
                    ts(9, 1),
                    ts(9, 1),
                    false,
                    false,
                    false,
                    Context::default(),
                ),
                expect_fail_callback(tx.clone(), 0, |e| match e {
                    Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Epaxos(epaxos::Error(
                        box epaxos::ErrorInner::TxnNotFound { .. },
                    ))))) => (),
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();

        assert_eq!(cm.max_ts(), ts(9, 1));

        // No dagger and no commit info. If specified rollback_if_not_exist, the key will be rolled
        // back.
        storage
            .sched_solitontxn_command(
                commands::CheckTxnStatus::new(
                    k.clone(),
                    ts(9, 0),
                    ts(9, 1),
                    ts(9, 1),
                    true,
                    false,
                    false,
                    Context::default(),
                ),
                expect_value_callback(tx.clone(), 0, DaggerNotExist),
            )
            .unwrap();
        rx.recv().unwrap();

        // A rollback will be written, so an later-arriving prewrite will fail.
        storage
            .sched_solitontxn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::make_put(k.clone(), v.clone())],
                    k.as_encoded().to_vec(),
                    ts(9, 0),
                ),
                expect_fail_callback(tx.clone(), 0, |e| match e {
                    Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Epaxos(epaxos::Error(
                        box epaxos::ErrorInner::WriteConflict { .. },
                    ))))) => (),
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_solitontxn_command(
                commands::Prewrite::new(
                    vec![Mutation::make_put(k.clone(), v.clone())],
                    b"k".to_vec(),
                    ts(10, 0),
                    100,
                    false,
                    3,
                    ts(10, 1),
                    TimeStamp::default(),
                    Some(vec![b"k1".to_vec(), b"k2".to_vec()]),
                    false,
                    AssertionLevel::Off,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // If dagger exists and not expired, returns the dagger's information.
        storage
            .sched_solitontxn_command(
                commands::CheckTxnStatus::new(
                    k.clone(),
                    ts(10, 0),
                    0.into(),
                    0.into(),
                    true,
                    false,
                    false,
                    Context::default(),
                ),
                expect_value_callback(
                    tx.clone(),
                    0,
                    uncommitted(
                        solitontxn_types::Dagger::new(
                            DaggerType::Put,
                            b"k".to_vec(),
                            ts(10, 0),
                            100,
                            Some(v.clone()),
                            0.into(),
                            3,
                            ts(10, 1),
                        )
                        .use_async_commit(vec![b"k1".to_vec(), b"k2".to_vec()]),
                        false,
                    ),
                ),
            )
            .unwrap();
        rx.recv().unwrap();

        // TODO: Check the dagger's min_commit_ts field.

        storage
            .sched_solitontxn_command(
                commands::Commit::new(vec![k.clone()], ts(10, 0), ts(20, 0), Context::default()),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // If the transaction is committed, returns the commit_ts.
        storage
            .sched_solitontxn_command(
                commands::CheckTxnStatus::new(
                    k.clone(),
                    ts(10, 0),
                    ts(12, 0),
                    ts(15, 0),
                    true,
                    false,
                    false,
                    Context::default(),
                ),
                expect_value_callback(tx.clone(), 0, committed(ts(20, 0))),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_solitontxn_command(
                commands::Prewrite::with_dagger_ttl(
                    vec![Mutation::make_put(k.clone(), v)],
                    k.as_encoded().to_vec(),
                    ts(25, 0),
                    100,
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // If the dagger has expired, cleanup it.
        storage
            .sched_solitontxn_command(
                commands::CheckTxnStatus::new(
                    k.clone(),
                    ts(25, 0),
                    ts(126, 0),
                    ts(127, 0),
                    true,
                    false,
                    false,
                    Context::default(),
                ),
                expect_value_callback(tx.clone(), 0, TtlExpire),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_solitontxn_command(
                commands::Commit::new(vec![k], ts(25, 0), ts(28, 0), Context::default()),
                expect_fail_callback(tx, 0, |e| match e {
                    Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Epaxos(epaxos::Error(
                        box epaxos::ErrorInner::TxnDaggerNotFound { .. },
                    ))))) => (),
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();
    }

    #[test]
    fn test_check_secondary_daggers() {
        let storage = TestStorageBuilder::new(DummyDaggerManager {}, ApiVersion::V1)
            .build()
            .unwrap();
        let cm = storage.concurrency_manager.clone();
        let (tx, rx) = channel();

        let k1 = Key::from_cocauset(b"k1");
        let k2 = Key::from_cocauset(b"k2");

        storage
            .sched_solitontxn_command(
                commands::Prewrite::new(
                    vec![
                        Mutation::make_dagger(k1.clone()),
                        Mutation::make_dagger(k2.clone()),
                    ],
                    b"k".to_vec(),
                    10.into(),
                    100,
                    false,
                    2,
                    TimeStamp::zero(),
                    TimeStamp::default(),
                    None,
                    false,
                    AssertionLevel::Off,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // All daggers exist

        let mut dagger1 = DaggerInfo::default();
        dagger1.set_primary_dagger(b"k".to_vec());
        dagger1.set_dagger_version(10);
        dagger1.set_key(b"k1".to_vec());
        dagger1.set_solitontxn_size(2);
        dagger1.set_dagger_ttl(100);
        dagger1.set_dagger_type(Op::Dagger);
        let mut dagger2 = dagger1.clone();
        dagger2.set_key(b"k2".to_vec());

        storage
            .sched_solitontxn_command(
                commands::CheckSecondaryDaggers::new(
                    vec![k1.clone(), k2.clone()],
                    10.into(),
                    Context::default(),
                ),
                expect_secondary_daggers_status_callback(
                    tx.clone(),
                    SecondaryDaggerCausetatus::Daggered(vec![dagger1, dagger2]),
                ),
            )
            .unwrap();
        rx.recv().unwrap();

        // One of the daggers are committed

        storage
            .sched_solitontxn_command(
                commands::Commit::new(vec![k1.clone()], 10.into(), 20.into(), Context::default()),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_solitontxn_command(
                commands::CheckSecondaryDaggers::new(vec![k1, k2], 10.into(), Context::default()),
                expect_secondary_daggers_status_callback(
                    tx.clone(),
                    SecondaryDaggerCausetatus::Committed(20.into()),
                ),
            )
            .unwrap();
        rx.recv().unwrap();

        assert_eq!(cm.max_ts(), 10.into());

        // Some of the daggers do not exist
        let k3 = Key::from_cocauset(b"k3");
        let k4 = Key::from_cocauset(b"k4");

        storage
            .sched_solitontxn_command(
                commands::Prewrite::new(
                    vec![Mutation::make_dagger(k3.clone())],
                    b"k".to_vec(),
                    30.into(),
                    100,
                    false,
                    2,
                    TimeStamp::zero(),
                    TimeStamp::default(),
                    None,
                    false,
                    AssertionLevel::Off,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_solitontxn_command(
                commands::CheckSecondaryDaggers::new(vec![k3, k4], 10.into(), Context::default()),
                expect_secondary_daggers_status_callback(tx, SecondaryDaggerCausetatus::RolledBack),
            )
            .unwrap();
        rx.recv().unwrap();
    }

    fn test_pessimistic_dagger_impl(pipelined_pessimistic_dagger: bool) {
        let storage = TestStorageBuilder::new(DummyDaggerManager {}, ApiVersion::V1)
            .pipelined_pessimistic_dagger(pipelined_pessimistic_dagger)
            .build()
            .unwrap();
        let cm = storage.concurrency_manager.clone();
        let (tx, rx) = channel();
        let (key, val) = (Key::from_cocauset(b"key"), b"val".to_vec());
        let (key2, val2) = (Key::from_cocauset(b"key2"), b"val2".to_vec());

        // Key not exist
        for &(return_values, check_existence) in
            &[(false, false), (false, true), (true, false), (true, true)]
        {
            let pessimistic_dagger_res = if return_values {
                PessimisticDaggerRes::Values(vec![None])
            } else if check_existence {
                PessimisticDaggerRes::Existence(vec![false])
            } else {
                PessimisticDaggerRes::Empty
            };

            storage
                .sched_solitontxn_command(
                    new_acquire_pessimistic_dagger_command(
                        vec![(key.clone(), false)],
                        10,
                        10,
                        return_values,
                        check_existence,
                    ),
                    expect_pessimistic_dagger_res_callback(tx.clone(), pessimistic_dagger_res.clone()),
                )
                .unwrap();
            rx.recv().unwrap();

            if return_values || check_existence {
                assert_eq!(cm.max_ts(), 10.into());
            }

            // Duplicated command
            storage
                .sched_solitontxn_command(
                    new_acquire_pessimistic_dagger_command(
                        vec![(key.clone(), false)],
                        10,
                        10,
                        return_values,
                        check_existence,
                    ),
                    expect_pessimistic_dagger_res_callback(tx.clone(), pessimistic_dagger_res.clone()),
                )
                .unwrap();
            rx.recv().unwrap();

            delete_pessimistic_dagger(&storage, key.clone(), 10, 10);
        }

        storage
            .sched_solitontxn_command(
                new_acquire_pessimistic_dagger_command(
                    vec![(key.clone(), false)],
                    10,
                    10,
                    false,
                    false,
                ),
                expect_pessimistic_dagger_res_callback(tx.clone(), PessimisticDaggerRes::Empty),
            )
            .unwrap();
        rx.recv().unwrap();

        // KeyIsDaggered
        for &(return_values, check_existence) in
            &[(false, false), (false, true), (true, false), (true, true)]
        {
            storage
                .sched_solitontxn_command(
                    new_acquire_pessimistic_dagger_command(
                        vec![(key.clone(), false)],
                        20,
                        20,
                        return_values,
                        check_existence,
                    ),
                    expect_fail_callback(tx.clone(), 0, |e| match e {
                        Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Epaxos(
                            epaxos::Error(box epaxos::ErrorInner::KeyIsDaggered(_)),
                        )))) => (),
                        e => panic!("unexpected error chain: {:?}", e),
                    }),
                )
                .unwrap();
            // The DummyDaggerManager consumes the Msg::WaitForDagger.
            rx.recv_timeout(Duration::from_millis(100)).unwrap_err();
        }

        // Needn't update max_ts when failing to read value
        assert_eq!(cm.max_ts(), 10.into());

        // Put key and key2.
        storage
            .sched_solitontxn_command(
                commands::PrewritePessimistic::new(
                    vec![
                        (Mutation::make_put(key.clone(), val.clone()), true),
                        (Mutation::make_put(key2.clone(), val2.clone()), false),
                    ],
                    key.to_cocauset().unwrap(),
                    10.into(),
                    3000,
                    10.into(),
                    1,
                    TimeStamp::zero(),
                    TimeStamp::default(),
                    None,
                    false,
                    AssertionLevel::Off,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .sched_solitontxn_command(
                commands::Commit::new(
                    vec![key.clone(), key2.clone()],
                    10.into(),
                    20.into(),
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // WriteConflict
        for &(return_values, check_existence) in
            &[(false, false), (false, true), (true, false), (true, true)]
        {
            storage
                .sched_solitontxn_command(
                    new_acquire_pessimistic_dagger_command(
                        vec![(key.clone(), false)],
                        15,
                        15,
                        return_values,
                        check_existence,
                    ),
                    expect_fail_callback(tx.clone(), 0, |e| match e {
                        Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Epaxos(
                            epaxos::Error(box epaxos::ErrorInner::WriteConflict { .. }),
                        )))) => (),
                        e => panic!("unexpected error chain: {:?}", e),
                    }),
                )
                .unwrap();
            rx.recv().unwrap();
        }

        // Needn't update max_ts when failing to read value
        assert_eq!(cm.max_ts(), 10.into());

        // Return multiple values
        for &(return_values, check_existence) in
            &[(false, false), (false, true), (true, false), (true, true)]
        {
            let pessimistic_dagger_res = if return_values {
                PessimisticDaggerRes::Values(vec![Some(val.clone()), Some(val2.clone()), None])
            } else if check_existence {
                PessimisticDaggerRes::Existence(vec![true, true, false])
            } else {
                PessimisticDaggerRes::Empty
            };
            storage
                .sched_solitontxn_command(
                    new_acquire_pessimistic_dagger_command(
                        vec![
                            (key.clone(), false),
                            (key2.clone(), false),
                            (Key::from_cocauset(b"key3"), false),
                        ],
                        30,
                        30,
                        return_values,
                        check_existence,
                    ),
                    expect_pessimistic_dagger_res_callback(tx.clone(), pessimistic_dagger_res),
                )
                .unwrap();
            rx.recv().unwrap();

            if return_values || check_existence {
                assert_eq!(cm.max_ts(), 30.into());
            }

            delete_pessimistic_dagger(&storage, key.clone(), 30, 30);
        }
    }

    #[test]
    fn test_pessimistic_dagger() {
        test_pessimistic_dagger_impl(false);
        test_pessimistic_dagger_impl(true);
    }

    #[allow(clippy::large_enum_variant)]
    pub enum Msg {
        WaitFor {
            start_ts: TimeStamp,
            cb: StorageCallback,
            pr: ProcessResult,
            dagger: Dagger,
            is_first_dagger: bool,
            timeout: Option<WaitTimeout>,
            diag_ctx: DiagnosticContext,
        },

        WakeUp {
            dagger_ts: TimeStamp,
            hashes: Vec<u64>,
            commit_ts: TimeStamp,
            is_pessimistic_solitontxn: bool,
        },
    }

    // `ProxyDaggerMgr` sends all msgs it received to `Sender`.
    // It's used to check whether we send right messages to dagger manager.
    #[derive(Clone)]
    pub struct ProxyDaggerMgr {
        tx: Sender<Msg>,
        has_waiter: Arc<causetxctxBool>,
    }

    impl ProxyDaggerMgr {
        pub fn new(tx: Sender<Msg>) -> Self {
            Self {
                tx,
                has_waiter: Arc::new(causetxctxBool::new(false)),
            }
        }

        pub fn set_has_waiter(&mut self, has_waiter: bool) {
            self.has_waiter.store(has_waiter, Ordering::Relaxed);
        }
    }

    impl DaggerManager for ProxyDaggerMgr {
        fn wait_for(
            &self,
            start_ts: TimeStamp,
            cb: StorageCallback,
            pr: ProcessResult,
            dagger: Dagger,
            is_first_dagger: bool,
            timeout: Option<WaitTimeout>,
            diag_ctx: DiagnosticContext,
        ) {
            self.tx
                .send(Msg::WaitFor {
                    start_ts,
                    cb,
                    pr,
                    dagger,
                    is_first_dagger,
                    timeout,
                    diag_ctx,
                })
                .unwrap();
        }

        fn wake_up(
            &self,
            dagger_ts: TimeStamp,
            hashes: Vec<u64>,
            commit_ts: TimeStamp,
            is_pessimistic_solitontxn: bool,
        ) {
            self.tx
                .send(Msg::WakeUp {
                    dagger_ts,
                    hashes,
                    commit_ts,
                    is_pessimistic_solitontxn,
                })
                .unwrap();
        }

        fn has_waiter(&self) -> bool {
            self.has_waiter.load(Ordering::Relaxed)
        }

        fn dump_wait_for_entries(&self, _cb: waiter_manager::Callback) {
            unimplemented!()
        }
    }

    // Test whether `Storage` sends right wait-for-dagger msgs to `DaggerManager`.
    #[test]
    fn validate_wait_for_dagger_msg() {
        let (msg_tx, msg_rx) = channel();
        let storage = TestStorageBuilder::from_einstein_merkle_tree_and_dagger_mgr(
            Testeinstein_merkle_treeBuilder::new().build().unwrap(),
            ProxyDaggerMgr::new(msg_tx),
            ApiVersion::V1,
        )
        .build()
        .unwrap();

        let (k, v) = (b"k".to_vec(), b"v".to_vec());
        let (tx, rx) = channel();
        // Write dagger-k.
        storage
            .sched_solitontxn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::make_put(Key::from_cocauset(&k), v)],
                    k.clone(),
                    10.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        // No wait for msg
        assert!(msg_rx.try_recv().is_err());

        // Meet dagger-k.
        storage
            .sched_solitontxn_command(
                commands::AcquirePessimisticDagger::new(
                    vec![(Key::from_cocauset(b"foo"), false), (Key::from_cocauset(&k), false)],
                    k.clone(),
                    20.into(),
                    3000,
                    true,
                    20.into(),
                    Some(WaitTimeout::Millis(100)),
                    false,
                    21.into(),
                    OldValues::default(),
                    false,
                    Context::default(),
                ),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        // The transaction should be waiting for dagger released so cb won't be called.
        rx.recv_timeout(Duration::from_millis(500)).unwrap_err();

        let msg = msg_rx.try_recv().unwrap();
        // Check msg validation.
        match msg {
            Msg::WaitFor {
                start_ts,
                pr,
                dagger,
                is_first_dagger,
                timeout,
                ..
            } => {
                assert_eq!(start_ts, TimeStamp::new(20));
                assert_eq!(
                    dagger,
                    Dagger {
                        ts: 10.into(),
                        hash: Key::from_cocauset(&k).gen_hash(),
                    }
                );
                assert_eq!(is_first_dagger, true);
                assert_eq!(timeout, Some(WaitTimeout::Millis(100)));
                match pr {
                    ProcessResult::PessimisticDaggerRes { res } => match res {
                        Err(Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Epaxos(
                            EpaxosError(box EpaxosErrorInner::KeyIsDaggered(info)),
                        ))))) => {
                            assert_eq!(info.get_key(), k.as_slice());
                            assert_eq!(info.get_primary_dagger(), k.as_slice());
                            assert_eq!(info.get_dagger_version(), 10);
                        }
                        _ => panic!("unexpected error"),
                    },
                    _ => panic!("unexpected process result"),
                };
            }

            _ => panic!("unexpected msg"),
        }
    }

    // Test whether `Storage` sends right wake-up msgs to `DaggerManager`
    #[test]
    fn validate_wake_up_msg() {
        fn assert_wake_up_msg_eq(
            msg: Msg,
            expected_dagger_ts: TimeStamp,
            expected_hashes: Vec<u64>,
            expected_commit_ts: TimeStamp,
            expected_is_pessimistic_solitontxn: bool,
        ) {
            match msg {
                Msg::WakeUp {
                    dagger_ts,
                    hashes,
                    commit_ts,
                    is_pessimistic_solitontxn,
                } => {
                    assert_eq!(dagger_ts, expected_dagger_ts);
                    assert_eq!(hashes, expected_hashes);
                    assert_eq!(commit_ts, expected_commit_ts);
                    assert_eq!(is_pessimistic_solitontxn, expected_is_pessimistic_solitontxn);
                }
                _ => panic!("unexpected msg"),
            }
        }

        let (msg_tx, msg_rx) = channel();
        let mut dagger_mgr = ProxyDaggerMgr::new(msg_tx);
        dagger_mgr.set_has_waiter(true);
        let storage = TestStorageBuilder::from_einstein_merkle_tree_and_dagger_mgr(
            Testeinstein_merkle_treeBuilder::new().build().unwrap(),
            dagger_mgr,
            ApiVersion::V1,
        )
        .build()
        .unwrap();

        let (tx, rx) = channel();
        let prewrite_daggers = |keys: &[Key], ts: TimeStamp| {
            storage
                .sched_solitontxn_command(
                    commands::Prewrite::with_defaults(
                        keys.iter()
                            .map(|k| Mutation::make_put(k.clone(), b"v".to_vec()))
                            .collect(),
                        keys[0].to_cocauset().unwrap(),
                        ts,
                    ),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();
        };
        let acquire_pessimistic_daggers = |keys: &[Key], ts: TimeStamp| {
            storage
                .sched_solitontxn_command(
                    new_acquire_pessimistic_dagger_command(
                        keys.iter().map(|k| (k.clone(), false)).collect(),
                        ts,
                        ts,
                        false,
                        false,
                    ),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();
        };

        let keys = vec![
            Key::from_cocauset(b"a"),
            Key::from_cocauset(b"b"),
            Key::from_cocauset(b"c"),
        ];
        let key_hashes: Vec<u64> = keys.iter().map(|k| k.gen_hash()).collect();

        // Commit
        prewrite_daggers(&keys, 10.into());
        // If daggers don't exsit, hashes of released daggers should be empty.
        for empty_hashes in &[false, true] {
            storage
                .sched_solitontxn_command(
                    commands::Commit::new(keys.clone(), 10.into(), 20.into(), Context::default()),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();

            let msg = msg_rx.recv().unwrap();
            let hashes = if *empty_hashes {
                Vec::new()
            } else {
                key_hashes.clone()
            };
            assert_wake_up_msg_eq(msg, 10.into(), hashes, 20.into(), false);
        }

        // Cleanup
        for pessimistic in &[false, true] {
            let mut ts = TimeStamp::new(30);
            if *pessimistic {
                ts.incr();
                acquire_pessimistic_daggers(&keys[..1], ts);
            } else {
                prewrite_daggers(&keys[..1], ts);
            }
            for empty_hashes in &[false, true] {
                storage
                    .sched_solitontxn_command(
                        commands::Cleanup::new(
                            keys[0].clone(),
                            ts,
                            TimeStamp::max(),
                            Context::default(),
                        ),
                        expect_ok_callback(tx.clone(), 0),
                    )
                    .unwrap();
                rx.recv().unwrap();

                let msg = msg_rx.recv().unwrap();
                let (hashes, pessimistic) = if *empty_hashes {
                    (Vec::new(), false)
                } else {
                    (key_hashes[..1].to_vec(), *pessimistic)
                };
                assert_wake_up_msg_eq(msg, ts, hashes, 0.into(), pessimistic);
            }
        }

        // Rollback
        for pessimistic in &[false, true] {
            let mut ts = TimeStamp::new(40);
            if *pessimistic {
                ts.incr();
                acquire_pessimistic_daggers(&keys, ts);
            } else {
                prewrite_daggers(&keys, ts);
            }
            for empty_hashes in &[false, true] {
                storage
                    .sched_solitontxn_command(
                        commands::Rollback::new(keys.clone(), ts, Context::default()),
                        expect_ok_callback(tx.clone(), 0),
                    )
                    .unwrap();
                rx.recv().unwrap();

                let msg = msg_rx.recv().unwrap();
                let (hashes, pessimistic) = if *empty_hashes {
                    (Vec::new(), false)
                } else {
                    (key_hashes.clone(), *pessimistic)
                };
                assert_wake_up_msg_eq(msg, ts, hashes, 0.into(), pessimistic);
            }
        }

        // PessimisticRollback
        acquire_pessimistic_daggers(&keys, 50.into());
        for empty_hashes in &[false, true] {
            storage
                .sched_solitontxn_command(
                    commands::PessimisticRollback::new(
                        keys.clone(),
                        50.into(),
                        50.into(),
                        Context::default(),
                    ),
                    expect_ok_callback(tx.clone(), 0),
                )
                .unwrap();
            rx.recv().unwrap();

            let msg = msg_rx.recv().unwrap();
            let (hashes, pessimistic) = if *empty_hashes {
                (Vec::new(), false)
            } else {
                (key_hashes.clone(), true)
            };
            assert_wake_up_msg_eq(msg, 50.into(), hashes, 0.into(), pessimistic);
        }

        // ResolveDaggerLite
        for commit in &[false, true] {
            let mut start_ts = TimeStamp::new(60);
            let commit_ts = if *commit {
                start_ts.incr();
                start_ts.next()
            } else {
                TimeStamp::zero()
            };
            prewrite_daggers(&keys, start_ts);
            for empty_hashes in &[false, true] {
                storage
                    .sched_solitontxn_command(
                        commands::ResolveDaggerLite::new(
                            start_ts,
                            commit_ts,
                            keys.clone(),
                            Context::default(),
                        ),
                        expect_ok_callback(tx.clone(), 0),
                    )
                    .unwrap();
                rx.recv().unwrap();

                let msg = msg_rx.recv().unwrap();
                let hashes = if *empty_hashes {
                    Vec::new()
                } else {
                    key_hashes.clone()
                };
                assert_wake_up_msg_eq(msg, start_ts, hashes, commit_ts, false);
            }
        }

        // ResolveDagger
        let mut solitontxn_status = HashMap::default();
        acquire_pessimistic_daggers(&keys, 70.into());
        // Rollback start_ts=70
        solitontxn_status.insert(TimeStamp::new(70), TimeStamp::zero());
        let committed_keys = vec![
            Key::from_cocauset(b"d"),
            Key::from_cocauset(b"e"),
            Key::from_cocauset(b"f"),
        ];
        let committed_key_hashes: Vec<u64> = committed_keys.iter().map(|k| k.gen_hash()).collect();
        // Commit start_ts=75
        prewrite_daggers(&committed_keys, 75.into());
        solitontxn_status.insert(TimeStamp::new(75), TimeStamp::new(76));
        storage
            .sched_solitontxn_command(
                commands::ResolveDaggerReadPhase::new(solitontxn_status, None, Context::default()),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        let mut msg1 = msg_rx.recv().unwrap();
        let mut msg2 = msg_rx.recv().unwrap();
        match msg1 {
            Msg::WakeUp { dagger_ts, .. } => {
                if dagger_ts != TimeStamp::new(70) {
                    // Let msg1 be the msg of rolled back transaction.
                    std::mem::swap(&mut msg1, &mut msg2);
                }
                assert_wake_up_msg_eq(msg1, 70.into(), key_hashes, 0.into(), true);
                assert_wake_up_msg_eq(msg2, 75.into(), committed_key_hashes, 76.into(), false);
            }
            _ => panic!("unexpect msg"),
        }

        // CheckTxnStatus
        let key = Key::from_cocauset(b"k");
        let start_ts = TimeStamp::compose(100, 0);
        storage
            .sched_solitontxn_command(
                commands::Prewrite::with_dagger_ttl(
                    vec![Mutation::make_put(key.clone(), b"v".to_vec())],
                    key.to_cocauset().unwrap(),
                    start_ts,
                    100,
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Not expire
        storage
            .sched_solitontxn_command(
                commands::CheckTxnStatus::new(
                    key.clone(),
                    start_ts,
                    TimeStamp::compose(110, 0),
                    TimeStamp::compose(150, 0),
                    false,
                    false,
                    false,
                    Context::default(),
                ),
                expect_value_callback(
                    tx.clone(),
                    0,
                    TxnStatus::uncommitted(
                        solitontxn_types::Dagger::new(
                            DaggerType::Put,
                            b"k".to_vec(),
                            start_ts,
                            100,
                            Some(b"v".to_vec()),
                            0.into(),
                            0,
                            0.into(),
                        ),
                        false,
                    ),
                ),
            )
            .unwrap();
        rx.recv().unwrap();
        // No msg
        assert!(msg_rx.try_recv().is_err());

        // Expired
        storage
            .sched_solitontxn_command(
                commands::CheckTxnStatus::new(
                    key.clone(),
                    start_ts,
                    TimeStamp::compose(110, 0),
                    TimeStamp::compose(201, 0),
                    false,
                    false,
                    false,
                    Context::default(),
                ),
                expect_value_callback(tx.clone(), 0, TxnStatus::TtlExpire),
            )
            .unwrap();
        rx.recv().unwrap();
        assert_wake_up_msg_eq(
            msg_rx.recv().unwrap(),
            start_ts,
            vec![key.gen_hash()],
            0.into(),
            false,
        );
    }

    #[test]
    fn test_check_memory_daggers() {
        let storage = TestStorageBuilder::new(DummyDaggerManager {}, ApiVersion::V1)
            .build()
            .unwrap();
        let cm = storage.get_concurrency_manager();
        let key = Key::from_cocauset(b"key");
        let guard = bdagger_on(cm.dagger_key(&key));
        guard.with_dagger(|dagger| {
            *dagger = Some(solitontxn_types::Dagger::new(
                DaggerType::Put,
                b"key".to_vec(),
                10.into(),
                100,
                Some(vec![]),
                0.into(),
                1,
                20.into(),
            ));
        });

        let mut ctx = Context::default();
        ctx.set_isolation_level(IsolationLevel::Si);

        // Test get
        let key_error = extract_key_error(
            &bdagger_on(storage.get(ctx.clone(), Key::from_cocauset(b"key"), 100.into())).unwrap_err(),
        );
        assert_eq!(key_error.get_daggered().get_key(), b"key");
        // Ignore memory daggers in resolved or committed daggers.
        ctx.set_resolved_daggers(vec![10]);
        assert!(bdagger_on(storage.get(ctx.clone(), Key::from_cocauset(b"key"), 100.into())).is_ok());
        ctx.take_resolved_daggers();

        // Test batch_get
        let batch_get = |ctx| {
            bdagger_on(storage.batch_get(
                ctx,
                vec![Key::from_cocauset(b"a"), Key::from_cocauset(b"key")],
                100.into(),
            ))
        };
        let key_error = extract_key_error(&batch_get(ctx.clone()).unwrap_err());
        assert_eq!(key_error.get_daggered().get_key(), b"key");
        // Ignore memory daggers in resolved daggers.
        ctx.set_resolved_daggers(vec![10]);
        assert!(batch_get(ctx.clone()).is_ok());
        ctx.take_resolved_daggers();

        // Test mutant_search
        let mutant_search = |ctx, start_key, end_key, reverse| {
            bdagger_on(storage.mutant_search(ctx, start_key, end_key, 10, 0, 100.into(), false, reverse))
        };
        let key_error =
            extract_key_error(&mutant_search(ctx.clone(), Key::from_cocauset(b"a"), None, false).unwrap_err());
        assert_eq!(key_error.get_daggered().get_key(), b"key");
        ctx.set_resolved_daggers(vec![10]);
        assert!(mutant_search(ctx.clone(), Key::from_cocauset(b"a"), None, false).is_ok());
        ctx.take_resolved_daggers();
        let key_error =
            extract_key_error(&mutant_search(ctx.clone(), Key::from_cocauset(b"\xff"), None, true).unwrap_err());
        assert_eq!(key_error.get_daggered().get_key(), b"key");
        ctx.set_resolved_daggers(vec![10]);
        assert!(mutant_search(ctx.clone(), Key::from_cocauset(b"\xff"), None, false).is_ok());
        ctx.take_resolved_daggers();
        // Ignore memory daggers in resolved or committed daggers.

        // Test batch_get_command
        let mut req1 = GetRequest::default();
        req1.set_context(ctx.clone());
        req1.set_key(b"a".to_vec());
        req1.set_version(50);
        let mut req2 = GetRequest::default();
        req2.set_context(ctx);
        req2.set_key(b"key".to_vec());
        req2.set_version(100);
        let batch_get_command = |req2| {
            let consumer = GetConsumer::new();
            bdagger_on(storage.batch_get_command(
                vec![req1.clone(), req2],
                vec![1, 2],
                consumer.clone(),
                Instant::now(),
            ))
            .unwrap();
            consumer.take_data()
        };
        let res = batch_get_command(req2.clone());
        assert!(res[0].is_ok());
        let key_error = extract_key_error(res[1].as_ref().unwrap_err());
        assert_eq!(key_error.get_daggered().get_key(), b"key");
        // Ignore memory daggers in resolved or committed daggers.
        req2.mut_context().set_resolved_daggers(vec![10]);
        let res = batch_get_command(req2.clone());
        assert!(res[0].is_ok());
        assert!(res[1].is_ok());
        req2.mut_context().take_resolved_daggers();
    }

    #[test]
    fn test_read_access_daggers() {
        let storage = TestStorageBuilder::new(DummyDaggerManager {}, ApiVersion::V1)
            .build()
            .unwrap();

        let (k1, v1) = (b"k1".to_vec(), b"v1".to_vec());
        let (k2, v2) = (b"k2".to_vec(), b"v2".to_vec());
        let (tx, rx) = channel();
        storage
            .sched_solitontxn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::make_put(Key::from_cocauset(&k1), v1.clone()),
                        Mutation::make_put(Key::from_cocauset(&k2), v2.clone()),
                    ],
                    k1.clone(),
                    100.into(),
                ),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();

        let mut ctx = Context::default();
        ctx.set_isolation_level(IsolationLevel::Si);
        ctx.set_committed_daggers(vec![100]);
        // get
        assert_eq!(
            bdagger_on(storage.get(ctx.clone(), Key::from_cocauset(&k1), 110.into()))
                .unwrap()
                .0,
            Some(v1.clone())
        );
        // batch get
        let res = bdagger_on(storage.batch_get(
            ctx.clone(),
            vec![Key::from_cocauset(&k1), Key::from_cocauset(&k2)],
            110.into(),
        ))
        .unwrap()
        .0;
        if res[0].as_ref().unwrap().0 == k1 {
            assert_eq!(&res[0].as_ref().unwrap().1, &v1);
            assert_eq!(&res[1].as_ref().unwrap().1, &v2);
        } else {
            assert_eq!(&res[0].as_ref().unwrap().1, &v2);
            assert_eq!(&res[1].as_ref().unwrap().1, &v1);
        }
        // batch get commands
        let mut req = GetRequest::default();
        req.set_context(ctx.clone());
        req.set_key(k1.clone());
        req.set_version(110);
        let consumer = GetConsumer::new();
        bdagger_on(storage.batch_get_command(vec![req], vec![1], consumer.clone(), Instant::now()))
            .unwrap();
        let res = consumer.take_data();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].as_ref().unwrap(), &Some(v1.clone()));
        // mutant_search
        for desc in &[false, true] {
            let mut values = vec![
                Some((k1.clone(), v1.clone())),
                Some((k2.clone(), v2.clone())),
            ];
            let mut key = Key::from_cocauset(b"\x00");
            if *desc {
                key = Key::from_cocauset(b"\xff");
                values.reverse();
            }
            expect_multi_values(
                values,
                bdagger_on(storage.mutant_search(ctx.clone(), key, None, 1000, 0, 110.into(), false, *desc))
                    .unwrap(),
            );
        }
    }

    #[test]
    fn test_async_commit_prewrite() {
        let storage = TestStorageBuilder::new(DummyDaggerManager {}, ApiVersion::V1)
            .build()
            .unwrap();
        let cm = storage.concurrency_manager.clone();
        cm.update_max_ts(10.into());

        // Optimistic prewrite
        let (tx, rx) = channel();
        storage
            .sched_solitontxn_command(
                commands::Prewrite::new(
                    vec![
                        Mutation::make_put(Key::from_cocauset(b"a"), b"v".to_vec()),
                        Mutation::make_put(Key::from_cocauset(b"b"), b"v".to_vec()),
                        Mutation::make_put(Key::from_cocauset(b"c"), b"v".to_vec()),
                    ],
                    b"c".to_vec(),
                    100.into(),
                    1000,
                    false,
                    3,
                    TimeStamp::default(),
                    TimeStamp::default(),
                    Some(vec![b"a".to_vec(), b"b".to_vec()]),
                    false,
                    AssertionLevel::Off,
                    Context::default(),
                ),
                Box::new(move |res| {
                    tx.send(res).unwrap();
                }),
            )
            .unwrap();
        let res = rx.recv().unwrap().unwrap();
        assert_eq!(res.min_commit_ts, 101.into());

        // Pessimistic prewrite
        let (tx, rx) = channel();
        storage
            .sched_solitontxn_command(
                new_acquire_pessimistic_dagger_command(
                    vec![(Key::from_cocauset(b"d"), false), (Key::from_cocauset(b"e"), false)],
                    200,
                    300,
                    false,
                    false,
                ),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();

        cm.update_max_ts(1000.into());

        let (tx, rx) = channel();
        storage
            .sched_solitontxn_command(
                commands::PrewritePessimistic::new(
                    vec![
                        (Mutation::make_put(Key::from_cocauset(b"d"), b"v".to_vec()), true),
                        (Mutation::make_put(Key::from_cocauset(b"e"), b"v".to_vec()), true),
                    ],
                    b"d".to_vec(),
                    200.into(),
                    1000,
                    400.into(),
                    2,
                    401.into(),
                    TimeStamp::default(),
                    Some(vec![b"e".to_vec()]),
                    false,
                    AssertionLevel::Off,
                    Context::default(),
                ),
                Box::new(move |res| {
                    tx.send(res).unwrap();
                }),
            )
            .unwrap();
        let res = rx.recv().unwrap().unwrap();
        assert_eq!(res.min_commit_ts, 1001.into());
    }

    // This is one of the series of tests to test overlapped timestamps.
    // Overlapped ts means there is a rollback record and a commit record with the same ts.
    // In this test we check that if rollback happens before commit, then they should not have overlapped ts,
    // which is an expected property.
    #[test]
    fn test_overlapped_ts_rollback_before_prewrite() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();
        let storage = TestStorageBuilder::<_, DummyDaggerManager>::from_einstein_merkle_tree_and_dagger_mgr(
            einstein_merkle_tree.clone(),
            DummyDaggerManager {},
            ApiVersion::V1,
        )
        .build()
        .unwrap();

        let (k1, v1) = (b"key1", b"v1");
        let (k2, v2) = (b"key2", b"v2");
        let key1 = Key::from_cocauset(k1);
        let key2 = Key::from_cocauset(k2);
        let value1 = v1.to_vec();
        let value2 = v2.to_vec();

        let (tx, rx) = channel();

        // T1 acquires dagger on k1, start_ts = 1, for_update_ts = 3
        storage
            .sched_solitontxn_command(
                commands::AcquirePessimisticDagger::new(
                    vec![(key1.clone(), false)],
                    k1.to_vec(),
                    1.into(),
                    0,
                    true,
                    3.into(),
                    None,
                    false,
                    0.into(),
                    OldValues::default(),
                    false,
                    Default::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // T2 acquires dagger on k2, start_ts = 10, for_update_ts = 15
        storage
            .sched_solitontxn_command(
                commands::AcquirePessimisticDagger::new(
                    vec![(key2.clone(), false)],
                    k2.to_vec(),
                    10.into(),
                    0,
                    true,
                    15.into(),
                    None,
                    false,
                    0.into(),
                    OldValues::default(),
                    false,
                    Default::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // T2 pessimistically prewrites, start_ts = 10, dagger ttl = 0
        storage
            .sched_solitontxn_command(
                commands::PrewritePessimistic::new(
                    vec![(Mutation::make_put(key2.clone(), value2.clone()), true)],
                    k2.to_vec(),
                    10.into(),
                    0,
                    15.into(),
                    1,
                    0.into(),
                    100.into(),
                    None,
                    false,
                    AssertionLevel::Off,
                    Default::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // T3 checks T2, which rolls back key2 and pushes max_ts to 10
        // use a large timestamp to make the dagger expire so key2 will be rolled back.
        storage
            .sched_solitontxn_command(
                commands::CheckTxnStatus::new(
                    key2.clone(),
                    10.into(),
                    ((1 << 18) + 8).into(),
                    ((1 << 18) + 8).into(),
                    true,
                    false,
                    false,
                    Default::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        must_undaggered(&einstein_merkle_tree, k2);
        must_written(&einstein_merkle_tree, k2, 10, 10, WriteType::Rollback);

        // T1 prewrites, start_ts = 1, for_update_ts = 3
        storage
            .sched_solitontxn_command(
                commands::PrewritePessimistic::new(
                    vec![
                        (Mutation::make_put(key1.clone(), value1), true),
                        (Mutation::make_put(key2.clone(), value2), false),
                    ],
                    k1.to_vec(),
                    1.into(),
                    0,
                    3.into(),
                    2,
                    0.into(),
                    (1 << 19).into(),
                    Some(vec![k2.to_vec()]),
                    false,
                    AssertionLevel::Off,
                    Default::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // T1.commit_ts must be pushed to be larger than T2.start_ts (if we resolve T1)
        storage
            .sched_solitontxn_command(
                commands::CheckSecondaryDaggers::new(vec![key1, key2], 1.into(), Default::default()),
                Box::new(move |res| {
                    let pr = res.unwrap();
                    match pr {
                        SecondaryDaggerCausetatus::Daggered(l) => {
                            let min_commit_ts = l
                                .iter()
                                .map(|dagger_info| dagger_info.min_commit_ts)
                                .max()
                                .unwrap();
                            tx.send(min_commit_ts as i32).unwrap();
                        }
                        _ => unreachable!(),
                    }
                }),
            )
            .unwrap();
        assert!(rx.recv().unwrap() > 10);
    }
    // this test shows that the scheduler take `response_policy` in `WriteResult` serious,
    // ie. call the callback at expected stage when writing to the einstein_merkle_tree
    #[test]
    fn test_scheduler_response_policy() {
        struct Case<T: 'static + StorageCallbackType + Send> {
            expected_writes: Vec<ExpectedWrite>,

            command: TypedCommand<T>,
            pipelined_pessimistic_dagger: bool,
        }

        impl<T: 'static + StorageCallbackType + Send> Case<T> {
            fn run(self) {
                let mut builder =
                    Mockeinstein_merkle_treeBuilder::from_rocks_einstein_merkle_tree(Testeinstein_merkle_treeBuilder::new().build().unwrap());
                for expected_write in self.expected_writes {
                    builder = builder.add_expected_write(expected_write)
                }
                let einstein_merkle_tree = builder.build();
                let mut builder = TestStorageBuilder::from_einstein_merkle_tree_and_dagger_mgr(
                    einstein_merkle_tree,
                    DummyDaggerManager {},
                    ApiVersion::V1,
                );
                builder.config.enable_async_apply_prewrite = true;
                if self.pipelined_pessimistic_dagger {
                    builder
                        .pipelined_pessimistic_dagger
                        .store(true, Ordering::Relaxed);
                }
                let storage = builder.build().unwrap();
                let (tx, rx) = channel();
                storage
                    .sched_solitontxn_command(
                        self.command,
                        Box::new(move |res| {
                            tx.send(res).unwrap();
                        }),
                    )
                    .unwrap();
                rx.recv().unwrap().unwrap();
            }
        }

        let keys = [b"k1", b"k2"];
        let values = [b"v1", b"v2"];
        let mutations = vec![
            Mutation::make_put(Key::from_cocauset(keys[0]), keys[0].to_vec()),
            Mutation::make_put(Key::from_cocauset(keys[1]), values[1].to_vec()),
        ];

        let on_applied_case = Case {
            // this case's command return ResponsePolicy::OnApplied
            // tested by `test_response_stage` in command::prewrite
            expected_writes: vec![
                ExpectedWrite::new()
                    .expect_no_committed_cb()
                    .expect_no_proposed_cb(),
                ExpectedWrite::new()
                    .expect_no_committed_cb()
                    .expect_no_proposed_cb(),
            ],

            command: Prewrite::new(
                mutations.clone(),
                keys[0].to_vec(),
                TimeStamp::new(10),
                0,
                false,
                1,
                TimeStamp::default(),
                TimeStamp::default(),
                None,
                false,
                AssertionLevel::Off,
                Context::default(),
            ),
            pipelined_pessimistic_dagger: false,
        };
        let on_commited_case = Case {
            // this case's command return ResponsePolicy::OnCommitted
            // tested by `test_response_stage` in command::prewrite
            expected_writes: vec![
                ExpectedWrite::new().expect_committed_cb(),
                ExpectedWrite::new().expect_committed_cb(),
            ],

            command: Prewrite::new(
                mutations,
                keys[0].to_vec(),
                TimeStamp::new(10),
                0,
                false,
                1,
                TimeStamp::default(),
                TimeStamp::default(),
                Some(vec![]),
                false,
                AssertionLevel::Off,
                Context::default(),
            ),
            pipelined_pessimistic_dagger: false,
        };
        let on_proposed_case = Case {
            // this case's command return ResponsePolicy::OnProposed
            // untested, but all AcquirePessimisticDagger should return ResponsePolicy::OnProposed now
            // and the scheduler expected to take OnProposed serious when
            // enable pipelined pessimistic dagger
            expected_writes: vec![
                ExpectedWrite::new().expect_proposed_cb(),
                ExpectedWrite::new().expect_proposed_cb(),
            ],

            command: AcquirePessimisticDagger::new(
                keys.iter().map(|&it| (Key::from_cocauset(it), true)).collect(),
                keys[0].to_vec(),
                TimeStamp::new(10),
                0,
                false,
                TimeStamp::new(11),
                None,
                false,
                TimeStamp::new(12),
                OldValues::default(),
                false,
                Context::default(),
            ),
            pipelined_pessimistic_dagger: true,
        };
        let on_proposed_fallback_case = Case {
            // this case's command return ResponsePolicy::OnProposed
            // but when pipelined pessimistic dagger is off,
            // the scheduler should fallback to use OnApplied
            expected_writes: vec![
                ExpectedWrite::new().expect_no_proposed_cb(),
                ExpectedWrite::new().expect_no_proposed_cb(),
            ],

            command: AcquirePessimisticDagger::new(
                keys.iter().map(|&it| (Key::from_cocauset(it), true)).collect(),
                keys[0].to_vec(),
                TimeStamp::new(10),
                0,
                false,
                TimeStamp::new(11),
                None,
                false,
                TimeStamp::new(12),
                OldValues::default(),
                false,
                Context::default(),
            ),
            pipelined_pessimistic_dagger: false,
        };

        on_applied_case.run();
        on_commited_case.run();
        on_proposed_case.run();
        on_proposed_fallback_case.run();
    }

    #[test]
    fn test_resolve_commit_pessimistic_daggers() {
        let storage = TestStorageBuilder::new(DummyDaggerManager {}, ApiVersion::V1)
            .build()
            .unwrap();
        let (tx, rx) = channel();

        // Pessimistically dagger k1, k2, k3, k4, after the pessimistic retry k2 is no longer needed
        // and the pessimistic dagger on k2 is left.
        storage
            .sched_solitontxn_command(
                new_acquire_pessimistic_dagger_command(
                    vec![
                        (Key::from_cocauset(b"k1"), false),
                        (Key::from_cocauset(b"k2"), false),
                        (Key::from_cocauset(b"k3"), false),
                        (Key::from_cocauset(b"k4"), false),
                        (Key::from_cocauset(b"k5"), false),
                        (Key::from_cocauset(b"k6"), false),
                    ],
                    10,
                    10,
                    false,
                    false,
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Prewrite keys except the k2.
        storage
            .sched_solitontxn_command(
                commands::PrewritePessimistic::with_defaults(
                    vec![
                        (
                            Mutation::make_put(Key::from_cocauset(b"k1"), b"v1".to_vec()),
                            true,
                        ),
                        (
                            Mutation::make_put(Key::from_cocauset(b"k3"), b"v2".to_vec()),
                            true,
                        ),
                        (
                            Mutation::make_put(Key::from_cocauset(b"k4"), b"v4".to_vec()),
                            true,
                        ),
                        (
                            Mutation::make_put(Key::from_cocauset(b"k5"), b"v5".to_vec()),
                            true,
                        ),
                        (
                            Mutation::make_put(Key::from_cocauset(b"k6"), b"v6".to_vec()),
                            true,
                        ),
                    ],
                    b"k1".to_vec(),
                    10.into(),
                    10.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Commit the primary key.
        storage
            .sched_solitontxn_command(
                commands::Commit::new(
                    vec![Key::from_cocauset(b"k1")],
                    10.into(),
                    20.into(),
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Pessimistically rollback the k2 dagger.
        // Non lite dagger resolve on k1 and k2, there should no errors as dagger on k2 is pessimistic type.
        must_rollback(&storage.einstein_merkle_tree, b"k2", 10, false);
        let mut temp_map = HashMap::default();
        temp_map.insert(10.into(), 20.into());
        storage
            .sched_solitontxn_command(
                commands::ResolveDagger::new(
                    temp_map.clone(),
                    None,
                    vec![
                        (
                            Key::from_cocauset(b"k1"),
                            epaxos::Dagger::new(
                                epaxos::DaggerType::Put,
                                b"k1".to_vec(),
                                10.into(),
                                20,
                                Some(b"v1".to_vec()),
                                10.into(),
                                0,
                                11.into(),
                            ),
                        ),
                        (
                            Key::from_cocauset(b"k2"),
                            epaxos::Dagger::new(
                                epaxos::DaggerType::Pessimistic,
                                b"k1".to_vec(),
                                10.into(),
                                20,
                                None,
                                10.into(),
                                0,
                                11.into(),
                            ),
                        ),
                    ],
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Non lite dagger resolve on k3 and k4, there should be no errors.
        storage
            .sched_solitontxn_command(
                commands::ResolveDagger::new(
                    temp_map.clone(),
                    None,
                    vec![
                        (
                            Key::from_cocauset(b"k3"),
                            epaxos::Dagger::new(
                                epaxos::DaggerType::Put,
                                b"k1".to_vec(),
                                10.into(),
                                20,
                                Some(b"v3".to_vec()),
                                10.into(),
                                0,
                                11.into(),
                            ),
                        ),
                        (
                            Key::from_cocauset(b"k4"),
                            epaxos::Dagger::new(
                                epaxos::DaggerType::Put,
                                b"k1".to_vec(),
                                10.into(),
                                20,
                                Some(b"v4".to_vec()),
                                10.into(),
                                0,
                                11.into(),
                            ),
                        ),
                    ],
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Undagger the k6 first.
        // Non lite dagger resolve on k5 and k6, error should be reported.
        must_rollback(&storage.einstein_merkle_tree, b"k6", 10, true);
        storage
            .sched_solitontxn_command(
                commands::ResolveDagger::new(
                    temp_map,
                    None,
                    vec![
                        (
                            Key::from_cocauset(b"k5"),
                            epaxos::Dagger::new(
                                epaxos::DaggerType::Put,
                                b"k1".to_vec(),
                                10.into(),
                                20,
                                Some(b"v5".to_vec()),
                                10.into(),
                                0,
                                11.into(),
                            ),
                        ),
                        (
                            Key::from_cocauset(b"k6"),
                            epaxos::Dagger::new(
                                epaxos::DaggerType::Put,
                                b"k1".to_vec(),
                                10.into(),
                                20,
                                Some(b"v6".to_vec()),
                                10.into(),
                                0,
                                11.into(),
                            ),
                        ),
                    ],
                    Context::default(),
                ),
                expect_fail_callback(tx, 6, |e| match e {
                    Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Epaxos(epaxos::Error(
                        box epaxos::ErrorInner::TxnDaggerNotFound { .. },
                    ))))) => (),
                    e => panic!("unexpected error chain: {:?}", e),
                }),
            )
            .unwrap();
        rx.recv().unwrap();
    }

    // Test check_api_version.
    // See the following for detail:
    //   * rfc: https://github.com/einstfdbhikv/rfcs/blob/master/text/0069-api-v2.md.
    //   * proto: https://github.com/pingcap/fdbhikvproto/blob/master/proto/fdbhikvrpcpb.proto, enum APIVersion.
    #[test]
    fn test_check_api_version() {
        use error_code::storage::*;

        const TIDB_KEY_CASE: &[u8] = b"t_a";
        const TXN_KEY_CASE: &[u8] = b"x\0a";
        const cocauset_KEY_CASE: &[u8] = b"r\0a";

        let test_data = vec![
            // storage api_version = V1, for timelike_curvature compatible.
            (
                ApiVersion::V1,                    // storage api_version
                ApiVersion::V1,                    // request api_version
                CommandKind::get,                  // command kind
                vec![TIDB_KEY_CASE, cocauset_KEY_CASE], // keys
                None,                              // expected error code
            ),
            (
                ApiVersion::V1,
                ApiVersion::V1,
                CommandKind::cocauset_get,
                vec![cocauset_KEY_CASE, TXN_KEY_CASE],
                None,
            ),
            // storage api_version = V1ttl, allow cocausetKV request only.
            (
                ApiVersion::V1ttl,
                ApiVersion::V1,
                CommandKind::cocauset_get,
                vec![cocauset_KEY_CASE],
                None,
            ),
            (
                ApiVersion::V1ttl,
                ApiVersion::V1,
                CommandKind::get,
                vec![TIDB_KEY_CASE],
                Some(API_VERSION_NOT_MATCHED),
            ),
            // storage api_version = V1, reject V2 request.
            (
                ApiVersion::V1,
                ApiVersion::V2,
                CommandKind::get,
                vec![TIDB_KEY_CASE],
                Some(API_VERSION_NOT_MATCHED),
            ),
            // storage api_version = V2.
            // timelike_curvature compatible for TiDB request, and TiDB request only.
            (
                ApiVersion::V2,
                ApiVersion::V1,
                CommandKind::get,
                vec![TIDB_KEY_CASE, TIDB_KEY_CASE],
                None,
            ),
            (
                ApiVersion::V2,
                ApiVersion::V1,
                CommandKind::cocauset_get,
                vec![TIDB_KEY_CASE, TIDB_KEY_CASE],
                Some(API_VERSION_NOT_MATCHED),
            ),
            (
                ApiVersion::V2,
                ApiVersion::V1,
                CommandKind::get,
                vec![TIDB_KEY_CASE, TXN_KEY_CASE],
                Some(INVALID_KEY_MODE),
            ),
            (
                ApiVersion::V2,
                ApiVersion::V1,
                CommandKind::get,
                vec![cocauset_KEY_CASE],
                Some(INVALID_KEY_MODE),
            ),
            // V2 api validation.
            (
                ApiVersion::V2,
                ApiVersion::V2,
                CommandKind::get,
                vec![TXN_KEY_CASE],
                None,
            ),
            (
                ApiVersion::V2,
                ApiVersion::V2,
                CommandKind::cocauset_get,
                vec![cocauset_KEY_CASE, cocauset_KEY_CASE],
                None,
            ),
            (
                ApiVersion::V2,
                ApiVersion::V2,
                CommandKind::get,
                vec![cocauset_KEY_CASE, TXN_KEY_CASE],
                Some(INVALID_KEY_MODE),
            ),
            (
                ApiVersion::V2,
                ApiVersion::V2,
                CommandKind::cocauset_get,
                vec![cocauset_KEY_CASE, TXN_KEY_CASE],
                Some(INVALID_KEY_MODE),
            ),
            (
                ApiVersion::V2,
                ApiVersion::V2,
                CommandKind::get,
                vec![TIDB_KEY_CASE],
                Some(INVALID_KEY_MODE),
            ),
        ];

        for (i, (storage_api_version, req_api_version, cmd, keys, err)) in
            test_data.into_iter().enumerate()
        {
            let res = Storage::<Rockseinstein_merkle_tree, DummyDaggerManager>::check_api_version(
                storage_api_version,
                req_api_version,
                cmd,
                keys,
            );
            if let Some(err) = err {
                assert!(res.is_err(), "case {}", i);
                assert_eq!(res.unwrap_err().error_code(), err, "case {}", i);
            } else {
                assert!(res.is_ok(), "case {}", i);
            }
        }
    }

    #[test]
    #[allow(clippy::type_complexity)]
    fn test_check_api_version_ranges() {
        use error_code::storage::*;

        const TIDB_KEY_CASE: &[(Option<&[u8]>, Option<&[u8]>)] = &[
            (Some(b"t_a"), Some(b"t_z")),
            (Some(b"t"), Some(b"u")),
            (Some(b"m"), Some(b"n")),
            (Some(b"m_a"), Some(b"m_z")),
        ];
        const TXN_KEY_CASE: &[(Option<&[u8]>, Option<&[u8]>)] =
            &[(Some(b"x\0a"), Some(b"x\0z")), (Some(b"x"), Some(b"y"))];
        const cocauset_KEY_CASE: &[(Option<&[u8]>, Option<&[u8]>)] =
            &[(Some(b"r\0a"), Some(b"r\0z")), (Some(b"r"), Some(b"s"))];
        // The cases that should fail in API V2
        const TIDB_KEY_CASE_APIV2_ERR: &[(Option<&[u8]>, Option<&[u8]>)] = &[
            (Some(b"t_a"), Some(b"ua")),
            (Some(b"t"), None),
            (None, Some(b"t_z")),
            (Some(b"m_a"), Some(b"na")),
            (Some(b"m"), None),
            (None, Some(b"m_z")),
        ];
        const TXN_KEY_CASE_APIV2_ERR: &[(Option<&[u8]>, Option<&[u8]>)] = &[
            (Some(b"x\0a"), Some(b"ya")),
            (Some(b"x"), None),
            (None, Some(b"x\0z")),
        ];
        const cocauset_KEY_CASE_APIV2_ERR: &[(Option<&[u8]>, Option<&[u8]>)] = &[
            (Some(b"r\0a"), Some(b"sa")),
            (Some(b"r"), None),
            (None, Some(b"r\0z")),
        ];

        let test_case = |storage_api_version,
                         req_api_version,
                         cmd,
                         range: &[(Option<&[u8]>, Option<&[u8]>)],
                         err| {
            let res = Storage::<Rockseinstein_merkle_tree, DummyDaggerManager>::check_api_version_ranges(
                storage_api_version,
                req_api_version,
                cmd,
                range.iter().cloned(),
            );
            if let Some(err) = err {
                assert!(res.is_err());
                assert_eq!(res.unwrap_err().error_code(), err);
            } else {
                assert!(res.is_ok());
            }
        };

        // storage api_version = V1, for timelike_curvature compatible.
        test_case(
            ApiVersion::V1,    // storage api_version
            ApiVersion::V1,    // request api_version
            CommandKind::mutant_search, // command kind
            TIDB_KEY_CASE,     // ranges
            None,              // expected error code
        );
        test_case(
            ApiVersion::V1,
            ApiVersion::V1,
            CommandKind::cocauset_mutant_search,
            TIDB_KEY_CASE,
            None,
        );
        test_case(
            ApiVersion::V1,
            ApiVersion::V1,
            CommandKind::cocauset_mutant_search,
            TIDB_KEY_CASE_APIV2_ERR,
            None,
        );
        // storage api_version = V1ttl, allow cocausetKV request only.
        test_case(
            ApiVersion::V1ttl,
            ApiVersion::V1,
            CommandKind::cocauset_mutant_search,
            cocauset_KEY_CASE,
            None,
        );
        test_case(
            ApiVersion::V1ttl,
            ApiVersion::V1,
            CommandKind::cocauset_mutant_search,
            cocauset_KEY_CASE_APIV2_ERR,
            None,
        );
        test_case(
            ApiVersion::V1ttl,
            ApiVersion::V1,
            CommandKind::mutant_search,
            TIDB_KEY_CASE,
            Some(API_VERSION_NOT_MATCHED),
        );
        // storage api_version = V1, reject V2 request.
        test_case(
            ApiVersion::V1,
            ApiVersion::V2,
            CommandKind::mutant_search,
            TIDB_KEY_CASE,
            Some(API_VERSION_NOT_MATCHED),
        );
        // storage api_version = V2.
        // timelike_curvature compatible for TiDB request, and TiDB request only.
        test_case(
            ApiVersion::V2,
            ApiVersion::V1,
            CommandKind::mutant_search,
            TIDB_KEY_CASE,
            None,
        );
        test_case(
            ApiVersion::V2,
            ApiVersion::V1,
            CommandKind::cocauset_mutant_search,
            TIDB_KEY_CASE,
            Some(API_VERSION_NOT_MATCHED),
        );
        test_case(
            ApiVersion::V2,
            ApiVersion::V1,
            CommandKind::mutant_search,
            TXN_KEY_CASE,
            Some(INVALID_KEY_MODE),
        );
        test_case(
            ApiVersion::V2,
            ApiVersion::V1,
            CommandKind::mutant_search,
            cocauset_KEY_CASE,
            Some(INVALID_KEY_MODE),
        );
        // V2 api validation.
        test_case(
            ApiVersion::V2,
            ApiVersion::V2,
            CommandKind::mutant_search,
            TXN_KEY_CASE,
            None,
        );
        test_case(
            ApiVersion::V2,
            ApiVersion::V2,
            CommandKind::cocauset_mutant_search,
            cocauset_KEY_CASE,
            None,
        );
        test_case(
            ApiVersion::V2,
            ApiVersion::V2,
            CommandKind::mutant_search,
            cocauset_KEY_CASE,
            Some(INVALID_KEY_MODE),
        );
        test_case(
            ApiVersion::V2,
            ApiVersion::V2,
            CommandKind::cocauset_mutant_search,
            TXN_KEY_CASE,
            Some(INVALID_KEY_MODE),
        );
        test_case(
            ApiVersion::V2,
            ApiVersion::V2,
            CommandKind::mutant_search,
            TIDB_KEY_CASE,
            Some(INVALID_KEY_MODE),
        );

        for range in TIDB_KEY_CASE_APIV2_ERR {
            test_case(
                ApiVersion::V2,
                ApiVersion::V1,
                CommandKind::mutant_search,
                &[*range],
                Some(INVALID_KEY_MODE),
            );
        }
        for range in TXN_KEY_CASE_APIV2_ERR {
            test_case(
                ApiVersion::V2,
                ApiVersion::V2,
                CommandKind::mutant_search,
                &[*range],
                Some(INVALID_KEY_MODE),
            );
        }
        for range in cocauset_KEY_CASE_APIV2_ERR {
            test_case(
                ApiVersion::V2,
                ApiVersion::V2,
                CommandKind::cocauset_mutant_search,
                &[*range],
                Some(INVALID_KEY_MODE),
            );
        }
    }

    #[test]
    fn test_write_in_memory_pessimistic_daggers() {
        let solitontxn_ext = Arc::new(TxnExt::default());
        let storage = TestStorageBuilder::new(DummyDaggerManager {}, ApiVersion::V1)
            .pipelined_pessimistic_dagger(true)
            .in_memory_pessimistic_dagger(true)
            .build_for_solitontxn(solitontxn_ext.clone())
            .unwrap();
        let (tx, rx) = channel();

        let k1 = Key::from_cocauset(b"k1");
        storage
            .sched_solitontxn_command(
                new_acquire_pessimistic_dagger_command(
                    vec![(k1.clone(), false)],
                    10,
                    10,
                    false,
                    false,
                ),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();

        {
            let pessimistic_daggers = solitontxn_ext.pessimistic_daggers.read();
            let dagger = pessimistic_daggers.get(&k1).unwrap();
            assert_eq!(
                dagger,
                &(
                    PessimisticDagger {
                        primary: Box::new(*b"k1"),
                        start_ts: 10.into(),
                        ttl: 3000,
                        for_update_ts: 10.into(),
                        min_commit_ts: 11.into(),
                    },
                    false
                )
            );
        }

        let (tx, rx) = channel();
        // The written in-memory pessimistic dagger should be visible, so the new dagger request should fail.
        storage
            .sched_solitontxn_command(
                new_acquire_pessimistic_dagger_command(
                    vec![(k1.clone(), false)],
                    20,
                    20,
                    false,
                    false,
                ),
                Box::new(move |res| {
                    tx.send(res).unwrap();
                }),
            )
            .unwrap();
        // DummyDaggerManager just drops the callback, so it will fail to receive anything.
        assert!(rx.recv().is_err());

        let (tx, rx) = channel();
        storage
            .sched_solitontxn_command(
                commands::PrewritePessimistic::new(
                    vec![(Mutation::make_put(k1.clone(), b"v".to_vec()), true)],
                    b"k1".to_vec(),
                    10.into(),
                    3000,
                    10.into(),
                    1,
                    20.into(),
                    TimeStamp::default(),
                    None,
                    false,
                    AssertionLevel::Off,
                    Context::default(),
                ),
                Box::new(move |res| {
                    tx.send(res).unwrap();
                }),
            )
            .unwrap();
        assert!(rx.recv().unwrap().is_ok());
        // After prewrite, the memory dagger should be removed.
        {
            let pessimistic_daggers = solitontxn_ext.pessimistic_daggers.read();
            assert!(pessimistic_daggers.get(&k1).is_none());
        }
    }

    #[test]
    fn test_disable_in_memory_pessimistic_daggers() {
        let solitontxn_ext = Arc::new(TxnExt::default());
        let storage = TestStorageBuilder::new(DummyDaggerManager {}, ApiVersion::V1)
            .pipelined_pessimistic_dagger(true)
            .in_memory_pessimistic_dagger(false)
            .build_for_solitontxn(solitontxn_ext.clone())
            .unwrap();
        let (tx, rx) = channel();

        let k1 = Key::from_cocauset(b"k1");
        storage
            .sched_solitontxn_command(
                new_acquire_pessimistic_dagger_command(
                    vec![(k1.clone(), false)],
                    10,
                    10,
                    false,
                    false,
                ),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();
        // When disabling in-memory pessimistic dagger, the dagger map should remain unchanged.
        assert!(solitontxn_ext.pessimistic_daggers.read().is_empty());

        let (tx, rx) = channel();
        storage
            .sched_solitontxn_command(
                commands::PrewritePessimistic::new(
                    vec![(Mutation::make_put(k1, b"v".to_vec()), true)],
                    b"k1".to_vec(),
                    10.into(),
                    3000,
                    10.into(),
                    1,
                    20.into(),
                    TimeStamp::default(),
                    None,
                    false,
                    AssertionLevel::Off,
                    Context::default(),
                ),
                Box::new(move |res| {
                    tx.send(res).unwrap();
                }),
            )
            .unwrap();
        // Prewrite still succeeds
        assert!(rx.recv().unwrap().is_ok());
    }
}
