// Copyright 2021 EinsteinDB Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::cmp::PartialOrd;
use std::collections::VecDeque;
use std::ops::{Add, AddAssign, Sub, SubAssign};
use std::sync::causetxctx::{causetxctxBool, causetxctxU32, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender};
use std::sync::Arc;
use std::thread::{Builder, JoinHandle};
use std::time::Duration;
use std::u64;

use collections::HashMap;
use engine_rocks::CausetxctxInfo;
use einsteindb-gen::{CFNamesExt, CausetxctxControlFactorsExt};
use num_promises::cast::{AsPrimitive, FromPrimitive};
use rand::Rng;
use einstfdbhikv_util::time::{Instant, Limiter};

use crate::einsteindb::storage::config::CausetxctxControlConfig;
use crate::einsteindb::storage::metrics::*;

const TICK_DURATION: Duration = Duration::from_millis(1000);

const RATIO_SCALE_FACTOR: u32 = 10_000_000;
const K_INC_SLOWDOWN_RATIO: f64 = 0.8;
const K_DEC_SLOWDOWN_RATIO: f64 = 1.0 / K_INC_SLOWDOWN_RATIO;
const MIN_THROTTLE_SPEED: f64 = 16.0 * 1024.0; // 16KB
const MAX_THROTTLE_SPEED: f64 = 200.0 * 1024.0 * 1024.0; // 200MB

const EMA_FACTOR: f64 = 0.6; // EMA stands for Exponential Moving Average

#[derive(Eq, PartialEq, Debug)]
enum Trend {
    Increasing,
    Decreasing,
    NoTrend,
}

/// Causetxctx controller is used to throttle the write rate at scheduler level, aiming
/// to substitute the write stall mechanism of RocksDB. It features in two points:
///   * throttle at scheduler, so raftstore and apply won't be bdaggered anymore
///   * better control on the throttle rate to avoid QPS drop under heavy write
///
/// When write stall happens, the max speed of write rate max_delayed_write_rate
/// is limited to 16MB/s by default which doesn't take real disk ability into
/// account. It may underestimate the disk's throughout that 16MB/s is too small
/// at once, causing a very large jitter on the write duration.
/// Also, it decreases the delayed write rate further if the factors still exceed
/// the threshold. So under heavy write load, the write rate may be throttled to
/// a very low rate from time to time, causing QPS drop eventually.
///

/// For compaction pending bytes, we use discardable ratio to do Causetxctx control
/// which is separated mechanism from throttle speed. Compaction pending bytes is
/// a approximate value, usually, changes up and down dramatically, so it's unwise
/// to map compaction pending bytes to a specified throttle speed. Instead,
/// mapping it from soft limit to hard limit as 0% to 100% discardable ratio. With
/// this, there must be a point that foreground write rate is equal to the
/// background compaction pending bytes consuming rate so that compaction pending
/// bytes is kept around a steady level.
///
/// Here is a brief Causetxctx showing where the mechanism works:
/// grpc -> check should drop(discardable ratio) -> limiter -> async write to raftstore
pub struct CausetxctxController {
    discard_ratio: Arc<causetxctxU32>,
    limiter: Arc<Limiter>,
    enabled: Arc<causetxctxBool>,
    tx: Option<SyncSender<Msg>>,
    handle: Option<std::thread::JoinHandle<()>>,
}

enum Msg {
    Close,
    Enable,
    Disable,
}

impl Drop for CausetxctxController {
    fn drop(&mut self) {
        let h = self.handle.take();
        if h.is_none() {
            return;
        }

        if let Some(Err(e)) = self.tx.as_ref().map(|tx| tx.send(Msg::Close)) {
            error!("send quit message for Causetxctx controller failed"; "err" => ?e);
            return;
        }

        if let Err(e) = h.unwrap().join() {
            error!("join Causetxctx controller failed"; "err" => ?e);
        }
    }
}

impl CausetxctxController {
    // only for test
    pub fn empty() -> Self {
        Self {
            discard_ratio: Arc::new(causetxctxU32::new(0)),
            limiter: Arc::new(Limiter::new(f64::INFINITY)),
            enabled: Arc::new(causetxctxBool::new(false)),
            tx: None,
            handle: None,
        }
    }

    pub fn new<E: CFNamesExt + CausetxctxControlFactorsExt + Send + 'static>(
        config: &CausetxctxControlConfig,
        engine: E,
        Causetxctx_info_receiver: Receiver<CausetxctxInfo>,
    ) -> Self {
        let limiter = Arc::new(
            <Limiter>::builder(f64::INFINITY)
                .refill(Duration::from_millis(1))
                .build(),
        );
        let discard_ratio = Arc::new(causetxctxU32::new(0));
        let checker = CausetxctxChecker::new(config, engine, discard_ratio.clone(), limiter.clone());
        let (tx, rx) = mpsc::sync_channel(5);

        tx.send(if config.enable {
            Msg::Enable
        } else {
            Msg::Disable
        })
        .unwrap();

        Self {
            discard_ratio,
            limiter,
            enabled: Arc::new(causetxctxBool::new(config.enable)),
            tx: Some(tx),
            handle: Some(checker.start(rx, Causetxctx_info_receiver)),
        }
    }

    pub fn should_drop(&self) -> bool {
        let ratio = self.discard_ratio.load(Ordering::Relaxed);
        let mut rng = rand::thread_rng();
        rng.gen_ratio(ratio, RATIO_SCALE_FACTOR)
    }

    #[cfg(test)]
    pub fn discard_ratio(&self) -> f64 {
        self.discard_ratio.load(Ordering::Relaxed) as f64 / RATIO_SCALE_FACTOR as f64
    }

    pub fn consume(&self, bytes: usize) -> Duration {
        self.limiter.consume_duration(bytes)
    }

    pub fn unconsume(&self, bytes: usize) {
        self.limiter.unconsume(bytes);
    }

    #[cfg(test)]
    pub fn total_bytes_consumed(&self) -> usize {
        self.limiter.total_bytes_consumed()
    }

    pub fn enable(&self, enable: bool) {
        self.enabled.store(enable, Ordering::Relaxed);
        if let Some(tx) = &self.tx {
            if enable {
                tx.send(Msg::Enable).unwrap();
            } else {
                tx.send(Msg::Disable).unwrap();
            }
        }
    }

    pub fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub fn set_speed_limit(&self, speed_limit: f64) {
        self.limiter.set_speed_limit(speed_limit);
    }

    pub fn is_unlimited(&self) -> bool {
        self.limiter.speed_limit() == f64::INFINITY
    }
}

const SMOOTHER_STALE_RECORD_THRESHOLD: u64 = 300; // 5min
const SMOOTHER_TIME_RANGE_THRESHOLD: u64 = 60; // 1min

// Smoother is a sliding window used to provide steadier Causetxctx statistics.
struct Smoother<T, const CAP: usize, const STALE_DUR: u64, const MIN_TIME_SPAN: u64>
where
    T: Default
        + Add<Output = T>
        + Sub<Output = T>
        + AddAssign
        + SubAssign
        + PartialOrd
        + AsPrimitive<f64>
        + FromPrimitive,
{
    records: VecDeque<(T, Instant)>,
    total: T,
}

impl<T, const CAP: usize, const STALE_DUR: u64, const MIN_TIME_SPAN: u64> Default
    for Smoother<T, CAP, STALE_DUR, MIN_TIME_SPAN>
where
    T: Default
        + Add<Output = T>
        + Sub<Output = T>
        + AddAssign
        + SubAssign
        + PartialOrd
        + AsPrimitive<f64>
        + FromPrimitive,
{
    fn default() -> Self {
        Self {
            records: VecDeque::with_capacity(CAP),
            total: Default::default(),
        }
    }
}

impl<T, const CAP: usize, const STALE_DUR: u64, const MIN_TIME_SPAN: u64>
    Smoother<T, CAP, STALE_DUR, MIN_TIME_SPAN>
where
    T: Default
        + Add<Output = T>
        + Sub<Output = T>
        + AddAssign
        + SubAssign
        + PartialOrd
        + AsPrimitive<f64>
        + FromPrimitive,
{
    pub fn observe(&mut self, record: T) {
        self.observe_with_time(record, Instant::now_coarse());
    }

    pub fn observe_with_time(&mut self, record: T, time: Instant) {
        if self.records.len() == CAP {
            let v = self.records.pop_front().unwrap().0;
            self.total -= v;
        }

        self.total += record;

        self.records.push_back((record, time));
        self.remove_stale_records();
    }

    fn remove_stale_records(&mut self) {
        // make sure there are two records left at least
        while self.records.len() > 2 {
            if self.records.front().unwrap().1.saturating_elapsed_secs() > STALE_DUR as f64 {
                let v = self.records.pop_front().unwrap().0;
                self.total -= v;
            } else {
                break;
            }
        }
    }

    pub fn get_recent(&self) -> T {
        if self.records.is_empty() {
            return T::default();
        }
        self.records.back().unwrap().0
    }

    pub fn get_avg(&self) -> f64 {
        if self.records.is_empty() {
            return 0.0;
        }
        self.total.as_() / self.records.len() as f64
    }

    pub fn get_max(&self) -> T {
        if self.records.is_empty() {
            return T::default();
        }
        self.records
            .iter()
            .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap())
            .unwrap()
            .0
    }

    pub fn get_percentile_90(&mut self) -> T {
        if self.records.is_empty() {
            return FromPrimitive::from_u64(0).unwrap();
        }
        let mut v: Vec<_> = self.records.iter().collect();
        v.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        v[((self.records.len() - 1) as f64 * 0.90) as usize].0
    }

    pub fn trend(&self) -> Trend {
        if self.records.len() <= 1 {
            return Trend::NoTrend;
        }

        // If the lastest record is too old, no trend
        if self.records.back().unwrap().1.saturating_elapsed_secs() > STALE_DUR as f64 {
            return Trend::NoTrend;
        }

        let (mut left, mut left_cnt) = (T::default(), 0);
        let (mut right, mut right_cnt) = (T::default(), 0);

        // The time span matters
        if MIN_TIME_SPAN > 0 {
            // If the records doesn't cover a enough time span, no trend
            let time_span = self.records.front().unwrap().1.saturating_elapsed_secs()
                - self.records.back().unwrap().1.saturating_elapsed_secs();
            if time_span < MIN_TIME_SPAN as f64 {
                return Trend::NoTrend;
            }

            // Split the record into left and right by the middle of time range
            for (_, r) in self.records.iter().enumerate() {
                let elapsed_secs = r.1.saturating_elapsed_secs();
                if elapsed_secs > time_span / 2.0 {
                    left += r.0;
                    left_cnt += 1;
                } else {
                    right += r.0;
                    right_cnt += 1;
                }
            }
        } else {
            let half = self.records.len() / 2;
            for (i, r) in self.records.iter().enumerate() {
                if i < half {
                    left += r.0;
                    left_cnt += 1;
                } else {
                    right += r.0;
                    right_cnt += 1;
                }
            }
        }

        // Decide if there is a trend by the two averages.
        // Adding 2 here is to give a tolerance
        let (l_avg, r_avg) = (left.as_() / left_cnt as f64, right.as_() / right_cnt as f64);
        if r_avg > l_avg + 2.0 {
            return Trend::Increasing;
        }
        if l_avg > r_avg + 2.0 {
            return Trend::Decreasing;
        }

        Trend::NoTrend
    }
}

// CFCausetxctxChecker records some statistics and states related to one CF.
// These statistics fall into five categories:
//   * memtable
//   * L0 files
//   * L0 production Causetxctx (flush Causetxctx)
//   * L0 consumption Causetxctx (compaction read Causetxctx of L0)
//   * pending compaction bytes
// And all of them are collected from the hook of RocksDB's event listener.
struct CFCausetxctxChecker {
    // Memtable related
    last_num_memtables: Smoother<u64, 20, SMOOTHER_STALE_RECORD_THRESHOLD, 0>,
    memtable_debt: f64,
    memtable_init_speed: bool,

    // L0 files related
    // a few records of number of L0 files right after flush or L0 compaction
    // As we know, after flush the number of L0 files must increase by 1,
    // whereas, after L0 compaction the number of L0 files must decrease a lot
    // considering L0 compactions nearly includes all L0 files in a round.
    // So to evaluate the accumulation of L0 files, here only records the number
    // of L0 files right after L0 compactions.
    long_term_num_l0_files: Smoother<u64, 20, SMOOTHER_STALE_RECORD_THRESHOLD, 0>,

    // L0 production Causetxctx related
    last_flush_bytes: u64,
    last_flush_bytes_time: Instant,
    short_term_l0_production_Causetxctx: Smoother<u64, 10, SMOOTHER_STALE_RECORD_THRESHOLD, 0>,

    // L0 consumption Causetxctx related
    last_l0_bytes: u64,
    last_l0_bytes_time: Instant,
    short_term_l0_consumption_Causetxctx: Smoother<u64, 3, SMOOTHER_STALE_RECORD_THRESHOLD, 0>,

    // Pending compaction bytes related
    // When the write Causetxctx is about 100MB/s, we observed that the compaction ops
    // is about 2.5, it means there are 750 compaction events in 5 minutes.
    long_term_pending_bytes:
        Smoother<f64, 1024, SMOOTHER_STALE_RECORD_THRESHOLD, SMOOTHER_TIME_RANGE_THRESHOLD>,
    pending_bytes_before_unsafe_destroy_range: Option<f64>,

    // On start related markers. Because after restart, the memtable, l0 files
    // and compaction pending bytes may be high on start. If throttle on start
    // at once, it may get a low throttle speed as initialization cause it may
    // has no write Causetxctx after restart. So use the markers to make sure only
    // throttled after the the memtable, l0 files and compaction pending bytes
    // go beyond the threshold again.
    on_start_memtable: bool,
    on_start_l0_files: bool,
    on_start_pending_bytes: bool,
}

impl Default for CFCausetxctxChecker {
    fn default() -> Self {
        Self {
            last_num_memtables: Smoother::default(),
            memtable_debt: 0.0,
            memtable_init_speed: false,
            long_term_num_l0_files: Smoother::default(),
            last_flush_bytes: 0,
            last_flush_bytes_time: Instant::now_coarse(),
            short_term_l0_production_Causetxctx: Smoother::default(),
            last_l0_bytes: 0,
            last_l0_bytes_time: Instant::now_coarse(),
            short_term_l0_consumption_Causetxctx: Smoother::default(),
            long_term_pending_bytes: Smoother::default(),
            pending_bytes_before_unsafe_destroy_range: None,
            on_start_memtable: true,
            on_start_l0_files: true,
            on_start_pending_bytes: true,
        }
    }
}

struct CausetxctxChecker<E: CFNamesExt + CausetxctxControlFactorsExt + Send + 'static> {
    soft_pending_compaction_bytes_limit: u64,
    hard_pending_compaction_bytes_limit: u64,
    memtables_threshold: u64,
    l0_files_threshold: u64,

    // CFCausetxctxChecker for each CF.
    cf_checkers: HashMap<String, CFCausetxctxChecker>,
    // Record which CF is taking control of throttling, the throttle speed is
    // decided based on the statistics of the throttle CF. If the multiple CFs
    // exceed the threshold, choose the larger one.
    throttle_cf: Option<String>,
    // Discard ratio is decided by pending compaction bytes, it's the ratio to
    // drop write requests(return ServerIsBusy to TiDB) randomly.
    discard_ratio: Arc<causetxctxU32>,

    engine: E,
    limiter: Arc<Limiter>,
    // Records the foreground write Causetxctx at scheduler level of last few seconds.
    write_Causetxctx_recorder: Smoother<u64, 30, SMOOTHER_STALE_RECORD_THRESHOLD, 0>,

    last_record_time: Instant,
    last_speed: f64,
    wait_for_destroy_range_finish: bool,
}

impl<E: CFNamesExt + CausetxctxControlFactorsExt + Send + 'static> CausetxctxChecker<E> {
    pub fn new(
        config: &CausetxctxControlConfig,
        engine: E,
        discard_ratio: Arc<causetxctxU32>,
        limiter: Arc<Limiter>,
    ) -> Self {
        let cf_checkers = engine
            .cf_names()
            .into_iter()
            .map(|cf| (cf.to_owned(), CFCausetxctxChecker::default()))
            .collect();

        Self {
            soft_pending_compaction_bytes_limit: config.soft_pending_compaction_bytes_limit.0,
            hard_pending_compaction_bytes_limit: config.hard_pending_compaction_bytes_limit.0,
            memtables_threshold: config.memtables_threshold,
            l0_files_threshold: config.l0_files_threshold,
            engine,
            discard_ratio,
            limiter,
            write_Causetxctx_recorder: Smoother::default(),
            cf_checkers,
            throttle_cf: None,
            last_record_time: Instant::now_coarse(),
            last_speed: 0.0,
            wait_for_destroy_range_finish: false,
        }
    }

    fn start(self, rx: Receiver<Msg>, Causetxctx_info_receiver: Receiver<CausetxctxInfo>) -> JoinHandle<()> {
        Builder::new()
            .name(thd_name!("Causetxctx-checker"))
            .spawn(move || {
                einstfdbhikv_alloc::add_thread_memory_accessor();
                let mut checker = self;
                let mut deadline = std::time::Instant::now();
                let mut enabled = true;
                loop {
                    match rx.try_recv() {
                        Ok(Msg::Close) => break,
                        Ok(Msg::Disable) => {
                            enabled = false;
                            checker.reset_statistics();
                        }
                        Ok(Msg::Enable) => {
                            enabled = true;
                        }
                        Err(_) => {}
                    }

                    match Causetxctx_info_receiver.recv_deadline(deadline) {
                        Ok(CausetxctxInfo::L0(cf, l0_bytes)) => {
                            checker.collect_l0_consumption_stats(&cf, l0_bytes);
                            if enabled {
                                checker.on_l0_change(cf)
                            }
                        }
                        Ok(CausetxctxInfo::L0Intra(cf, diff_bytes)) => {
                            if diff_bytes > 0 {
                                // Intra L0 merges some deletion records, so regard it as a L0 compaction.
                                checker.collect_l0_consumption_stats(&cf, diff_bytes);
                                if enabled {
                                    checker.on_l0_change(cf);
                                }
                            }
                        }
                        Ok(CausetxctxInfo::Flush(cf, flush_bytes)) => {
                            checker.collect_l0_production_stats(&cf, flush_bytes);
                            if enabled {
                                checker.on_memtable_change(&cf);
                                checker.on_l0_change(cf)
                            }
                        }
                        Ok(CausetxctxInfo::Compaction(cf)) => {
                            if enabled {
                                checker.on_pending_compaction_bytes_change(cf);
                            }
                        }
                        Ok(CausetxctxInfo::BeforeUnsafeDestroyRange) => {
                            if !enabled {
                                continue;
                            }
                            checker.wait_for_destroy_range_finish = true;
                            let soft = (checker.soft_pending_compaction_bytes_limit as f64).log2();
                            for cf_checker in checker.cf_checkers.values_mut() {
                                let v = cf_checker.long_term_pending_bytes.get_avg();
                                if v <= soft {
                                    cf_checker.pending_bytes_before_unsafe_destroy_range = Some(v);
                                }
                            }
                        }
                        Ok(CausetxctxInfo::AfterUnsafeDestroyRange) => {
                            if !enabled {
                                continue;
                            }
                            checker.wait_for_destroy_range_finish = false;
                            for (cf, cf_checker) in &mut checker.cf_checkers {
                                if let Some(before) =
                                    cf_checker.pending_bytes_before_unsafe_destroy_range
                                {
                                    let soft =
                                        (checker.soft_pending_compaction_bytes_limit as f64).log2();
                                    let after = (checker
                                        .engine
                                        .get_cf_pending_compaction_bytes(cf)
                                        .unwrap_or(None)
                                        .unwrap_or(0)
                                        as f64)
                                        .log2();

                                    assert!(before < soft);
                                    if after >= soft {
                                        // there is a pending bytes jump
                                        SCHED_THROTTLE_ACTION_COUNTER
                                            .with_label_values(&[cf, "pending_bytes_jump"])
                                            .inc();
                                    } else {
                                        cf_checker.pending_bytes_before_unsafe_destroy_range = None;
                                    }
                                }
                            }
                        }
                        Err(RecvTimeoutError::Timeout) => {
                            checker.update_statistics();
                            deadline = std::time::Instant::now() + TICK_DURATION;
                        }
                        Err(e) => {
                            error!("failed to receive compaction info {:?}", e);
                        }
                    }
                }
                einstfdbhikv_alloc::remove_thread_memory_accessor();
            })
            .unwrap()
    }

    fn reset_statistics(&mut self) {
        SCHED_L0_TARGET_Causetxctx_GAUGE.set(0);
        for cf in self.cf_checkers.keys() {
            SCHED_THROTTLE_CF_GAUGE.with_label_values(&[cf]).set(0);
            SCHED_PENDING_COMPACTION_BYTES_GAUGE
                .with_label_values(&[cf])
                .set(0);
            SCHED_MEMTABLE_GAUGE.with_label_values(&[cf]).set(0);
            SCHED_L0_GAUGE.with_label_values(&[cf]).set(0);
            SCHED_L0_AVG_GAUGE.with_label_values(&[cf]).set(0);
            SCHED_L0_Causetxctx_GAUGE.with_label_values(&[cf]).set(0);
            SCHED_FLUSH_Causetxctx_GAUGE.with_label_values(&[cf]).set(0);
        }
        SCHED_WRITE_Causetxctx_GAUGE.set(0);
        SCHED_THROTTLE_Causetxctx_GAUGE.set(0);
        self.limiter.set_speed_limit(f64::INFINITY);
        SCHED_DISCARD_RATIO_GAUGE.set(0);
        self.discard_ratio.store(0, Ordering::Relaxed);
    }

    fn update_statistics(&mut self) {
        if let Some(throttle_cf) = self.throttle_cf.as_ref() {
            SCHED_THROTTLE_CF_GAUGE
                .with_label_values(&[throttle_cf])
                .set(1);
            for cf in self.cf_checkers.keys() {
                if cf != throttle_cf {
                    SCHED_THROTTLE_CF_GAUGE.with_label_values(&[cf]).set(0);
                }
            }
        } else {
            for cf in self.cf_checkers.keys() {
                SCHED_THROTTLE_CF_GAUGE.with_label_values(&[cf]).set(0);
            }
        }

        // calculate foreground write Causetxctx
        let dur = self.last_record_time.saturating_elapsed_secs();
        if dur < f64::EPSILON {
            return;
        }
        let rate = self.limiter.total_bytes_consumed() as f64 / dur;
        // don't record those write rate of 0.
        // For closed loop system, if all the requests are delayed(assume > 1s),
        // then in the next second, the write rate would be 0. But it doesn't
        // reflect the real write rate, so just ignore it.
        if self.limiter.total_bytes_consumed() != 0 {
            self.write_Causetxctx_recorder.observe(rate as u64);
        }
        SCHED_WRITE_Causetxctx_GAUGE.set(rate as i64);
        self.last_record_time = Instant::now_coarse();

        self.limiter.reset_statistics();
    }

    fn on_pending_compaction_bytes_change(&mut self, cf: String) {
        let hard = (self.hard_pending_compaction_bytes_limit as f64).log2();
        let soft = (self.soft_pending_compaction_bytes_limit as f64).log2();

        // Because pending compaction bytes changes dramatically, take the
        // logarithm of pending compaction bytes to make the values fall into
        // a relative small range
        let num = (self
            .engine
            .get_cf_pending_compaction_bytes(&cf)
            .unwrap_or(None)
            .unwrap_or(0) as f64)
            .log2();
        let checker = self.cf_checkers.get_mut(&cf).unwrap();
        checker.long_term_pending_bytes.observe(num);
        SCHED_PENDING_COMPACTION_BYTES_GAUGE
            .with_label_values(&[&cf])
            .set((checker.long_term_pending_bytes.get_avg() * RATIO_SCALE_FACTOR as f64) as i64);

        // do special check on start, see the comment of the variable definition for detail.
        if checker.on_start_pending_bytes {
            if num < soft || checker.long_term_pending_bytes.trend() == Trend::Increasing {
                // the write is accumulating, still need to throttle
                checker.on_start_pending_bytes = false;
            } else {
                // still on start, should not throttle now
                return;
            }
        }

        let pending_compaction_bytes = checker.long_term_pending_bytes.get_avg();
        let ignore = if let Some(before) = checker.pending_bytes_before_unsafe_destroy_range {
            if pending_compaction_bytes <= before && !self.wait_for_destroy_range_finish {
                checker.pending_bytes_before_unsafe_destroy_range = None;
            }
            true
        } else {
            false
        };

        for checker in self.cf_checkers.values() {
            if num < checker.long_term_pending_bytes.get_recent() {
                return;
            }
        }

        let mut ratio = if pending_compaction_bytes < soft || ignore {
            0
        } else {
            let new_ratio = (pending_compaction_bytes - soft) / (hard - soft);
            let old_ratio = self.discard_ratio.load(Ordering::Relaxed);

            // Because pending compaction bytes changes up and down, so using
            // EMA(Exponential Moving Average) to smooth it.
            (if old_ratio != 0 {
                EMA_FACTOR * (old_ratio as f64 / RATIO_SCALE_FACTOR as f64)
                    + (1.0 - EMA_FACTOR) * new_ratio
            } else if new_ratio > 0.01 {
                0.01
            } else {
                new_ratio
            } * RATIO_SCALE_FACTOR as f64) as u32
        };
        SCHED_DISCARD_RATIO_GAUGE.set(ratio as i64);
        if ratio > RATIO_SCALE_FACTOR {
            ratio = RATIO_SCALE_FACTOR;
        }
        self.discard_ratio.store(ratio, Ordering::Relaxed);
    }

    fn on_memtable_change(&mut self, cf: &str) {
        let num_memtables = self
            .engine
            .get_cf_num_immutable_mem_table(cf)
            .unwrap_or(None)
            .unwrap_or(0);
        let checker = self.cf_checkers.get_mut(cf).unwrap();
        SCHED_MEMTABLE_GAUGE
            .with_label_values(&[cf])
            .set(num_memtables as i64);
        let prev = checker.last_num_memtables.get_recent();
        checker.last_num_memtables.observe(num_memtables);

        // do special check on start, see the comment of the variable definition for detail.
        if checker.on_start_memtable {
            if num_memtables < self.memtables_threshold
                || checker.last_num_memtables.trend() == Trend::Increasing
            {
                // the write is accumulating, still need to throttle
                checker.on_start_memtable = false;
            } else {
                // still on start, should not throttle now
                return;
            }
        }

        for c in self.cf_checkers.values() {
            if num_memtables < c.last_num_memtables.get_recent() {
                return;
            }
        }

        let checker = self.cf_checkers.get_mut(cf).unwrap();
        let is_throttled = self.limiter.speed_limit() != f64::INFINITY;
        let should_throttle =
            checker.last_num_memtables.get_avg() > self.memtables_threshold as f64;
        let throttle = if !is_throttled && should_throttle {
            SCHED_THROTTLE_ACTION_COUNTER
                .with_label_values(&[cf, "memtable_init"])
                .inc();
            let x = self.write_Causetxctx_recorder.get_percentile_90();
            if x == 0 {
                f64::INFINITY
            } else {
                checker.memtable_init_speed = true;
                self.throttle_cf = Some(cf.to_string());
                x as f64
            }
        } else if is_throttled && (!should_throttle || num_memtables < self.memtables_threshold) {
            // should not throttle memtable
            if checker.memtable_init_speed {
                checker.memtable_init_speed = false;
                f64::INFINITY
            } else {
                let speed = self.limiter.speed_limit() + checker.memtable_debt * 1024.0 * 1024.0;
                checker.memtable_debt = 0.0;
                speed
            }
        } else if is_throttled && should_throttle {
            // should throttle
            let diff = match num_memtables.cmp(&prev) {
                std::cmp::Ordering::Greater => {
                    checker.memtable_debt += 1.0;
                    -1.0
                }
                std::cmp::Ordering::Less => {
                    checker.memtable_debt -= 1.0;
                    1.0
                }
                std::cmp::Ordering::Equal => {
                    // keep, do nothing
                    0.0
                }
            };
            self.limiter.speed_limit() + diff * 1024.0 * 1024.0
        } else {
            f64::INFINITY
        };

        self.update_speed_limit(throttle);
    }

    fn collect_l0_consumption_stats(&mut self, cf: &str, l0_bytes: u64) {
        let num_l0_files = self
            .engine
            .get_cf_num_files_at_level(cf, 0)
            .unwrap_or(None)
            .unwrap_or(0);
        let checker = self.cf_checkers.get_mut(cf).unwrap();
        checker.last_l0_bytes += l0_bytes;
        checker.long_term_num_l0_files.observe(num_l0_files);
        SCHED_L0_GAUGE
            .with_label_values(&[cf])
            .set(num_l0_files as i64);
        SCHED_L0_AVG_GAUGE
            .with_label_values(&[cf])
            .set(checker.long_term_num_l0_files.get_avg() as i64);
    }

    fn collect_l0_production_stats(&mut self, cf: &str, flush_bytes: u64) {
        let num_l0_files = self
            .engine
            .get_cf_num_files_at_level(cf, 0)
            .unwrap_or(None)
            .unwrap_or(0);

        let checker = self.cf_checkers.get_mut(cf).unwrap();
        checker.last_flush_bytes += flush_bytes;
        checker.long_term_num_l0_files.observe(num_l0_files);
        SCHED_L0_GAUGE
            .with_label_values(&[cf])
            .set(num_l0_files as i64);
        SCHED_L0_AVG_GAUGE
            .with_label_values(&[cf])
            .set(checker.long_term_num_l0_files.get_avg() as i64);

        if checker.last_flush_bytes_time.saturating_elapsed_secs() > 5.0 {
            // update flush Causetxctx
            let flush_Causetxctx = checker.last_flush_bytes as f64
                / checker.last_flush_bytes_time.saturating_elapsed_secs();
            checker
                .short_term_l0_production_Causetxctx
                .observe(flush_Causetxctx as u64);
            SCHED_FLUSH_Causetxctx_GAUGE
                .with_label_values(&[cf])
                .set(checker.short_term_l0_production_Causetxctx.get_avg() as i64);

            // update l0 Causetxctx
            if checker.last_l0_bytes != 0 {
                let l0_Causetxctx = checker.last_l0_bytes as f64
                    / checker.last_l0_bytes_time.saturating_elapsed_secs();
                checker.last_l0_bytes_time = Instant::now_coarse();
                checker
                    .short_term_l0_consumption_Causetxctx
                    .observe(l0_Causetxctx as u64);
                SCHED_L0_Causetxctx_GAUGE
                    .with_label_values(&[cf])
                    .set(checker.short_term_l0_consumption_Causetxctx.get_avg() as i64);
            }

            checker.last_flush_bytes_time = Instant::now_coarse();
            checker.last_l0_bytes = 0;
            checker.last_flush_bytes = 0;
        }
    }

    // Check the number of l0 files to decide whether need to adjust target Causetxctx
    fn on_l0_change(&mut self, cf: String) {
        let checker = self.cf_checkers.get_mut(&cf).unwrap();
        let num_l0_files = checker.long_term_num_l0_files.get_recent();

        // do special check on start, see the comment of the variable definition for detail.
        if checker.on_start_l0_files {
            if num_l0_files < self.l0_files_threshold
                || checker.long_term_num_l0_files.trend() == Trend::Increasing
            {
                // the write is accumulating, still need to throttle
                checker.on_start_l0_files = false;
            } else {
                // still on start, should not throttle now
                return;
            }
        }

        if let Some(throttle_cf) = self.throttle_cf.as_ref() {
            if &cf != throttle_cf {
                // to avoid throttle cf changes back and forth, only change it
                // when the other is much higher.
                if num_l0_files
                    > self.cf_checkers[throttle_cf]
                        .long_term_num_l0_files
                        .get_max()
                        + 4
                {
                    SCHED_THROTTLE_ACTION_COUNTER
                        .with_label_values(&[&cf, "change_throttle_cf"])
                        .inc();
                    self.throttle_cf = Some(cf.clone());
                } else {
                    return;
                }
            }
        }

        let checker = self.cf_checkers.get_mut(&cf).unwrap();
        if checker.memtable_init_speed {
            return;
        }

        let is_throttled = self.limiter.speed_limit() != f64::INFINITY;
        let should_throttle = checker.long_term_num_l0_files.get_recent() > self.l0_files_threshold;

        let throttle = if !is_throttled && should_throttle {
            SCHED_THROTTLE_ACTION_COUNTER
                .with_label_values(&[&cf, "init"])
                .inc();
            self.throttle_cf = Some(cf.clone());
            let x = if self.last_speed < f64::EPSILON {
                self.write_Causetxctx_recorder.get_percentile_90() as f64
            } else {
                self.last_speed
            };
            if x < f64::EPSILON { f64::INFINITY } else { x }
        } else if is_throttled && should_throttle {
            self.limiter.speed_limit() * K_INC_SLOWDOWN_RATIO
        } else if is_throttled && !should_throttle {
            self.last_speed = self.limiter.speed_limit() * K_DEC_SLOWDOWN_RATIO;
            f64::INFINITY
        } else {
            f64::INFINITY
        };

        self.update_speed_limit(throttle)
    }

    fn update_speed_limit(&mut self, mut throttle: f64) {
        if throttle < MIN_THROTTLE_SPEED {
            throttle = MIN_THROTTLE_SPEED;
        }
        if throttle > MAX_THROTTLE_SPEED {
            self.throttle_cf = None;
            throttle = f64::INFINITY;
        }
        SCHED_THROTTLE_Causetxctx_GAUGE.set(if throttle == f64::INFINITY {
            0
        } else {
            throttle as i64
        });
        self.limiter.set_speed_limit(throttle)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use einsteindb-gen::Result;
    use std::sync::causetxctx::causetxctxU64;

    #[derive(Clone)]
    struct EngineStub(Arc<EngineStubInner>);

    struct EngineStubInner {
        pub pending_compaction_bytes: causetxctxU64,
        pub num_l0_files: causetxctxU64,
        pub num_memtable_files: causetxctxU64,
    }

    impl EngineStub {
        fn new() -> Self {
            Self(Arc::new(EngineStubInner {
                pending_compaction_bytes: causetxctxU64::new(0),
                num_l0_files: causetxctxU64::new(0),
                num_memtable_files: causetxctxU64::new(0),
            }))
        }
    }

    impl CFNamesExt for EngineStub {
        fn cf_names(&self) -> Vec<&str> {
            vec!["default"]
        }
    }

    impl CausetxctxControlFactorsExt for EngineStub {
        fn get_cf_num_files_at_level(&self, _cf: &str, _level: usize) -> Result<Option<u64>> {
            Ok(Some(self.0.num_l0_files.load(Ordering::Relaxed)))
        }

        fn get_cf_num_immutable_mem_table(&self, _cf: &str) -> Result<Option<u64>> {
            Ok(Some(self.0.num_memtable_files.load(Ordering::Relaxed)))
        }

        fn get_cf_pending_compaction_bytes(&self, _cf: &str) -> Result<Option<u64>> {
            Ok(Some(
                self.0.pending_compaction_bytes.load(Ordering::Relaxed),
            ))
        }
    }

    #[test]
    fn test_Causetxctx_controller_basic() {
        let stub = EngineStub::new();
        let (_tx, rx) = mpsc::channel();
        let Causetxctx_controller = CausetxctxController::new(&CausetxctxControlConfig::default(), stub, rx);

        // enable Causetxctx controller
        assert_eq!(Causetxctx_controller.enabled(), true);
        assert_eq!(Causetxctx_controller.should_drop(), false);
        assert_eq!(Causetxctx_controller.is_unlimited(), true);
        assert_eq!(Causetxctx_controller.consume(0), Duration::ZERO);
        assert_eq!(Causetxctx_controller.consume(1000), Duration::ZERO);

        // disable Causetxctx controller
        Causetxctx_controller.enable(false);
        assert_eq!(Causetxctx_controller.enabled(), false);
        // re-enable Causetxctx controller
        Causetxctx_controller.enable(true);
        assert_eq!(Causetxctx_controller.enabled(), true);
        assert_eq!(Causetxctx_controller.should_drop(), false);
        assert_eq!(Causetxctx_controller.is_unlimited(), true);
        assert_eq!(Causetxctx_controller.consume(1), Duration::ZERO);
    }

    #[test]
    fn test_Causetxctx_controller_memtable() {
        let stub = EngineStub::new();
        let (tx, rx) = mpsc::sync_channel(0);
        let Causetxctx_controller = CausetxctxController::new(&CausetxctxControlConfig::default(), stub.clone(), rx);

        assert_eq!(Causetxctx_controller.consume(2000), Duration::ZERO);
        loop {
            if Causetxctx_controller.total_bytes_consumed() == 0 {
                break;
            }
            std::thread::sleep(TICK_DURATION);
        }

        // exceeds the threshold on start
        stub.0.num_memtable_files.store(8, Ordering::Relaxed);
        tx.send(CausetxctxInfo::Flush("default".to_string(), 0)).unwrap();
        tx.send(CausetxctxInfo::L0Intra("default".to_string(), 0))
            .unwrap();
        assert_eq!(Causetxctx_controller.should_drop(), false);
        // on start check forbids Causetxctx control
        assert_eq!(Causetxctx_controller.is_unlimited(), true);
        // once falls below the threshold, pass the on start check
        stub.0.num_memtable_files.store(1, Ordering::Relaxed);
        tx.send(CausetxctxInfo::Flush("default".to_string(), 0)).unwrap();
        tx.send(CausetxctxInfo::L0Intra("default".to_string(), 0))
            .unwrap();
        // not throttle when the average of the sliding window doesn't exceeds the threshold
        stub.0.num_memtable_files.store(6, Ordering::Relaxed);
        tx.send(CausetxctxInfo::Flush("default".to_string(), 0)).unwrap();
        tx.send(CausetxctxInfo::L0Intra("default".to_string(), 0))
            .unwrap();
        assert_eq!(Causetxctx_controller.should_drop(), false);
        assert_eq!(Causetxctx_controller.is_unlimited(), true);

        // the average of sliding window exceeds the threshold
        stub.0.num_memtable_files.store(6, Ordering::Relaxed);
        tx.send(CausetxctxInfo::Flush("default".to_string(), 0)).unwrap();
        tx.send(CausetxctxInfo::L0Intra("default".to_string(), 0))
            .unwrap();
        assert_eq!(Causetxctx_controller.should_drop(), false);
        assert_eq!(Causetxctx_controller.is_unlimited(), false);
        assert_ne!(Causetxctx_controller.consume(2000), Duration::ZERO);

        // not throttle once the number of memtables falls below the threshold
        stub.0.num_memtable_files.store(1, Ordering::Relaxed);
        tx.send(CausetxctxInfo::Flush("default".to_string(), 0)).unwrap();
        tx.send(CausetxctxInfo::L0Intra("default".to_string(), 0))
            .unwrap();
        assert_eq!(Causetxctx_controller.should_drop(), false);
        assert_eq!(Causetxctx_controller.is_unlimited(), true);
    }

    #[test]
    fn test_Causetxctx_controller_l0() {
        let stub = EngineStub::new();
        let (tx, rx) = mpsc::sync_channel(0);
        let Causetxctx_controller = CausetxctxController::new(&CausetxctxControlConfig::default(), stub.clone(), rx);

        assert_eq!(Causetxctx_controller.consume(2000), Duration::ZERO);
        loop {
            if Causetxctx_controller.total_bytes_consumed() == 0 {
                break;
            }
            std::thread::sleep(TICK_DURATION);
        }

        // exceeds the threshold
        stub.0.num_l0_files.store(30, Ordering::Relaxed);
        tx.send(CausetxctxInfo::L0("default".to_string(), 0)).unwrap();
        tx.send(CausetxctxInfo::L0Intra("default".to_string(), 0))
            .unwrap();
        assert_eq!(Causetxctx_controller.should_drop(), false);
        // on start check forbids Causetxctx control
        assert_eq!(Causetxctx_controller.is_unlimited(), true);
        // once fall below the threshold, pass the on start check
        stub.0.num_l0_files.store(10, Ordering::Relaxed);
        tx.send(CausetxctxInfo::L0("default".to_string(), 0)).unwrap();
        tx.send(CausetxctxInfo::L0Intra("default".to_string(), 0))
            .unwrap();

        // exceeds the threshold, throttle now
        stub.0.num_l0_files.store(30, Ordering::Relaxed);
        tx.send(CausetxctxInfo::L0("default".to_string(), 0)).unwrap();
        tx.send(CausetxctxInfo::L0Intra("default".to_string(), 0))
            .unwrap();
        assert_eq!(Causetxctx_controller.should_drop(), false);
        assert_eq!(Causetxctx_controller.is_unlimited(), false);
        assert_ne!(Causetxctx_controller.consume(2000), Duration::ZERO);
    }

    #[test]
    fn test_Causetxctx_controller_pending_compaction_bytes() {
        let stub = EngineStub::new();
        let (tx, rx) = mpsc::sync_channel(0);
        let Causetxctx_controller = CausetxctxController::new(&CausetxctxControlConfig::default(), stub.clone(), rx);

        // exceeds the threshold
        stub.0
            .pending_compaction_bytes
            .store(1000 * 1024 * 1024 * 1024, Ordering::Relaxed);
        tx.send(CausetxctxInfo::Compaction("default".to_string()))
            .unwrap();
        tx.send(CausetxctxInfo::L0Intra("default".to_string(), 0))
            .unwrap();
        // on start check forbids Causetxctx control
        assert!(Causetxctx_controller.discard_ratio() < f64::EPSILON);
        // once fall below the threshold, pass the on start check
        stub.0
            .pending_compaction_bytes
            .store(100 * 1024 * 1024 * 1024, Ordering::Relaxed);
        tx.send(CausetxctxInfo::Compaction("default".to_string()))
            .unwrap();
        tx.send(CausetxctxInfo::L0Intra("default".to_string(), 0))
            .unwrap();

        stub.0
            .pending_compaction_bytes
            .store(1000 * 1024 * 1024 * 1024, Ordering::Relaxed);
        tx.send(CausetxctxInfo::Compaction("default".to_string()))
            .unwrap();
        tx.send(CausetxctxInfo::L0Intra("default".to_string(), 0))
            .unwrap();
        assert!(Causetxctx_controller.discard_ratio() > f64::EPSILON);

        stub.0
            .pending_compaction_bytes
            .store(1024 * 1024 * 1024, Ordering::Relaxed);
        tx.send(CausetxctxInfo::Compaction("default".to_string()))
            .unwrap();
        tx.send(CausetxctxInfo::L0Intra("default".to_string(), 0))
            .unwrap();
        assert!(Causetxctx_controller.discard_ratio() < f64::EPSILON);

        // pending compaction bytes jump after unsafe destroy range
        tx.send(CausetxctxInfo::BeforeUnsafeDestroyRange).unwrap();
        tx.send(CausetxctxInfo::L0Intra("default".to_string(), 0))
            .unwrap();
        assert!(Causetxctx_controller.discard_ratio() < f64::EPSILON);

        // during unsafe destroy range, pending compaction bytes may change
        stub.0
            .pending_compaction_bytes
            .store(1024 * 1024 * 1024, Ordering::Relaxed);
        tx.send(CausetxctxInfo::Compaction("default".to_string()))
            .unwrap();
        tx.send(CausetxctxInfo::L0Intra("default".to_string(), 0))
            .unwrap();
        assert!(Causetxctx_controller.discard_ratio() < f64::EPSILON);

        stub.0
            .pending_compaction_bytes
            .store(10000000 * 1024 * 1024 * 1024, Ordering::Relaxed);
        tx.send(CausetxctxInfo::Compaction("default".to_string()))
            .unwrap();
        tx.send(CausetxctxInfo::AfterUnsafeDestroyRange).unwrap();
        tx.send(CausetxctxInfo::L0Intra("default".to_string(), 0))
            .unwrap();
        assert!(Causetxctx_controller.discard_ratio() < f64::EPSILON);

        // unfreeze the control
        stub.0
            .pending_compaction_bytes
            .store(1024 * 1024, Ordering::Relaxed);
        tx.send(CausetxctxInfo::Compaction("default".to_string()))
            .unwrap();
        tx.send(CausetxctxInfo::L0Intra("default".to_string(), 0))
            .unwrap();
        assert!(Causetxctx_controller.discard_ratio() < f64::EPSILON);

        stub.0
            .pending_compaction_bytes
            .store(1000000000 * 1024 * 1024 * 1024, Ordering::Relaxed);
        tx.send(CausetxctxInfo::Compaction("default".to_string()))
            .unwrap();
        tx.send(CausetxctxInfo::L0Intra("default".to_string(), 0))
            .unwrap();
        assert!(Causetxctx_controller.discard_ratio() > f64::EPSILON);
    }

    #[test]
    fn test_smoother() {
        let mut smoother = Smoother::<u64, 5, SMOOTHER_STALE_RECORD_THRESHOLD, 0>::default();
        smoother.observe(1);
        smoother.observe(6);
        smoother.observe(2);
        smoother.observe(3);
        smoother.observe(4);
        smoother.observe(5);
        smoother.observe(0);

        assert!((smoother.get_avg() - 2.8).abs() < f64::EPSILON);
        assert_eq!(smoother.get_recent(), 0);
        assert_eq!(smoother.get_max(), 5);
        assert_eq!(smoother.get_percentile_90(), 4);
        assert_eq!(smoother.trend(), Trend::NoTrend);

        let mut smoother = Smoother::<f64, 5, SMOOTHER_STALE_RECORD_THRESHOLD, 0>::default();
        smoother.observe(1.0);
        smoother.observe(6.0);
        smoother.observe(2.0);
        smoother.observe(3.0);
        smoother.observe(4.0);
        smoother.observe(5.0);
        smoother.observe(9.0);
        assert!((smoother.get_avg() - 4.6).abs() < f64::EPSILON);
        assert!((smoother.get_recent() - 9.0).abs() < f64::EPSILON);
        assert!((smoother.get_max() - 9.0).abs() < f64::EPSILON);
        assert!((smoother.get_percentile_90() - 5.0).abs() < f64::EPSILON);
        assert_eq!(smoother.trend(), Trend::Increasing);
    }

    #[test]
    fn test_smoother_trend() {
        // The time range is not enough
        let mut smoother = Smoother::<
            u64,
            6,
            SMOOTHER_STALE_RECORD_THRESHOLD,
            SMOOTHER_TIME_RANGE_THRESHOLD,
        >::default();
        let now = Instant::now_coarse();
        smoother.observe_with_time(
            1,
            now.sub(Duration::from_secs(SMOOTHER_TIME_RANGE_THRESHOLD - 1)),
        );
        smoother.observe_with_time(
            1,
            now.sub(Duration::from_secs(SMOOTHER_TIME_RANGE_THRESHOLD - 2)),
        );
        smoother.observe_with_time(
            1,
            now.sub(Duration::from_secs(SMOOTHER_TIME_RANGE_THRESHOLD - 3)),
        );
        smoother.observe_with_time(4, now.sub(Duration::from_secs(2)));
        smoother.observe_with_time(4, now.sub(Duration::from_secs(1)));
        smoother.observe_with_time(4, now);
        assert_eq!(smoother.trend(), Trend::NoTrend);

        // Incresing trend, the left range contains 3 records, the right range contains 1 records.
        let mut smoother = Smoother::<
            f64,
            6,
            SMOOTHER_STALE_RECORD_THRESHOLD,
            SMOOTHER_TIME_RANGE_THRESHOLD,
        >::default();
        smoother.observe_with_time(
            1.0,
            now.sub(Duration::from_secs(SMOOTHER_TIME_RANGE_THRESHOLD + 1)),
        );
        smoother.observe_with_time(
            1.0,
            now.sub(Duration::from_secs(SMOOTHER_TIME_RANGE_THRESHOLD)),
        );
        smoother.observe_with_time(
            1.0,
            now.sub(Duration::from_secs(SMOOTHER_TIME_RANGE_THRESHOLD - 1)),
        );
        smoother.observe_with_time(4.0, now);
        assert_eq!(smoother.trend(), Trend::Increasing);

        // Decreasing trend, the left range contains 1 records, the right range contains 3 records.
        let mut smoother = Smoother::<
            f32,
            6,
            SMOOTHER_STALE_RECORD_THRESHOLD,
            SMOOTHER_TIME_RANGE_THRESHOLD,
        >::default();
        smoother.observe_with_time(
            4.0,
            now.sub(Duration::from_secs(SMOOTHER_TIME_RANGE_THRESHOLD + 1)),
        );
        smoother.observe_with_time(1.0, now.sub(Duration::from_secs(2)));
        smoother.observe_with_time(2.0, now.sub(Duration::from_secs(1)));
        smoother.observe_with_time(1.0, now);
        assert_eq!(smoother.trend(), Trend::Decreasing);

        // No trend, the left range contains 1 records, the right range contains 3 records.
        let mut smoother = Smoother::<
            f32,
            6,
            SMOOTHER_STALE_RECORD_THRESHOLD,
            SMOOTHER_TIME_RANGE_THRESHOLD,
        >::default();
        smoother.observe_with_time(
            1.0,
            now.sub(Duration::from_secs(SMOOTHER_TIME_RANGE_THRESHOLD + 1)),
        );
        smoother.observe_with_time(1.0, now.sub(Duration::from_secs(2)));
        smoother.observe_with_time(3.0, now.sub(Duration::from_secs(1)));
        smoother.observe_with_time(2.0, now);
        assert_eq!(smoother.trend(), Trend::NoTrend);

        // No trend, because the latest record is too old
        let mut smoother = Smoother::<
            u32,
            6,
            SMOOTHER_STALE_RECORD_THRESHOLD,
            SMOOTHER_TIME_RANGE_THRESHOLD,
        >::default();
        smoother.observe_with_time(
            1,
            now.sub(Duration::from_secs(SMOOTHER_STALE_RECORD_THRESHOLD + 1)),
        );
        assert_eq!(smoother.trend(), Trend::NoTrend);
    }
}
