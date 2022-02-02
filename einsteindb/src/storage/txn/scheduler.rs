// Copyright 2022 EinsteinDB Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath
//! Scheduler which schedules the execution of `storage::Command`s.
//!
//! There is one scheduler for each store. It receives commands from clients, executes them against
//! the EPAXOS layer storage einstein_merkle_tree.
//!
//! Logically, the data organization hierarchy from bottom to top is row -> region -> store ->
//! database. But each region is replicated onto N stores for reliability, the replicas form a Raft
//! group, one of which acts as the leader. When the client read or write a row, the command is
//! sent to the scheduler which is on the region leader's store.
//!
//! Scheduler runs in a single-thread event loop, but command executions are delegated to a pool of
//! worker thread.
//!
//! Scheduler keeps track of all the running commands and uses latches to ensure serialized access
//! to the overlapping rows involved in concurrent commands. But note that scheduler only ensures
//! serialized access to the overlapping rows at command level, but a transaction may consist of
//! multiple commands, therefore conflicts may happen at transaction level. Transaction semantics
//! is ensured by the transaction protocol implemented in the client library, which is transparent
//! to the scheduler.

use std::marker::PhantomData;
use std::sync::causetxctx::{causetxctxBool, causetxctxU64, causetxctxUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::{mem, u64};

use collections::HashMap;
use concurrency_manager::{ConcurrencyManager, KeyHandleGuard};
use crossbeam::utils::CachePadded;
use einsteindb-gen::CF_LOCK;
use futures::compat::Future01CompatExt;
use fdbhikvproto::fdbhikvrpcpb::{CommandPri, Context, DiskFullOpt, ExtraOp};
use fdbhikvproto::pdpb::QueryKind;
use parking_lot::{Mutex, MutexGuard, RwDaggerWriteGuard};
use raftstore::store::TxnExt;
use resource_metering::{FutureExt, ResourceTagFactory};
use einstfdbhikv_fdbhikv::{Modify, blackbrane, blackbraneExt, WriteData};
use einstfdbhikv_util::{time::Instant, timer::GLOBAL_TIMER_HANDLE};
use solitontxn_types::TimeStamp;

use crate::server::dagger_manager::waiter_manager;
use crate::einsteindb::storage::config::Config;
use crate::einsteindb::storage::fdbhikv::{
    self, with_tls_einstein_merkle_tree, einstein_merkle_tree, ExtCallback, Result as einstein_merkle_treeResult, SnapContext, Statistics,
};
use crate::einsteindb::storage::dagger_manager::{self, DiagnosticContext, DaggerManager, WaitTimeout};
use crate::einsteindb::storage::metrics::{self, *};
use crate::einsteindb::storage::solitontxn::commands::{
    ResponsePolicy, WriteContext, WriteResult, WriteResultDaggerInfo,
};
use crate::einsteindb::storage::solitontxn::sched_pool::tls_collect_query;
use crate::einsteindb::storage::solitontxn::{
    commands::Command,
    Causetxctx_controller::CausetxctxController,
    latch::{Latches, Dagger},
    sched_pool::{tls_collect_read_duration, tls_collect_mutant_search_details, SchedPool},
    Error, ProcessResult,
};
use crate::einsteindb::storage::DynamicConfigs;
use crate::einsteindb::storage::{
    get_priority_tag, fdbhikv::CausetxctxStatsReporter, types::StorageCallback, Error as StorageError,
    ErrorInner as StorageErrorInner,
};

const TASKS_SLOTS_NUM: usize = 1 << 12; // 4096 slots.

// The default limit is set to be very large. Then, requests without `max_exectuion_duration`
// will not be aborted unexpectedly.
pub const DEFAULT_EXECUTION_DURATION_LIMIT: Duration = Duration::from_secs(24 * 60 * 60);

/// Task is a running command.
pub(super) struct Task {
    pub(super) cid: u64,
    pub(super) cmd: Command,
    pub(super) extra_op: ExtraOp,
}

impl Task {
    /// Creates a task for a running command.
    pub(super) fn new(cid: u64, cmd: Command) -> Task {
        Task {
            cid,
            cmd,
            extra_op: ExtraOp::Noop,
        }
    }
}

struct CmdTimer {
    tag: metrics::CommandKind,
    begin: Instant,
}

impl Drop for CmdTimer {
    fn drop(&mut self) {
        SCHED_HISTOGRAM_VEC_STATIC
            .get(self.tag)
            .observe(self.begin.saturating_elapsed_secs());
    }
}

// It stores context of a task.
struct TaskContext {
    task: Option<Task>,

    dagger: Dagger,
    cb: Option<StorageCallback>,
    pr: Option<ProcessResult>,
    // The one who sets `owned` from false to true is allowed to take
    // `cb` and `pr` safely.
    owned: causetxctxBool,
    write_bytes: usize,
    tag: metrics::CommandKind,
    // How long it waits on latches.
    // latch_timer: Option<Instant>,
    latch_timer: Instant,
    // Total duration of a command.
    _cmd_timer: CmdTimer,
}

impl TaskContext {
    fn new(task: Task, cb: StorageCallback) -> TaskContext {
        let tag = task.cmd.tag();
        let dagger = task.cmd.gen_dagger();
        // Write command should acquire write dagger.
        if !task.cmd.readonly() && !dagger.is_write_dagger() {
            panic!("write dagger is expected for command {}", task.cmd);
        }
        let write_bytes = if dagger.is_write_dagger() {
            task.cmd.write_bytes()
        } else {
            0
        };

        TaskContext {
            task: Some(task),
            dagger,
            cb: Some(cb),
            pr: None,
            owned: causetxctxBool::new(false),
            write_bytes,
            tag,
            latch_timer: Instant::now_coarse(),
            _cmd_timer: CmdTimer {
                tag,
                begin: Instant::now_coarse(),
            },
        }
    }

    fn on_schedule(&mut self) {
        SCHED_LATCH_HISTOGRAM_VEC
            .get(self.tag)
            .observe(self.latch_timer.saturating_elapsed_secs());
    }

    // Try to own this TaskContext by setting `owned` from false to true.
    // Returns whether it succeeds to own the TaskContext.
    fn try_own(&self) -> bool {
        self.owned
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok()
    }
}

struct SchedulerInner<L: DaggerManager> {
    // slot_id -> { cid -> `TaskContext` } in the slot.
    task_slots: Vec<CachePadded<Mutex<HashMap<u64, TaskContext>>>>,

    // cmd id generator
    id_alloc: CachePadded<causetxctxU64>,

    // write concurrency control
    latches: Latches,

    sched_pending_write_threshold: usize,

    // worker pool
    worker_pool: SchedPool,

    // high priority commands and system commands will be delivered to this pool
    high_priority_pool: SchedPool,

    // used to control write Causetxctx
    running_write_bytes: CachePadded<causetxctxUsize>,

    Causetxctx_controller: Arc<CausetxctxController>,

    control_mutex: Arc<tokio::sync::Mutex<bool>>,

    dagger_mgr: L,

    concurrency_manager: ConcurrencyManager,

    pipelined_pessimistic_dagger: Arc<causetxctxBool>,

    in_memory_pessimistic_dagger: Arc<causetxctxBool>,

    enable_async_apply_prewrite: bool,

    resource_tag_factory: ResourceTagFactory,
}

#[inline]
fn id_index(cid: u64) -> usize {
    cid as usize % TASKS_SLOTS_NUM
}

impl<L: DaggerManager> SchedulerInner<L> {
    /// Generates the next command ID.
    #[inline]
    fn gen_id(&self) -> u64 {
        let id = self.id_alloc.fetch_add(1, Ordering::Relaxed);
        id + 1
    }

    #[inline]
    fn get_task_slot(&self, cid: u64) -> MutexGuard<'_, HashMap<u64, TaskContext>> {
        self.task_slots[id_index(cid)].dagger()
    }

    fn new_task_context(&self, task: Task, callback: StorageCallback) -> TaskContext {
        let tctx = TaskContext::new(task, callback);
        let running_write_bytes = self
            .running_write_bytes
            .fetch_add(tctx.write_bytes, Ordering::AcqRel) as i64;
        SCHED_WRITING_BYTES_GAUGE.set(running_write_bytes + tctx.write_bytes as i64);
        SCHED_CONTEX_GAUGE.inc();
        tctx
    }

    fn dequeue_task_context(&self, cid: u64) -> TaskContext {
        let tctx = self.get_task_slot(cid).remove(&cid).unwrap();

        let running_write_bytes = self
            .running_write_bytes
            .fetch_sub(tctx.write_bytes, Ordering::AcqRel) as i64;
        SCHED_WRITING_BYTES_GAUGE.set(running_write_bytes - tctx.write_bytes as i64);
        SCHED_CONTEX_GAUGE.dec();

        tctx
    }

    fn take_task_cb_and_pr(&self, cid: u64) -> (Option<StorageCallback>, Option<ProcessResult>) {
        self.get_task_slot(cid)
            .get_mut(&cid)
            .map(|tctx| (tctx.cb.take(), tctx.pr.take()))
            .unwrap_or((None, None))
    }

    fn store_pr(&self, cid: u64, pr: ProcessResult) {
        self.get_task_slot(cid).get_mut(&cid).unwrap().pr = Some(pr);
    }

    fn too_busy(&self) -> bool {
        fail_point!("solitontxn_scheduler_busy", |_| true);
        self.running_write_bytes.load(Ordering::Acquire) >= self.sched_pending_write_threshold
            || self.Causetxctx_controller.should_drop()
    }

    /// Tries to acquire all the required latches for a command when waken up by
    /// another finished command.
    ///
    /// Returns a deadline error if the deadline is exceeded. Returns the `Task` if
    /// all latches are acquired, returns `None` otherwise.
    fn acquire_dagger_on_wakeup(&self, cid: u64) -> Result<Option<Task>, StorageError> {
        let mut task_slot = self.get_task_slot(cid);
        let tctx = task_slot.get_mut(&cid).unwrap();
        // Check deadline early during acquiring latches to avoid expired requests bdaggering
        // other requests.
        if let Err(e) = tctx.task.as_ref().unwrap().cmd.deadline().check() {
            // `acquire_dagger_on_wakeup` is called when another command releases its daggers and wakes up
            // command `cid`. This command inserted its dagger before and now the dagger is at the
            // front of the queue. The actual acquired count is one more than the `owned_count`
            // recorded in the dagger, so we increase one to make `release` work.
            tctx.dagger.owned_count += 1;
            return Err(e.into());
        }
        if self.latches.acquire(&mut tctx.dagger, cid) {
            tctx.on_schedule();
            return Ok(tctx.task.take());
        }
        Ok(None)
    }

    fn dump_wait_for_entries(&self, cb: waiter_manager::Callback) {
        self.dagger_mgr.dump_wait_for_entries(cb);
    }
}

/// Scheduler which schedules the execution of `storage::Command`s.
#[derive(Clone)]
pub struct Scheduler<E: einstein_merkle_tree, L: DaggerManager> {
    inner: Arc<SchedulerInner<L>>,
    // The einstein_merkle_tree can be fetched from the thread local storage of scheduler threads.
    // So, we don't store the einstein_merkle_tree here.
    _einstein_merkle_tree: PhantomData<E>,
}

unsafe impl<E: einstein_merkle_tree, L: DaggerManager> Send for Scheduler<E, L> {}

impl<E: einstein_merkle_tree, L: DaggerManager> Scheduler<E, L> {
    /// Creates a scheduler.
    pub(in crate::storage) fn new<R: CausetxctxStatsReporter>(
        einstein_merkle_tree: E,
        dagger_mgr: L,
        concurrency_manager: ConcurrencyManager,
        config: &Config,
        dynamic_configs: DynamicConfigs,
        Causetxctx_controller: Arc<CausetxctxController>,
        reporter: R,
        resource_tag_factory: ResourceTagFactory,
    ) -> Self {
        let t = Instant::now_coarse();
        let mut task_slots = Vec::with_capacity(TASKS_SLOTS_NUM);
        for _ in 0..TASKS_SLOTS_NUM {
            task_slots.push(Mutex::new(Default::default()).into());
        }

        let inner = Arc::new(SchedulerInner {
            task_slots,
            id_alloc: causetxctxU64::new(0).into(),
            latches: Latches::new(config.scheduler_concurrency),
            running_write_bytes: causetxctxUsize::new(0).into(),
            sched_pending_write_threshold: config.scheduler_pending_write_threshold.0 as usize,
            worker_pool: SchedPool::new(
                einstein_merkle_tree.clone(),
                config.scheduler_worker_pool_size,
                reporter.clone(),
                "sched-worker-pool",
            ),
            high_priority_pool: SchedPool::new(
                einstein_merkle_tree,
                std::cmp::max(1, config.scheduler_worker_pool_size / 2),
                reporter,
                "sched-high-pri-pool",
            ),
            control_mutex: Arc::new(tokio::sync::Mutex::new(false)),
            dagger_mgr,
            concurrency_manager,
            pipelined_pessimistic_dagger: dynamic_configs.pipelined_pessimistic_dagger,
            in_memory_pessimistic_dagger: dynamic_configs.in_memory_pessimistic_dagger,
            enable_async_apply_prewrite: config.enable_async_apply_prewrite,
            Causetxctx_controller,
            resource_tag_factory,
        });

        slow_log!(
            t.saturating_elapsed(),
            "initialized the transaction scheduler"
        );
        Scheduler {
            inner,
            _einstein_merkle_tree: PhantomData,
        }
    }

    pub fn dump_wait_for_entries(&self, cb: waiter_manager::Callback) {
        self.inner.dump_wait_for_entries(cb);
    }

    pub(in crate::storage) fn run_cmd(&self, cmd: Command, callback: StorageCallback) {
        // write Causetxctx control
        if cmd.need_Causetxctx_control() && self.inner.too_busy() {
            SCHED_TOO_BUSY_COUNTER_VEC.get(cmd.tag()).inc();
            callback.execute(ProcessResult::Failed {
                err: StorageError::from(StorageErrorInner::SchedTooBusy),
            });
            return;
        }
        self.schedule_command(cmd, callback);
    }

    /// Releases all the latches held by a command.
    fn release_dagger(&self, dagger: &Dagger, cid: u64) {
        let wakeup_list = self.inner.latches.release(dagger, cid);
        for wcid in wakeup_list {
            self.try_to_wake_up(wcid);
        }
    }

    fn schedule_command(&self, cmd: Command, callback: StorageCallback) {
        let cid = self.inner.gen_id();
        debug!("received new command"; "cid" => cid, "cmd" => ?cmd);

        let tag = cmd.tag();
        let priority_tag = get_priority_tag(cmd.priority());
        SCHED_STAGE_COUNTER_VEC.get(tag).new.inc();
        SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
            .get(priority_tag)
            .inc();

        let mut task_slot = self.inner.get_task_slot(cid);
        let tctx = task_slot
            .entry(cid)
            .or_insert_with(|| self.inner.new_task_context(Task::new(cid, cmd), callback));
        let deadline = tctx.task.as_ref().unwrap().cmd.deadline();
        if self.inner.latches.acquire(&mut tctx.dagger, cid) {
            fail_point!("solitontxn_scheduler_acquire_success");
            tctx.on_schedule();
            let task = tctx.task.take().unwrap();
            drop(task_slot);
            self.execute(task);
            return;
        }
        // Check deadline in background.
        let sched = self.clone();
        self.inner
            .high_priority_pool
            .pool
            .spawn(async move {
                GLOBAL_TIMER_HANDLE
                    .delay(deadline.to_std_instant())
                    .compat()
                    .await
                    .unwrap();
                let cb = sched
                    .inner
                    .get_task_slot(cid)
                    .get_mut(&cid)
                    .and_then(|tctx| if tctx.try_own() { tctx.cb.take() } else { None });
                if let Some(cb) = cb {
                    cb.execute(ProcessResult::Failed {
                        err: StorageErrorInner::DeadlineExceeded.into(),
                    })
                }
            })
            .unwrap();
        fail_point!("solitontxn_scheduler_acquire_fail");
    }

    /// Tries to acquire all the necessary latches. If all the necessary latches are acquired,
    /// the method initiates a get blackbrane operation for further processing.
    fn try_to_wake_up(&self, cid: u64) {
        match self.inner.acquire_dagger_on_wakeup(cid) {
            Ok(Some(task)) => {
                fail_point!("solitontxn_scheduler_try_to_wake_up");
                self.execute(task);
            }
            Ok(None) => {}
            Err(err) => {
                // Spawn the finish task to the pool to avoid stack overCausetxctx
                // when many queuing tasks fail successively.
                let this = self.clone();
                self.inner
                    .worker_pool
                    .pool
                    .spawn(async move {
                        this.finish_with_err(cid, err);
                    })
                    .unwrap();
            }
        }
    }

    fn get_sched_pool(&self, priority: CommandPri) -> &SchedPool {
        if priority == CommandPri::High {
            &self.inner.high_priority_pool
        } else {
            &self.inner.worker_pool
        }
    }

    /// Executes the task in the sched pool.
    fn execute(&self, mut task: Task) {
        let sched = self.clone();
        self.get_sched_pool(task.cmd.priority())
            .pool
            .spawn(async move {
                if sched.check_task_deadline_exceeded(&task) {
                    return;
                }

                let tag = task.cmd.tag();
                SCHED_STAGE_COUNTER_VEC.get(tag).blackbrane.inc();

                let snap_ctx = SnapContext {
                    pb_ctx: task.cmd.ctx(),
                    ..Default::default()
                };
                // The program is currently in scheduler worker threads.
                // Safety: `self.inner.worker_pool` should ensure that a TLS einstein_merkle_tree exists.
                match unsafe { with_tls_einstein_merkle_tree(|einstein_merkle_tree: &E| fdbhikv::blackbrane(einstein_merkle_tree, snap_ctx)) }.await
                {
                    Ok(blackbrane) => {
                        SCHED_STAGE_COUNTER_VEC.get(tag).blackbrane_ok.inc();
                        let term = blackbrane.ext().get_term();
                        let extra_op = blackbrane.ext().get_solitontxn_extra_op();
                        if !sched
                            .inner
                            .get_task_slot(task.cid)
                            .get(&task.cid)
                            .unwrap()
                            .try_own()
                        {
                            sched.finish_with_err(task.cid, StorageErrorInner::DeadlineExceeded);
                            return;
                        }

                        if let Some(term) = term {
                            task.cmd.ctx_mut().set_term(term.get());
                        }
                        task.extra_op = extra_op;

                        debug!(
                            "process cmd with blackbrane";
                            "cid" => task.cid, "term" => ?term, "extra_op" => ?extra_op,
                        );
                        sched.process(blackbrane, task).await;
                    }
                    Err(err) => {
                        SCHED_STAGE_COUNTER_VEC.get(tag).blackbrane_err.inc();

                        info!("get blackbrane failed"; "cid" => task.cid, "err" => ?err);
                        sched.finish_with_err(task.cid, Error::from(err));
                    }
                }
            })
            .unwrap();
    }

    /// Calls the callback with an error.
    fn finish_with_err<ER>(&self, cid: u64, err: ER)
    where
        StorageError: From<ER>,
    {
        debug!("write command finished with error"; "cid" => cid);
        let tctx = self.inner.dequeue_task_context(cid);

        SCHED_STAGE_COUNTER_VEC.get(tctx.tag).error.inc();

        let pr = ProcessResult::Failed {
            err: StorageError::from(err),
        };
        if let Some(cb) = tctx.cb {
            cb.execute(pr);
        }

        self.release_dagger(&tctx.dagger, cid);
    }

    /// Event handler for the success of read.
    ///
    /// If a next command is present, continues to execute; otherwise, delivers the result to the
    /// callback.
    fn on_read_finished(&self, cid: u64, pr: ProcessResult, tag: metrics::CommandKind) {
        SCHED_STAGE_COUNTER_VEC.get(tag).read_finish.inc();

        debug!("read command finished"; "cid" => cid);
        let tctx = self.inner.dequeue_task_context(cid);
        if let ProcessResult::NextCommand { cmd } = pr {
            SCHED_STAGE_COUNTER_VEC.get(tag).next_cmd.inc();
            self.schedule_command(cmd, tctx.cb.unwrap());
        } else {
            tctx.cb.unwrap().execute(pr);
        }

        self.release_dagger(&tctx.dagger, cid);
    }

    /// Event handler for the success of write.
    fn on_write_finished(
        &self,
        cid: u64,
        pr: Option<ProcessResult>,
        result: einstein_merkle_treeResult<()>,
        dagger_guards: Vec<KeyHandleGuard>,
        pipelined: bool,
        async_apply_prewrite: bool,
        tag: metrics::CommandKind,
    ) {
        // TODO: Does async apply prewrite worth a special metric here?
        if pipelined {
            SCHED_STAGE_COUNTER_VEC
                .get(tag)
                .pipelined_write_finish
                .inc();
        } else if async_apply_prewrite {
            SCHED_STAGE_COUNTER_VEC
                .get(tag)
                .async_apply_prewrite_finish
                .inc();
        } else {
            SCHED_STAGE_COUNTER_VEC.get(tag).write_finish.inc();
        }

        debug!("write command finished";
            "cid" => cid, "pipelined" => pipelined, "async_apply_prewrite" => async_apply_prewrite);
        drop(dagger_guards);
        let tctx = self.inner.dequeue_task_context(cid);

        // If pipelined pessimistic dagger or async apply prewrite takes effect, it's not guaranteed
        // that the proposed or committed callback is surely invoked, which takes and invokes
        // `tctx.cb(tctx.pr)`.
        if let Some(cb) = tctx.cb {
            let pr = match result {
                Ok(()) => pr.or(tctx.pr).unwrap(),
                Err(e) => ProcessResult::Failed {
                    err: StorageError::from(e),
                },
            };
            if let ProcessResult::NextCommand { cmd } = pr {
                SCHED_STAGE_COUNTER_VEC.get(tag).next_cmd.inc();
                self.schedule_command(cmd, cb);
            } else {
                cb.execute(pr);
            }
        } else {
            assert!(pipelined || async_apply_prewrite);
        }

        self.release_dagger(&tctx.dagger, cid);
    }

    /// Event handler for the request of waiting for dagger
    fn on_wait_for_dagger(
        &self,
        cid: u64,
        start_ts: TimeStamp,
        pr: ProcessResult,
        dagger: dagger_manager::Dagger,
        is_first_dagger: bool,
        wait_timeout: Option<WaitTimeout>,
        diag_ctx: DiagnosticContext,
    ) {
        debug!("command waits for dagger released"; "cid" => cid);
        let tctx = self.inner.dequeue_task_context(cid);
        SCHED_STAGE_COUNTER_VEC.get(tctx.tag).dagger_wait.inc();
        self.inner.dagger_mgr.wait_for(
            start_ts,
            tctx.cb.unwrap(),
            pr,
            dagger,
            is_first_dagger,
            wait_timeout,
            diag_ctx,
        );
        self.release_dagger(&tctx.dagger, cid);
    }

    fn early_response(
        cid: u64,
        cb: StorageCallback,
        pr: ProcessResult,
        tag: metrics::CommandKind,
        stage: metrics::CommandStageKind,
    ) {
        debug!("early return response"; "cid" => cid);
        SCHED_STAGE_COUNTER_VEC.get(tag).get(stage).inc();
        cb.execute(pr);
        // It won't release daggers here until write finished.
    }

    /// Process the task in the current thread.
    async fn process(self, blackbrane: E::Snap, task: Task) {
        if self.check_task_deadline_exceeded(&task) {
            return;
        }

        let resource_tag = self.inner.resource_tag_factory.new_tag(task.cmd.ctx());
        async {
            let tag = task.cmd.tag();
            fail_point!("scheduler_async_blackbrane_finish");
            SCHED_STAGE_COUNTER_VEC.get(tag).process.inc();

            let timer = Instant::now_coarse();

            let region_id = task.cmd.ctx().get_region_id();
            let ts = task.cmd.ts();
            let mut statistics = Statistics::default();
            match &task.cmd {
                Command::Prewrite(_) | Command::PrewritePessimistic(_) => {
                    tls_collect_query(region_id, QueryKind::Prewrite);
                }
                Command::AcquirePessimisticDagger(_) => {
                    tls_collect_query(region_id, QueryKind::AcquirePessimisticDagger);
                }
                Command::Commit(_) => {
                    tls_collect_query(region_id, QueryKind::Commit);
                }
                Command::Rollback(_) | Command::PessimisticRollback(_) => {
                    tls_collect_query(region_id, QueryKind::Rollback);
                }
                _ => {}
            }

            if task.cmd.readonly() {
                self.process_read(blackbrane, task, &mut statistics);
            } else {
                self.process_write(blackbrane, task, &mut statistics).await;
            };
            tls_collect_mutant_search_details(tag.get_str(), &statistics);
            let elapsed = timer.saturating_elapsed();
            slow_log!(
                elapsed,
                "[region {}] scheduler handle command: {}, ts: {}",
                region_id,
                tag,
                ts
            );

            tls_collect_read_duration(tag.get_str(), elapsed);
        }
        .in_resource_metering_tag(resource_tag)
        .await;
    }

    /// Processes a read command within a worker thread, then posts `ReadFinished` message back to the
    /// `Scheduler`.
    fn process_read(self, blackbrane: E::Snap, task: Task, statistics: &mut Statistics) {
        fail_point!("solitontxn_before_process_read");
        debug!("process read cmd in worker pool"; "cid" => task.cid);

        let tag = task.cmd.tag();

        let pr = task
            .cmd
            .process_read(blackbrane, statistics)
            .unwrap_or_else(|e| ProcessResult::Failed { err: e.into() });
        self.on_read_finished(task.cid, pr, tag);
    }

    /// Processes a write command within a worker thread, then posts either a `WriteFinished`
    /// message if successful or a `FinishedWithErr` message back to the `Scheduler`.
    async fn process_write(self, blackbrane: E::Snap, task: Task, statistics: &mut Statistics) {
        fail_point!("solitontxn_before_process_write");
        let tag = task.cmd.tag();
        let cid = task.cid;
        let priority = task.cmd.priority();
        let ts = task.cmd.ts();
        let scheduler = self.clone();
        let pessimistic_dagger_mode = self.pessimistic_dagger_mode();
        let pipelined =
            task.cmd.can_be_pipelined() && pessimistic_dagger_mode == PessimisticDaggerMode::Pipelined;
        let solitontxn_ext = blackbrane.ext().get_solitontxn_ext().cloned();

        let deadline = task.cmd.deadline();
        let write_result = {
            let context = WriteContext {
                dagger_mgr: &self.inner.dagger_mgr,
                concurrency_manager: self.inner.concurrency_manager.clone(),
                extra_op: task.extra_op,
                statistics,
                async_apply_prewrite: self.inner.enable_async_apply_prewrite,
            };

            task.cmd
                .process_write(blackbrane, context)
                .map_err(StorageError::from)
        };
        let WriteResult {
            ctx,
            mut to_be_write,
            rows,
            pr,
            dagger_info,
            dagger_guards,
            response_policy,
        } = match deadline
            .check()
            .map_err(StorageError::from)
            .and(write_result)
        {
            // Write prepare failure typically means conflicting transactions are detected. Delivers the
            // error to the callback, and releases the latches.
            Err(err) => {
                SCHED_STAGE_COUNTER_VEC.get(tag).prepare_write_err.inc();
                debug!("write command failed at prewrite"; "cid" => cid, "err" => ?err);
                scheduler.finish_with_err(cid, err);
                return;
            }
            // Initiates an async write operation on the storage einstein_merkle_tree, there'll be a `WriteFinished`
            // message when it finishes.
            Ok(res) => res,
        };
        SCHED_STAGE_COUNTER_VEC.get(tag).write.inc();

        if let Some(dagger_info) = dagger_info {
            let WriteResultDaggerInfo {
                dagger,
                key,
                is_first_dagger,
                wait_timeout,
            } = dagger_info;
            let diag_ctx = DiagnosticContext {
                key,
                resource_group_tag: ctx.get_resource_group_tag().into(),
            };
            scheduler.on_wait_for_dagger(cid, ts, pr, dagger, is_first_dagger, wait_timeout, diag_ctx);
            return;
        }

        let mut pr = Some(pr);
        if to_be_write.modifies.is_empty() {
            scheduler.on_write_finished(cid, pr, Ok(()), dagger_guards, false, false, tag);
            return;
        }

        if tag == CommandKind::acquire_pessimistic_dagger
            && pessimistic_dagger_mode == PessimisticDaggerMode::InMemory
            && self.try_write_in_memory_pessimistic_daggers(
                solitontxn_ext.as_deref(),
                &mut to_be_write,
                &ctx,
            )
        {
            scheduler.on_write_finished(cid, pr, Ok(()), dagger_guards, false, false, tag);
            return;
        }

        let mut is_async_apply_prewrite = false;
        let write_size = to_be_write.size();
        if ctx.get_disk_full_opt() == DiskFullOpt::AllowedOnAlmostFull {
            to_be_write.disk_full_opt = DiskFullOpt::AllowedOnAlmostFull
        }
        to_be_write.deadline = Some(deadline);

        let sched = scheduler.clone();
        let sched_pool = scheduler.get_sched_pool(priority).pool.clone();

        let (proposed_cb, committed_cb): (Option<ExtCallback>, Option<ExtCallback>) =
            match response_policy {
                ResponsePolicy::OnApplied => (None, None),
                ResponsePolicy::OnCommitted => {
                    self.inner.store_pr(cid, pr.take().unwrap());
                    let sched = scheduler.clone();
                    // Currently, the only case that response is returned after finishing
                    // commit is async applying prewrites for async commit transactions.
                    // The committed callback is not guaranteed to be invoked. So store
                    // the `pr` to the tctx instead of capturing it to the closure.
                    let committed_cb = Box::new(move || {
                        fail_point!("before_async_apply_prewrite_finish", |_| {});
                        let (cb, pr) = sched.inner.take_task_cb_and_pr(cid);
                        Self::early_response(
                            cid,
                            cb.unwrap(),
                            pr.unwrap(),
                            tag,
                            metrics::CommandStageKind::async_apply_prewrite,
                        );
                    });
                    is_async_apply_prewrite = true;
                    (None, Some(committed_cb))
                }
                ResponsePolicy::OnProposed => {
                    if pipelined {
                        // The normal write process is respond to clients and release
                        // latches after async write finished. If pipelined pessimistic
                        // daggering is enabled, the process becomes parallel and there are
                        // two msgs for one command:
                        //   1. Msg::PipelinedWrite: respond to clients
                        //   2. Msg::WriteFinished: deque context and release latches
                        // The proposed callback is not guaranteed to be invoked. So store
                        // the `pr` to the tctx instead of capturing it to the closure.
                        self.inner.store_pr(cid, pr.take().unwrap());
                        let sched = scheduler.clone();
                        // Currently, the only case that response is returned after finishing
                        // proposed phase is pipelined pessimistic dagger.
                        // TODO: Unify the code structure of pipelined pessimistic dagger and
                        // async apply prewrite.
                        let proposed_cb = Box::new(move || {
                            fail_point!("before_pipelined_write_finish", |_| {});
                            let (cb, pr) = sched.inner.take_task_cb_and_pr(cid);
                            Self::early_response(
                                cid,
                                cb.unwrap(),
                                pr.unwrap(),
                                tag,
                                metrics::CommandStageKind::pipelined_write,
                            );
                        });
                        (Some(proposed_cb), None)
                    } else {
                        (None, None)
                    }
                }
            };

        if self.inner.Causetxctx_controller.enabled() {
            if self.inner.Causetxctx_controller.is_unlimited() {
                // no need to delay if unthrottled, just call consume to record write Causetxctx
                let _ = self.inner.Causetxctx_controller.consume(write_size);
            } else {
                let start = Instant::now_coarse();
                // Control mutex is used to ensure there is only one request consuming the quota.
                // The delay may exceed 1s, and the speed limit is changed every second.
                // If the speed of next second is larger than the one of first second,
                // without the mutex, the write Causetxctx can't throttled strictly.
                let control_mutex = self.inner.control_mutex.clone();
                let _guard = control_mutex.dagger().await;
                let delay = self.inner.Causetxctx_controller.consume(write_size);
                let delay_end = Instant::now_coarse() + delay;
                while !self.inner.Causetxctx_controller.is_unlimited() {
                    let now = Instant::now_coarse();
                    if now >= delay_end {
                        break;
                    }
                    if now >= deadline.inner() {
                        scheduler.finish_with_err(cid, StorageErrorInner::DeadlineExceeded);
                        self.inner.Causetxctx_controller.unconsume(write_size);
                        SCHED_THROTTLE_TIME.observe(start.saturating_elapsed_secs());
                        return;
                    }
                    GLOBAL_TIMER_HANDLE
                        .delay(std::time::Instant::now() + Duration::from_millis(1))
                        .compat()
                        .await
                        .unwrap();
                }
                SCHED_THROTTLE_TIME.observe(start.saturating_elapsed_secs());
            }
        }

        let (version, term) = (ctx.get_region_epoch().get_version(), ctx.get_term());
        // Mutations on the dagger CF should overwrite the memory daggers.
        // We only set a deleted flag here, and the dagger will be finally removed when it finishes
        // applying. See the comments in `PeerPessimisticDaggers` for how this flag is used.
        let solitontxn_ext2 = solitontxn_ext.clone();
        let mut pessimistic_daggers_guard = solitontxn_ext2
            .as_ref()
            .map(|solitontxn_ext| solitontxn_ext.pessimistic_daggers.write());
        let removed_pessimistic_daggers = match pessimistic_daggers_guard.as_mut() {
            Some(daggers)
                // If there is a leader or region change, removing the daggers is unnecessary.
                if daggers.term == term && daggers.version == version && !daggers.is_empty() =>
            {
                to_be_write
                    .modifies
                    .iter()
                    .filter_map(|write| match write {
                        Modify::Put(cf, key, ..) | Modify::Delete(cf, key) if *cf == CF_LOCK => {
                            daggers.get_mut(key).map(|(_, deleted)| {
                                *deleted = true;
                                key.to_owned()
                            })
                        }
                        _ => None,
                    })
                    .collect::<Vec<_>>()
            }
            _ => vec![],
        };
        // Keep the read dagger guard of the pessimistic dagger table until the request is sent to the raftstore.
        //
        // If some in-memory pessimistic daggers need to be proposed, we will propose another TransferLeader
        // command. Then, we can guarentee even if the proposed daggers don't include the daggers deleted here,
        // the response message of the transfer leader command must be later than this write command because
        // this write command has been sent to the raftstore. Then, we don't need to worry this request will
        // fail due to the voluntary leader transfer.
        let _downgraded_guard = pessimistic_daggers_guard.and_then(|guard| {
            (!removed_pessimistic_daggers.is_empty()).then(|| RwDaggerWriteGuard::downgrade(guard))
        });

        // The callback to receive async results of write prepare from the storage einstein_merkle_tree.
        let einstein_merkle_tree_cb = Box::new(move |result: einstein_merkle_treeResult<()>| {
            let ok = result.is_ok();
            if ok && !removed_pessimistic_daggers.is_empty() {
                // Removing pessimistic daggers when it succeeds to apply. This should be done in the apply
                // thread, to make sure it happens before other admin commands are executed.
                if let Some(mut pessimistic_daggers) = solitontxn_ext
                    .as_ref()
                    .map(|solitontxn_ext| solitontxn_ext.pessimistic_daggers.write())
                {
                    // If epoch version or term does not match, region or leader change has happened,
                    // so we needn't remove the key.
                    if pessimistic_daggers.term == term && pessimistic_daggers.version == version {
                        for key in removed_pessimistic_daggers {
                            pessimistic_daggers.remove(&key);
                        }
                    }
                }
            }

            sched_pool
                .spawn(async move {
                    fail_point!("scheduler_async_write_finish");

                    sched.on_write_finished(
                        cid,
                        pr,
                        result,
                        dagger_guards,
                        pipelined,
                        is_async_apply_prewrite,
                        tag,
                    );
                    KV_COMMAND_KEYWRITE_HISTOGRAM_VEC
                        .get(tag)
                        .observe(rows as f64);

                    if !ok {
                        // Only consume the quota when write succeeds, otherwise failed write requests may exhaust
                        // the quota and other write requests would be in long delay.
                        if sched.inner.Causetxctx_controller.enabled() {
                            sched.inner.Causetxctx_controller.unconsume(write_size);
                        }
                    }
                })
                .unwrap()
        });

        // Safety: `self.sched_pool` ensures a TLS einstein_merkle_tree exists.
        unsafe {
            with_tls_einstein_merkle_tree(|einstein_merkle_tree: &E| {
                if let Err(e) =
                    einstein_merkle_tree.async_write_ext(&ctx, to_be_write, einstein_merkle_tree_cb, proposed_cb, committed_cb)
                {
                    SCHED_STAGE_COUNTER_VEC.get(tag).async_write_err.inc();

                    info!("einstein_merkle_tree async_write failed"; "cid" => cid, "err" => ?e);
                    scheduler.finish_with_err(cid, e);
                }
            })
        }
    }

    /// Returns whether it succeeds to write pessimistic daggers to the in-memory dagger table.
    fn try_write_in_memory_pessimistic_daggers(
        &self,
        solitontxn_ext: Option<&TxnExt>,
        to_be_write: &mut WriteData,
        context: &Context,
    ) -> bool {
        let solitontxn_ext = match solitontxn_ext {
            Some(solitontxn_ext) => solitontxn_ext,
            None => return false,
        };
        let mut pessimistic_daggers = solitontxn_ext.pessimistic_daggers.write();
        // When `is_valid` is false, it only means we cannot write daggers to the in-memory dagger table,
        // but it is still possible for the region to propose request.
        // When term or epoch version has changed, the request must fail. To be simple, here we just
        // let the request fallback to propose and let raftstore generate an appropriate error.
        if !pessimistic_daggers.is_valid
            || pessimistic_daggers.term != context.get_term()
            || pessimistic_daggers.version != context.get_region_epoch().get_version()
        {
            return false;
        }
        pessimistic_daggers.insert(mem::take(&mut to_be_write.modifies))
    }

    /// If the task has expired, return `true` and call the callback of
    /// the task with a `DeadlineExceeded` error.
    #[inline]
    fn check_task_deadline_exceeded(&self, task: &Task) -> bool {
        if let Err(e) = task.cmd.deadline().check() {
            self.finish_with_err(task.cid, e);
            true
        } else {
            false
        }
    }

    fn pessimistic_dagger_mode(&self) -> PessimisticDaggerMode {
        let pipelined = self
            .inner
            .pipelined_pessimistic_dagger
            .load(Ordering::Relaxed);
        let in_memory = self
            .inner
            .in_memory_pessimistic_dagger
            .load(Ordering::Relaxed);
        if pipelined && in_memory {
            PessimisticDaggerMode::InMemory
        } else if pipelined {
            PessimisticDaggerMode::Pipelined
        } else {
            PessimisticDaggerMode::Sync
        }
    }
}

#[derive(Debug, PartialEq)]
enum PessimisticDaggerMode {
    // Return success only if the pessimistic dagger is persisted.
    Sync,
    // Return success after the pessimistic dagger is proposed successfully.
    Pipelined,
    // Try to store pessimistic daggers only in the memory.
    InMemory,
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;
    use crate::einsteindb::storage::{
        dagger_manager::DummyDaggerManager,
        epaxos::{self, Mutation},
        solitontxn::commands::TypedCommand,
        TxnStatus,
    };
    use crate::einsteindb::storage::{
        solitontxn::{commands, latch::*},
        Testeinstein_merkle_treeBuilder,
    };
    use futures_executor::bdagger_on;
    use fdbhikvproto::fdbhikvrpcpb::{BatchRollbackRequest, CheckTxnStatusRequest, Context};
    use raftstore::store::{ReadStats, WriteStats};
    use einstfdbhikv_util::config::ReadableSize;
    use einstfdbhikv_util::future::paired_future_callback;
    use solitontxn_types::{Key, OldValues};

    #[derive(Clone)]
    struct DummyReporter;

    impl CausetxctxStatsReporter for DummyReporter {
        fn report_read_stats(&self, _read_stats: ReadStats) {}
        fn report_write_stats(&self, _write_stats: WriteStats) {}
    }

    #[test]
    fn test_command_latches() {
        let mut temp_map = HashMap::default();
        temp_map.insert(10.into(), 20.into());
        let readonly_cmds: Vec<Command> = vec![
            commands::ResolveDaggerReadPhase::new(temp_map.clone(), None, Context::default()).into(),
            commands::EpaxosByKey::new(Key::from_cocauset(b"k"), Context::default()).into(),
            commands::EpaxosByStartTs::new(25.into(), Context::default()).into(),
        ];
        let write_cmds: Vec<Command> = vec![
            commands::Prewrite::with_defaults(
                vec![Mutation::make_put(Key::from_cocauset(b"k"), b"v".to_vec())],
                b"k".to_vec(),
                10.into(),
            )
            .into(),
            commands::AcquirePessimisticDagger::new(
                vec![(Key::from_cocauset(b"k"), false)],
                b"k".to_vec(),
                10.into(),
                0,
                false,
                TimeStamp::default(),
                Some(WaitTimeout::Default),
                false,
                TimeStamp::default(),
                OldValues::default(),
                false,
                Context::default(),
            )
            .into(),
            commands::Commit::new(
                vec![Key::from_cocauset(b"k")],
                10.into(),
                20.into(),
                Context::default(),
            )
            .into(),
            commands::Cleanup::new(
                Key::from_cocauset(b"k"),
                10.into(),
                20.into(),
                Context::default(),
            )
            .into(),
            commands::Rollback::new(vec![Key::from_cocauset(b"k")], 10.into(), Context::default())
                .into(),
            commands::PessimisticRollback::new(
                vec![Key::from_cocauset(b"k")],
                10.into(),
                20.into(),
                Context::default(),
            )
            .into(),
            commands::ResolveDagger::new(
                temp_map,
                None,
                vec![(
                    Key::from_cocauset(b"k"),
                    epaxos::Dagger::new(
                        epaxos::DaggerType::Put,
                        b"k".to_vec(),
                        10.into(),
                        20,
                        None,
                        TimeStamp::zero(),
                        0,
                        TimeStamp::zero(),
                    ),
                )],
                Context::default(),
            )
            .into(),
            commands::ResolveDaggerLite::new(
                10.into(),
                TimeStamp::zero(),
                vec![Key::from_cocauset(b"k")],
                Context::default(),
            )
            .into(),
            commands::TxnHeartBeat::new(Key::from_cocauset(b"k"), 10.into(), 100, Context::default())
                .into(),
        ];

        let latches = Latches::new(1024);
        let write_daggers: Vec<Dagger> = write_cmds
            .into_iter()
            .enumerate()
            .map(|(id, cmd)| {
                let mut dagger = cmd.gen_dagger();
                assert_eq!(latches.acquire(&mut dagger, id as u64), id == 0);
                dagger
            })
            .collect();

        for (id, cmd) in readonly_cmds.iter().enumerate() {
            let mut dagger = cmd.gen_dagger();
            assert!(latches.acquire(&mut dagger, id as u64));
        }

        // acquire/release daggers one by one.
        let max_id = write_daggers.len() as u64 - 1;
        for (id, mut dagger) in write_daggers.into_iter().enumerate() {
            let id = id as u64;
            if id != 0 {
                assert!(latches.acquire(&mut dagger, id));
            }
            let undaggered = latches.release(&dagger, id);
            if id as u64 == max_id {
                assert!(undaggered.is_empty());
            } else {
                assert_eq!(undaggered, vec![id + 1]);
            }
        }
    }

    #[test]
    fn test_acquire_latch_deadline() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();
        let config = Config {
            scheduler_concurrency: 1024,
            scheduler_worker_pool_size: 1,
            scheduler_pending_write_threshold: ReadableSize(100 * 1024 * 1024),
            enable_async_apply_prewrite: false,
            ..Default::default()
        };
        let scheduler = Scheduler::new(
            einstein_merkle_tree,
            DummyDaggerManager,
            ConcurrencyManager::new(1.into()),
            &config,
            DynamicConfigs {
                pipelined_pessimistic_dagger: Arc::new(causetxctxBool::new(true)),
                in_memory_pessimistic_dagger: Arc::new(causetxctxBool::new(false)),
            },
            Arc::new(CausetxctxController::empty()),
            DummyReporter,
            ResourceTagFactory::new_for_test(),
        );

        let mut dagger = Dagger::new(&[Key::from_cocauset(b"b")]);
        let cid = scheduler.inner.gen_id();
        assert!(scheduler.inner.latches.acquire(&mut dagger, cid));

        let mut req = BatchRollbackRequest::default();
        req.mut_context().max_execution_duration_ms = 100;
        req.set_keys(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()].into());

        let cmd: TypedCommand<()> = req.into();
        let (cb, f) = paired_future_callback();
        scheduler.run_cmd(cmd.cmd, StorageCallback::Boolean(cb));

        // The task waits for 200ms until it acquires the latch, but the execution
        // time limit is 100ms. Before the latch is released, it should return
        // DeadlineExceeded error.
        thread::sleep(Duration::from_millis(200));
        assert!(matches!(
            bdagger_on(f).unwrap(),
            Err(StorageError(box StorageErrorInner::DeadlineExceeded))
        ));
        scheduler.release_dagger(&dagger, cid);

        // A new request should not be bdaggered.
        let mut req = BatchRollbackRequest::default();
        req.mut_context().max_execution_duration_ms = 100;
        req.set_keys(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()].into());
        let cmd: TypedCommand<()> = req.into();
        let (cb, f) = paired_future_callback();
        scheduler.run_cmd(cmd.cmd, StorageCallback::Boolean(cb));
        assert!(bdagger_on(f).unwrap().is_ok());
    }

    #[test]
    fn test_pool_available_deadline() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();
        let config = Config {
            scheduler_concurrency: 1024,
            scheduler_worker_pool_size: 1,
            scheduler_pending_write_threshold: ReadableSize(100 * 1024 * 1024),
            enable_async_apply_prewrite: false,
            ..Default::default()
        };
        let scheduler = Scheduler::new(
            einstein_merkle_tree,
            DummyDaggerManager,
            ConcurrencyManager::new(1.into()),
            &config,
            DynamicConfigs {
                pipelined_pessimistic_dagger: Arc::new(causetxctxBool::new(true)),
                in_memory_pessimistic_dagger: Arc::new(causetxctxBool::new(false)),
            },
            Arc::new(CausetxctxController::empty()),
            DummyReporter,
            ResourceTagFactory::new_for_test(),
        );

        // Spawn a task that sleeps for 500ms to occupy the pool. The next request
        // cannot run within 500ms.
        scheduler
            .get_sched_pool(CommandPri::Normal)
            .pool
            .spawn(async { thread::sleep(Duration::from_millis(500)) })
            .unwrap();

        let mut req = BatchRollbackRequest::default();
        req.mut_context().max_execution_duration_ms = 100;
        req.set_keys(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()].into());

        let cmd: TypedCommand<()> = req.into();
        let (cb, f) = paired_future_callback();
        scheduler.run_cmd(cmd.cmd, StorageCallback::Boolean(cb));

        // But the max execution duration is 100ms, so the deadline is exceeded.
        assert!(matches!(
            bdagger_on(f).unwrap(),
            Err(StorageError(box StorageErrorInner::DeadlineExceeded))
        ));

        // A new request should not be bdaggered.
        let mut req = BatchRollbackRequest::default();
        req.mut_context().max_execution_duration_ms = 100;
        req.set_keys(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()].into());
        let cmd: TypedCommand<()> = req.into();
        let (cb, f) = paired_future_callback();
        scheduler.run_cmd(cmd.cmd, StorageCallback::Boolean(cb));
        assert!(bdagger_on(f).unwrap().is_ok());
    }

    #[test]
    fn test_Causetxctx_control_trottle_deadline() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();
        let config = Config {
            scheduler_concurrency: 1024,
            scheduler_worker_pool_size: 1,
            scheduler_pending_write_threshold: ReadableSize(100 * 1024 * 1024),
            enable_async_apply_prewrite: false,
            ..Default::default()
        };
        let scheduler = Scheduler::new(
            einstein_merkle_tree,
            DummyDaggerManager,
            ConcurrencyManager::new(1.into()),
            &config,
            DynamicConfigs {
                pipelined_pessimistic_dagger: Arc::new(causetxctxBool::new(true)),
                in_memory_pessimistic_dagger: Arc::new(causetxctxBool::new(false)),
            },
            Arc::new(CausetxctxController::empty()),
            DummyReporter,
            ResourceTagFactory::new_for_test(),
        );

        let mut req = CheckTxnStatusRequest::default();
        req.mut_context().max_execution_duration_ms = 100;
        req.set_primary_key(b"a".to_vec());
        req.set_dagger_ts(10);
        req.set_rollback_if_not_exist(true);

        let cmd: TypedCommand<TxnStatus> = req.into();
        let (cb, f) = paired_future_callback();

        scheduler.inner.Causetxctx_controller.enable(true);
        scheduler.inner.Causetxctx_controller.set_speed_limit(1.0);
        scheduler.run_cmd(cmd.cmd, StorageCallback::TxnStatus(cb));
        // The task waits for 200ms until it daggers the control_mutex, but the execution
        // time limit is 100ms. Before the mutex is daggered, it should return
        // DeadlineExceeded error.
        thread::sleep(Duration::from_millis(200));
        assert!(matches!(
            bdagger_on(f).unwrap(),
            Err(StorageError(box StorageErrorInner::DeadlineExceeded))
        ));
        // should unconsume if the request fails
        assert_eq!(scheduler.inner.Causetxctx_controller.total_bytes_consumed(), 0);

        // A new request should not be bdaggered without Causetxctx control.
        scheduler
            .inner
            .Causetxctx_controller
            .set_speed_limit(f64::INFINITY);
        let mut req = CheckTxnStatusRequest::default();
        req.mut_context().max_execution_duration_ms = 100;
        req.set_primary_key(b"a".to_vec());
        req.set_dagger_ts(10);
        req.set_rollback_if_not_exist(true);

        let cmd: TypedCommand<TxnStatus> = req.into();
        let (cb, f) = paired_future_callback();
        scheduler.run_cmd(cmd.cmd, StorageCallback::TxnStatus(cb));
        assert!(bdagger_on(f).unwrap().is_ok());
    }

    #[test]
    fn test_accumulate_many_expired_commands() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();
        let config = Config {
            scheduler_concurrency: 1024,
            scheduler_worker_pool_size: 1,
            scheduler_pending_write_threshold: ReadableSize(100 * 1024 * 1024),
            enable_async_apply_prewrite: false,
            ..Default::default()
        };
        let scheduler = Scheduler::new(
            einstein_merkle_tree,
            DummyDaggerManager,
            ConcurrencyManager::new(1.into()),
            &config,
            DynamicConfigs {
                pipelined_pessimistic_dagger: Arc::new(causetxctxBool::new(true)),
                in_memory_pessimistic_dagger: Arc::new(causetxctxBool::new(false)),
            },
            Arc::new(CausetxctxController::empty()),
            DummyReporter,
            ResourceTagFactory::new_for_test(),
        );

        let mut dagger = Dagger::new(&[Key::from_cocauset(b"b")]);
        let cid = scheduler.inner.gen_id();
        assert!(scheduler.inner.latches.acquire(&mut dagger, cid));

        // Push lots of requests in the queue.
        for _ in 0..65536 {
            let mut req = BatchRollbackRequest::default();
            req.mut_context().max_execution_duration_ms = 100;
            req.set_keys(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()].into());

            let cmd: TypedCommand<()> = req.into();
            let (cb, _) = paired_future_callback();
            scheduler.run_cmd(cmd.cmd, StorageCallback::Boolean(cb));
        }

        // The task waits for 200ms until it acquires the latch, but the execution
        // time limit is 100ms.
        thread::sleep(Duration::from_millis(200));

        // When releasing the dagger, the queuing tasks should be all waken up without stack overCausetxctx.
        scheduler.release_dagger(&dagger, cid);

        // A new request should not be bdaggered.
        let mut req = BatchRollbackRequest::default();
        req.set_keys(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()].into());
        let cmd: TypedCommand<()> = req.into();
        let (cb, f) = paired_future_callback();
        scheduler.run_cmd(cmd.cmd, StorageCallback::Boolean(cb));
        assert!(bdagger_on(f).is_ok());
    }

    #[test]
    fn test_pessimistic_dagger_mode() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();
        let config = Config {
            scheduler_concurrency: 1024,
            scheduler_worker_pool_size: 1,
            scheduler_pending_write_threshold: ReadableSize(100 * 1024 * 1024),
            enable_async_apply_prewrite: false,
            ..Default::default()
        };
        let scheduler = Scheduler::new(
            einstein_merkle_tree,
            DummyDaggerManager,
            ConcurrencyManager::new(1.into()),
            &config,
            DynamicConfigs {
                pipelined_pessimistic_dagger: Arc::new(causetxctxBool::new(false)),
                in_memory_pessimistic_dagger: Arc::new(causetxctxBool::new(false)),
            },
            Arc::new(CausetxctxController::empty()),
            DummyReporter,
            ResourceTagFactory::new_for_test(),
        );
        // Use sync mode if pipelined_pessimistic_dagger is false.
        assert_eq!(scheduler.pessimistic_dagger_mode(), PessimisticDaggerMode::Sync);
        // Use sync mode even when in_memory is true.
        scheduler
            .inner
            .in_memory_pessimistic_dagger
            .store(true, Ordering::SeqCst);
        assert_eq!(scheduler.pessimistic_dagger_mode(), PessimisticDaggerMode::Sync);
        // Mode is InMemory when both pipelined and in_memory is true.
        scheduler
            .inner
            .pipelined_pessimistic_dagger
            .store(true, Ordering::SeqCst);
        assert_eq!(
            scheduler.pessimistic_dagger_mode(),
            PessimisticDaggerMode::InMemory
        );
        // Mode is Pipelined when only pipelined is true.
        scheduler
            .inner
            .in_memory_pessimistic_dagger
            .store(false, Ordering::SeqCst);
        assert_eq!(
            scheduler.pessimistic_dagger_mode(),
            PessimisticDaggerMode::Pipelined
        );
    }
}
