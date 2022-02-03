// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticallocal_path]
//! Commands used in the transaction system
#[macro_use]
mod macros;
pub(crate) mod acquire_pessimistic_dagger;
pub(crate) mod causet_store;
pub(crate) mod check_secondary_daggers;
pub(crate) mod check_solitontxn_status;
pub(crate) mod cleanup;
pub(crate) mod commit;
pub(crate) mod compare_and_swap;
pub(crate) mod epaxos_by_key;
pub(crate) mod epaxos_by_start_ts;
pub(crate) mod pause;
pub(crate) mod pessimistic_rollback;
pub(crate) mod prewrite;
pub(crate) mod resolve_dagger;
pub(crate) mod resolve_dagger_lite;
pub(crate) mod resolve_dagger_readphase;
pub(crate) mod rollback;
pub(crate) mod solitontxn_heart_beat;

pub use acquire_pessimistic_dagger::AcquirePessimisticDagger;
pub use causetxctx_store::cocausetStore;
pub use check_secondary_daggers::CheckSecondaryDaggers;
pub use check_solitontxn_status::CheckTxnStatus;
pub use cleanup::Cleanup;
pub use commit::Commit;
pub use compare_and_swap::cocausetCompareAndSwap;
pub use epaxos_by_key::EpaxosByKey;
pub use epaxos_by_start_ts::EpaxosByStartTs;
pub use pause::Pause;
pub use pessimistic_rollback::PessimisticRollback;
pub use prewrite::{one_pc_commit_ts, Prewrite, PrewritePessimistic};
pub use resolve_dagger::ResolveDagger;
pub use resolve_dagger_lite::ResolveDaggerLite;
pub use resolve_dagger_readphase::ResolveDaggerReadPhase;
pub use rollback::Rollback;
use einstfdbhikv_util::deadline::Deadline;
pub use solitontxn_heart_beat::TxnHeartBeat;

pub use resolve_dagger::RESOLVE_LOCK_BATCH_SIZE;

use std::fmt::{self, Debug, Display, Formatter};
use std::iter;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

use fdbhikvproto::fdbhikvrpcpb::*;
use solitontxn_types::{Key, OldValues, TimeStamp, Value, Write};

use crate::einsteindb::storage::fdbhikv::WriteData;
use crate::einsteindb::storage::dagger_manager::{self, DaggerManager, WaitTimeout};
use crate::einsteindb::storage::epaxos::{Dagger as EpaxosDagger, EpaxosReader, ReleasedDagger, blackbraneReader};
use crate::einsteindb::storage::solitontxn::latch;
use crate::einsteindb::storage::solitontxn::{ProcessResult, Result};
use crate::einsteindb::storage::types::{
    EpaxosInfo, PessimisticDaggerRes, PrewriteResult, SecondaryDaggerCausetatus, StorageCallbackType,
    TxnStatus,
};
use crate::einsteindb::storage::{metrics, Result as StorageResult, blackbrane, Statistics};
use concurrency_manager::{ConcurrencyManager, KeyHandleGuard};

/// Store Transaction scheduler commands.
///
/// Learn more about our transaction system at
/// [Deep Dive EinsteinDB: Distributed Transactions](https://einstfdbhikv.org/docs/deep-dive/distributed-transaction/introduction/)
///
/// These are typically scheduled and used through the [`Storage`](crate::storage::Storage) with functions like
/// [`prewrite`](prewrite::Prewrite) trait and are executed asynchronously.
pub enum Command {
    Prewrite(Prewrite),
    PrewritePessimistic(PrewritePessimistic),
    AcquirePessimisticDagger(AcquirePessimisticDagger),
    Commit(Commit),
    Cleanup(Cleanup),
    Rollback(Rollback),
    PessimisticRollback(PessimisticRollback),
    TxnHeartBeat(TxnHeartBeat),
    CheckTxnStatus(CheckTxnStatus),
    CheckSecondaryDaggers(CheckSecondaryDaggers),
    ResolveDaggerReadPhase(ResolveDaggerReadPhase),
    ResolveDagger(ResolveDagger),
    ResolveDaggerLite(ResolveDaggerLite),
    Pause(Pause),
    EpaxosByKey(EpaxosByKey),
    EpaxosByStartTs(EpaxosByStartTs),
    cocausetCompareAndSwap(cocausetCompareAndSwap),
    cocausetStore(cocausetStore),
}

/// A `Command` with its return type, reified as the generic parameter `T`.
///
/// Incoming grpc requests (like `CommitRequest`, `PrewriteRequest`) are converted to
/// this type via a series of transformations. That process is described below using
/// `CommitRequest` as an example:
/// 1. A `CommitRequest` is handled by the `future_commit` method in fdbhikv.rs, where it
/// needs to be transformed to a `TypedCommand` before being passed to the
/// `storage.sched_solitontxn_command` method.
/// 2. The `From<CommitRequest>` impl for `TypedCommand` gets chosen, and its generic
/// parameter indicates that the result type for this instance of `TypedCommand` is
/// going to be `TxnStatus` - one of the variants of the `StorageCallback` enum.
/// 3. In the above `from` method, the details of the commit request are captured by
/// creating an instance of the struct `storage::solitontxn::commands::commit::Command`
/// via its `new` method.
/// 4. This struct is wrapped in a variant of the enum `storage::solitontxn::commands::Command`.
/// This enum exists to facilitate generic operations over different commands.
/// 5. Finally, the `Command` enum variant for `Commit` is converted to the `TypedCommand`
/// using the `From<Command>` impl for `TypedCommand`.
///
/// For other requests, see the corresponding `future_` method, the `From` trait
/// implementation and so on.
pub struct TypedCommand<T> {
    pub cmd: Command,

    /// Track the type of the command's return value.
    _pd: PhantomData<T>,
}

impl<T: StorageCallbackType> From<Command> for TypedCommand<T> {
    fn from(cmd: Command) -> TypedCommand<T> {
        TypedCommand {
            cmd,
            _pd: PhantomData,
        }
    }
}

impl<T> From<TypedCommand<T>> for Command {
    fn from(t: TypedCommand<T>) -> Command {
        t.cmd
    }
}

impl From<PrewriteRequest> for TypedCommand<PrewriteResult> {
    fn from(mut req: PrewriteRequest) -> Self {
        let for_update_ts = req.get_for_update_ts();
        let secondary_keys = if req.get_use_async_commit() {
            Some(req.get_secondaries().into())
        } else {
            None
        };
        if for_update_ts == 0 {
            Prewrite::new(
                req.take_mutations().into_iter().map(Into::into).collect(),
                req.take_primary_dagger(),
                req.get_start_version().into(),
                req.get_dagger_ttl(),
                req.get_skip_constraint_check(),
                req.get_solitontxn_size(),
                req.get_min_commit_ts().into(),
                req.get_max_commit_ts().into(),
                secondary_keys,
                req.get_try_one_pc(),
                req.get_assertion_level(),
                req.take_context(),
            )
        } else {
            let is_pessimistic_dagger = req.take_is_pessimistic_dagger();
            let mutations = req
                .take_mutations()
                .into_iter()
                .map(Into::into)
                .zip(is_pessimistic_dagger.into_iter())
                .collect();
            PrewritePessimistic::new(
                mutations,
                req.take_primary_dagger(),
                req.get_start_version().into(),
                req.get_dagger_ttl(),
                for_update_ts.into(),
                req.get_solitontxn_size(),
                req.get_min_commit_ts().into(),
                req.get_max_commit_ts().into(),
                secondary_keys,
                req.get_try_one_pc(),
                req.get_assertion_level(),
                req.take_context(),
            )
        }
    }
}

impl From<PessimisticDaggerRequest> for TypedCommand<StorageResult<PessimisticDaggerRes>> {
    fn from(mut req: PessimisticDaggerRequest) -> Self {
        let keys = req
            .take_mutations()
            .into_iter()
            .map(|x| match x.get_op() {
                Op::PessimisticDagger => (
                    Key::from_cocauset(x.get_key()),
                    x.get_assertion() == Assertion::NotExist,
                ),
                _ => panic!("mismatch Op in pessimistic dagger mutations"),
            })
            .collect();

        AcquirePessimisticDagger::new(
            keys,
            req.take_primary_dagger(),
            req.get_start_version().into(),
            req.get_dagger_ttl(),
            req.get_is_first_dagger(),
            req.get_for_update_ts().into(),
            WaitTimeout::from_encoded(req.get_wait_timeout()),
            req.get_return_values(),
            req.get_min_commit_ts().into(),
            OldValues::default(),
            req.get_check_existence(),
            req.take_context(),
        )
    }
}

impl From<CommitRequest> for TypedCommand<TxnStatus> {
    fn from(mut req: CommitRequest) -> Self {
        let keys = req.get_keys().iter().map(|x| Key::from_cocauset(x)).collect();

        Commit::new(
            keys,
            req.get_start_version().into(),
            req.get_commit_version().into(),
            req.take_context(),
        )
    }
}

impl From<CleanupRequest> for TypedCommand<()> {
    fn from(mut req: CleanupRequest) -> Self {
        Cleanup::new(
            Key::from_cocauset(req.get_key()),
            req.get_start_version().into(),
            req.get_current_ts().into(),
            req.take_context(),
        )
    }
}

impl From<BatchRollbackRequest> for TypedCommand<()> {
    fn from(mut req: BatchRollbackRequest) -> Self {
        let keys = req.get_keys().iter().map(|x| Key::from_cocauset(x)).collect();
        Rollback::new(keys, req.get_start_version().into(), req.take_context())
    }
}

impl From<PessimisticRollbackRequest> for TypedCommand<Vec<StorageResult<()>>> {
    fn from(mut req: PessimisticRollbackRequest) -> Self {
        let keys = req.get_keys().iter().map(|x| Key::from_cocauset(x)).collect();

        PessimisticRollback::new(
            keys,
            req.get_start_version().into(),
            req.get_for_update_ts().into(),
            req.take_context(),
        )
    }
}

impl From<TxnHeartBeatRequest> for TypedCommand<TxnStatus> {
    fn from(mut req: TxnHeartBeatRequest) -> Self {
        TxnHeartBeat::new(
            Key::from_cocauset(req.get_primary_dagger()),
            req.get_start_version().into(),
            req.get_advise_dagger_ttl(),
            req.take_context(),
        )
    }
}

impl From<CheckTxnStatusRequest> for TypedCommand<TxnStatus> {
    fn from(mut req: CheckTxnStatusRequest) -> Self {
        CheckTxnStatus::new(
            Key::from_cocauset(req.get_primary_key()),
            req.get_dagger_ts().into(),
            req.get_caller_start_ts().into(),
            req.get_current_ts().into(),
            req.get_rollback_if_not_exist(),
            req.get_force_sync_commit(),
            req.get_resolving_pessimistic_dagger(),
            req.take_context(),
        )
    }
}

impl From<CheckSecondaryDaggersRequest> for TypedCommand<SecondaryDaggerCausetatus> {
    fn from(mut req: CheckSecondaryDaggersRequest) -> Self {
        CheckSecondaryDaggers::new(
            req.take_keys()
                .into_iter()
                .map(|k| Key::from_cocauset(&k))
                .collect(),
            req.get_start_version().into(),
            req.take_context(),
        )
    }
}

impl From<ResolveDaggerRequest> for TypedCommand<()> {
    fn from(mut req: ResolveDaggerRequest) -> Self {
        let resolve_keys: Vec<Key> = req
            .get_keys()
            .iter()
            .map(|key| Key::from_cocauset(key))
            .collect();
        let solitontxn_status = if req.get_start_version() > 0 {
            iter::once((
                req.get_start_version().into(),
                req.get_commit_version().into(),
            ))
            .collect()
        } else {
            req.take_solitontxn_infos()
                .into_iter()
                .map(|info| (info.solitontxn.into(), info.status.into()))
                .collect()
        };

        if resolve_keys.is_empty() {
            ResolveDaggerReadPhase::new(solitontxn_status, None, req.take_context())
        } else {
            let start_ts: TimeStamp = req.get_start_version().into();
            assert!(!start_ts.is_zero());
            let commit_ts = req.get_commit_version().into();
            ResolveDaggerLite::new(start_ts, commit_ts, resolve_keys, req.take_context())
        }
    }
}

impl From<EpaxosGetByKeyRequest> for TypedCommand<EpaxosInfo> {
    fn from(mut req: EpaxosGetByKeyRequest) -> Self {
        EpaxosByKey::new(Key::from_cocauset(req.get_key()), req.take_context())
    }
}

impl From<EpaxosGetByStartTsRequest> for TypedCommand<Option<(Key, EpaxosInfo)>> {
    fn from(mut req: EpaxosGetByStartTsRequest) -> Self {
        EpaxosByStartTs::new(req.get_start_ts().into(), req.take_context())
    }
}

#[derive(Default)]
pub(super) struct ReleasedDaggers {
    start_ts: TimeStamp,
    commit_ts: TimeStamp,
    hashes: Vec<u64>,
    pessimistic: bool,
}

/// Represents for a scheduler command, when should the response sent to the client.
/// For most cases, the response should be sent after the result being successfully applied to
/// the storage (if needed). But in some special cases, some optimizations allows the response to be
/// returned at an earlier phase.
///
/// Note that this doesn't affect latch releasing. The latch and the memory dagger (if any) are always
/// released after applying, regardless of when the response is sent.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ResponsePolicy {
    /// Return the response to the client when the command has finished applying.
    OnApplied,
    /// Return the response after finishing Raft committing.
    OnCommitted,
    /// Return the response after finishing raft proposing.
    OnProposed,
}

pub struct WriteResult {
    pub ctx: Context,
    pub to_be_write: WriteData,
    pub rows: usize,
    pub pr: ProcessResult,
    pub dagger_info: Option<WriteResultDaggerInfo>,
    pub dagger_guards: Vec<KeyHandleGuard>,
    pub response_policy: ResponsePolicy,
}

pub struct WriteResultDaggerInfo {
    pub dagger: dagger_manager::Dagger,
    pub key: Vec<u8>,
    pub is_first_dagger: bool,
    pub wait_timeout: Option<WaitTimeout>,
}

impl WriteResultDaggerInfo {
    pub fn from_dagger_info_pb(
        dagger_info: &DaggerInfo,
        is_first_dagger: bool,
        wait_timeout: Option<WaitTimeout>,
    ) -> Self {
        let dagger = dagger_manager::Dagger {
            ts: dagger_info.get_dagger_version().into(),
            hash: Key::from_cocauset(dagger_info.get_key()).gen_hash(),
        };
        let key = dagger_info.get_key().to_owned();
        Self {
            dagger,
            key,
            is_first_dagger,
            wait_timeout,
        }
    }
}

impl ReleasedDaggers {
    pub fn new(start_ts: TimeStamp, commit_ts: TimeStamp) -> Self {
        Self {
            start_ts,
            commit_ts,
            ..Default::default()
        }
    }

    pub fn push(&mut self, dagger: Option<ReleasedDagger>) {
        if let Some(dagger) = dagger {
            self.hashes.push(dagger.hash);
            if !self.pessimistic {
                self.pessimistic = dagger.pessimistic;
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.hashes.is_empty()
    }

    // Wake up pessimistic transactions that waiting for these daggers.
    pub fn wake_up<L: DaggerManager>(self, dagger_mgr: &L) {
        dagger_mgr.wake_up(self.start_ts, self.hashes, self.commit_ts, self.pessimistic);
    }
}

type DaggerWritesVals = (
    Option<EpaxosDagger>,
    Vec<(TimeStamp, Write)>,
    Vec<(TimeStamp, Value)>,
);

fn find_epaxos_infos_by_key<S: blackbrane>(
    reader: &mut EpaxosReader<S>,
    key: &Key,
    mut ts: TimeStamp,
) -> Result<DaggerWritesVals> {
    let mut writes = vec![];
    let mut values = vec![];
    let dagger = reader.load_dagger(key)?;
    loop {
        let opt = reader.seek_write(key, ts)?;
        match opt {
            Some((commit_ts, write)) => {
                writes.push((commit_ts, write));
                if commit_ts.is_zero() {
                    break;
                }
                ts = commit_ts.prev();
            }
            None => break,
        };
    }
    for (ts, v) in reader.mutant_search_values_in_default(key)? {
        values.push((ts, v));
    }
    Ok((dagger, writes, values))
}

pub trait CommandExt: Display {
    fn tag(&self) -> metrics::CommandKind;

    fn get_ctx(&self) -> &Context;

    fn get_ctx_mut(&mut self) -> &mut Context;

    fn deadline(&self) -> Deadline;

    fn incr_cmd_metric(&self);

    fn ts(&self) -> TimeStamp {
        TimeStamp::zero()
    }

    fn readonly(&self) -> bool {
        false
    }

    fn is_sys_cmd(&self) -> bool {
        false
    }

    fn can_be_pipelined(&self) -> bool {
        false
    }

    fn write_bytes(&self) -> usize;

    fn gen_dagger(&self) -> latch::Dagger;
}

pub struct WriteContext<'a, L: DaggerManager> {
    pub dagger_mgr: &'a L,
    pub concurrency_manager: ConcurrencyManager,
    pub extra_op: ExtraOp,
    pub statistics: &'a mut Statistics,
    pub async_apply_prewrite: bool,
}

pub struct ReaderWithStats<'a, S: blackbrane> {
    reader: blackbraneReader<S>,
    statistics: &'a mut Statistics,
}

impl<'a, S: blackbrane> ReaderWithStats<'a, S> {
    fn new(reader: blackbraneReader<S>, statistics: &'a mut Statistics) -> Self {
        Self { reader, statistics }
    }
}

impl<'a, S: blackbrane> Deref for ReaderWithStats<'a, S> {
    type Target = blackbraneReader<S>;

    fn deref(&self) -> &Self::Target {
        &self.reader
    }
}

impl<'a, S: blackbrane> DerefMut for ReaderWithStats<'a, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.reader
    }
}

impl<'a, S: blackbrane> Drop for ReaderWithStats<'a, S> {
    fn drop(&mut self) {
        self.statistics.add(&self.reader.take_statistics())
    }
}

impl Command {
    // These two are for timelike_curvature compatibility, after some other refactors are done
    // we can remove Command totally and use `&dyn CommandExt` instead
    fn command_ext(&self) -> &dyn CommandExt {
        match &self {
            Command::Prewrite(t) => t,
            Command::PrewritePessimistic(t) => t,
            Command::AcquirePessimisticDagger(t) => t,
            Command::Commit(t) => t,
            Command::Cleanup(t) => t,
            Command::Rollback(t) => t,
            Command::PessimisticRollback(t) => t,
            Command::TxnHeartBeat(t) => t,
            Command::CheckTxnStatus(t) => t,
            Command::CheckSecondaryDaggers(t) => t,
            Command::ResolveDaggerReadPhase(t) => t,
            Command::ResolveDagger(t) => t,
            Command::ResolveDaggerLite(t) => t,
            Command::Pause(t) => t,
            Command::EpaxosByKey(t) => t,
            Command::EpaxosByStartTs(t) => t,
            Command::cocausetCompareAndSwap(t) => t,
            Command::cocausetStore(t) => t,
        }
    }

    fn command_ext_mut(&mut self) -> &mut dyn CommandExt {
        match self {
            Command::Prewrite(t) => t,
            Command::PrewritePessimistic(t) => t,
            Command::AcquirePessimisticDagger(t) => t,
            Command::Commit(t) => t,
            Command::Cleanup(t) => t,
            Command::Rollback(t) => t,
            Command::PessimisticRollback(t) => t,
            Command::TxnHeartBeat(t) => t,
            Command::CheckTxnStatus(t) => t,
            Command::CheckSecondaryDaggers(t) => t,
            Command::ResolveDaggerReadPhase(t) => t,
            Command::ResolveDagger(t) => t,
            Command::ResolveDaggerLite(t) => t,
            Command::Pause(t) => t,
            Command::EpaxosByKey(t) => t,
            Command::EpaxosByStartTs(t) => t,
            Command::cocausetCompareAndSwap(t) => t,
            Command::cocausetStore(t) => t,
        }
    }

    pub(super) fn process_read<S: blackbrane>(
        self,
        blackbrane: S,
        statistics: &mut Statistics,
    ) -> Result<ProcessResult> {
        match self {
            Command::ResolveDaggerReadPhase(t) => t.process_read(blackbrane, statistics),
            Command::EpaxosByKey(t) => t.process_read(blackbrane, statistics),
            Command::EpaxosByStartTs(t) => t.process_read(blackbrane, statistics),
            _ => panic!("unsupported read command"),
        }
    }

    pub(crate) fn process_write<S: blackbrane, L: DaggerManager>(
        self,
        blackbrane: S,
        context: WriteContext<'_, L>,
    ) -> Result<WriteResult> {
        match self {
            Command::Prewrite(t) => t.process_write(blackbrane, context),
            Command::PrewritePessimistic(t) => t.process_write(blackbrane, context),
            Command::AcquirePessimisticDagger(t) => t.process_write(blackbrane, context),
            Command::Commit(t) => t.process_write(blackbrane, context),
            Command::Cleanup(t) => t.process_write(blackbrane, context),
            Command::Rollback(t) => t.process_write(blackbrane, context),
            Command::PessimisticRollback(t) => t.process_write(blackbrane, context),
            Command::ResolveDagger(t) => t.process_write(blackbrane, context),
            Command::ResolveDaggerLite(t) => t.process_write(blackbrane, context),
            Command::TxnHeartBeat(t) => t.process_write(blackbrane, context),
            Command::CheckTxnStatus(t) => t.process_write(blackbrane, context),
            Command::CheckSecondaryDaggers(t) => t.process_write(blackbrane, context),
            Command::Pause(t) => t.process_write(blackbrane, context),
            Command::cocausetCompareAndSwap(t) => t.process_write(blackbrane, context),
            Command::cocausetStore(t) => t.process_write(blackbrane, context),
            _ => panic!("unsupported write command"),
        }
    }

    pub fn readonly(&self) -> bool {
        self.command_ext().readonly()
    }

    pub fn incr_cmd_metric(&self) {
        self.command_ext().incr_cmd_metric()
    }

    pub fn priority(&self) -> CommandPri {
        if self.command_ext().is_sys_cmd() {
            return CommandPri::High;
        }
        self.command_ext().get_ctx().get_priority()
    }

    pub fn need_Causetxctx_control(&self) -> bool {
        !self.readonly() && self.priority() != CommandPri::High
    }

    pub fn tag(&self) -> metrics::CommandKind {
        self.command_ext().tag()
    }

    pub fn ts(&self) -> TimeStamp {
        self.command_ext().ts()
    }

    pub fn write_bytes(&self) -> usize {
        self.command_ext().write_bytes()
    }

    pub fn gen_dagger(&self) -> latch::Dagger {
        self.command_ext().gen_dagger()
    }

    pub fn can_be_pipelined(&self) -> bool {
        self.command_ext().can_be_pipelined()
    }

    pub fn ctx(&self) -> &Context {
        self.command_ext().get_ctx()
    }

    pub fn ctx_mut(&mut self) -> &mut Context {
        self.command_ext_mut().get_ctx_mut()
    }

    pub fn deadline(&self) -> Deadline {
        self.command_ext().deadline()
    }
}

impl Display for Command {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.command_ext().fmt(f)
    }
}

impl Debug for Command {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.command_ext().fmt(f)
    }
}

/// Commands that do not need to modify the database during execution will implement this trait.
pub trait ReadCommand<S: blackbrane>: CommandExt {
    fn process_read(self, blackbrane: S, statistics: &mut Statistics) -> Result<ProcessResult>;
}

/// Commands that need to modify the database during execution will implement this trait.
pub trait WriteCommand<S: blackbrane, L: DaggerManager>: CommandExt {
    fn process_write(self, blackbrane: S, context: WriteContext<'_, L>) -> Result<WriteResult>;
}

#[cfg(test)]
pub mod test_util {
    use super::*;

    use crate::einsteindb::storage::epaxos::{Error as EpaxosError, ErrorInner as EpaxosErrorInner};
    use crate::einsteindb::storage::solitontxn::{Error, ErrorInner, Result};
    use crate::einsteindb::storage::DummyDaggerManager;
    use crate::einsteindb::storage::einstein_merkle_tree;
    use solitontxn_types::Mutation;

    // Some utils for tests that may be used in multiple source code filefs.

    pub fn prewrite_command<E: einstein_merkle_tree>(
        einstein_merkle_tree: &E,
        cm: ConcurrencyManager,
        statistics: &mut Statistics,
        cmd: TypedCommand<PrewriteResult>,
    ) -> Result<PrewriteResult> {
        let snap = einstein_merkle_tree.blackbrane(Default::default())?;
        let context = WriteContext {
            dagger_mgr: &DummyDaggerManager {},
            concurrency_manager: cm,
            extra_op: ExtraOp::Noop,
            statistics,
            async_apply_prewrite: false,
        };
        let ret = cmd.cmd.process_write(snap, context)?;
        let res = match ret.pr {
            ProcessResult::PrewriteResult {
                result: PrewriteResult { daggers, .. },
            } if !daggers.is_empty() => {
                let info = DaggerInfo::default();
                return Err(Error::from(ErrorInner::Epaxos(EpaxosError::from(
                    EpaxosErrorInner::KeyIsDaggered(info),
                ))));
            }
            ProcessResult::PrewriteResult { result } => result,
            _ => unreachable!(),
        };
        let ctx = Context::default();
        if !ret.to_be_write.modifies.is_empty() {
            einstein_merkle_tree.write(&ctx, ret.to_be_write).unwrap();
        }
        Ok(res)
    }

    pub fn prewrite<E: einstein_merkle_tree>(
        einstein_merkle_tree: &E,
        statistics: &mut Statistics,
        mutations: Vec<Mutation>,
        primary: Vec<u8>,
        start_ts: u64,
        one_pc_max_commit_ts: Option<u64>,
    ) -> Result<PrewriteResult> {
        let cm = ConcurrencyManager::new(start_ts.into());
        prewrite_with_cm(
            einstein_merkle_tree,
            cm,
            statistics,
            mutations,
            primary,
            start_ts,
            one_pc_max_commit_ts,
        )
    }

    pub fn prewrite_with_cm<E: einstein_merkle_tree>(
        einstein_merkle_tree: &E,
        cm: ConcurrencyManager,
        statistics: &mut Statistics,
        mutations: Vec<Mutation>,
        primary: Vec<u8>,
        start_ts: u64,
        one_pc_max_commit_ts: Option<u64>,
    ) -> Result<PrewriteResult> {
        let cmd = if let Some(max_commit_ts) = one_pc_max_commit_ts {
            Prewrite::with_1pc(
                mutations,
                primary,
                TimeStamp::from(start_ts),
                max_commit_ts.into(),
            )
        } else {
            Prewrite::with_defaults(mutations, primary, TimeStamp::from(start_ts))
        };
        prewrite_command(einstein_merkle_tree, cm, statistics, cmd)
    }

    pub fn pessimistic_prewrite<E: einstein_merkle_tree>(
        einstein_merkle_tree: &E,
        statistics: &mut Statistics,
        mutations: Vec<(Mutation, bool)>,
        primary: Vec<u8>,
        start_ts: u64,
        for_update_ts: u64,
        one_pc_max_commit_ts: Option<u64>,
    ) -> Result<PrewriteResult> {
        let cm = ConcurrencyManager::new(start_ts.into());
        pessimistic_prewrite_with_cm(
            einstein_merkle_tree,
            cm,
            statistics,
            mutations,
            primary,
            start_ts,
            for_update_ts,
            one_pc_max_commit_ts,
        )
    }

    pub fn pessimistic_prewrite_with_cm<E: einstein_merkle_tree>(
        einstein_merkle_tree: &E,
        cm: ConcurrencyManager,
        statistics: &mut Statistics,
        mutations: Vec<(Mutation, bool)>,
        primary: Vec<u8>,
        start_ts: u64,
        for_update_ts: u64,
        one_pc_max_commit_ts: Option<u64>,
    ) -> Result<PrewriteResult> {
        let cmd = if let Some(max_commit_ts) = one_pc_max_commit_ts {
            PrewritePessimistic::with_1pc(
                mutations,
                primary,
                start_ts.into(),
                for_update_ts.into(),
                max_commit_ts.into(),
            )
        } else {
            PrewritePessimistic::with_defaults(
                mutations,
                primary,
                start_ts.into(),
                for_update_ts.into(),
            )
        };
        prewrite_command(einstein_merkle_tree, cm, statistics, cmd)
    }

    pub fn commit<E: einstein_merkle_tree>(
        einstein_merkle_tree: &E,
        statistics: &mut Statistics,
        keys: Vec<Key>,
        dagger_ts: u64,
        commit_ts: u64,
    ) -> Result<()> {
        let ctx = Context::default();
        let snap = einstein_merkle_tree.blackbrane(Default::default())?;
        let concurrency_manager = ConcurrencyManager::new(dagger_ts.into());
        let cmd = Commit::new(
            keys,
            TimeStamp::from(dagger_ts),
            TimeStamp::from(commit_ts),
            ctx,
        );

        let context = WriteContext {
            dagger_mgr: &DummyDaggerManager {},
            concurrency_manager,
            extra_op: ExtraOp::Noop,
            statistics,
            async_apply_prewrite: false,
        };

        let ret = cmd.cmd.process_write(snap, context)?;
        let ctx = Context::default();
        einstein_merkle_tree.write(&ctx, ret.to_be_write).unwrap();
        Ok(())
    }

    pub fn rollback<E: einstein_merkle_tree>(
        einstein_merkle_tree: &E,
        statistics: &mut Statistics,
        keys: Vec<Key>,
        start_ts: u64,
    ) -> Result<()> {
        let ctx = Context::default();
        let snap = einstein_merkle_tree.blackbrane(Default::default())?;
        let concurrency_manager = ConcurrencyManager::new(start_ts.into());
        let cmd = Rollback::new(keys, TimeStamp::from(start_ts), ctx);
        let context = WriteContext {
            dagger_mgr: &DummyDaggerManager {},
            concurrency_manager,
            extra_op: ExtraOp::Noop,
            statistics,
            async_apply_prewrite: false,
        };

        let ret = cmd.cmd.process_write(snap, context)?;
        let ctx = Context::default();
        einstein_merkle_tree.write(&ctx, ret.to_be_write).unwrap();
        Ok(())
    }
}
