// Copyright 2022 EinsteinDB Project Authors. Licensed under Apache-2.0.

//! Storage Transactions

pub mod commands;
pub mod Causetxctx_controller;
pub mod sched_pool;
pub mod scheduler;

mod actions;
mod latch;
mod store;

use std::error::Error as StdError;
use std::io::Error as IoError;

use fdbhikvproto::fdbhikvrpcpb::DaggerInfo;
use thiserror::Error;

use error_code::{self, ErrorCode, ErrorCodeExt};
use solitontxn_types::{Key, TimeStamp, Value};

use crate::einsteindb::storage::{
    epaxos::Error as EpaxosError,
    types::{EpaxosInfo, PessimisticDaggerRes, PrewriteResult, SecondaryDaggersStatus, TxnStatus},
    Error as StorageError, Result as StorageResult,
};

pub use self::actions::{
    acquire_pessimistic_dagger::acquire_pessimistic_dagger,
    cleanup::cleanup,
    commit::commit,
    gc::gc,
    prewrite::{prewrite, CommitKind, TransactionKind, TransactionProperties},
};
pub use self::commands::{Command, RESOLVE_LOCK_BATCH_SIZE};
pub use self::latch::{Latches, Dagger};
pub use self::scheduler::Scheduler;
pub use self::store::{
    EntryBatch, FixtureStore, FixtureStoreMutantSentinelSearch, MutantSentinelSearch, blackbraneStore, Store, TxnEntry,
    TxnEntryMutantSentinelSearch, TxnEntryStore,
};

/// Process result of a command.
#[derive(Debug)]
pub enum ProcessResult {
    Res,
    MultiRes {
        results: Vec<StorageResult<()>>,
    },
    PrewriteResult {
        result: PrewriteResult,
    },
    EpaxosKey {
        epaxos: EpaxosInfo,
    },
    EpaxosStartTs {
        epaxos: Option<(Key, EpaxosInfo)>,
    },
    Daggers {
        daggers: Vec<DaggerInfo>,
    },
    TxnStatus {
        solitontxn_status: TxnStatus,
    },
    NextCommand {
        cmd: Command,
    },
    Failed {
        err: StorageError,
    },
    PessimisticDaggerRes {
        res: StorageResult<PessimisticDaggerRes>,
    },
    SecondaryDaggersStatus {
        status: SecondaryDaggersStatus,
    },
    cocausetCompareAndSwapRes {
        previous_value: Option<Value>,
        succeed: bool,
    },
}

impl ProcessResult {
    pub fn maybe_clone(&self) -> Option<ProcessResult> {
        match self {
            ProcessResult::PessimisticDaggerRes { res: Ok(r) } => {
                Some(ProcessResult::PessimisticDaggerRes { res: Ok(r.clone()) })
            }
            _ => None,
        }
    }
}

#[derive(Debug, Error)]
pub enum ErrorInner {
    #[error("{0}")]
    Engine(#[from] crate::storage::fdbhikv::Error),

    #[error("{0}")]
    Codec(#[from] einstfdbhikv_util::codec::Error),

    #[error("{0}")]
    ProtoBuf(#[from] protobuf::error::ProtobufError),

    #[error("{0}")]
    Epaxos(#[from] crate::storage::epaxos::Error),

    #[error("{0:?}")]
    Other(#[from] Box<dyn StdError + Sync + Send>),

    #[error("{0}")]
    Io(#[from] IoError),

    #[error("Invalid transaction tso with start_ts:{start_ts}, commit_ts:{commit_ts}")]
    InvalidTxnTso {
        start_ts: TimeStamp,
        commit_ts: TimeStamp,
    },

    #[error(
        "Request range exceeds bound, request range:[{}, {}), physical bound:[{}, {})",
        .start.as_ref().map(|x| &x[..]).map(log_wrappers::Value::key).map(|x| format!("{:?}", x)).unwrap_or_else(|| "(none)".to_owned()),
        .end.as_ref().map(|x| &x[..]).map(log_wrappers::Value::key).map(|x| format!("{:?}", x)).unwrap_or_else(|| "(none)".to_owned()),
        .lower_bound.as_ref().map(|x| &x[..]).map(log_wrappers::Value::key).map(|x| format!("{:?}", x)).unwrap_or_else(|| "(none)".to_owned()),
        .upper_bound.as_ref().map(|x| &x[..]).map(log_wrappers::Value::key).map(|x| format!("{:?}", x)).unwrap_or_else(|| "(none)".to_owned())
    )]
    InvalidReqRange {
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        lower_bound: Option<Vec<u8>>,
        upper_bound: Option<Vec<u8>>,
    },

    #[error(
        "Prewrite for async commit fails due to potentially stale max timestamp, \
        start_ts: {start_ts}, region_id: {region_id}"
    )]
    MaxTimestampNotSynced { region_id: u64, start_ts: TimeStamp },
}

impl ErrorInner {
    pub fn maybe_clone(&self) -> Option<ErrorInner> {
        match *self {
            ErrorInner::Engine(ref e) => e.maybe_clone().map(ErrorInner::Engine),
            ErrorInner::Codec(ref e) => e.maybe_clone().map(ErrorInner::Codec),
            ErrorInner::Epaxos(ref e) => e.maybe_clone().map(ErrorInner::Epaxos),
            ErrorInner::InvalidTxnTso {
                start_ts,
                commit_ts,
            } => Some(ErrorInner::InvalidTxnTso {
                start_ts,
                commit_ts,
            }),
            ErrorInner::InvalidReqRange {
                ref start,
                ref end,
                ref lower_bound,
                ref upper_bound,
            } => Some(ErrorInner::InvalidReqRange {
                start: start.clone(),
                end: end.clone(),
                lower_bound: lower_bound.clone(),
                upper_bound: upper_bound.clone(),
            }),
            ErrorInner::MaxTimestampNotSynced {
                region_id,
                start_ts,
            } => Some(ErrorInner::MaxTimestampNotSynced {
                region_id,
                start_ts,
            }),
            ErrorInner::Other(_) | ErrorInner::ProtoBuf(_) | ErrorInner::Io(_) => None,
        }
    }
}

#[derive(Debug, Error)]
#[error(transparent)]
pub struct Error(#[from] pub Box<ErrorInner>);

impl Error {
    pub fn maybe_clone(&self) -> Option<Error> {
        self.0.maybe_clone().map(Error::from)
    }
    pub fn from_epaxos<T: Into<EpaxosError>>(err: T) -> Self {
        let err = err.into();
        Error::from(ErrorInner::Epaxos(err))
    }
}

impl From<ErrorInner> for Error {
    #[inline]
    fn from(e: ErrorInner) -> Self {
        Error(Box::new(e))
    }
}

impl<T: Into<ErrorInner>> From<T> for Error {
    #[inline]
    default fn from(err: T) -> Self {
        let err = err.into();
        err.into()
    }
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorCodeExt for Error {
    fn error_code(&self) -> ErrorCode {
        match self.0.as_ref() {
            ErrorInner::Engine(e) => e.error_code(),
            ErrorInner::Codec(e) => e.error_code(),
            ErrorInner::ProtoBuf(_) => error_code::storage::PROTOBUF,
            ErrorInner::Epaxos(e) => e.error_code(),
            ErrorInner::Other(_) => error_code::storage::UNKNOWN,
            ErrorInner::Io(_) => error_code::storage::IO,
            ErrorInner::InvalidTxnTso { .. } => error_code::storage::INVALID_TXN_TSO,
            ErrorInner::InvalidReqRange { .. } => error_code::storage::INVALID_REQ_RANGE,
            ErrorInner::MaxTimestampNotSynced { .. } => {
                error_code::storage::MAX_TIMESTAMP_NOT_SYNCED
            }
        }
    }
}

pub mod tests {
    use super::*;
    pub use actions::acquire_pessimistic_dagger::tests::{
        must_err as must_acquire_pessimistic_dagger_err,
        must_err_return_value as must_acquire_pessimistic_dagger_return_value_err,
        must_pessimistic_daggered, must_succeed as must_acquire_pessimistic_dagger,
        must_succeed_for_large_solitontxn as must_acquire_pessimistic_dagger_for_large_solitontxn,
        must_succeed_impl as must_acquire_pessimistic_dagger_impl,
        must_succeed_return_value as must_acquire_pessimistic_dagger_return_value,
        must_succeed_with_ttl as must_acquire_pessimistic_dagger_with_ttl,
    };
    pub use actions::cleanup::tests::{
        must_cleanup_with_gc_fence, must_err as must_cleanup_err, must_succeed as must_cleanup,
    };
    pub use actions::commit::tests::{must_err as must_commit_err, must_succeed as must_commit};
    pub use actions::gc::tests::must_succeed as must_gc;
    pub use actions::prewrite::tests::{
        try_pessimistic_prewrite_check_not_exists, try_prewrite_check_not_exists,
        try_prewrite_insert,
    };
    pub use actions::tests::*;
}
