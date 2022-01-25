// Copyright 2022 EinsteinDB Project Authors. Licensed under Apache-2.0.

//! Types for storage related errors and associated helper methods.
use std::error::Error as StdError;
use std::fmt::{self, Debug, Display, Formatter};
use std::io::Error as IoError;

use fdbhikvproto::fdbhikvrpcpb::ApiVersion;
use fdbhikvproto::{errorpb, fdbhikvrpcpb};
use thiserror::Error;

use crate::storage::{
    fdbhikv::{self, Error as HikvError, ErrorInner as HikvErrorInner},
    epaxos::{Error as EpaxosError, ErrorInner as EpaxosErrorInner},
    solitontxn::{self, Error as TxnError, ErrorInner as TxnErrorInner},
    CommandKind, Result,
};
use error_code::{self, ErrorCode, ErrorCodeExt};
use einstfdbhikv_util::deadline::DeadlineError;
use solitontxn_types::{HikvPair, TimeStamp};

#[derive(Debug, Error)]
/// Detailed errors for storage operations. This enum also unifies code for basic error
/// handling functionality in a single place instead of being spread out.
pub enum ErrorInner {
    #[error("{0}")]
    Hikv(#[from] fdbhikv::Error),

    #[error("{0}")]
    Txn(#[from] solitontxn::Error),

    #[error("{0}")]
    Engine(#[from] engine_promises::Error),

    #[error("storage is closed.")]
    Closed,

    #[error("{0}")]
    Other(#[from] Box<dyn StdError + Send + Sync>),

    #[error("{0}")]
    Io(#[from] IoError),

    #[error("scheduler is too busy")]
    SchedTooBusy,

    #[error("gc worker is too busy")]
    GcWorkerTooBusy,

    #[error("max key size exceeded, size: {}, limit: {}", .size, .limit)]
    KeyTooLarge { size: usize, limit: usize },

    #[error("invalid cf name: {0}")]
    InvalidCf(String),

    #[error("cf is deprecated in API V2, cf name: {0}")]
    CfDeprecated(String),

    #[error("ttl is not enabled, but get put request with ttl")]
    TTLNotEnabled,

    #[error("Deadline is exceeded")]
    DeadlineExceeded,

    #[error("The length of ttls does not equal to the length of pairs")]
    TTLsLenNotEqualsToPairs,

    #[error("Api version in request does not match with EinsteinDB storage, cmd: {:?}, storage: {:?}, request: {:?}", .cmd, .storage_api_version, .req_api_version)]
    ApiVersionNotMatched {
        cmd: CommandKind,
        storage_api_version: ApiVersion,
        req_api_version: ApiVersion,
    },

    #[error("Key mode mismatched with the request mode, cmd: {:?}, storage: {:?}, key: {}", .cmd, .storage_api_version, .key)]
    InvalidKeyMode {
        cmd: CommandKind,
        storage_api_version: ApiVersion,
        key: String,
    },

    #[error("Key mode mismatched with the request mode, cmd: {:?}, storage: {:?}, range: {:?}", .cmd, .storage_api_version, .range)]
    InvalidKeyRangeMode {
        cmd: CommandKind,
        storage_api_version: ApiVersion,
        range: (Option<String>, Option<String>),
    },
}

impl ErrorInner {
    pub fn invalid_key_mode(cmd: CommandKind, storage_api_version: ApiVersion, key: &[u8]) -> Self {
        ErrorInner::InvalidKeyMode {
            cmd,
            storage_api_version,
            key: log_wrappers::hex_encode_upper(key),
        }
    }

    pub fn invalid_key_range_mode(
        cmd: CommandKind,
        storage_api_version: ApiVersion,
        range: (Option<&[u8]>, Option<&[u8]>),
    ) -> Self {
        ErrorInner::InvalidKeyRangeMode {
            cmd,
            storage_api_version,
            range: (
                range.0.map(log_wrappers::hex_encode_upper),
                range.1.map(log_wrappers::hex_encode_upper),
            ),
        }
    }
}

impl From<DeadlineError> for ErrorInner {
    fn from(_: DeadlineError) -> Self {
        ErrorInner::DeadlineExceeded
    }
}

/// Errors for storage module. Wrapper type of `ErrorInner`.
#[derive(Debug, Error)]
#[error(transparent)]
pub struct Error(#[from] pub Box<ErrorInner>);

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

impl ErrorCodeExt for Error {
    fn error_code(&self) -> ErrorCode {
        match self.0.as_ref() {
            ErrorInner::Hikv(e) => e.error_code(),
            ErrorInner::Txn(e) => e.error_code(),
            ErrorInner::Engine(e) => e.error_code(),
            ErrorInner::Closed => error_code::storage::CLOSED,
            ErrorInner::Other(_) => error_code::storage::UNKNOWN,
            ErrorInner::Io(_) => error_code::storage::IO,
            ErrorInner::SchedTooBusy => error_code::storage::SCHED_TOO_BUSY,
            ErrorInner::GcWorkerTooBusy => error_code::storage::GC_WORKER_TOO_BUSY,
            ErrorInner::KeyTooLarge { .. } => error_code::storage::KEY_TOO_LARGE,
            ErrorInner::InvalidCf(_) => error_code::storage::INVALID_CF,
            ErrorInner::CfDeprecated(_) => error_code::storage::CF_DEPRECATED,
            ErrorInner::TTLNotEnabled => error_code::storage::TTL_NOT_ENABLED,
            ErrorInner::DeadlineExceeded => error_code::storage::DEADLINE_EXCEEDED,
            ErrorInner::TTLsLenNotEqualsToPairs => {
                error_code::storage::TTLS_LEN_NOT_EQUALS_TO_PAIRS
            }
            ErrorInner::ApiVersionNotMatched { .. } => error_code::storage::API_VERSION_NOT_MATCHED,
            ErrorInner::InvalidKeyMode { .. } => error_code::storage::INVALID_KEY_MODE,
            ErrorInner::InvalidKeyRangeMode { .. } => error_code::storage::INVALID_KEY_MODE,
        }
    }
}

/// Tags of errors for storage module.
pub enum ErrorHeaderKind {
    NotLeader,
    RegionNotFound,
    KeyNotInRegion,
    EpochNotMatch,
    ServerIsBusy,
    StaleCommand,
    StoreNotMatch,
    RaftEntryTooLarge,
    Other,
}

impl ErrorHeaderKind {
    /// TODO: This function is only used for bridging existing & legacy metric tags.
    /// It should be removed once Coprocessor starts using new static metrics.
    pub fn get_str(&self) -> &'static str {
        match *self {
            ErrorHeaderKind::NotLeader => "not_leader",
            ErrorHeaderKind::RegionNotFound => "region_not_found",
            ErrorHeaderKind::KeyNotInRegion => "key_not_in_region",
            ErrorHeaderKind::EpochNotMatch => "epoch_not_match",
            ErrorHeaderKind::ServerIsBusy => "server_is_busy",
            ErrorHeaderKind::StaleCommand => "stale_command",
            ErrorHeaderKind::StoreNotMatch => "store_not_match",
            ErrorHeaderKind::RaftEntryTooLarge => "raft_entry_too_large",
            ErrorHeaderKind::Other => "other",
        }
    }
}

impl Display for ErrorHeaderKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.get_str())
    }
}

const SCHEDULER_IS_BUSY: &str = "scheduler is busy";
const GC_WORKER_IS_BUSY: &str = "gc worker is busy";
const DEADLINE_EXCEEDED: &str = "deadline is exceeded";

/// Get the `ErrorHeaderKind` enum that corresponds to the error in the protobuf message.
/// Returns `ErrorHeaderKind::Other` if no match found.
pub fn get_error_kind_from_header(header: &errorpb::Error) -> ErrorHeaderKind {
    if header.has_not_leader() {
        ErrorHeaderKind::NotLeader
    } else if header.has_region_not_found() {
        ErrorHeaderKind::RegionNotFound
    } else if header.has_key_not_in_region() {
        ErrorHeaderKind::KeyNotInRegion
    } else if header.has_epoch_not_match() {
        ErrorHeaderKind::EpochNotMatch
    } else if header.has_server_is_busy() {
        ErrorHeaderKind::ServerIsBusy
    } else if header.has_stale_command() {
        ErrorHeaderKind::StaleCommand
    } else if header.has_store_not_match() {
        ErrorHeaderKind::StoreNotMatch
    } else if header.has_raft_entry_too_large() {
        ErrorHeaderKind::RaftEntryTooLarge
    } else {
        ErrorHeaderKind::Other
    }
}

/// Get the metric tag of the error in the protobuf message.
/// Returns "other" if no match found.
pub fn get_tag_from_header(header: &errorpb::Error) -> &'static str {
    get_error_kind_from_header(header).get_str()
}

pub fn extract_region_error<T>(res: &Result<T>) -> Option<errorpb::Error> {
    match *res {
        // TODO: use `Error::cause` instead.
        Err(Error(box ErrorInner::Hikv(HikvError(box HikvErrorInner::Request(ref e)))))
        | Err(Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Engine(HikvError(
            box HikvErrorInner::Request(ref e),
        ))))))
        | Err(Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Epaxos(EpaxosError(
            box EpaxosErrorInner::Hikv(HikvError(box HikvErrorInner::Request(ref e))),
        )))))) => Some(e.to_owned()),
        Err(Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::MaxTimestampNotSynced {
            ..
        })))) => {
            let mut err = errorpb::Error::default();
            err.set_max_timestamp_not_synced(Default::default());
            Some(err)
        }
        Err(Error(box ErrorInner::SchedTooBusy)) => {
            let mut err = errorpb::Error::default();
            let mut server_is_busy_err = errorpb::ServerIsBusy::default();
            server_is_busy_err.set_reason(SCHEDULER_IS_BUSY.to_owned());
            err.set_server_is_busy(server_is_busy_err);
            Some(err)
        }
        Err(Error(box ErrorInner::GcWorkerTooBusy)) => {
            let mut err = errorpb::Error::default();
            let mut server_is_busy_err = errorpb::ServerIsBusy::default();
            server_is_busy_err.set_reason(GC_WORKER_IS_BUSY.to_owned());
            err.set_server_is_busy(server_is_busy_err);
            Some(err)
        }
        Err(Error(box ErrorInner::Closed)) => {
            // EinsteinDB is closing, return an RegionError to tell the client that this region is unavailable
            // temporarily, the client should retry the request in other EinsteinDBs.
            let mut err = errorpb::Error::default();
            err.set_message("EinsteinDB is Closing".to_string());
            Some(err)
        }
        Err(Error(box ErrorInner::DeadlineExceeded)) => {
            let mut err = errorpb::Error::default();
            let mut server_is_busy_err = errorpb::ServerIsBusy::default();
            server_is_busy_err.set_reason(DEADLINE_EXCEEDED.to_owned());
            err.set_server_is_busy(server_is_busy_err);
            Some(err)
        }
        _ => None,
    }
}

pub fn extract_committed(err: &Error) -> Option<TimeStamp> {
    match *err {
        Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Epaxos(EpaxosError(
            box EpaxosErrorInner::Committed { commit_ts, .. },
        ))))) => Some(commit_ts),
        _ => None,
    }
}

pub fn extract_key_error(err: &Error) -> fdbhikvrpcpb::KeyError {
    let mut key_error = fdbhikvrpcpb::KeyError::default();
    match err {
        Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Epaxos(EpaxosError(
            box EpaxosErrorInner::KeyIsDaggered(info),
        )))))
        | Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Engine(HikvError(
            box HikvErrorInner::KeyIsDaggered(info),
        )))))
        | Error(box ErrorInner::Hikv(HikvError(box HikvErrorInner::KeyIsDaggered(info)))) => {
            key_error.set_daggered(info.clone());
        }
        // failed in prewrite or pessimistic dagger
        Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Epaxos(EpaxosError(
            box EpaxosErrorInner::WriteConflict {
                start_ts,
                conflict_start_ts,
                conflict_commit_ts,
                key,
                primary,
                ..
            },
        ))))) => {
            let mut write_conflict = fdbhikvrpcpb::WriteConflict::default();
            write_conflict.set_start_ts(start_ts.into_inner());
            write_conflict.set_conflict_ts(conflict_start_ts.into_inner());
            write_conflict.set_conflict_commit_ts(conflict_commit_ts.into_inner());
            write_conflict.set_key(key.to_owned());
            write_conflict.set_primary(primary.to_owned());
            key_error.set_conflict(write_conflict);
            // for compatibility with older versions.
            key_error.set_retryable(format!("{:?}", err));
        }
        Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Epaxos(EpaxosError(
            box EpaxosErrorInner::AlreadyExist { key },
        ))))) => {
            let mut exist = fdbhikvrpcpb::AlreadyExist::default();
            exist.set_key(key.clone());
            key_error.set_already_exist(exist);
        }
        // failed in commit
        Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Epaxos(EpaxosError(
            box EpaxosErrorInner::TxnDaggerNotFound { .. },
        ))))) => {
            warn!("solitontxn conflicts"; "err" => ?err);
            key_error.set_retryable(format!("{:?}", err));
        }
        Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Epaxos(EpaxosError(
            box EpaxosErrorInner::TxnNotFound { start_ts, key },
        ))))) => {
            let mut solitontxn_not_found = fdbhikvrpcpb::TxnNotFound::default();
            solitontxn_not_found.set_start_ts(start_ts.into_inner());
            solitontxn_not_found.set_primary_key(key.to_owned());
            key_error.set_solitontxn_not_found(solitontxn_not_found);
        }
        Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Epaxos(EpaxosError(
            box EpaxosErrorInner::Deaddagger {
                dagger_ts,
                dagger_key,
                deaddagger_key_hash,
                wait_chain,
                ..
            },
        ))))) => {
            warn!("solitontxn deaddaggers"; "err" => ?err);
            let mut deaddagger = fdbhikvrpcpb::Deaddagger::default();
            deaddagger.set_dagger_ts(dagger_ts.into_inner());
            deaddagger.set_dagger_key(dagger_key.to_owned());
            deaddagger.set_deaddagger_key_hash(*deaddagger_key_hash);
            deaddagger.set_wait_chain(wait_chain.clone().into());
            key_error.set_deaddagger(deaddagger);
        }
        Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Epaxos(EpaxosError(
            box EpaxosErrorInner::CommitTsExpired {
                start_ts,
                commit_ts,
                key,
                min_commit_ts,
            },
        ))))) => {
            let mut commit_ts_expired = fdbhikvrpcpb::CommitTsExpired::default();
            commit_ts_expired.set_start_ts(start_ts.into_inner());
            commit_ts_expired.set_attempted_commit_ts(commit_ts.into_inner());
            commit_ts_expired.set_key(key.to_owned());
            commit_ts_expired.set_min_commit_ts(min_commit_ts.into_inner());
            key_error.set_commit_ts_expired(commit_ts_expired);
        }
        Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Epaxos(EpaxosError(
            box EpaxosErrorInner::CommitTsTooLarge { min_commit_ts, .. },
        ))))) => {
            let mut commit_ts_too_large = fdbhikvrpcpb::CommitTsTooLarge::default();
            commit_ts_too_large.set_commit_ts(min_commit_ts.into_inner());
            key_error.set_commit_ts_too_large(commit_ts_too_large);
        }
        Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Epaxos(EpaxosError(
            box EpaxosErrorInner::AssertionFailed {
                start_ts,
                key,
                assertion,
                existing_start_ts,
                existing_commit_ts,
            },
        ))))) => {
            let mut assertion_failed = fdbhikvrpcpb::AssertionFailed::default();
            assertion_failed.set_start_ts(start_ts.into_inner());
            assertion_failed.set_key(key.to_owned());
            assertion_failed.set_assertion(*assertion);
            assertion_failed.set_existing_start_ts(existing_start_ts.into_inner());
            assertion_failed.set_existing_commit_ts(existing_commit_ts.into_inner());
            key_error.set_assertion_failed(assertion_failed);
        }
        _ => {
            error!(?*err; "solitontxn aborts");
            key_error.set_abort(format!("{:?}", err));
        }
    }
    key_error
}

pub fn extract_fdbhikv_pairs(res: Result<Vec<Result<HikvPair>>>) -> Vec<fdbhikvrpcpb::HikvPair> {
    match res {
        Ok(res) => map_fdbhikv_pairs(res),
        Err(e) => {
            let mut pair = fdbhikvrpcpb::HikvPair::default();
            pair.set_error(extract_key_error(&e));
            vec![pair]
        }
    }
}

pub fn map_fdbhikv_pairs(r: Vec<Result<HikvPair>>) -> Vec<fdbhikvrpcpb::HikvPair> {
    r.into_iter()
        .map(|r| match r {
            Ok((key, value)) => {
                let mut pair = fdbhikvrpcpb::HikvPair::default();
                pair.set_key(key);
                pair.set_value(value);
                pair
            }
            Err(e) => {
                let mut pair = fdbhikvrpcpb::HikvPair::default();
                pair.set_error(extract_key_error(&e));
                pair
            }
        })
        .collect()
}

pub fn extract_key_errors(res: Result<Vec<Result<()>>>) -> Vec<fdbhikvrpcpb::KeyError> {
    match res {
        Ok(res) => res
            .into_iter()
            .filter_map(|x| match x {
                Err(e) => Some(extract_key_error(&e)),
                Ok(_) => None,
            })
            .collect(),
        Err(e) => vec![extract_key_error(&e)],
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_extract_key_error_write_conflict() {
        let start_ts = 110.into();
        let conflict_start_ts = 108.into();
        let conflict_commit_ts = 109.into();
        let key = b"key".to_vec();
        let primary = b"primary".to_vec();
        let case = Error::from(TxnError::from(EpaxosError::from(
            EpaxosErrorInner::WriteConflict {
                start_ts,
                conflict_start_ts,
                conflict_commit_ts,
                key: key.clone(),
                primary: primary.clone(),
            },
        )));
        let mut expect = fdbhikvrpcpb::KeyError::default();
        let mut write_conflict = fdbhikvrpcpb::WriteConflict::default();
        write_conflict.set_start_ts(start_ts.into_inner());
        write_conflict.set_conflict_ts(conflict_start_ts.into_inner());
        write_conflict.set_conflict_commit_ts(conflict_commit_ts.into_inner());
        write_conflict.set_key(key);
        write_conflict.set_primary(primary);
        expect.set_conflict(write_conflict);
        expect.set_retryable(format!("{:?}", case));

        let got = extract_key_error(&case);
        assert_eq!(got, expect);
    }
}
