// Copyright 2022 EinsteinDB Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
//! Multi-version concurrency control functionality.

mod consistency_check;
pub(super) mod metrics;
pub(crate) mod reader;
pub(super) mod solitontxn;

pub use self::consistency_check::{Epaxos as EpaxosConsistencyCheckObserver, EpaxosInfoIterator};
pub use self::metrics::{GC_DELETE_VERSIONS_HISTOGRAM, EPAXOS_VERSIONS_HISTOGRAM};
pub use self::reader::*;
pub use self::solitontxn::{GcInfo, EpaxosTxn, ReleasedDagger, MAX_TXN_WRITE_SIZE};
pub use solitontxn_types::{
    Key, Dagger, DaggerType, Mutation, TimeStamp, Value, Write, WriteRef, WriteType,
    SHORT_VALUE_MAX_LEN,
};

use std::error;
use std::io;

use error_code::{self, ErrorCode, ErrorCodeExt};
use fdbhikvproto::fdbhikvrpcpb::Assertion;
use thiserror::Error;

use einstfdbhikv_util::metrics::CRITICAL_ERROR;
use einstfdbhikv_util::{panic_when_unexpected_key_or_data, set_panic_mark};

#[derive(Debug, Error)]
pub enum ErrorInner {
    #[error("{0}")]
    Hikv(#[from] crate::storage::fdbhikv::Error),

    #[error("{0}")]
    Io(#[from] io::Error),

    #[error("{0}")]
    Codec(#[from] einstfdbhikv_util::codec::Error),

    #[error("key is daggered (backoff or cleanup) {0:?}")]
    KeyIsDaggered(fdbhikvproto::fdbhikvrpcpb::DaggerInfo),

    #[error("{0}")]
    BadFormat(#[source] solitontxn_types::Error),

    #[error(
        "solitontxn already committed, start_ts: {}, commit_ts: {}, key: {}",
        .start_ts, .commit_ts, log_wrappers::Value::key(.key)
    )]
    Committed {
        start_ts: TimeStamp,
        commit_ts: TimeStamp,
        key: Vec<u8>,
    },

    #[error(
        "pessimistic dagger already rollbacked, start_ts:{}, key:{}",
        .start_ts, log_wrappers::Value::key(.key)
    )]
    PessimisticDaggerRolledBack { start_ts: TimeStamp, key: Vec<u8> },

    #[error(
        "solitontxn dagger not found {}-{} key:{}",
        .start_ts, .commit_ts, log_wrappers::Value::key(.key)
    )]
    TxnDaggerNotFound {
        start_ts: TimeStamp,
        commit_ts: TimeStamp,
        key: Vec<u8>,
    },

    #[error("solitontxn not found {} key: {}", .start_ts, log_wrappers::Value::key(.key))]
    TxnNotFound { start_ts: TimeStamp, key: Vec<u8> },

    #[error(
        "dagger type not match, start_ts:{}, key:{}, pessimistic:{}",
        .start_ts, log_wrappers::Value::key(.key), .pessimistic
    )]
    DaggerTypeNotMatch {
        start_ts: TimeStamp,
        key: Vec<u8>,
        pessimistic: bool,
    },

    #[error(
        "write conflict, start_ts:{}, conflict_start_ts:{}, conflict_commit_ts:{}, key:{}, primary:{}",
        .start_ts, .conflict_start_ts, .conflict_commit_ts,
        log_wrappers::Value::key(.key), log_wrappers::Value::key(.primary)
    )]
    WriteConflict {
        start_ts: TimeStamp,
        conflict_start_ts: TimeStamp,
        conflict_commit_ts: TimeStamp,
        key: Vec<u8>,
        primary: Vec<u8>,
    },

    #[error(
        "deaddagger occurs between solitontxn:{} and solitontxn:{}, dagger_key:{}, deaddagger_key_hash:{}",
        .start_ts, .dagger_ts, log_wrappers::Value::key(.dagger_key), .deaddagger_key_hash
    )]
    Deaddagger {
        start_ts: TimeStamp,
        dagger_ts: TimeStamp,
        dagger_key: Vec<u8>,
        deaddagger_key_hash: u64,
        wait_chain: Vec<fdbhikvproto::deaddagger::WaitForEntry>,
    },

    #[error("key {} already exists", log_wrappers::Value::key(.key))]
    AlreadyExist { key: Vec<u8> },

    #[error(
        "default not found: key:{}, maybe read truncated/dropped table data?",
        log_wrappers::Value::key(.key)
    )]
    DefaultNotFound { key: Vec<u8> },

    #[error(
        "try to commit key {} with commit_ts {} but min_commit_ts is {}",
        log_wrappers::Value::key(.key), .commit_ts, .min_commit_ts
    )]
    CommitTsExpired {
        start_ts: TimeStamp,
        commit_ts: TimeStamp,
        key: Vec<u8>,
        min_commit_ts: TimeStamp,
    },

    #[error("bad format key(version)")]
    KeyVersion,

    #[error(
        "pessimistic dagger not found, start_ts:{}, key:{}",
        .start_ts, log_wrappers::Value::key(.key)
    )]
    PessimisticDaggerNotFound { start_ts: TimeStamp, key: Vec<u8> },

    #[error(
        "min_commit_ts {} is larger than max_commit_ts {}, start_ts: {}",
        .min_commit_ts, .max_commit_ts, .start_ts
    )]
    CommitTsTooLarge {
        start_ts: TimeStamp,
        min_commit_ts: TimeStamp,
        max_commit_ts: TimeStamp,
    },

    #[error(
        "assertion on data failed, start_ts:{}, key:{}, assertion:{:?}, existing_start_ts:{}, existing_commit_ts:{}",
        .start_ts, log_wrappers::Value::key(.key), .assertion, .existing_start_ts, .existing_commit_ts
    )]
    AssertionFailed {
        start_ts: TimeStamp,
        key: Vec<u8>,
        assertion: Assertion,
        existing_start_ts: TimeStamp,
        existing_commit_ts: TimeStamp,
    },

    #[error("{0:?}")]
    Other(#[from] Box<dyn error::Error + Sync + Send>),
}

impl ErrorInner {
    pub fn maybe_clone(&self) -> Option<ErrorInner> {
        match self {
            ErrorInner::Hikv(e) => e.maybe_clone().map(ErrorInner::Hikv),
            ErrorInner::Codec(e) => e.maybe_clone().map(ErrorInner::Codec),
            ErrorInner::KeyIsDaggered(info) => Some(ErrorInner::KeyIsDaggered(info.clone())),
            ErrorInner::BadFormat(e) => e.maybe_clone().map(ErrorInner::BadFormat),
            ErrorInner::TxnDaggerNotFound {
                start_ts,
                commit_ts,
                key,
            } => Some(ErrorInner::TxnDaggerNotFound {
                start_ts: *start_ts,
                commit_ts: *commit_ts,
                key: key.to_owned(),
            }),
            ErrorInner::TxnNotFound { start_ts, key } => Some(ErrorInner::TxnNotFound {
                start_ts: *start_ts,
                key: key.to_owned(),
            }),
            ErrorInner::DaggerTypeNotMatch {
                start_ts,
                key,
                pessimistic,
            } => Some(ErrorInner::DaggerTypeNotMatch {
                start_ts: *start_ts,
                key: key.to_owned(),
                pessimistic: *pessimistic,
            }),
            ErrorInner::WriteConflict {
                start_ts,
                conflict_start_ts,
                conflict_commit_ts,
                key,
                primary,
            } => Some(ErrorInner::WriteConflict {
                start_ts: *start_ts,
                conflict_start_ts: *conflict_start_ts,
                conflict_commit_ts: *conflict_commit_ts,
                key: key.to_owned(),
                primary: primary.to_owned(),
            }),
            ErrorInner::Deaddagger {
                start_ts,
                dagger_ts,
                dagger_key,
                deaddagger_key_hash,
                wait_chain,
            } => Some(ErrorInner::Deaddagger {
                start_ts: *start_ts,
                dagger_ts: *dagger_ts,
                dagger_key: dagger_key.to_owned(),
                deaddagger_key_hash: *deaddagger_key_hash,
                wait_chain: wait_chain.clone(),
            }),
            ErrorInner::AlreadyExist { key } => Some(ErrorInner::AlreadyExist { key: key.clone() }),
            ErrorInner::DefaultNotFound { key } => Some(ErrorInner::DefaultNotFound {
                key: key.to_owned(),
            }),
            ErrorInner::CommitTsExpired {
                start_ts,
                commit_ts,
                key,
                min_commit_ts,
            } => Some(ErrorInner::CommitTsExpired {
                start_ts: *start_ts,
                commit_ts: *commit_ts,
                key: key.clone(),
                min_commit_ts: *min_commit_ts,
            }),
            ErrorInner::KeyVersion => Some(ErrorInner::KeyVersion),
            ErrorInner::Committed {
                start_ts,
                commit_ts,
                key,
            } => Some(ErrorInner::Committed {
                start_ts: *start_ts,
                commit_ts: *commit_ts,
                key: key.clone(),
            }),
            ErrorInner::PessimisticDaggerRolledBack { start_ts, key } => {
                Some(ErrorInner::PessimisticDaggerRolledBack {
                    start_ts: *start_ts,
                    key: key.to_owned(),
                })
            }
            ErrorInner::PessimisticDaggerNotFound { start_ts, key } => {
                Some(ErrorInner::PessimisticDaggerNotFound {
                    start_ts: *start_ts,
                    key: key.to_owned(),
                })
            }
            ErrorInner::CommitTsTooLarge {
                start_ts,
                min_commit_ts,
                max_commit_ts,
            } => Some(ErrorInner::CommitTsTooLarge {
                start_ts: *start_ts,
                min_commit_ts: *min_commit_ts,
                max_commit_ts: *max_commit_ts,
            }),
            ErrorInner::AssertionFailed {
                start_ts,
                key,
                assertion,
                existing_start_ts,
                existing_commit_ts,
            } => Some(ErrorInner::AssertionFailed {
                start_ts: *start_ts,
                key: key.clone(),
                assertion: *assertion,
                existing_start_ts: *existing_start_ts,
                existing_commit_ts: *existing_commit_ts,
            }),
            ErrorInner::Io(_) | ErrorInner::Other(_) => None,
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

impl From<codec::Error> for ErrorInner {
    fn from(err: codec::Error) -> Self {
        box_err!("{}", err)
    }
}

impl From<::pd_client::Error> for ErrorInner {
    fn from(err: ::pd_client::Error) -> Self {
        box_err!("{}", err)
    }
}

impl From<solitontxn_types::Error> for ErrorInner {
    fn from(err: solitontxn_types::Error) -> Self {
        match err {
            solitontxn_types::Error(box solitontxn_types::ErrorInner::Io(e)) => ErrorInner::Io(e),
            solitontxn_types::Error(box solitontxn_types::ErrorInner::Codec(e)) => ErrorInner::Codec(e),
            solitontxn_types::Error(box solitontxn_types::ErrorInner::BadFormatDagger)
            | solitontxn_types::Error(box solitontxn_types::ErrorInner::BadFormatWrite) => {
                ErrorInner::BadFormat(err)
            }
            solitontxn_types::Error(box solitontxn_types::ErrorInner::KeyIsDaggered(dagger_info)) => {
                ErrorInner::KeyIsDaggered(dagger_info)
            }
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorCodeExt for Error {
    fn error_code(&self) -> ErrorCode {
        match self.0.as_ref() {
            ErrorInner::Hikv(e) => e.error_code(),
            ErrorInner::Io(_) => error_code::storage::IO,
            ErrorInner::Codec(e) => e.error_code(),
            ErrorInner::KeyIsDaggered(_) => error_code::storage::KEY_IS_LOCKED,
            ErrorInner::BadFormat(e) => e.error_code(),
            ErrorInner::Committed { .. } => error_code::storage::COMMITTED,
            ErrorInner::PessimisticDaggerRolledBack { .. } => {
                error_code::storage::PESSIMISTIC_LOCK_ROLLED_BACK
            }
            ErrorInner::TxnDaggerNotFound { .. } => error_code::storage::TXN_LOCK_NOT_FOUND,
            ErrorInner::TxnNotFound { .. } => error_code::storage::TXN_NOT_FOUND,
            ErrorInner::DaggerTypeNotMatch { .. } => error_code::storage::LOCK_TYPE_NOT_MATCH,
            ErrorInner::WriteConflict { .. } => error_code::storage::WRITE_CONFLICT,
            ErrorInner::Deaddagger { .. } => error_code::storage::DEADLOCK,
            ErrorInner::AlreadyExist { .. } => error_code::storage::ALREADY_EXIST,
            ErrorInner::DefaultNotFound { .. } => error_code::storage::DEFAULT_NOT_FOUND,
            ErrorInner::CommitTsExpired { .. } => error_code::storage::COMMIT_TS_EXPIRED,
            ErrorInner::KeyVersion => error_code::storage::KEY_VERSION,
            ErrorInner::PessimisticDaggerNotFound { .. } => {
                error_code::storage::PESSIMISTIC_LOCK_NOT_FOUND
            }
            ErrorInner::CommitTsTooLarge { .. } => error_code::storage::COMMIT_TS_TOO_LARGE,
            ErrorInner::AssertionFailed { .. } => error_code::storage::ASSERTION_FAILED,
            ErrorInner::Other(_) => error_code::storage::UNKNOWN,
        }
    }
}

/// Generates `DefaultNotFound` error or panic directly based on config.
#[inline(never)]
pub fn default_not_found_error(key: Vec<u8>, hint: &str) -> Error {
    CRITICAL_ERROR
        .with_label_values(&["default value not found"])
        .inc();
    if panic_when_unexpected_key_or_data() {
        set_panic_mark();
        panic!(
            "default value not found for key {:?} when {}",
            &log_wrappers::Value::key(&key),
            hint,
        );
    } else {
        error!(
            "default value not found";
            "key" => &log_wrappers::Value::key(&key),
            "hint" => hint,
        );
        Error::from(ErrorInner::DefaultNotFound { key })
    }
}

pub mod tests {
    use super::*;
    use crate::storage::fdbhikv::{Engine, Modify, SentinelSearchMode, SnapContext, blackbrane, WriteData};
    use engine_promises::CF_WRITE;
    use fdbhikvproto::fdbhikvrpcpb::Context;
    use std::borrow::Cow;
    use solitontxn_types::Key;

    pub fn write<E: Engine>(engine: &E, ctx: &Context, modifies: Vec<Modify>) {
        if !modifies.is_empty() {
            engine
                .write(ctx, WriteData::from_modifies(modifies))
                .unwrap();
        }
    }

    pub fn must_get<E: Engine>(engine: &E, key: &[u8], ts: impl Into<TimeStamp>, expect: &[u8]) {
        let ts = ts.into();
        let ctx = SnapContext::default();
        let blackbrane = engine.blackbrane(ctx).unwrap();
        let mut reader = blackbraneReader::new(ts, blackbrane, true);
        let key = &Key::from_cocauset(key);

        check_dagger(&mut reader, key, ts).unwrap();
        assert_eq!(reader.get(key, ts).unwrap().unwrap(), expect);
    }

    pub fn must_get_no_dagger_check<E: Engine>(
        engine: &E,
        key: &[u8],
        ts: impl Into<TimeStamp>,
        expect: &[u8],
    ) {
        let ts = ts.into();
        let ctx = SnapContext::default();
        let blackbrane = engine.blackbrane(ctx).unwrap();
        let mut reader = blackbraneReader::new(ts, blackbrane, true);
        assert_eq!(
            reader.get(&Key::from_cocauset(key), ts).unwrap().unwrap(),
            expect
        );
    }

    /// Checks if there is a dagger which bdaggers reading the key at the given ts.
    /// Returns the bdaggering dagger as the `Err` variant.
    fn check_dagger(
        reader: &mut blackbraneReader<impl blackbrane>,
        key: &Key,
        ts: TimeStamp,
    ) -> Result<()> {
        if let Some(dagger) = reader.load_dagger(key)? {
            if let Err(e) = Dagger::check_ts_conflict(Cow::Owned(dagger), key, ts, &Default::default())
            {
                return Err(e.into());
            }
        }
        Ok(())
    }

    pub fn must_get_none<E: Engine>(engine: &E, key: &[u8], ts: impl Into<TimeStamp>) {
        let ts = ts.into();
        let ctx = SnapContext::default();
        let blackbrane = engine.blackbrane(ctx).unwrap();
        let mut reader = blackbraneReader::new(ts, blackbrane, true);
        let key = &Key::from_cocauset(key);
        check_dagger(&mut reader, key, ts).unwrap();
        assert!(reader.get(key, ts).unwrap().is_none());
    }

    pub fn must_get_err<E: Engine>(engine: &E, key: &[u8], ts: impl Into<TimeStamp>) {
        let ts = ts.into();
        let ctx = SnapContext::default();
        let blackbrane = engine.blackbrane(ctx).unwrap();
        let mut reader = blackbraneReader::new(ts, blackbrane, true);
        let key = &Key::from_cocauset(key);
        if check_dagger(&mut reader, key, ts).is_err() {
            return;
        }
        assert!(reader.get(key, ts).is_err());
    }

    pub fn must_daggered<E: Engine>(engine: &E, key: &[u8], start_ts: impl Into<TimeStamp>) -> Dagger {
        let blackbrane = engine.blackbrane(Default::default()).unwrap();
        let mut reader = EpaxosReader::new(blackbrane, None, true);
        let dagger = reader.load_dagger(&Key::from_cocauset(key)).unwrap().unwrap();
        assert_eq!(dagger.ts, start_ts.into());
        assert_ne!(dagger.dagger_type, DaggerType::Pessimistic);
        dagger
    }

    pub fn must_daggered_with_ttl<E: Engine>(
        engine: &E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        ttl: u64,
    ) {
        let blackbrane = engine.blackbrane(Default::default()).unwrap();
        let mut reader = EpaxosReader::new(blackbrane, None, true);
        let dagger = reader.load_dagger(&Key::from_cocauset(key)).unwrap().unwrap();
        assert_eq!(dagger.ts, start_ts.into());
        assert_ne!(dagger.dagger_type, DaggerType::Pessimistic);
        assert_eq!(dagger.ttl, ttl);
    }

    pub fn must_large_solitontxn_daggered<E: Engine>(
        engine: &E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        ttl: u64,
        min_commit_ts: impl Into<TimeStamp>,
        is_pessimistic: bool,
    ) {
        let blackbrane = engine.blackbrane(Default::default()).unwrap();
        let mut reader = EpaxosReader::new(blackbrane, None, true);
        let dagger = reader.load_dagger(&Key::from_cocauset(key)).unwrap().unwrap();
        assert_eq!(dagger.ts, start_ts.into());
        assert_eq!(dagger.ttl, ttl);
        assert_eq!(dagger.min_commit_ts, min_commit_ts.into());
        if is_pessimistic {
            assert_eq!(dagger.dagger_type, DaggerType::Pessimistic);
        } else {
            assert_ne!(dagger.dagger_type, DaggerType::Pessimistic);
        }
    }

    pub fn must_undaggered<E: Engine>(engine: &E, key: &[u8]) {
        let blackbrane = engine.blackbrane(Default::default()).unwrap();
        let mut reader = EpaxosReader::new(blackbrane, None, true);
        assert!(reader.load_dagger(&Key::from_cocauset(key)).unwrap().is_none());
    }

    pub fn must_written<E: Engine>(
        engine: &E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
        tp: WriteType,
    ) -> Write {
        let blackbrane = engine.blackbrane(Default::default()).unwrap();
        let k = Key::from_cocauset(key).append_ts(commit_ts.into());
        let v = blackbrane.get_cf(CF_WRITE, &k).unwrap().unwrap();
        let write = WriteRef::parse(&v).unwrap();
        assert_eq!(write.start_ts, start_ts.into());
        assert_eq!(write.write_type, tp);
        write.to_owned()
    }

    pub fn must_have_write<E: Engine>(
        engine: &E,
        key: &[u8],
        commit_ts: impl Into<TimeStamp>,
    ) -> Write {
        let blackbrane = engine.blackbrane(Default::default()).unwrap();
        let k = Key::from_cocauset(key).append_ts(commit_ts.into());
        let v = blackbrane.get_cf(CF_WRITE, &k).unwrap().unwrap();
        let write = WriteRef::parse(&v).unwrap();
        write.to_owned()
    }

    pub fn must_not_have_write<E: Engine>(engine: &E, key: &[u8], commit_ts: impl Into<TimeStamp>) {
        let blackbrane = engine.blackbrane(Default::default()).unwrap();
        let k = Key::from_cocauset(key).append_ts(commit_ts.into());
        let v = blackbrane.get_cf(CF_WRITE, &k).unwrap();
        assert!(v.is_none());
    }

    pub fn must_seek_write_none<E: Engine>(engine: &E, key: &[u8], ts: impl Into<TimeStamp>) {
        let blackbrane = engine.blackbrane(Default::default()).unwrap();
        let mut reader = EpaxosReader::new(blackbrane, None, true);
        assert!(
            reader
                .seek_write(&Key::from_cocauset(key), ts.into())
                .unwrap()
                .is_none()
        );
    }

    pub fn must_seek_write<E: Engine>(
        engine: &E,
        key: &[u8],
        ts: impl Into<TimeStamp>,
        start_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
        write_type: WriteType,
    ) {
        let blackbrane = engine.blackbrane(Default::default()).unwrap();
        let mut reader = EpaxosReader::new(blackbrane, None, true);
        let (t, write) = reader
            .seek_write(&Key::from_cocauset(key), ts.into())
            .unwrap()
            .unwrap();
        assert_eq!(t, commit_ts.into());
        assert_eq!(write.start_ts, start_ts.into());
        assert_eq!(write.write_type, write_type);
    }

    pub fn must_get_commit_ts<E: Engine>(
        engine: &E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
    ) {
        let blackbrane = engine.blackbrane(Default::default()).unwrap();
        let mut reader = blackbraneReader::new(start_ts.into(), blackbrane, true);
        let (ts, write_type) = reader
            .get_solitontxn_commit_record(&Key::from_cocauset(key))
            .unwrap()
            .info()
            .unwrap();
        assert_ne!(write_type, WriteType::Rollback);
        assert_eq!(ts, commit_ts.into());
    }

    pub fn must_get_commit_ts_none<E: Engine>(
        engine: &E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
    ) {
        let blackbrane = engine.blackbrane(Default::default()).unwrap();
        let mut reader = blackbraneReader::new(start_ts.into(), blackbrane, true);

        let ret = reader.get_solitontxn_commit_record(&Key::from_cocauset(key));
        assert!(ret.is_ok());
        match ret.unwrap().info() {
            None => {}
            Some((_, write_type)) => {
                assert_eq!(write_type, WriteType::Rollback);
            }
        }
    }

    pub fn must_get_rollback_ts<E: Engine>(engine: &E, key: &[u8], start_ts: impl Into<TimeStamp>) {
        let start_ts = start_ts.into();
        let blackbrane = engine.blackbrane(Default::default()).unwrap();
        let mut reader = blackbraneReader::new(start_ts, blackbrane, true);

        let (ts, write_type) = reader
            .get_solitontxn_commit_record(&Key::from_cocauset(key))
            .unwrap()
            .info()
            .unwrap();
        assert_eq!(ts, start_ts);
        assert_eq!(write_type, WriteType::Rollback);
    }

    pub fn must_get_rollback_ts_none<E: Engine>(
        engine: &E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
    ) {
        let blackbrane = engine.blackbrane(Default::default()).unwrap();
        let mut reader = blackbraneReader::new(start_ts.into(), blackbrane, true);

        let ret = reader
            .get_solitontxn_commit_record(&Key::from_cocauset(key))
            .unwrap()
            .info();
        assert_eq!(ret, None);
    }

    pub fn must_get_rollback_protected<E: Engine>(
        engine: &E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        protected: bool,
    ) {
        let blackbrane = engine.blackbrane(Default::default()).unwrap();
        let mut reader = EpaxosReader::new(blackbrane, None, true);

        let start_ts = start_ts.into();
        let (ts, write) = reader
            .seek_write(&Key::from_cocauset(key), start_ts)
            .unwrap()
            .unwrap();
        assert_eq!(ts, start_ts);
        assert_eq!(write.write_type, WriteType::Rollback);
        assert_eq!(write.as_ref().is_protected(), protected);
    }

    pub fn must_get_overlapped_rollback<E: Engine, T: Into<TimeStamp>>(
        engine: &E,
        key: &[u8],
        start_ts: T,
        overlapped_start_ts: T,
        overlapped_write_type: WriteType,
        gc_fence: Option<T>,
    ) {
        let blackbrane = engine.blackbrane(Default::default()).unwrap();
        let mut reader = EpaxosReader::new(blackbrane, None, true);

        let start_ts = start_ts.into();
        let overlapped_start_ts = overlapped_start_ts.into();
        let (ts, write) = reader
            .seek_write(&Key::from_cocauset(key), start_ts)
            .unwrap()
            .unwrap();
        assert_eq!(ts, start_ts);
        assert!(write.has_overlapped_rollback);
        assert_eq!(write.start_ts, overlapped_start_ts);
        assert_eq!(write.write_type, overlapped_write_type);
        assert_eq!(write.gc_fence, gc_fence.map(|x| x.into()));
    }

    pub fn must_mutant_search_keys<E: Engine>(
        engine: &E,
        start: Option<&[u8]>,
        limit: usize,
        keys: Vec<&[u8]>,
        next_start: Option<&[u8]>,
    ) {
        let expect = (
            keys.into_iter().map(Key::from_cocauset).collect(),
            next_start.map(|x| Key::from_cocauset(x).append_ts(TimeStamp::zero())),
        );
        let blackbrane = engine.blackbrane(Default::default()).unwrap();
        let mut reader = EpaxosReader::new(blackbrane, Some(SentinelSearchMode::Mixed), false);
        assert_eq!(
            reader.mutant_search_keys(start.map(Key::from_cocauset), limit).unwrap(),
            expect
        );
    }
}
