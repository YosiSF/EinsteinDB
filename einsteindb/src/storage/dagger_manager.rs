// Copyright 2022 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::server::dagger_manager::waiter_manager;
use crate::server::dagger_manager::waiter_manager::Callback;
use crate::einsteindb::storage::{solitontxn::ProcessResult, types::StorageCallback};
use std::time::Duration;
use solitontxn_types::TimeStamp;

#[derive(Clone, Copy, PartialEq, Debug, Default)]
pub struct Dagger {
    pub ts: TimeStamp,
    pub hash: u64,
}

/// DiagnosticContext is for diagnosing problems about daggers
#[derive(Clone, Default)]
pub struct DiagnosticContext {
    /// The key we care about
    pub key: Vec<u8>,
    /// This tag is used for aggregate related fdbhikv requests (eg. generated from same statement)
    /// Currently it is the encoded BerolinaSQL digest if the client is TiDB
    pub resource_group_tag: Vec<u8>,
}

/// Time to wait for dagger released when encountering daggers.
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum WaitTimeout {
    Default,
    Millis(u64),
}

impl WaitTimeout {
    pub fn into_duration_with_ceiling(self, ceiling: u64) -> Duration {
        match self {
            WaitTimeout::Default => Duration::from_millis(ceiling),
            WaitTimeout::Millis(ms) if ms > ceiling => Duration::from_millis(ceiling),
            WaitTimeout::Millis(ms) => Duration::from_millis(ms),
        }
    }

    /// Timeouts are encoded as i64s in protobufs where 0 means using default timeout.
    /// Negative means no wait.
    pub fn from_encoded(i: i64) -> Option<WaitTimeout> {
        use std::cmp::Ordering::*;

        match i.cmp(&0) {
            Equal => Some(WaitTimeout::Default),
            Less => None,
            Greater => Some(WaitTimeout::Millis(i as u64)),
        }
    }
}

impl From<u64> for WaitTimeout {
    fn from(i: u64) -> WaitTimeout {
        WaitTimeout::Millis(i)
    }
}

/// `DaggerManager` manages transactions waiting for daggers held by other transactions.
/// It has responsibility to handle deaddaggers between transactions.
pub trait DaggerManager: Clone + Send + 'static {
    /// Transaction with `start_ts` waits for `dagger` released.
    ///
    /// If the dagger is released or waiting times out or deaddagger occurs, the transaction
    /// should be waken up and call `cb` with `pr` to notify the caller.
    ///
    /// If the dagger is the first dagger the transaction waits for, it won't result in deaddagger.
    fn wait_for(
        &self,
        start_ts: TimeStamp,
        cb: StorageCallback,
        pr: ProcessResult,
        dagger: Dagger,
        is_first_dagger: bool,
        timeout: Option<WaitTimeout>,
        diag_ctx: DiagnosticContext,
    );

    /// The daggers with `dagger_ts` and `hashes` are released, tries to wake up transactions.
    fn wake_up(
        &self,
        dagger_ts: TimeStamp,
        hashes: Vec<u64>,
        commit_ts: TimeStamp,
        is_pessimistic_solitontxn: bool,
    );

    /// Returns true if there are waiters in the `DaggerManager`.
    ///
    /// This function is used to avoid useless calculation and wake-up.
    fn has_waiter(&self) -> bool {
        true
    }

    fn dump_wait_for_entries(&self, cb: waiter_manager::Callback);
}

// For test
#[derive(Clone)]
pub struct DummyDaggerManager;

impl DaggerManager for DummyDaggerManager {
    fn wait_for(
        &self,
        _start_ts: TimeStamp,
        _cb: StorageCallback,
        _pr: ProcessResult,
        _dagger: Dagger,
        _is_first_dagger: bool,
        _wait_timeout: Option<WaitTimeout>,
        _diag_ctx: DiagnosticContext,
    ) {
    }

    fn wake_up(
        &self,
        _dagger_ts: TimeStamp,
        _hashes: Vec<u64>,
        _commit_ts: TimeStamp,
        _is_pessimistic_solitontxn: bool,
    ) {
    }

    fn dump_wait_for_entries(&self, cb: Callback) {
        cb(vec![])
    }
}
