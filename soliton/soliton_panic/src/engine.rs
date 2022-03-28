// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.


use crate::{
    error::{Error, Result},
    storage::{
        kv::{self, Key, Value},
        mvcc::{MvccTxn, MvccTxnExtra},
        txn::{TxnEntry, TxnExtra, TxnStatus},
    },
    util::{
        collections::{HashMap, HashSet},
        hash::{BuildHasherDefault, HasherBuilder},
        time::{self, Duration, Instant},
    },
};

pub const DEFAULT_MAX_TXN_WRITE_SIZE: usize = 1024 * 1024 * 1024;
pub const DEFAULT_MAX_TXN_READ_SIZE: usize = 1024 * 1024 * 1024;
pub const DEFAULT_MAX_TXN_SIZE: usize = 1024 * 1024 * 1024;
pub const DEFAULT_MAX_TXN_WRITE_COUNT: usize = 1024 * 1024;
pub const DEFAULT_MAX_TXN_READ_COUNT: usize = 1024 * 1024;
pub const DEFAULT_MAX_TXN_COUNT: usize = 1024 * 1024;
pub const DEFAULT_MAX_TXN_TIMEOUT: u64 = 60;
pub const DEFAULT_MAX_READ_LOCK_TIME: u64 = 60;
pub const DEFAULT_MAX_WRITE_LOCK_TIME: u64 = 60;
pub const DEFAULT_MAX_LOCK_EXPIRE_TIME: u64 = 60;
pub const DEFAULT_MAX_TXN_PROPOSAL_TIMEOUT: u64 = 60;
pub const DEFAULT_MAX_TXN_APPLY_TIMEOUT: u64 = 60;
pub const DEFAULT_MAX_TXN_PROPOSAL_BACKOFF: u64 = 60;
pub const DEFAULT_MAX_TXN_APPLY_BACKOFF: u64 = 60;
pub const DEFAULT_MAX_TXN_PROPOSAL_BACKOFF_JITTER: u64 = 60;
pub const DEFAULT_MAX_TXN_APPLY_BACKOFF_JITTER: u64 = 60;
pub const DEFAULT_MAX_TXN_PROPOSAL_BACKOFF_FACTOR: u64 = 60;
pub const DEFAULT_MAX_TXN_APPLY_BACKOFF_FACTOR: u64 = 60;
pub const DEFAULT_MAX_TXN_PROPOSAL_BACKOFF_MAX: u64 = 60;
pub const DEFAULT_MAX_TXN_APPLY_BACKOFF_MAX: u64 = 60;
pub const DEFAULT_MAX_TXN_PROPOSAL_BACKOFF_MIN: u64 = 60;
pub const DEFAULT_MAX_TXN_APPLY_BACKOFF_MIN: u64 = 60;
pub const DEFAULT_MAX_TXN_PROPOSAL_BACKOFF_RESET: u64 = 60;
pub const DEFAULT_MAX_TXN_APPLY_BACKOFF_RESET: u64 = 60;
pub const DEFAULT_MAX_TXN_PROPOSAL_BACKOFF_RESET_JITTER: u64 = 60;
pub const DEFAULT_MAX_TXN_APPLY_BACKOFF_RESET_JITTER: u64 = 60;
pub const DEFAULT_MAX_TXN_PROPOSAL_BACKOFF_RESET_FACTOR: u64 = 60;
pub const DEFAULT_MAX_TXN_APPLY_BACKOFF_RESET_FACTOR: u64 = 60;
pub const DEFAULT_MAX_TXN_PROPOSAL_BACKOFF_RESET_MAX: u64 = 60;
pub const DEFAULT_MAX_TXN_APPLY_BACKOFF_RESET_MAX: u64 = 60;
pub const DEFAULT_MAX_TXN_PROPOSAL_BACKOFF_RESET_MIN: u64 = 60;
pub const DEFAULT_MAX_TXN_APPLY_BACKOFF_RESET_MIN: u64 = 60;


#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TxnStatus {
    Pending,
    Committed,
    RolledBack,

}


#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockType {
    Read,
    Write,
}


#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockStatus {
    Acquired,
    Pending,
    Released,
}


#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockMode {
    Pessimistic,
    Optimistic,
}


#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockTtl {
    Forever,
    Timestamp,
}

//lockfree
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockFree {
    Yes,
    No,
}

//foundationdb
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FoundationDb {
    Yes,
    No,
}

//leveldb
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LevelDb {
    Yes,
    No,
}

//sqlite
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Sqlite {
    Yes,
    No,
}

//innodb
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InnoDb {
    Yes,
    No,
}


#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TxnExtra {
    FoundationDb(FoundationDb),
    LevelDb(LevelDb),
    Sqlite(Sqlite),
    InnoDb(InnoDb),
    LockFree(LockFree),

}


#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TxnEntry {
    Pending,
    Committed,
    RolledBack,
}


