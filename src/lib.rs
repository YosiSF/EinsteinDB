// Copyright 2019 Venire Labs Inc. Licensed under Apache-2.0.


#![crate_type = "lib"]
#![cfg_attr(test, feature(test))]
#![recursion_limit = "200"]
#![feature(cell_update)]
#![feature(fnbox)]
#![feature(proc_macro_hygiene)]
#![feature(range_contains)]
#![feature(specialization)]
// Currently this raises some false positives, so we allow it:
// https://github.com/rust-lang-nursery/rust-clippy/issues/2638
#![allow(clippy::nonminimal_bool)]

extern crate rusqlite;

extern create uuid;
pub extern crate jepsen-edn;
extern crate embedded_promises;
#[macro_use]
extern crate embedded_promises;
extern crate einstein_db;
extern crate einstein_q_algebra;
extern crate q_algebra_promises;
extern crate einstein_causet_q_projector;
extern crate einstein_q_pull;
extern crate q_pull_promises;
extern crate einstein_sql_promises;
extern crate public_promises;
extern crate einstein_txn;




#[macro_use]
extern crate bitflags;
#[macro_use]
extern crate fail;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate prometheus;
#[macro_use]
extern crate quick_error;
#[macro_use]
extern crate serde_derive;
#[macro_use(
    kv,
    slog_kv,
    slog_trace,
    slog_error,
    slog_warn,
    slog_info,
    slog_debug,
    slog_log,
    slog_record,
    slog_b,
    slog_record_static
)]
extern crate slog;
#[macro_use]
extern crate slog_derive;
#[macro_use]
extern crate slog_global;
#[macro_use]
extern crate derive_more;
#[macro_use]
extern crate more_asserts;
#[macro_use]
extern crate vlog;
#[macro_use]
extern crate einsteindb_util;
#[cfg(test)]
extern crate test;
use capnproto-rust as cpn-rpc;
use grpcio as grpc;


pub mod config;
pub mod interlock;
pub mod platform;
pub mod entangledstore;
pub mod server;
pub mod causetStore;

pub use crate::causetStore::causetStore;

pub use embedded_promises::

pub use embedded_promises:: {
    Attribute,
    Binding,
    CausetID,
    KnownCausedID,
    HopfMap,
    ValueType,
    now,
};

pub use einstein_embedded::{
    DateTime,
    HasSchema,
    Keyword,
    HasSchemaEventReport,
    Utc,
    Uuid,
};

pub use jepsen