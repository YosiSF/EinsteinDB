// Venire Labs Inc 2019
//
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![recursion_limit = "200"]

#[macro_use(
    kv,
    slog_kv,
    slog_error,
    slog_warn,
    slog_record,
    slog_b,
    slog_log,
    slog_record_static
)]
extern crate slog;
#[macro_use]
extern crate slog_global;
#[macro_use]
extern crate prometheus;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate quick_error;
#[macro_use]
extern crate serde_derive;
#[allow(unused_extern_crates)]
extern crate tikv_alloc;

use std::sync::Arc;

pub mod util;

pub mod rocks;
pub use crate::rocks::{
    CFHandle, DBIterator, DBVector, Range, ReadOptions, Snapshot, SyncSnapshot, WriteBatch,
    WriteOptions, DB,
};
mod errors;
pub use crate::errors::*;
mod peekable;
pub use crate::peekable::*;
mod iterable;
pub use crate::iterable::*;
mod mutable;
pub use crate::mutable::*;
mod cf;
pub use crate::cf::*;

#[derive(Clone, Debug)]
pub struct Engines {
    pub kv: Arc<DB>,
    pub raft: Arc<DB>,
}

impl Engines {
    pub fn new(kv_engine: Arc<DB>, raft_engine: Arc<DB>) -> Engines {
        Engines {
            kv: kv_engine,
            raft: raft_engine,
        }
    }

    pub fn write_kv(&self, wb: &WriteBatch) -> Result<()> {
        self.kv.write(wb).map_err(Error::RocksDb)
    }

    pub fn write_kv_opt(&self, wb: &WriteBatch, opts: &WriteOptions) -> Result<()> {
        self.kv.write_opt(wb, opts).map_err(Error::RocksDb)
    }

    pub fn sync_kv(&self) -> Result<()> {
        self.kv.sync_wal().map_err(Error::RocksDb)
    }

    pub fn write_raft(&self, wb: &WriteBatch) -> Result<()> {
        self.raft.write(wb).map_err(Error::RocksDb)
    }

    pub fn write_raft_opt(&self, wb: &WriteBatch, opts: &WriteOptions) -> Result<()> {
        self.raft.write_opt(wb, opts).map_err(Error::RocksDb)
    }

    pub fn sync_raft(&self) -> Result<()> {
        self.raft.sync_wal().map_err(Error::RocksDb)
    }
}

extern crate enum_set;
extern crate ordered_float;
extern crate chrono;
extern crate indexmap;
#[macro_use] extern crate serde_derive;
extern crate uuid;

use std::fmt;

use std::ffi::{
    CString,
};

use std::ops::{
    Deref,
};

use std::os::raw::{
    c_char,
};

use std::rc::{
    Rc,
};

use std::sync::{
    Arc,
};

use std::collections::BTreeMap;

use indexmap::{
    IndexMap,
};

use enum_set::EnumSet;

use ordered_float::OrderedFloat;

use chrono::{
    DateTime,
    Timelike,
};

use uuid::Uuid;

//Causet Space

pub type CausetID = i64;


//causet heap
#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct KnownCausetID(pub CausetID);

impl From<KnownCausetID> for CausetID{
    fn from(k: KnownCausetID) -> CausetID {
       k.0
    }
}