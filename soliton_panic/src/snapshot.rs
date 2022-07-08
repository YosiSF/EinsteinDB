// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.
// -----------------------------------------------------------------------------
//! # EinsteinDB
//! # ----------------------------------------------------------------
//!
//!   #[macro_use]
//! extern crate lazy_static;
//!
//! #[macro_use]
//! extern crate serde_derive;
//!
//! #[macro_use]
//! extern crate serde_json;
use std::fmt;
use std::hash::Hash;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::rc::Rc;
use std::cell::RefCell;
use std::cell::Ref;
use std::ops::{
    Add,
    AddAssign,
    Sub,
    SubAssign,
    Mul,
    MulAssign,
    Div,
    DivAssign,
    Deref,
    DerefMut,
    Index,
    IndexMut,
};


// -----------------------------------------------------------------------------
//! # EinsteinDB
//! # ----------------------------------------------------------------
//! # Observable
//! # ----------------------------------------------------------------
//! # Copyright (C) 2020 EinsteinDB Project Authors. All rights reserved.
//! # License: Apache-2.0 License Terms for the Project, see the LICENSE file.
//! # ----------------------------------------------------------------
//! # Redistribution and use in source and binary forms, with or without
//! # modification, are permitted provided that the following conditions are met:
//! # - Redistributions of source code must retain the above copyright notice,
//! # this list of conditions and the following disclaimer.
//! # - Redistributions in binary form must reproduce the above copyright notice,
//! # this list of conditions and the following disclaimer in the documentation
//! # and/or other materials provided with the distribution.
//! # - Neither the name of the EinsteinDB Project Authors nor the names of its
//! # contributors may be used to endorse or promote products derived from this
//! # software without specific prior written permission.
//!
//! # THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
//! # AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
//! # IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
//! # ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
//! # LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
//! # CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
//! # SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
//! # INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
//! # CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
//! # ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
//! # POSSIBILITY OF SUCH DAMAGE.
//!
//! # -----------------------------------------------------------------------------
use std::ops::Deref;
use std::ops::DerefMut;
use std::ops::Index;
use std::ops::IndexMut;
use std::ops::Add;
use std::sync::Arc;
use std::time::{Duration, Instant};

use EinsteinDB::soliton_panic::snapshot::Snapshot;
use EinsteinDB::soliton_panic::snapshot::Snapshotable;


use EinsteinDB::causet::{self, Causet};
use EinsteinDB::causet::{CausetQ, CausetQRef};
use EinsteinDB::causetq::{CausetQ, CausetQRef};
use EinsteinDB::einstein_db::{self, EinsteinDB};


use crate as soliton_panic;

::lazy_static! {
    pub static ref SNAPSHOT_DB: EinsteinDB = EinsteinDB::new();
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SnapshotId {
    pub id: u64,
}


impl SnapshotId {
    pub fn new(id: u64) -> Self {
        Self { id }
    }
}


impl fmt::Display for SnapshotId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.id)
    }
}


impl PartialOrd for SnapshotId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct Observable<T> {
    pub value: T,
    pub observers: Vec<Arc<dyn Observer<T>>>,
}

/// Observer is a trait that defines the interface for listening to changes in an observable.
/// The observer is notified when the observable changes.
/// This is a trait because it is not possible to implement it for a concrete type.
///


////The SQL CUBE BY clause is used to specify a set of dimensions to be used in the SQL query.

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CubeBy<T> {
    pub dimensions: Vec<T>,
}

pub struct CubeByRef<T> {
    pub dimensions: Vec<T>,
}

pub trait Observer<T> {
    fn update(&self, value: &T);
}


#[derive(Clone, Debug)]
pub struct ObserverWeak<T> {
    pub observer: Arc<dyn Observer<T>>,
}


#[derive(Clone, Debug)]
pub struct PanicLightlikePersistence;


#[derive(Clone, Debug)]
pub struct PanicMerkleTree;

impl PanicMerkleTree {
    pub fn new() -> Self {
        panic!()
    }
}


impl Deref for PanicMerkleTree {
    type Target = PanicLightlikePersistence;
    fn deref(&self) -> &Self::Target {
        panic!()
    }
}



/// # PanicLightlikePersistence
/// # ----------------------------------------------------------------
///
///
/// # ----------------------------------------------------------------
/// # PanicMerkleTree::DBOptions


#[derive(Clone, Debug)]
pub struct PanicLightlikePersistenceDBOptions {
    pub db_path: String,
    pub db_options: soliton_panic::einstein_db::DBOptions,
}


impl PanicLightlikePersistenceDBOptions {
    pub fn new(db_path: String, db_options: soliton_panic::einstein_db::DBOptions) -> Self {
        Self { db_path, db_options }
    }
}


#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct PanicLightlikePersistenceDB {
    pub db_path: String,
    pub db_options: soliton_panic::einstein_db::DBOptions,
}