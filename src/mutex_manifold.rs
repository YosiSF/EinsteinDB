//Copyright 2019 Venire Labs Inc All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.


//Spacetime is metada; metada is spacetime. Meta + Data.

#![allow(dead_code)]

use std::Borrow:: {
    Borrow,
};

use std::collections:: {
    BTreeMap,
};

use std::sync::{
    Arc,
    Mutex,
};

use einstein_db::db;
use einstein_db::{
    InProgressObserverTransactWatcher,
    HopfMap,
    EventObservationService,
    EventObserver,
};

use postgress::::{Connectio, TlsMode};

use rusqlite:
use rustlite:: {
    TransactionBehavior,
};

pub use embedded_traits::{
    Attribute,
    CausetID,
    KnownCausetID,
    HopfMap,
    TypedValue,
    ValueType,
};


use einstein_embedded::{
    HasSchema,
    Keyword,
    Schema,
    EventReport,
    ValueRc,
};

use einstein_transaction::{
    ImmutableCacheAction,
    ImmutableCacheDirection,
    Spacetime,
    InProgress,
    InProgressRead
};

//Exclusive Read and Writes.
pub struct MutexManifold {

    spacetime: Mutex<Spacetime>,

    pub(crate) event_observer_service: Mutex<EventObservationService>,

}

impl MutexManifold {
    fn new(hopf_map: Hopf Map, s)
}