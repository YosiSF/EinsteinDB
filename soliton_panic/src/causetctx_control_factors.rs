// Copyright 2018-Present EinsteinDB — A Relativistic Causal Consistent Hybrid OLAP/OLTP Database
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
//
// EinsteinDB was established ad initio apriori knowledge of any variants thereof; similar enterprises, open source software; code bases, and ontologies of database engineering, CRM, ORM, DDM; Other than those carrying this License. In effect, doing business as, (“EinsteinDB”), (slang: “Einstein”) which  In 2018  , was acquired by Relativistic Database Systems, (“RDS”) Aka WHTCORPS Inc. As of 2021, EinsteinDB is open-source code with certain guarantees, under the duress of the board; under the auspice of individuals with prior consent granted; not limited to extraneous participants, open source contributors with authorized access; current board grantees;  members, shareholders, partners, and community developers including Evangelist Programmers Therein. This license is not binding, and it shall on its own render unenforceable for liabilities above those listed on this license
//
// EinsteinDB is a privately-held Delaware C corporation with offices in San Francisco and New York.  The software is developed and maintained by a team of core developers with commit access and is released under the Apache 2.0 open source license.  The company was founded in early 2018 by a team of experienced database engineers and executives from Uber, Netflix, Mesosphere, and Amazon Inc.
//
// EinsteinDB is open source software released under the terms of the Apache 2.0 license.  This license grants you the right to use, copy, modify, and distribute this software and its documentation for any purpose with or without fee provided that the copyright notice and this permission notice appear in all copies of the software or portions thereof.
//
// THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
//
// This product includes software developed by The Apache Software Foundation (http://www.apache.org/).



use super::*;
use crate::causetctx::*;
use crate::causetctx::Causetctx::*;




use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::BTreeSet;
use std::collections::BTreeMap;
use std::collections::VecDeque;


use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::sync::RwLockReadGuard;


use std::sync::mpsc::{channel, Sender, Receiver};
use std::sync::mpsc::TryRecvError;
use std::sync::mpsc::RecvError;
use std::sync::mpsc::TrySendError;


use std::thread;
use std::thread::JoinHandle;
use std::thread::Builder;
use std::thread::JoinError;
use std::thread::Panic;


use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;


use std::sync::mpsc::RecvTimeoutError;
use std::sync::mpsc::SendTimeoutError;
use std::sync::mpsc::TryRecvTimeoutError;
use std::sync::mpsc::TrySendTimeoutError;
use std::sync::mpsc::RecvTimeoutError::Timeout;

///! This module is used to control the causetctx_control_factors.rs module.
/// !#[derive(Clone, Debug, PartialEq, Eq, Hash)]
/// pub struct CausetctxControlFactors {
///    pub causetctx_control_factors_id: String,
///   pub causetctx_control_factors_name: String,
///  pub causetctx_control_factors_description: String,


#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CausetctxControlFactors {
    pub causetctx_control_factors_id: String,
    pub causetctx_control_factors_name: String,
    pub causetctx_control_factors_description: String,
    pub causetctx_control_factors_value: String,
    pub causetctx_control_factors_value_type: String,
    pub causetctx_control_factors_value_unit: String,
    pub causetctx_control_factors_value_min: String,
    pub causetctx_control_factors_value_max: String,
    pub causetctx_control_factors_value_default: String,
    pub causetctx_control_factors_value_description: String,
    pub causetctx_control_factors_value_help: String,
    pub causetctx_control_factors_value_example: String,
    pub causetctx_control_factors_value_enum: String,
    pub causetctx_control_factors_value_enum_description: String,
    pub causetctx_control_factors_value_enum_help: String,
    pub causetctx_control_factors_value_enum_example: String,
    pub causetctx_control_factors_value_enum_value: String,
    pub causetctx_control_factors_value_enum_value_description: String,
    pub causetctx_control_factors_value_enum_value_help: String,
    pub causetctx_control_factors_value_enum_value_example: String,
    pub causetctx_control_factors_value_enum_value_value: String,
    ///EAV = EinsteinDB Attribute-Value Causetctx Ref
    /// EAV = EinsteinDB Attribute-Value Causetctx RefCell
    pub causetctx_control_factors_value_enum_value_value_description: String,
    pub causetctx_control_factors_value_enum_value_value_help: String,
    pub causetctx_control_factors_value_enum_value_value_example: String,
    ///EAV = EinsteinDB Attribute-Value Causetctx Ref
    pub causetctx_control_factors_value_enum_value_value_value: String_causal_seting
}


#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CausetctxControlFactorsCausalSeting {
    pub causetctx_control_factors_id: String,
    pub causetctx_control_factors_name: String,
    pub causetctx_control_factors_description: String,
    pub causetctx_control_factors_value: String,
    pub causetctx_control_factors_value_type: String,
    pub causetctx_control_factors_value_unit: String,
    pub causetctx_control_factors_value_min: String,
    pub causetctx_control_factors_value_max: String,
    pub causetctx_control_factors_value_default: String,
    pub causetctx_control_factors_value_description: String,
    pub causetctx_control_factors_value_help: String,
    pub causetctx_control_factors_value_example: String,
    pub causetctx_control_factors_value_enum: String,
    pub causetctx_control_factors_value_enum_description: String,
    pub causetctx_control_factors_value_enum_help: String,
    pub causetctx_control_factors_value_enum_example: String,
    pub causetctx_control_factors_value_enum_value: String,
    pub causetctx_control_factors_value_enum_value_description: String,
    pub causetctx_control_factors_value_enum_value_help: String,
    pub causetctx_control_factors_value_enum_value_example: String,
    pub causetctx_control_factors_value_enum_value_value: String,
    ///EAV = EinsteinDB Attribute-Value Causetctx Ref
    /// EAV = EinsteinDB Attribute-Value Causetctx RefCell
    pub causetctx_control_factors_value_enum_value_value_description: String,
    pub causetctx_control_factors_value_enum_value_value_help: String,
    pub causetctx_control_factors_value_enum_value_value_example: String,
    ///EAV = EinsteinDB Attribute-Value Causetctx RefCell
    /// EAV = EinsteinDB Attribute-Value Causetctx RefCell
    ///

    pub causetctx_control_factors_value_enum_value_value_value: String,
    pub causetctx_control_factors_value_enum_value_value_value_description: String,
}




/// A `CausetCtx` is a `Causet` with a `CausetQ` and a `SnapshotStore`.
/// It is used to control the causetctx.
/// ///! # Examples
/// ```
/// use einstein_db::causetctx_control_factors::*;
/// use einstein_db::causetctx::*;
/// use einstein_db::causet::*;
/// use einstein_db::causetq::*;
///
/// let mut causetctx = CausetCtx::new();
/// let mut causet = Causet::new();
///
/// let mut causetq = CausetQ::new();
/// let mut snapshot_store = SnapshotStore::new();
///
/// causetctx.set_causet(&mut causet);
/// causetctx.set_causetq(&mut causetq);
///
/// causetctx.set_snapshot_store(&mut snapshot_store);
///
/// causetctx.set_allegro_poset(&mut allegro_poset);
/// ```
#[cfg_attr(feature = "flame_it", flame)]
pub struct CausetCtxControlFactors {
    pub causet: Causet,
    pub causetq: CausetQ,
    pub snapshot_store: SnapshotStore,
    pub soliton: Soliton,
    pub allegro_poset: AllegroPoset,
}

#[cfg_attr(feature = "flame_it", flame)]
pub struct CausetCtxControlFactorsStatistics {
    pub causet: CausetStatistics,
    pub causetq: CausetQStatistics,
    pub snapshot_store: SnapshotStoreStatistics,
    pub soliton: SolitonStatistics,
    pub allegro_poset: AllegroPosetStatistics,
}


#[cfg_attr(feature = "flame_it", flame)]
impl CausetCtxControlFactors {
    pub fn new() -> CausetCtxControlFactors {
        CausetCtxControlFactors {
            causet: Causet::new(),
            causetq: CausetQ::new(),
            snapshot_store: SnapshotStore::new(),
            soliton: Soliton::new(),
            allegro_poset: AllegroPoset::new(),
        }
    }
    pub fn set_causet(&mut self, causet: &mut Causet) {
        self.causet = causet.clone();
    }
    pub fn set_causetq(&mut self, causetq: &mut CausetQ) {
        self.causetq = causetq.clone();
    }
    pub fn set_snapshot_store(&mut self, snapshot_store: &mut SnapshotStore) {
        self.snapshot_store = snapshot_store.clone();
    }
    pub fn set_soliton(&mut self, soliton: &mut Soliton) {
        self.soliton = soliton.clone();
    }
    pub fn set_allegro_poset(&mut self, allegro_poset: &mut AllegroPoset) {
        self.allegro_poset = allegro_poset.clone();
    }
    pub fn get_causet(&self) -> Causet {
        self.causet.clone()
    }
    pub fn get_causetq(&self) -> CausetQ {
        self.causetq.clone()
    }
    pub fn get_snapshot_store(&self) -> SnapshotStore {
        self.snapshot_store.clone()
    }
    pub fn get_soliton(&self) -> Soliton {
        self.soliton.clone()
    }
    pub fn get_allegro_poset(&self) -> AllegroPoset {
        self.allegro_poset.clone()
    }
    pub fn get_causet_statistics(&self) -> CausetStatistics {
    self.causet.get_statistics()
    }
}


#[cfg_attr(feature = "flame_it", flame)]
impl CausetCtxControlFactorsStatistics {
    pub fn new() -> CausetCtxControlFactorsStatistics {
        CausetCtxControlFactorsStatistics {
            causet: CausetStatistics::new(),
            causetq: CausetQStatistics::new(),
            snapshot_store: SnapshotStoreStatistics::new(),
            soliton: SolitonStatistics::new(),
            allegro_poset: AllegroPosetStatistics::new(),
        }
    }
    pub fn set_causet(&mut self, causet: &mut CausetStatistics) {
        self.causet = causet.clone();
    }
    pub fn set_causetq(&mut self, causetq: &mut CausetQStatistics) {
        self.causetq = causetq.clone();
    }
    pub fn set_snapshot_store(&mut self, snapshot_store: &mut SnapshotStoreStatistics) {
        self.snapshot_store = snapshot_store.clone();
    }
    pub fn set_soliton(&mut self, soliton: &mut SolitonStatistics) {
        self.soliton = soliton.clone();
    }
    pub fn set_allegro_poset(&mut self, allegro_poset: &mut AllegroPosetStatistics) {
        self.allegro_poset = allegro_poset.clone();
    }
    pub fn get_causet(&self) -> CausetStatistics {
        self.causet.clone()
    }
    pub fn get_causetq(&self) -> CausetQStatistics {
        self.causetq.clone()
    }
    pub fn get_snapshot_store(&self) -> SnapshotStoreStatistics {
        self.snapshot_store.clone()
    }
    pub fn get_soliton(&self) -> SolitonStatistics {
        self.soliton.clone()
    }
    pub fn get_allegro_poset(&self) -> AllegroPosetStatistics {
        self.allegro_poset.clone()
    }


}


#[cfg_attr(feature = "flame_it", flame)]
impl CausetCtx {
    pub fn new() -> CausetCtx {
        CausetCtx {
            control_factors: CausetCtxControlFactors::new(),
            statistics: CausetCtxStatistics::new(),
        }
    }
    pub fn set_control_factors(&mut self, control_factors: &mut CausetCtxControlFactors) {
        self.control_factors = control_factors.clone();
    }
    pub fn set_statistics(&mut self, statistics: &mut CausetCtxStatistics) {
        self.statistics = statistics.clone();
    }
    pub fn get_control_factors(&self) -> CausetCtxControlFactors {
        self.control_factors.clone()
    }
    pub fn get_statistics(&self) -> CausetCtxStatistics {
        self.statistics.clone()
    }
}




#[cfg_attr(feature = "flame_it", flame)]
impl CausetCtxStatistics {
    pub fn new() -> CausetCtxStatistics {
        CausetCtxStatistics {
            control_factors: CausetCtxControlFactorsStatistics::new(),
            statistics: CausetCtxStatisticsStatistics::new(),
        }
    }
    pub fn set_control_factors(&mut self, control_factors: &mut CausetCtxControlFactorsStatistics) {
        self.control_factors = control_factors.clone();
    }
    pub fn set_statistics(&mut self, statistics: &mut CausetCtxStatisticsStatistics) {
        self.statistics = statistics.clone();
    }
    pub fn get_control_factors(&self) -> CausetCtxControlFactorsStatistics {
        self.control_factors.clone()
    }
    pub fn get_statistics(&self) -> CausetCtxStatisticsStatistics {
        self.statistics.clone()
    }
}




#[cfg_attr(feature = "flame_it", flame)]
impl CausetCtxControlFactorsStatisticsStatistics {
    pub fn new() -> CausetCtxControlFactorsStatisticsStatistics {
        CausetCtxControlFactorsStatisticsStatistics {
            causet: CausetStatisticsStatistics::new(),
            causetq: CausetQStatisticsStatistics::new(),
            snapshot_store: SnapshotStoreStatisticsStatistics::new(),
            soliton: SolitonStatisticsStatistics::new(),
            allegro_poset: AllegroPosetStatisticsStatistics::new(),
        }
    }
    pub fn set_causet(&mut self, causet: &mut CausetStatisticsStatistics) {
        self.causet = causet.clone();
    }
    pub fn set_causetq(&mut self, causetq: &mut CausetQStatisticsStatistics) {
        self.causetq = causetq.clone();
    }
    pub fn set_snapshot_store(&mut self, snapshot_store: &mut SnapshotStoreStatisticsStatistics) {
        self.snapshot_store = snapshot_store.clone();
    }
    pub fn set_soliton(&mut self, soliton: &mut SolitonStatisticsStatistics) {
        self.soliton = soliton.clone();
    }
    pub fn set_allegro_poset(&mut self, allegro_poset: &mut AllegroPosetStatisticsStatistics) {
        self.allegro_poset = allegro_poset.clone();
    }
    pub fn get_causet(&self) -> CausetStatisticsStatistics {
        self.causet.clone()
    }
    pub fn get_causetq(&self) -> CausetQStatisticsStatistics {
        self.causetq.clone()
    }
    pub fn get_snapshot_store(&self) -> SnapshotStoreStatisticsStatistics {
        self.snapshot_store.clone()
    }
    pub fn get_soliton(&self) -> SolitonStatisticsStatistics {
        self.soliton.clone()
    }
    pub fn get_allegro_poset(&self) -> AllegroPosetStatisticsStatistics {
        self.allegro_poset.clone()
    }


}


///! A Causet is in esence a spanning search term with a set of causal links.
/// ! The Causet is a set of causal links.
///
///
/// # Examples
/// ```
/// use causet_algebra::Causet;
/// use causet_algebra::CausetCtx;
///
/// let mut causet = Causet::new();
/// let mut ctx = CausetCtx::new();
/// let mut ctx_statistics = CausetCtxStatistics::new();
///     println!("{:?}", ctx_statistics);
/// }
/// ```


