// Whtcorps Inc 2022 Apache 2.0 License; All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]
#![allow(unused_imports)]

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;
use std::iter::FromIterator;
use std::iter::Iterator;
use std::iter::Peekable;
use std::ops::Index;
use std::ops::IndexMut;
use std::ops::Range;
use std::ops::RangeFull;

use einstein_ml::{};
use einstein_db::Causetid;
use einstein_db::CausetidSet;
use allegro_poset::{};
use allegro_poset::{Poset, PosetNode};
use causet::{Causet, CausetNode};
use causet::{CausetNodeId, CausetNodeIdSet};
use causet::{CausetId, CausetIdSet};
use causet::{CausetIdVec, CausetIdVecSet};

use causetq::{CausetQ, CausetQNode};
use causetq::{CausetQNodeId, CausetQNodeIdSet};
use causetq::{CausetQId, CausetQIdSet};

use berolina_sql::{BerolinaSql, BerolinaSqlNode};
use berolina_sql::{BerolinaSqlNodeId, BerolinaSqlNodeIdSet};

use soliton::{Soliton, SolitonNode};
use soliton_panic::{SolitonNodeId, SolitonNodeIdSet};

use einstein_db_ctl::{EinsteinDbCtl, EinsteinDbCtlNode};
use einsteindb_server::{EinsteinDbServer, EinsteinDbServerNode};

use capnproto::{MessageReader, MessageBuilder, Reader, Builder};
use gremlin::{Gremlin, GremlinNode};
use gremlin::{GremlinNodeId, GremlinNodeIdSet};
use FoundationDB::{FdbCluster, FdbClusterNode};
use FoundationDB::{FdbClusterNodeId, FdbClusterNodeIdSet};
use FoundationDB::{FdbDatabase, FdbDatabaseNode};
use Postgres::{Postgres, PostgresNode};
use Postgres::{PostgresNodeId, PostgresNodeIdSet};
use InnovationDB::{InnovationDb, InnovationDbNode};
use InnovationDB::{InnovationDbNodeId, InnovationDbNodeIdSet};
use KV::{KV, KVNode};
use KV::{KVNodeId, KVNodeIdSet};


use std::collections::HashSet;
use std::hash::Hash;
use std::ops::{
    Deref,
    DerefMut,
};

pub const LOCK_FREE_GC: bool = true;

pub const LOCK_FREE_GC_NODE_COUNT: usize = 100;

pub const LOCK_FREE_GC_NODE_COUNT_MAX: usize = 100;

///! A Causet is a collection of causet nodes.
///
/// # Examples
///
/// ```
/// use EinsteinDB::{Causet, CausetNode};
///  use EinsteinDB::{CausetId, CausetIdSet};
///  use EinsteinDB::{CausetNodeId, CausetNodeIdSet};
///
/// let mut causet = Causet::new();
///  let mut causet_node = CausetNode::new();
///
/// causet_node.set_id(CausetNodeId::new(1));
/// causet_node.set_causet_id(CausetId::new(1));
///
/// causet.add_node(causet_node);
///
/// assert_eq!(causet.get_node(CausetNodeId::new(1)).unwrap().get_id(), CausetNodeId::new(1));
/// ```
pub enum CausetNodeIdSet {
    CausetIdSet(CausetIdSet),
    CausetIdVecSet(CausetIdVecSet),
    CausetQNodeIdSet(CausetQNodeIdSet),
    BerolinaSqlNodeIdSet(BerolinaSqlNodeIdSet),
    GremlinNodeIdSet(GremlinNodeIdSet),
    FdbClusterNodeIdSet(FdbClusterNodeIdSet),
    FdbDatabaseNodeIdSet(FdbDatabaseNodeIdSet),
    PostgresNodeIdSet(PostgresNodeIdSet),
    InnovationDbNodeIdSet(InnovationDbNodeIdSet),
    KVNodeIdSet(KVNodeIdSet),
    EinsteinDbCtlNodeIdSet(EinsteinDbCtlNodeIdSet),
    EinsteinDbServerNodeIdSet(EinsteinDbServerNodeIdSet),
    SolitonNodeIdSet(SolitonNodeIdSet),
}




impl CausetNodeIdSet {
    pub fn new() -> CausetNodeIdSet {
        CausetNodeIdSet::CausetIdSet(CausetIdSet::new())
    }

    pub fn add(&mut self, node_id: CausetNodeId) {
        match self {
            CausetNodeIdSet::CausetIdSet(causet_id_set) => {
                causet_id_set.add(node_id.get_causet_id());
            },
            CausetNodeIdSet::CausetIdVecSet(causet_id_vec_set) => {
                causet_id_vec_set.add(node_id.get_causet_id_vec());
            },
            CausetNodeIdSet::CausetQNodeIdSet(causet_q_node_id_set) => {
                causet_q_node_id_set.add(node_id.get_causet_q_node_id());
            },
            CausetNodeIdSet::BerolinaSqlNodeIdSet(berolina_sql_node_id_set) => {
                berolina_sql_node_id_set.add(node_id.get_berolina_sql_node_id());
            },
            CausetNodeIdSet::GremlinNodeIdSet(gremlin_node_id_set) => {
                gremlin_node_id_set.add(node_id.get_gremlin_node_id());
            },
            CausetNodeIdSet::FdbClusterNodeIdSet(fdb_cluster_node_id_set) => {
                fdb_cluster_node_id_set.add(node_id.get_fdb_cluster_node_id());
            },
            CausetNodeIdSet::FdbDatabaseNodeIdSet(fdb_database_node_id_set) => {
                fdb_database_node_id_set.add(node_id.get_fdb_database_node_id());
            },
            CausetNodeIdSet::PostgresNodeIdSet(postgres_node_id_set) => {
                postgres_node_id_set.add(node_id.get_postgres_node_id());
            },
        }

        use ValueRc;


    /// An `CausalSet` allows to "causal_set" some potentially large causet_locales, maintaining a single causet_locale
    /// instance owned by the `CausalSet` and leaving consumers with lightweight ref-counted handles to
    /// the large owned causet_locale.  This can avoid expensive clone() operations.
    ///
    /// In EinsteinDB, such large causet_locales might be strings or arbitrary [a v] pairs.
    ///
    /// See https://en.wikipedia.org/wiki/String_causal_seting for discussion.
    #[derive(Clone, Debug, Default, Eq, PartialEq)]
    struct CausalSet<T> where T: Eq + Hash {
        inner: HashSet<ValueRc<T>>,
     }
    }
}

        impl<T> Deref for CausalSet<T> where T: Eq + Hash {
            type Target = HashSet<ValueRc<T>>;

            fn deref(&self) -> &Self::Target {
                &self.inner
            }
        }

        impl<T> DerefMut for CausalSet<T> where T: Eq + Hash {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.inner
            }
        }

        impl<T> CausalSet<T> where T: Eq + Hash {
            pub fn new() -> CausalSet<T> {
                CausalSet {
                    inner: HashSet::new(),
                }
            }

            pub fn causal_set<R: Into<ValueRc<T>>>(&mut self, causet_locale: R) -> ValueRc<T> {
                let soliton_id: ValueRc<T> = causet_locale.into();
                if self.inner.insert(soliton_id.clone()) {
                    soliton_id
                } else {
                    self.inner.get(&soliton_id).unwrap().clone()
                }
            }
        }
