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
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_assignments)]
#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate serde_json;
extern crate serde_yaml;
extern crate serde_cbor;
extern crate serde_xml_rs;
extern crate serde_urlencoded;
//extern crate serde_bincode;
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

use EinsteinDB::*;
use causet::{ CausalSet, CausalSetMut, CausalSetMutExt, CausalSetExt };
use causet::{ CausalSetMutExt, CausalSetExt };
use causetq::{ CausalSetQ, CausalSetQMut, CausalSetQMutExt, CausalSetQExt };
use causetq::{ CausalSetQMutExt, CausalSetQExt };
use beroinasql::{ Beroinasql, BeroinasqlMut, BeroinasqlMutExt, BeroinasqlExt };
use beroinasql::{ BeroinasqlMutExt, BeroinasqlExt };
use causets::{ CausalSetS, CausalSetSMut, CausalSetSMutExt, CausalSetSExt };
use einstein_db_alexandrov_processing::{ EDSL, EDSLMut, EDSLMutExt, EDSLExt };
use einstein_ml::*;
use allegro_poset::{ Poset, PosetMut, PosetMutExt, PosetExt };
use super::super::encoder::{Column, RowEncoder};
use super::super::{
    ColumnType,
    ColumnValue,
    ColumnValueType,
    ColumnValueType::{
        Bool,
        Bytes,
        Float,
        Int,
        String,
        Uint,
    },
};

///! # Causal Set (CS)
///  A causal set is a set of ordered pairs of keys and values.
/// The keys are ordered by the causal order of the values.
/// The values are ordered by the causal order of the keys.
/// The causal order of a key is the order in which it was inserted into the causal set.
/// The causal order of a value is the order in which it was inserted into the causal set.
/// The causal order of a key-value pair is the order in which it was inserted into the causal set.
///

/*
SELECT srcport, dstport, dstip, COUNT(*)
FROM practice_dataset_2013_practice_1 WHERE srcip = '10.220.0.36'
GROUP BY srcport, dstport, dstip ORDER BY COUNT(*) ESC LIMIT 5;
# Calculate the average duration of sessions in minutes.
%%sql
SELECT AVG(unix_timestamp(lasttime)-unix_timestamp(firsttime))/60 as avgDuration FROM practice_dataset_2013 WHERE srcip = '10.220.0.36';
# Find the most frequently visited website by IP address: 10.220.0.36




  # Order By count descending and then limit to top 5 results, then join dataframes on dstip (top5DF) and dstport (websitesDF). Then Join these two dataframes into one final dataframe called visitDataFrame where each row contains a unique combination of IP Address, Port & Website URL with values for Count in descending order based on ipAddressCounts table

  /* Describe existing table*/
  %%sql select * from practiceDataset20131 limit 1
          /*** Overview of first row ***/
                   SELECT DSTIP AS "Destination IP",
                   DSTPORT AS "Port Used" , SUM("BYTES") AS "Number Of Bytes Transferred"

                   FROM PRACTICE_DATASET2013 WHERE SRCIP LIKE '%61%75%2E%32%30%2E%30%37' GROUP BY SRCIP,
                   DSTPORT            UNION SELECT SRCIP AS "Source IP",
                   PRTCLNAME AS "Protocol Name" ,
       SUM("BYTES")  AS "Number Of Bytes Transferred" FROM PRACTICE_DATASET2013  WHERE FIRSTTIME LIKE "%06%" AND TIMESTAMP
 BETWEEN                     TO_TIMESTAMP('2018-01-20 06:00:00',                     				                'YYYY-MM-DD HH24:MI:SS')                   AND TO_TIMESTAMP('2018-01-20 19:59:59',                       		             'YYYY-MM-DD HH24:MI:SS')                  GROUP BY SRCIP, PRTCLNAME ORDER BY NumberOfBytesTransferred DESC NULLS LAST LIMIT 20 ;         /**Findings **/           SELECT V.*, W.*                                                    FROM VisitDataFrame V LEFT OUTER JOIN Websites W ON V."Website URL"=W."Website URL";     %%sql select srcip as SourceIp, sum(bytes) as TotalBytesSent                  from PracticeDataset2013 where dstsubnet = 61 > 30 group by srcip order by 2 desc nulls last limit 10               /*** Top Ten Sources Based On Destination Subnets Greater Than 255 ***/    %% sql select count(*), dstport from PracticeDataset2013 where dstsubnet = 61 > 30 and dstsubnet != 255 group by dstport order by 1 desc nulls*/






use crate::{
    data_structures::{
        CausalSet,
        CausalSetEntry,
        CausalSetKey,
        CausalSetValue,
    },
    error::Error,
    utils::{
        CausalOrder,
        CausalOrderError,
        CausalOrderResult,
    },
};




use std::{
    collections::{
        BTreeMap,
        BTreeSet,
    },
    fmt::{
        Display,
        Formatter,
    },
    hash::{
        Hash,
        Hasher,
    },
    iter::{
        FromIterator,
        IntoIterator,
    },
    ops::{
        Add,
        AddAssign,
        Deref,
        DerefMut,
        Sub,
        SubAssign,
    },
    slice::{
        Iter,
        IterMut,
    },
    str::FromStr,
    sync::{
        atomic::{
            AtomicUsize,
            Ordering,
        },
        Arc,
    },
    usize,
};




#[derive(Clone, Debug, Default, PartialEq)]
pub struct CausalSetS<T> {
    pub data: Vec<T>,
    pub poset: Poset,
}




impl<T> CausalSetS<T> {
    pub fn new() -> Self {
        Self {
            data: vec![],
            poset: Poset::new(),
        }
    }
}



#[derive(Clone, Debug, Default, PartialEq)]
pub struct CausalSetSMut<T> {
    pub data: Vec<T>,
    pub poset: PosetMut,
}


impl<T> CausalSetSMut<T> {
    pub fn new() -> Self {
        Self {
            data: vec![],
            poset: PosetMut::new(),
        }
    }
}




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
            CausetNodeIdSet::InnovationDbNodeIdSet(innovation_db_node_id_set) => {
                innovation_db_node_id_set.add(node_id.get_innovation_db_node_id());
            },
            CausetNodeIdSet::KVNodeIdSet(kv_node_id_set) => {
                kv_node_id_set.add(node_id.get_kv_node_id());
            },
        }
    }
}



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

    impl<T> CausalSet<T> where T: Eq + Hash {
        pub fn new() -> CausalSet<T> {
            CausalSet {
                inner: HashSet::new(),
            }
        }

        pub fn add(&mut self, value: T) {

            self.inner.insert(ValueRc::new(value));
        }

        pub fn contains(&self, value: &T) -> bool {
            self.inner.contains(value)
        }

        pub fn len(&self) -> usize {
            self.inner.len()
        }

        pub fn is_empty(&self) -> bool {
            self.inner.is_empty()
        }

        pub fn iter(&self) -> Iter<T> {
            self.inner.iter().map(|v| v.as_ref())
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



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_causet_node_id_set() {
        let mut causet_node_id_set = CausetNodeIdSet::new();
        let causet_id = CausetId::new(1);
        let causet_id_vec = CausetIdVec::new(vec![1, 2, 3]);
        let causet_q_node_id = CausetQNodeId::new(1);
        let berolina_sql_node_id = BerolinaSqlNodeId::new(1);
        let gremlin_node_id = GremlinNodeId::new(1);
        let fdb_cluster_node_id = FdbClusterNodeId::new(1);
        let fdb_database_node_id = FdbDatabaseNodeId::new(1);
        let postgres_node_id = PostgresNodeId::new(1);

        causet_node_id_set.add(causet_id);
        causet_node_id_set.add(causet_id_vec);
        causet_node_id_set.add(causet_q_node_id);
        causet_node_id_set.add(berolina_sql_node_id);
        causet_node_id_set.add(gremlin_node_id);
        causet_node_id_set.add(fdb_cluster_node_id);
        causet_node_id_set.add(fdb_database_node_id);
        causet_node_id_set.add(postgres_node_id);

        assert_eq!(causet_node_id_set.len(), 7);
        assert_eq!(causet_node_id_set.get_causet_id_set().len(), 1);
        assert_eq!(causet_node_id_set.get_causet_id_vec_set().len(), 1);
        assert_eq!(causet_node_id_set.get_causet_q_node_id_set().len(), 1);

        assert_eq!(causet_node_id_set.get_berolina_sql_node_id_set().len(), 1);
        assert_eq!(causet_node_id_set.get_gremlin_node_id_set().len(), 1);
    }
}




