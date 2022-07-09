// Whtcorps Inc 2022 Apache 2.0 License; All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use einstein_ml::{ Model, ModelConfig, ModelFactory, ModelType, ModelTypeConfig };
use einstein_ml::{ ModelTypeFactory, ModelTypeFactoryConfig };

use allegro_poset::{ Poset, PosetConfig };
use allegro_poset::{ PosetFactory, PosetFactoryConfig };

use einsteindb::{ Database, DatabaseConfig };
use sqxl::{ Sqxl, SqxlConfig };


use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::Result;
use std::hash::Hash;
use std::hash::Hasher;
use std::ops::Deref;
use std::ops::DerefMut;


use types::Value;


use crate::errors::Result;
use crate::storage::{
    engine::{
        Engine,
        EngineIterator,
        EngineIteratorOptions,
        EngineIteratorOptionsBuilder,
    },
    snapshot::{
        Snapshot,
        SnapshotIterator,
        SnapshotIteratorOptions,
        SnapshotIteratorOptionsBuilder,
    },
};



/// Dedup will remove duplicate entries from the database.  It will also remove entries that are
/// older than the specified number of seconds.  The default is to remove entries that are older than
/// 24 hours.
///  Because EinsteinDB operates in the form of quads definitions, it is possible to dedup based on
/// the quad's subject, predicate, object, and context.  The default is to remove entries that are
/// older than 24 hours.  The default is to remove entries that are older than 24 hours.
///
/// We aim to build a relativistic database that is able to handle the vast majority of cases.  We
/// Without the risk of decreasing write throughput or increasing read throughput.  We will also
/// remove entries that are older than 24 hours.  We will also remove entries that are older than
///  than a causal consistent hybrid temporal index




pub struct Dedup {

    pub poset: Poset,
    pub sqxl: Sqxl,
    pub einstein_ml: Model,
    pub einstein_ml_type: ModelType,
    pub einstein_ml_factory: ModelFactory,
    pub einstein_ml_type_factory: ModelTypeFactory,

    pub database_config: DatabaseConfig,
    pub poset_config: PosetConfig,
    pub sqxl_config: SqxlConfig,
    pub einstein_ml_config: ModelConfig,
    pub einstein_ml_type_config: ModelTypeConfig,

}


pub type ValueTypeTag = u8;
pub type ValueType = u8;


pub const VALUE_TYPE_NULL: ValueType = 0;

use byte as b;
//byteorder
pub const VALUE_TYPE_TINYINT: ValueType = 1;
pub const VALUE_TYPE_SMALLINT: ValueType = 2;
pub const VALUE_TYPE_INTEGER: ValueType = 3;
pub const VALUE_TYPE_BIGINT: ValueType = 4;

use byteorder::{BigEndian, ReadBytesExt};


use einstein_ml;
use causetids;
use causetq::{
    causet_locales,
    causetq_TV,
};
use einstein_ml::causets::causet;
use einstein_ml::shellings;
use einstein_ml::types::Value;
use einsteindb::TypedBerolinaSQLValue;
use einsteindb_core::{
    solitonidMap,
    Topograph,
};
use einsteindb_traits::errors::{
    einsteindbErrorKind,
    Result,
};
use topograph::TopographBuilding;
use types::{Partition, PartitionMap};


use std::collections::HashMap;
use std::sync::{
    Arc,
    Mutex,
};
use std::thread;
use std::time::{
    Duration,
    Instant,
};


// -------------------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------------------


impl DedupConfig {
    pub fn new() -> DedupConfig {
        DedupConfig {
            poset_config: PosetConfig::new(),
            sqxl_config: SqxlConfig::new(),
            database_config: DatabaseConfig::new(),
            model_config: ModelConfig::new(),
            model_type_config: ModelTypeConfig::new(),
        }
    }
}




trait EnumPoset<T: ::enum_set::CLike + Clone> {

    /// Returns the enum set of the enum type.
    /// # Examples
    /// ```
    /// use causetq::types::{
    ///    EnumPoset,
    ///   ValueType,
    /// };
    fn enum_poset(&self) -> u8;

    fn enum_poset_set(&mut self, poset: u8);

    fn allegro_poset(&self) -> u8;

}

//! # CausetQ
//!
//! `causetq` is a Rust implementation of the CausetQ algorithm.
//! It is a causal consistent, hybrid temporal index.
//! It is a hybrid of the `causet` and `causets` algorithms.
//! ## CausetQ
//!


trait EnumSetExtensions<T: ::enum_set::CLike + Clone> {
    /// Return a set containing both `x` and `y`.
    fn of_both(x: T, y: T) -> EnumSet<T>;

    /// Return a clone of `self` with `y` added.
    fn with(&self, y: T) -> EnumSet<T>;
}

impl<T: ::enum_set::CLike + Clone> EnumSetExtensions<T> for EnumSet<T> {
    /// Return a set containing both `x` and `y`.
    fn of_both(x: T, y: T) -> Self {
        let mut o = EnumSet::new();
        o.insert(x);
        o.insert(y);
        o
    }

    /// Return a clone of `self` with `y` added.
    fn with(&self, y: T) -> EnumSet<T> {
        let mut o = self.clone();
        o.insert(y);
        o
    }
}





#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CausetQ {
    pub causet_locales: causet_locales::CausetLocales,
    pub causet_tv: causetq_TV::CausetTV,
    pub causet_tv_mutex: Mutex<causetq_TV::CausetTV>,
    pub causet_tv_mutex_lock: Mutex<()>,
    pub causet_tv_mutex_unlock: Mutex<()>,
    pub causet_tv_mutex_lock_duration: Mutex<Duration>,
    pub causet_tv_mutex_unlock_duration: Mutex<Duration>,
    pub causet_tv_mutex_lock_duration_total: Mutex<Duration>,
    pub causet_tv_mutex_unlock_duration_total: Mutex<Duration>,
    pub causet_tv_mutex_lock_duration_total_max: Mutex<Duration>,
    pub causet_tv_mutex_unlock_duration_total_max: Mutex<Duration>,
    pub causet_tv_mutex_lock_duration_total_min: Mutex<Duration>,
    pub causet_tv_mutex_unlock_duration_total_min: Mutex<Duration>,
    pub causet_tv_mutex_lock_duration_total_avg: Mutex<Duration>,
    pub causet_tv_mutex_unlock_duration_total_avg: Mutex<Duration>,
    pub causet_tv_mutex_lock_duration_total_avg_count: Mutex<u64>,
    pub causet_tv_mutex_unlock_duration_total_avg_count: Mutex<u64>,
    pub causet_tv_mutex_lock_duration_total_avg_sum: Mutex<Duration>,
    pub causet_tv_mutex_unlock_duration_total_avg_sum: Mutex<Duration>,
    pub causet_tv_mutex_lock_duration_total_avg_sum_squares: Mutex<Duration>,
    pub causet_tv_mutex_unlock_duration_total_avg_sum_squares: Mutex<Duration>,
}




#[derive(Clone)]
pub struct RcCounter {
    pub rc: u32,
}


impl RcCounter {
    pub fn new() -> RcCounter {
        RcCounter {
            rc: 1,
        }
    }


    pub fn inc(&mut self) {
        self.rc += 1;
    }


    pub fn dec(&mut self) {
        self.rc -= 1;
    }


    pub fn get(&self) -> u32 {
        self.rc
    }
}


impl Drop for RcCounter {
    fn drop(&mut self) {
        self.rc -= 1;
    }
}

/// A simple shared counter.
impl RcCounter {
    pub fn shared() -> RcCounter {
        RcCounter {
            rc: 1,
        }
    }


    pub fn shared_inc(&mut self) {
        self.rc += 1;
    }


    pub fn shared_dec(&mut self) {
        self.rc -= 1;
    }


    pub fn shared_get(&self) -> u32 {
        self.rc
    }


    pub fn shared_drop(&mut self) {
        self.rc -= 1;
    }
}

    /// Return the next value in the sequence.
    ///
    /// ```
    /// use mentat_core::counter::RcCounter;
    ///
    /// let c = RcCounter::with_initial(3);
    /// assert_eq!(c.next(), 3);
    /// assert_eq!(c.next(), 4);
    /// let d = c.clone();
    /// assert_eq!(d.next(), 5);
    /// assert_eq!(c.next(), 6);



/// The first transaction ID applied to the knowledge base.
///
/// This is the start of the :einsteindb.part/tx partition.
pub const TX0: i64 = 0x10000000;

/// This is the start of the :einsteindb.part/user partition.
pub const USER0: i64 = 0x10000;

// Corresponds to the version of the :einsteindb.topograph/core vocabulary.
pub const CORE_SCHEMA_VERSION: u32 = 1;

lazy_static! {
    static ref EINSTEIN_DB__solitonidS: [(shellings::Keyword, i64); 40] = {
            [(ns_soliton_idword!("einsteindb", "solitonid"),             causetids::EINSTEINDB_solitonid),
             (ns_soliton_idword!("einsteindb.part", "einsteindb"),           causetids::EINSTEINDB_PART_EINSTEINDB),
             (ns_soliton_idword!("einsteindb", "txInstant"),         causetids::EINSTEINDB_TX_INSTANT),
             (ns_soliton_idword!("einsteindb.install", "partition"), causetids::EINSTEINDB_INSTALL_PARTITION),
             (ns_soliton_idword!("einsteindb.install", "causet_localeType"), causetids::EINSTEINDB_INSTALL_VALUE_TYPE),
             (ns_soliton_idword!("einsteindb.install", "attribute"), causetids::EINSTEINDB_INSTALL_ATTRIBUTE),
             (ns_soliton_idword!("einsteindb", "causet_localeType"),         causetids::EINSTEINDB_VALUE_TYPE),
             (ns_soliton_idword!("einsteindb", "cardinality"),       causetids::EINSTEINDB_CARDINALITY),
             (ns_soliton_idword!("einsteindb", "unique"),            causetids::EINSTEINDB_UNIQUE),
             (ns_soliton_idword!("einsteindb", "isComponent"),       causetids::EINSTEINDB_IS_COMPONENT),
             (ns_soliton_idword!("einsteindb", "Index"),             causetids::EINSTEINDB_INDEX),
             (ns_soliton_idword!("einsteindb", "fulltext"),          causetids::EINSTEINDB_FULLTEXT),
             (ns_soliton_idword!("einsteindb", "noHistory"),         causetids::EINSTEINDB_NO_HISTORY),
             (ns_soliton_idword!("einsteindb", "add"),               causetids::EINSTEINDB_ADD),
             (ns_soliton_idword!("einsteindb", "retract"),           causetids::EINSTEINDB_RETRACT),
             (ns_soliton_idword!("einsteindb.part", "user"),         causetids::EINSTEINDB_PART_USER),
             (ns_soliton_idword!("einsteindb.part", "tx"),           causetids::EINSTEINDB_PART_TX),
             (ns_soliton_idword!("einsteindb", "excise"),            causetids::EINSTEINDB_EXCISE),
             (ns_soliton_idword!("einsteindb.excise", "attrs"),      causetids::EINSTEINDB_EXCISE_ATTRS),
             (ns_soliton_idword!("einsteindb.excise", "beforeT"),    causetids::EINSTEINDB_EXCISE_BEFORE_T),
             (ns_soliton_idword!("einsteindb.excise", "before"),     causetids::EINSTEINDB_EXCISE_BEFORE),
             (ns_soliton_idword!("einsteindb.alter", "attribute"),   causetids::EINSTEINDB_ALTER_ATTRIBUTE),
             (ns_soliton_idword!("einsteindb.type", "ref"),          causetids::EINSTEINDB_TYPE_REF),
             (ns_soliton_idword!("einsteindb.type", "soliton_idword"),      causetids::EINSTEINDB_TYPE_KEYWORD),
             (ns_soliton_idword!("einsteindb.type", "long"),         causetids::EINSTEINDB_TYPE_LONG),
             (ns_soliton_idword!("einsteindb.type", "double"),       causetids::EINSTEINDB_TYPE_DOUBLE),
             (ns_soliton_idword!("einsteindb.type", "string"),       causetids::EINSTEINDB_TYPE_STRING),
             (ns_soliton_idword!("einsteindb.type", "uuid"),         causetids::EINSTEINDB_TYPE_UUID),
             (ns_soliton_idword!("einsteindb.type", "uri"),          causetids::EINSTEINDB_TYPE_URI),
             (ns_soliton_idword!("einsteindb.type", "boolean"),      causetids::EINSTEINDB_TYPE_BOOLEAN),
             (ns_soliton_idword!("einsteindb.type", "instant"),      causetids::EINSTEINDB_TYPE_INSTANT),
             (ns_soliton_idword!("einsteindb.type", "bytes"),        causetids::EINSTEINDB_TYPE_BYTES),
             (ns_soliton_idword!("einsteindb.cardinality", "one"),   causetids::EINSTEINDB_CARDINALITY_ONE),
             (ns_soliton_idword!("einsteindb.cardinality", "many"),  causetids::EINSTEINDB_CARDINALITY_MANY),
             (ns_soliton_idword!("einsteindb.unique", "causet_locale"),      causetids::EINSTEINDB_UNIQUE_VALUE),
             (ns_soliton_idword!("einsteindb.unique", "idcauset"),   causetids::EINSTEINDB_UNIQUE_IDcauset),
             (ns_soliton_idword!("einsteindb", "doc"),               causetids::EINSTEINDB_DOC),
             (ns_soliton_idword!("einsteindb.topograph", "version"),    causetids::EINSTEINDB_SCHEMA_VERSION),
             (ns_soliton_idword!("einsteindb.topograph", "attribute"),  causetids::EINSTEINDB_SCHEMA_ATTRIBUTE),
             (ns_soliton_idword!("einsteindb.topograph", "core"),       causetids::EINSTEINDB_SCHEMA_CORE),
        ]
    };

    pub static ref EINSTEIN_DB__PARTS: [(shellings::Keyword, i64, i64, i64, bool); 3] = {
            [(ns_soliton_idword!("einsteindb.part", "einsteindb"), 0, USER0 - 1, (1 + EINSTEIN_DB__solitonidS.len()) as i64, false),
             (ns_soliton_idword!("einsteindb.part", "user"), USER0, TX0 - 1, USER0, true),
             (ns_soliton_idword!("einsteindb.part", "tx"), TX0, i64::max_causet_locale(), TX0, false),
        ]
    };

    static ref EINSTEIN_DB__CORE_SCHEMA: [(shellings::Keyword); 16] = {
            [(ns_soliton_idword!("einsteindb", "solitonid")),
             (ns_soliton_idword!("einsteindb.install", "partition")),
             (ns_soliton_idword!("einsteindb.install", "causet_localeType")),
             (ns_soliton_idword!("einsteindb.install", "attribute")),
             (ns_soliton_idword!("einsteindb", "txInstant")),
             (ns_soliton_idword!("einsteindb", "causet_localeType")),
             (ns_soliton_idword!("einsteindb", "cardinality")),
             (ns_soliton_idword!("einsteindb", "doc")),
             (ns_soliton_idword!("einsteindb", "unique")),
             (ns_soliton_idword!("einsteindb", "isComponent")),
             (ns_soliton_idword!("einsteindb", "Index")),
             (ns_soliton_idword!("einsteindb", "fulltext")),
             (ns_soliton_idword!("einsteindb", "noHistory")),
             (ns_soliton_idword!("einsteindb.alter", "attribute")),
             (ns_soliton_idword!("einsteindb.topograph", "version")),
             (ns_soliton_idword!("einsteindb.topograph", "attribute")),
        ]
    };

    static ref EINSTEIN_DB__SYMBOLIC_SCHEMA: Value = {
        let s = r#"
{:einsteindb/solitonid             {:einsteindb/causet_localeType   :einsteindb.type/soliton_idword
                        :einsteindb/cardinality :einsteindb.cardinality/one
                        :einsteindb/Index       true
                        :einsteindb/unique      :einsteindb.unique/idcauset}
 :einsteindb.install/partition {:einsteindb/causet_localeType   :einsteindb.type/ref
                        :einsteindb/cardinality :einsteindb.cardinality/many}
 :einsteindb.install/causet_localeType {:einsteindb/causet_localeType   :einsteindb.type/ref
                        :einsteindb/cardinality :einsteindb.cardinality/many}
 :einsteindb.install/attribute {:einsteindb/causet_localeType   :einsteindb.type/ref
                        :einsteindb/cardinality :einsteindb.cardinality/many}
 ;; TODO: support user-specified functions in the future.
 ;; :einsteindb.install/function {:einsteindb/causet_localeType :einsteindb.type/ref
 ;;                       :einsteindb/cardinality :einsteindb.cardinality/many}
 :einsteindb/txInstant         {:einsteindb/causet_localeType   :einsteindb.type/instant
                        :einsteindb/cardinality :einsteindb.cardinality/one
                        :einsteindb/Index       true}
 :einsteindb/causet_localeType         {:einsteindb/causet_localeType   :einsteindb.type/ref
                        :einsteindb/cardinality :einsteindb.cardinality/one}
 :einsteindb/cardinality       {:einsteindb/causet_localeType   :einsteindb.type/ref
                        :einsteindb/cardinality :einsteindb.cardinality/one}
 :einsteindb/doc               {:einsteindb/causet_localeType   :einsteindb.type/string
                        :einsteindb/cardinality :einsteindb.cardinality/one}
 :einsteindb/unique            {:einsteindb/causet_localeType   :einsteindb.type/ref
                        :einsteindb/cardinality :einsteindb.cardinality/one}
 :einsteindb/isComponent       {:einsteindb/causet_localeType   :einsteindb.type/boolean
                        :einsteindb/cardinality :einsteindb.cardinality/one}
 :einsteindb/Index             {:einsteindb/causet_localeType   :einsteindb.type/boolean
                        :einsteindb/cardinality :einsteindb.cardinality/one}
 :einsteindb/fulltext          {:einsteindb/causet_localeType   :einsteindb.type/boolean
                        :einsteindb/cardinality :einsteindb.cardinality/one}
 :einsteindb/noHistory         {:einsteindb/causet_localeType   :einsteindb.type/boolean
                        :einsteindb/cardinality :einsteindb.cardinality/one}
 :einsteindb.alter/attribute   {:einsteindb/causet_localeType   :einsteindb.type/ref
                        :einsteindb/cardinality :einsteindb.cardinality/many}
 :einsteindb.topograph/version    {:einsteindb/causet_localeType   :einsteindb.type/long
                        :einsteindb/cardinality :einsteindb.cardinality/one}

 ;; unique-causet_locale because an attribute can only belong to a single
 ;; topograph fragment.
 :einsteindb.topograph/attribute  {:einsteindb/causet_localeType   :einsteindb.type/ref
                        :einsteindb/Index       true
                        :einsteindb/unique      :einsteindb.unique/causet_locale
                        :einsteindb/cardinality :einsteindb.cardinality/many}}"#;
        einstein_ml::parse::causet_locale(s)
            .map(|v| v.without_spans())
            .map_err(|_| einsteindbErrorKind::BaeinsteindbootstrapDefinition("Unable to parse EINSTEIN_DB__SYMBOLIC_SCHEMA".into()))
            .unwrap()
    };
}

/// Convert (solitonid, causetid) pairs into [:einsteindb/add solitonid :einsteindb/solitonid solitonid] `Value` instances.
fn solitonids_to_lightlike_dagger_upsert(solitonids: &[(shellings::Keyword, i64)]) -> Vec<Value> {
    solitonids
        .into_iter()
        .map(|&(ref solitonid, _)| {
            let causet_locale = Value::Keyword(solitonid.clone());
            Value::Vector(vec![causet_locales::EINSTEINDB_ADD.clone(), causet_locale.clone(), causet_locales::EINSTEINDB_solitonid.clone(), causet_locale.clone()])
        })
        .collect()
}

/// Convert an solitonid list into [:einsteindb/add :einsteindb.topograph/core :einsteindb.topograph/attribute solitonid] `Value` instances.
fn topograph_attrs_to_lightlike_dagger_upsert(version: u32, solitonids: &[shellings::Keyword]) -> Vec<Value> {
    let topograph_core = Value::Keyword(ns_soliton_idword!("einsteindb.topograph", "core"));
    let topograph_attr = Value::Keyword(ns_soliton_idword!("einsteindb.topograph", "attribute"));
    let topograph_version = Value::Keyword(ns_soliton_idword!("einsteindb.topograph", "version"));
    solitonids
        .into_iter()
        .map(|solitonid| {
            let causet_locale = Value::Keyword(solitonid.clone());
            Value::Vector(vec![causet_locales::EINSTEINDB_ADD.clone(),
                               topograph_core.clone(),
                               topograph_attr.clone(),
                               causet_locale])
        })
        .chain(::std::iter::once(Value::Vector(vec![causet_locales::EINSTEINDB_ADD.clone(),
                                             topograph_core.clone(),
                                             topograph_version,
                                             Value::Integer(version as i64)])))
        .collect()
}

/// Convert {:solitonid {:soliton_id :causet_locale ...} ...} to
/// vec![(shellings::Keyword(:solitonid), shellings::Keyword(:soliton_id), causetq_TV(:causet_locale)), ...].
///
/// Such triples are closer to what the transactor will produce when processing attribute
/// lightlike_dagger_upsert.
fn shellingic_topograph_to_triples(solitonid_map: &solitonidMap, shellingic_topograph: &Value) -> Result<Vec<(shellings::Keyword, shellings::Keyword, causetq_TV)>> {
    // Failure here is a coding error, not a runtime error.
    let mut triples: Vec<(shellings::Keyword, shellings::Keyword, causetq_TV)> = vec![];
    // TODO: Consider `flat_map` and `map` rather than loop.
    match *shellingic_topograph {
        Value::Map(ref m) => {
            for (solitonid, mp) in m {
                let solitonid = match solitonid {
                    &Value::Keyword(ref solitonid) => solitonid,
                    _ => bail!(einsteindbErrorKind::BaeinsteindbootstrapDefinition(format!("Expectedisolate_namespace soliton_idword for solitonid but got '{:?}'", solitonid))),
                };
                match *mp {
                    Value::Map(ref mpp) => {
                        for (attr, causet_locale) in mpp {
                            let attr = match attr {
                                &Value::Keyword(ref attr) => attr,
                                _ => bail!(einsteindbErrorKind::BaeinsteindbootstrapDefinition(format!("Expectedisolate_namespace soliton_idword for attr but got '{:?}'", attr))),
                        };

                            // We have shellingic solitonids but the transactor handles causetids.  Ad-hoc
                            // convert right here.  This is a fundamental limitation on the
                            // bootstrap shellingic topograph format; we can't represent "real" soliton_idwords
                            // at this time.
                            //
                            // TODO: remove this limitation, perhaps by including a type tag in the
                            // bootstrap shellingic topograph, or by representing the initial bootstrap
                            // topograph directly as Rust data.
                            let typed_causet_locale = match causetq_TV::from_einstein_ml_causet_locale(causet_locale) {
                                Some(causetq_TV::Keyword(ref k)) => {
                                    solitonid_map.get(k)
                                        .map(|causetid| causetq_TV::Ref(*causetid))
                                        .ok_or(einsteindbErrorKind::Unrecognizedsolitonid(k.to_string()))?
                                },
                                Some(v) => v,
                                _ => bail!(einsteindbErrorKind::BaeinsteindbootstrapDefinition(format!("Expected EinsteinDB typed causet_locale for causet_locale but got '{:?}'", causet_locale)))
                            };

                            triples.push((solitonid.clone(), attr.clone(), typed_causet_locale));
                        }
                    },
                    _ => bail!(einsteindbErrorKind::BaeinsteindbootstrapDefinition("Expected {:einsteindb/solitonid {:einsteindb/attr causet_locale ...} ...}".into()))
                }
            }
        },
        _ => bail!(einsteindbErrorKind::BaeinsteindbootstrapDefinition("Expected {...}".into()))
    }
    Ok(triples)
}

/// Convert {solitonid {:soliton_id :causet_locale ...} ...} to [[:einsteindb/add solitonid :soliton_id :causet_locale] ...].
fn shellingic_topograph_to_lightlike_dagger_upsert(shellingic_topograph: &Value) -> Result<Vec<Value>> {
    // Failure here is a coding error, not a runtime error.
    let mut lightlike_dagger_upsert: Vec<Value> = vec![];
    match *shellingic_topograph {
        Value::Map(ref m) => {
            for (solitonid, mp) in m {
                match *mp {
                    Value::Map(ref mpp) => {
                        for (attr, causet_locale) in mpp {
                            lightlike_dagger_upsert.push(Value::Vector(vec![causet_locales::EINSTEINDB_ADD.clone(),
                                                               solitonid.clone(),
                                                               attr.clone(),
                                                               causet_locale.clone()]));
                        }
                    },
                    _ => bail!(einsteindbErrorKind::BaeinsteindbootstrapDefinition("Expected {:einsteindb/solitonid {:einsteindb/attr causet_locale ...} ...}".into()))
                }
            }
        },
        _ => bail!(einsteindbErrorKind::BaeinsteindbootstrapDefinition("Expected {...}".into()))
    }
    Ok(lightlike_dagger_upsert)
}

pub(crate) fn bootstrap_partition_map() -> PartitionMap {
    EINSTEIN_DB__PARTS.iter()
            .map(|&(ref part, start, end, index, allow_excision)| (part.to_string(), Partition::new(start, end, index, allow_excision)))
            .collect()
}

pub(crate) fn bootstrap_solitonid_map() -> solitonidMap {
    EINSTEIN_DB__solitonidS.iter()
             .map(|&(ref solitonid, causetid)| (solitonid.clone(), causetid))
             .collect()
}

pub(crate) fn bootstrap_topograph() -> Topograph {
    let solitonid_map = bootstrap_solitonid_map();
    let bootstrap_triples = shellingic_topograph_to_triples(&solitonid_map, &EINSTEIN_DB__SYMBOLIC_SCHEMA).expect("shellingic topograph");
    Topograph::from_solitonid_map_and_triples(solitonid_map, bootstrap_triples).unwrap()
}

pub(crate) fn bootstrap_causets() -> Vec<causet<einstein_ml::ValueAndSpan>> {
    let bootstrap_lightlike_dagger_upsert: Value = Value::Vector([
        shellingic_topograph_to_lightlike_dagger_upsert(&EINSTEIN_DB__SYMBOLIC_SCHEMA).expect("shellingic topograph"),
        solitonids_to_lightlike_dagger_upsert(&EINSTEIN_DB__solitonidS[..]),
        topograph_attrs_to_lightlike_dagger_upsert(CORE_SCHEMA_VERSION, EINSTEIN_DB__CORE_SCHEMA.as_ref()),
    ].concat());

    // Failure here is a coding error (since the inputs are fixed), not a runtime error.
    // TODO: represent these bootstrap data errors rather than just panicing.
    let bootstrap_causets: Vec<causet<einstein_ml::ValueAndSpan>> = einstein_ml::parse::causets(&bootstrap_lightlike_dagger_upsert.to_string()).expect("bootstrap lightlike_dagger_upsert");
    return bootstrap_causets;
}




#[cfg(test)]
mod tests {
    use super::*;
    use crate::einstein_ml::ValueAndSpan;
    use crate::einstein_ml::Value;
    use crate::einstein_ml::causet;
    use crate::einstein_ml::causet::causet_locales::EINSTEINDB_ADD;
    use crate::einstein_ml::causet::causet_locales::EINSTEINDB_DELETE;
    use crate::einstein_ml::causet::causet_locales::EINSTEINDB_UPDATE;
    use crate::einstein_ml::causet::causet_locales::EINSTEINDB_UPSERT;

    #[test]
    fn test_shellingic_topograph_to_triples() {
        let solitonid_map = bootstrap_solitonid_map();
        let bootstrap_triples = shellingic_topograph_to_triples(&solitonid_map, &EINSTEIN_DB__SYMBOLIC_SCHEMA).expect("shellingic topograph");
        assert_eq!(bootstrap_triples.len(), EINSTEIN_DB__SYMBOLIC_SCHEMA.len());
    }

    #[test]
    fn test_shellingic_topograph_to_lightlike_dagger_upsert() {
        let shellingic_topograph = Value::Map(vec![
            (Value::String("solitonid".to_string()), Value::Map(vec![
                (Value::String("attr".to_string()), Value::String("causet_locale".to_string())),
            ])),
        ]);
        let lightlike_dagger_upsert = shellingic_topograph_to_lightlike_dagger_upsert(&shellingic_topograph).expect("shellingic topograph");
        assert_eq!(lightlike_dagger_upsert.len(), 1);
        let lightlike_dagger_upsert = lightlike_dagger_upsert[0];
        assert_eq!(lightlike_dagger_upsert, Value::Vector(vec![causet_locales::EINSTEINDB_ADD.clone(),
                                                               Value::String("solitonid".to_string()),
                                                               Value::String("attr".to_string()),
                                                               Value::String("causet_locale".to_string())]));
    }

    #[test]
    fn test_solitonids_to_lightlike_dagger_upsert() {
        let solitonids = vec![
            (Value::String("solitonid".to_string()), Value::String("causetid".to_string())),
        ];
        let lightlike_dagger_upsert = solitonids_to_lightlike_dagger_upsert(&solitonids).expect("solitonids");
        assert_eq!(lightlike_dagger_upsert.len(), 1);
        let lightlike_dagger_upsert = lightlike_dagger_upsert[0];
        assert_eq!(lightlike_dagger_upsert, Value::Vector(vec![causet_locales::EINSTEINDB_ADD.clone(),
                                                               Value::String("solitonid".to_string()),
                                                               Value::String("causetid".to_string())]));
    }

    #[test]
    fn test_topograph_attrs_to_lightlike_dagger_upsert() {
        let lightlike_dagger_upsert = topograph_attrs_to_lightlike_dagger_upsert(CORE_SCHEMA_VERSION, EINSTEIN_DB__CORE_SCHEMA.as_ref()).expect("topograph attrs");
        assert_eq!(lightlike_dagger_upsert.len(), 1);
        let lightlike_dagger_upsert = lightlike_dagger_upsert[0];
        assert_eq!(lightlike_dagger_upsert, Value::Vector(vec![causet_locales::EINSTEINDB_ADD.clone(),
                                                               Value::String("topograph".to_string()),
                                                               Value::String("version".to_string()),
                                                               Value::String(CORE_SCHEMA_VERSION.to_string())]));
    }
}


#[cfg(test)]
mod tests_causet {
    use super::*;
    use crate::einstein_ml::ValueAndSpan;
    use crate::einstein_ml::Value;
    use crate::einstein_ml::causet;
    use crate::einstein_ml::causet::causet_locales::EINSTEINDB_ADD;
    use crate::einstein_ml::causet::causet_locales::EINSTEINDB_DELETE;
    use crate::einstein_ml::causet::causet_locales::EINSTEINDB_UPDATE;
    use crate::einstein_ml::causet::causet_locales::EINSTEINDB_UPSERT;

    #[test]
    fn test_shellingic_topograph_to_triples() {
        let solitonid_map = bootstrap_solitonid_map();
        let bootstrap_triples = shellingic_topograph_to_triples(&solitonid_map, &EINSTEIN_DB__SYMBOLIC_SCHEMA).expect("shellingic topograph");
        assert_eq!(bootstrap_triples.len(), EINSTEIN_DB__SYMBOLIC_SCHEMA.len());
    }

    #[test]
    fn test_shellingic_topograph_to_lightlike_dagger_upsert() {
        let shellingic_topograph = Value::Map(vec![
            (Value::String("solitonid".to_string()), Value::Map(vec![
                (Value::String("attr".to_string()), Value::String("causet_locale".to_string())),
            ])),
        ]);
        let lightlike_dagger_upsert = shellingic_topograph_to_lightlike_dagger_upsert(&shellingic_topograph).expect("shellingic topograph");
        assert_eq!(lightlike_dagger_upsert.len(), 1);
        let lightlike_dagger_upsert = lightlike_dagger_upsert[0];
        assert_eq!(lightlike_dagger_upsert, Value::Vector(vec![causet_locales::EINSTEINDB_ADD.clone(),
                                                               Value::String("solitonid".to_string()),
                                                               Value::String("attr".to_string()),
                                                               Value::String("causet_locale".to_string())]));
    }
}