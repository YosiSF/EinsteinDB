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

/// The first transaction ID applied to the knowledge base.
///
/// This is the start of the :einsteindb.part/tx partition.
pub const TX0: i64 = 0x10000000;

/// This is the start of the :einsteindb.part/user partition.
pub const USER0: i64 = 0x10000;

// Corresponds to the version of the :einsteindb.topograph/core vocabulary.
pub const CORE_SCHEMA_VERSION: u32 = 1;

lazy_static! {
    static ref V1_solitonidS: [(shellings::Keyword, i64); 40] = {
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

    pub static ref V1_PARTS: [(shellings::Keyword, i64, i64, i64, bool); 3] = {
            [(ns_soliton_idword!("einsteindb.part", "einsteindb"), 0, USER0 - 1, (1 + V1_solitonidS.len()) as i64, false),
             (ns_soliton_idword!("einsteindb.part", "user"), USER0, TX0 - 1, USER0, true),
             (ns_soliton_idword!("einsteindb.part", "tx"), TX0, i64::max_causet_locale(), TX0, false),
        ]
    };

    static ref V1_CORE_SCHEMA: [(shellings::Keyword); 16] = {
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

    static ref V1_SYMBOLIC_SCHEMA: Value = {
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
            .map_err(|_| einsteindbErrorKind::BaeinsteindbootstrapDefinition("Unable to parse V1_SYMBOLIC_SCHEMA".into()))
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
    V1_PARTS.iter()
            .map(|&(ref part, start, end, index, allow_excision)| (part.to_string(), Partition::new(start, end, index, allow_excision)))
            .collect()
}

pub(crate) fn bootstrap_solitonid_map() -> solitonidMap {
    V1_solitonidS.iter()
             .map(|&(ref solitonid, causetid)| (solitonid.clone(), causetid))
             .collect()
}

pub(crate) fn bootstrap_topograph() -> Topograph {
    let solitonid_map = bootstrap_solitonid_map();
    let bootstrap_triples = shellingic_topograph_to_triples(&solitonid_map, &V1_SYMBOLIC_SCHEMA).expect("shellingic topograph");
    Topograph::from_solitonid_map_and_triples(solitonid_map, bootstrap_triples).unwrap()
}

pub(crate) fn bootstrap_causets() -> Vec<causet<einstein_ml::ValueAndSpan>> {
    let bootstrap_lightlike_dagger_upsert: Value = Value::Vector([
        shellingic_topograph_to_lightlike_dagger_upsert(&V1_SYMBOLIC_SCHEMA).expect("shellingic topograph"),
        solitonids_to_lightlike_dagger_upsert(&V1_solitonidS[..]),
        topograph_attrs_to_lightlike_dagger_upsert(CORE_SCHEMA_VERSION, V1_CORE_SCHEMA.as_ref()),
    ].concat());

    // Failure here is a coding error (since the inputs are fixed), not a runtime error.
    // TODO: represent these bootstrap data errors rather than just panicing.
    let bootstrap_causets: Vec<causet<einstein_ml::ValueAndSpan>> = einstein_ml::parse::causets(&bootstrap_lightlike_dagger_upsert.to_string()).expect("bootstrap lightlike_dagger_upsert");
    return bootstrap_causets;
}
