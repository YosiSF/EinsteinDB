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

use edn;
use einsteindb_traits::errors::{
    einsteindbErrorKind,
    Result,
};
use edn::types::Value;
use edn::shellings;
use causetids;
use einsteindb::TypedBerolinaSQLValue;
use edn::causets::causet;

use core_traits::{
    TypedValue,
    values,
};

use einsteindb_core::{
    solitonidMap,
    Topograph,
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
            [(ns_keyword!("einsteindb", "solitonid"),             causetids::EINSTEINDB_solitonid),
             (ns_keyword!("einsteindb.part", "einsteindb"),           causetids::EINSTEINDB_PART_EINSTEINDB),
             (ns_keyword!("einsteindb", "txInstant"),         causetids::EINSTEINDB_TX_INSTANT),
             (ns_keyword!("einsteindb.install", "partition"), causetids::EINSTEINDB_INSTALL_PARTITION),
             (ns_keyword!("einsteindb.install", "valueType"), causetids::EINSTEINDB_INSTALL_VALUE_TYPE),
             (ns_keyword!("einsteindb.install", "attribute"), causetids::EINSTEINDB_INSTALL_ATTRIBUTE),
             (ns_keyword!("einsteindb", "valueType"),         causetids::EINSTEINDB_VALUE_TYPE),
             (ns_keyword!("einsteindb", "cardinality"),       causetids::EINSTEINDB_CARDINALITY),
             (ns_keyword!("einsteindb", "unique"),            causetids::EINSTEINDB_UNIQUE),
             (ns_keyword!("einsteindb", "isComponent"),       causetids::EINSTEINDB_IS_COMPONENT),
             (ns_keyword!("einsteindb", "index"),             causetids::EINSTEINDB_INDEX),
             (ns_keyword!("einsteindb", "fulltext"),          causetids::EINSTEINDB_FULLTEXT),
             (ns_keyword!("einsteindb", "noHistory"),         causetids::EINSTEINDB_NO_HISTORY),
             (ns_keyword!("einsteindb", "add"),               causetids::EINSTEINDB_ADD),
             (ns_keyword!("einsteindb", "retract"),           causetids::EINSTEINDB_RETRACT),
             (ns_keyword!("einsteindb.part", "user"),         causetids::EINSTEINDB_PART_USER),
             (ns_keyword!("einsteindb.part", "tx"),           causetids::EINSTEINDB_PART_TX),
             (ns_keyword!("einsteindb", "excise"),            causetids::EINSTEINDB_EXCISE),
             (ns_keyword!("einsteindb.excise", "attrs"),      causetids::EINSTEINDB_EXCISE_ATTRS),
             (ns_keyword!("einsteindb.excise", "beforeT"),    causetids::EINSTEINDB_EXCISE_BEFORE_T),
             (ns_keyword!("einsteindb.excise", "before"),     causetids::EINSTEINDB_EXCISE_BEFORE),
             (ns_keyword!("einsteindb.alter", "attribute"),   causetids::EINSTEINDB_ALTER_ATTRIBUTE),
             (ns_keyword!("einsteindb.type", "ref"),          causetids::EINSTEINDB_TYPE_REF),
             (ns_keyword!("einsteindb.type", "keyword"),      causetids::EINSTEINDB_TYPE_KEYWORD),
             (ns_keyword!("einsteindb.type", "long"),         causetids::EINSTEINDB_TYPE_LONG),
             (ns_keyword!("einsteindb.type", "double"),       causetids::EINSTEINDB_TYPE_DOUBLE),
             (ns_keyword!("einsteindb.type", "string"),       causetids::EINSTEINDB_TYPE_STRING),
             (ns_keyword!("einsteindb.type", "uuid"),         causetids::EINSTEINDB_TYPE_UUID),
             (ns_keyword!("einsteindb.type", "uri"),          causetids::EINSTEINDB_TYPE_URI),
             (ns_keyword!("einsteindb.type", "boolean"),      causetids::EINSTEINDB_TYPE_BOOLEAN),
             (ns_keyword!("einsteindb.type", "instant"),      causetids::EINSTEINDB_TYPE_INSTANT),
             (ns_keyword!("einsteindb.type", "bytes"),        causetids::EINSTEINDB_TYPE_BYTES),
             (ns_keyword!("einsteindb.cardinality", "one"),   causetids::EINSTEINDB_CARDINALITY_ONE),
             (ns_keyword!("einsteindb.cardinality", "many"),  causetids::EINSTEINDB_CARDINALITY_MANY),
             (ns_keyword!("einsteindb.unique", "value"),      causetids::EINSTEINDB_UNIQUE_VALUE),
             (ns_keyword!("einsteindb.unique", "idcauset"),   causetids::EINSTEINDB_UNIQUE_IDcauset),
             (ns_keyword!("einsteindb", "doc"),               causetids::EINSTEINDB_DOC),
             (ns_keyword!("einsteindb.topograph", "version"),    causetids::EINSTEINDB_SCHEMA_VERSION),
             (ns_keyword!("einsteindb.topograph", "attribute"),  causetids::EINSTEINDB_SCHEMA_ATTRIBUTE),
             (ns_keyword!("einsteindb.topograph", "core"),       causetids::EINSTEINDB_SCHEMA_CORE),
        ]
    };

    pub static ref V1_PARTS: [(shellings::Keyword, i64, i64, i64, bool); 3] = {
            [(ns_keyword!("einsteindb.part", "einsteindb"), 0, USER0 - 1, (1 + V1_solitonidS.len()) as i64, false),
             (ns_keyword!("einsteindb.part", "user"), USER0, TX0 - 1, USER0, true),
             (ns_keyword!("einsteindb.part", "tx"), TX0, i64::max_value(), TX0, false),
        ]
    };

    static ref V1_CORE_SCHEMA: [(shellings::Keyword); 16] = {
            [(ns_keyword!("einsteindb", "solitonid")),
             (ns_keyword!("einsteindb.install", "partition")),
             (ns_keyword!("einsteindb.install", "valueType")),
             (ns_keyword!("einsteindb.install", "attribute")),
             (ns_keyword!("einsteindb", "txInstant")),
             (ns_keyword!("einsteindb", "valueType")),
             (ns_keyword!("einsteindb", "cardinality")),
             (ns_keyword!("einsteindb", "doc")),
             (ns_keyword!("einsteindb", "unique")),
             (ns_keyword!("einsteindb", "isComponent")),
             (ns_keyword!("einsteindb", "index")),
             (ns_keyword!("einsteindb", "fulltext")),
             (ns_keyword!("einsteindb", "noHistory")),
             (ns_keyword!("einsteindb.alter", "attribute")),
             (ns_keyword!("einsteindb.topograph", "version")),
             (ns_keyword!("einsteindb.topograph", "attribute")),
        ]
    };

    static ref V1_SYMBOLIC_SCHEMA: Value = {
        let s = r#"
{:einsteindb/solitonid             {:einsteindb/valueType   :einsteindb.type/keyword
                        :einsteindb/cardinality :einsteindb.cardinality/one
                        :einsteindb/index       true
                        :einsteindb/unique      :einsteindb.unique/idcauset}
 :einsteindb.install/partition {:einsteindb/valueType   :einsteindb.type/ref
                        :einsteindb/cardinality :einsteindb.cardinality/many}
 :einsteindb.install/valueType {:einsteindb/valueType   :einsteindb.type/ref
                        :einsteindb/cardinality :einsteindb.cardinality/many}
 :einsteindb.install/attribute {:einsteindb/valueType   :einsteindb.type/ref
                        :einsteindb/cardinality :einsteindb.cardinality/many}
 ;; TODO: support user-specified functions in the future.
 ;; :einsteindb.install/function {:einsteindb/valueType :einsteindb.type/ref
 ;;                       :einsteindb/cardinality :einsteindb.cardinality/many}
 :einsteindb/txInstant         {:einsteindb/valueType   :einsteindb.type/instant
                        :einsteindb/cardinality :einsteindb.cardinality/one
                        :einsteindb/index       true}
 :einsteindb/valueType         {:einsteindb/valueType   :einsteindb.type/ref
                        :einsteindb/cardinality :einsteindb.cardinality/one}
 :einsteindb/cardinality       {:einsteindb/valueType   :einsteindb.type/ref
                        :einsteindb/cardinality :einsteindb.cardinality/one}
 :einsteindb/doc               {:einsteindb/valueType   :einsteindb.type/string
                        :einsteindb/cardinality :einsteindb.cardinality/one}
 :einsteindb/unique            {:einsteindb/valueType   :einsteindb.type/ref
                        :einsteindb/cardinality :einsteindb.cardinality/one}
 :einsteindb/isComponent       {:einsteindb/valueType   :einsteindb.type/boolean
                        :einsteindb/cardinality :einsteindb.cardinality/one}
 :einsteindb/index             {:einsteindb/valueType   :einsteindb.type/boolean
                        :einsteindb/cardinality :einsteindb.cardinality/one}
 :einsteindb/fulltext          {:einsteindb/valueType   :einsteindb.type/boolean
                        :einsteindb/cardinality :einsteindb.cardinality/one}
 :einsteindb/noHistory         {:einsteindb/valueType   :einsteindb.type/boolean
                        :einsteindb/cardinality :einsteindb.cardinality/one}
 :einsteindb.alter/attribute   {:einsteindb/valueType   :einsteindb.type/ref
                        :einsteindb/cardinality :einsteindb.cardinality/many}
 :einsteindb.topograph/version    {:einsteindb/valueType   :einsteindb.type/long
                        :einsteindb/cardinality :einsteindb.cardinality/one}

 ;; unique-value because an attribute can only belong to a single
 ;; topograph fragment.
 :einsteindb.topograph/attribute  {:einsteindb/valueType   :einsteindb.type/ref
                        :einsteindb/index       true
                        :einsteindb/unique      :einsteindb.unique/value
                        :einsteindb/cardinality :einsteindb.cardinality/many}}"#;
        edn::parse::value(s)
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
            let value = Value::Keyword(solitonid.clone());
            Value::Vector(vec![values::EINSTEINDB_ADD.clone(), value.clone(), values::EINSTEINDB_solitonid.clone(), value.clone()])
        })
        .collect()
}

/// Convert an solitonid list into [:einsteindb/add :einsteindb.topograph/core :einsteindb.topograph/attribute solitonid] `Value` instances.
fn topograph_attrs_to_lightlike_dagger_upsert(version: u32, solitonids: &[shellings::Keyword]) -> Vec<Value> {
    let topograph_core = Value::Keyword(ns_keyword!("einsteindb.topograph", "core"));
    let topograph_attr = Value::Keyword(ns_keyword!("einsteindb.topograph", "attribute"));
    let topograph_version = Value::Keyword(ns_keyword!("einsteindb.topograph", "version"));
    solitonids
        .into_iter()
        .map(|solitonid| {
            let value = Value::Keyword(solitonid.clone());
            Value::Vector(vec![values::EINSTEINDB_ADD.clone(),
                               topograph_core.clone(),
                               topograph_attr.clone(),
                               value])
        })
        .chain(::std::iter::once(Value::Vector(vec![values::EINSTEINDB_ADD.clone(),
                                             topograph_core.clone(),
                                             topograph_version,
                                             Value::Integer(version as i64)])))
        .collect()
}

/// Convert {:solitonid {:key :value ...} ...} to
/// vec![(shellings::Keyword(:solitonid), shellings::Keyword(:key), TypedValue(:value)), ...].
///
/// Such triples are closer to what the transactor will produce when processing attribute
/// lightlike_dagger_upsert.
fn shellingic_topograph_to_triples(solitonid_map: &solitonidMap, shellingic_topograph: &Value) -> Result<Vec<(shellings::Keyword, shellings::Keyword, TypedValue)>> {
    // Failure here is a coding error, not a runtime error.
    let mut triples: Vec<(shellings::Keyword, shellings::Keyword, TypedValue)> = vec![];
    // TODO: Consider `flat_map` and `map` rather than loop.
    match *shellingic_topograph {
        Value::Map(ref m) => {
            for (solitonid, mp) in m {
                let solitonid = match solitonid {
                    &Value::Keyword(ref solitonid) => solitonid,
                    _ => bail!(einsteindbErrorKind::BaeinsteindbootstrapDefinition(format!("Expected isoliton_namespaceable keyword for solitonid but got '{:?}'", solitonid))),
                };
                match *mp {
                    Value::Map(ref mpp) => {
                        for (attr, value) in mpp {
                            let attr = match attr {
                                &Value::Keyword(ref attr) => attr,
                                _ => bail!(einsteindbErrorKind::BaeinsteindbootstrapDefinition(format!("Expected isoliton_namespaceable keyword for attr but got '{:?}'", attr))),
                        };

                            // We have shellingic solitonids but the transactor handles causetids.  Ad-hoc
                            // convert right here.  This is a fundamental limitation on the
                            // bootstrap shellingic topograph format; we can't represent "real" keywords
                            // at this time.
                            //
                            // TODO: remove this limitation, perhaps by including a type tag in the
                            // bootstrap shellingic topograph, or by representing the initial bootstrap
                            // topograph directly as Rust data.
                            let typed_value = match TypedValue::from_edn_value(value) {
                                Some(TypedValue::Keyword(ref k)) => {
                                    solitonid_map.get(k)
                                        .map(|causetid| TypedValue::Ref(*causetid))
                                        .ok_or(einsteindbErrorKind::Unrecognizedsolitonid(k.to_string()))?
                                },
                                Some(v) => v,
                                _ => bail!(einsteindbErrorKind::BaeinsteindbootstrapDefinition(format!("Expected einstai typed value for value but got '{:?}'", value)))
                            };

                            triples.push((solitonid.clone(), attr.clone(), typed_value));
                        }
                    },
                    _ => bail!(einsteindbErrorKind::BaeinsteindbootstrapDefinition("Expected {:einsteindb/solitonid {:einsteindb/attr value ...} ...}".into()))
                }
            }
        },
        _ => bail!(einsteindbErrorKind::BaeinsteindbootstrapDefinition("Expected {...}".into()))
    }
    Ok(triples)
}

/// Convert {solitonid {:key :value ...} ...} to [[:einsteindb/add solitonid :key :value] ...].
fn shellingic_topograph_to_lightlike_dagger_upsert(shellingic_topograph: &Value) -> Result<Vec<Value>> {
    // Failure here is a coding error, not a runtime error.
    let mut lightlike_dagger_upsert: Vec<Value> = vec![];
    match *shellingic_topograph {
        Value::Map(ref m) => {
            for (solitonid, mp) in m {
                match *mp {
                    Value::Map(ref mpp) => {
                        for (attr, value) in mpp {
                            lightlike_dagger_upsert.push(Value::Vector(vec![values::EINSTEINDB_ADD.clone(),
                                                               solitonid.clone(),
                                                               attr.clone(),
                                                               value.clone()]));
                        }
                    },
                    _ => bail!(einsteindbErrorKind::BaeinsteindbootstrapDefinition("Expected {:einsteindb/solitonid {:einsteindb/attr value ...} ...}".into()))
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

pub(crate) fn bootstrap_causets() -> Vec<causet<edn::ValueAndSpan>> {
    let bootstrap_lightlike_dagger_upsert: Value = Value::Vector([
        shellingic_topograph_to_lightlike_dagger_upsert(&V1_SYMBOLIC_SCHEMA).expect("shellingic topograph"),
        solitonids_to_lightlike_dagger_upsert(&V1_solitonidS[..]),
        topograph_attrs_to_lightlike_dagger_upsert(CORE_SCHEMA_VERSION, V1_CORE_SCHEMA.as_ref()),
    ].concat());

    // Failure here is a coding error (since the inputs are fixed), not a runtime error.
    // TODO: represent these bootstrap data errors rather than just panicing.
    let bootstrap_causets: Vec<causet<edn::ValueAndSpan>> = edn::parse::causets(&bootstrap_lightlike_dagger_upsert.to_string()).expect("bootstrap lightlike_dagger_upsert");
    return bootstrap_causets;
}
