// Copyright 2022 Whtcorps Inc and EinstAI Inc
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]

use einsteinml;
use einsteineinsteindb_traits::errors::{
    einsteindbErrorKind,
    Result,
};
use einsteinml::types::Value;
use einsteinml::symbols;
use causetids;
use einsteindb::TypedSQLValue;
use einsteinml::causets::causet;

use core_traits::{
    TypedValue,
    values,
};

use einsteineinsteindb_core::{
    solitonidMap,
    Schema,
};
use schema::SchemaBuilding;
use types::{Partition, PartitionMap};

/// The first transaction ID applied to the knowledge base.
///
/// This is the start of the :einsteindb.part/tx partition.
pub const TX0: i64 = 0x10000000;

/// This is the start of the :einsteindb.part/user partition.
pub const USER0: i64 = 0x10000;

// Corresponds to the version of the :einsteindb.schema/core vocabulary.
pub const CORE_SCHEMA_VERSION: u32 = 1;

lazy_static! {
    static ref V1_solitonidS: [(symbols::Keyword, i64); 40] = {
            [(ns_keyword!("einsteindb", "solitonid"),             causetids::EINSTEINeinsteindb_solitonid),
             (ns_keyword!("einsteindb.part", "einsteindb"),           causetids::EINSTEINeinsteindb_PART_EINSTEINeinsteindb),
             (ns_keyword!("einsteindb", "txInstant"),         causetids::EINSTEINeinsteindb_TX_INSTANT),
             (ns_keyword!("einsteindb.install", "partition"), causetids::EINSTEINeinsteindb_INSTALL_PARTITION),
             (ns_keyword!("einsteindb.install", "valueType"), causetids::EINSTEINeinsteindb_INSTALL_VALUE_TYPE),
             (ns_keyword!("einsteindb.install", "attribute"), causetids::EINSTEINeinsteindb_INSTALL_ATTRIBUTE),
             (ns_keyword!("einsteindb", "valueType"),         causetids::EINSTEINeinsteindb_VALUE_TYPE),
             (ns_keyword!("einsteindb", "cardinality"),       causetids::EINSTEINeinsteindb_CARDINALITY),
             (ns_keyword!("einsteindb", "unique"),            causetids::EINSTEINeinsteindb_UNIQUE),
             (ns_keyword!("einsteindb", "isComponent"),       causetids::EINSTEINeinsteindb_IS_COMPONENT),
             (ns_keyword!("einsteindb", "index"),             causetids::EINSTEINeinsteindb_INDEX),
             (ns_keyword!("einsteindb", "fulltext"),          causetids::EINSTEINeinsteindb_FULLTEXT),
             (ns_keyword!("einsteindb", "noHistory"),         causetids::EINSTEINeinsteindb_NO_HISTORY),
             (ns_keyword!("einsteindb", "add"),               causetids::EINSTEINeinsteindb_ADD),
             (ns_keyword!("einsteindb", "retract"),           causetids::EINSTEINeinsteindb_RETRACT),
             (ns_keyword!("einsteindb.part", "user"),         causetids::EINSTEINeinsteindb_PART_USER),
             (ns_keyword!("einsteindb.part", "tx"),           causetids::EINSTEINeinsteindb_PART_TX),
             (ns_keyword!("einsteindb", "excise"),            causetids::EINSTEINeinsteindb_EXCISE),
             (ns_keyword!("einsteindb.excise", "attrs"),      causetids::EINSTEINeinsteindb_EXCISE_ATTRS),
             (ns_keyword!("einsteindb.excise", "beforeT"),    causetids::EINSTEINeinsteindb_EXCISE_BEFORE_T),
             (ns_keyword!("einsteindb.excise", "before"),     causetids::EINSTEINeinsteindb_EXCISE_BEFORE),
             (ns_keyword!("einsteindb.alter", "attribute"),   causetids::EINSTEINeinsteindb_ALTER_ATTRIBUTE),
             (ns_keyword!("einsteindb.type", "ref"),          causetids::EINSTEINeinsteindb_TYPE_REF),
             (ns_keyword!("einsteindb.type", "keyword"),      causetids::EINSTEINeinsteindb_TYPE_KEYWORD),
             (ns_keyword!("einsteindb.type", "long"),         causetids::EINSTEINeinsteindb_TYPE_LONG),
             (ns_keyword!("einsteindb.type", "double"),       causetids::EINSTEINeinsteindb_TYPE_DOUBLE),
             (ns_keyword!("einsteindb.type", "string"),       causetids::EINSTEINeinsteindb_TYPE_STRING),
             (ns_keyword!("einsteindb.type", "uuid"),         causetids::EINSTEINeinsteindb_TYPE_UUID),
             (ns_keyword!("einsteindb.type", "uri"),          causetids::EINSTEINeinsteindb_TYPE_URI),
             (ns_keyword!("einsteindb.type", "boolean"),      causetids::EINSTEINeinsteindb_TYPE_BOOLEAN),
             (ns_keyword!("einsteindb.type", "instant"),      causetids::EINSTEINeinsteindb_TYPE_INSTANT),
             (ns_keyword!("einsteindb.type", "bytes"),        causetids::EINSTEINeinsteindb_TYPE_BYTES),
             (ns_keyword!("einsteindb.cardinality", "one"),   causetids::EINSTEINeinsteindb_CARDINALITY_ONE),
             (ns_keyword!("einsteindb.cardinality", "many"),  causetids::EINSTEINeinsteindb_CARDINALITY_MANY),
             (ns_keyword!("einsteindb.unique", "value"),      causetids::EINSTEINeinsteindb_UNIQUE_VALUE),
             (ns_keyword!("einsteindb.unique", "idcauset"),   causetids::EINSTEINeinsteindb_UNIQUE_IDcauset),
             (ns_keyword!("einsteindb", "doc"),               causetids::EINSTEINeinsteindb_DOC),
             (ns_keyword!("einsteindb.schema", "version"),    causetids::EINSTEINeinsteindb_SCHEMA_VERSION),
             (ns_keyword!("einsteindb.schema", "attribute"),  causetids::EINSTEINeinsteindb_SCHEMA_ATTRIBUTE),
             (ns_keyword!("einsteindb.schema", "core"),       causetids::EINSTEINeinsteindb_SCHEMA_CORE),
        ]
    };

    pub static ref V1_PARTS: [(symbols::Keyword, i64, i64, i64, bool); 3] = {
            [(ns_keyword!("einsteindb.part", "einsteindb"), 0, USER0 - 1, (1 + V1_solitonidS.len()) as i64, false),
             (ns_keyword!("einsteindb.part", "user"), USER0, TX0 - 1, USER0, true),
             (ns_keyword!("einsteindb.part", "tx"), TX0, i64::max_value(), TX0, false),
        ]
    };

    static ref V1_CORE_SCHEMA: [(symbols::Keyword); 16] = {
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
             (ns_keyword!("einsteindb.schema", "version")),
             (ns_keyword!("einsteindb.schema", "attribute")),
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
 :einsteindb.schema/version    {:einsteindb/valueType   :einsteindb.type/long
                        :einsteindb/cardinality :einsteindb.cardinality/one}

 ;; unique-value because an attribute can only belong to a single
 ;; schema fragment.
 :einsteindb.schema/attribute  {:einsteindb/valueType   :einsteindb.type/ref
                        :einsteindb/index       true
                        :einsteindb/unique      :einsteindb.unique/value
                        :einsteindb/cardinality :einsteindb.cardinality/many}}"#;
        einsteinml::parse::value(s)
            .map(|v| v.without_spans())
            .map_err(|_| einsteindbErrorKind::BaeinsteindbootstrapDefinition("Unable to parse V1_SYMBOLIC_SCHEMA".into()))
            .unwrap()
    };
}

/// Convert (solitonid, causetid) pairs into [:einsteindb/add solitonid :einsteindb/solitonid solitonid] `Value` instances.
fn solitonids_to_assertions(solitonids: &[(symbols::Keyword, i64)]) -> Vec<Value> {
    solitonids
        .into_iter()
        .map(|&(ref solitonid, _)| {
            let value = Value::Keyword(solitonid.clone());
            Value::Vector(vec![values::EINSTEINeinsteindb_ADD.clone(), value.clone(), values::EINSTEINeinsteindb_solitonid.clone(), value.clone()])
        })
        .collect()
}

/// Convert an solitonid list into [:einsteindb/add :einsteindb.schema/core :einsteindb.schema/attribute solitonid] `Value` instances.
fn schema_attrs_to_assertions(version: u32, solitonids: &[symbols::Keyword]) -> Vec<Value> {
    let schema_core = Value::Keyword(ns_keyword!("einsteindb.schema", "core"));
    let schema_attr = Value::Keyword(ns_keyword!("einsteindb.schema", "attribute"));
    let schema_version = Value::Keyword(ns_keyword!("einsteindb.schema", "version"));
    solitonids
        .into_iter()
        .map(|solitonid| {
            let value = Value::Keyword(solitonid.clone());
            Value::Vector(vec![values::EINSTEINeinsteindb_ADD.clone(),
                               schema_core.clone(),
                               schema_attr.clone(),
                               value])
        })
        .chain(::std::iter::once(Value::Vector(vec![values::EINSTEINeinsteindb_ADD.clone(),
                                             schema_core.clone(),
                                             schema_version,
                                             Value::Integer(version as i64)])))
        .collect()
}

/// Convert {:solitonid {:key :value ...} ...} to
/// vec![(symbols::Keyword(:solitonid), symbols::Keyword(:key), TypedValue(:value)), ...].
///
/// Such triples are closer to what the transactor will produce when processing attribute
/// assertions.
fn symbolic_schema_to_triples(solitonid_map: &solitonidMap, symbolic_schema: &Value) -> Result<Vec<(symbols::Keyword, symbols::Keyword, TypedValue)>> {
    // Failure here is a coding error, not a runtime error.
    let mut triples: Vec<(symbols::Keyword, symbols::Keyword, TypedValue)> = vec![];
    // TODO: Consider `flat_map` and `map` rather than loop.
    match *symbolic_schema {
        Value::Map(ref m) => {
            for (solitonid, mp) in m {
                let solitonid = match solitonid {
                    &Value::Keyword(ref solitonid) => solitonid,
                    _ => bail!(einsteindbErrorKind::BaeinsteindbootstrapDefinition(format!("Expected namespaced keyword for solitonid but got '{:?}'", solitonid))),
                };
                match *mp {
                    Value::Map(ref mpp) => {
                        for (attr, value) in mpp {
                            let attr = match attr {
                                &Value::Keyword(ref attr) => attr,
                                _ => bail!(einsteindbErrorKind::BaeinsteindbootstrapDefinition(format!("Expected namespaced keyword for attr but got '{:?}'", attr))),
                        };

                            // We have symbolic solitonids but the transactor handles causetids.  Ad-hoc
                            // convert right here.  This is a fundamental limitation on the
                            // bootstrap symbolic schema format; we can't represent "real" keywords
                            // at this time.
                            //
                            // TODO: remove this limitation, perhaps by including a type tag in the
                            // bootstrap symbolic schema, or by representing the initial bootstrap
                            // schema directly as Rust data.
                            let typed_value = match TypedValue::from_einsteinml_value(value) {
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
fn symbolic_schema_to_assertions(symbolic_schema: &Value) -> Result<Vec<Value>> {
    // Failure here is a coding error, not a runtime error.
    let mut assertions: Vec<Value> = vec![];
    match *symbolic_schema {
        Value::Map(ref m) => {
            for (solitonid, mp) in m {
                match *mp {
                    Value::Map(ref mpp) => {
                        for (attr, value) in mpp {
                            assertions.push(Value::Vector(vec![values::EINSTEINeinsteindb_ADD.clone(),
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
    Ok(assertions)
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

pub(crate) fn bootstrap_schema() -> Schema {
    let solitonid_map = bootstrap_solitonid_map();
    let bootstrap_triples = symbolic_schema_to_triples(&solitonid_map, &V1_SYMBOLIC_SCHEMA).expect("symbolic schema");
    Schema::from_solitonid_map_and_triples(solitonid_map, bootstrap_triples).unwrap()
}

pub(crate) fn bootstrap_causets() -> Vec<causet<einsteinml::ValueAndSpan>> {
    let bootstrap_assertions: Value = Value::Vector([
        symbolic_schema_to_assertions(&V1_SYMBOLIC_SCHEMA).expect("symbolic schema"),
        solitonids_to_assertions(&V1_solitonidS[..]),
        schema_attrs_to_assertions(CORE_SCHEMA_VERSION, V1_CORE_SCHEMA.as_ref()),
    ].concat());

    // Failure here is a coding error (since the inputs are fixed), not a runtime error.
    // TODO: represent these bootstrap data errors rather than just panicing.
    let bootstrap_causets: Vec<causet<einsteinml::ValueAndSpan>> = einsteinml::parse::causets(&bootstrap_assertions.to_string()).expect("bootstrap assertions");
    return bootstrap_causets;
}
