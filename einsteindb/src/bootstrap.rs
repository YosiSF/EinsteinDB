//Copyright 2021-2023 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]

use edbn;
use einsteindb_promises::errors::{
    DbErrorKind,
    Result,
};
use edbn::types::Value;
use edbn::symbols;
use causetids;
use einsteindb::TypedSQLValue;
use edbn::causets::Causets;

use embedded_promises::{
    TypedValue,
    values,
};

use EinsteinDB_embedded::{
    CausetIDMap,
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

// Corresponds to the version of the :einsteindb.schema/embedded vocabulary.
pub const embedded_SCHEMA_VERSION: u32 = 1;

lazy_static! {
    static ref V1_solitonidS: [(symbols::Keyword, i64); 40] = {
            [(ns_keyword!("einsteindb", "solitonid"),             causetids::EINSTEINDB_solitonid),
             (ns_keyword!("db.part", "einsteindb"),           causetids::EINSTEINDB_PART_EINSTEINDB),
             (ns_keyword!("einsteindb", "txInstant"),         causetids::EINSTEINDB_TX_INSTANT),
             (ns_keyword!("db.install", "partition"), causetids::EINSTEINDB_INSTALL_PARTITION),
             (ns_keyword!("db.install", "valueType"), causetids::EINSTEINDB_INSTALL_VALUE_TYPE),
             (ns_keyword!("db.install", "attribute"), causetids::EINSTEINDB_INSTALL_ATTRIBUTE),
             (ns_keyword!("einsteindb", "valueType"),         causetids::EINSTEINDB_VALUE_TYPE),
             (ns_keyword!("einsteindb", "cardinality"),       causetids::EINSTEINDB_CARDINALITY),
             (ns_keyword!("einsteindb", "unique"),            causetids::EINSTEINDB_UNIQUE),
             (ns_keyword!("einsteindb", "isComponent"),       causetids::EINSTEINDB_IS_COMPONENT),
             (ns_keyword!("einsteindb", "index"),             causetids::EINSTEINDB_INDEX),
             (ns_keyword!("einsteindb", "fulltext"),          causetids::EINSTEINDB_FULLTEXT),
             (ns_keyword!("einsteindb", "noHistory"),         causetids::EINSTEINDB_NO_HISTORY),
             (ns_keyword!("einsteindb", "add"),               causetids::EINSTEINDB_ADD),
             (ns_keyword!("einsteindb", "retract"),           causetids::EINSTEINDB_RETRACT),
             (ns_keyword!("db.part", "user"),         causetids::EINSTEINDB_PART_USER),
             (ns_keyword!("db.part", "tx"),           causetids::EINSTEINDB_PART_TX),
             (ns_keyword!("einsteindb", "excise"),            causetids::EINSTEINDB_EXCISE),
             (ns_keyword!("db.excise", "attrs"),      causetids::EINSTEINDB_EXCISE_ATTRS),
             (ns_keyword!("db.excise", "beforeT"),    causetids::EINSTEINDB_EXCISE_BEFORE_T),
             (ns_keyword!("db.excise", "before"),     causetids::EINSTEINDB_EXCISE_BEFORE),
             (ns_keyword!("db.alter", "attribute"),   causetids::EINSTEINDB_ALTER_ATTRIBUTE),
             (ns_keyword!("db.type", "ref"),          causetids::EINSTEINDB_TYPE_REF),
             (ns_keyword!("db.type", "keyword"),      causetids::EINSTEINDB_TYPE_KEYWORD),
             (ns_keyword!("db.type", "long"),         causetids::EINSTEINDB_TYPE_LONG),
             (ns_keyword!("db.type", "double"),       causetids::EINSTEINDB_TYPE_DOUBLE),
             (ns_keyword!("db.type", "string"),       causetids::EINSTEINDB_TYPE_STRING),
             (ns_keyword!("db.type", "uuid"),         causetids::EINSTEINDB_TYPE_UUID),
             (ns_keyword!("db.type", "uri"),          causetids::EINSTEINDB_TYPE_URI),
             (ns_keyword!("db.type", "boolean"),      causetids::EINSTEINDB_TYPE_BOOLEAN),
             (ns_keyword!("db.type", "instant"),      causetids::EINSTEINDB_TYPE_INSTANT),
             (ns_keyword!("db.type", "bytes"),        causetids::EINSTEINDB_TYPE_BYTES),
             (ns_keyword!("db.cardinality", "one"),   causetids::EINSTEINDB_CARDINALITY_ONE),
             (ns_keyword!("db.cardinality", "many"),  causetids::EINSTEINDB_CARDINALITY_MANY),
             (ns_keyword!("db.unique", "value"),      causetids::EINSTEINDB_UNIQUE_VALUE),
             (ns_keyword!("db.unique", "solitonidity"),   causetids::EINSTEINDB_UNIQUE_solitonidITY),
             (ns_keyword!("einsteindb", "doc"),               causetids::EINSTEINDB_DOC),
             (ns_keyword!("db.schema", "version"),    causetids::EINSTEINDB_SCHEMA_VERSION),
             (ns_keyword!("db.schema", "attribute"),  causetids::EINSTEINDB_SCHEMA_ATTRIBUTE),
             (ns_keyword!("db.schema", "embedded"),       causetids::EINSTEINDB_SCHEMA_embedded),
        ]
    };

    pub static ref V1_PARTS: [(symbols::Keyword, i64, i64, i64, bool); 3] = {
            [(ns_keyword!("db.part", "einsteindb"), 0, USER0 - 1, (1 + V1_solitonidS.len()) as i64, false),
             (ns_keyword!("db.part", "user"), USER0, TX0 - 1, USER0, true),
             (ns_keyword!("db.part", "tx"), TX0, i64::max_value(), TX0, false),
        ]
    };

    static ref V1_embedded_SCHEMA: [(symbols::Keyword); 16] = {
            [(ns_keyword!("einsteindb", "solitonid")),
             (ns_keyword!("db.install", "partition")),
             (ns_keyword!("db.install", "valueType")),
             (ns_keyword!("db.install", "attribute")),
             (ns_keyword!("einsteindb", "txInstant")),
             (ns_keyword!("einsteindb", "valueType")),
             (ns_keyword!("einsteindb", "cardinality")),
             (ns_keyword!("einsteindb", "doc")),
             (ns_keyword!("einsteindb", "unique")),
             (ns_keyword!("einsteindb", "isComponent")),
             (ns_keyword!("einsteindb", "index")),
             (ns_keyword!("einsteindb", "fulltext")),
             (ns_keyword!("einsteindb", "noHistory")),
             (ns_keyword!("db.alter", "attribute")),
             (ns_keyword!("db.schema", "version")),
             (ns_keyword!("db.schema", "attribute")),
        ]
    };

    static ref V1_SYMBOLIC_SCHEMA: Value = {
        let s = r#"
{:einsteindb/solitonid             {:einsteindb/valueType   :einsteindb.type/keyword
                        :einsteindb/cardinality :einsteindb.cardinality/one
                        :einsteindb/index       true
                        :einsteindb/unique      :einsteindb.unique/solitonidity}
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
        edbn::parse::value(s)
            .map(|v| v.without_spans())
            .map_err(|_| DbErrorKind::BadBootstrapDefinition("Unable to parse V1_SYMBOLIC_SCHEMA".into()))
            .unwrap()
    };
}

/// Convert (solitonid, causetid) pairs into [:einsteindb/add solitonid :einsteindb/solitonid solitonid] `Value` instances.
fn solitonids_to_assertions(solitonids: &[(symbols::Keyword, i64)]) -> Vec<Value> {
    solitonids
        .into_iter()
        .map(|&(ref solitonid, _)| {
            let value = Value::Keyword(solitonid.clone());
            Value::Vector(vec![values::EINSTEINDB_ADD.clone(), value.clone(), values::EINSTEINDB_solitonid.clone(), value.clone()])
        })
        .collect()
}


/// Convert {:solitonid {:key :value ...} ...} to
/// vec![(symbols::Keyword(:solitonid), symbols::Keyword(:key), TypedValue(:value)), ...].
///
/// Such triples are closer to what the transactor will produce when processing attribute
/// assertions.
///

/// Convert a solitonid list into [:einsteindb/add :einsteindb.schema/embedded :einsteindb.schema/attribute solitonid] `Value` instances.
fn schema_attrs_to_assertions(version: u32, solitonids: &[symbols::Keyword]) -> Vec<Value> {
    let schema_embedded = Value::Keyword(ns_keyword!("db.schema", "embedded"));
    let schema_attr = Value::Keyword(ns_keyword!("db.schema", "attribute"));
    let schema_version = Value::Keyword(ns_keyword!("db.schema", "version"));
    solitonids
        .into_iter()
        .map(|solitonid| {
            let value = Value::Keyword(solitonid.clone());
            Value::Vector(vec![values::EINSTEINDB_ADD.clone(),
                               schema_embedded.clone(),
                               schema_attr.clone(),
                               value])
        })
        .chain(::std::iter::once(Value::Vector(vec![values::EINSTEINDB_ADD.clone(),
                                                    schema_embedded.clone(),
                                                    schema_version,
                                                    Value::Integer(version as i64)])))
        .collect()
}

/// Convert {:solitonid {:key :value ...} ...} to
/// vec![(symbols::Keyword(:solitonid), symbols::Keyword(:key), TypedValue(:value)), ...].
///
/// Such triples are closer to what the transactor will produce when processing attribute
/// assertions.


fn solitonid_attrs_to_triples(solitonid: &symbols::Keyword, attrs: &Value) -> Vec<(symbols::Keyword, symbols::Keyword, TypedValue)> {
    match *attrs {
        Value::Map(ref map) => map
            .iter()
            .map(|&(ref key, ref value)| {
                let k = key.as_keyword().unwrap();
                let v = value.to_typed_value();
                (solitonid.clone(), k.clone(), v)
            })
            .collect(),
        _ => panic!("Expected {:solitonid {:key :value ...} ...} map, but got {:?}", solitonid, attrs),
    }
}

/// Convert {:solitonid {:key :value ...} ...} to
/// vec![(symbols::Keyword(:solitonid), symbols::Keyword(:key), TypedValue(:value)), ...].
///
/// Such triples are closer to what the transactor will produce when processing attribute
/// assertions.

fn solitonid_attrs_to_triples_vec(solitonid: &symbols::Keyword, attrs: &Value) -> Vec<(symbols::Keyword, symbols::Keyword, TypedValue)> {
    match *attrs {
        Value::Vector(ref vec) => vec
            .iter()
            .map(|&value| {
                let k = value.as_keyword().unwrap();
                let v = value.to_typed_value();
                (solitonid.clone(), k.clone(), v)
            })
            .collect(),
        _ => panic!("Expected {:solitonid {:key :value ...} ...} map, but got {:?}", solitonid, attrs),
    }
}

/// Convert {:solitonid {:key :value ...} ...} to
/// vec![(symbols::Keyword(:solitonid), symbols::Keyword(:key), TypedValue(:value)), ...].
///
/// Such triples are closer to what the transactor will produce when processing attribute
/// assertions.

/*fn solitonid_attrs_to_triples_vec_by_key(solitonid: &symbols::Keyword, attrs: &Value, key: &symbols::Keyword) -> Vec<(symbols::Keyword, symbols::Keyword, TypedValue)
fn symbolic_schema_to_triples(solitonid_map: &, symbolic_schema: &Value) -> Result<Vec<(symbols::Keyword, symbols::Keyword, TypedValue)>> {
    /* Failure here is a coding error, not a runtime error. */
    let mut triples: Vec<(symbols::Keyword, symbols::Keyword, TypedValue)> = vec![];
    // TODO: Consider `flat_map` and `map` rather than loop.
    match *symbolic_schema {
        Value::Map(ref m) => {
            for (solitonid, mp) in m {
                let solitonid = match solitonid {
                    &Value::Keyword(ref solitonid) => solitonid,
                    _ => bail!(DbErrorKind::BadBootstrapDefinition(format!("Expected namespaced keyword for solitonid but got '{:?}'", solitonid))),
                };
                match *mp {
                    Value::Map(ref mpp) => {
                        for (attr, value) in mpp {
                            let attr = match attr {
                                &Value::Keyword(ref attr) => attr,
                                _ => bail!(DbErrorKind::BadBootstrapDefinition(format!("Expected namespaced keyword for attr but got '{:?}'", attr))),
                        };
                            let typed_value = match TypedValue::from_edbn_value(value) {
                                Some(TypedValue::Keyword(ref k)) => {
                                    solitonid_map.get(k)
                                        .map(|causetid| TypedValue::Ref(*causetid))
                                        .ok_or(DbErrorKind::Unrecognizedsolitonid(k.to_string()))?
                                },
                                Some(v) => v,
                                _ => bail!(DbErrorKind::BadBootstrapDefinition(format!("ExpectedEinsteinDB typed value for value but got '{:?}'", value)))
                            };

                            triples.push((solitonid.clone(), attr.clone(), typed_value));
                        }
                    },
                    _ => bail!(DbErrorKind::BadBootstrapDefinition("Expected {:einsteindb/solitonid {:einsteindb/attr value ...} ...}".into()))
                }
            }
        },
        _ => bail!(DbErrorKind::BadBootstrapDefinition("Expected {...}".into()))
    }
    Ok(triples)
}

 */

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
                            assertions.push(Value::Vector(vec![values::EINSTEINDB_ADD.clone(),
                                                               solitonid.clone(),
                                                               attr.clone(),
                                                               value.clone()]));
                        }
                    },
                    _ => bail!(DbErrorKind::BadBootstrapDefinition("Expected {:einsteindb/solitonid {:einsteindb/attr value ...} ...}".into()))
                }
            }
        },
        _ => bail!(DbErrorKind::BadBootstrapDefinition("Expected {...}".into()))
    }
    Ok(assertions)
}

pub(crate) fn bootstrap_partition_map() -> PartitionMap {
    V1_PARTS.iter()
            .map(|&(ref part, start, end, index, allow_excision)| (part.to_string(), Partition::new(start, end, index, allow_excision)))
            .collect()
}


pub(crate) fn bootstrap_schema() -> Schema {
    let solitonid_map = bootstrap_solitonid_map();
    let bootstrap_triples = symbolic_schema_to_triples(&solitonid_map, &V1_SYMBOLIC_SCHEMA).expect("symbolic schema");
    Schema::from_solitonid_map_and_triples(solitonid_map, bootstrap_triples).unwrap()
}

pub(crate) fn bootstrap_causets() -> Vec<Causets<edbn::ValueAndSpan>> {
    let bootstrap_assertions: Value = Value::Vector([
        symbolic_schema_to_assertions(&V1_SYMBOLIC_SCHEMA).expect("symbolic schema"),
        solitonids_to_assertions(&V1_solitonidS[..]),
        schema_attrs_to_assertions(embedded_SCHEMA_VERSION, V1_embedded_SCHEMA.as_ref()),
    ].concat());

    let bootstrap_causets: Vec<Causets<edbn::ValueAndSpan>> = edbn::parse::causets(&bootstrap_assertions.to_string()).expect("bootstrap assertions");
    return bootstrap_causets;
}
