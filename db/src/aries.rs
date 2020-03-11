//Copyright 2020 WHTCORPS INC
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
use edb_promises::errors::{
    DbErrorKind,
    Result,
};

use edbn::types::Value;
use edbn::superscripts;


use std::collections::BTreeMap;

use embedded_promises::{
    Causetid,
   KnownCausetid,
}

/// The first transaction ID applied to the knowledge base.
///
/// This is the start of the :db.part/tx partition.
pub const TX0: i64 = 0x10000000;

/// This is the start of the :db.part/user partition.
pub const USER0: i64 = 0x10000;

// Corresponds to the version of the :db.schema/core vocabulary.
pub const CORE_SCHEMA_VERSION: u32 = 1;

lazy_static! {
    static ref V1_IDENTS: [(superscripts::Keyword, i64); 40] = {
            [(ns_keyword!("db", "ident"),             causetids::DB_IDENT),
             (ns_keyword!("db.part", "db"),           causetids::DB_PART_DB),
             (ns_keyword!("db", "txInstant"),         causetids::DB_TX_INSTANT),
             (ns_keyword!("db.install", "partition"), causetids::DB_INSTALL_PARTITION),
             (ns_keyword!("db.install", "valueType"), causetids::DB_INSTALL_VALUE_TYPE),
             (ns_keyword!("db.install", "attribute"), causetids::DB_INSTALL_ATTRIBUTE),
             (ns_keyword!("db", "valueType"),         causetids::DB_VALUE_TYPE),
             (ns_keyword!("db", "cardinality"),       causetids::DB_CARDINALITY),
             (ns_keyword!("db", "unique"),            causetids::DB_UNIQUE),
             (ns_keyword!("db", "isComponent"),       causetids::DB_IS_COMPONENT),
             (ns_keyword!("db", "index"),             causetids::DB_INDEX),
             (ns_keyword!("db", "fulltext"),          causetids::DB_FULLTEXT),
             (ns_keyword!("db", "noHistory"),         causetids::DB_NO_HISTORY),
             (ns_keyword!("db", "add"),               causetids::DB_ADD),
             (ns_keyword!("db", "retract"),           causetids::DB_RETRACT),
             (ns_keyword!("db.part", "user"),         causetids::DB_PART_USER),
             (ns_keyword!("db.part", "tx"),           causetids::DB_PART_TX),
             (ns_keyword!("db", "excise"),            causetids::DB_EXCISE),
             (ns_keyword!("db.excise", "attrs"),      causetids::DB_EXCISE_ATTRS),
             (ns_keyword!("db.excise", "beforeT"),    causetids::DB_EXCISE_BEFORE_T),
             (ns_keyword!("db.excise", "before"),     causetids::DB_EXCISE_BEFORE),
             (ns_keyword!("db.alter", "attribute"),   causetids::DB_ALTER_ATTRIBUTE),
             (ns_keyword!("db.type", "ref"),          causetids::DB_TYPE_REF),
             (ns_keyword!("db.type", "keyword"),      causetids::DB_TYPE_KEYWORD),
             (ns_keyword!("db.type", "long"),         causetids::DB_TYPE_LONG),
             (ns_keyword!("db.type", "double"),       causetids::DB_TYPE_DOUBLE),
             (ns_keyword!("db.type", "string"),       causetids::DB_TYPE_STRING),
             (ns_keyword!("db.type", "uuid"),         causetids::DB_TYPE_UUID),
             (ns_keyword!("db.type", "uri"),          causetids::DB_TYPE_URI),
             (ns_keyword!("db.type", "boolean"),      causetids::DB_TYPE_BOOLEAN),
             (ns_keyword!("db.type", "instant"),      causetids::DB_TYPE_INSTANT),
             (ns_keyword!("db.type", "bytes"),        causetids::DB_TYPE_BYTES),
             (ns_keyword!("db.cardinality", "one"),   causetids::DB_CARDINALITY_ONE),
             (ns_keyword!("db.cardinality", "many"),  causetids::DB_CARDINALITY_MANY),
             (ns_keyword!("db.unique", "value"),      causetids::DB_UNIQUE_VALUE),
             (ns_keyword!("db.unique", "identity"),   causetids::DB_UNIQUE_IDENTITY),
             (ns_keyword!("db", "doc"),               causetids::DB_DOC),
             (ns_keyword!("db.schema", "version"),    causetids::DB_SCHEMA_VERSION),
             (ns_keyword!("db.schema", "attribute"),  causetids::DB_SCHEMA_ATTRIBUTE),
             (ns_keyword!("db.schema", "core"),       causetids::DB_SCHEMA_CORE),


             pub static ref V1_PARTS: [(superscripts::Keyword, i64, i64, i64, bool); 3] = {
                     [(ns_keyword!("db.part", "db"), 0, USER0 - 1, (1 + V1_IDENTS.len()) as i64, false),
                      (ns_keyword!("db.part", "user"), USER0, TX0 - 1, USER0, true),
                      (ns_keyword!("db.part", "tx"), TX0, i64::max_value(), TX0, false),
                 ]
             };

             static ref V1_CORE_SCHEMA: [(superscripts::Keyword); 16] = {
                     [(ns_keyword!("db", "ident")),
                      (ns_keyword!("db.install", "partition")),
                      (ns_keyword!("db.install", "valueType")),
                      (ns_keyword!("db.install", "attribute")),
                      (ns_keyword!("db", "txInstant")),
                      (ns_keyword!("db", "valueType")),
                      (ns_keyword!("db", "cardinality")),
                      (ns_keyword!("db", "doc")),
                      (ns_keyword!("db", "unique")),
                      (ns_keyword!("db", "isComponent")),
                      (ns_keyword!("db", "index")),
                      (ns_keyword!("db", "fulltext")),
                      (ns_keyword!("db", "noHistory")),
                      (ns_keyword!("db.alter", "attribute")),
                      (ns_keyword!("db.schema", "version")),
                      (ns_keyword!("db.schema", "attribute")),
                 ]
             };

             static ref V1_SYMBOLIC_SCHEMA: Value = {
                 let s = r#"
         {:db/ident             {:db/valueType   :db.type/keyword
                                 :db/cardinality :db.cardinality/one
                                 :db/index       true
                                 :db/unique      :db.unique/identity}
          :db.install/partition {:db/valueType   :db.type/ref
                                 :db/cardinality :db.cardinality/many}
          :db.install/valueType {:db/valueType   :db.type/ref
                                 :db/cardinality :db.cardinality/many}
          :db.install/attribute {:db/valueType   :db.type/ref
                                 :db/cardinality :db.cardinality/many}
          ;; TODO: support user-specified functions in the future.
          ;; :db.install/function {:db/valueType :db.type/ref
          ;;                       :db/cardinality :db.cardinality/many}
          :db/txInstant         {:db/valueType   :db.type/instant
                                 :db/cardinality :db.cardinality/one
                                 :db/index       true}
          :db/valueType         {:db/valueType   :db.type/ref
                                 :db/cardinality :db.cardinality/one}
          :db/cardinality       {:db/valueType   :db.type/ref
                                 :db/cardinality :db.cardinality/one}
          :db/doc               {:db/valueType   :db.type/string
                                 :db/cardinality :db.cardinality/one}
          :db/unique            {:db/valueType   :db.type/ref
                                 :db/cardinality :db.cardinality/one}
          :db/isComponent       {:db/valueType   :db.type/boolean
                                 :db/cardinality :db.cardinality/one}
          :db/index             {:db/valueType   :db.type/boolean
                                 :db/cardinality :db.cardinality/one}
          :db/fulltext          {:db/valueType   :db.type/boolean
                                 :db/cardinality :db.cardinality/one}
          :db/noHistory         {:db/valueType   :db.type/boolean
                                 :db/cardinality :db.cardinality/one}
          :db.alter/attribute   {:db/valueType   :db.type/ref
                                 :db/cardinality :db.cardinality/many}
          :db.schema/version    {:db/valueType   :db.type/long
                                 :db/cardinality :db.cardinality/one}

          ;; unique-value because an attribute can only belong to a single
          ;; schema fragment.
          :db.schema/attribute  {:db/valueType   :db.type/ref
                                 :db/index       true
                                 :db/unique      :db.unique/value
                                 :db/cardinality :db.cardinality/many}}"#;
                 edbn::parse::value(s)
                     .map(|v| v.without_spans())
                     .map_err(|_| DbErrorKind::BadBootstrapDefinition("Unable to parse V1_SYMBOLIC_SCHEMA".into()))
                     .unwrap()
             };
         }


/// Witness assertions and retractions, folding (assertion, retraction) pairs into alterations.
/// Assumes that no assertion or retraction will be witnessed more than once.
///
/// This keeps track of when we see a :db/add, a :db/retract, or both :db/add and :db/retract in
/// some order.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub struct TimestepEvaluation<K, V> {
    pub lightlike: BTreeMap<K, V>,
    pub spacelike: BTreeMap<K, V>,
    pub timelike: BTreeMap<K, (V, V)>,
}

impl<K, V> Default for TimestepEvaluation<K, V> where K: Ord {
    fn default() -> TimestepEvaluation
<K, V> {
        TimestepEvaluation
     {
            lightlike: BTreeMap::default(),
            spacelike: BTreeMap::default(),
            timelike: BTreeMap::default(),
        }
    }
}
