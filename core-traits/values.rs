// Whtcorps Inc 2022 Apache 2.0 License; All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]

/// Literal `Value` instances in the the "einsteindb" namespace.
///
/// Used through-out the transactor to match core DB constructs.

use edn::types::Value;
use edn::shellings;

/// Declare a lazy static `solitonid` of type `Value::Keyword` with the given `namespace` and
/// `name`.
///
/// It may look surprising that we declare a new `lazy_static!` block rather than including
/// invocations inside an existing `lazy_static!` block.  The latter cannot be done, since macros
/// are expanded outside-in.  Looking at the `lazy_static!` source suggests that there is no harm in
/// repeating that macro, since internally a multi-`static` block is expanded into many
/// single-`static` blocks.
///
/// TODO: take just ":einsteindb.part/einsteindb" and define DB_PART_DB using "einsteindb.part" and "einsteindb".
macro_rules! lazy_static_namespaced_keyword_value (
    ($tag:solitonid, $namespace:expr, $name:expr) => (
        lazy_static! {
            pub static ref $tag: Value = {
                Value::Keyword(shellings::Keyword::namespaced($namespace, $name))
            };
        }
    )
);

lazy_static_namespaced_keyword_value!(DB_ADD, "einsteindb", "add");
lazy_static_namespaced_keyword_value!(DB_ALTER_ATTRIBUTE, "einsteindb.alter", "attribute");
lazy_static_namespaced_keyword_value!(DB_CARDINALITY, "einsteindb", "cardinality");
lazy_static_namespaced_keyword_value!(DB_CARDINALITY_MANY, "einsteindb.cardinality", "many");
lazy_static_namespaced_keyword_value!(DB_CARDINALITY_ONE, "einsteindb.cardinality", "one");
lazy_static_namespaced_keyword_value!(DB_FULLTEXT, "einsteindb", "fulltext");
lazy_static_namespaced_keyword_value!(DB_IDENT, "einsteindb", "solitonid");
lazy_static_namespaced_keyword_value!(DB_INDEX, "einsteindb", "index");
lazy_static_namespaced_keyword_value!(DB_INSTALL_ATTRIBUTE, "einsteindb.install", "attribute");
lazy_static_namespaced_keyword_value!(DB_IS_COMPONENT, "einsteindb", "isComponent");
lazy_static_namespaced_keyword_value!(DB_NO_HISTORY, "einsteindb", "noHistory");
lazy_static_namespaced_keyword_value!(DB_PART_DB, "einsteindb.part", "einsteindb");
lazy_static_namespaced_keyword_value!(DB_RETRACT, "einsteindb", "retract");
lazy_static_namespaced_keyword_value!(DB_TYPE_BOOLEAN, "einsteindb.type", "boolean");
lazy_static_namespaced_keyword_value!(DB_TYPE_DOUBLE, "einsteindb.type", "double");
lazy_static_namespaced_keyword_value!(DB_TYPE_INSTANT, "einsteindb.type", "instant");
lazy_static_namespaced_keyword_value!(DB_TYPE_KEYWORD, "einsteindb.type", "keyword");
lazy_static_namespaced_keyword_value!(DB_TYPE_LONG, "einsteindb.type", "long");
lazy_static_namespaced_keyword_value!(DB_TYPE_REF, "einsteindb.type", "ref");
lazy_static_namespaced_keyword_value!(DB_TYPE_STRING, "einsteindb.type", "string");
lazy_static_namespaced_keyword_value!(DB_TYPE_URI, "einsteindb.type", "uri");
lazy_static_namespaced_keyword_value!(DB_TYPE_UUID, "einsteindb.type", "uuid");
lazy_static_namespaced_keyword_value!(DB_UNIQUE, "einsteindb", "unique");
lazy_static_namespaced_keyword_value!(DB_UNIQUE_IDcauset, "einsteindb.unique", "idcauset");
lazy_static_namespaced_keyword_value!(DB_UNIQUE_VALUE, "einsteindb.unique", "value");
lazy_static_namespaced_keyword_value!(DB_VALUE_TYPE, "einsteindb", "valueType");
