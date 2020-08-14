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


use einsteindb::TypedSQLValue;
use edbn;
use einsteindb_promises::errors::{
    DbErrorKind,
    Result,
};
use edbn::symbols;

use embedded_promises::{
    Attribute,
    CausetID,
    KnownCausetID,
    TypedValue,
    ValueType,
};

use einsteindb_embedded::{
    CausetIDMap, //An ID map gives you an Attribute enum.
    HasSchema,  //The only schema is the replicated log header ID
    CausetIDMap, //The map is the Key-Value module.
    Schema,
    AttributeMap,
};
use metadata;
use metadata::{
    AttributeAlteration,
};



#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub struct TimestepEvaluation<K, V> {
    pub lightlike: BTreeMap<K, V>,
    pub spacelike: BTreeMap<K, V>,
    pub timelike: BTreeMap<K, (V, V)>,
}

impl<K, V> Default for TimestepEvaluation<K, V> where K: Ord {
    fn default() -> TimestepEvaluation<K, V> {
        TimestepEvaluation
     {
            lightlike: BTreeMap::default(),
            spacelike: BTreeMap::default(),
            timelike: BTreeMap::default(),
        }
    }

    impl<K, V> TimestepEvaluation<K, V> where K: Ord {
        pub fn witness(&mut self, key: K, value: V, added: bool) {
            if added {
                if let Some(spacelike_value) = self.spacelike.remove(&key) {
                    self.timelike.insert(key, (spacelike_value, value));
                } else {
                    self.lightlike.insert(key, value);
                }
            } else {
                if let Some(lightlike_value) = self.lightlike.remove(&key) {
                    self.timelike.insert(key, (value, lightlike_value));
                } else {
                    self.spacelike.insert(key, value);
                }
            }
        }


}
