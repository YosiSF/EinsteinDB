//Copyright 2021 WHTCORPS INC
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
}

impl<K, V> TimestepEvaluation<K, V> where K: Ord {
    pub fn add_lightlike(&mut self, key: K, value: V) {
        self.lightlike.insert(key, value);
    }

    pub fn remove_lightlike(&mut self, key: &K) -> Option<V> {
        self.lightlike.remove(key)
    }

    pub fn get_lightlike(&self, key: &K) -> Option<&V> {
        self.lightlike.get(key)
    }

    pub fn remove_spacelike(&mut self, key: &K) -> Option<V> {
        self.spacelike.remove(key)  //returns None if the key is not present in the map (einsteindb), this does not mean that we can have a empty map!

    }

    pub fn add_spacelike(&mut self, key: K, value: V) { //if the item exists we replace it with a new one (we dont want a set here because we are only adding single items to the map.) We need to update spacelike and timelike here!
        let _ = self.spacelike.insert(key,value); //add an element if it doesnt exist and return true or false if it did exist before... also returns old value!
        //
        // /* if let Some((t
        // /* if let Some((timestamp2)) = spacelike[1].0{} */ //then check against timestamp2...
        // they should be different!!!
        //
        // so we can simply add them!!! even though its a set data structure it will not allow duplicates!! :) This will work for any number of elements in our case spacialist and lightlist are both sets which means that duplicates cannot be added - thats good for us!! :) :) :) :) :) :) imestamp1)) = spacelike[0].0{} */ //take out first tuple... and assign its 0 to timestamp1... then check against current timestamp1 in spacelike[0] index! is there any other values??? yes see below...


