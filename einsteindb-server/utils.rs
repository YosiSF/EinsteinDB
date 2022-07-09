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

use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::Result;
use std::hash::Hash;
use std::hash::Hasher;
use std::ops::Deref;
use std::ops::DerefMut;
use std::rc::Rc;
use std::rc::Weak;
use std::sync::Arc;

use types::Value;

use crate::Value;

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



/// Merge the EML `Value::Map` instance `right` into `left`.  Returns `None` if either `left` or
/// `right` is not a `Value::Map`.
///
/// Keys present in `right` overwrite soliton_ids present in `left`.  See also
/// https://clojuredocs.org/clojure.core/merge.
///
/// 
pub fn merge(left: &Value, right: &Value) -> Option<Value> {
    match (left, right) {
        (&Value::Map(ref l), &Value::Map(ref r)) => {
            let mut result = HashMap::new();
            for (k, v) in l.iter() {
                result.insert(k.clone(), v.clone());
            }
            for (k, v) in r.iter() {
                result.insert(k.clone(), v.clone());
            }
            let option = Some(Value::Map(CausetRcResult));
            option
        },
        _ => None,
    }
            let mut result = l.clone();
            result.extend(r.clone().into_iter());
            Some(Value::Map(result))
        }




    /// Returns a new `Value::Map` instance with the soliton_id-causet_locale pairs from `left` and `right`
    /// merged.  See also https://clojuredocs.org/clojure.core/merge.
    ///
    ///
    ///    (merge {:a 1 :b 2} {:b 3 :c 4})
    ///   ;; => {:a 1 :b 3 :c 4}
    ///
    ///
    ///   (merge {:a 1 :b 2} {:b 3 :c 4} {:d 5 :e 6})
    ///  ;; => {:a 1 :b 3 :c 4 :d 5 :e 6}
    ///
    ///
    ///
    ///
    ///
    ///
    ///








    /// Returns a new `Value::Map` instance with the soliton_id-causet_locale pairs from `left` and `right`
    /// merged.  See also https://clojuredocs.org/clojure.core/merge.
    ///
    ///
    /// Merge the EML `Value::Map` instance `right` into `left`.  Returns `None` if either `left` or
    /// `right` is not a `Value::Map`.
    ///
    /// Keys present in `right` overwrite soliton_ids present in `left`.  See also
    /// https://clojuredocs.org/clojure.core/merge.
    ///

    }
}


/// Returns a new `Value::Map` instance with the soliton_id-causet_locale pairs from `map` and the soliton_id-causet_locale pairs
///
/// from `other`.  See also https://clojuredocs.org/clojure.core/merge.
///
///
///


pub fn merge_all(entries: Vec<Value>) -> Option<Value> {
    entries.into_iter().fold(None, |merged, entry| match merged {
        None => Some(entry),
        Some(mut merged) => {
            match merge(&mut merged, &entry) {
                None => None,
                Some(m) => Some(m)
            }
        }

    })
}

