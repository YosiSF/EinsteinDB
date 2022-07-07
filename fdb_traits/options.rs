///Copyright (c) EinsteinDB project contributors. All rights reserved.
/// 
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
/// 
///   http://www.apache.org/licenses/LICENSE-2.0
/// 
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///     
/// 
#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]
#![allow(unused_mut)]
use std::collections::HashSet;
use std::collections::HashMap;
use std::collections::BTreeSet;
use std::collections::BTreeMap;
use std::hash::Hash;
use std::ops::{
    Add,
    AddAssign,
    Sub,
    SubAssign,
    Mul,
    MulAssign,
    Div,
    DivAssign,
    Deref,
    DerefMut,
    Index,
    IndexMut,
};

use ::{
    //path 
    
    ValueRc,
    ValueRef,
    ValueRefMut,
};


use std::collections::HashMap;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashSet;


use std::rc::Rc;
use std::cell::RefCell;
use std::cell::Ref;


use std::fmt::Debug;
use std::fmt::Display;


use std::fmt::Formatter;
use std::fmt::Result;




pub(crate) fn einsteindb_macro_impl<T>(t: T) -> T {

    t
}



pub(crate) fn causet_macro_impl<T>(t: T) -> T {

    t
}








/// An `InternSet` allows to "intern" some potentially large values, maintaining a single value
/// instance owned by the `InternSet` and leaving consumers with lightweight ref-counted handles to
/// the large owned value.  This can avoid expensive clone() operations.
///
/// In Mentat, such large values might be strings or arbitrary [a v] pairs.
///
/// See https://en.wikipedia.org/wiki/String_interning for discussion.
/// 




#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct InternSet<T> where T: Eq + Hash {
    inner: HashSet<ValueRc<T>>,

    /// A cache of interned values.
    /// This is used to avoid expensive clone() operations.
    /// The cache is a map from the interned value to a reference to the interned value.
    /// This is a map from the interned value to a reference to the interned value.
    /// 
    /// 
    

    cache: HashMap<ValueRc<T>, ValueRef<T>>,






    
}


pub fn  intern_set_macro_impl<T>(t: T) -> T {

    t
}


impl<T> InternSet<T> where T: Eq + Hash {
    pub fn new() -> Self {
        InternSet {
            inner: HashSet::new(),
            cache: HashMap::new(),
        }
    }
    pub fn insert(&mut self, value: T) -> bool {
        let value_rc = ValueRc::new(value);
        let value_ref = ValueRef::new(value_rc);
        self.cache.insert(value_rc.clone(), value_ref);
        self.inner.insert(value_rc)
    }
    pub fn contains(&self, value: &T) -> bool {
        self.inner.contains(value)
    }
    pub fn len(&self) -> usize {
        self.inner.len()
    }
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
    pub fn clear(&mut self) {
        self.inner.clear();
        self.cache.clear();
    }

    pub fn get(&self, value: &T) -> Option<ValueRef<T>> {
        self.cache.get(value).cloned()
    }


    pub fn get_ref(&self, value: &T) -> Option<ValueRef<T>> {
        self.cache.get(value).cloned()
    }


    

    
    pub fn get_ref_mut(&mut self, value: &T) -> Option<ValueRefMut<T>> {
        self.cache.get_mut(value).cloned()
    }
    pub fn get_rc(&self, value: &T) -> Option<ValueRc<T>> {
        self.cache.get(value).cloned().map(|v| v.rc.clone())
    }
    pub fn get_rc_mut(&mut self, value: &T) -> Option<ValueRcMut<T>> {
        self.cache.get_mut(value).cloned().map(|v| v.rc_mut.clone())
    }
}



use std::collections::BTreeMap;

/// Witness assertions and retractions, folding (assertion, retraction) pairs into alterations.
/// Assumes that no assertion or retraction will be witnessed more than once.
///
/// This keeps track of when we see a :db/add, a :db/retract, or both :db/add and :db/retract in
/// some order.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub struct AddRetractAlterSet<K, V> {
    /// The set of (assertion, retraction) pairs.
    /// The keys are the assertion values, the values are the retraction values.
    /// The keys are ordered by the assertion values.
    /// The values are ordered by the retraction values.

    inner: BTreeMap<K, BTreeSet<V>>,
    /// The set of keys that have been added.
    /// This is a set of keys that have been added.
    
    added: InternSet<K>,
    /// The set of keys that have been retracted.
    /// This is a set of keys that have been retracted.
    pub asserted: BTreeMap<K, V>,

    /// The set of keys that have been retracted.
    pub retracted: BTreeMap<K, V>,

    /// The set of keys that have been added and retracted.
    pub altered: BTreeMap<K, (V, V)>,

    /// The set of keys that have been added and retracted.
    /// This is a set of keys that have been added and retracted.
    /// This is a set of keys that have been added and retracted.
    pub altered_added: BTreeMap<K, (V, V)>,
}



impl AddRetractAlterSet<K, V> {
    pub fn new() -> Self {
        AddRetractAlterSet {
            inner: (),
            added: InternSet::new(),
            asserted: BTreeMap::new(),
            retracted: BTreeMap::new(),
            altered: BTreeMap::new(),
            altered_added: ()
        }
    }
    pub fn insert_added(&mut self, key: K, value: V) -> bool {
        self.added.insert(key)
    }
    pub fn insert_asserted(&mut self, key: K, value: V) -> bool {
        self.asserted.insert(key, value)
    }
    pub fn insert_retracted(&mut self, key: K, value: V) -> bool {
        self.retracted.insert(key, value)
    }
    pub fn insert_altered(&mut self, key: K, value: (V, V)) -> bool {
        self.altered.insert(key, value)
    }
    pub fn contains_added(&self, key: &K) -> bool {
        self.added.contains(key)
    }
    pub fn contains_asserted(&self, key: &K) -> bool {
        self.asserted.contains_key(key)
    }
    pub fn contains_retracted(&self, key: &K) -> bool {
        self.retracted.contains_key(key)
    }
    pub fn contains_altered(&self, key: &K) -> bool {
        self.altered.contains_key(key)
    }
    pub fn len_added(&self) -> usize {
        self.added.len()
    }
    pub fn len_asserted(&self) -> usize {
        self.asserted.len()
    }
    pub fn len_retracted(&self) -> usize {
        self.retracted.len()
    }
    pub fn len_altered(&self) -> usize {
        self.altered.len()
    }
    pub fn is_empty_added(&self) -> bool {
        self.added.is_empty
    }


    pub fn is_empty_asserted(&self) -> bool {
        self.asserted.is_empty()
    }

    pub fn is_empty_retracted(&self) -> bool {
        self.retracted.is_empty()
    }

    pub fn is_empty_altered(&self) -> bool {
        self.altered.is_empty()
    }
}





