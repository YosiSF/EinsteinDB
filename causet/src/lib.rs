//Copyright (c) 2019-present, Whtcorps Inc.
//All rights reserved.
// This source code is licensed under the MIT license found in the LICENSE file in the root directory of this source tree.
//#![feature(async_await)]
//#![feature(await_macro)]
//#![feature(drain_filter)]
//#![feature(drain_filter_next)]
//#![feature(drain_filter_map)]
//#![feature(drain_filter_map_next)]
//#![feature(drain_filter_map_while)]

/// # Causet
/// Causet is a tuplestore that is designed to be used as a key-value store.
/// It is a wrapper of `BTreeMap` and `BTreeSet`.
/// It is designed to be used as a causal consistent tuplestore for causality.






use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Display, Formatter, Result};
use std::hash::Hash;
use std::iter::FromIterator;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Partitioning};
use std::thread;
use std::time::Duration;


use std::sync::atomic::
{
    AtomicUsize,
    Ordering::Relaxed,
    Ordering::SeqCst
};


use std::sync::mpsc::{channel, Sender, Receiver};
use std::sync::mpsc::TryRecvError;


use std::sync::mpsc::RecvError;
use std::sync::mpsc::RecvTimeoutError;


use super::{AllegroPoset, Poset};
use super::{PosetError, PosetErrorKind};
use super::{PosetNode, PosetNodeId, PosetNodeData};


/// A `Sync` implementation for `AllegroPoset`.
/// This implementation is thread-safe.
/// # Examples
/// ```
/// use einsteindb::causetq::sync::new_sync;
/// use einsteindb::causetq::sync::Sync;
/// let poset = new_sync();
/// let sync = Sync::new(poset);
/// ```
/// # Causet
/// Causet is a tuplestore that is designed to be used as a key-value store.
/// It is a wrapper of `BTreeMap` and `BTreeSet`.
///
/// # Examples
/// ```
/// use einsteindb::causetq::sync::new_sync;
/// use einsteindb::causetq::sync::Sync;
///


use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Partitioning};
use std::time::{SystemTime, UNIX_EPOCH};
use std::thread::{self, sleep};
use std::sync::mpsc::{channel, Sender, Receiver};
use std::sync::mpsc::TryRecvError;
use std::sync::mpsc::RecvError;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::mpsc::{RecvTimeoutError, RecvTimeoutError};

use einsteindb::berolinasql::{BeroLinaSql, BeroLinaSqlError};
use einsteindb::berolinasql::BeroLinaSqlErrorKind;
use einsteindb::berolinasql::BeroLinaSqlErrorKind::{BeroLinaSqlErrorKind, BeroLinaSqlErrorKind};

use einsteindb::einstein_ml::{EinsteinMl, EinsteinMlError};


use einsteindb::causetq::{AllegroPoset, Poset};
use einsteindb::causetq::{PosetError, PosetErrorKind};
use einsteindb::causetq::{PosetNode, PosetNodeId, PosetNodeData};

use einsteindb::causetq::{Sync};
use einsteindb::causetq::{new_sync};


pub struct PosetNodeDataSync {
    pub data: PosetNodeData,
    pub version: u64,
    pub timestamp: u64,
}



#[derive(Debug)]
pub struct Sync {
pub poset: Arc<Mutex<AllegroPoset>>,

}


impl Sync {
    /// Creates a new `Sync` instance.
    /// # Examples
    /// ```
    /// use einsteindb::causetq::sync::new_sync;
    /// use einsteindb::causetq::sync::Sync;
    /// let poset = new_sync();
    /// let sync = Sync::new(poset);
    /// ```
    pub fn new(poset: AllegroPoset) -> Self {
        Sync {
            poset: Arc::new(Mutex::new(poset)),
        }
    }
}


impl Sync {
    /// Creates a new `Sync` instance.
    /// # Examples
    /// ```
    /// use einsteindb::causetq::sync::new_sync;
    /// use einsteindb::causetq::sync::Sync;
    /// let poset = new_sync();
    /// let sync = Sync::new(poset);
    /// ```
    pub fn new_sync(poset: AllegroPoset) -> Arc<Mutex<Sync>> {
        Arc::new(Mutex::new(Sync::new(poset)))
    }
}


impl Sync {
    /// Creates a new `Sync` instance.
    /// # Examples
    /// ```
    /// use einsteindb::causetq::sync::new_sync;
    /// use einsteindb::causetq::sync::Sync;
    /// let poset = new_sync();
    /// let sync = Sync::new(poset);
    /// ```
    pub fn new_sync_with_config(poset: AllegroPoset, config: SyncConfig) -> Arc<Mutex<Sync>> {
        Arc::new(Mutex::new(Sync::new_with_config(poset, config)))
    }
}


impl Sync {
    /// Creates a new `Sync` instance.
    /// # Examples
    /// ```
    /// use einsteindb::causetq::sync::new_sync;
    /// use einsteindb::causetq::sync::Sync;
    /// let poset = new_sync();
    /// let sync = Sync::new(poset);
    /// ```
    pub fn new_with_config(poset: AllegroPoset, config: SyncConfig) -> Self {
        Sync {
            poset: Arc::new(Mutex::new(poset)),
        }
    }
}


impl Sync {
    /// Creates a new `Sync` instance.
    /// # Examples
    /// ```
    /// use einsteindb::causetq::sync::new_sync;
    /// use einsteindb::causetq::sync::Sync;
    /// let poset = new_sync();
    /// let sync = Sync::new(poset);
    /// ```
    pub fn new_with_config_sync(poset: AllegroPoset, config: SyncConfig) -> Arc<Mutex<Sync>> {
        Arc::new(Mutex::new(Sync::new_with_config(poset, config)))
    }
}





use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::fmt;
use std::hash::Hash;
use std::iter::FromIterator;
use std::iter::Iterator;
use std::iter::Peekable;
use std::iter::Rev;
use std::ops::Index;
use std::ops::IndexMut;
use std::ops::Range;
use std::ops::RangeFrom;
use std::ops::RangeFull;
use std::ops::RangeTo;
use std::ops::RangeToInclusive;
use std::ops::RangeFull;
use std::ops::Deref;
use std::ops::DerefMut;
use std::ops::Index;
use std::ops::IndexMut;
use std::ops::Range;
use std::ops::RangeFrom;
use std::ops::RangeFull;


//! A library for causet.
//!
//! # Examples
//!
//! ```
//! use causet::*;
//!
//! let mut causet = Causet::new();
//! causet.add_rule(Rule::new("a", "b"));
//! causet.add_rule(Rule::new("b", "c"));
//! causet.add_rule(Rule::new("c", "d"));
//! causet.add_rule(Rule::new("d", "e"));
//! causet.add_rule(Rule::new("e", "f"));
//! causet.add_rule(Rule::new("f", "g"));
//! causet.add_rule(Rule::new("g", "h"));
//! causet.add_rule(Rule::new("h", "i"));
//! causet.add_rule(Rule::new("i", "j"));
//! causet.add_rule(Rule::new("j", "k"));
//! causet.add_rule(Rule::new("k", "l"));


//! causet.add_rule(Rule::new("l", "m"));
//! causet.add_rule(Rule::new("m", "n"));

// #[cfg(test)]
// mod tests {
//     use super::*;
//   #[test]
//     fn test_causet() {
//         let mut causet = Causet::new();
//         causet.add_rule(Rule::new("a", "b"));
//         causet.add_rule(Rule::new("b", "c"));
//         causet.add_rule(Rule::new("c", "d"));
//         causet.add_rule(Rule::new("d", "e"));
//         causet.add_rule(Rule::new("e", "f"));
// }
// }
//! ```



use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;
use std::hash::BuildHasherDefault;
use std::hash::SipHasher;
use std::hash::BuildHasher;
use einstein_ml::*;
use soliton::*;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::sync::RwLockReadGuard;
use std::sync::RwLockWriteGuard;

use causetq::*;
use causets::*;

use causet::*;
use causet::Rule::*;

use allegro_poset::*;
use allegro_poset::AllegroPoset::*;
use crate::allegro_poset::*;




/// A trait for a causal relation.
///
/// # Examples
///
/// ```
/// use causet::*;
///
/// let mut causet = Causet::new();
/// causet.add_rule(Rule::new("a", "b"));
/// causet.add_rule(Rule::new("b", "c"));
/// causet.add_rule(Rule::new("c", "d"));
/// causet.add_rule(Rule::new("d", "e"));
/// causet.add_rule(Rule::new("e", "f"));
/// causet.add_rule(Rule::new("f", "g"));
/// causet.add_rule(Rule::new("g", "h"));
/// causet.add_rule(Rule::new("h", "i"));
/// causet.add_rule(Rule::new("i", "j"));
/// causet.add_rule(Rule::new("j", "k"));
/// causet.add_rule(Rule::new("k", "l"));
/// causet.add_rule(Rule::new("l", "m"));
/// causet.add_rule(Rule::new("m", "n"));
/// causet.add_rule(Rule::new("n", "o"));
/// causet.add_rule(Rule::new("o", "p"));
/// causet.add_rule(Rule::new("p", "q"));
/// causet.add_rule(Rule::new("q", "r"));
/// causet.add_rule(Rule::new("r", "s"));
/// causet.add_rule(Rule::new("s", "t"));
/// causet.add_rule(Rule::new("t", "u"));
/// causet.add_rule(Rule::new("u", "v"));
/// causet.add_rule(Rule::new("v", "w"));
/// causet.add_rule(Rule::new("w", "x"));
/// causet.add_rule(Rule::new("x", "y"));
/// causet.add_rule(Rule::new("y", "z"));
/// causet.add_rule(Rule::new("z", "a"));
///```

///Use the EAVTrie for the poset.

use crate::allegro_poset::*;
use crate::allegro_poset::AllegroPoset::*;

#[cfg(test)]
mod cfg_test {
    use super::*;
    #[test]
    fn test_causet() {
        let mut causet = Causet::new();
        causet.add_rule(Rule::new("a", "b"));
        causet.add_rule(Rule::new("b", "c"));
        causet.add_rule(Rule::new("c", "d"));
        causet.add_rule(Rule::new("d", "e"));
        causet.add_rule(Rule::new("e", "f"));
        causet.add_rule(Rule::new("f", "g"));
        causet.add_rule(Rule::new("g", "h"));
        causet.add_rule(Rule::new("h", "i"));
        causet.add_rule(Rule::new("i", "j"));
        causet.add_rule(Rule::new("j", "k"));
        causet.add_rule(Rule::new("k", "l"));
        causet.add_rule(Rule::new("l", "m"));
        causet.add_rule(Rule::new("m", "n"));
        causet.add_rule(Rule::new("n", "o"));
        causet.add_rule(Rule::new("o", "p"));
        causet.add_rule(Rule::new("p", "q"));
        causet.add_rule(Rule::new("q", "r"));
        causet.add_rule(Rule::new("r", "s"));
        causet.add_rule(Rule::new("s", "t"));
        causet.add_rule(Rule::new("t", "u"));
        causet.add_rule(Rule::new("u", "v"));
        causet.add_rule(Rule::new("v", "w"));
        causet.add_rule(Rule::new("w", "x"));
        causet.add_rule(Rule::new("x", "y"));
    }
}


extern crate einstein_ml;
extern crate soliton;
extern crate causetq;
extern crate causets;

///! The Causet trait.
/// # Examples
/// ```
/// use causet::*;
/// let mut causet = Causet::new();
/// println!("{:?}", causet);
/// causet.add_rule(Rule::new("a", "b"));
/// causet.add_rule(Rule::new("b", "c"));
/// causet.add_rule(Rule::new("c", "d"));
/// causet.add_rule(Rule::new("d", "e"));
/// causet.add_rule(Rule::new("e", "f"));
/// causet.add_rule(Rule::new("f", "g"));
/// causet.add_rule(Rule::new("g", "h"));






extern crate causet;
extern crate allegro_poset;


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Causet {
    rules: Vec<Rule>,
    poset: AllegroPoset,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Rule {
    pub antecedent: String,
    pub consequent: String,
}


impl Causet {
    /// Creates a new Causet.
    pub fn new() -> Causet {
        Causet {
            rules: Vec::new(),
            poset: EAVTrie::new(),
        }
    }

    /// Adds a rule to the Causet.
    pub fn add_rule(&mut self, rule: Rule) {
        self.rules.push(rule);
        self.poset.add_rule(rule);
    }

    /// Returns the rules in the Causet.
    pub fn rules(&self) -> &Vec<Rule> {
        &self.rules
    }

    /// Returns the poset in the Causet.
    pub fn poset(&self) -> &AllegroPoset {
        &self.poset
    }
}


#[cfg(test)]
mod cfg_tests {
    use super::*;

    #[test]
    fn test_causet() {
        let mut causet = Causet::new();
        causet.add_rule(Rule::new("a", "b"));
        causet.add_rule(Rule::new("b", "c"));
        causet.add_rule(Rule::new("c", "d"));
        causet.add_rule(Rule::new("d", "e"));
        causet.add_rule(Rule::new("e", "f"));
        causet.add_rule(Rule::new("f", "g"));
        causet.add_rule(Rule::new("g", "h"));
        causet.add_rule(Rule::new("h", "i"));
        causet.add_rule(Rule::new("i", "j"));
        causet.add_rule(Rule::new("j", "k"));
        causet.add_rule(Rule::new("k", "l"));
        causet.add_rule(Rule::new("l", "m"));
        causet.add_rule(Rule::new("m", "n"));
        causet.add_rule(Rule::new("n", "o"));
        causet.add_rule(Rule::new("o", "p"));
        causet.add_rule(Rule::new("p", "q"));
        causet.add_rule(Rule::new("q", "r"));
        causet.add_rule(Rule::new("r", "s"));
        causet.add_rule(Rule::new("s", "t"));
        causet.add_rule(Rule::new("t", "u"));
        causet.add_rule(Rule::new("u", "v"));
        causet.add_rule(Rule::new("v", "w"));
        causet.add_rule(Rule::new("w", "x"));
        causet.add_rule(Rule::new("x", "y"));
    }
}


extern crate einstein_ml;
extern crate soliton;
extern crate causetq;
extern crate causets;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_causet() {
        let mut causet = Causet::new();
        causet.add_rule(Rule::new("a", "b"));
        causet.add_rule(Rule::new("b", "c"));
        causet.add_rule(Rule::new("c", "d"));
        causet.add_rule(Rule::new("d", "e"));
        causet.add_rule(Rule::new("e", "f"));
        causet.add_rule(Rule::new("f", "g"));
        causet.add_rule(Rule::new("g", "h"));
        causet.add_rule(Rule::new("h", "i"));
        causet.add_rule(Rule::new("i", "j"));
        causet.add_rule(Rule::new("j", "k"));
        causet.add_rule(Rule::new("k", "l"));
        causet.add_rule(Rule::new("l", "m"));
        causet.add_rule(Rule::new("m", "n"));
        causet.add_rule(Rule::new("n", "o"));
        causet.add_rule(Rule::new("o", "p"));
        causet.add_rule(Rule::new("p", "q"));
        causet.add_rule(Rule::new("q", "r"));
        causet.add_rule(Rule::new("r", "s"));
        causet.add_rule(Rule::new("s", "t"));
        causet.add_rule(Rule::new("t", "u"));
        causet.add_rule(Rule::new("u", "v"));
        causet.add_rule(Rule::new("v", "w"));
        causet.add_rule(Rule::new("w", "x"));
        causet.add_rule(Rule::new("x", "y"));
    }
}


extern crate einstein_ml;
extern crate soliton;
extern crate causetq;
extern crate causets;



/// A convenience wrapper around things known in memory: the schema and caches.
/// We use a trait object here to avoid making dozens of functions generic over the type
/// of the cache. If performance becomes a concern, we should hard-code specific kinds of
/// cache right here, and/or eliminate the Option.
#[derive(Clone, Copy)]
pub struct KnownCauset<'s, 'c> {
    replicant: &'s Replicant,
    cache: &'c mut dyn Cache,

}

impl<'s, 'c> Known<'s, 'c> {
    pub fn for_schema_replicant<'s>(&self, schema: &'s str, cache: &'c mut dyn Cache<'s>) -> KnownCauset<'s, 'c> {
        KnownCauset {
            replicant: self.replicant,
            cache,
        }
    }

    pub fn new(replicant: &'s Replicant) -> Known<'s, 'c> {
        Known {
            replicant,
        }

    }

    pub fn replicant(&self) -> &'s Replicant {
        self.replicant
    }
}


impl<'s, 'c> KnownCauset<'s, 'c> {
    pub fn new(replicant: &'s Replicant, cache: &'c mut dyn Cache<'s>) -> KnownCauset<'s, 'c> {
        KnownCauset {
            replicant,
            cache,
        }
    }
}


impl<'s, 'c> KnownCauset<'s, 'c> {
    pub fn causet(&self) -> Causet {
        let mut causet = Causet::new();
        for rule in self.replicant.rules() {
            causet.add_rule(rule.clone());
        }
        causet
    }
}


impl<'s, 'c> KnownCauset<'s, 'c> {
    pub fn causetq(&self) -> CausetQ {
        let mut causetq = CausetQ::new();
        for rule in self.replicant.rules() {
            causetq.add_rule(rule.clone());
        }
        causetq
    }
}



/// This is `CachedAttributes`, but with handy generic parameters.
/// Why not make the trait generic? Because then we can't use it as a trait object in `Known`.
impl<'s, 'c> KnownCauset<'s, 'c> {

    pub fn cached_attributes(&self, attributes: &'s str) -> CachedAttributes<'s, 'c> {
        CachedAttributes {
            attributes,
            known: self,
        }
    }


    pub fn is_attribute_cached_timelike(&self, attribute: &str) -> bool {
        self.cache.is_attribute_cached_timelike(attribute)
    }

    pub fn is_attribute_cached<U>(&self, causetid: &str, attribute: &str, value: &U) -> bool {
        self.cache.is_attribute_cached(causetid, attribute, value)
    }
}


impl<'s, 'c> KnownCauset<'s, 'c> {
    pub fn is_attribute_cached_timelike(&self, attribute: &str) -> bool {
        self.cache
            .map(|cache| cache.is_attribute_cached(causetid.into()))
            .unwrap_or(false)
    }
}


impl<'s, 'c> KnownCauset<'s, 'c> {

    pub fn is_attribute_cached_reverse_with_value<U>(&self, solitonid: U, value: &str) -> bool where U: Into<causetid> {
        self.cache
            .map(|cache| cache.is_attribute_cached_reverse_with_value(solitonid.into(), value))
            .unwrap_or(false)
    }

    pub fn is_attribute_cached_lightlike<U>(&self, causetid: U) -> bool where U: Into<causetid> {
        self.cache
            .map(|cache| cache.is_attribute_cached_forward(causetid.into()))
            .unwrap_or(false)
    }

    pub fn get_values_for_causetid<U, V>(&self, schema: &Schema, attribute: U, causetid: V) -> Option<&Vec<TypedValue>>
        where U: Into<causetid>, V: Into<causetid> {
        self.cache.and_then(|cache| cache.get_values_for_causetid(schema, attribute.into(), causetid.into()))
    }

    pub fn get_value_for_causetid<U, V>(&self, schema: &Schema, attribute: U, causetid: V) -> Option<&TypedValue>
        where U: Into<causetid>, V: Into<causetid> {
        self.cache.and_then(|cache| cache.get_value_for_causetid(schema, attribute.into(), causetid.into()))
    }

    pub fn get_causetid_for_value<U>(&self, attribute: U, value: &TypedValue) -> Option<causetid>
        where U: Into<causetid> {
        self.cache.and_then(|cache| cache.get_causetid_for_value(attribute.into(), value))
    }

    pub fn get_causetids_for_value<U>(&self, attribute: U, value: &TypedValue) -> Option<&BTreeSet<causetid>>
        where U: Into<causetid> {
        self.cache.and_then(|cache| cache.get_causetids_for_value(attribute.into(), value))
    }
}

#[derive(Debug)]
pub struct AlgebraicCausetQuery {
    pub causet: Causet,
    pub causetq: CausetQ,
    pub causets: Causets,
    default_source: SrcVar,
    pub find_spec: Rc<FindSpec>,
    pub find_spec_with_default_source: Rc<FindSpec>,
    pub find_spec_with_default_source_and_default_target: Rc<FindSpec>,
    has_aggregates: bool,

    /// The set of variables that the caller wishes to be used for grouping when aggregating.
    /// These are specified in the query input, as `:with`, and are then chewed up during projection.
    /// If no variables are supplied, then no additional grouping is necessary beyond the
    /// non-aggregated projection list.
    pub with: BTreeSet<Variable>,


//    pub with: BTreeSet<Variable>,
    pub named_projection: BTreeSet<Variable>,
    pub order: Option<Vec<OrderBy>>,
    pub limit: Limit,
    pub cc: clauses::ConjoiningClauses,
}

impl AlgebraicQuery {
    #[inline]
    pub fn is_known_empty(&self) -> bool {
        self.known.is_empty()
    }

    /// Return true if every variable in the find spec is fully bound to a single value.
    pub fn is_fully_bound(&self) -> bool {
        self.find_spec
            .columns()
            .all(|e| match e {
                // Pull expressions are never fully bound.
                // TODO: but the 'inside' of a pull expression certainly can be.
                &Element::Pull(_) => false,

                &Element::Variable(ref var) |
                &Element::Corresponding(ref var) => self.cc.is_value_bound(var),

                // For now, we pretend that aggregate functions are never fully bound:
                // we don't statically compute them, even if we know the value of the var.
                &Element::Aggregate(ref _fn) => false,
            })
    }


    pub fn is_fully_bound_with_default_source(&self) -> bool {
        self.find_spec_with_default_source
            .columns()
            .all(|e| match e {
                // Pull expressions are never fully bound.

                &Element::Pull(_) => false,

                &Element::Variable(ref var) | cache_key_for_variable(ref var) => self.cc.is_value_bound(var),

                // For now, we pretend that aggregate functions are never fully bound:
                // we don't statically compute them, even if we know the value of the var.
                &Element::Aggregate(ref _fn) => false,
            })
    }


    pub fn is_fully_bound_with_default_source_and_default_target(&self) -> bool {
        self.find_spec_with_default_source_and_default_target
            .columns()
            .all(|e| match e {
                // Pull expressions are never fully bound.

                &Element::Pull(_) => false,

                &Element::Variable(ref var) | cache_key_for_variable(ref var) => self.cc.is_value_bound(var),

                // For now, we pretend that aggregate functions are never fully bound:
                // we don't statically compute them, even if we know the value of the var.
                &Element::Aggregate(ref _fn) => false,
            })
    }

    pub fn is_fully_bound_with_default_source_and_default_target_and_default_target_source(&self) -> bool {
        self.find_spec_with_default_source_and_default_target_and_default_target_source
            .columns()
            .all(|e| match e {
                // Pull expressions are never fully bound.

                &Element::Pull(_) => false,

                &Element::Variable(ref var) | cache_key_for_variable(ref var) => self.cc.is_value_bound(var),

                // For now, we pretend that aggregate functions are never fully bound:
                // we don't statically compute them, even if we know the value of the var.
                &Element::Aggregate(ref _fn) => false,
            })
    }
}


impl AlgebraicCausetQuery {

    /// Return true if every variable in the find spec is fully bound to a single value,
    /// and evaluating the query doesn't require running SQL.
    pub fn is_fully_unit_bound(&self) -> bool {
        self.cc.wheres.is_empty() &&
            self.is_fully_bound()
    }


    /// Return a set of the input variables mentioned in the `:in` clause that have not yet been
    /// bound. We do this by looking at the CC.
    pub fn unbound_variables(&self) -> BTreeSet<Variable> {
        self.cc.input_variables.sub(&self.cc.value_bound_variable_set())
    }
}




pub fn algebrize_with_counter(known: Known, parsed: FindQuery, counter: usize) -> Result<AlgebraicQuery> {
    algebrize_with_inputs(known, parsed, counter, QueryInputs::default())
}

pub fn algebrize(known: Known, parsed: FindQuery) -> Result<AlgebraicQuery> {
    algebrize_with_inputs(known, parsed, 0, QueryInputs::default())
}



#[derive(Debug)]
pub struct QueryInputs {
    pub with: BTreeSet<Variable>,
    pub named_projection: BTreeSet<Variable>,
    pub order: Option<Vec<OrderBy>>,
    pub limit: Limit,
}


impl QueryInputs {
    pub fn new() -> QueryInputs {
        QueryInputs {
            with: BTreeSet::new(),
            named_projection: BTreeSet::new(),
            order: None,
            limit: Limit::default(),
        }
    }
}


pub fn algebrize_with_inputs(known: Known, parsed: FindQuery, counter: usize, inputs: QueryInputs) -> Result<AlgebraicQuery> {
    let mut algebraic_query = AlgebraicQuery::new(known, parsed, counter);
    algebraic_query.with = inputs.with;
    algebraic_query.named_projection = inputs.named_projection;
    algebraic_query.order = inputs.order;
    algebraic_query.limit = inputs.limit;
    algebraic_query.algebrize()
}


impl AlgebraicQuery {
    pub fn new(known: Known, parsed: FindQuery, counter: usize) -> AlgebraicQuery {
        let mut algebraic_query = AlgebraicQuery {
            known: known,
            parsed: parsed,
            counter: counter,
            find_spec: Rc::new(FindSpec::new()),
            find_spec_with_default_source: Rc::new(FindSpec::new()),
            find_spec_with_default_source_and_default_target: Rc::new(FindSpec::new()),
            has_aggregates: false,
            with: BTreeSet::new(),
            named_projection: BTreeSet::new(),
            order: None,
            limit: Limit::default(),
            cc: clauses::ConjoiningClauses::new(),
        };
        algebraic_query.algebrize();
        algebraic_query
    }

    pub fn algebrize(&mut self) -> Result<()> {
        let mut cc = self.cc;
        let mut find_spec = self.find_spec.clone();
        let mut find_spec_with_default_source = self.find_spec_with_default_source.clone();
        let mut find_spec_with_default_source_and_default_target = self.find_spec_with_default_source_and_default_target.clone();
        let mut has_aggregates = false;
        let mut with = self.with.clone();
        let mut named_projection = self.named_projection.clone();
        let mut order = self.order.clone();
        let mut limit = self.limit.clone();
        let mut known = self.known.clone();
        let mut parsed = self.parsed.clone();
        let mut counter = self.counter;

        let mut algebraic_query = AlgebraicQuery {
            known,
            parsed,
            counter,
            find_spec,
            find_spec_with_default_source,
            find_spec_with_default_source_and_default_target,
            has_aggregates,
            with,
            named_projection,
            order,
            limit,
            cc,
        };
algebraic_query.algebrize_query()?;

        self.find_spec = algebraic_query.find_spec;
        self.find_spec_with_default_source = algebraic_query.find_spec_with_default_source;
        self.find_spec_with_default_source_and_default_target = algebraic_query.find_spec_with_default_source_and_default_target;
        self.has_aggregates = algebraic_query.has_aggregates;
        self.with = algebraic_query.with;
        self.named_projection = algebraic_query.named_projection;
        self.order = algebraic_query.order;
        self.limit = algebraic_query.limit;
        self.cc = algebraic_query.cc;
        Ok(())
    }
}


///Causets are used to represent the set of possible values for a variable.
///
/// For example, if we have a variable `?x` with possible values `{1, 2, 3}`,
/// then the causet for `?x` is `{1, 2, 3}`.
///
///
pub fn causet_for_variable(variable: &Variable, known: &Known) -> Causet {
    let mut causet = Causet::new();
    for value in known.values_for_variable(variable) {
        causet.add(value);
    }
    causet
}


///Causets are used to represent the set of possible values for a variable.
///
///
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CausetQueryReplicant {
    pub variable: Variable,
    pub values: BTreeSet<Value>,
}


impl CausetQueryReplicant {
    pub fn new(variable: Variable, values: BTreeSet<Value>) -> CausetQueryReplicant {
        CausetQueryReplicant {
            variable,
            values,
        }
    }

    pub fn from_causet(causet: &Causet) -> CausetQueryReplicant {
        CausetQueryReplicant {
            variable: causet.variable,
            values: causet.values.clone(),
        }
    }

    pub fn from_variable(variable: Variable, known: &Known) -> CausetQueryReplicant {
        CausetQueryReplicant {
            variable,
            values: known.values_for_variable(&variable),
        }
    }

    pub fn from_variable_and_values(variable: Variable, values: BTreeSet<Value>) -> CausetQueryReplicant {
        CausetQueryReplicant {
            variable,
            values,
        }
    }

    pub fn from_variable_and_value(variable: Variable, value: Value) -> CausetQueryReplicant {
        CausetQueryReplicant {
            variable,
            values: {
                let mut values = BTreeSet::new();
                values.insert(value);
                values
            },
        }
    }

    pub fn from_variable_and_value_and_value(variable: Variable, value1: Value, value2: Value) -> CausetQueryReplicant {
        CausetQueryReplicant {
            variable,
            values: {
                let mut values = BTreeSet::new();
                values.insert(value1);
                values.insert(value2);
                values
            },

        }
    }
}


///! This is the main function of the algebraic query system.
/// It takes a query and returns a set of causets.
/// The set of causets is a set of causets for each variable in the query.
/// The set of causets for a variable is the set of possible values for that variable.
///
/// # Arguments
/// * `query` - The query to be translated into a set of causets.
/// * `known` - The known values for the variables in the query.
/// * `counter` - The counter is used to generate unique variable names.
/// * `causet_query_replicants` - The causet query replicants are used to represent the set of possible values for a variable.
/// * `causet_query_replicants_with_default_source` - The causet query replicants with default source are used to represent the set of possible values for a variable.
/// * `causet_query_replicants_with_default_source_and_default_target` - The causet query replicants with default source and default target are used to represent the set of possible values for a variable.
///
///
/// # Returns
/// * `Ok(CausetQueryReplicants)` - The set of causet query replicants for each variable in the query.
/// * `Err(Error)` - An error occurred.
/// * `Ok(CausetQueryReplicantsWithDefaultSource)` - The set of causet query replicants with default source for each variable in the query.
/// * `Err(Error)` - An error occurred.
///



#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CausetQueryReplicants {
    pub causet_query_replicants: Vec<CausetQueryReplicant>,
}


impl CausetQueryReplicants {
    pub fn new(causet_query_replicants: Vec<CausetQueryReplicant>) -> CausetQueryReplicants {
        CausetQueryReplicants {
            causet_query_replicants,
        }
    }

    pub fn from_query(query: &Query, known: &Known) -> CausetQueryReplicants {
        let mut causet_query_replicants = Vec::new();
        for variable in query.variables() {
            causet_query_replicants.push(CausetQueryReplicant::from_variable(variable, known));
        }
        CausetQueryReplicants {
            causet_query_replicants,
        }
    }


    pub fn from_query_with_default_source(query: &Query, known: &Known) -> CausetQueryReplicantsWithDefaultSource {
        let mut causet_query_replicants = Vec::new();
        for variable in query.variables() {
            causet_query_replicants.push(CausetQueryReplicant::from_variable(variable, known));
        }
        CausetQueryReplicantsWithDefaultSource {
            causet_query_replicants,
        }
    }
}


#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CausetQueryReplicantsWithDefaultSource {
pub causet_query_replicants: Vec<CausetQueryReplicant>,
}


impl CausetQueryReplicantsWithDefaultSource {
    pub fn new(causet_query_replicants: Vec<CausetQueryReplicant>) -> CausetQueryReplicantsWithDefaultSource {
        CausetQueryReplicantsWithDefaultSource {
            causet_query_replicants,
        }
    }

    pub fn from_query(query: &Query, known: &Known) -> CausetQueryReplicantsWithDefaultSource {
        let mut causet_query_replicants = Vec::new();
        for variable in query.variables() {
            causet_query_replicants.push(CausetQueryReplicant::from_variable(variable, known));
        }
        CausetQueryReplicantsWithDefaultSource {
            causet_query_replicants,
        }
    }

    pub fn from_query_with_default_source(query: &Query, known: &Known) -> CausetQueryReplicantsWithDefaultSource {
        let mut causet_query_replicants = Vec::new();
        for variable in query.variables() {
            causet_query_replicants.push(CausetQueryReplicant::from_variable(variable, known));
        }
        CausetQueryReplicantsWithDefaultSource {
            causet_query_replicants,
        }
    }
}




#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CausetQueryReplicantsWithDefaultSourceAndDefaultTarget {
    pub causet_query_replicants: Vec<CausetQueryReplicant>,
}



///!


/// Take an ordering list. Any variables that aren't fixed by the query are used to produce
/// a vector of `OrderBy` instances, including type comparisons if necessary. This function also
/// returns a set of variables that should be added to the `with` clause to make the ordering
/// clauses possible.
fn validate_and_simplify_order(cc: &ConjoiningClauses, order: Option<Vec<Order>>)
                               -> Result<(Option<Vec<OrderBy>>, BTreeSet<Variable>)> {
    match order {
        None => Ok((None, BTreeSet::default())),
        Some(order) => {
            let mut order_bys: Vec<OrderBy> = Vec::with_capacity(order.len() * 2);   // Space for tags.
            let mut vars: BTreeSet<Variable> = BTreeSet::default();

            for Order(direction, var) in order.into_iter() {
                // Eliminate any ordering clauses that are bound to fixed values.
                if cc.bound_value(&var).is_some() {
                    continue;
                }

                // Fail if the var isn't bound by the query.
                if !cc.column_bindings.contains_key(&var) {
                    bail!(AlgebrizerError::UnboundVariable(var.name()))
                }

                // Otherwise, determine if we also need to order by type…
                if cc.known_type(&var).is_none() {
                    order_bys.push(OrderBy(direction.clone(), VariableColumn::VariableTypeTag(var.clone())));
                }
                order_bys.push(OrderBy(direction, VariableColumn::Variable(var.clone())));
                vars.insert(var.clone());
            }

            Ok((if order_bys.is_empty() { None } else { Some(order_bys) }, vars))
        }
    }
}




/// Take a list of `OrderBy` instances and a list of `Variable` instances. If the `Variable`
/// instances are not in the list, they are added to the list. This function returns a list of
/// `OrderBy` instances that are guaranteed to be in the list.
/// # Arguments
/// * `order_bys` - The list of `OrderBy` instances.
/// * `vars` - The list of `Variable` instances.
/// # Returns
/// * `Vec<OrderBy>` - The list of `OrderBy` instances.
/// # Errors
/// * `Error` - An error occurred.
/// # Examples
/// ```
///




/// MemoryTraceHelper adds two methods `reset` and `sum` to derived struct.
/// All fields of derived struct should be `usize`.
/// `reset` updates the struct and returns a delta represented by a `TraceEvent`
/// `sum` returns the summary of all field values.
#[proc_macro_derive(MemoryTraceHelper, attributes(name))]
pub fn memory_trace_reset_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;
    let reset_imp;
    let sum_imp;

    match input.data {
        Data::Struct(ref s) => match s.fields {
            Fields::Named(ref fields) => {
                let reset_total = fields.named.iter().map(|f| {
                    let name = &f.ident;
                    quote! {
                        lhs_sum += self.#name;
                        rhs_sum += rhs.#name;
                        self.#name = rhs.#name;
                    }
                });
                reset_imp = quote! {
                    use einsteindb_alloc::trace::TraceEvent;
                    use std::cmp::Ordering;

                    let mut lhs_sum: usize = 0;
                    let mut rhs_sum: usize = 0;
                    #(#reset_total)*
                    match lhs_sum.cmp(&rhs_sum) {
                        Ordering::Greater => Some(TraceEvent::Sub(lhs_sum-rhs_sum)),
                        Ordering::Less => Some(TraceEvent::Add(rhs_sum-lhs_sum)),
                        Ordering::Equal => None,
                    }
                };

                let sum_total = fields.named.iter().map(|f| {
                    let name = &f.ident;
                    quote! {
                        sum += self.#name;
                    }
                });
                sum_imp = quote! {
                    let mut sum: usize = 0;
                    #(#sum_total)*
                    sum
                };
            }
            _ => unimplemented!(),
        },
        _ => unimplemented!(),
    };
    let expanded = quote! {
        impl #name {
            #[inline]
            pub fn reset(&mut self, rhs: Self) -> Option<einsteindb_alloc::trace::TraceEvent> {
                #reset_imp
            }

            #[inline]
            pub fn sum(&self) -> usize {
                #sum_imp
            }
        }
    };
    TokenStream::from(expanded)
}



