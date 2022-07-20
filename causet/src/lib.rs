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


use std::time::{SystemTime, UNIX_EPOCH};
use std::thread::{self, sleep};
use std::sync::mpsc::{channel, Sender, Receiver};
use std::sync::mpsc::TryRecvError;
use std::sync::mpsc::RecvError;
pub(crate) use std::sync::mpsc::RecvTimeoutError;

//inherit allegro_poset
use crate::allegro_poset::{AllegroPoset, AllegroPosetError};
use crate::allegro_poset::{AllegroPosetConfig, AllegroPosetConfigError};
use crate::allegro_poset::{AllegroPosetConfigBuilder, AllegroPosetConfigBuilderError};


//inherit causet_store
use crate::EinsteinDB::{EinsteinDB, EinsteinDBError};




use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Display, Formatter, Result};
use std::time::Duration;
use std::sync::atomic::{AtomicBool, Ordering};


#[derive(Debug)]
pub struct AlgebraicCauset {
    pub with: BTreeSet<Variable>,
    pub named_projection: BTreeSet<Variable>,
    pub order: Option<Vec<OrderBy>>,
    pub limit: Limit,
}

impl AlgebraicCauset {
    fn default() -> Self {
        QueryInputs {
            with: BTreeSet::new(),
            named_projection: BTreeSet::new(),
            order: None,
            limit: Limit::default(),
        }
    }
}






///! This is the main interface to the CausetCache.
/// It is a wrapper around a CausetCache that provides a way to get a Causet
/// from a CausetCache. If the CausetCache is a KnownCauset, then the Causet is returned
/// from the KnownCauset. Otherwise, the Causet is loaded from the database.
/// TODO: SRDMA is not yet implemented.



impl QueryInputs {


    pub fn with_with(mut self, with: BTreeSet<Variable>) -> Self {
        self.with = with;
        self
    }

    pub fn with_named_projection(mut self, named_projection: BTreeSet<Variable>) -> Self {
        self.named_projection = named_projection;
        self
    }

    pub fn with_order(mut self, order: Option<Vec<OrderBy>>) -> Self {
        self.order = order;
        self
    }

    pub fn with_limit(mut self, limit: Limit) -> Self {
        self.limit = limit;
        self
    }
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


impl QueryInputs {
    pub fn with_with(mut self, with: BTreeSet<Variable>) -> Self {
        self.with = with;
        self
    }

    pub fn with_with_mut(&mut self, with: BTreeSet<Variable>) -> &mut Self {
        self.with = with;
        self
    }

    pub fn with_named_projection_mut(&mut self, named_projection: BTreeSet<Variable>) -> &mut Self {
        self.named_projection = named_projection;
        self
    }
}

impl QueryInputs {
    pub fn with_named_projection(mut self, named_projection: BTreeSet<Variable>) -> Self {
        self.named_projection = named_projection;
        self
    }
}

impl QueryInputs {
    pub fn with_order(mut self, order: Option<Vec<OrderBy>>) -> Self {
        self.order = order;
        self
    }
}

impl QueryInputs {
    pub fn with_order_mut(&mut self, order: Option<Vec<OrderBy>>) -> &mut Self {
        self.order = order;
        self
    }
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


    pub fn algebrize_with_inputs(known: Known, parsed: FindQuery, counter: usize, inputs: QueryInputs) -> Result {
        let mut algebraic_query = AlgebraicQuery::new(known, parsed, counter);
        algebraic_query.with = inputs.with;
        algebraic_query.named_projection = inputs.named_projection;
        algebraic_query.order = inputs.order;
        algebraic_query.limit = inputs.limit;
        algebraic_query.algebrize()
    }
}
        impl AlgebraicQuery {
            pub fn new(known: Known, parsed: FindQuery, counter: usize) -> AlgebraicQuery {
                let mut algebraic_query = AlgebraicQuery {
                    known,
                    parsed,
                    counter,
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

            pub fn algebrize(&mut self) -> Result {
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
        pub causet_query_replicants: Vec<CausetQueryReplicants>,
    }

    impl CausetQueryReplicantsWithDefaultSource {
        pub fn new(causet_query_replicants: Vec<CausetQueryReplicants>) -> CausetQueryReplicantsWithDefaultSource {
            CausetQueryReplicantsWithDefaultSource {
                causet_query_replicants,
            }
        }
    }

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CausetQueryReplicantsWithDefaultSourceAndDefaultTarget {
    pub causet_query_replicants: Vec<CausetQueryReplicantsWithDefaultSource>,
}


impl CausetQueryReplicantsWithDefaultSourceAndDefaultTarget {


    pub fn from_query(query: &Query, known: &Known) -> CausetQueryReplicantsWithDefaultSource {
        let mut causet_query_replicants = Vec::new();
        for variable in query.variables() {
            causet_query_replicants.push(CausetQueryReplicant::from_variable(variable, known));
        }
        CausetQueryReplicantsWithDefaultSource {
            causet_query_replicants,
        }
    }

    pub fn new(causet_query_replicants: Vec<CausetQueryReplicantsWithDefaultSource>) -> CausetQueryReplicantsWithDefaultSourceAndDefaultTarget {
        CausetQueryReplicantsWithDefaultSourceAndDefaultTarget {
            causet_query_replicants,
        }




    }



}






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


type Poset = BTreeMap<String, BTreeSet<String>>;

type PosetNode = (String, BTreeSet<String>);

type PosetNodeData = (String, BTreeSet<String>);

//AEVTrie
//type Poset = AEVTrie<String, BTreeSet<String>>;
type Compaction = BTreeMap<String, BTreeSet<String>>;




pub struct AllegroPoset{
    poset: Poset,
    //pub sender: Sender<PosetNodeData>,
    //pub receiver: Receiver<PosetNodeData>,
    pub sender: Sender<PosetNodeData>,
    pub receiver: Receiver<PosetNodeData>,
    pub sender_thread: thread::JoinHandle<()>,
    pub receiver_thread: thread::JoinHandle<()>,
    pub sender_thread_running: bool,
    pub receiver_thread_running: bool,
    pub sender_thread_running_mutex: Mutex<bool>,
    pub receiver_thread_running_mutex: std::sync::Mutex<bool>,
    pub sender_thread_running_cond: std::sync::Condvar,
    pub receiver_thread_running_cond: std::sync::Condvar,
    pub sender_thread_running_cond_mutex: Mutex<bool>,
    pub receiver_thread_running_cond_mutex: Mutex<bool>,
    pub sender_thread_running_cond_cond: std::sync::Condvar,
    pub receiver_thread_running_cond_cond: std::sync::Condvar,
    pub sender_thread_running_cond_cond_mutex: std::sync::Mutex<bool>,
    pub receiver_thread_running_cond_cond_mutex: std::sync::Mutex<bool>,
    pub sender_thread_running_cond_cond_cond: std::sync::Condvar,
    pub receiver_thread_running_cond_cond_cond: std::sync::Condvar,
    pub sender_thread_running_cond_cond_cond_mutex: std::sync::Mutex<bool>,
    pub receiver_thread_running_cond_cond_cond_mutex: std::sync::Mutex<bool>,
    pub sender_thread_running_cond_cond_cond_cond: std::sync::Condvar,
    pub receiver_thread_running_cond_cond_cond_cond: std::sync::Condvar,
    pub sender_thread_running_cond_cond_cond_cond_mutex: std::sync::Mutex<bool>,

}

///! # Causet
/// A Squuid is a unique identifier for a node in the causal poset.
/// It is a string of hexadecimal digits.
/// # Examples
/// ```
/// use einsteindb::causetq::sync::new_sync;
/// use einsteindb::causetq::sync::Sync;
/// let poset = new_sync();
/// let sync = Sync::new(poset);
///
/// let squuid = sync.squuid();
/// ```
///
/// # Causetq<T> {
/// #     poset: Poset,
/// #     sender: Sender<PosetNodeData>,
/// #     receiver: Receiver<PosetNodeData>,
/// #     sender_thread: thread::JoinHandle<()>,
/// #     receiver_thread: thread::JoinHandle<()>,
/// #     sender_thread_running: bool,}











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
            poset: std::sync::Arc::new(Mutex::new(poset)),
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
    pub fn new_sync_with_options(poset: AllegroPoset, options: EinsteindbOptions) -> Arc<Mutex<Sync>> {
        Arc::new(Mutex::new(Sync::new_with_config(poset, options)))
    }
}

//EinsteinDB options
#[derive(Debug)]
pub struct EinsteindbOptions {
    pub sender_thread_running: bool,
    pub receiver_thread_running: bool,
    pub sender_thread_running_mutex: bool,
    pub receiver_thread_running_mutex: bool,
    pub sender_thread_running_cond: bool,
    pub receiver_thread_running_cond: bool,
    pub sender_thread_running_cond_mutex: bool,
    pub receiver_thread_running_cond_mutex: bool,
    pub sender_thread_running_cond_cond: bool,
    pub receiver_thread_running_cond_cond: bool,
    pub sender_thread_running_cond_cond_mutex: bool,
    pub receiver_thread_running_cond_cond_mutex: bool,
    pub sender_thread_running_cond_cond_cond: bool,
    pub receiver_thread_running_cond_cond_cond: bool,
    pub sender_thread_running_cond_cond_cond_mutex: bool,
    pub receiver_thread_running_cond_cond_cond_mutex: bool,
    pub sender_thread_running_cond_cond_cond_cond: bool,
    pub receiver_thread_running_cond_cond_cond_cond: bool,
    pub sender_thread_running_cond_cond_cond_cond_mutex: bool,
    pub receiver_thread_running_cond_cond_cond_cond_mutex: bool,
    pub sender_thread_running_cond_cond_cond_cond_cond: bool,
    pub receiver_thread_running_cond_cond_cond_cond_cond: bool,
    pub sender_thread_running_cond_cond_cond_cond_cond_mutex: bool,
    pub receiver_thread_running_cond_cond_cond_cond_cond_mutex: bool,
    pub sender_thread_running_cond_cond_cond_cond_cond_cond: bool,
    pub receiver_thread_running_cond_cond_cond_cond_cond_cond: bool,
    pub sender_thread_running_cond_cond_cond_cond_cond_cond_mutex: bool,
}



/// Creates a new `Sync` instance.

impl Sync {
    /// Creates a new `Sync` instance.
    /// # Examples
    /// ```
    /// use einsteindb::causetq::sync::new_sync;
    /// use einsteindb::causetq::sync::Sync;
    /// let poset = new_sync();
    /// let sync = Sync::new(poset);
    /// ```
    pub fn new_with_config(poset: AllegroPoset, options: EinsteindbOptions) -> Self {
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
    pub fn new_with_config_sync(poset: AllegroPoset, options: EinsteindbOptions) -> Arc<Mutex<Sync>> {
        Arc::new(Mutex::new(Sync::new_with_config(poset, options)))
    }
}






use std::iter::FromIterator;
use std::iter::Iterator;
use std::iter::Peekable;
use std::iter::Rev;
use std::ops::Index;
use std::ops::IndexMut;
use std::ops::Range;

use std::ops::Deref;
use std::ops::DerefMut;





use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;
use std::hash::BuildHasherDefault;
use std::hash::SipHasher;
use std::hash::BuildHasher;
use std::rc::Rc;

use std::sync::{Arc, Mutex};
use std::sync::RwLock;
use std::sync::RwLockReadGuard;
use std::sync::RwLockWriteGuard;





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



//Typed Value struct and trait which is a homology for CausetTV and CausetTVMut.
//The CausetTV is a read-only view of the Causet.
//The CausetTVMut is a mutable view of the Causet.


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CausetTV<'a, T: 'a> {
    causet: &'a Causet,
    value: T,
}


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


trait AEVTrie {
fn add_rule(&mut self, rule: Rule);
    fn get_rules(&self) -> &Vec<Rule>;
    fn get_poset(&self) -> &AllegroPoset;
}






impl Causet {
    /// Creates a new Causet.
    pub fn new() -> Causet {
        Causet {
            rules: Vec::new(),
            poset: AllegroPoset::new(),
        }
    }

    /// Adds a rule to the Causet.
    pub fn add_rule(&mut self, rule: Rule) {
        self.rules.push(rule);
        self.poset.add_rule( rule.antecedent.clone(), rule.consequent.clone());
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

pub trait Schema {
    fn get_schema(&self) -> &Vec<String>;
}

/// A convenience wrapper around things known in memory: the Schema and caches.
/// We use a trait object here to avoid making dozens of functions generic over the type
/// of the cache. If performance becomes a concern, we should hard-code specific kinds of
/// cache right here, and/or eliminate the Option.
#[derive(Clone, Copy)]
pub struct KnownCauset<'s, 'c> {
    schema: &'s dyn Schema,
    causetid: &'s str,



}

impl<'s, 'c> KnownCauset<'s, 'c> {
    pub fn for_schema_replicant<'a>(&'a self) -> KnownCauset<'s, 'c> {
        KnownCauset {
    schema: self.schema,
                causetid: self.causetid,
          }
        }

    pub fn for_schema_replicant_with_cache(&self, schema: &'s str) -> KnownCauset<'s, 'c> {
        KnownCauset {
            schema: &(),

            causetid: self.causetid,
        }
    }

    pub fn replicant(&self) -> &'s str {
        self.replicant
    }
}


impl<'s, 'c> KnownCauset<'s, 'c> {
    pub fn new(replicant: &'s str, schema: &'s dyn Schema) -> KnownCauset<'s, 'c> {
        KnownCauset {
            schema: &(),

            causetid: replicant,
        }
    }
}






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


    #[test]
    fn test_causet_with_cache() {
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


    trait CausetCacheMutMut {
        fn get_causet(&mut self, causetid: &str) -> &mut Causet;

        fn get_causetq(&mut self, causetid: &str) -> &mut Causet;

        fn get_causet_mut(&mut self, causetid: &str) -> &mut Causet;


        fn schema(&self) -> &str;

        fn schema_mut(&mut self) -> &mut str;


        fn causet(&self) -> &Causet;


        fn causet_mut(&mut self) -> &mut Causet;

        fn causetq(&self) -> &Causet;

        fn causetq_mut(&mut self) -> &mut Causet;
    }


    ///! This is a test for the causet cache.
    ///
    /// It tests the following:
    /// 1. The cache is created correctly.
    /// 2. The cache is updated correctly.
    /// 3. The cache is read correctly.
    ///
    #[test]
    fn test_causet_cache() {
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

        let mut causet_cache = CausetCache::new();
    }

    #[test]
    fn test_causet_cache_with_cache() {
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

        let mut causet_cache = CausetCache::new();
        causet_cache.add_causet("causet1", causet);
        let mut causet = Causet::new();
        causet.add_rule(Rule::new("a", "b"));
        causet.add_rule(Rule::new("b", "c"));

        causet_cache.add_causet("causet2", causet);
    }
}


impl<'s, 'c> CausetCacheMutMut for KnownCauset<'s, 'c> {
    fn get_causet(&mut self, causetid: &str) -> &mut Causet {
        self.causet_mut()
    }

    fn get_causetq(&mut self, causetid: &str) -> &mut Causet {
        self.causetq_mut()
    }

    fn get_causet_mut(&mut self, causetid: &str) -> &mut Causet {
        self.causet_mut()
    }

    fn schema(&self) -> &str {
        self.schema
    }

    fn schema_mut(&mut self) -> &mut str {
        self.schema_mut()
    }

    fn causet(&self) -> &Causet {
        &self.causet()
    }

    fn causet_mut(&mut self) -> &mut Causet {
        self.causet_mut()
    }

    fn causetq(&self) -> &Causet {
        &self.causetq()
    }

    fn causetq_mut(&mut self) -> &mut Causet {
        self.causetq_mut()
    }

    /// Return true if every variable in the find spec is fully bound to a single value,
    /// and evaluating the query doesn't require running SQL.
    /// This is_fully_bound_mut
}




impl<'s, 'c> CausetCacheMutMut for KnownCauset<'s, 'c> {
    fn is_fully_bound_mut_mut(&mut self) -> bool {
        self.cc.wheres.is_empty() &&
            self.cc.find_spec.is_fully_bound()
    }


    fn get_causetq(&mut self, causetid: &str) -> &mut Causet {
        self.causetq_mut()
    }


    fn algebrize_with_counter(
        causet: &Causet,
        find: &Find,
        counter: &mut Counter,
    ) -> AlgebraicCauset {
        let mut algebrized = AlgebraicFind::new();
        for clause in find.clauses() {
            algebrized.add_clause(clause.algebrize(causet, counter));
        }
        algebrized
    }


    fn algebrize_with_counter_mut(&mut self, find: &Find) -> AlgebraicCauset {
        let mut counter = Counter::new();
        let algebrized = Self::algebrize_with_counter(
            &self.causet(),
            find,
            &mut counter,
        );
        self.counter = counter;
        algebrize_with_counter(known, parsed, &mut Counter::new())
    }

    fn get_causet_mut<'a, 'b>(&'a mut self, causetid: &'b str) -> &'a mut Causet {
        self.causet_mut()
    }

    fn get_causet(&self, causetid: &str) -> &Causet {
        self.causet()
    }

    fn schema_mut(&mut self) -> &mut str {
        todo!()
    }

    fn causet(&self) -> &Causet {
        todo!()
    }

    fn causet_mut(&mut self) -> &mut Causet {
        todo!()
    }

    fn causetq(&self) -> &Causet {
        todo!()
    }

    fn causetq_mut(&mut self) -> &mut Causet {
        todo!()
    }
}
