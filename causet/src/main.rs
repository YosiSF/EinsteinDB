/// Copyright (c) 2019-present, EinsteinDB. All rights reserved.
///
/// @author:  EinsteinDB <  @einstein-db.com>
/// @date:  2019-12-16
/// @version:  0.1.0
///
///
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};


use crate::causet::*;
use crate::causets::*;
use crate::einstein_db_alexandrov_processing::*;


pub struct Causet {
    pub events: Vec<String>,
    pub edges: Vec<(String, String)>,
}


impl Causet {
    pub fn new() -> Causet {
        Causet {
            events: Vec::new(),
            edges: Vec::new(),
        }
    }

    pub fn add_event(&mut self, event: String) {
        self.events.push(event);
    }

    pub fn add_edge(&mut self, event1: String, event2: String) {
        self.edges.push((event1, event2));
    }
}


#[macro_use]
extern crate lazy_static;


use std::sync::Arc;
use std::sync::Mutex;



use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

#[macro_use]
extern crate log;

#[macro_use]
extern crate serde_derive;


use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::causet::*;
use crate::causets::*;
use crate::einstein_db_alexandrov_processing::*;

/// # Causet
/// A Causal Set is a Poset of AllegroCL semantics. It is a set of ordered pairs of events.
/// The set is represented as a DAG, where each event is a node and each ordered pair of events is an edge.
/// The set is ordered by the order of the events in the set.
/// causets are FoundationDB Records with tuplestore secondary attributes and copy on write access.
/// The tuplestore is used to store the edges of the set.
/// The tuplestore is a key-value store, where the key is the event and the value is the set of events that follow the event.
/// The tuplestore is implemented as a B+Tree.


pub struct CausalSet <T> {
    pub events: Vec<T>,
    pub edges: Vec<(T, T)>,
}


impl <T> CausalSet <T> {
    pub fn new() -> CausalSet <T> {
        CausalSet {
            events: Vec::new(),
            edges: Vec::new(),
        }
    }

    pub fn add_event(&mut self, event: T) {
        self.events.push(event);
    }

    pub fn add_edge(&mut self, event1: T, event2: T) {
        self.edges.push((event1, event2));
    }
}




/// # Causets
/// A Causets is a set of Causal Sets.
/// Causets are FoundationDB Records with tuplestore secondary attributes and copy on write access.
/// The tuplestore is used to store the Causal Sets.
/// The tuplestore is a key-value store, where the key is the event and the value is the set of events that follow the event.
/// The tuplestore is implemented as a B+Tree.
/// The Causets are ordered by the order of the events in the Causal Sets.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Causets <T> {
    pub causets: Vec<CausalSet <T>>,
}


impl <T> Causets <T> {
    pub fn new() -> Causets <T> {
        Causets {
            causets: Vec::new(),
        }
    }

    pub fn add_causet(&mut self, causet: CausalSet <T>) {
        self.causets.push(causet);
    }
}




type ErrorBuilder = Box<dyn Send + Sync + Fn() -> crate::error::StorageError>;


type FixtureValue = std::result::Result<Vec<u8>, ErrorBuilder>;


type Fixture = Arc<Mutex<FixtureValue>>;


lazy_static! {
    static ref Fixture: Fixture = Arc::new(Mutex::new(Ok(Vec::new())));
}

fn main() {
    let mut causet = Causet::new();
    causet.add_event("A".to_string());
    causet.add_event("B".to_string());
    causet.add_event("C".to_string());
    causet.add_event("D".to_string());

    causet.add_edge("A".to_string(), "B".to_string());
    causet.add_edge("B".to_string(), "C".to_string());
    causet.add_edge("C".to_string(), "D".to_string());
    causet.add_edge("D".to_string(), "A".to_string());

    let causet = causet;


    let causets = Causets::new();
    causets.add_causet(causet);


    let causet_arc = Arc::new(Mutex::new(causet));
    let causet_arc_clone = causet_arc.clone();

    let mut causets = Causets::new();

    causets.add_causet(causet_arc);

    let causets = causets;

    let causets_arc = Arc::new(Mutex::new(causets));

    let causets_arc_clone = causets_arc.clone();

    let mut causets_clone = causets_arc.lock().unwrap().clone();

    let causets_clone = causets_clone;

    async fn async_causets_add_causet(causets_arc: Arc<Mutex<Causets<String>>>) {
        let causets_arc_clone = causets_arc.clone();
        let causets_arc_clone = causets_arc_clone;
        let causets_clone = causets_arc.lock().unwrap().clone();
        let causet_clone = causets_clone.add_causet(causet_arc_clone);
        println!("{:?}", causet_clone);
    }

    #[tokio::main]
    async fn main() {
        let causets_arc = causets_arc_clone.clone();
        let causets_arc = causets_arc;
        let causets_clone = causets_arc.lock().unwrap().clone();
        let causets_clone = causets_clone;
        let causet_clone = causets_clone.add_causet(causet_arc_clone);
        println!("{:?}", causet_clone);
    }

    async_causets_add_causet(causets_arc_clone).await;

    let causets_clone = causets_arc.lock().unwrap().clone();

    let causets_clone = causets_clone;

    let causet_clone = causets_clone.add_causet(causet_arc_clone);
}

