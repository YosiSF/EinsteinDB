/// Copyright (c) 2022 Whtcorps Inc and EinsteinDB Project contributors
///     Licensed under the Apache License, Version 2.0 (the "License");
///    you may not use this file except in compliance with the License.
///   You may obtain a copy of the License at
///       http://www.apache.org/licenses/LICENSE-2.0
///    Unless required by applicable law or agreed to in writing, software
///   distributed under the License is distributed on an "AS IS" BASIS,
///  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
///  See the License for the specific language governing permissions and
///  limitations under the License.
/// =================================================================
///


#[macro_use]
extern crate soliton_panic;

#[macro_use]
extern crate soliton;

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate log;

#[macro_use]
extern crate causet;

#[macro_use]
extern crate causetq;

#[macro_use]
extern crate causets;

// #[macro_use]
// extern crate soliton_panic;
//
// #[macro_use]
// extern crate soliton;
//
// #[macro_use]
// extern crate lazy_static;
//
// #[macro_use]
// extern crate log;
#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate serde_value;


#[macro_use]
extern crate serde_yaml;


#[macro_use]
extern crate serde_cbor;


#[macro_use]
extern crate failure;




// #[macro_use]
// extern crate failure_derive;
// #[macro_use]

#[macro_use]
extern crate einstein_db;




use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[macro_use]
extern crate lazy_static;

#[Derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EvalTypeTp {
    pub eval_type: EvalType,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EvalType {
    pub eval_type: String,
    pub eval_type_id: i32,
}



pub struct Causet {
    pub causet_id: i32,
    pub causet_name: String,
    pub causet: CausetT,
    pub events: Vec<String>,
    pub edges: Vec<(String, String)>,
}


impl Causet {
    pub fn new() -> Causet {
        Causet {
            causet_id: 0,
            causet_name: (),
            causet: CausetT::new(),

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

        switch_to_db_thread();
        Vec::new();

        // Causets {
        //     causets: Vec::new(),
        // }
    }

    pub fn add_causet(&mut self, causet: CausalSet <T>) {
        self.causets.push(causet);

        // switch_to_db_thread();
        // self.causets.push(causet);
    }
}


//Causets are now stored in a FoundationDB database.
//The database is a key-value store, where the key is the event and the value is the set of events that follow the event.
//The database is implemented as a B+Tree.
//The Causets are ordered by the order of the events in the Causal Sets.


// #[derive(Serialize, Deserialize, Debug, Clone)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CausetsDB <T> {
    pub causets: Vec<CausalSet <T>>,

}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CausetsDB_ <T> {
    pub causets: Vec<CausalSet <T>>,

}



///! # CausetDB
/// A CausetDB is a Causets with FoundationDB secondary attributes.
/// The secondary attributes are:
/// - A tuplestore, where the key is the event and the value is the set of events that follow the event.
/// - A key-value store, where the key is the event and the value is the set of events that follow the event.
///
/// The tuplestore is implemented as a B+Tree.
///







type ErrorBuilder = Box<dyn Send + Sync + Fn() -> crate::error::StorageError>;


type FixtureValue = std::result::Result<Vec<u8>, ErrorBuilder>;


type Fixture = Arc<Mutex<FixtureValue>>;


lazy_static! {
    static ref Fixture: Fixture = Arc::new(Mutex::new(Ok(Vec::new())));
}

////////////////////////////////
/// Fixture
/// A Fixture is a set of fixtures.
/// Fixtures are FoundationDB Records with tuplestore secondary attributes and copy on write access.
/// The tuplestore is used to store the fixtures.
/// The tuplestore is a key-value store, where the key is the event and the value is the set of events that follow the event.
/// The tuplestore is implemented as a B+Tree.
/// we use async to simulate the async behavior of FoundationDB.
/// The Fixture is ordered by the order of the events in the Fixture.

impl <T> Fixture <T> {
    pub fn new() -> Fixture <T> {
        Fixture {
            fixtures: Vec::new(),
        }
    }

    pub fn add_fixture(&mut self, fixture: T) {
        self.fixtures.push(fixture);
    }

pub fn get_fixture(&self, event: T) -> Option<T> {
        self.fixtures.iter().find(|&f| f == &event).map(|f| f.clone())
    }

    pub fn get_fixture_mut(&mut self, event: T) -> Option<T> {
        self.fixtures.iter_mut().find(|&f| f == &event).map(|f| f.clone())
    }

    pub fn get_fixture_ref(&self, event: T) -> Option<&T> {
        self.fixtures.iter().find(|&f| f == &event)
    }

    pub fn get_fixture_ref_mut(&mut self, event: T) -> Option<&mut T> {
        self.fixtures.iter_mut().find(|&f| f == &event)
    }
    /// # Causet
    /// A Causet is a Poset of AllegroCL semantics. It is a set of ordered pairs of events.
    /// The set is represented as a DAG, where each event is a node and each ordered pair of events is an edge.
    /// The set is ordered by the order of the events in the set.
    /// causets are FoundationDB Records with tuplestore secondary attributes and copy on write access.
    /// The tuplestore is used to store the edges of the set.
    ///
    /// The tuplestore is a key-value store, where the key is the event and the value is the set of events that follow the event.
    /// The tuplestore is implemented as a B+Tree.
    ///
    ///

    ///main function to get the fixture for a given event
    /// # Arguments
    /// * `event` - the event to get the fixture for
    /// # Returns
    /// * `Option<T>` - the fixture for the event
    /// # Errors
    /// * `StorageError` - if there is an error getting the fixture
    /// # Examples
    ///
    ///



    pub async fn get_fixture_async(&self, event: T) -> Result<Option<T>, crate::error::StorageError> {
        let mut fixture = self.get_fixture_ref(event);
        if fixture.is_none() {
            return Ok(None);
        }
        let fixture = fixture.unwrap();
        Ok(Some(fixture.clone()))
    }

    ///main function to get the fixture for a given event
    /// # Arguments
    ///
    ///



    pub async fn get_fixture_mut_async(&mut self, event: T) -> Result<Option<T>, crate::error::StorageError> {
        let mut fixture = self.get_fixture_ref_mut(event);
        if fixture.is_none() {
            return Ok(None);
        }
        let fixture = fixture.unwrap();
        Ok(Some(fixture.clone()))
    }
}


////////////////////////////////
/// Fixture
/// A Fixture is a set of fixtures.
/// Fixtures are FoundationDB Records with tuplestore secondary attributes and copy on write access.
/// The tuplestore is used to store the fixtures.
/// The tuplestore is a key-value store, where the key is the event and the value is the set of events that follow the event.
/// The tuplestore is implemented as a B+Tree.
/// we use async to simulate the async behavior of FoundationDB.
/// The Fixture is ordered by the order of the events in the Fixture.
/// !# Examples
/// ```
/// use allegrocl::fixture::Fixture;
/// use allegrocl::event::Event;
/// use allegrocl::error::StorageError;
///
/// let mut fixture = Fixture::new();
/// fixture.add_fixture(Event::new("a"));
/// fixture.add_fixture(Event::new("b"));
/// fixture.add_fixture(Event::new("c"));
/// fixture.add_fixture(Event::new("d"));
/// fixture.add_fixture(Event::new("e"));
///
///
///
/// let mut fixture2 = Fixture::new();
/// fixture2.add_fixture(Event::new("a"));
/// fixture2.add_fixture(Event::new("b"));
/// fixture2.add_fixture(Event::new("c"));
/// fixture2.add_fixture(Event::new("d"));
///
///
/// let mut fixture3 = Fixture::new();
/// fixture3.add_fixture(Event::new("a"));
/// fixture3.add_fixture(Event::new("b"));
/// fixture3.add_fixture(Event::new("c"));
///
///
/// let mut fixture4 = Fixture::new();
///
///
///
///

pub async fn get_fixture_async(fixture: &Fixture <Event>, event: Event) -> Result<Option<Event>, crate::error::StorageError> {
    let mut fixture = fixture.get_fixture_ref(event);
    if fixture.is_none() {
        return Ok(None);
    }

    let mut causet = Causet::new();

    for event in fixture.unwrap() {
        causet.add_event(event.clone());
        causet.add_event(event.clone());
    }

    Ok(Some(causet.get_event(0).unwrap().clone()))
}


////////////////////////////////
/// Fixture
///
    ///
/// causet.add_event("a".to_string());
/// causet.add_event("b".to_string());
/// causet.add_event("c".to_string());



    ///main function to get the fixture for a given event
    /// # Arguments
    /// * event - the event to get the fixture formatted
    /// # Returns   the fixture formatted   as a string


pub async fn get_fixture_mut_async(fixture: &mut Fixture <Event>, event: Event) -> Result<Option<Event>, crate::error::StorageError> {
    let mut fixture = fixture.get_fixture_ref_mut(event);
    if fixture.is_none() {
        return Ok(None);
    }
}

    pub async fn get_fixture_formatted_async(fixture: &Fixture <Event>, event: Event) -> Result<Option<String>, crate::error::StorageError> {
        let mut fixture = fixture.get_fixture_ref(event);
        if fixture.is_none() {
            return Ok(None);
        }
        let mut fixture = fixture.unwrap();
        let mut causet = Causet::new();
        for event in fixture {
            causet.add_event(event.clone());
            causet.add_event(event.clone());
        }
        Ok(Some(causet.get_event(0).unwrap().clone().to_string()))
    }



    ///main function to get the fixture for a given event
    /// # Arguments
    /// * event - the event to get the fixture formatted
    /// # Returns   the fixture formatted   as a string
    /// # Errors
    /// * `StorageError` - if there is an error getting the fixture
    /// # Examples
    /// ```
    /// use allegrocl::fixture::Fixture;
    /// use allegrocl::event::Event;
    ///
    /// fn() {
    ///    let mut fixture = Fixture::new();
    /// fixture.
    /// }




    ///main function to get the fixture for a given event
    /// # Arguments




    ///main function to get the fixture for a given event
    /// # Arguments
    /// * event - the event to get the fixture formatted
    /// # Returns   the fixture formatted   as a string
    /// # Errors






fn get_fixture_formatted(fixture: &Fixture <Event>, event: Event) -> Result<Option<String>, crate::error::StorageError> {
    let mut fixture = fixture.get_fixture_ref(event);
    if fixture.is_none() {
        return Ok(None);
    }
    let mut fixture = fixture.unwrap();
    let mut causet = Causet::new();
    for event in fixture {
        causet.add_event(event.clone());
        causet.add_event(event.clone());
    }
    Ok(Some(causet.get_event(0).unwrap().clone().to_string()))
}



