//Copyright (c) 2022, <MIT License> Copyright (c) 2022,
// <Karl Whitford and Josh Leder>

/// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.
///
/// # CausetXContext Control Factors

//A Causet is a set of causets.
//We call this set of causets a Causet of Causets.
//in singleton Causets, the Causet of Causets is the set of singleton Causets.


use std::cmp::Eq;
use std::cmp::Ord;
use std::cmp::Partitioning;
use std::cmp::PartialEq;
use std::cmp::PartialOrd;
use std::collections::BinaryHeap;
use std::collections::BTreeMap;
use std::collections::BTreeMap::Entry;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashMap::{Iter, IterMut};
use std::collections::HashMap::{Keys, Values, ValuesMut};
use std::collections::HashMap::{Entry, OccupiedEntry, VacantEntry};
use std::collections::HashMap::{Keys, Values, ValuesMut};
use std::collections::HashMap::{Entry, OccupiedEntry, VacantEntry};
use std::collections::HashMap::{Keys, Values, ValuesMut};
use std::collections::HashMap::Entry::{Occupied, Vacant};
use std::collections::HashMap::Entry::{Occupied, Vacant};
use std::collections::HashSet;
use std::collections::LinkedList;
use std::collections::LinkedList::{Iter, IterMut};
use std::collections::VecDeque;
use std::collections::VecDeque::{Iter, IterMut};
use std::fmt;
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;
use std::hash::Hash;
use std::hash::Hasher;

use berolina_sql::{Sql, SqlType};
use causet::causet::Causet;
use causet::causet::Causet::{Causet, CausetOfCausets};

use allegro_poset::{AllegroPoset, Poset};
use rusqlite::{Connection, OpenFlags};
use postgres_protocol::{Postgres, PostgresConnection};
use foundationdb::{Fdb, FdbError, FdbResult};
use foundationdb::{Fdb, FdbError, FdbResult};







#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[derive(Serialize, Deserialize)]
pub struct EvalTypeTp {
    pub eval_type: EvalType,
}



#[derive(Debug, Clone)]
pub struct EvalTypeWrap {
    pub eval_type: EvalType,
    pub eval_wrap: EvalWrap,
}



//implementation of the Causet of Causet

//Allegro_Poset is an Allegro Lisp based off of the Partial Partition Poset of Causets  (POSET)
//      (see https://en.wikipedia.org/wiki/Partial_order_poset)
//      (see https://en.wikipedia.org/wiki/Allegro_LISP)
//       (see https://en.wikipedia.org/wiki/Allegro_LISP#Allegro_LISP)
//
//
//




#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[derive(Serialize, Deserialize)]
pub enum EvalType {
    Int = 0,
    Real = 1,
    Decimal = 2,
    Datetime = 3,
    Duration = 4,
    Bytes = 5,
    String = 6,
    Json = 7,
    Enum = 8,
    Set = 9,
    Bit = 10,
    Tiny = 11,
    Small = 12,
    Medium = 13,
    Big = 14,
    Null = 15,
    Max = 16,
}


#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Causet<T> {

    pub causet: Vec<T>,

    //foundationdb is a database that is a key value store
    //      (see https://foundationdb.com/documentation/developer-guide.html)

    //we need to be able to store the causet of causets in the database
    //for this use Causet of Causets as the key
    //and the solitonid of causets as the value for each event in the causet of causets

    //the set of elements in the Causet
    pub elements: Vec<T>,

    //we need planar geometry
    //      (see https://en.wikipedia.org/wiki/Planar_graph)
    pub planar_graph: Vec<Vec<usize>>,

    //we need to be able to store the causet of causets in the database
    //for this use Causet of Causets as the key
    //and the solitonid of causets as the value for each event in the causet of causets

    //the set of elements in the Causet
    pub causet_of_causets: Vec<Causet<T>>,

    //we need to be able to store the causet of causets in the database
    //for this use Causet of Causets as the key
    //and the solitonid of causets as the value for each event in the causet of causets

    //the set of elements in the Causet
    pub causet_of_causets_of_causets: Vec<Causet<T>>,

    //the set of causets that are in the Causet of Causets
    //      (see https://en.wikipedia.org/wiki/Causet_of_causets)

    pub causets: Vec<Causet<T>>,

    //now we need an object to cache the foundationdb connection
    #[allow(dead_code)]
    pub fdb_connection: FdbConnection, //FdbConnection is a struct that is a connection to the foundationdb database
}


fn a_causet_into_graph<T>(causet: &Causet<T>) -> Vec<Vec<usize>>
where
    T: Clone + Eq + Hash + Display,
{
    let mut graph = Vec::new();
    for i in 0..causet.causets.len() {
        let mut causet_graph = Vec::new();
        for j in 0..causet.causets[i].causets.len() {
            causet_graph.push(causet.causets[i].causets[j].solutionid);
        }
        graph.push(causet_graph);
    }
    graph
}


fn a_causet_into_elements<T>(causet: &Causet<T>) -> Vec<T>
where
    T: Clone + Eq + Hash + Display,
{
    let mut elements = Vec::new();
    for i in 0..causet.causets.len() {
        for j in 0..causet.causets[i].causets.len() {
            elements.push(causet.causets[i].causets[j].solutionid);
        }
    }
    elements
}

fn from_einstein_merkle_to_causet<T>(einstein_merkle: &EinsteinMerkle<T>) -> Causet<T>
where
    T: Clone + Eq + Hash + Display,
{
    let mut causet = Causet::default();
    causet.causets = einstein_merkle.causets.clone();
    causet.elements = a_causet_into_elements(&causet);
    causet.planar_graph = a_causet_into_graph(&causet);
    causet
}
//lets work on that foundationdb connection; if there exists one then we can use it
//if there does not exist one then we need to create one

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct FdbConnection {
    pub fdb: Fdb,
    pub fdb_connection: FdbResult<PostgresConnection>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct CausetOfCausets<T> {
    pub causet_of_causets: Causet<T>,
    pub causet_of_causets_of_causets: Causet<T>,
}


impl Causet<String> {
    pub fn new(elements: Vec<String>) -> Causet<String> {
        //relativistic time
        //      (see https://en.wikipedia.org/wiki/Relativistic_time)

        let causet_of_causets = Causet::new(elements);
        for causet in causet_of_causets.causets.iter() // iterate over the causets in the causet of causets
        {
            ///! we need to add the causet of causets to the causet of causets of causets index in the database
            let causet_of_causets_of_causets = Causet::new(causet.elements.clone());
            ///! now we instantiate the causet of causets of causets
            causet.causets.push(causet_of_causets_of_causets);
        }
        Causet {
            elements: elements,
            planar_graph: vec![],
            causets: causet_of_causets.causets,
            fdb_connection: FdbConnection {
                fdb: Fdb::new(),
                fdb_connection: FdbResult::Ok(PostgresConnection::new("localhost", 5432, "postgres", "postgres", "postgres")),
            },
        }
    }

    pub fn add_causet(&mut self, causet: Causet<String>) {
        self.causets.push(causet);
    }

    pub fn add_causet_of_causets(&mut self, causet_of_causets: Causet<String>) {
        self.causets.push(causet_of_causets);
    }


    pub fn add_causet_of_causets_of_causets(&mut self, causet_of_causets_of_causets: Causet<String>) {
        self.causets.push(causet_of_causets_of_causets);
    }


    pub fn add_causet_of_causets_of_k8s(&mut self, causet_of_causets_of_causets_of_causets: Causet<String>) {

        //k8s
        //      (see https://en.wikipedia.org/wiki/Kubernetes)

        let k8s_causet = Causet::new(vec!["k8s".to_string()]);

        let k8s_solitonid_fetch = self.fdb_connection.fdb_connection.unwrap().query("SELECT solitonid FROM causet WHERE elements = 'k8s'", &[]).unwrap();

        fn get_solitonid_from_row(row: &Row) -> i32 {
            row.get(0)
        }

        if k8s_solitonid_fetch.len() == 0 {
            struct K8sCauset {
                pub fn new_k8s_causet() -> Causet < String > {
                let k8s_causet = Causet::new(vec ! ["k8s".to_string()]);
                k8s_causet

                }

                match k8s_solitonid_fetch.iter().map(get_solitonid_from_row).next() {
                Some(solitonid) => {
                let k8s_causet = Causet::new(vec ! ["k8s".to_string()]);
                k8s_causet
                },
                None => {
                let k8s_causet = Causet::new(vec ! ["k8s".to_string()]);
                k8s_causet
                },
                }   //end of match
            }   //end of fnamespaced_opts
            if k8s_solitonid_fetch.len() == 0 {
                let k8s_causet = Causet::new(vec!["k8s".to_string()]);
                self.causets.push(k8s_causet);
            } else {
                let k8s_causet = Causet::new(vec!["k8s".to_string()]);
                self.causets.push(k8s_causet);
                for causet in self.causets.iter() {
                    if causet.elements[0] == "k8s" {
                        self.causets.push(causet_of_causets_of_causets_of_causets);
                    }
                }
            }
        }
        for causet in self.causets.iter() {
            if causet.elements[0] == "k8s" {
                self.causets.push(causet_of_causets_of_causets_of_causets);
            }
        }
        for causet in self.causets.iter() {
            if causet.elements[0] == "k8s" {
                self.causets.push(causet_of_causets_of_causets_of_causets);
            }
        }
    }
}

///changelog: we are ready to k8s solitonid_range
    let k8s_solitonid = k8s_solitonid_fetch.iter().map( | row| get_solitonid_from_row(row)).collect::<Vec<i32> > ();
        let k8s_solitonid = k8s_solitonid.iter().cloned().collect::<Vec<i32> > (){
    //mspc with lock free interlock
    //      (see https://en.wikipedia.org/wiki/MSPC_with_lock-free_interlock)
    //      (see https://en.wikipedia.org/wiki/Lock-free_interlocked_queue)
    match self.fdb_connection.fdb_connection.unwrap().query("SELECT solitonid FROM causet WHERE elements = 'k8s'", &[]).unwrap() {
        Ok(k8s_solitonid_fetch) => {
            let k8s_solitonid = k8s_solitonid_fetch.iter().map( | row| get_solitonid_from_row(row)).collect::<Vec<i32> > ();
            let k8s_solitonid = k8s_solitonid.iter().cloned().collect::<Vec<i32> > ()
        },
        Err(e) => {
            println!("{:?}", e);
        }

    fn get_solitonid_from_row(row: &Row) -> i32 {
        row.get(0)

for i in 0..k8s_causet.causets.len() {
    k8s_causet.causets[i].solutionid = k8s_solitonid;
        wait_for_k8s_to_be_ready();
    match self.fdb_connection.fdb_connection.unwrap().query("SELECT solitonid FROM causet WHERE elements = 'k8s'", &[]).unwrap() {
    let k8s_solitonid = k8s_solitonid_fetch.iter().map( | row| get_solitonid_from_row(row)).collect::<Vec<i32> > ();
    for i in 0..k8s_causet.causets.len() {
        k8s_causet.causets[i].solutionid = k8s_solitonid;
    }
        let k8s_causet_of_causets = Causet::new(vec!["k8s".to_string()]);
        let k8s_solitonid = k8s_solitonid as usize;
        for causet in k8s_causet.causets.iter() // iterate over the causets in the causet of causets
        {
            ///! we need to add the causet of causets to the causet of causets of causets index in the database
            let causet_of_causets_of_causets = Causet::new(causet.elements.clone());
            ///! now we instantiate the causet of causets of causets
            self.causets[k8s_solitonid].causets.push(causet_of_causets_of_causets);
        while self.causets[k8s_solitonid].causets.len() < 1 {
            self.causets[k8s_solitonid].causets.push(causet_of_causets_of_causets).unwrap();
            //connect with wait and suspend
            //      (see https://en.wikipedia.org/wiki/Connect_and_wait)
            //      (see https://en.wikipedia.org/wiki/Suspend_and_resume)

            //connect with wait and suspend
            //      (see https://en.wikipedia.org/wiki/Connect_and_wait)
            //      (see https://en.wikipedia.org/wiki/Suspend_and_resume)

            //connect with wait and suspend


            fn get_solitonid_from_row(row: &Row) -> i32 {
                row.get(0)
            }

            //
            // elements,
            // planar_graph: (),
            // causets: (),
            // fdb_connection: FdbConnection {
            //     //enter kubernetes cluster
            //     fdb: Fdb::new(),
            //     fdb_connection: FdbResult::Ok(PostgresConnection::new("foundationdb://
            //     ").unwrap()),
            // }, //enter kubernetes cluster
        }
    }


    pub fn new_from_vec(elements: Vec<&str>) -> Causet<String> {
        let mut new_elements = Vec::new();
        for element in elements {
            new_elements.push(element.to_string());
        }
        Causet {
            elements: new_elements,
            planar_graph: (),
            causets: (),
            fdb_connection: FdbConnection { fdb: (), fdb_connection: () }
        }
    }
}
    pub fn new_from_vec_ref(elements: Vec<&str>) -> Causet<String> {
        let mut new_elements = Vec::new();
        for element in elements {
            new_elements.push(element.to_string());
        }
        Causet {
            elements: new_elements,
            planar_graph: (),
            causets: (),
            fdb_connection: FdbConnection { fdb: (), fdb_connection: () }
        }
    }

    pub fn new_from_vec_ref_mut(elements: Vec<&str>) -> Causet<String> {
        let mut new_elements = Vec::new();
        for element in elements {
            new_elements.push(element.to_string());
        }
        Causet {
            elements: new_elements,
            planar_graph: (),
            causets: (),
            fdb_connection: FdbConnection { fdb: (), fdb_connection: () }
        }
    }


    pub fn new_from_vec_ref_mut_mut_mut(elements: Vec<&str>) -> Causet<String> {
        let mut new_elements = Vec::new();
        for element in elements {
            new_elements.push(element.to_string());
        }
        Causet {
            elements: new_elements,
            planar_graph: (),
            causets: (),
            fdb_connection: FdbConnection { fdb: (), fdb_connection: () }
        }
    }


    ///! TODO: implement this with_capacity method to bin the elements in the causet
    pub fn with_capacity(capacity: usize) -> Causet<T> {
        Causet {
            elements: Vec::with_capacity(capacity),
            planar_graph: (),
            causets: (),
            fdb_connection: FdbConnection { fdb: (), fdb_connection: () }
        }
    }

    //constructor
    pub fn with_elements(elements: Vec<T>) -> Causet<T> {
        Causet {
            elements,
            planar_graph: (),
            causets: (),
            fdb_connection: FdbConnection { fdb: (), fdb_connection: () }
        }
    }

    //constructor
    pub fn with_element(element: T) -> Causet<T> {
        Causet {
            elements: vec![element],
            planar_graph: (),
            causets: (),
            fdb_connection: FdbConnection { fdb: (), fdb_connection: () }
        }
    }

    //constructor
    pub fn with_elements_and_capacity(elements: Vec<T>, capacity: usize) -> Causet<T> {
        Causet {
            elements,
            planar_graph: (),
            causets: (),
            fdb_connection: FdbConnection { fdb: (), fdb_connection: () }
        }
    }












