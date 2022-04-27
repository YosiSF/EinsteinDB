//Copyright (c) 2022, <MIT License> Copyright (c) 2022, <Karl Whitford and Josh Leder>

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




//implementation of the Causet of Causet

//Allegro_Poset is an Allegro Lisp based off of the Partial Partition Poset of Causets  (POSET)
//      (see https://en.wikipedia.org/wiki/Partial_order_poset)
//      (see https://en.wikipedia.org/wiki/Allegro_LISP)
//       (see https://en.wikipedia.org/wiki/Allegro_LISP#Allegro_LISP)
//
//
//







#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Causet<T> {

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


    //the set of causets that are in the Causet of Causets
    //      (see https://en.wikipedia.org/wiki/Causet_of_causets)

    pub causets: Vec<Causet<T>>,

    //now we need an object to cache the foundationdb connection
    #[allow(dead_code)]
    pub fdb_connection: FdbConnection, //FdbConnection is a struct that is a connection to the foundationdb database
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

        Causet {
            elements,
            planar_graph: (),
            causets: (),
            fdb_connection: FdbConnection {
                //enter kubernetes cluster
                fdb: Fdb::new(),
                fdb_connection: FdbResult::Ok(PostgresConnection::new("foundationdb://
                ").unwrap()),
            }, //enter kubernetes cluster
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












