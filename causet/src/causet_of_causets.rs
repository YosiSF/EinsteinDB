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
    pub elements: Vec<T>,
}

impl Causet<String> {
    pub fn new(elements: Vec<String>) -> Causet<String> {
        Causet {
            elements: elements,
        }
    }

    pub fn new_from_vec(elements: Vec<&str>) -> Causet<String> {
        let mut new_elements = Vec::new();
        for element in elements {
            new_elements.push(element.to_string());
        }
        Causet {
            elements: new_elements,
        }
    }

    pub fn new_from_vec_ref(elements: Vec<&str>) -> Causet<String> {
        let mut new_elements = Vec::new();
        for element in elements {
            new_elements.push(element.to_string());
        }
        Causet {
            elements: new_elements,
        }
    }

    pub fn new_from_vec_ref_mut(elements: Vec<&str>) -> Causet<String> {
        let mut new_elements = Vec::new();
        for element in elements {
            new_elements.push(element.to_string());
        }
        Causet {
            elements: new_elements,
        }
    }

    pub fn new_from_vec_ref_mut_mut(elements: Vec<&str>) -> Causet<String> {
        let mut new_elements = Vec::new();
        for element in elements {
            new_elements.push(element.to_string());
        }
        Causet {
            elements: new_elements,
        }
    }

    pub fn new_from_vec_ref_mut_mut_mut(elements: Vec<&str>) -> Causet<String> {
        let mut new_elements = Vec::new();
        for element in elements {
            new_elements.push(element.to_string());
        }
        Causet {
            elements: new_elements,
        }
    }

    pub fn new_from_vec_ref_mut_mut_mut_mut(elements: Vec<&str>) -> Causet<String> {
        let mut new_elements = Vec::new();
        for element in elements {
            new_elements.push(element.to_string());
        }
        Causet {
            elements: new_elements,
        }
    }
    //constructor

    //constructor
    pub fn with_capacity(capacity: usize) -> Causet<T> {
        Causet {
            elements: Vec::with_capacity(capacity),
        }
    }

    //constructor
    pub fn with_elements(elements: Vec<T>) -> Causet<T> {
        Causet {
            elements: elements,
        }
    }

    //constructor
    pub fn with_element(element: T) -> Causet<T> {
        Causet {
            elements: vec![element],
        }
    }

    //constructor
    pub fn with_elements_and_capacity(elements: Vec<T>, capacity: usize) -> Causet<T> {
        Causet {
            elements: elements,
        }
    }
}











