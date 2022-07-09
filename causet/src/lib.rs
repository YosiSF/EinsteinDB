//Copyright (c) 2019-present, Whtcorps Inc.


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









