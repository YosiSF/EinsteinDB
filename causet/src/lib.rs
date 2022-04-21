//Copyright (c) 2019-present, Whtcorps Inc.

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
/// ```









































