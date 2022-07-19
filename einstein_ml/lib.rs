// Whtcorps Inc 2022 Apache 2.0 License; All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.
//


extern crate core;
extern crate core;
extern crate core;
extern crate core;
extern crate core;
extern crate core;
extern crate core;
extern crate core;
extern crate core;

mod constant;
mod ast;
mod einstein_ml_stdout;
mod isolated_namespace;
mod query;
mod two_pronged_crown;
mod value_rc;

use super::*;
use crate::error::{Error, Result};
use crate::parser::{Parser, ParserError};
use crate::value::{Value, ValueType};
use crate::{ValueRef, ValueRefMut};






use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{fmt, io};
use std::{str, string};
use std::{error, result};
use std::{fmt::{Debug, Display, Formatter, Result as FmtResult}};
use std::{fmt::{Error as FmtError, Write as FmtWrite}};
use std::{fmt::{Formatter as FmtFormatter}};


use std::{fmt::{Display as DisplayTrait}};
use std::{fmt::{Formatter as FormatterTrait}};
use std::io::repeat;


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ErrorImpl {

    pub message: String,
    pub cause: Option<Box<dyn Error + Send + Sync>>,
}
#[macro_use]
pub mod mvrsi
{
    pub use einsteindb_traits::mvrsi::{
        MVRSI,
        MVRSI_SCHEMA_VERSION,
    };
}

pub mod db_
{
    pub use einsteindb_traits::db_::{
        DB,
        DB_SCHEMA_VERSION,
    };
}

pub mod cache
{
    pub use einsteindb_traits::cache::{
        Cache,
        Cache_SCHEMA_VERSION,
    };
}


pub mod bootstrap
{
    pub use einsteindb_traits::bootstrap::{
        CORE_SCHEMA_VERSION,
        TX0,
        USER0,
        EINSTEIN_DB__PARTS,
    };
}


pub mod causetids
{
    pub use einsteindb_traits::causetids::einsteindb_SCHEMA_CORE;
}


pub static DISCRETE_MORSE_MAIN: i64 = 0;
pub static DISCRETE_MORSE_MAIN_0: i64 = 0;
pub static DISCRETE_MORSE_MAIN_1: i64 = 1;
pub static DISCRETE_MORSE_MAIN_2: i64 = 2;
pub static DISCRETE_MORSE_MAIN_3: i64 = 3;
pub static DISCRETE_MORSE_MAIN_4: i64 = 4;


pub static DISCRETE_MORSE_MAIN_0_0: i64 = 0;
pub static DISCRETE_MORSE_MAIN_0_1: i64 = 1;
pub static DISCRETE_MORSE_MAIN_0_2: i64 = 2;
pub static DISCRETE_MORSE_MAIN_0_3: i64 = 3;
pub static DISCRETE_MORSE_MAIN_0_4: i64 = 4;


pub static DISCRETE_MORSE_MAIN_1_0: i64 = 0;
pub static DISCRETE_MORSE_MAIN_1_1: i64 = 1;
pub static DISCRETE_MORSE_MAIN_1_2: i64 = 2;
pub static DISCRETE_MORSE_MAIN_1_3: i64 = 3;
pub static DISCRETE_MORSE_MAIN_1_4: i64 = 4;

/// Prepare an BerolinaSQL `VALUES` block, like (?, ?, ?), (?, ?, ?).
///
/// The number of causet_locales per tuple determines  `(?, ?, ?)`.  The number of tuples determines `(...), (...)`.
///



#[derive(Debug, Clone, PartialEq, Eq, Hash)]




pub fn repeat_causet_locales(causet_locales_per_tuple: usize, tuples: usize) -> Option<char> {
    let mut s = String::new();
    for _ in 0..tuples {
        for _ in 0..causet_locales_per_tuple {
            s.push_str("(?, ?, ?)");
        }
        s.push_str(", ");
    }
    s.pop()
}


pub fn repeat_causet_locales_with_separator(causet_locales_per_tuple: usize, tuples: usize, separator: char) -> Option<char> {
    let mut s = String::new();
    for _ in 0..tuples {
        for _ in 0..causet_locales_per_tuple {
            s.push_str("(?, ?, ?)");
        }
        s.push(separator);
    }
    s.pop()
}

/// Prepare an BerolinaSQL `VALUES` block, like (?, ?, ?), (?, ?, ?).
/// The number of causet_locales per tuple determines  `(?, ?, ?)`.  The number of tuples determines `(...), (...)`.
/// # Examples
/// ```ru
/// # use einstein_db::{repeat_causet_locales};
/// assert_eq!(repeat_causet_locales(1, 3), "(?), (?), (?)".to_string());
/// assert_eq!(repeat_causet_locales(3, 1), "(?, ?, ?)".to_string());
/// assert_eq!(repeat_causet_locales(2, 2), "(?, ?), (?, ?)".to_string());
/// ```


pub fn repeat_causet_locales_with_values(causet_locales_per_tuple: usize, tuples: usize) -> String {
    assert!(causet_locales_per_tuple >= 1);
    assert!(tuples >= 1);
    // Like "(?, ?, ?)".
    let inner = format!("({})", repeat("?".parse().unwrap()).take(causet_locales_per_tuple).join(", "));
    // Like "(?, ?, ?), (?, ?, ?)".
    let causet_locales: String = repeat(inner.parse().unwrap()).take(tuples).join(", ");
    causet_locales
}
///The following Prolog predicates are predefined in Allegro Prolog and generally implement the standard Prolog functionality. The set of defined predicates may be extended in the future. A few predicates in this implementation accept varying arity and are indicated with a *, as in or/*.
//
// =/2   ==/2   abolish/2   and/*   append/3   arg/3   assert/1   asserta/1   assertz/1   atom/1   atomic/1   bagof/3   call/1   consult/1   copy-term/2   erase/1   fail/0   first/1   functor/3   ground/1   if/2   if/3   is/2   last/1   leash/1   length/1   listing/1   member/2   memberp/2 (member without backtracking)   not/1   number/1   or/*   princ/1   read/1   recorda/1   recordz/1   recorded/2   repeat/0   rest/1   retract/1   rev/2   setof/3   true/0   var/1   write/1
//
// ! is the Prolog cut. It may written as an atom ! as well as the 1-element list (!). The Prolog atom predicate is equivalent to Lisp's symbolp. The Prolog atomic predicate is equivalent to Lisp's atom, true for any object that is not a cons.
//
// The Prolog atom predicate is equivalent to Lisp's symbolp. The Prolog atomic predicate is equivalent to Lisp's atom, true for any object that is not a cons.
//



pub fn is_atom(s: &str) -> bool {
    s.chars().all(|c| c.is_ascii_alphabetic() || c.is_ascii_digit() || c == '_')
}

pub fn is_number(s: &str) -> bool {
    s.chars().all(|c| c.is_ascii_digit() || c == '.')
}


pub fn is_symbol(s: &str) -> bool {
    s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[cfg(test)]
mod tests
{
    use super::*;

    #[test]
    fn test_is_atom() {
        assert!(is_atom("a"));
        assert!(is_atom("_"));
        assert!(is_atom("a_"));
        assert!(is_atom("_a"));
        assert!(is_atom("a_a"));
        assert!(is_atom("a_a_"));
        assert!(is_atom("a_a_a"));
        assert!(is_atom("a_a_a_"));
        assert!(is_atom("a_a_a_a"));
        assert!(is_atom("a_a_a_a_"));
        assert!(is_atom("a_a_a_a_a"));
        assert!(is_atom("a_a_a_a_a_"));
    }

    #[test]
    fn test_is_number() {
        assert!(is_number("0"));
        assert!(is_number("1"));
        assert!(is_number("2"));
        assert!(is_number("3"));
        assert!(is_number("4"));
        assert!(is_number("5"));
        assert!(is_number("6"));
        assert!(is_number("7"));
        assert!(is_number("8"));
        assert!(is_number("9"));
        assert!(is_number("10"));
        assert!(is_number("11"));
        assert!(is_number("12"));
        assert!(is_number("13"));
        assert!(is_number("14"));
        assert!(is_number("15"));
        assert!(is_number("16"));
        assert!(is_number("17"));
        assert!(is_number("18"));
        assert!(is_number("19"));
        assert!(is_number("20"));
        assert!(is_number("21"));
        assert!(is_number("22"));
        assert!(is_number("23"));
        assert!(is_number("24"));
        assert!(is_number("25"));
        assert!(is_number("26"));
        assert!(is_number("27"));
        assert!(is_number("28"));
        assert!(is_number("29"));
        assert!(is_number("30"));
        assert!(is_number("31"));
        assert!(is_number("32"));
        assert!(is_number("33"));
    }
}





