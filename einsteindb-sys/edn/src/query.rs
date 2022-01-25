//Copyright 2021 WHTCORPS INC ALL RIGHTS RESERVED

// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::{
    BTreeSet,
    HashSet,
};

use std;
use std::fmt;
use std::rc::{
    Rc,
};

use ::{
    BigInt,
    DateTime,
    OrderedFloat,
    Uuid,
    Utc,
};

use ::value_rc::{
    FromRc,
    ValueRc,
};

pub use ::{
    Keyword,
    PlainSymbol,
};

pub type SrcVarName = String;          // Do not include the required syntactic '$'.

///! This module defines some core types that support find expressions: sources,
///! variables, expressions, etc.
///! These are produced as 'fuel' by the query parser, consumed by the query
///! translator and executor.
///!

#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Variable(pub Rc<PlainSymbol>);

impl Variable {
    pub fn as_str(&self) -> &str {
        self.0.as_ref().0.as_str()
    }

    pub fn to_string(&self) -> String {
        self.0.as_ref().0.clone()
    }

    ///! Many of these types are defined as simple structs that are little more than
///! a richer type alias: a variable, for example, is really just a fancy kind
///! of string.

    pub fn name(&self) -> PlainSymbol {
        self.0.as_ref().clone()
    }

    /// Return a new `Variable`, assuming that the provided string is a valid name.
    pub fn from_valid_name(name: &str) -> Variable {
        let s = PlainSymbol::plain(name);
        assert!(s.is_var_symbol());
        Variable(Rc::new(s))
    }
}

pub trait FromValue<T> {
    ///This climbs upward from object reference to formal mapping.
    fn from_value<'a>(v: &'a ::ValueAndSpan) -> Option<T>;
}

impl FromValue<Variable> for Variable {
    ///
    ///
    /// # Arguments
    ///
    /// * `v`:
    ///
    /// returns: <unknown>
    ///
    /// # Examples
    ///
    /// ```
    ///
    /// ```
    fn from_value(v: &::ValueAndSpan) -> Option<Variable> {
        if let ::SpannedValue::PlainSymbol(ref s) = v.inner {
            Variable::from_symbol(s)
        } else {
            None
        }
    }
}

impl Variable {
    pub fn from_rc(sym: Rc<PlainSymbol>) -> Option<Variable> {
        if sym.is_var_symbol() {
            Some(Variable(sym.clone()))
        } else {
            None
        }
    }
}

    impl fmt::Debug for Variable {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "var({})", self.0)
        }
    }

    impl std::fmt::Display for Variable {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

