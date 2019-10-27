//Copyright 2019 EinsteinDB

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

pub type SrcVarName = String; //exclude syntactic '$'.

#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Variable(pub Rc<PlainSymbol>);

impl Variable {
    pub fn as_str(&self) -> &str {
        self.0.as_ref().0.as_str()
    }

    pub fn to_string(&self) -> String {
        self.0.as_ref().0.clone()
    }

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
    fn from_value(v: &::ValueAndSpan) -> Option<T>;
}

/// If the provided EDB value is a PlainSymbol beginning with '?', return
/// it wrapped in a Variable. If not, return None.

impl FromValue<Variable> for Variable {
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

    /// TODO: intern strings. #398.
    pub fn from_symbol(sym: &PlainSymbol) -> Option<Variable> {
        if sym.is_var_symbol() {
            Some(Variable(Rc::new(sym.clone())))
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


#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct QueryFunction(pub PlainSymbol);

impl FromValue<QueryFunction> for QueryFunction {
    fn from_value(v: &::ValueAndSpan) -> Option<QueryFunction> {
        if let ::SpannedValue::PlainSymbol(ref s) = v.inner {
            QueryFunction::from_symbol(s)
        } else {
            None
        }
    }
}

impl QueryFunction {
    pub fn from_symbol(sym: &PlainSymbol) -> Option<QueryFunction> {
        // TODO: validate the acceptable set of function names.
        Some(QueryFunction(sym.clone()))
    }
}

impl std::fmt::Display for QueryFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Direction {
    Ascending,
    Descending,
}

/// An abstract declaration of ordering: direction and variable.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Order(pub Direction, pub Variable);   // Future: Element instead of Variable?

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum SrcVar {
    DefaultSrc,
    NamedSrc(SrcVarName),
}

impl FromValue<SrcVar> for SrcVar {
    fn from_value(v: &::ValueAndSpan) -> Option<SrcVar> {
        if let ::SpannedValue::PlainSymbol(ref s) = v.inner {
            SrcVar::from_symbol(s)
        } else {
            None
        }
    }
}

impl SrcVar {
    pub fn from_symbol(sym: &PlainSymbol) -> Option<SrcVar> {
        if sym.is_src_symbol() {
            if sym.0 == "$" {
                Some(SrcVar::DefaultSrc)
            } else {
                Some(SrcVar::NamedSrc(sym.name().to_string()))
            }
        } else {
            None
        }
    }
}

/// These are the scalar values representable in EDN.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NonIntegerConstant {
    Boolean(bool),
    BigInteger(BigInt),
    Float(OrderedFloat<f64>),
    Text(ValueRc<String>),
    Instant(DateTime<Utc>),
    Uuid(Uuid),
}

impl<'a> From<&'a str> for NonIntegerConstant {
    fn from(val: &'a str) -> NonIntegerConstant {
        NonIntegerConstant::Text(ValueRc::new(val.to_string()))
    }
}

impl From<String> for NonIntegerConstant {
    fn from(val: String) -> NonIntegerConstant {
        NonIntegerConstant::Text(ValueRc::new(val))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FnArg {
    Variable(Variable),
    SrcVar(SrcVar),
    CausetIddOrInteger(i64),
    IdentOrKeyword(Keyword),
    Constant(NonIntegerConstant),
    // The collection values representable in EDN.  There's no advantage to destructuring up front,
    // since consumers will need to handle arbitrarily nested EDN themselves anyway.
    Vector(Vec<FnArg>),
}

impl FromValue<FnArg> for FnArg {
    fn from_value(v: &::ValueAndSpan) -> Option<FnArg> {
        use ::SpannedValue::*;
        match v.inner {
            Integer(x) =>
                Some(FnArg::EntidOrInteger(x)),
            PlainSymbol(ref x) if x.is_src_symbol() =>
                SrcVar::from_symbol(x).map(FnArg::SrcVar),
            PlainSymbol(ref x) if x.is_var_symbol() =>
                Variable::from_symbol(x).map(FnArg::Variable),
            PlainSymbol(_) => None,
            Keyword(ref x) =>
                Some(FnArg::IdentOrKeyword(x.clone())),
            Instant(x) =>
                Some(FnArg::Constant(NonIntegerConstant::Instant(x))),
            Uuid(x) =>
                Some(FnArg::Constant(NonIntegerConstant::Uuid(x))),
            Boolean(x) =>
                Some(FnArg::Constant(NonIntegerConstant::Boolean(x))),
            Float(x) =>
                Some(FnArg::Constant(NonIntegerConstant::Float(x))),
            BigInteger(ref x) =>
                Some(FnArg::Constant(NonIntegerConstant::BigInteger(x.clone()))),
            Text(ref x) =>
                // TODO: intern strings. #398.
                Some(FnArg::Constant(x.clone().into())),
            Nil |
            NamespacedSymbol(_) |
            Vector(_) |
            List(_) |
            Set(_) |
            Map(_) => None,
        }
    }
}

// For display in column headings in the repl.
impl std::fmt::Display for FnArg {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            &FnArg::Variable(ref var) => write!(f, "{}", var),
            &FnArg::SrcVar(ref var) => {
                if var == &SrcVar::DefaultSrc {
                    write!(f, "$")
                } else {
                    write!(f, "{:?}", var)
                }
            },
            &FnArg::CausetIdOrInteger(causetId) => write!(f, "{}", causetId),
            &FnArg::IdentOrKeyword(ref kw) => write!(f, "{}", kw),
            &FnArg::Constant(ref constant) => write!(f, "{:?}", constant),
            &FnArg::Vector(ref vec) => write!(f, "{:?}", vec),
        }
    }
}

impl FnArg {
    pub fn as_variable(&self) -> Option<&Variable> {
        match self {
            &FnArg::Variable(ref v) => Some(v),
            _ => None,
        }
    }
}
