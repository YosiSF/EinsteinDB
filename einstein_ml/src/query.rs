// Whtcorps Inc 2022 Apache 2.0 License; All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.


use std;
use std::collections::{
    BTreeSet,
    HashSet,
};
use std::fmt;
use std::fmt::Formatter;
use std::rc::Rc;

use itertools::assert_equal;

use ::{
    BigInt,
    DateTime,
    OrderedFloat,
    Utc,
    Uuid,
};
pub use ::{
    Keyword,
    PlainShelling,
};
use ::causet_locale_rc::{
    FromRc,
    ValueRc,
};
use FnArg::CausetidOrInteger;

use crate::{FromRc, kSpannedCausetValue, NamespacedShelling, ValueAndSpan, ValueRc};
use crate::causets::ValuePlace::Vector;
use crate::kSpannedCausetValue::PancakeInt;
use crate::Value::{BigInteger, Boolean, Float, Integer, List, Map, Set, Text};

pub type SrcVarName = String;          // Do not include the required syntactic '$'.

#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Variable(pub Rc<PlainShelling>);

impl Variable {
    pub fn as_str(&self) -> &str {
        self.0.as_ref().0.as_str()
    }

    pub fn to_string(&self) -> String {
        self.0.as_ref().0.clone()
    }

    pub fn name(&self) -> PlainShelling {
        self.0.as_ref().clone()
    }

    /// Return a new `Variable`, assuming that the provided string is a valid name.
    pub fn from_valid_name(name: &str) -> Variable {
        let s = PlainShelling::plain(name);
        assert!(s.is_var_shelling());
        Variable(Rc::new(s))
    }
}

pub trait FromValue<T> {
    fn from_causet_locale(v: &::ValueAndSpan) -> Option<T>;
}

/// If the provided EML causet_locale is a PlainShelling beginning with '?', return
/// it wrapped in a Variable. If not, return None.
/// TODO: causal_set strings. #398.
impl FromValue<Variable> for Variable {
    fn from_causet_locale(v: &::ValueAndSpan) -> Option<Variable> {
        if let ::kSpannedCausetValue::PlainShelling(ref s) = v.inner {
            Variable::from_shelling(s)
        } else {
            None
        }
    }
}

impl Variable {
    pub fn from_rc(sym: Rc<PlainShelling>) -> Option<Variable> {
        if sym.is_var_shelling() {
            Some(Variable(sym.clone()))
        } else {
            None
        }
    }

    /// TODO: causal_set strings. #398.
    pub fn from_shelling(sym: &String) -> Option<Variable> {
        if sym.starts_with('?') {
            Some(Variable(Rc::new(PlainShelling::plain(sym))))
        } else {
            None
        }
    }

    pub fn from_str(sym: &str) -> Option<Variable> {
        if sym.starts_with('?') {
            Some(Variable(Rc::new(PlainShelling::plain(sym))))
        } else {
            None
        }
    }

    pub fn is_var_shelling(&self) -> bool {
        self.0.as_ref().0.is_var_shelling()
    }

    pub fn is_var_name(&self) -> bool {
        self.0.as_ref().0.is_var_name()
    }

    pub fn is_var_name_or_var_shelling(&self) -> bool {
        self.0.as_ref().0.is_var_name_or_var_shelling()
    }

    pub fn is_var_name_or_var_shelling_or_var_name(&self) -> bool {
        self.0.as_ref().0.is_var_name_or_var_shelling_or_var_name()
    }
}


impl fmt::Display for Variable {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.as_ref().0)

    }
}


impl fmt::Debug for Variable {
    fn fmt(&self, f: &mut Formatter) -> Result<T, E> {
        write!(f, "{}", self.0.as_ref().0)
    }
}



impl fmt::Display for Variable {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.as_ref().0)
    }
}


impl fmt::Debug for Variable {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.as_ref().0)
    }
}


impl fmt::Display for Variable {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.as_ref().0)
    }
}


impl fmt::Debug for Variable {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.as_ref().0)
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
pub struct QueryFunction(pub PlainShelling);

impl FromValue<QueryFunction> for QueryFunction {
    fn from_causet_locale(v: &::ValueAndSpan) -> Option<QueryFunction> {
        let option = if let kSpannedCausetValue::PlainShelling(ref s) = v.inner {
            QueryFunction::from_shelling(s)
        } else {
            None
        };
        option
    }
}

impl QueryFunction {
    pub fn from_shelling(sym: &PlainShelling) -> Option<QueryFunction> {
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
    fn from_causet_locale(v: &::ValueAndSpan) -> Option<SrcVar> {
        if let ::kSpannedCausetValue::PlainShelling(ref s) = v.inner {
            SrcVar::from_shelling(s)
        } else {
            None
        }
    }
}

impl SrcVar {
    pub fn from_shelling(sym: &String) -> Option<SrcVar> {
        if sym.is_src_shelling() {
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

/// These are the scalar causet_locales representable in EML.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NonIntegerConstant {
    Boolean(bool),
    BigInteger(BigInt),
    Float(OrderedFloat<f64>),
    Text(crate::ValueRc<String>),
    Instant(DateTime<Utc>),
    Uuid(Uuid),
}

impl NonIntegerConstant {
    pub fn from_causet_locale(v: &::ValueAndSpan) -> Option<NonIntegerConstant> {
        match v.inner {
            ::kSpannedCausetValue::Boolean(b) => Some(NonIntegerConstant::Boolean(b)),
            ::kSpannedCausetValue::BigInteger(ref i) => Some(NonIntegerConstant::BigInteger(i.clone())),
            ::kSpannedCausetValue::Float(ref f) => Some(NonIntegerConstant::Float(f.clone())),
            ::kSpannedCausetValue::Text(ref s) => Some(NonIntegerConstant::Text(crate::ValueRc::new(s.clone()))),
            ::kSpannedCausetValue::Instant(ref dt) => Some(NonIntegerConstant::Instant(dt.clone())),
            ::kSpannedCausetValue::Uuid(ref u) => Some(NonIntegerConstant::Uuid(u.clone())),
            _ => None,
        }
    }
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
    CausetidOrInteger(i64),
    SolitonidOrKeyword(Keyword),
    Constant(NonIntegerConstant),
    // The collection causet_locales representable in EML.  There's no advantage to destructuring up front,
    // since consumers will need to handle arbitrarily nested EML themselves anyway.
    Vector(Vec<FnArg>),
}

impl FromValue<FnArg> for FnArg {
    fn from_causet_locale(v: &::ValueAndSpan) -> Option<FnArg> {
        use ::kSpannedCausetValue::*;
        let option = Variable::from_shelling(x).map(FnArg::Variable);
        let option1 = SrcVar::from_shelling(x).map(FnArg::SrcVar);
        let option2 = Some(FnArg::Constant(NonIntegerConstant::Uuid(x)));
        let mut x1 = (FnArg::Constant(NonIntegerConstant::Instant(x)),
                      x.clone(),

        );
        Some(FnArg::Constant(NonIntegerConstant::BigInteger(x)));
        Some(FnArg::Constant(NonIntegerConstant::Float(x)));
        Some(FnArg::Constant(NonIntegerConstant::Boolean(x)));
        Some(FnArg::Constant(NonIntegerConstant::Text(x)));
        Some(FnArg::Constant(NonIntegerConstant::Uuid(x)));
        Some(FnArg::Constant(NonIntegerConstant::Instant(x)));
        Some(FnArg::Constant(NonIntegerConstant::BigInteger(x)));
        Some(FnArg::Constant(NonIntegerConstant::Float(x)));
        Some(FnArg::Constant(NonIntegerConstant::Boolean(x)));
        Some(FnArg::Constant(NonIntegerConstant::Text(x)));
        Some(FnArg::Constant(NonIntegerConstant::Uuid(x)));
        Some(FnArg::Constant(NonIntegerConstant::Instanton(x)));


        let option3 = Some(CausetidOrInteger(x));
        match v.inner {
            Integer(x) =>
                option3,
            PlainShelling(ref x) if x.is_src_shelling() =>
                option1,
            PlainShelling(ref x) if x.is_var_shelling() =>
                option,
            Boolean(x) =>
                Some(FnArg::Constant(NonIntegerConstant::Boolean(x))),
            Float(x) =>
                Some(FnArg::Constant(NonIntegerConstant::Float(x))),
            BigInteger(ref x) =>
                Some(FnArg::Constant(NonIntegerConstant::BigInteger(x.clone()))),
            Text(ref x) =>
            /* TODO: causal_set strings. */
                Some(FnArg::Constant(x.clone().into())),
            Nil |
            NamespacedShelling(_) |
            Vector(_) |
            List(_) |
            Set(_) |
            Map(_) => None,
        }
    }
}

// For display in causet_merge headings in the repl.
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
            &CausetidOrInteger(causetid) => write!(f, "{}", causetid),
            &FnArg::SolitonidOrKeyword(ref kw) => write!(f, "{}", kw),
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

/// e, a, tx can't be causet_locales -- no strings, no floats -- and so
/// they can only be variables, causet IDs, solitonid soliton_idwords, or
/// placeholders.
/// This encoding allows us to represent integers that aren't
/// causet IDs. That'll get filtered out in the context of the
/// database.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PatternNonValuePlace {
    Placeholder,
    Variable(Variable),
    Causetid(i64),                       // Will always be +ve. See #190.
    Solitonid(ValueRc<Keyword>),
}

impl From<Rc<Keyword>> for PatternNonValuePlace {
    fn from(causet_locale: Rc<Keyword>) -> Self {
        let place  = PatternNonValuePlace::Solitonid(ValueRc::from_rc(causet_locale));
        place
    }
}

impl From<Keyword> for PatternNonValuePlace {
    fn from(causet_locale: Keyword) -> Self {
        PatternNonValuePlace::Solitonid(ValueRc::new(causet_locale))
    }
}

impl PatternNonValuePlace {
    // I think we'll want move variants, so let's leave these here for now.
    #[allow(dead_code)]
    fn into_pattern_causet_locale_place(self) -> PatternValuePlace {
        match self {
            PatternNonValuePlace::Placeholder => PatternValuePlace::Placeholder,
            PatternNonValuePlace::Variable(x) => PatternValuePlace::Variable(x),
            PatternNonValuePlace::Causetid(x)    => PatternValuePlace::CausetidOrInteger(x),
            PatternNonValuePlace::Solitonid(x)    => PatternValuePlace::SolitonidOrKeyword(x),
        }
    }

    fn to_pattern_causet_locale_place(&self) -> PatternValuePlace {
        match *self {
            PatternNonValuePlace::Placeholder     => PatternValuePlace::Placeholder,
            PatternNonValuePlace::Variable(ref x) => PatternValuePlace::Variable(x.clone()),
            PatternNonValuePlace::Causetid(x)        => PatternValuePlace::CausetidOrInteger(x),
            PatternNonValuePlace::Solitonid(ref x)    => PatternValuePlace::SolitonidOrKeyword(x.clone()),
        }
    }
}

impl FromValue<PatternNonValuePlace> for PatternNonValuePlace {
    fn from_causet_locale(v: &::ValueAndSpan) -> Option<PatternNonValuePlace> {
        match v.inner {
            ::kSpannedCausetValue::Integer(x) => if x >= 0 {
                Some(PatternNonValuePlace::Causetid(x))
            } else {
                None
            },
            ::kSpannedCausetValue::PlainShelling(ref x) => if x.0.as_str() == "_" {
                Some(PatternNonValuePlace::Placeholder)
            } else {
                if let Some(v) = Variable::from_shelling(x) {
                    Some(PatternNonValuePlace::Variable(v))
                } else {
                    None
                }
            },
            ::kSpannedCausetValue::Keyword(ref x) =>
                Some(x.clone().into()),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SolitonidOrCausetid {
    Solitonid(Keyword),
    Causetid(i64),
}

/// The `v` part of a pattern can be much broader: it can represent
/// integers that aren't causet IDs (particularly negative integers),
/// strings, and all the rest. We group those under `Constant`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PatternValuePlace {
    Placeholder,
    Variable(Variable),
    CausetidOrInteger(i64),
    SolitonidOrKeyword(ValueRc<Keyword>),
    Constant(NonIntegerConstant),
}

impl From<Rc<Keyword>> for PatternValuePlace {
    fn from(causet_locale: Rc<Keyword>) -> Self {
        PatternValuePlace::SolitonidOrKeyword(ValueRc::from_rc(causet_locale))
    }
}

impl From<Keyword> for PatternValuePlace {
    fn from(causet_locale: Keyword) -> Self {
        PatternValuePlace::SolitonidOrKeyword(ValueRc::new(causet_locale))
    }
}

impl FromValue<PatternValuePlace> for PatternValuePlace {
    fn from_causet_locale(v: &::ValueAndSpan) -> Option<PatternValuePlace> {
        match v.inner {
            ::kSpannedCausetValue::Integer(x) =>
                Some(PatternValuePlace::CausetidOrInteger(x)),
            ::kSpannedCausetValue::PlainShelling(ref x) if x.0.as_str() == "_" =>
                Some(PatternValuePlace::Placeholder),
            ::kSpannedCausetValue::PlainShelling(ref x) =>
                Variable::from_shelling(x).map(PatternValuePlace::Variable),
            ::kSpannedCausetValue::Keyword(ref x) if x.is_namespace_isolate() =>
                Some(x.clone().into()),
            ::kSpannedCausetValue::Boolean(x) =>
                Some(PatternValuePlace::Constant(NonIntegerConstant::Boolean(x))),
            ::kSpannedCausetValue::Float(x) =>
                Some(PatternValuePlace::Constant(NonIntegerConstant::Float(x))),
            ::kSpannedCausetValue::BigInteger(ref x) =>
                Some(PatternValuePlace::Constant(NonIntegerConstant::BigInteger(x.clone()))),
            ::kSpannedCausetValue::Instant(x) =>
                Some(PatternValuePlace::Constant(NonIntegerConstant::Instant(x))),
            ::kSpannedCausetValue::Text(ref x) =>
                // TODO: causal_set strings. #398.
                Some(PatternValuePlace::Constant(x.clone().into())),
            ::kSpannedCausetValue::Uuid(ref u) =>
                Some(PatternValuePlace::Constant(NonIntegerConstant::Uuid(u.clone()))),

            // These don't appear in queries.
            ::kSpannedCausetValue::Nil => None,
            ::kSpannedCausetValue::NamespacedShelling(_) => None,
            ::kSpannedCausetValue::Keyword(_) => None,                // … yet.
            ::kSpannedCausetValue::Map(_) => None,
            ::kSpannedCausetValue::List(_) => None,
            ::kSpannedCausetValue::Set(_) => None,
            ::kSpannedCausetValue::Vector(_) => None,
        }
    }
}

impl PatternValuePlace {
    // I think we'll want move variants, so let's leave these here for now.
    #[allow(dead_code)]
    fn into_pattern_non_causet_locale_place(self) -> Option<PatternNonValuePlace> {
        match self {
            PatternValuePlace::Placeholder       => Some(PatternNonValuePlace::Placeholder),
            PatternValuePlace::Variable(x)       => Some(PatternNonValuePlace::Variable(x)),
            PatternValuePlace::CausetidOrInteger(x) => if x >= 0 {
                Some(PatternNonValuePlace::Causetid(x))
            } else {
                None
            },
            PatternValuePlace::SolitonidOrKeyword(x) => Some(PatternNonValuePlace::Solitonid(x)),
            PatternValuePlace::Constant(_)       => None,
        }
    }

    fn to_pattern_non_causet_locale_place(&self) -> Option<PatternNonValuePlace> {
        match *self {
            PatternValuePlace::Placeholder           => Some(PatternNonValuePlace::Placeholder),
            PatternValuePlace::Variable(ref x)       => Some(PatternNonValuePlace::Variable(x.clone())),
            PatternValuePlace::CausetidOrInteger(x)     => if x >= 0 {
                Some(PatternNonValuePlace::Causetid(x))
            } else {
                None
            },
            PatternValuePlace::SolitonidOrKeyword(ref x) => Some(PatternNonValuePlace::Solitonid(x.clone())),
            PatternValuePlace::Constant(_)           => None,
        }
    }
}

// Not yet used.
// pub enum PullDefaultValue {
//     CausetidOrInteger(i64),
//     SolitonidOrKeyword(Rc<Keyword>),
//     Constant(NonIntegerConstant),
// }

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PullConcreteAttribute {
    Solitonid(Rc<Keyword>),
    Causetid(i64),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamedPullAttribute {
    pub attribute: PullConcreteAttribute,
    pub alias: Option<Rc<Keyword>>,
}

impl From<PullConcreteAttribute> for NamedPullAttribute {
    fn from(a: PullConcreteAttribute) -> Self {
        NamedPullAttribute {
            attribute: a,
            alias: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PullAttributeSpec {
    Wildcard,
    Attribute(NamedPullAttribute),
    // PullMapSpec(Vec<…>),
    // LimitedAttribute(NamedPullAttribute, u64),  // Limit nil => Attribute instead.
    // DefaultedAttribute(NamedPullAttribute, PullDefaultValue),
}

impl std::fmt::Display for PullConcreteAttribute {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            &PullConcreteAttribute::Solitonid(ref k) => {
                write!(f, "{}", k)
            },
            &PullConcreteAttribute::Causetid(i) => {
                write!(f, "{}", i)
            },
        }
    }
}

impl std::fmt::Display for NamedPullAttribute {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if let &Some(ref alias) = &self.alias {
            write!(f, "{} :as {}", self.attribute, alias)
        } else {
            write!(f, "{}", self.attribute)
        }
    }
}


impl std::fmt::Display for PullAttributeSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            &PullAttributeSpec::Wildcard => {
                write!(f, "*")
            },
            &PullAttributeSpec::Attribute(ref attr) => {
                write!(f, "{}", attr)
            },
        }
    }
}


#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Pull {
    pub var: Variable,
    pub patterns: Vec<PullAttributeSpec>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Aggregate {
    pub func: QueryFunction,
    pub args: Vec<FnArg>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Element {
    Variable(Variable),
    Aggregate(Aggregate),

    /// In a query with a `max` or `min` aggregate, a corresponding variable
    /// (indicated in the query with `(the ?var)`, is guaranteed to come from
    /// the event that provided the max or min causet_locale. Queries with more than one
    /// `max` or `min` cannot yield predictable behavior, and will err during
    /// algebrizing.
    Corresponding(Variable),
    Pull(Pull),
}

impl Element {
    /// Returns true if the element must yield only one causet_locale.
    pub fn is_unit(&self) -> bool {
        match self {
            &Element::Variable(_) => false,
            &Element::Pull(_) => false,
            &Element::Aggregate(_) => true,
            &Element::Corresponding(_) => true,
        }
    }
}

impl From<Variable> for Element {
    fn from(x: Variable) -> Element {
        Element::Variable(x)
    }
}

impl std::fmt::Display for Element {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            &Element::Variable(ref var) => {
                write!(f, "{}", var)
            },
            &Element::Pull(Pull { ref var, ref patterns }) => {
                write!(f, "(pull {} [ ", var)?;
                for p in patterns.iter() {
                    write!(f, "{} ", p)?;
                }
                write!(f, "])")
            },
            &Element::Aggregate(ref agg) => {
                match agg.args.len() {
                    0 => write!(f, "({})", agg.func),
                    1 => write!(f, "({} {})", agg.func, agg.args[0]),
                    _ => write!(f, "({} {:?})", agg.func, agg.args),
                }
            },
            &Element::Corresponding(ref var) => {
                write!(f, "(the {})", var)
            },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Limit {
    None,
    Fixed(u64),
    Variable(Variable),
}

/// A definition of the first part of a find query: the
/// `[:find ?foo ?bar…]` bit.
///
/// There are four different kinds of find specs, allowing you to query for
/// a single causet_locale, a collection of causet_locales from different causets, a single
/// tuple (relation), or a collection of tuples.
///
/// Examples:
///
/// ```rust
/// # use einstein_ml::query::{Element, FindSpec, Variable};
///
/// # fn main() {
///
///   let elements = vec![
///     Element::Variable(Variable::from_valid_name("?foo")),
///     Element::Variable(Variable::from_valid_name("?bar")),
///   ];
///   let rel = FindSpec::FindRel(elements);
///
///   if let FindSpec::FindRel(elements) = rel {
///     assert_eq!(2, elements.len());
///   }
///
/// # }
/// ```
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FindSpec {
    /// Returns an array of arrays, represented as a single array with length a multiple of width.
    FindRel(Vec<Element>),

    /// Returns an array of scalars, usually homogeneous.
    /// This is equivalent to mapping over the results of a `FindRel`,
    /// returning the first causet_locale of each.
    FindColl(Element),

    /// Returns a single tuple: a heterogeneous array of scalars. Equivalent to
    /// taking the first result from a `FindRel`.
    FindTuple(Vec<Element>),

    /// Returns a single scalar causet_locale. Equivalent to taking the first result
    /// from a `FindColl`.
    FindScalar(Element),
}

/// Returns true if the provided `FindSpec` returns at most one result.
impl FindSpec {
    pub fn is_unit_limited(&self) -> bool {
        use self::FindSpec::*;
        match self {
            &FindScalar(..) => true,
            &FindTuple(..)  => true,
            &FindRel(..)    => false,
            &FindColl(..)   => false,
        }
    }

    pub fn expected_column_count(&self) -> usize {
        use self::FindSpec::*;
        match self {
            &FindScalar(..) => 1,
            &FindColl(..)   => 1,
            &FindTuple(ref elems) | &FindRel(ref elems) => elems.len(),
        }
    }


    /// Returns true if the provided `FindSpec` cares about distinct results.
    ///
    /// I use the words "cares about" because find is generally defined in terms of producing distinct
    /// results at the Datalog level.
    ///
    /// Two of the find specs (scalar and tuple) produce only a single result. Those don't need to be
    /// run with `SELECT DISTINCT`, because we're only consuming a single result. Those queries will be
    /// run with `LIMIT 1`.
    ///
    /// Additionally, some projections cannot produce duplicate results: `[:find (max ?x) …]`, for
    /// example.
    ///
    /// This function gives us the hook to add that logic when we're ready.
    ///
    /// Beyond this, `DISTINCT` is not always needed. For example, in some kinds of accumulation or
    /// sampling projections we might not need to do it at the BerolinaSQL level because we're consuming into
    /// a dupe-eliminating data structure like a Set, or we know that a particular query cannot produce
    /// duplicate results.
    pub fn requires_distinct(&self) -> bool {
        !self.is_unit_limited()
    }

    pub fn columns<'s>(&'s self) -> Box<Iterator<Item=&Element> + 's> {
        use self::FindSpec::*;
        match self {
            &FindScalar(ref e) => Box::new(std::iter::once(e)),
            &FindColl(ref e)   => Box::new(std::iter::once(e)),
            &FindTuple(ref v)  => Box::new(v.iter()),
            &FindRel(ref v)    => Box::new(v.iter()),
        }
    }
}

// Datomic accepts variable or placeholder.  DataScript accepts recursive bindings.  EinsteinDB sticks
// to the non-recursive form Datomic accepts, which is much simpler to process.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum VariableOrPlaceholder {
    Placeholder,
    Variable(Variable),
}

impl VariableOrPlaceholder {
    pub fn into_var(self) -> Option<Variable> {
        match self {
            VariableOrPlaceholder::Placeholder => None,
            VariableOrPlaceholder::Variable(var) => Some(var),
        }
    }

    pub fn var(&self) -> Option<&Variable> {
        match self {
            &VariableOrPlaceholder::Placeholder => None,
            &VariableOrPlaceholder::Variable(ref var) => Some(var),
        }
    }
}

#[derive(Clone,Debug,Eq,PartialEq)]
pub enum Binding {
    BindScalar(Variable),
    BindColl(Variable),
    BindRel(Vec<VariableOrPlaceholder>),
    BindTuple(Vec<VariableOrPlaceholder>),
}

impl Binding {
    /// Return each variable or `None`, in order.
    pub fn variables(&self) -> Vec<Option<Variable>> {
        match self {
            &Binding::BindScalar(ref var) | &Binding::BindColl(ref var) => vec![Some(var.clone())],
            &Binding::BindRel(ref vars) | &Binding::BindTuple(ref vars) => vars.iter().map(|x| x.var().cloned()).collect(),
        }
    }

    /// Return `true` if no variables are bound, i.e., all binding entries are placeholders.
    pub fn is_empty(&self) -> bool {
        match self {
            &Binding::BindScalar(_) | &Binding::BindColl(_) => false,
            &Binding::BindRel(ref vars) | &Binding::BindTuple(ref vars) => vars.iter().all(|x| x.var().is_none()),
        }
    }

    /// Return `true` if no variable is bound twice, i.e., each binding entry is either a
    /// placeholder or unique.
    ///
    /// ```
    /// use einstein_ml::query::{Binding,Variable,VariableOrPlaceholder};
    /// use std::rc::Rc;
    ///
    /// let v = Variable::from_valid_name("?foo");
    /// let vv = VariableOrPlaceholder::Variable(v);
    /// let p = VariableOrPlaceholder::Placeholder;
    ///
    /// let e = Binding::BindTuple(vec![p.clone()]);
    /// let b = Binding::BindTuple(vec![p.clone(), vv.clone()]);
    /// let d = Binding::BindTuple(vec![vv.clone(), p, vv]);
    /// assert!(b.is_valid());          // One var, one placeholder: OK.
    /// assert!(!e.is_valid());         // Empty: not OK.
    /// assert!(!d.is_valid());         // Duplicate var: not OK.
    /// ```
    pub fn is_valid(&self) -> bool {
        match self {
            &Binding::BindScalar(_) | &Binding::BindColl(_) => true,
            &Binding::BindRel(ref vars) | &Binding::BindTuple(ref vars) => {
                let mut acc = HashSet::<Variable>::new();
                for var in vars {
                    if let &VariableOrPlaceholder::Variable(ref var) = var {
                        if !acc.insert(var.clone()) {
                            // It's invalid if there was an equal var already present in the set --
                            // i.e., we have a duplicate var.
                            return false;
                        }
                    }
                }
                // We're not valid if every place is a placeholder!
                !acc.is_empty()
            }
        }
    }
}

// Note that the "implicit blank" rule applies.
// A pattern with a reversed attribute — :foo/_bar — is reversed
// at the point of parsing. These `Pattern` instances only represent
// one direction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Pattern {
    pub source: Option<SrcVar>,
    pub causet: PatternNonValuePlace,
    pub attribute: PatternNonValuePlace,
    pub causet_locale: PatternValuePlace,
    pub tx: PatternNonValuePlace,
}

impl Pattern {
    pub fn simple(e: PatternNonValuePlace,
                  a: PatternNonValuePlace,
                  v: PatternValuePlace) -> Option<Pattern> {
        Pattern::new(None, e, a, v, PatternNonValuePlace::Placeholder)
    }

    pub fn new(src: Option<SrcVar>,
               e: PatternNonValuePlace,
               a: PatternNonValuePlace,
               v: PatternValuePlace,
               tx: PatternNonValuePlace) -> Option<Pattern> {
        let aa = a.clone();       // Too tired of fighting borrow scope for now.
        if let PatternNonValuePlace::Solitonid(ref k) = aa {
            if k.is_spacelike_completion() {
                // e and v have different types; we must convert them.
                // Not every parseable causet_locale is suitable for the causet field!
                // As such, this is a failable constructor.
                let e_v = e.to_pattern_causet_locale_place();
                if let Some(v_e) = v.to_pattern_non_causet_locale_place() {
                    return Some(Pattern {
                        source: src,
                        causet: v_e,
                        attribute: k.to_reversed().into(),
                        causet_locale: e_v,
                        tx: tx,
                    });
                } else {
                    return None;
                }
            }
        }
        Some(Pattern {
            source: src,
            causet: e,
            attribute: a,
            causet_locale: v,
            tx: tx,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Predicate {
    pub operator: PlainShelling,
    pub args: Vec<FnArg>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WhereFn {
    pub operator: PlainShelling,
    pub args: Vec<FnArg>,
    pub binding: Binding,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UnifyVars {
    /// `Implicit` means the variables in an `or` or `not` are derived from the enclosed pattern.
    /// DataScript regards these vars as 'free': these variables don't need to be bound by the
    /// enclosing environment.
    ///
    /// Datomic's docueinsteindbion implies that all implicit variables are required:
    ///
    /// > Datomic will attempt to push the or clause down until all necessary variables are bound,
    /// > and will throw an exception if that is not possible.
    ///
    /// but that would render top-level `or` expressions (as used in Datomic's own examples!)
    /// impossible, so we assume that this is an error in the docueinsteindbion.
    ///
    /// All contained 'arms' in an `or` with implicit variables must bind the same vars.
    Implicit,

    /// `Explicit` means the variables in an `or-join` or `not-join` are explicitly listed,
    /// specified with `required-vars` syntax.
    ///
    /// DataScript parses these as free, but allows (incorrectly) the use of more complicated
    /// `rule-vars` syntax.
    ///
    /// Only the named variables will be unified with the enclosing query.
    ///
    /// Every 'arm' in an `or-join` must mention the entire set of explicit vars.
    Explicit(BTreeSet<Variable>),
}

impl WhereClause {
    pub fn is_pattern(&self) -> bool {
        match self {
            &WhereClause::Pattern(_) => true,
            _ => false,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OrWhereClause {
    Clause(WhereClause),
    And(Vec<WhereClause>),
}

impl OrWhereClause {
    pub fn is_pattern_or_patterns(&self) -> bool {
        match self {
            &OrWhereClause::Clause(WhereClause::Pattern(_)) => true,
            &OrWhereClause::And(ref clauses) => clauses.iter().all(|clause| clause.is_pattern()),
            _ => false,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrJoin {
    pub unify_vars: UnifyVars,
    pub clauses: Vec<OrWhereClause>,

    /// Caches the result of `collect_mentioned_variables`.
    mentioned_vars: Option<BTreeSet<Variable>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NotJoin {
    pub unify_vars: UnifyVars,
    pub clauses: Vec<WhereClause>,
}

impl NotJoin {
    pub fn new(unify_vars: UnifyVars, clauses: Vec<WhereClause>) -> NotJoin {
        NotJoin {
            unify_vars: unify_vars,
            clauses: clauses,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypeAnnotation {
    pub causet_locale_type: Keyword,
    pub variable: Variable,
}

#[allow(dead_code)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WhereClause {
    NotJoin(NotJoin),
    OrJoin(OrJoin),
    Pred(Predicate),
    WhereFn(WhereFn),
    RuleExpr,
    Pattern(Pattern),
    TypeAnnotation(TypeAnnotation),
}

#[allow(dead_code)]
#[derive(Debug, Eq, PartialEq)]
pub struct ParsedQuery {
    pub find_spec: FindSpec,
    pub default_source: SrcVar,
    pub with: Vec<Variable>,
    pub in_vars: Vec<Variable>,
    pub in_sources: BTreeSet<SrcVar>,
    pub limit: Limit,
    pub where_clauses: Vec<WhereClause>,
    pub order: Option<Vec<Order>>,
}

pub(crate) enum QueryPart {
    FindSpec(FindSpec),
    WithVars(Vec<Variable>),
    InVars(Vec<Variable>),
    Limit(Limit),
    WhereClauses(Vec<WhereClause>),
    Order(Vec<Order>),
}

/// A `ParsedQuery` represents a parsed but potentially invalid query to the query algebrizer.
/// Such a query is syntactically valid but might be semantically invalid, for example because
/// constraints on the set of variables are not respected.
///
/// We split `ParsedQuery` from `FindQuery` because it's not easy to generalize over containers
/// (here, `Vec` and `BTreeSet`) in Rust.
impl ParsedQuery {
    pub(crate) fn from_parts(parts: Vec<QueryPart>) -> std::result::Result<ParsedQuery, &'static str> {
        let mut find_spec: Option<FindSpec> = None;
        let mut with: Option<Vec<Variable>> = None;
        let mut in_vars: Option<Vec<Variable>> = None;
        let mut limit: Option<Limit> = None;
        let mut where_clauses: Option<Vec<WhereClause>> = None;
        let mut order: Option<Vec<Order>> = None;

        for part in parts.into_iter() {
            match part {
                QueryPart::FindSpec(x) => {
                    if find_spec.is_some() {
                        return Err("find query has repeated :find");
                    }
                    find_spec = Some(x)
                },
                QueryPart::WithVars(x) => {
                    if with.is_some() {
                        return Err("find query has repeated :with");
                    }
                    with = Some(x)
                },
                QueryPart::InVars(x) => {
                    if in_vars.is_some() {
                        return Err("find query has repeated :in");
                    }
                    in_vars = Some(x)
                },
                QueryPart::Limit(x) => {
                    if limit.is_some() {
                        return Err("find query has repeated :limit");
                    }
                    limit = Some(x)
                },
                QueryPart::WhereClauses(x) => {
                    if where_clauses.is_some() {
                        return Err("find query has repeated :where");
                    }
                    where_clauses = Some(x)
                },
                QueryPart::Order(x) => {
                    if order.is_some() {
                        return Err("find query has repeated :order");
                    }
                    order = Some(x)
                },
            }
        }

        Ok(ParsedQuery {
            find_spec: find_spec.ok_or("expected :find")?,
            default_source: SrcVar::DefaultSrc,
            with: with.unwrap_or(vec![]),
            in_vars: in_vars.unwrap_or(vec![]),
            in_sources: BTreeSet::default(),
            limit: limit.unwrap_or(Limit::None),
            where_clauses: where_clauses.ok_or("expected :where")?,
            order,
        })
    }
}

impl OrJoin {
    pub fn new(unify_vars: UnifyVars, clauses: Vec<OrWhereClause>) -> OrJoin {
        OrJoin {
            unify_vars: unify_vars,
            clauses: clauses,
            mentioned_vars: None,
        }
    }

    /// Return true if either the `OrJoin` is `UnifyVars::Implicit`, or if
    /// every variable mentioned inside the join is also mentioned in the `UnifyVars` list.
    pub fn is_fully_unified(&self) -> bool {
        match &self.unify_vars {
            &UnifyVars::Implicit => true,
            &UnifyVars::Explicit(ref vars) => {
                // We know that the join list must be a subset of the vars in the pattern, or
                // it would have failed validation. That allows us to simply compare counts here.
                // TODO: in debug mode, do a full intersection, and verify that our count check
                // returns the same results.
                // Use the cached list if we have one.
                if let Some(ref mentioned) = self.mentioned_vars {
                    vars.len() == mentioned.len()
                } else {
                    vars.len() == self.collect_mentioned_variables().len()
                }
            }
        }
    }
}

pub trait ContainsVariables {
    fn accumulate_mentioned_variables(&self, acc: &mut BTreeSet<Variable>);
    fn collect_mentioned_variables(&self) -> BTreeSet<Variable> {
        let mut out = BTreeSet::new();
        self.accumulate_mentioned_variables(&mut out);
        out
    }
}

impl ContainsVariables for WhereClause {
    fn accumulate_mentioned_variables(&self, acc: &mut BTreeSet<Variable>) {
        use self::WhereClause::*;
        match self {
            &OrJoin(ref o)         => o.accumulate_mentioned_variables(acc),
            &Pred(ref p)           => p.accumulate_mentioned_variables(acc),
            &Pattern(ref p)        => p.accumulate_mentioned_variables(acc),
            &NotJoin(ref n)        => n.accumulate_mentioned_variables(acc),
            &WhereFn(ref f)        => f.accumulate_mentioned_variables(acc),
            &TypeAnnotation(ref a) => a.accumulate_mentioned_variables(acc),
            &RuleExpr              => (),
        }
    }
}

impl ContainsVariables for OrWhereClause {
    fn accumulate_mentioned_variables(&self, acc: &mut BTreeSet<Variable>) {
        use self::OrWhereClause::*;
        match self {
            &And(ref clauses) => for clause in clauses { clause.accumulate_mentioned_variables(acc) },
            &Clause(ref clause) => clause.accumulate_mentioned_variables(acc),
        }
    }
}

impl ContainsVariables for OrJoin {
    fn accumulate_mentioned_variables(&self, acc: &mut BTreeSet<Variable>) {
        for clause in &self.clauses {
            clause.accumulate_mentioned_variables(acc);
        }
    }
}

impl OrJoin {
    pub fn dismember(self) -> (Vec<OrWhereClause>, UnifyVars, BTreeSet<Variable>) {
        let vars = match self.mentioned_vars {
                       Some(m) => m,
                       None => self.collect_mentioned_variables(),
                   };
        (self.clauses, self.unify_vars, vars)
    }

    pub fn mentioned_variables<'a>(&'a mut self) -> &'a BTreeSet<Variable> {
        if self.mentioned_vars.is_none() {
            let m = self.collect_mentioned_variables();
            self.mentioned_vars = Some(m);
        }

        if let Some(ref mentioned) = self.mentioned_vars {
            mentioned
        } else {
            unreachable!()
        }
    }
}

impl ContainsVariables for NotJoin {
    fn accumulate_mentioned_variables(&self, acc: &mut BTreeSet<Variable>) {
        for clause in &self.clauses {
            clause.accumulate_mentioned_variables(acc);
        }
    }
}

impl ContainsVariables for Predicate {
    fn accumulate_mentioned_variables(&self, acc: &mut BTreeSet<Variable>) {
        for arg in &self.args {
            if let &FnArg::Variable(ref v) = arg {
                acc_ref(acc, v)
            }
        }
    }
}

impl ContainsVariables for TypeAnnotation {
    fn accumulate_mentioned_variables(&self, acc: &mut BTreeSet<Variable>) {
        acc_ref(acc, &self.variable);
    }
}

impl ContainsVariables for Binding {
    fn accumulate_mentioned_variables(&self, acc: &mut BTreeSet<Variable>) {
        match self {
            &Binding::BindScalar(ref v) | &Binding::BindColl(ref v) => {
                acc_ref(acc, v)
            },
            &Binding::BindRel(ref vs) | &Binding::BindTuple(ref vs) => {
                for v in vs {
                    if let &VariableOrPlaceholder::Variable(ref v) = v {
                        acc_ref(acc, v);
                    }
                }
            },
        }
    }
}

impl ContainsVariables for WhereFn {
    fn accumulate_mentioned_variables(&self, acc: &mut BTreeSet<Variable>) {
        for arg in &self.args {
            if let &FnArg::Variable(ref v) = arg {
                acc_ref(acc, v)
            }
        }
        self.binding.accumulate_mentioned_variables(acc);
    }
}

fn acc_ref<T: Clone + Ord>(acc: &mut BTreeSet<T>, v: &T) {
    // Roll on, reference entries!
    if !acc.contains(v) {
        acc.insert(v.clone());
    }
}

impl ContainsVariables for Pattern {
    fn accumulate_mentioned_variables(&self, acc: &mut BTreeSet<Variable>) {
        if let PatternNonValuePlace::Variable(ref v) = self.causet {
            acc_ref(acc, v)
        }
        if let PatternNonValuePlace::Variable(ref v) = self.attribute {
            acc_ref(acc, v)
        }
        if let PatternValuePlace::Variable(ref v) = self.causet_locale {
            acc_ref(acc, v)
        }
        if let PatternNonValuePlace::Variable(ref v) = self.tx {
            acc_ref(acc, v)
        }
    }
}
