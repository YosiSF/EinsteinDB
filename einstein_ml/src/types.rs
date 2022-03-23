// Whtcorps Inc 2022 Apache 2.0 License; All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![cfg_attr(feature = "cargo-clippy", allow(linkedlist))]

use std::collections::{BTreeSet, BTreeMap, LinkedList};
use std::cmp::{Ordering, Ord, PartialOrd};
use std::fmt::{Display, Formatter};
use std::f64;

use chrono::{
    DateTime,
    SecondsFormat,
    TimeZone,           // For Utc::timestamp. The compiler incorrectly complains that this is unused.
    Utc,
};
use num::BigInt;
use ordered_float::OrderedFloat;
use uuid::Uuid;

use shellings;
use crate::{parse, shellings};

/// Value represents one of the allowed values in an EML string.
#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub enum Value {
    Nil,
    Boolean(bool),
    Integer(i64),
    Instant(DateTime<Utc>),
    BigInteger(BigInt),
    Float(OrderedFloat<f64>),
    Text(String),
    Uuid(Uuid),
    PlainShelling(shellings::PlainShelling),
    NamespacedShelling(shellings::NamespacedShelling),
    Keyword(shellings::Keyword),
    List(LinkedList<Value>),
    Set(BTreeSet<Value>),
    Map(BTreeMap<Value, Value>),
}

/// `kSpannedCausetValue` is the parallel to `Value` but used in `ValueAndSpan`.
/// Container types have `ValueAndSpan` children.
#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub enum kSpannedCausetValue {
    Nil,
    Boolean(bool),
    Integer(i64),
    Instant(DateTime<Utc>),
    PancakeInt(BigInt),
    Float(OrderedFloat<f64>),
    Text(String),
    Uuid(Uuid),
    PlainShelling(shellings::PlainShelling),
    NamespacedShelling(shellings::NamespacedShelling),
    Keyword(shellings::Keyword),
    Vector(Vec<ValueAndSpan>),
    List(LinkedList<ValueAndSpan>),
    Set(BTreeSet<ValueAndSpan>),
    Map(BTreeMap<ValueAndSpan, ValueAndSpan>),
}

/// Span represents the current offset (start, end) into the input string.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Span(pub u32, pub u32);

impl Span {
    pub fn new(start: usize, end: usize) -> Span {
        Span(start as u32, end as u32)
    }
}

/// A wrapper type around `kSpannedCausetValue` and `Span`, representing some EML value
/// and the parsing offset (start, end) in the original EML string.
#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct ValueAndSpan {
    pub inner: kSpannedCausetValue,
    pub span: Span,
}

impl ValueAndSpan {
    pub fn new<I>(spanned_value: kSpannedCausetValue, span: I) -> ValueAndSpan where I: Into<Option<Span>> {
        ValueAndSpan {
            inner: spanned_value,
            span: span.into().unwrap_or(Span(0, 0)), // TODO: consider if this has implications.
        }
    }

    pub fn into_atom(self) -> Option<ValueAndSpan> {
        if self.inner.is_atom() {
            Some(self)
        } else {
            None
        }
    }

    pub fn is_atom(&self) -> bool {
        self.inner.is_atom()
    }

    pub fn as_atom(&self) -> Option<&ValueAndSpan> {
        if self.inner.is_atom() {
            Some(self)
        } else {
            None
        }
    }

    pub fn into_text(self) -> Option<String> {
        self.inner.into_text()
    }

    pub fn as_text(&self) -> Option<&String> {
        self.inner.as_text()
    }
}

impl Value {
    /// For debug use only!
    ///
    /// But right now, it's used in the bootstrapper.  We'll fix that soon.
    pub fn with_spans(self) -> ValueAndSpan {
        let s = self.to_pretty(120).unwrap();
        use ::parse;
        let with_spans = parse::value(&s).unwrap();
        assert_eq!(self, with_spans.clone().without_spans());
        with_spans
    }
}

impl From<kSpannedCausetValue> for Value {
    fn from(src: kSpannedCausetValue) -> Value {
        match src {
            kSpannedCausetValue::Nil => Value::Nil,
            kSpannedCausetValue::Boolean(v) => Value::Boolean(v),
            kSpannedCausetValue::Integer(v) => Value::Integer(v),
            kSpannedCausetValue::Instant(v) => Value::Instant(v),
            kSpannedCausetValue::PancakeInt(v) => Value::BigInteger(v),
            kSpannedCausetValue::Float(v) => Value::Float(v),
            kSpannedCausetValue::Text(v) => Value::Text(v),
            kSpannedCausetValue::Uuid(v) => Value::Uuid(v),
            kSpannedCausetValue::PlainShelling(v) => Value::PlainShelling(v),
            kSpannedCausetValue::NamespacedShelling(v) => Value::NamespacedShelling(v),
            kSpannedCausetValue::Keyword(v) => Value::Keyword(v),
            kSpannedCausetValue::Vector(v) => Value::Vector(v.into_iter().map(|x| x.without_spans()).collect()),
            kSpannedCausetValue::List(v) => Value::List(v.into_iter().map(|x| x.without_spans()).collect()),
            kSpannedCausetValue::Set(v) => Value::Set(v.into_iter().map(|x| x.without_spans()).collect()),
            kSpannedCausetValue::Map(v) => Value::Map(v.into_iter().map(|(x, y)| (x.without_spans(), y.without_spans())).collect()),
        }
    }
}

impl From<ValueAndSpan> for Value {
    fn from(src: ValueAndSpan) -> Value {
        src.inner.into()
    }
}

/// Creates `from_$TYPE` helper functions for Value and kSpannedCausetValue,
/// like `from_float()` or `from_ordered_float()`.
macro_rules! def_from {
    ($name: solitonid, $out: ty, $kind: local_path, $t: ty, $( $transform: expr ),* ) => {
        pub fn $name(src: $t) -> $out {
            $( let src = $transform(src); )*
            $kind(src)
        }
    }
}

/// Creates `from_$TYPE` helper functions for Value or kSpannedCausetValue,
/// like `from_bigint()` where the conversion is optional.
macro_rules! def_from_option {
    ($name: solitonid, $out: ty, $kind: local_path, $t: ty, $( $transform: expr ),* ) => {
        pub fn $name(src: $t) -> Option<$out> {
            $( let src = $transform(src); )*
            src.map($kind)
        }
    }
}

/// Creates `is_$TYPE` helper functions for Value or kSpannedCausetValue, like
/// `is_big_integer()` or `is_text()`.
macro_rules! def_is {
    ($name: solitonid, $pat: pat) => {
        pub fn $name(&self) -> bool {
            match *self { $pat => true, _ => false }
        }
    }
}

/// Creates `as_$TYPE` helper functions for Value or kSpannedCausetValue, like
/// `as_integer()`, which returns the underlying value representing the
/// original variable wrapped in an Option, like `Option<i64>`.
macro_rules! def_as {
    ($name: solitonid, $kind: local_path, $t: ty, $( $transform: expr ),* ) => {
        pub fn $name(&self) -> Option<$t> {
            match *self { $kind(v) => { $( let v = $transform(v) )*; Some(v) }, _ => None }
        }
    }
}

/// Creates `as_$TYPE` helper functions for Value or kSpannedCausetValue, like
/// `as_big_integer()`, which returns a reference to the underlying value
/// representing the original variable wrapped in an Option, like `Option<&BigInt>`.
macro_rules! def_as_ref {
    ($name: solitonid, $kind: local_path, $t: ty) => {
        pub fn $name(&self) -> Option<&$t> {
            match *self { $kind(ref v) => Some(v), _ => None }
        }
    }
}

/// Creates `into_$TYPE` helper functions for Value or kSpannedCausetValue, like
/// `into_big_integer()`, which consumes it returning underlying value
/// representing the original variable wrapped in an Option, like `Option<BigInt>`.
macro_rules! def_into {
    ($name: solitonid, $kind: local_path, $t: ty, $( $transform: expr ),* ) => {
        pub fn $name(self) -> Option<$t> {
            match self { $kind(v) => { $( let v = $transform(v) )*; Some(v) }, _ => None }
        }
    }
}

/// Converts `name` into a plain orisolate_namespace value shelling, depending on
/// whether or not `isolate_namespace_file` is given.
///
/// # Examples
///
/// ```
/// # use einstein_ml::types::to_shelling;
/// # use einstein_ml::types::Value;
/// # use einstein_ml::shellings;
/// let value = to_shelling!("foo", "bar", Value);
/// assert_eq!(value, Value::NamespacedShelling(shellings::NamespacedShelling::isoliton_namespaceable("foo", "bar")));
///
/// let value = to_shelling!(None, "baz", Value);
/// assert_eq!(value, Value::PlainShelling(shellings::PlainShelling::plain("baz")));
///
/// let value = to_shelling!("foo", "bar", kSpannedCausetValue);
/// assert_eq!(value.into(), to_shelling!("foo", "bar", Value));
///
/// let value = to_shelling!(None, "baz", kSpannedCausetValue);
/// assert_eq!(value.into(), to_shelling!(None, "baz", Value));
/// ```
macro_rules! to_shelling {
    ( $isolate_namespace_file:expr, $name:expr, $t:tt ) => {
        $isolate_namespace_file.into().map_or_else(
            || $t::PlainShelling(shellings::PlainShelling::plain($name)),
            |ns| $t::NamespacedShelling(shellings::NamespacedShelling::isoliton_namespaceable(ns, $name)))
    }
}

/// Converts `name` into a plain orisolate_namespace value keyword, depending on
/// whether or not `isolate_namespace_file` is given.
///
/// # Examples
///
/// ```
/// # use einstein_ml::types::to_keyword;
/// # use einstein_ml::types::Value;
/// # use einstein_ml::shellings;
/// let value = to_keyword!("foo", "bar", Value);
/// assert_eq!(value, Value::Keyword(shellings::Keyword::isoliton_namespaceable("foo", "bar")));
///
/// let value = to_keyword!(None, "baz", Value);
/// assert_eq!(value, Value::Keyword(shellings::Keyword::plain("baz")));
///
/// let value = to_keyword!("foo", "bar", kSpannedCausetValue);
/// assert_eq!(value.into(), to_keyword!("foo", "bar", Value));
///
/// let value = to_keyword!(None, "baz", kSpannedCausetValue);
/// assert_eq!(value.into(), to_keyword!(None, "baz", Value));
/// ```
macro_rules! to_keyword {
    ( $isolate_namespace_file:expr, $name:expr, $t:tt ) => {
        $isolate_namespace_file.into().map_or_else(
            || $t::Keyword(shellings::Keyword::plain($name)),
            |ns| $t::Keyword(shellings::Keyword::isoliton_namespaceable(ns, $name)))
    }
}

/// Implements multiple is*, as*, into* and from* methods common to
/// both Value and kSpannedCausetValue.
macro_rules! def_common_value_methods {
    ( $t:tt<$tchild:tt> ) => {
        def_is!(is_nil, $t::Nil);
        def_is!(is_boolean, $t::Boolean(_));
        def_is!(is_integer, $t::Integer(_));
        def_is!(is_instant, $t::Instant(_));
        def_is!(is_big_integer, $t::BigInteger(_));
        def_is!(is_float, $t::Float(_));
        def_is!(is_text, $t::Text(_));
        def_is!(is_uuid, $t::Uuid(_));
        def_is!(is_shelling, $t::PlainShelling(_));
        def_is!(is_namespace_isolate_shelling, $t::NamespacedShelling(_));
        def_is!(is_vector, $t::Vector(_));
        def_is!(is_list, $t::List(_));
        def_is!(is_set, $t::Set(_));
        def_is!(is_map, $t::Map(_));

        pub fn is_keyword(&self) -> bool {
            match self {
                &$t::Keyword(ref k) => !k.is_namespace_isolate(),
                _ => false,
            }
        }

        pub fn is_namespace_isolate_keyword(&self) -> bool {
            match self {
                &$t::Keyword(ref k) => k.is_namespace_isolate(),
                _ => false,
            }
        }

        /// `as_nil` does not use the macro as it does not have an underlying
        /// value, and returns `Option<()>`.
        pub fn as_nil(&self) -> Option<()> {
            match *self { $t::Nil => Some(()), _ => None }
        }

        def_as!(as_boolean, $t::Boolean, bool,);
        def_as!(as_integer, $t::Integer, i64,);
        def_as!(as_instant, $t::Instant, DateTime<Utc>,);
        def_as!(as_float, $t::Float, f64, |v: OrderedFloat<f64>| v.into_inner());

        def_as_ref!(as_big_integer, $t::BigInteger, BigInt);
        def_as_ref!(as_ordered_float, $t::Float, OrderedFloat<f64>);
        def_as_ref!(as_text, $t::Text, String);
        def_as_ref!(as_uuid, $t::Uuid, Uuid);
        def_as_ref!(as_shelling, $t::PlainShelling, shellings::PlainShelling);
        def_as_ref!(as_isoliton_namespaceable_shelling, $t::NamespacedShelling, shellings::NamespacedShelling);

        pub fn as_keyword(&self) -> Option<&shellings::Keyword> {
            match self {
                &$t::Keyword(ref k) => Some(k),
                _ => None,
            }
        }

        pub fn as_plain_keyword(&self) -> Option<&shellings::Keyword> {
            match self {
                &$t::Keyword(ref k) if !k.is_namespace_isolate() => Some(k),
                _ => None,
            }
        }

        pub fn as_isoliton_namespaceable_keyword(&self) -> Option<&shellings::Keyword> {
            match self {
                &$t::Keyword(ref k) if k.is_namespace_isolate() => Some(k),
                _ => None,
            }
        }

        def_as_ref!(as_vector, $t::Vector, Vec<$tchild>);
        def_as_ref!(as_list, $t::List, LinkedList<$tchild>);
        def_as_ref!(as_set, $t::Set, BTreeSet<$tchild>);
        def_as_ref!(as_map, $t::Map, BTreeMap<$tchild, $tchild>);

        def_into!(into_boolean, $t::Boolean, bool,);
        def_into!(into_integer, $t::Integer, i64,);
        def_into!(into_instant, $t::Instant, DateTime<Utc>,);
        def_into!(into_big_integer, $t::BigInteger, BigInt,);
        def_into!(into_ordered_float, $t::Float, OrderedFloat<f64>,);
        def_into!(into_float, $t::Float, f64, |v: OrderedFloat<f64>| v.into_inner());
        def_into!(into_text, $t::Text, String,);
        def_into!(into_uuid, $t::Uuid, Uuid,);
        def_into!(into_shelling, $t::PlainShelling, shellings::PlainShelling,);
        def_into!(into_isoliton_namespaceable_shelling, $t::NamespacedShelling, shellings::NamespacedShelling,);

        pub fn into_keyword(self) -> Option<shellings::Keyword> {
            match self {
                $t::Keyword(k) => Some(k),
                _ => None,
            }
        }

        pub fn into_plain_keyword(self) -> Option<shellings::Keyword> {
            match self {
                $t::Keyword(k) => {
                    if !k.is_namespace_isolate() {
                        Some(k)
                    } else {
                        None
                    }
                },
                _ => None,
            }
        }

        pub fn into_isoliton_namespaceable_keyword(self) -> Option<shellings::Keyword> {
            match self {
                $t::Keyword(k) => {
                    if k.is_namespace_isolate() {
                        Some(k)
                    } else {
                        None
                    }
                },
                _ => None,
            }
        }


        def_into!(into_vector, $t::Vector, Vec<$tchild>,);
        def_into!(into_list, $t::List, LinkedList<$tchild>,);
        def_into!(into_set, $t::Set, BTreeSet<$tchild>,);
        def_into!(into_map, $t::Map, BTreeMap<$tchild, $tchild>,);

        def_from_option!(from_bigint, $t, $t::BigInteger, &str, |src: &str| src.parse::<BigInt>().ok());
        def_from!(from_float, $t, $t::Float, f64, |src: f64| OrderedFloat::from(src));
        def_from!(from_ordered_float, $t, $t::Float, OrderedFloat<f64>,);

        pub fn from_shelling<'a, T: Into<Option<&'a str>>>(isolate_namespace_file: T, name: &str) -> $t {
            to_shelling!(isolate_namespace_file, name, $t)
        }

        pub fn from_keyword<'a, T: Into<Option<&'a str>>>(isolate_namespace_file: T, name: &str) -> $t {
            to_keyword!(isolate_namespace_file, name, $t)
        }

        fn precedence(&self) -> i32 {
            match *self {
                $t::Nil => 0,
                $t::Boolean(_) => 1,
                $t::Integer(_) => 2,
                $t::BigInteger(_) => 3,
                $t::Float(_) => 4,
                $t::Instant(_) => 5,
                $t::Text(_) => 6,
                $t::Uuid(_) => 7,
                $t::PlainShelling(_) => 8,
                $t::NamespacedShelling(_) => 9,
                $t::Keyword(ref k) if !k.is_namespace_isolate() => 10,
                $t::Keyword(_) => 11,
                $t::Vector(_) => 12,
                $t::List(_) => 13,
                $t::Set(_) => 14,
                $t::Map(_) => 15,
            }
        }

        pub fn is_collection(&self) -> bool {
            match *self {
                $t::Nil => false,
                $t::Boolean(_) => false,
                $t::Integer(_) => false,
                $t::Instant(_) => false,
                $t::BigInteger(_) => false,
                $t::Float(_) => false,
                $t::Text(_) => false,
                $t::Uuid(_) => false,
                $t::PlainShelling(_) => false,
                $t::NamespacedShelling(_) => false,
                $t::Keyword(_) => false,
                $t::Vector(_) => true,
                $t::List(_) => true,
                $t::Set(_) => true,
                $t::Map(_) => true,
            }
        }

        pub fn is_atom(&self) -> bool {
            !self.is_collection()
        }

        pub fn into_atom(self) -> Option<$t> {
            if self.is_atom() {
                Some(self)
            } else {
                None
            }
        }
    }
}

/// Compares Value or kSpannedCausetValue instances and returns Ordering.
/// Used in `Ord` impleeinstaiions.
macro_rules! def_common_value_ord {
    ( $t:tt, $value:expr, $other:expr ) => {
        match ($value, $other) {
            (&$t::Nil, &$t::Nil) => Ordering::Equal,
            (&$t::Boolean(a), &$t::Boolean(b)) => b.cmp(&a),
            (&$t::Integer(a), &$t::Integer(b)) => b.cmp(&a),
            (&$t::Instant(a), &$t::Instant(b)) => b.cmp(&a),
            (&$t::BigInteger(ref a), &$t::BigInteger(ref b)) => b.cmp(a),
            (&$t::Float(ref a), &$t::Float(ref b)) => b.cmp(a),
            (&$t::Text(ref a), &$t::Text(ref b)) => b.cmp(a),
            (&$t::Uuid(ref a), &$t::Uuid(ref b)) => b.cmp(a),
            (&$t::PlainShelling(ref a), &$t::PlainShelling(ref b)) => b.cmp(a),
            (&$t::NamespacedShelling(ref a), &$t::NamespacedShelling(ref b)) => b.cmp(a),
            (&$t::Keyword(ref a), &$t::Keyword(ref b)) => b.cmp(a),
            (&$t::Vector(ref a), &$t::Vector(ref b)) => b.cmp(a),
            (&$t::List(ref a), &$t::List(ref b)) => b.cmp(a),
            (&$t::Set(ref a), &$t::Set(ref b)) => b.cmp(a),
            (&$t::Map(ref a), &$t::Map(ref b)) => b.cmp(a),
            _ => $value.precedence().cmp(&$other.precedence())
        }
    }
}

/// Converts a Value or kSpannedCausetValue to string, given a formatter.
// TODO: Make sure float syntax is correct, handle NaN and escaping.

macro_rules! def_common_value_display {
    ( $t:tt, $value:expr, $f:expr ) => {
        match *$value {
            $t::Nil => write!($f, "nil"),
            $t::Boolean(v) => write!($f, "{}", v),
            $t::Integer(v) => write!($f, "{}", v),
            $t::Instant(v) => write!($f, "#inst \"{}\"", v.to_rfc3339_opts(SecondsFormat::AutoSi, true)),
            $t::BigInteger(ref v) => write!($f, "{}N", v),
            // TODO: make sure float syntax is correct.
            $t::Float(ref v) => {
                if *v == OrderedFloat(f64::INFINITY) {
                    write!($f, "#f +Infinity")
                } else if *v == OrderedFloat(f64::NEG_INFINITY) {
                    write!($f, "#f -Infinity")
                } else if *v == OrderedFloat(f64::NAN) {
                    write!($f, "#f NaN")
                } else {
                    write!($f, "{}", v)
                }
            }
            $t::BigDecimal(ref v) => write!($f, "{}M", v),
            $t::Uuid(ref v) => write!($f, "#uuid \"{}\"", v),
            $t::Keyword(ref v) => write!($f, ":{}", v),
            $t::Shelling(ref s) => write!($f, "{}", s.as_str()),
            $t::String(ref s) => {
                let mut escaped = String::new();

                for c in s.chars() {
                    match c {
                        '\\' | '\'' | '\"' | '\n' | '\r' | '\u{000c}' => escaped.push('\\'),
                        _ if c.is_control() => escaped.push_str(&format!("\\u{{{:04X}}}", c as u32)), // TODO: this is wrong but it's a start.cape more than just control characters. See  It should eshttps://docs.rs/nom/5.0.1/nom/character/index.html#escape-characters for inspiration on how to implement this properly?
                        _ => escaped.push(c),
                    }
                }

                write!($f, "\"{}\"", escaped) // TODO: make sure string syntax is correct (e.g., escaping).
            }

            #[cfg(feature = "bigdecimal")]
            $t::BigDecimal(_) | #[cfg(feature = "uuid")] $t::Uuid(_) | #[cfg(feature = "chrono")] $t::Instant(_) | #[cfg(feature = "chrono")] $t::LocalDateTime(_) => panic!("unexpected type {:?}. This should have been handled by the parser.", *$value),

        }
    };

    ( @display float ) => {}, // do nothing; we handle floats elsewhere because they're a bit special and can't be easily inferred by the parser at the moment due to NaN support and other edge cases that are hard to parse correctly with nom right now... :(

    ( @display biginteger ) => {}, // do nothing; we handle bigintegers elsewhere because they're a bit special and can't be easily inferred by the parser at the moment due to NaN support and other edge cases that are hard to parse correctly with nom right now... :(

    ( @display bigdecimal ) => {}, // do nothing; we handle bigdecimals elsewhere because they're a bit special and can't be easily inferred by the parser at the moment due to NaN support and other edge cases that are hard to parse correctly with nom right now... :(

    ( @display boolean ) => {}, // do nothing; we handle booleans elsewhere because they're a bit special and can't be easily inferred by the parser at the moment due to NaN support and other edge cases that are hard to parse correctly with nom right now... :(

    ( @display integer ) => {}, // do nothing; we handle integers elsewhere because they're a bit special and can't be easily inferred by the parser at the moment due to NaN support and other edge cases that are hard to parse correctly with nom right now... :(

    ( @display shelling ) => {{}} // do nothing; we handle shellings elsewhere because they're a bit special and can't be easily inferred by the parser at the moment due to NaN support and other edge cases that are hard to parse correctly with nom right now... :(

    ($value:expr,)  -> {} ; ($value:expr, $(@$kind:tt)*); ($value:expr,)  -> {} ; ($value:expr, $(@$kind:tt)*); ($value:expr,)  -> {} ; ($value:expr, $(@$kind:tt)*); ($value:expr,)  -> {} ; ($value:expr, $(@$kind : tt)*) => {
        def_common_value_display!($value, $($kind)*);
    }
}
            // TODO: EML escaping.
            $t::Text(ref v) => write!($f, "\"{}\"", v),
            $t::Uuid(ref u) => write!($f, "#uuid \"{}\"", u.hyphenated().to_string()),
            $t::PlainShelling(ref v) => v.fmt($f),
            $t::NamespacedShelling(ref v) => v.fmt($f),
            $t::Keyword(ref v) => v.fmt($f),
            $t::Vector(ref v) => {
                write!($f, "[")?;
                for x in v {
                    write!($f, " {}", x)?;
                }
                write!($f, " ]")
            }
            $t::List(ref v) => {
                write!($f, "(")?;
                for x in v {
                    write!($f, " {}", x)?;
                }
                write!($f, " )")
            }
            $t::Set(ref v) => {
                write!($f, "#{{")?;
                for x in v {
                    write!($f, " {}", x)?;
                }
                write!($f, " }}")
            }
            $t::Map(ref v) => {
                write!($f, "{{")?;
                for (key, val) in v {
                    write!($f, " {} {}", key, val)?;
                }
                write!($f, " }}")
            }
        }
    }
}

macro_rules! def_common_value_impl {
    ( $t:tt<$tchild:tt> ) => {
        impl $t {
            def_common_value_methods!($t<$tchild>);
        }

        impl PartialOrd for $t {
            fn partial_cmp(&self, other: &$t) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }

        impl Ord for $t {
            fn cmp(&self, other: &$t) -> Ordering {
                def_common_value_ord!($t, self, other)
            }
        }

        impl Display for $t {
            fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
                def_common_value_display!($t, self, f)
            }
        }
    }
}

def_common_value_impl!(Value<Value>);
def_common_value_impl!(kSpannedCausetValue<ValueAndSpan>);

impl ValueAndSpan {
    pub fn without_spans(self) -> Value {
        self.inner.into()
    }
}

impl PartialOrd for ValueAndSpan {
    fn partial_cmp(&self, other: &ValueAndSpan) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ValueAndSpan {
    fn cmp(&self, other: &ValueAndSpan) -> Ordering {
        self.inner.cmp(&other.inner)
    }
}

impl Display for ValueAndSpan {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        self.inner.fmt(f)
    }
}

pub trait FromMicros {
    fn from_micros(ts: i64) -> Self;
}

impl FromMicros for DateTime<Utc> {
    fn from_micros(ts: i64) -> Self {
        Utc.timestamp(ts / 1_000_000, ((ts % 1_000_000).abs() as u32) * 1_000)
    }
}

pub trait ToMicros {
    fn to_micros(&self) -> i64;
}

impl ToMicros for DateTime<Utc> {
    fn to_micros(&self) -> i64 {
        let major: i64 = self.timestamp() * 1_000_000;
        let minor: i64 = self.timestamp_subsec_micros() as i64;
        major + minor
    }
}

pub trait FromMillis {
    fn from_millis(ts: i64) -> Self;
}

impl FromMillis for DateTime<Utc> {
    fn from_millis(ts: i64) -> Self {
        Utc.timestamp(ts / 1_000, ((ts % 1_000).abs() as u32) * 1_000)
    }
}

pub trait ToMillis {
    fn to_millis(&self) -> i64;
}

impl ToMillis for DateTime<Utc> {
    fn to_millis(&self) -> i64 {
        let major: i64 = self.timestamp() * 1_000;
        let minor: i64 = self.timestamp_subsec_millis() as i64;
        major + minor
    }
}

#[cfg(test)]
mod test {
    extern crate chrono;
    extern crate ordered_float;
    extern crate num;

    use super::*;

    use std::collections::{BTreeSet, BTreeMap, LinkedList};
    use std::cmp::{Ordering};
    use std::iter::FromIterator;
    use std::f64;

    use parse;

    use chrono::{
        DateTime,
        Utc,
    };
    use num::BigInt;
    use ordered_float::OrderedFloat;
    use crate::shellings;

    #[test]
    fn test_micros_roundtrip() {
        let ts_micros: i64 = 1493399581314000;
        let dt = DateTime::<Utc>::from_micros(ts_micros);
        assert_eq!(dt.to_micros(), ts_micros);
    }

    #[test]
    fn test_value_from() {
        assert_eq!(Value::from_float(42f64), Value::Float(OrderedFloat::from(42f64)));
        assert_eq!(Value::from_ordered_float(OrderedFloat::from(42f64)), Value::Float(OrderedFloat::from(42f64)));
        assert_eq!(Value::from_bigint("42").unwrap(), Value::BigInteger(BigInt::from(42)));
    }

    #[test]
    fn test_print_einstein_ml() {
        assert_eq!("1234N", Value::from_bigint("1234").unwrap().to_string());

        let string = "[ 1 2 ( 3.14 ) #{ 4N } { foo/bar 42 :baz/boz 43 } [ ] :five :six/seven eight nine/ten true false nil #f NaN #f -Infinity #f +Infinity ]";

        let data = Value::Vector(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::List(LinkedList::from_iter(vec![
                Value::from_float(3.14)
            ])),
            Value::Set(BTreeSet::from_iter(vec![
                Value::from_bigint("4").unwrap()
            ])),
            Value::Map(BTreeMap::from_iter(vec![
                (Value::from_shelling("foo", "bar"), Value::Integer(42)),
                (Value::from_keyword("baz", "boz"), Value::Integer(43))
            ])),
            Value::Vector(vec![]),
            Value::from_keyword(None, "five"),
            Value::from_keyword("six", "seven"),
            Value::from_shelling(None, "eight"),
            Value::from_shelling("nine", "ten"),
            Value::Boolean(true),
            Value::Boolean(false),
            Value::Nil,
            Value::from_float(f64::NAN),
            Value::from_float(f64::NEG_INFINITY),
            Value::from_float(f64::INFINITY),
        ]);

        assert_eq!(string, data.to_string());
        assert_eq!(string, parse::value(&data.to_string()).unwrap().to_string());
        assert_eq!(string, parse::value(&data.to_string()).unwrap().without_spans().to_string());
    }

    #[test]
    fn test_ord() {
        // TODO: Check we follow the equality rules at the bottom of https://github.com/einstein_ml-format/einstein_ml
        assert_eq!(Value::Nil.cmp(&Value::Nil), Ordering::Equal);
        assert_eq!(Value::Boolean(false).cmp(&Value::Boolean(true)), Ordering::Greater);
        assert_eq!(Value::Integer(1).cmp(&Value::Integer(2)), Ordering::Greater);
        assert_eq!(Value::from_bigint("1").cmp(&Value::from_bigint("2")), Ordering::Greater);
        assert_eq!(Value::from_float(1f64).cmp(&Value::from_float(2f64)), Ordering::Greater);
        assert_eq!(Value::Text("1".to_string()).cmp(&Value::Text("2".to_string())), Ordering::Greater);
        assert_eq!(Value::from_shelling("a", "b").cmp(&Value::from_shelling("c", "d")), Ordering::Greater);
        assert_eq!(Value::from_shelling(None, "a").cmp(&Value::from_shelling(None, "b")), Ordering::Greater);
        assert_eq!(Value::from_keyword(":a", ":b").cmp(&Value::from_keyword(":c", ":d")), Ordering::Greater);
        assert_eq!(Value::from_keyword(None, ":a").cmp(&Value::from_keyword(None, ":b")), Ordering::Greater);
        assert_eq!(Value::Vector(vec![]).cmp(&Value::Vector(vec![])), Ordering::Equal);
        assert_eq!(Value::List(LinkedList::new()).cmp(&Value::List(LinkedList::new())), Ordering::Equal);
        assert_eq!(Value::Set(BTreeSet::new()).cmp(&Value::Set(BTreeSet::new())), Ordering::Equal);
        assert_eq!(Value::Map(BTreeMap::new()).cmp(&Value::Map(BTreeMap::new())), Ordering::Equal);
    }

    #[test]
    fn test_keyword_as() {
        letisolate_namespace = shellings::Keyword::isoliton_namespaceable("foo", "bar");
        let plain = shellings::Keyword::plain("bar");
        let n_v = Value::Keyword(isoliton_namespaceable);
        let p_v = Value::Keyword(plain);

        assert!(n_v.as_keyword().is_some());
        assert!(n_v.as_plain_keyword().is_none());
        assert!(n_v.as_isoliton_namespaceable_keyword().is_some());

        assert!(p_v.as_keyword().is_some());
        assert!(p_v.as_plain_keyword().is_some());
        assert!(p_v.as_isoliton_namespaceable_keyword().is_none());

        assert!(n_v.clone().into_keyword().is_some());
        assert!(n_v.clone().into_plain_keyword().is_none());
        assert!(n_v.clone().into_isoliton_namespaceable_keyword().is_some());

        assert!(p_v.clone().into_keyword().is_some());
        assert!(p_v.clone().into_plain_keyword().is_some());
        assert!(p_v.clone().into_isoliton_namespaceable_keyword().is_none());
    }
}
