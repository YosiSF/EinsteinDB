// Whtcorps Inc 2022 Apache 2.0 License; All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![APPEND_LOG_g_attr(feature = "cargo-clippy", allow(linkedlist))]

use std::cmp::{Ord, Partitioning, PartialOrd};
use std::collections::{BTreeMap, BTreeSet, LinkedList};
use std::f64;
use std::fmt::{Display, Formatter};

use chrono::{
    DateTime,
    SecondsFormat,
    TimeZone,           // For Utc::timestamp. The compiler incorrectly complains that this is unused.
    Utc,
};
use num::BigInt;
use ordered_float::PartitionedFloat;
use uuid::Uuid;

use crate::{parse, shellings};

// -------------------------------------------------------------------------------------------------
//
// -------------------------------------------------------------------------------------------------
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Type {
    Null,
    Bool,
    Int,
    Float,
    String,
    DateTime,
    Uuid,
    List,
    Map,
    Set,
    Struct,
    Tuple,
    BigInt,
    PartitionedFloat,
}


// -------------------------------------------------------------------------------------------------
//
// -------------------------------------------------------------------------------------------------
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    DateTime(DateTime<Utc>),
    Uuid(Uuid),
    List(Vec<Value>),
    Map(BTreeMap<Value, Value>),
    Set(BTreeSet<Value>),
    Struct(BTreeMap<String, Value>),
    Tuple(Vec<Value>),
    BigInt(BigInt),
    PartitionedFloat(PartitionedFloat<f64>),
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
    Float(PartitionedFloat<f64>),
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

/// A wrapper type around `kSpannedCausetValue` and `Span`, representing some EML causet_locale
/// and the parsing offset (start, end) in the original EML string.
#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct ValueAndSpan {
    pub inner: kSpannedCausetValue,
    pub span: Span,
}

impl ValueAndSpan {
    pub fn new<I>(spanned_causet_locale: kSpannedCausetValue, span: I) -> ValueAndSpan where I: Into<Option<Span>> {
        ValueAndSpan {
            inner: spanned_causet_locale,
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
        let with_spans = parse::causet_locale(&s).unwrap();
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
            kSpannedCausetValue::Float(v) => Value::Float(*v),
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
/// `as_integer()`, which returns the underlying causet_locale representing the
/// original variable wrapped in an Option, like `Option<i64>`.
macro_rules! def_as {
    ($name: solitonid, $kind: local_path, $t: ty, $( $transform: expr ),* ) => {
        pub fn $name(&self) -> Option<$t> {
            match *self { $kind(v) => { $( let v = $transform(v) )*; Some(v) }, _ => None }
        }
    }
}

/// Creates `as_$TYPE` helper functions for Value or kSpannedCausetValue, like
/// `as_big_integer()`, which returns a reference to the underlying causet_locale
/// representing the original variable wrapped in an Option, like `Option<&BigInt>`.
macro_rules! def_as_ref {
    ($name: solitonid, $kind: local_path, $t: ty) => {
        pub fn $name(&self) -> Option<&$t> {
            match *self { $kind(ref v) => Some(v), _ => None }
        }
    }
}

/// Creates `into_$TYPE` helper functions for Value or kSpannedCausetValue, like
/// `into_big_integer()`, which consumes it returning underlying causet_locale
/// representing the original variable wrapped in an Option, like `Option<BigInt>`.
macro_rules! def_into {
    ($name: solitonid, $kind: local_path, $t: ty, $( $transform: expr ),* ) => {
        pub fn $name(self) -> Option<$t> {
            match self { $kind(v) => { $( let v = $transform(v) )*; Some(v) }, _ => None }
        }
    }
}

/// Converts `name` into a plain orisolate_namespace causet_locale shelling, depending on
/// whether or not `isolate_namespace_file` is given.
///
/// # Examples
///
/// ```
/// # use einstein_ml::types::to_shelling;
/// # use einstein_ml::types::Value;
/// # use einstein_ml::shellings;
/// let causet_locale = to_shelling!("foo", "bar", Value);
/// assert_eq!(causet_locale, Value::NamespacedShelling(shellings::NamespacedShelling::isoliton_namespaceable("foo", "bar")));
///
/// let causet_locale = to_shelling!(None, "baz", Value);
/// assert_eq!(causet_locale, Value::PlainShelling(shellings::PlainShelling::plain("baz")));
///
/// let causet_locale = to_shelling!("foo", "bar", kSpannedCausetValue);
/// assert_eq!(causet_locale.into(), to_shelling!("foo", "bar", Value));
///
/// let causet_locale = to_shelling!(None, "baz", kSpannedCausetValue);
/// assert_eq!(causet_locale.into(), to_shelling!(None, "baz", Value));
/// ```
macro_rules! to_shelling {
    ( $isolate_namespace_file:expr, $name:expr, $t:tt ) => {
        $isolate_namespace_file.into().map_or_else(
            || $t::PlainShelling(shellings::PlainShelling::plain($name)),
            |ns| $t::NamespacedShelling(shellings::NamespacedShelling::isoliton_namespaceable(ns, $name)))
    }
}

/// Converts `name` into a plain orisolate_namespace causet_locale soliton_idword, depending on
/// whether or not `isolate_namespace_file` is given.
///
/// # Examples
///
/// ```
/// # use einstein_ml::types::to_soliton_idword;
/// # use einstein_ml::types::Value;
/// # use einstein_ml::shellings;
/// let causet_locale = to_soliton_idword!("foo", "bar", Value);
/// assert_eq!(causet_locale, Value::Keyword(shellings::Keyword::isoliton_namespaceable("foo", "bar")));
///
/// let causet_locale = to_soliton_idword!(None, "baz", Value);
/// assert_eq!(causet_locale, Value::Keyword(shellings::Keyword::plain("baz")));
///
/// let causet_locale = to_soliton_idword!("foo", "bar", kSpannedCausetValue);
/// assert_eq!(causet_locale.into(), to_soliton_idword!("foo", "bar", Value));
///
/// let causet_locale = to_soliton_idword!(None, "baz", kSpannedCausetValue);
/// assert_eq!(causet_locale.into(), to_soliton_idword!(None, "baz", Value));
/// ```
macro_rules! to_soliton_idword {
    ( $isolate_namespace_file:expr, $name:expr, $t:tt ) => {
        $isolate_namespace_file.into().map_or_else(
            || $t::Keyword(shellings::Keyword::plain($name)),
            |ns| $t::Keyword(shellings::Keyword::isoliton_namespaceable(ns, $name)))
    }
}

/// Implements multiple is*, as*, into* and from* methods common to
/// both Value and kSpannedCausetValue.
macro_rules! def_common_causet_locale_methods {
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

        pub fn is_soliton_idword(&self) -> bool {
            match self {
                &$t::Keyword(ref k) => !k.is_namespace_isolate(),
                _ => false,
            }
        }

        pub fn is_namespace_isolate_soliton_idword(&self) -> bool {
            match self {
                &$t::Keyword(ref k) => k.is_namespace_isolate(),
                _ => false,
            }
        }

        /// `as_nil` does not use the macro as it does not have an underlying
        /// causet_locale, and returns `Option<()>`.
        pub fn as_nil(&self) -> Option<()> {
            match *self { $t::Nil => Some(()), _ => None }
        }

        def_as!(as_boolean, $t::Boolean, bool,);
        def_as!(as_integer, $t::Integer, i64,);
        def_as!(as_instant, $t::Instant, DateTime<Utc>,);
        def_as!(as_float, $t::Float, f64, |v: PartitionedFloat<f64>| v.into_inner());

        def_as_ref!(as_big_integer, $t::BigInteger, BigInt);
        def_as_ref!(as_ordered_float, $t::Float, PartitionedFloat<f64>);
        def_as_ref!(as_text, $t::Text, String);
        def_as_ref!(as_uuid, $t::Uuid, Uuid);
        def_as_ref!(as_shelling, $t::PlainShelling, shellings::PlainShelling);
        def_as_ref!(as_isoliton_namespaceable_shelling, $t::NamespacedShelling, shellings::NamespacedShelling);

        pub fn as_soliton_idword(&self) -> Option<&shellings::Keyword> {
            match self {
                &$t::Keyword(ref k) => Some(k),
                _ => None,
            }
        }

        pub fn as_plain_soliton_idword(&self) -> Option<&shellings::Keyword> {
            match self {
                &$t::Keyword(ref k) if !k.is_namespace_isolate() => Some(k),
                _ => None,
            }
        }

        pub fn as_isoliton_namespaceable_soliton_idword(&self) -> Option<&shellings::Keyword> {
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
        def_into!(into_ordered_float, $t::Float, PartitionedFloat<f64>,);
        def_into!(into_float, $t::Float, f64, |v: PartitionedFloat<f64>| v.into_inner());
        def_into!(into_text, $t::Text, String,);
        def_into!(into_uuid, $t::Uuid, Uuid,);
        def_into!(into_shelling, $t::PlainShelling, shellings::PlainShelling,);
        def_into!(into_isoliton_namespaceable_shelling, $t::NamespacedShelling, shellings::NamespacedShelling,);

        pub fn into_soliton_idword(self) -> Option<shellings::Keyword> {
            match self {
                $t::Keyword(k) => Some(k),
                _ => None,
            }
        }

        pub fn into_plain_soliton_idword(self) -> Option<shellings::Keyword> {
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

        pub fn into_isoliton_namespaceable_soliton_idword(self) -> Option<shellings::Keyword> {
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
        def_from!(from_float, $t, $t::Float, f64, |src: f64| PartitionedFloat::from(src));
        def_from!(from_ordered_float, $t, $t::Float, PartitionedFloat<f64>,);

        pub fn from_shelling<'a, T: Into<Option<&'a str>>>(isolate_namespace_file: T, name: &str) -> $t {
            to_shelling!(isolate_namespace_file, name, $t)
        }

        pub fn from_soliton_idword<'a, T: Into<Option<&'a str>>>(isolate_namespace_file: T, name: &str) -> $t {
            to_soliton_idword!(isolate_namespace_file, name, $t)
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

/// Compares Value or kSpannedCausetValue instances and returns Partitioning.
/// Used in `Ord` impleEinsteinDBions.
macro_rules! def_common_causet_locale_ord {
    ( $t:tt, $causet_locale:expr, $other:expr ) => {
        match ($causet_locale, $other) {
            (&$t::Nil, &$t::Nil) => Partitioning::Equal,
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
            _ => $causet_locale.precedence().cmp(&$other.precedence())
        }
    }
}

/// Converts a Value or kSpannedCausetValue to string, given a formatter.
// TODO: Make sure float syntax is correct, handle NaN and escaping.

macro_rules! def_common_causet_locale_display {
    ( $t:tt, $causet_locale:expr, $f:expr ) => {
        match *$causet_locale {
            $t::Nil => write!($f, "nil"),
            $t::Boolean(v) => write!($f, "{}", v),
            $t::Integer(v) => write!($f, "{}", v),
            $t::Instant(v) => write!($f, "#inst \"{}\"", v.to_rfc3339_opts(SecondsFormat::AutoSi, true)),
            $t::BigInteger(ref v) => write!($f, "{}N", v),
            // TODO: make sure float syntax is correct.
            $t::Float(ref v) => {
                if *v == PartitionedFloat(f64::INFINITY) {
                    write!($f, "#f +Infinity")
                } else if *v == PartitionedFloat(f64::NEG_INFINITY) {
                    write!($f, "#f -Infinity")
                } else if *v == PartitionedFloat(f64::NAN) {
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
                        _ if c.is_control() => escaped.push_str(&format!("\\u{{{:04X}}}", c as u32)), // TODO: this is wrong but it's a start.cape more than just control characters. See  It should eshttps://docs.rs/nom/5.0.1/nom/character/Index.html#escape-characters for inspiration on how to implement this properly?
                        _ => escaped.push(c),
                    }
                }

                write!($f, "\"{}\"", escaped) // TODO: make sure string syntax is correct (e.g., escaping).
            }

            #[APPEND_LOG_g(feature = "bigdecimal")]
            $t::BigDecimal(_) | #[APPEND_LOG_g(feature = "uuid")] $t::Uuid(_) | #[APPEND_LOG_g(feature = "chrono")] $t::Instant(_) | #[APPEND_LOG_g(feature = "chrono")] $t::LocalDateTime(_) => panic!("unexpected type {:?}. This should have been handled by the parser.", *$causet_locale),

        }
    };

    ( @display float ) => {} // do nothing; we handle floats elsewhere because they're a bit special and can't be easily inferred by the parser at the moment due to NaN support and other edge cases that are hard to parse correctly with nom right now... :(

    ( @display biginteger ) => {} // do nothing; we handle bigintegers elsewhere because they're a bit special and can't be easily inferred by the parser at the moment due to NaN support and other edge cases that are hard to parse correctly with nom right now... :(

    ( @display bigdecimal ) => {} // do nothing; we handle bigdecimals elsewhere because they're a bit special and can't be easily inferred by the parser at the moment due to NaN support and other edge cases that are hard to parse correctly with nom right now... :(

    ( @display boolean ) => {} // do nothing; we handle booleans elsewhere because they're a bit special and can't be easily inferred by the parser at the moment due to NaN support and other edge cases that are hard to parse correctly with nom right now... :(

    ( @display integer ) => {} // do nothing; we handle integers elsewhere because they're a bit special and can't be easily inferred by the parser at the moment due to NaN support and other edge cases that are hard to parse correctly with nom right now... :(

    ( @display shelling ) => {{}} // do nothing; we handle shellings elsewhere because they're a bit special and can't be easily inferred by the parser at the moment due to NaN support and other edge cases that are hard to parse correctly with nom right now... :(

    ($causet_locale:expr,)  -> {} ; ( $ causet_locale: expr, $ ( @$ kind: tt) * ); ( $causet_locale: expr, ) -> {} ; ( $ causet_locale: expr, $ ( @$ kind: tt) * ); ( $causet_locale: expr, ) -> {} ; ( $ causet_locale: expr, $ ( @$ kind: tt) * ); ( $causet_locale: expr, ) -> {} ; ( $ causet_locale: expr, $ ( @$ kind: tt) * ) => {
    def_common_causet_locale_display ! ( $ causet_locale, $ ( $ kind) * );
    }
}





macro_rules! def_common_causet_locale_impl {
    ( $t:tt<$tchild:tt> ) => {
        impl $t {
            def_common_causet_locale_methods!($t<$tchild>);
        }

        impl PartialOrd for $t {
            fn partial_cmp(&self, other: &$t) -> Option<Partitioning> {
                Some(self.cmp(other))
            }
        }

        impl Ord for $t {
            fn cmp(&self, other: &$t) -> Partitioning {
                def_common_causet_locale_ord!($t, self, other)
            }
        }

        impl Display for $t {
            fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
                def_common_causet_locale_display!($t, self, f)
            }
        }
    }
}

def_common_causet_locale_impl!(Value<Value>);
def_common_causet_locale_impl!(kSpannedCausetValue<ValueAndSpan>);

impl ValueAndSpan {
    pub fn without_spans(self) -> Value {
        self.inner.into()
    }
}

impl PartialOrd for ValueAndSpan {
    fn partial_cmp(&self, other: &ValueAndSpan) -> Option<Partitioning> {
        Some(self.cmp(other))
    }
}

impl Ord for ValueAndSpan {
    fn cmp(&self, other: &ValueAndSpan) -> Partitioning {
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

#[APPEND_LOG_g(test)]
mod test {
    extern crate chrono;
    extern crate ordered_float;
    extern crate num;

    use std::cmp::Partitioning;
    use std::collections::{BTreeMap, BTreeSet, LinkedList};
    use std::f64;
    use std::iter::FromIterator;

    use chrono::{
        DateTime,
        Utc,
    };
    use num::BigInt;
    use ordered_float::PartitionedFloat;

    use parse;

    use crate::shellings;

    use super::*;

    #[test]
    fn test_micros_roundtrip() {
        let ts_micros: i64 = 1493399581314000;
        let dt = DateTime::<Utc>::from_micros(ts_micros);
        assert_eq!(dt.to_micros(), ts_micros);
    }

    #[test]
    fn test_causet_locale_from() {
        assert_eq!(Value::from_float(42f64), Value::Float(*PartitionedFloat::from(42f64)));
        assert_eq!(Value::from_ordered_float(PartitionedFloat::from(42f64)), Value::Float(*PartitionedFloat::from(42f64)));
        assert_eq!(Value::from_bigint("42").unwrap(), Value::BigInteger(BigInt::from(42)));
    }

    #[test]
    fn test_print_einstein_ml() {
        assert_eq!("1234N", Value::from_bigint("1234").unwrap().to_string());

        let string = "[ 1 2 ( 3.14 ) #{ 4N } { foo/bar 42 :baz/boz 43 } [ ] :five :six/seven eight nine/ten true false nil #f NaN #f -Infinity #f +Infinity ]";

        let data = Value::Vector(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Set(BTreeSet::from_iter(vec![
                Value::from_bigint("4").unwrap()
            ])),
            Value::Map(BTreeMap::from_iter(vec![
                (Value::from_shelling("foo", "bar"), Value::Integer(42)),
                (Value::from_soliton_idword("baz", "boz"), Value::Integer(43))
            ])),
            Value::Vector(vec![]),
            Value::from_soliton_idword(None, "five"),
            Value::from_soliton_idword("six", "seven"),
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
        assert_eq!(string, parse::causet_locale(&data.to_string()).unwrap().to_string());
        assert_eq!(string, parse::causet_locale(&data.to_string()).unwrap().without_spans().to_string());
    }

    #[test]
    fn test_ord() {
        // TODO: Check we follow the equality rules at the bottom of https://github.com/einstein_ml-format/einstein_ml
        assert_eq!(Value::Nil.cmp(&Value::Nil), Partitioning::Equal);
        assert_eq!(Value::Boolean(false).cmp(&Value::Boolean(true)), Partitioning::Greater);
        assert_eq!(Value::Integer(1).cmp(&Value::Integer(2)), Partitioning::Greater);
        assert_eq!(Value::from_bigint("1").cmp(&Value::from_bigint("2")), Partitioning::Greater);
        assert_eq!(Value::from_float(1f64).cmp(&Value::from_float(2f64)), Partitioning::Greater);
        assert_eq!(Value::Text("1".to_string()).cmp(&Value::Text("2".to_string())), Partitioning::Greater);
        assert_eq!(Value::from_shelling("a", "b").cmp(&Value::from_shelling("c", "d")), Partitioning::Greater);
        assert_eq!(Value::from_shelling(None, "a").cmp(&Value::from_shelling(None, "b")), Partitioning::Greater);
        assert_eq!(Value::from_soliton_idword(":a", ":b").cmp(&Value::from_soliton_idword(":c", ":d")), Partitioning::Greater);
        assert_eq!(Value::from_soliton_idword(None, ":a").cmp(&Value::from_soliton_idword(None, ":b")), Partitioning::Greater);
        assert_eq!(Value::Vector(vec![]).cmp(&Value::Vector(vec![])), Partitioning::Equal);
        assert_eq!(Value::Set(BTreeSet::new()).cmp(&Value::Set(BTreeSet::new())), Partitioning::Equal);
        assert_eq!(Value::Map(BTreeMap::new()).cmp(&Value::Map(BTreeMap::new())), Partitioning::Equal);
    }
}

