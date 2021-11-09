//Copyright 2021-2023 WHTCORPS

// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
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

use symbols;

/// Value represents one of the allowed values in an EDN string.
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
    PlainSymbol(symbols::PlainSymbol),
    NamespacedSymbol(symbols::NamespacedSymbol),
    Keyword(symbols::Keyword),
    Vector(Vec<Value>),

    List(LinkedList<Value>),

    Set(BTreeSet<Value>),
    Map(BTreeMap<Value, Value>),
}

/// `SpannedValue` is the parallel to `Value` but used in `ValueAndSpan`.
/// Container types have `ValueAndSpan` children.
#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub enum SpannedValue {
    Nil,
    Boolean(bool),
    Integer(i64),
    Instant(DateTime<Utc>),
    BigInteger(BigInt),
    Float(OrderedFloat<f64>),
    Text(String),
    Uuid(Uuid),
    PlainSymbol(symbols::PlainSymbol),
    NamespacedSymbol(symbols::NamespacedSymbol),
    Keyword(symbols::Keyword),
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

/// A wrapper type around `SpannedValue` and `Span`, representing some EDN value
/// and the parsing offset (start, end) in the original EDN string.
#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct ValueAndSpan {
    pub inner: SpannedValue,
    pub span: Span,
}

impl ValueAndSpan {
    pub fn new<I>(spanned_value: SpannedValue, span: I) -> ValueAndSpan where I: Into<Option<Span>> {
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