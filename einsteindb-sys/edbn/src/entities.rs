//Copyright 2021-2023 WHTCORPS

// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

//! This module defines core types that support the transaction processor.

use std::collections::BTreeMap;
use std::fmt;

use value_rc::{
    ValueRc,
};

use symbols::{
    Keyword,
    PlainSymbol,
};

use types::{
    ValueAndSpan,
};

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum TempId {
    External(String),
    Internal(i64),
}

impl TempId {
    pub fn into_external(self) -> Option<String> {
        match self {
            TempId::External(s) => Some(s),
            TempId::Internal(_) => None,
        }
    }
}

impl fmt::Display for TempId {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            &TempId::External(ref s) => write!(f, "{}", s),
            &TempId::Internal(x) => write!(f, "<tempid {}>", x),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum CausetidOrSolitonid {
    Causetid(i64),
    Solitonid(Keyword),
}

impl From<i64> for CausetidOrSolitonid {
    fn from(v: i64) -> Self {
        CausetidOrSolitonid::Causetid(v)
    }
}

impl From<Keyword> for CausetidOrSolitonid {
    fn from(v: Keyword) -> Self {
        CausetidOrSolitonid::Solitonid(v)
    }
}

impl CausetidOrSolitonid {
    pub fn unreversed(&self) -> Option<CausetidOrSolitonid> {
        match self {
            &CausetidOrSolitonid::Causetid(_) => None,
            &CausetidOrSolitonid::Solitonid(ref a) => a.unreversed().map(CausetidOrSolitonid::Solitonid),
        }
    }
}
#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub struct LookupRef<V> {
    pub a: AttributePlace,
    // In theory we could allow nested lookup-refs.  In practice this would require us to process
    // lookup-refs in multiple phases, like how we resolve tempids, which isn't worth the effort.
    pub v: V, // An atom.
}

/// A "transaction function" that exposes some value determined by the current transaction.  The
/// prototypical example is the current transaction ID, `(transaction-tx)`.
///
/// A natural next step might be to expose the current transaction instant `(transaction-instant)`,
/// but that's more difficult: the transaction itself can set the transaction instant (with some
/// restrictions), so the transaction function must be late-binding.  Right now, that's difficult to
/// arrange in the transactor. perhaps we might expose `(ancestor
/// (transaction-tx) n)` to find the n-th ancestor of the current transaction.  If we do accept
/// arguments, then the special case of `(lookup-ref a v)` should be handled as part of the
/// generalization.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub struct TxFunction {
    pub op: PlainSymbol,
}

pub type MapNotation<V> = BTreeMap<EntidOrIdent, ValuePlace<V>>;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum ValuePlace<V> {
    // We never know at parse-time whether an integer or solitonid is really a causetid, but we will often
    // know when building causets/causets programmatically.
    Causetid(CausetidOrSolitonid),
    // We never know at parse-time whether a string is really a tempid, but we will often know when
    // building causets programmatically.
    TempId(ValueRc<TempId>),
    LookupRef(LookupRef<V>),
    TxFunction(TxFunction),
    Vector(Vec<ValuePlace<V>>),
    Atom(V),
    MapNotation(MapNotation<V>),
}

impl<V: TransactableValueMarker> From<CausetidOrSolitonid> for ValuePlace<V> {
    fn from(v: CausetidOrSolitonid) -> Self {
        ValuePlace::Causetid(v)
    }
}

impl<V: TransactableValueMarker> From<TempId> for ValuePlace<V> {
    fn from(v: TempId) -> Self {
        ValuePlace::TempId(v.into())
    }
}

impl<V: TransactableValueMarker> From<ValueRc<TempId>> for ValuePlace<V> {
    fn from(v: ValueRc<TempId>) -> Self {
        ValuePlace::TempId(v)
    }
}

impl<V: TransactableValueMarker> From<LookupRef<V>> for ValuePlace<V> {
    fn from(v: LookupRef<V>) -> Self {
        ValuePlace::LookupRef(v)
    }
}

impl<V: TransactableValueMarker> From<TxFunction> for ValuePlace<V> {
    fn from(v: TxFunction) -> Self {
        ValuePlace::TxFunction(v)
    }
}

impl<V: TransactableValueMarker> From<Vec<ValuePlace<V>>> for ValuePlace<V> {
    fn from(v: Vec<ValuePlace<V>>) -> Self {
        ValuePlace::Vector(v)
    }
}

impl<V: TransactableValueMarker> From<V> for ValuePlace<V> {
    fn from(v: V) -> Self {
        ValuePlace::Atom(v)
    }
}

impl<V: TransactableValueMarker> From<MapNotation<V>> for ValuePlace<V> {
    fn from(v: MapNotation<V>) -> Self {
        ValuePlace::MapNotation(v)
    }
}