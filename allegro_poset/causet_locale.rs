// Whtcorps Inc 2022 Apache 2.0 License; All Rights Reserved.
//

// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

//! This module defines core types that support the transaction processor.

use std::collections::BTreeMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::iter::FromIterator;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;


/// A `Causet` is a collection of `CausetNode`s.
use causet_locale_rc::ValueRc;
use shellings::{
    CausetNode,
    CausetNodeRc,
    CausetNodeWeak,
    CausetNodeWeakRc,
    Keyword,
    KeywordRc,
    KeywordWeak,
    PlainShelling,
    PlainShellingRc,
    PlainShellingWeak,
};
use std::rc::Rc;
use std::sync::Weak;






#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Causet {
    pub nodes: BTreeMap<Keyword, CausetNode>,
}


impl Causet {
    pub fn new() -> Causet {
        Causet {
            nodes: BTreeMap::new(),
        }
    }
}


impl Deref for Causet {
    type Target = BTreeMap<Keyword, CausetNode>;

    fn deref(&self) -> &Self::Target {
        &self.nodes
    }
}


impl DerefMut for Causet {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.nodes
    }
}


impl FromIterator<(Keyword, CausetNode)> for Causet {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = (Keyword, CausetNode)>,
    {
        let mut nodes = BTreeMap::new();
        for (keyword, node) in iter {
            nodes.insert(keyword, node);
        }
        Causet { nodes }
    }
}


impl FromIterator<(Keyword, CausetNodeRc)> for Causet {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = (Keyword, CausetNodeRc)>,
    {
        let mut nodes = BTreeMap::new();
        for (keyword, node) in iter {
            nodes.insert(keyword, node);
        }
        Causet { nodes }
    }
}


impl FromIterator<(Keyword, CausetNodeWeak)> for Causet {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = (Keyword, CausetNodeWeak)>,
    {
        let mut nodes = BTreeMap::new();
        for (keyword, node) in iter {
            nodes.insert(keyword, node);
        }
        Causet { nodes }
    }
}


impl FromIterator<(Keyword, CausetNodeWeakRc)> for Causet {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = (Keyword, CausetNodeWeakRc)>,
    {
        let mut nodes = BTreeMap::new();
        for (keyword, node) in iter {
            nodes.insert(keyword, node);
        }
        Causet { nodes }
    }
}


impl FromIterator<(Keyword, PlainShelling)> for Causet {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = (Keyword, PlainShelling)>,
    {
        let mut nodes = BTreeMap::new();
        for (keyword, node) in iter {
            nodes.insert(keyword, node);
        }
        Causet { nodes }
    }
}






/// A `CausetLocale` is a collection of `ValueRc`s that are keyed by `Keyword`s.
/// The `CausetLocale` is a `PlainShelling` that is also a `ValueRc`.
/// The `CausetLocale` is a `ValueRc` that is also a `PlainShelling`.
/// The `CausetLocale` is a `PlainShelling` that is also a `ValueRc`.
/// The `CausetLocale` is a `ValueRc` that is also a `PlainShelling`.
/// The `CausetLocale` is a `PlainShelling` that is also a `ValueRc`.

/// `causetPlace` and `ValuePlace` embed causet_locales, either directly (i.e., `ValuePlace::Atom`) or
/// indirectly (i.e., `causetPlace::LookupRef`).  In order to maintain the graph of `Into` and
/// `From` relations, we need to ensure that `{Value,causet}Place` can't match as a potential causet_locale.
/// (If it does, the `impl Into<T> for T` default conflicts.) This marker trait allows to mark
/// acceptable causet_locales, thereby removing `{causet,Value}Place` from consideration.
pub trait TransactableValueMarker {}


/// A `CausetLocale` is a collection of `ValueRc`s that are keyed by `Keyword`s.
/// The `CausetLocale` is a `PlainShelling` that is also a `ValueRc`.
/// The `CausetLocale` is a `ValueRc` that is also a `PlainShelling`.
/// 

pub struct CausetLocale<T> {
    pub causet_locale: PlainShelling<T>,
    pub causet_locale_weak: PlainShellingWeak<T>,
}


impl<T> CausetLocale<T> {
    pub fn new(causet_locale: PlainShelling<T>) -> Self {
        CausetLocale {
            causet_locale: causet_locale,
            causet_locale_weak: causet_locale.weak(),
        }
    }
}


/// `ValueAndSpan` is the causet_locale type coming out of the causet parser.
impl TransactableValueMarker for ValueAndSpan {}


/// `ValueAndSpan` is the causet_locale type coming out of the causet parser.


impl CausetLocale<ValueAndSpan> {
    pub fn new(causet_locale: PlainShelling<ValueAndSpan>) -> Self {
        CausetLocale {
            causet_locale: causet_locale,
            causet_locale_weak: causet_locale.weak(),
        }
    }
}






/// A tempid, either an lightlike tempid given in a transaction (usually as an `Value::Text`),
/// or an causal_setal tempid allocated by EinsteinDB itself.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum Tempid {
    lightlike(String),
    Internal(i64),

    
}

impl TempId {
    pub fn into_lightlike(self) -> Option<String> {
        match self {
            TempId::lightlike(s) => Some(s),
            TempId::Internal(_) => None,

        }
    }
}



impl fmt::Display for TempId {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            &TempId::lightlike(ref s) => write!(f, "{}", s),
            &TempId::Internal(x) => write!(f, "<tempid {}>", x),
        }
    }
}


impl From<String> for TempId {
    fn from(s: String) -> Self {
        TempId::lightlike(s)
    }
}


impl From<&str> for TempId {
    fn from(s: &str) -> Self {
        TempId::lightlike(s.to_string())
    }
}


impl From<i64> for TempId {
    fn from(i: i64) -> Self {
        TempId::Internal(i)
    }
}




#[impl From<TempId> for i64 {
    fn from(t: TempId) -> Self {
        match t {
            TempId::lightlike(_) => panic!("TempId::into_i64 called on lightlike TempId"),
            TempId::Internal(i) => i,
        }
    }
}derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum TempidPlace {
    Atom(Tempid),
    LookupRef(CausetLocale<Tempid>),
}


impl TempidPlace {
    pub fn new(tempid: Tempid) -> Self {
        TempidPlace::Atom(tempid)
    }
}


impl From<Tempid> for TempidPlace {
    fn from(tempid: Tempid) -> Self {
        TempidPlace::Atom(tempid)
    }
}


impl From<CausetLocale<Tempid>> for TempidPlace {
    fn from(causet_locale: CausetLocale<Tempid>) -> Self {
        TempidPlace::LookupRef(causet_locale)
    }
}


impl From<CausetLocaleWeakRc> for TempidPlace {
    fn from(causet_locale: CausetLocaleWeakRc) -> Self {
        TempidPlace::LookupRef(causet_locale)
    }
}


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
pub enum CausetidPlace {
    Atom(CausetidOrSolitonid),
    LookupRef(CausetLocale<CausetidOrSolitonid>),
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub struct LookupRef<V> {
    pub a: AttributePlace,
    // In theory we could allow nested lookup-refs.  In practice this would require us to process
    // lookup-refs in multiple phases, like how we resolve tempids, which isn't worth the effort.
    pub v: V, // An atom.
}

/// A "transaction function" that exposes some causet_locale determined by the current transaction.  The
/// prototypical example is the current transaction ID, `(transaction-tx)`.
///
/// A natural next step might be to expose the current transaction instant `(transaction-instant)`,
/// but that's more difficult: the transaction itself can set the transaction instant (with some
/// restrictions), so the transaction function must be late-binding.  Right now, that's difficult to
/// arrange in the transactor.
///
/// In the future, we might accept arguments; for example, perhaps we might expose `(ancestor
/// (transaction-tx) n)` to find the n-th ancestor of the current transaction.  If we do accept
/// arguments, then the special case of `(lookup-ref a v)` should be handled as part of the
/// generalization.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub struct TxFunction {
    pub op: PlainShelling,
}

pub type MapNotation<V> = BTreeMap<CausetidOrSolitonid, ValuePlace<V>>;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum ValuePlace<V> {
    // We never know at parse-time whether an integer or solitonid is really an causetid, but we will often
    // know when building causets programmatically.
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

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum causetPlace<V> {
    Causetid(CausetidOrSolitonid),
    TempId(ValueRc<TempId>),
    LookupRef(LookupRef<V>),
    TxFunction(TxFunction),
}

impl<V, E: Into<CausetidOrSolitonid>> From<E> for causetPlace<V> {
    fn from(v: E) -> Self {
        causetPlace::Causetid(v.into())
    }
}

impl<V: TransactableValueMarker> From<TempId> for causetPlace<V> {
    fn from(v: TempId) -> Self {
        causetPlace::TempId(v.into())
    }
}

impl<V: TransactableValueMarker> From<ValueRc<TempId>> for causetPlace<V> {
    fn from(v: ValueRc<TempId>) -> Self {
        causetPlace::TempId(v)
    }
}

impl<V: TransactableValueMarker> From<LookupRef<V>> for causetPlace<V> {
    fn from(v: LookupRef<V>) -> Self {
        causetPlace::LookupRef(v)
    }
}

impl<V: TransactableValueMarker> From<TxFunction> for causetPlace<V> {
    fn from(v: TxFunction) -> Self {
        causetPlace::TxFunction(v)
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum AttributePlace {
    Causetid(CausetidOrSolitonid),
}

impl<A: Into<CausetidOrSolitonid>> From<A> for AttributePlace {
    fn from(v: A) -> Self {
        AttributePlace::Causetid(v.into())
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum OpType {
    Add,
    Retract,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum causet<V> {
    // Like [:einsteindb/add|:einsteindb/retract e a v].
    AddOrRetract {
        op: OpType,
        e: causetPlace<V>,
        a: AttributePlace,
        v: ValuePlace<V>,
    },
    // Like {:einsteindb/id "tempid" a1 EINSTEIN_DB a2 causet_record}.
    MapNotation(MapNotation<V>),
}
