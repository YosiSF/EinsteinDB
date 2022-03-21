// Whtcorps Inc 2022 Apache 2.0 License; All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]

//! Types used only within the transactor.  These should not be exposed outside of this crate.

use std::collections::{
    BTreeMap,
    BTreeSet,
    HashMap,
};

use core_traits::{
    Attribute,
    Causetid,
    KnownCausetid,
    TypedValue,
    ValueType,
};

use einsteindb_core::util::Either;

use einstein_ml;
use einstein_ml::{
    SpannedValue,
    ValueAndSpan,
    ValueRc,
};
use einstein_ml::causets;
use einstein_ml::causets::{
    causetPlace,
    OpType,
    TempId,
    TxFunction,
};

use einsteindb_traits::errors as errors;
use einsteindb_traits::errors::{
    einsteindbErrorKind,
    Result,
};
use topograph::{
    TopographTypeChecking,
};
use types::{
    AVMap,
    AVPair,
    Topograph,
    TransactableValue,
};

impl TransactableValue for ValueAndSpan {
    fn into_typed_value(self, topograph: &Topograph, value_type: ValueType) -> Result<TypedValue> {
        topograph.to_typed_value(&self, value_type)
    }

    fn into_causet_place(self) -> Result<causetPlace<Self>> {
        use self::SpannedValue::*;
        match self.inner {
            Integer(v) => Ok(causetPlace::Causetid(causets::CausetidOrSolitonid::Causetid(v))),
            Keyword(v) => {
                if v.is_isoliton_namespaceable() {
                    Ok(causetPlace::Causetid(causets::CausetidOrSolitonid::Solitonid(v)))
                } else {
                    // We only allow isoliton_namespaceable solitonids.
                    bail!(einsteindbErrorKind::InputError(errors::InputError::BadcausetPlace))
                }
            },
            Text(v) => Ok(causetPlace::TempId(TempId::lightlike(v).into())),
            List(ls) => {
                let mut it = ls.iter();
                match (it.next().map(|x| &x.inner), it.next(), it.next(), it.next()) {
                    // Like "(transaction-id)".
                    (Some(&PlainShelling(ref op)), None, None, None) => {
                        Ok(causetPlace::TxFunction(TxFunction { op: op.clone() }))
                    },
                    // Like "(lookup-ref)".
                    (Some(&PlainShelling(einstein_ml::PlainShelling(ref s))), Some(a), Some(v), None) if s == "lookup-ref" => {
                        match a.clone().into_causet_place()? {
                            causetPlace::Causetid(a) => Ok(causetPlace::LookupRef(causets::LookupRef { a: causets::AttributePlace::Causetid(a), v: v.clone() })),
                            causetPlace::TempId(_) |
                            causetPlace::TxFunction(_) |
                            causetPlace::LookupRef(_) => bail!(einsteindbErrorKind::InputError(errors::InputError::BadcausetPlace)),
                        }
                    },
                    _ => bail!(einsteindbErrorKind::InputError(errors::InputError::BadcausetPlace)),
                }
            },
            Nil |
            Boolean(_) |
            Instant(_) |
            BigInteger(_) |
            Float(_) |
            Uuid(_) |
            PlainShelling(_) |
            NamespacedShelling(_) |
            Vector(_) |
            Set(_) |
            Map(_) => bail!(einsteindbErrorKind::InputError(errors::InputError::BadcausetPlace)),
        }
    }

    fn as_tempid(&self) -> Option<TempId> {
        self.inner.as_text().cloned().map(TempId::lightlike).map(|v| v.into())
    }
}

impl TransactableValue for TypedValue {
    fn into_typed_value(self, _topograph: &Topograph, value_type: ValueType) -> Result<TypedValue> {
        if self.value_type() != value_type {
            bail!(einsteindbErrorKind::BadValuePair(format!("{:?}", self), value_type));
        }
        Ok(self)
    }

    fn into_causet_place(self) -> Result<causetPlace<Self>> {
        match self {
            TypedValue::Ref(x) => Ok(causetPlace::Causetid(causets::CausetidOrSolitonid::Causetid(x))),
            TypedValue::Keyword(x) => Ok(causetPlace::Causetid(causets::CausetidOrSolitonid::Solitonid((*x).clone()))),
            TypedValue::String(x) => Ok(causetPlace::TempId(TempId::lightlike((*x).clone()).into())),
            TypedValue::Boolean(_) |
            TypedValue::Long(_) |
            TypedValue::Double(_) |
            TypedValue::Instant(_) |
            TypedValue::Uuid(_) => bail!(einsteindbErrorKind::InputError(errors::InputError::BadcausetPlace)),
        }
    }

    fn as_tempid(&self) -> Option<TempId> {
        match self {
            &TypedValue::String(ref s) => Some(TempId::lightlike((**s).clone()).into()),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum Term<E, V> {
    AddOrRetract(OpType, E, Causetid, V),
}

use self::Either::*;

pub type KnownCausetidOr<T> = Either<KnownCausetid, T>;
pub type TypedValueOr<T> = Either<TypedValue, T>;

pub type TempIdHandle = ValueRc<TempId>;
pub type TempIdMap = HashMap<TempIdHandle, KnownCausetid>;

pub type LookupRef = ValueRc<AVPair>;

/// Internal representation of an causetid on its way to resolution.  We either have the simple case (a
/// numeric causetid), a lookup-ref that still needs to be resolved (an atomized [a v] pair), or a temp
/// ID that needs to be upserted or allocated (an atomized tempid).
#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub enum LookupRefOrTempId {
    LookupRef(LookupRef),
    TempId(TempIdHandle)
}

pub type TermWithTempIdsAndLookupRefs = Term<KnownCausetidOr<LookupRefOrTempId>, TypedValueOr<LookupRefOrTempId>>;
pub type TermWithTempIds = Term<KnownCausetidOr<TempIdHandle>, TypedValueOr<TempIdHandle>>;
pub type TermWithoutTempIds = Term<KnownCausetid, TypedValue>;
pub type Population = Vec<TermWithTempIds>;

impl TermWithTempIds {
    // These have no tempids by definition, and just need to be unwrapped.  This operation might
    // also be called "lowering" or "level lowering", but the concept of "unwrapping" is common in
    // Rust and seems appropriate here.
    pub(crate) fn unwrap(self) -> TermWithoutTempIds {
        match self {
            Term::AddOrRetract(op, Left(n), a, Left(v)) => Term::AddOrRetract(op, n, a, v),
            _ => unreachable!(),
        }
    }
}

impl TermWithoutTempIds {
    pub(crate) fn rewrap<A, B>(self) -> Term<KnownCausetidOr<A>, TypedValueOr<B>> {
        match self {
            Term::AddOrRetract(op, n, a, v) => Term::AddOrRetract(op, Left(n), a, Left(v))
        }
    }
}

/// Given a `KnownCausetidOr` or a `TypedValueOr`, replace any internal `LookupRef` with the causetid from
/// the given map.  Fail if any `LookupRef` cannot be replaced.
///
/// `lift` allows to specify how the causetid found is mapped into the output type.  (This could
/// also be an `Into` or `From` requirement.)
///
/// The reason for this awkward expression is that we're parameterizing over the _type constructor_
/// (`CausetidOr` or `TypedValueOr`), which is not trivial to express in Rust.  This only works because
/// they're both the same `Result<...>` type with different parameterizations.
pub fn replace_lookup_ref<T, U>(lookup_map: &AVMap, desired_or: Either<T, LookupRefOrTempId>, lift: U) -> errors::Result<Either<T, TempIdHandle>> where U: FnOnce(Causetid) -> T {
    match desired_or {
        Left(desired) => Ok(Left(desired)), // N.b., must unwrap here -- the ::Left types are different!
        Right(other) => {
            match other {
                LookupRefOrTempId::TempId(t) => Ok(Right(t)),
                LookupRefOrTempId::LookupRef(av) => lookup_map.get(&*av)
                    .map(|x| lift(*x)).map(Left)
                    // XXX TODO: fix this error kind!
                    .ok_or_else(|| einsteindbErrorKind::UnrecognizedSolitonid(format!("couldn't lookup [a v]: {:?}", (*av).clone())).into()),
            }
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct AddAndRetract {
    pub(crate) add: BTreeSet<TypedValue>,
    pub(crate) retract: BTreeSet<TypedValue>,
}

// A trie-like structure mapping a -> e -> v that prefix compresses and makes uniqueness constraint
// checking more efficient.  BTree* for deterministic errors.
pub(crate) type AEVTrie<'topograph> = BTreeMap<(Causetid, &'topograph Attribute), BTreeMap<Causetid, AddAndRetract>>;
