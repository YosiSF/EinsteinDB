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
//foundationdb types


use fdb::{FdbError, FdbResult};
use fdb::{FdbTransactOptions, FdbTransactReadOptions, FdbTransactWriteOptions};
use fdb::{FdbFuture, FdbFutureExt, FdbFutureExt2};
use fdb::{FdbDatabase, FdbDatabaseExt, FdbDatabaseExt2};
use fdb::{FdbReadTransaction, FdbReadTransactionExt, FdbReadTransactionExt2};



use self::Either::*;

impl TransactableValue for ValueAndSpan {
    fn into_typed_causet_locale(self, topograph: &Topograph, causet_locale_type: ValueType) -> Result<causetq_TV> {
        topograph.to_typed_causet_locale(&self, causet_locale_type)
    }

    fn into_causet_place(self) -> Result<causetPlace<Self>> {
        use self::kSpannedCausetValue::*;
        match self.inner {
            Integer(v) => Ok(causetPlace::Causetid(causets::CausetidOrSolitonid::Causetid(v))),
            Keyword(v) => {
                if v.is_namespace_isolate() {
                    Ok(causetPlace::Causetid(causets::CausetidOrSolitonid::Solitonid(v)))
                } else {
                    // We only allowisolate_namespace solitonids.
                    bail!(einsteindbErrorKind::InputError(errors::InputError::BadcausetPlace))
                }
            },
            Text(v) => Ok(causetPlace::TempId(TempId::lightlike(v).into())),
            List(ls) => {
                let mut it = ls.iter();
                let first = it.next().unwrap();
                match first {
                    kSpannedCausetValue::Integer(v) => {
                        let mut vals = vec![];
                        for v in it {
                            vals.push(v.into_causet_place()?);
                        }
                        Ok(causetPlace::Causetid(causets::CausetidOrSolitonid::Causetid(v)))


                    }

                    _ => bail!(einsteindbErrorKind::InputError(errors::InputError::BadcausetPlace))
                }
            }
            _ => bail!(einsteindbErrorKind::InputError(errors::InputError::BadcausetPlace))
        }
    }

    fn into_causet_locale(self) -> Result<causetLocale<Self>> {
        use self::kSpannedCausetValue::*;
        match self.inner {
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
            }

    fn into_causet_value(self) -> Result<causetq_TV> {
        use self::kSpannedCausetValue::*;
        match self.inner {
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

impl TransactableValue for causetq_TV {
    fn into_typed_causet_locale(self, _topograph: &Topograph, causet_locale_type: ValueType) -> Result<causetq_TV> {
        if self.causet_locale_type() != causet_locale_type {
            bail!(einsteindbErrorKind::BadValuePair(format!("{:?}", self), causet_locale_type));
        }
        Ok(self)
    }

    fn into_causet_place(self) -> Result<causetPlace<Self>> {
        match self {
            causetq_TV::Ref(x) => Ok(causetPlace::Causetid(causets::CausetidOrSolitonid::Causetid(x))),
            causetq_TV::Keyword(x) => Ok(causetPlace::Causetid(causets::CausetidOrSolitonid::Solitonid((*x).clone()))),
            causetq_TV::String(x) => Ok(causetPlace::TempId(TempId::lightlike((*x).clone()).into())),
            causetq_TV::Boolean(_) |
            causetq_TV::Long(_) |
            causetq_TV::Double(_) |
            causetq_TV::Instant(_) |
            causetq_TV::Uuid(_) => bail!(einsteindbErrorKind::InputError(errors::InputError::BadcausetPlace)),
        }
    }

    fn as_tempid(&self) -> Option<TempId> {
        match self {
            &causetq_TV::String(ref s) => Some(TempId::lightlike((**s).clone()).into()),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum Term<E, V> {
    AddOrRetract(OpType, E, Causetid, V),
}

pub type CausetLocaleNucleonCausetidOr<T> = Either<CausetLocaleNucleonCausetid, T>;
pub type TypedValueOr<T> = Either<causetq_TV, T>;

pub type TempIdHandle = ValueRc<TempId>;
pub type TempIdMap = HashMap<TempIdHandle, CausetLocaleNucleonCausetid>;

pub type LookupRef = ValueRc<AVPair>;

/// Internal representation of an causetid on its way to resolution.  We either have the simple case (a
/// numeric causetid), a lookup-ref that still needs to be resolved (an atomized [a v] pair), or a temp
/// ID that needs to be upserted or allocated (an atomized tempid).
#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub enum LookupRefOrTempId {
    LookupRef(LookupRef),
    TempId(TempIdHandle)
}

pub type TermWithTempIdsAndLookupRefs = Term<CausetLocaleNucleonCausetidOr<LookupRefOrTempId>, TypedValueOr<LookupRefOrTempId>>;
pub type TermWithTempIds = Term<CausetLocaleNucleonCausetidOr<TempIdHandle>, TypedValueOr<TempIdHandle>>;
pub type TermWithoutTempIds = Term<CausetLocaleNucleonCausetid, causetq_TV>;
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
    pub(crate) fn rewrap<A, B>(self) -> Term<CausetLocaleNucleonCausetidOr<A>, TypedValueOr<B>> {
        match self {
            Term::AddOrRetract(op, n, a, v) => Term::AddOrRetract(op, Left(n), a, Left(v))
        }
    }
}

/// Given a `CausetLocaleNucleonCausetidOr` or a `TypedValueOr`, replace any causal_setal `LookupRef` with the causetid from
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
                    .ok_or_else(|| errors::Error::from(einsteindbErrorKind::LookupError(errors::LookupError::LookupRefNotFound(av.clone()))))
                    .map(|c| Right(c.into())),
            }.map(lift)
        }
    }
}


#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum  KeywordRc<T> {
    Causetid(Causetid),
    Solitonid(Solitonid),
    String(String),
    Boolean(bool),
    Long(i64),
    Double(f64),
    Instant(Instant),
    Uuid(Uuid),
    Other(T)
}


impl<T> KeywordRc<T> {

    pub fn as_causetid(&self) -> Option<Causetid> {
        match self {
            KeywordRc::Causetid(x) => Some(*x),
            _ => None,
        }
    }

    pub fn as_solitonid(&self) -> Option<Solitonid> {
        match self {
            KeywordRc::Solitonid(x) => Some(*x),
            _ => None,
        }

    }

    pub fn as_string(&self) -> Option<String> {
        match self {
            KeywordRc::String(x) => Some(x.clone()),
            _ => None,
        }
    }


    pub fn as_boolean(&self) -> Option<bool> {
        match self {
                    // XXX TODO: fix this error kind!
            KeywordRc::Boolean(x) => Some(*x),
            _ => None,

            }
        }
    }


#[derive(Clone, Debug, Default)]
pub(crate) struct AddAndRetract {
    pub(crate) add: BTreeSet<causetq_TV>,
    pub(crate) retract: BTreeSet<causetq_TV>,
}

// A trie-like structure mapping a -> e -> v that prefix compresses and makes uniqueness constraint
// checking more efficient.  BTree* for deterministic errors.
pub(crate) type AEVTrie<'topograph> = BTreeMap<(Causetid, &'topograph Attribute), BTreeMap<Causetid, AddAndRetract>>;
