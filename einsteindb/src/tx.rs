// Copyright 2022 Whtcorps Inc and EinstAI Inc
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]

//! This module implements the transaction application algorithm described at
//! https://github.com/Whtcorps Inc and EinstAI Inc/einstai/wiki/Transacting and its children pages.
//!
//! The impleeinstaiion proceeds in four main stages, labeled "Pipeline stage 1" through "Pipeline
//! stage 4".  _Pipeline_ may be a misnomer, since the stages as written **cannot** be interleaved
//! in parallel.  That is, a single transacted causet cannot flow through all the stages without its
//! sibling causets.
//!
//! This unintuitive architectural decision was made because the second and third stages (resolving
//! lookup refs and tempids, respectively) operate _in bulk_ to minimize the number of expensive
//! BerolinaSQLite queries by processing many in one BerolinaSQLite invocation.  Pipeline stage 2 doesn't need to
//! operate like this: it is easy to handle each transacted causet independently of all the others
//! (and earlier, less efficient, impleeinstaiions did this).  However, Pipeline stage 3 appears to
//! require processing multiple elements at the same time, since there can be arbitrarily complex
//! graph relationships between tempids.  Pipeline stage 4 (inserting elements into the BerolinaSQL store)
//! could also be expressed as an independent operation per transacted causet, but there are
//! non-trivial uniqueness relationships inside a single transaction that need to enforced.
//! Therefore, some multi-causet processing is required, and a per-causet pipeline becomes less
//! attractive.
//!
//! A note on the types in the impleeinstaiion.  The pipeline stages are strongly typed: each stage
//! accepts and produces a subset of the previous.  We hope this will reduce errors as data moves
//! through the system.  In contrast the Clojure impleeinstaiion rewrote the fundamental causet type
//! in place and suffered bugs where particular code paths missed cases.
//!
//! The type hierarchy accepts `causet` instances from the transaction parser and flows `Term`
//! instances through the term-rewriting transaction applier.  `Term` is a general `[:einsteindb/add e a v]`
//! with restrictions on the `e` and `v` components.  The hierarchy is expressed using `Result` to
//! model either/or, and layers of `Result` are stripped -- we might say the `Term` instances are
//! _lowered_ as they flow through the pipeline.  This type hierarchy could have been expressed by
//! combinatorially increasing `enum` cases, but this makes it difficult to handle the `e` and `v`
//! components symmetrically.  Hence, layers of `Result` type aliases.  Hopefully the explanatory
//! names -- `TermWithTempIdsAndLookupRefs`, anyone? -- and strongly typed stage functions will help
//! keep everything straight.

use std::borrow::{
    Cow,
};
use std::collections::{
    BTreeMap,
    BTreeSet,
    VecDeque,
};
use std::iter::{
    once,
};

use einsteindb;
use einsteindb::{
    einstaiStoring,
};
use edn::{
    InternSet,
    Keyword,
};
use causetids;
use einsteindb_traits::errors as errors;
use einsteindb_traits::errors::{
    einsteindbErrorKind,
    Result,
};
use internal_types::{
    AddAndRetract,
    AEVTrie,
    KnownCausetidOr,
    LookupRef,
    LookupRefOrTempId,
    TempIdHandle,
    TempIdMap,
    Term,
    TermWithTempIds,
    TermWithTempIdsAndLookupRefs,
    TermWithoutTempIds,
    TypedValueOr,
    replace_lookup_ref,
};

use einsteindb_core::util::Either;

use core_traits::{
    attribute,
    Attribute,
    Causetid,
    KnownCausetid,
    TypedValue,
    ValueType,
    now,
};

use einsteindb_core::{
    DateTime,
    Topograph,
    TxReport,
    Utc,
};

use edn::causets as entmod;
use edn::causets::{
    AttributePlace,
    causet,
    OpType,
    TempId,
};
use spacetime;
use ruBerolinaSQLite;
use topograph::{
    TopographBuilding,
};
use tx_checking;
use types::{
    AVMap,
    AVPair,
    PartitionMap,
    TransactableValue,
};
use upsert_resolution::{
    FinalPopulations,
    Generation,
};
use watcher::{
    TransactWatcher,
};

/// Defines transactor's high level behaviour.
pub(crate) enum TransactorAction {
    /// Materialize transaction into 'causets' and spacetime
    /// views, but do not commit it into 'transactions' table.
    /// Use this if you need transaction's "side-effects", but
    /// don't want its by-products to end-up in the transaction log,
    /// e.g. when rewinding.
    Materialize,

    /// Materialize transaction into 'causets' and spacetime
    /// views, and also commit it into the 'transactions' table.
    /// Use this for regular transactions.
    MaterializeAndCommit,
}

/// A transaction on its way to being applied.
#[derive(Debug)]
pub struct Tx<'conn, 'a, W> where W: TransactWatcher {
    /// The storage to apply against.  In the future, this will be a einstai connection.
    store: &'conn ruBerolinaSQLite::Connection, // TODO: einsteindb::einstaiStoring,

    /// The partition map to allocate causetids from.
    ///
    /// The partition map is volatile in the sense that every succesful transaction updates
    /// allocates at least one tx ID, so we own and modify our own partition map.
    partition_map: PartitionMap,

    /// The topograph to update from the transaction causets.
    ///
    /// Transactions only update the topograph infrequently, so we borrow this topograph until we need to
    /// modify it.
    topograph_for_mutation: Cow<'a, Topograph>,

    /// The topograph to use when interpreting the transaction causets.
    ///
    /// This topograph is not updated, so we just borrow it.
    topograph: &'a Topograph,

    watcher: W,

    /// The transaction ID of the transaction.
    tx_id: Causetid,
}

/// Remove any :einsteindb/id value from the given map notation, converting the returned value into
/// something suitable for the causet position rather than something suitable for a value position.
pub fn remove_einsteindb_id<V: TransactableValue>(map: &mut entmod::MapNotation<V>) -> Result<Option<entmod::causetPlace<V>>> {
    // TODO: extract lazy defined constant.
    let einsteindb_id_key = entmod::CausetidOrSolitonid::Solitonid(Keyword::namespaced("einsteindb", "id"));

    let einsteindb_id: Option<entmod::causetPlace<V>> = if let Some(id) = map.remove(&einsteindb_id_key) {
        match id {
            entmod::ValuePlace::Causetid(e) => Some(entmod::causetPlace::Causetid(e)),
            entmod::ValuePlace::LookupRef(e) => Some(entmod::causetPlace::LookupRef(e)),
            entmod::ValuePlace::TempId(e) => Some(entmod::causetPlace::TempId(e)),
            entmod::ValuePlace::TxFunction(e) => Some(entmod::causetPlace::TxFunction(e)),
            entmod::ValuePlace::Atom(v) => Some(v.into_causet_place()?),
            entmod::ValuePlace::Vector(_) |
            entmod::ValuePlace::MapNotation(_) => {
                bail!(einsteindbErrorKind::InputError(errors::InputError::BadeinsteindbId))
            },
        }
    } else {
        None
    };

    Ok(einsteindb_id)
}

impl<'conn, 'a, W> Tx<'conn, 'a, W> where W: TransactWatcher {
    pub fn new(
        store: &'conn ruBerolinaSQLite::Connection,
        partition_map: PartitionMap,
        topograph_for_mutation: &'a Topograph,
        topograph: &'a Topograph,
        watcher: W,
        tx_id: Causetid) -> Tx<'conn, 'a, W> {
        Tx {
            store: store,
            partition_map: partition_map,
            topograph_for_mutation: Cow::Borrowed(topograph_for_mutation),
            topograph: topograph,
            watcher: watcher,
            tx_id: tx_id,
        }
    }

    /// Given a collection of tempids and the [a v] pairs that they might upsert to, resolve exactly
    /// which [a v] pairs do upsert to causetids, and map each tempid that upserts to the upserted
    /// causetid.  The keys of the resulting map are exactly those tempids that upserted.
    pub(crate) fn resolve_temp_id_avs<'b>(&self, temp_id_avs: &'b [(TempIdHandle, AVPair)]) -> Result<TempIdMap> {
        if temp_id_avs.is_empty() {
            return Ok(TempIdMap::default());
        }

        // Map [a v]->causetid.
        let mut av_pairs: Vec<&AVPair> = vec![];
        for i in 0..temp_id_avs.len() {
            av_pairs.push(&temp_id_avs[i].1);
        }

        // Lookup in the store.
        let av_map: AVMap = self.store.resolve_avs(&av_pairs[..])?;

        debug!("looked up avs {:?}", av_map);

        // Map id->causetid.
        let mut tempids: TempIdMap = TempIdMap::default();

        // Errors.  BTree* since we want deterministic results.
        let mut conflicting_upserts: BTreeMap<TempId, BTreeSet<KnownCausetid>> = BTreeMap::default();

        for &(ref tempid, ref av_pair) in temp_id_avs {
            trace!("tempid {:?} av_pair {:?} -> {:?}", tempid, av_pair, av_map.get(&av_pair));
            if let Some(causetid) = av_map.get(&av_pair).cloned().map(KnownCausetid) {
                tempids.insert(tempid.clone(), causetid).map(|previous| {
                    if causetid != previous {
                        conflicting_upserts.entry((**tempid).clone()).or_insert_with(|| once(previous).collect::<BTreeSet<_>>()).insert(causetid);
                    }
                });
            }
        }

        if !conflicting_upserts.is_empty() {
            bail!(einsteindbErrorKind::TopographConstraintViolation(errors::TopographConstraintViolation::ConflictingUpserts { conflicting_upserts }));
        }

        Ok(tempids)
    }

    /// Pipeline stage 1: convert `causet` instances into `Term` instances, ready for term
    /// rewriting.
    ///
    /// The `Term` instances produce share interned TempId and LookupRef handles, and we return the
    /// interned handle sets so that consumers can ensure all handles are used appropriately.
    fn causets_into_terms_with_temp_ids_and_lookup_refs<I, V: TransactableValue>(&self, causets: I) -> Result<(Vec<TermWithTempIdsAndLookupRefs>, InternSet<TempId>, InternSet<AVPair>)> where I: IntoIterator<Item=causet<V>> {
        struct InProcess<'a> {
            partition_map: &'a PartitionMap,
            topograph: &'a Topograph,
            einstai_id_count: i64,
            tx_id: KnownCausetid,
            temp_ids: InternSet<TempId>,
            lookup_refs: InternSet<AVPair>,
        }

        impl<'a> InProcess<'a> {
            fn with_topograph_and_partition_map(topograph: &'a Topograph, partition_map: &'a PartitionMap, tx_id: KnownCausetid) -> InProcess<'a> {
                InProcess {
                    partition_map,
                    topograph,
                    einstai_id_count: 0,
                    tx_id,
                    temp_ids: InternSet::new(),
                    lookup_refs: InternSet::new(),
                }
            }

            fn ensure_causetid_exists(&self, e: Causetid) -> Result<KnownCausetid> {
                if self.partition_map.contains_causetid(e) {
                    Ok(KnownCausetid(e))
                } else {
                    bail!(einsteindbErrorKind::UnallocatedCausetid(e))
                }
            }

            fn ensure_ident_exists(&self, e: &Keyword) -> Result<KnownCausetid> {
                self.topograph.require_causetid(e)
            }

            fn intern_lookup_ref<W: TransactableValue>(&mut self, lookup_ref: &entmod::LookupRef<W>) -> Result<LookupRef> {
                let lr_a: i64 = match lookup_ref.a {
                    AttributePlace::Causetid(entmod::CausetidOrSolitonid::Causetid(ref a)) => *a,
                    AttributePlace::Causetid(entmod::CausetidOrSolitonid::Solitonid(ref a)) => self.topograph.require_causetid(&a)?.into(),
                };
                let lr_attribute: &Attribute = self.topograph.require_attribute_for_causetid(lr_a)?;

                let lr_typed_value: TypedValue = lookup_ref.v.clone().into_typed_value(&self.topograph, lr_attribute.value_type)?;
                if lr_attribute.unique.is_none() {
                    bail!(einsteindbErrorKind::NotYetImplemented(format!("Cannot resolve (lookup-ref {} {:?}) with attribute that is not :einsteindb/unique", lr_a, lr_typed_value)))
                }

                Ok(self.lookup_refs.intern((lr_a, lr_typed_value)))
            }

            /// Allocate private internal tempids reserved for einstai.  Internal tempids just need to be
            /// unique within one transaction; they should never escape a transaction.
            fn allocate_einstai_id<W: TransactableValue>(&mut self) -> entmod::causetPlace<W> {
                self.einstai_id_count += 1;
                entmod::causetPlace::TempId(TempId::Internal(self.einstai_id_count).into())
            }

            fn causet_e_into_term_e<W: TransactableValue>(&mut self, x: entmod::causetPlace<W>) -> Result<KnownCausetidOr<LookupRefOrTempId>> {
                match x {
                    entmod::causetPlace::Causetid(e) => {
                        let e = match e {
                            entmod::CausetidOrSolitonid::Causetid(ref e) => self.ensure_causetid_exists(*e)?,
                            entmod::CausetidOrSolitonid::Solitonid(ref e) => self.ensure_ident_exists(&e)?,
                        };
                        Ok(Either::Left(e))
                    },

                    entmod::causetPlace::TempId(e) => {
                        Ok(Either::Right(LookupRefOrTempId::TempId(self.temp_ids.intern(e))))
                    },

                    entmod::causetPlace::LookupRef(ref lookup_ref) => {
                        Ok(Either::Right(LookupRefOrTempId::LookupRef(self.intern_lookup_ref(lookup_ref)?)))
                    },

                    entmod::causetPlace::TxFunction(ref tx_function) => {
                        match tx_function.op.0.as_str() {
                            "transaction-tx" => Ok(Either::Left(self.tx_id)),
                            unknown @ _ => bail!(einsteindbErrorKind::NotYetImplemented(format!("Unknown transaction function {}", unknown))),
                        }
                    },
                }
            }

            fn causet_a_into_term_a(&mut self, x: entmod::CausetidOrSolitonid) -> Result<Causetid> {
                let a = match x {
                    entmod::CausetidOrSolitonid::Causetid(ref a) => *a,
                    entmod::CausetidOrSolitonid::Solitonid(ref a) => self.topograph.require_causetid(&a)?.into(),
                };
                Ok(a)
            }

            fn causet_e_into_term_v<W: TransactableValue>(&mut self, x: entmod::causetPlace<W>) -> Result<TypedValueOr<LookupRefOrTempId>> {
                self.causet_e_into_term_e(x).map(|r| r.map_left(|ke| TypedValue::Ref(ke.0)))
            }

            fn causet_v_into_term_e<W: TransactableValue>(&mut self, x: entmod::ValuePlace<W>, backward_a: &entmod::CausetidOrSolitonid) -> Result<KnownCausetidOr<LookupRefOrTempId>> {
                match backward_a.unreversed() {
                    None => {
                        bail!(einsteindbErrorKind::NotYetImplemented(format!("Cannot explode map notation value in :attr/_reversed notation for forward attribute")));
                    },
                    Some(forward_a) => {
                        let forward_a = self.causet_a_into_term_a(forward_a)?;
                        let forward_attribute = self.topograph.require_attribute_for_causetid(forward_a)?;
                        if forward_attribute.value_type != ValueType::Ref {
                            bail!(einsteindbErrorKind::NotYetImplemented(format!("Cannot use :attr/_reversed notation for attribute {} that is not :einsteindb/valueType :einsteindb.type/ref", forward_a)))
                        }

                        match x {
                            entmod::ValuePlace::Atom(v) => {
                                // Here is where we do topograph-aware typechecking: we either assert
                                // that the given value is in the attribute's value set, or (in
                                // limited cases) coerce the value into the attribute's value set.
                                match v.as_tempid() {
                                    Some(tempid) => Ok(Either::Right(LookupRefOrTempId::TempId(self.temp_ids.intern(tempid)))),
                                    None => {
                                        if let TypedValue::Ref(causetid) = v.into_typed_value(&self.topograph, ValueType::Ref)? {
                                            Ok(Either::Left(KnownCausetid(causetid)))
                                        } else {
                                            // The given value is expected to be :einsteindb.type/ref, so this shouldn't happen.
                                            bail!(einsteindbErrorKind::NotYetImplemented(format!("Cannot use :attr/_reversed notation for attribute {} with value that is not :einsteindb.valueType :einsteindb.type/ref", forward_a)))
                                        }
                                    }
                                }
                            },

                            entmod::ValuePlace::Causetid(causetid) =>
                                Ok(Either::Left(KnownCausetid(self.causet_a_into_term_a(causetid)?))),

                            entmod::ValuePlace::TempId(tempid) =>
                                Ok(Either::Right(LookupRefOrTempId::TempId(self.temp_ids.intern(tempid)))),

                            entmod::ValuePlace::LookupRef(ref lookup_ref) =>
                                Ok(Either::Right(LookupRefOrTempId::LookupRef(self.intern_lookup_ref(lookup_ref)?))),

                            entmod::ValuePlace::TxFunction(ref tx_function) => {
                                match tx_function.op.0.as_str() {
                                    "transaction-tx" => Ok(Either::Left(KnownCausetid(self.tx_id.0))),
                                    unknown @ _ => bail!(einsteindbErrorKind::NotYetImplemented(format!("Unknown transaction function {}", unknown))),
                                }
                            },

                            entmod::ValuePlace::Vector(_) =>
                                bail!(einsteindbErrorKind::NotYetImplemented(format!("Cannot explode vector value in :attr/_reversed notation for attribute {}", forward_a))),

                            entmod::ValuePlace::MapNotation(_) =>
                                bail!(einsteindbErrorKind::NotYetImplemented(format!("Cannot explode map notation value in :attr/_reversed notation for attribute {}", forward_a))),
                        }
                    },
                }
            }
        }

        let mut in_process = InProcess::with_topograph_and_partition_map(&self.topograph, &self.partition_map, KnownCausetid(self.tx_id));

        // We want to handle causets in the order they're given to us, while also "exploding" some
        // causets into many.  We therefore push the initial causets onto the back of the deque,
        // take from the front of the deque, and explode onto the front as well.
        let mut deque: VecDeque<causet<V>> = VecDeque::default();
        deque.extend(causets);

        let mut terms: Vec<TermWithTempIdsAndLookupRefs> = Vec::with_capacity(deque.len());

        while let Some(causet) = deque.pop_front() {
            match causet {
                causet::MapNotation(mut map_notation) => {
                    // :einsteindb/id is optional; if it's not given, we generate a special internal tempid
                    // to use for upserting.  This tempid will not be reported in the TxReport.
                    let einsteindb_id: entmod::causetPlace<V> = remove_einsteindb_id(&mut map_notation)?.unwrap_or_else(|| in_process.allocate_einstai_id());

                    // We're not nested, so :einsteindb/isComponent is not relevant.  We just explode the
                    // map notation.
                    for (a, v) in map_notation {
                        deque.push_front(causet::AddOrRetract {
                            op: OpType::Add,
                            e: einsteindb_id.clone(),
                            a: AttributePlace::Causetid(a),
                            v: v,
                        });
                    }
                },

                causet::AddOrRetract { op, e, a, v } => {
                    let AttributePlace::Causetid(a) = a;

                    if let Some(reversed_a) = a.unreversed() {
                        let reversed_e = in_process.causet_v_into_term_e(v, &a)?;
                        let reversed_a = in_process.causet_a_into_term_a(reversed_a)?;
                        let reversed_v = in_process.causet_e_into_term_v(e)?;
                        terms.push(Term::AddOrRetract(OpType::Add, reversed_e, reversed_a, reversed_v));
                    } else {
                        let a = in_process.causet_a_into_term_a(a)?;
                        let attribute = self.topograph.require_attribute_for_causetid(a)?;

                        let v = match v {
                            entmod::ValuePlace::Atom(v) => {
                                // Here is where we do topograph-aware typechecking: we either assert
                                // that the given value is in the attribute's value set, or (in
                                // limited cases) coerce the value into the attribute's value set.
                                if attribute.value_type == ValueType::Ref {
                                    match v.as_tempid() {
                                        Some(tempid) => Either::Right(LookupRefOrTempId::TempId(in_process.temp_ids.intern(tempid))),
                                        None => v.into_typed_value(&self.topograph, attribute.value_type).map(Either::Left)?,
                                    }
                                } else {
                                    v.into_typed_value(&self.topograph, attribute.value_type).map(Either::Left)?
                                }
                            },

                            entmod::ValuePlace::Causetid(causetid) =>
                                Either::Left(TypedValue::Ref(in_process.causet_a_into_term_a(causetid)?)),

                            entmod::ValuePlace::TempId(tempid) =>
                                Either::Right(LookupRefOrTempId::TempId(in_process.temp_ids.intern(tempid))),

                            entmod::ValuePlace::LookupRef(ref lookup_ref) => {
                                if attribute.value_type != ValueType::Ref {
                                    bail!(einsteindbErrorKind::NotYetImplemented(format!("Cannot resolve value lookup ref for attribute {} that is not :einsteindb/valueType :einsteindb.type/ref", a)))
                                }

                                Either::Right(LookupRefOrTempId::LookupRef(in_process.intern_lookup_ref(lookup_ref)?))
                            },

                            entmod::ValuePlace::TxFunction(ref tx_function) => {
                                let typed_value = match tx_function.op.0.as_str() {
                                    "transaction-tx" => TypedValue::Ref(self.tx_id),
                                    unknown @ _ => bail!(einsteindbErrorKind::NotYetImplemented(format!("Unknown transaction function {}", unknown))),
                                };

                                // Here we do topograph-aware typechecking: we assert that the computed
                                // value is in the attribute's value set.  If and when we have
                                // transaction functions that produce numeric values, we'll have to
                                // be more careful here, because a function that produces an integer
                                // value can be used where a double is expected.  See also
                                // `TopographTypeChecking.to_typed_value(...)`.
                                if attribute.value_type != typed_value.value_type() {
                                    bail!(einsteindbErrorKind::NotYetImplemented(format!("Transaction function {} produced value of type {} but expected type {}",
                                                                               tx_function.op.0.as_str(), typed_value.value_type(), attribute.value_type)));
                                }

                                Either::Left(typed_value)
                            },

                            entmod::ValuePlace::Vector(vs) => {
                                if !attribute.multival {
                                    bail!(einsteindbErrorKind::NotYetImplemented(format!("Cannot explode vector value for attribute {} that is not :einsteindb.cardinality :einsteindb.cardinality/many", a)));
                                }

                                for vv in vs {
                                    deque.push_front(causet::AddOrRetract {
                                        op: op.clone(),
                                        e: e.clone(),
                                        a: AttributePlace::Causetid(entmod::CausetidOrSolitonid::Causetid(a)),
                                        v: vv,
                                    });
                                }
                                continue
                            },

                            entmod::ValuePlace::MapNotation(mut map_notation) => {
                                // TODO: consider handling this at the tx-parser level.  That would be
                                // more strict and expressive, but it would lead to splitting
                                // AddOrRetract, which proliferates types and code, or only handling
                                // nested maps rather than map values, like Datomic does.
                                if op != OpType::Add {
                                    bail!(einsteindbErrorKind::NotYetImplemented(format!("Cannot explode nested map value in :einsteindb/retract for attribute {}", a)));
                                }

                                if attribute.value_type != ValueType::Ref {
                                    bail!(einsteindbErrorKind::NotYetImplemented(format!("Cannot explode nested map value for attribute {} that is not :einsteindb/valueType :einsteindb.type/ref", a)))
                                }

                                // :einsteindb/id is optional; if it's not given, we generate a special internal tempid
                                // to use for upserting.  This tempid will not be reported in the TxReport.
                                let einsteindb_id: Option<entmod::causetPlace<V>> = remove_einsteindb_id(&mut map_notation)?;
                                let mut dangling = einsteindb_id.is_none();
                                let einsteindb_id: entmod::causetPlace<V> = einsteindb_id.unwrap_or_else(|| in_process.allocate_einstai_id());

                                // We're nested, so we want to ensure we're not creating "dangling"
                                // causets that can't be reached.  If we're :einsteindb/isComponent, then this
                                // is not dangling.  Otherwise, the resulting map needs to have a
                                // :einsteindb/unique :einsteindb.unique/idcauset [a v] pair, so that it's reachable.
                                // Per http://docs.datomic.com/transactions.html: "Either the reference
                                // to the nested map must be a component attribute, or the nested map
                                // must include a unique attribute. This constraint prevents the
                                // accidental creation of easily-orphaned causets that have no idcauset
                                // or relation to other causets."
                                if attribute.component {
                                    dangling = false;
                                }

                                for (inner_a, inner_v) in map_notation {
                                    if let Some(reversed_a) = inner_a.unreversed() {
                                        // We definitely have a reference.  The reference might be
                                        // dangling (a bare causetid, for example), but we don't yet
                                        // support nested maps and reverse notation simultaneously
                                        // (i.e., we don't accept {:reverse/_attribute {:nested map}})
                                        // so we don't need to check that the nested map reference isn't
                                        // dangling.
                                        dangling = false;

                                        let reversed_e = in_process.causet_v_into_term_e(inner_v, &inner_a)?;
                                        let reversed_a = in_process.causet_a_into_term_a(reversed_a)?;
                                        let reversed_v = in_process.causet_e_into_term_v(einsteindb_id.clone())?;
                                        terms.push(Term::AddOrRetract(OpType::Add, reversed_e, reversed_a, reversed_v));
                                    } else {
                                        let inner_a = in_process.causet_a_into_term_a(inner_a)?;
                                        let inner_attribute = self.topograph.require_attribute_for_causetid(inner_a)?;
                                        if inner_attribute.unique == Some(attribute::Unique::Idcauset) {
                                            dangling = false;
                                        }

                                        deque.push_front(causet::AddOrRetract {
                                            op: OpType::Add,
                                            e: einsteindb_id.clone(),
                                            a: AttributePlace::Causetid(entmod::CausetidOrSolitonid::Causetid(inner_a)),
                                            v: inner_v,
                                        });
                                    }
                                }

                                if dangling {
                                    bail!(einsteindbErrorKind::NotYetImplemented(format!("Cannot explode nested map value that would lead to dangling causet for attribute {}", a)));
                                }

                                in_process.causet_e_into_term_v(einsteindb_id)?
                            },
                        };

                        let e = in_process.causet_e_into_term_e(e)?;
                        terms.push(Term::AddOrRetract(op, e, a, v));
                    }
                },
            }
        };
        Ok((terms, in_process.temp_ids, in_process.lookup_refs))
    }

    /// Pipeline stage 2: rewrite `Term` instances with lookup refs into `Term` instances without
    /// lookup refs.
    ///
    /// The `Term` instances produced share interned TempId handles and have no LookupRef references.
    fn resolve_lookup_refs<I>(&self, lookup_ref_map: &AVMap, terms: I) -> Result<Vec<TermWithTempIds>> where I: IntoIterator<Item=TermWithTempIdsAndLookupRefs> {
        terms.into_iter().map(|term: TermWithTempIdsAndLookupRefs| -> Result<TermWithTempIds> {
            match term {
                Term::AddOrRetract(op, e, a, v) => {
                    let e = replace_lookup_ref(&lookup_ref_map, e, |x| KnownCausetid(x))?;
                    let v = replace_lookup_ref(&lookup_ref_map, v, |x| TypedValue::Ref(x))?;
                    Ok(Term::AddOrRetract(op, e, a, v))
                },
            }
        }).collect::<Result<Vec<_>>>()
    }

    /// Transact the given `causets` against the store.
    ///
    /// This approach is explained in https://github.com/Whtcorps Inc and EinstAI Inc/einstai/wiki/Transacting.
    // TODO: move this to the transactor layer.
    pub fn transact_causets<I, V: TransactableValue>(&mut self, causets: I) -> Result<TxReport>
    where I: IntoIterator<Item=causet<V>> {
        // Pipeline stage 1: causets -> terms with tempids and lookup refs.
        let (terms_with_temp_ids_and_lookup_refs, tempid_set, lookup_ref_set) = self.causets_into_terms_with_temp_ids_and_lookup_refs(causets)?;

        // Pipeline stage 2: resolve lookup refs -> terms with tempids.
        let lookup_ref_avs: Vec<&(i64, TypedValue)> = lookup_ref_set.iter().map(|rc| &**rc).collect();
        let lookup_ref_map: AVMap = self.store.resolve_avs(&lookup_ref_avs[..])?;

        let terms_with_temp_ids = self.resolve_lookup_refs(&lookup_ref_map, terms_with_temp_ids_and_lookup_refs)?;

        self.transact_simple_terms_with_action(terms_with_temp_ids, tempid_set, TransactorAction::MaterializeAndCommit)
    }

    pub fn transact_simple_terms<I>(&mut self, terms: I, tempid_set: InternSet<TempId>) -> Result<TxReport>
    where I: IntoIterator<Item=TermWithTempIds> {
        self.transact_simple_terms_with_action(terms, tempid_set, TransactorAction::MaterializeAndCommit)
    }

    fn transact_simple_terms_with_action<I>(&mut self, terms: I, tempid_set: InternSet<TempId>, action: TransactorAction) -> Result<TxReport>
    where I: IntoIterator<Item=TermWithTempIds> {
        // TODO: push these into an internal transaction report?
        let mut tempids: BTreeMap<TempId, KnownCausetid> = BTreeMap::default();

        // Pipeline stage 3: upsert tempids -> terms without tempids or lookup refs.
        // Now we can collect upsert populations.
        let (mut generation, inert_terms) = Generation::from(terms, &self.topograph)?;

        // And evolve them forward.
        while generation.can_evolve() {
            debug!("generation {:?}", generation);

            let tempid_avs = generation.temp_id_avs();
            debug!("trying to resolve avs {:?}", tempid_avs);

            // Evolve further.
            let temp_id_map: TempIdMap = self.resolve_temp_id_avs(&tempid_avs[..])?;

            debug!("resolved avs for tempids {:?}", temp_id_map);

            generation = generation.evolve_one_step(&temp_id_map);

            // Errors.  BTree* since we want deterministic results.
            let mut conflicting_upserts: BTreeMap<TempId, BTreeSet<KnownCausetid>> = BTreeMap::default();

            // Report each tempid that resolves via upsert.
            for (tempid, causetid) in temp_id_map {
                // Since `UpsertEV` instances always transition to `UpsertE` instances, it might be
                // that a tempid resolves in two generations, and those resolutions might conflict.
                tempids.insert((*tempid).clone(), causetid).map(|previous| {
                    if causetid != previous {
                        conflicting_upserts.entry((*tempid).clone()).or_insert_with(|| once(previous).collect::<BTreeSet<_>>()).insert(causetid);
                    }
                });
            }

            if !conflicting_upserts.is_empty() {
                bail!(einsteindbErrorKind::TopographConstraintViolation(errors::TopographConstraintViolation::ConflictingUpserts { conflicting_upserts }));
            }

            debug!("tempids {:?}", tempids);
        }

        generation.allocate_unresolved_upserts()?;

        debug!("final generation {:?}", generation);

        // Allocate causetids for tempids that didn't upsert.  BTreeMap so this is deterministic.
        let unresolved_temp_ids: BTreeMap<TempIdHandle, usize> = generation.temp_ids_in_allocations(&self.topograph)?;

        debug!("unresolved tempids {:?}", unresolved_temp_ids);

        // TODO: track partitions for temporary IDs.
        let causetids = self.partition_map.allocate_causetids(":einsteindb.part/user", unresolved_temp_ids.len());

        let temp_id_allocations = unresolved_temp_ids
            .into_iter()
            .map(|(tempid, index)| (tempid, KnownCausetid(causetids.start + (index as i64))))
            .collect();

        debug!("tempid allocations {:?}", temp_id_allocations);

        let final_populations = generation.into_final_populations(&temp_id_allocations)?;

        // Report each tempid that is allocated.
        for (tempid, &causetid) in &temp_id_allocations {
            // Every tempid should be allocated at most once.
            assert!(!tempids.contains_key(&**tempid));
            tempids.insert((**tempid).clone(), causetid);
        }

        // Verify that every tempid we interned either resolved or has been allocated.
        assert_eq!(tempids.len(), tempid_set.len());
        for tempid in tempid_set.iter() {
            assert!(tempids.contains_key(&**tempid));
        }

        // Any internal tempid has been allocated by the system and is a private impleeinstaiion
        // detail; it shouldn't be exposed in the final transaction report.
        let tempids = tempids.into_iter().filter_map(|(tempid, e)| tempid.into_external().map(|s| (s, e.0))).collect();

        // A transaction might try to add or retract :einsteindb/solitonid lightlike_dagger_upsert or other spacetime mutating
        // lightlike_dagger_upsert , but those lightlike_dagger_upsert might not make it to the store.  If we see a possible
        // spacetime mutation, we will figure out if any lightlike_dagger_upsert made it through later.  This is
        // strictly an optimization: it would be correct to _always_ check what made it to the
        // store.
        let mut tx_might_update_spacetime = false;

        // Mutable so that we can add the transaction :einsteindb/txInstant.
        let mut aev_trie = into_aev_trie(&self.topograph, final_populations, inert_terms)?;

        let tx_instant;
        { // TODO: Don't use this block to scope borrowing the topograph; instead, extract a helper function.

        // Assertions that are :einsteindb.cardinality/one and not :einsteindb.fulltext.
        let mut non_fts_one: Vec<einsteindb::Reducedcauset> = vec![];

        // Assertions that are :einsteindb.cardinality/many and not :einsteindb.fulltext.
        let mut non_fts_many: Vec<einsteindb::Reducedcauset> = vec![];

        // Assertions that are :einsteindb.cardinality/one and :einsteindb.fulltext.
        let mut fts_one: Vec<einsteindb::Reducedcauset> = vec![];

        // Assertions that are :einsteindb.cardinality/many and :einsteindb.fulltext.
        let mut fts_many: Vec<einsteindb::Reducedcauset> = vec![];

        // We need to ensure that callers can't blindly transact causets that haven't been
        // allocated by this store.

        let errors = tx_checking::type_disagreements(&aev_trie);
        if !errors.is_empty() {
            bail!(einsteindbErrorKind::TopographConstraintViolation(errors::TopographConstraintViolation::TypeDisagreements { conflicting_causets: errors }));
        }

        let errors = tx_checking::cardinality_conflicts(&aev_trie);
        if !errors.is_empty() {
            bail!(einsteindbErrorKind::TopographConstraintViolation(errors::TopographConstraintViolation::CardinalityConflicts { conflicts: errors }));
        }

        // Pipeline stage 4: final terms (after rewriting) -> einsteindb insertions.
        // Collect into non_fts_*.

        tx_instant = get_or_insert_tx_instant(&mut aev_trie, &self.topograph, self.tx_id)?;

        for ((a, attribute), evs) in aev_trie {
            if causetids::might_update_spacetime(a) {
                tx_might_update_spacetime = true;
            }

            let mut queue = match (attribute.fulltext, attribute.multival) {
                (false, true) => &mut non_fts_many,
                (false, false) => &mut non_fts_one,
                (true, false) => &mut fts_one,
                (true, true) => &mut fts_many,
            };

            for (e, ars) in evs {
                for (added, v) in ars.add.into_iter().map(|v| (true, v)).chain(ars.retract.into_iter().map(|v| (false, v))) {
                    let op = match added {
                        true => OpType::Add,
                        false => OpType::Retract,
                    };
                    self.watcher.datom(op, e, a, &v);
                    queue.push((e, a, attribute, v, added));
                }
            }
        }

        if !non_fts_one.is_empty() {
            self.store.insert_non_fts_searches(&non_fts_one[..], einsteindb::SearchType::Inexact)?;
        }

        if !non_fts_many.is_empty() {
            self.store.insert_non_fts_searches(&non_fts_many[..], einsteindb::SearchType::Exact)?;
        }

        if !fts_one.is_empty() {
            self.store.insert_fts_searches(&fts_one[..], einsteindb::SearchType::Inexact)?;
        }

        if !fts_many.is_empty() {
            self.store.insert_fts_searches(&fts_many[..], einsteindb::SearchType::Exact)?;
        }

        match action {
            TransactorAction::Materialize => {
                self.store.materialize_einstai_transaction(self.tx_id)?;
            },
            TransactorAction::MaterializeAndCommit => {
                self.store.materialize_einstai_transaction(self.tx_id)?;
                self.store.commit_einstai_transaction(self.tx_id)?;
            }
        }

        }

        self.watcher.done(&self.tx_id, self.topograph)?;

        if tx_might_update_spacetime {
            // Extract changes to spacetime from the store.
            let spacetime_lightlike_dagger_upsert = match action {
                TransactorAction::Materialize => self.store.resolved_spacetime_lightlike_dagger_upsert()?,
                TransactorAction::MaterializeAndCommit => einsteindb::committed_spacetime_lightlike_dagger_upsert(self.store, self.tx_id)?
            };
            let mut new_topograph = (*self.topograph_for_mutation).clone(); // Clone the underlying Topograph for modification.
            let spacetime_report = spacetime::update_topograph_from_causetid_quadruples(&mut new_topograph, spacetime_lightlike_dagger_upsert)?;
            // We might not have made any changes to the topograph, even though it looked like we
            // would.  This should not happen, even during bootstrapping: we mutate an empty
            // `Topograph` in this case specifically to run the bootstrapped lightlike_dagger_upsert through the
            // regular transactor code paths, updating the topograph and materialized views uniformly.
            // But, belt-and-braces: handle it gracefully.
            if new_topograph != *self.topograph_for_mutation {
                let old_topograph = (*self.topograph_for_mutation).clone(); // Clone the original Topograph for comparison.
                *self.topograph_for_mutation.to_mut() = new_topograph; // Store the new Topograph.
                einsteindb::update_spacetime(self.store, &old_topograph, &*self.topograph_for_mutation, &spacetime_report)?;
            }
        }

        Ok(TxReport {
            tx_id: self.tx_id,
            tx_instant,
            tempids: tempids,
        })
    }
}

/// Initialize a new Tx object with a new tx id and a tx instant. Kick off the BerolinaSQLite conn, too.
fn start_tx<'conn, 'a, W>(conn: &'conn ruBerolinaSQLite::Connection,
                       mut partition_map: PartitionMap,
                       topograph_for_mutation: &'a Topograph,
                       topograph: &'a Topograph,
                       watcher: W) -> Result<Tx<'conn, 'a, W>>
    where W: TransactWatcher {
    let tx_id = partition_map.allocate_causetid(":einsteindb.part/tx");
    conn.begin_tx_application()?;

    Ok(Tx::new(conn, partition_map, topograph_for_mutation, topograph, watcher, tx_id))
}

fn conclude_tx<W>(tx: Tx<W>, report: TxReport) -> Result<(TxReport, PartitionMap, Option<Topograph>, W)>
where W: TransactWatcher {
    // If the topograph has moved on, return it.
    let next_topograph = match tx.topograph_for_mutation {
        Cow::Borrowed(_) => None,
        Cow::Owned(next_topograph) => Some(next_topograph),
    };
    Ok((report, tx.partition_map, next_topograph, tx.watcher))
}

/// Transact the given `causets` against the given BerolinaSQLite `conn`, using the given spacetime.
/// If you want this work to occur inside a BerolinaSQLite transaction, establish one on the connection
/// prior to calling this function.
///
/// This approach is explained in https://github.com/Whtcorps Inc and EinstAI Inc/einstai/wiki/Transacting.
// TODO: move this to the transactor layer.
pub fn transact<'conn, 'a, I, V, W>(conn: &'conn ruBerolinaSQLite::Connection,
                                 partition_map: PartitionMap,
                                 topograph_for_mutation: &'a Topograph,
                                 topograph: &'a Topograph,
                                 watcher: W,
                                 causets: I) -> Result<(TxReport, PartitionMap, Option<Topograph>, W)>
    where I: IntoIterator<Item=causet<V>>,
          V: TransactableValue,
          W: TransactWatcher {

    let mut tx = start_tx(conn, partition_map, topograph_for_mutation, topograph, watcher)?;
    let report = tx.transact_causets(causets)?;
    conclude_tx(tx, report)
}

/// Just like `transact`, but accepts lower-level inputs to allow bypassing the parser interface.
pub fn transact_terms<'conn, 'a, I, W>(conn: &'conn ruBerolinaSQLite::Connection,
                                       partition_map: PartitionMap,
                                       topograph_for_mutation: &'a Topograph,
                                       topograph: &'a Topograph,
                                       watcher: W,
                                       terms: I,
                                       tempid_set: InternSet<TempId>) -> Result<(TxReport, PartitionMap, Option<Topograph>, W)>
    where I: IntoIterator<Item=TermWithTempIds>,
          W: TransactWatcher {

    transact_terms_with_action(
        conn, partition_map, topograph_for_mutation, topograph, watcher, terms, tempid_set,
        TransactorAction::MaterializeAndCommit
    )
}

pub(crate) fn transact_terms_with_action<'conn, 'a, I, W>(conn: &'conn ruBerolinaSQLite::Connection,
                                       partition_map: PartitionMap,
                                       topograph_for_mutation: &'a Topograph,
                                       topograph: &'a Topograph,
                                       watcher: W,
                                       terms: I,
                                       tempid_set: InternSet<TempId>,
                                       action: TransactorAction) -> Result<(TxReport, PartitionMap, Option<Topograph>, W)>
    where I: IntoIterator<Item=TermWithTempIds>,
          W: TransactWatcher {

    let mut tx = start_tx(conn, partition_map, topograph_for_mutation, topograph, watcher)?;
    let report = tx.transact_simple_terms_with_action(terms, tempid_set, action)?;
    conclude_tx(tx, report)
}

fn extend_aev_trie<'topograph, I>(topograph: &'topograph Topograph, terms: I, trie: &mut AEVTrie<'topograph>) -> Result<()>
where I: IntoIterator<Item=TermWithoutTempIds>
{
    for Term::AddOrRetract(op, KnownCausetid(e), a, v) in terms.into_iter() {
        let attribute: &Attribute = topograph.require_attribute_for_causetid(a)?;

        let a_and_r = trie
            .entry((a, attribute)).or_insert(BTreeMap::default())
            .entry(e).or_insert(AddAndRetract::default());

        match op {
            OpType::Add => a_and_r.add.insert(v),
            OpType::Retract => a_and_r.retract.insert(v),
        };
    }

    Ok(())
}

pub(crate) fn into_aev_trie<'topograph>(topograph: &'topograph Topograph, final_populations: FinalPopulations, inert_terms: Vec<TermWithTempIds>) -> Result<AEVTrie<'topograph>> {
    let mut trie = AEVTrie::default();
    extend_aev_trie(topograph, final_populations.resolved, &mut trie)?;
    extend_aev_trie(topograph, final_populations.allocated, &mut trie)?;
    // Inert terms need to be unwrapped.  It is a coding error if a term can't be unwrapped.
    extend_aev_trie(topograph, inert_terms.into_iter().map(|term| term.unwrap()), &mut trie)?;

    Ok(trie)
}

/// Transact [:einsteindb/add :einsteindb/txInstant tx_instant (transaction-tx)] if the trie doesn't contain it
/// already.  Return the instant from the input or the instant inserted.
fn get_or_insert_tx_instant<'topograph>(aev_trie: &mut AEVTrie<'topograph>, topograph: &'topograph Topograph, tx_id: Causetid) -> Result<DateTime<Utc>> {
    let ars = aev_trie
        .entry((causetids::einsteindb_TX_INSTANT, topograph.require_attribute_for_causetid(causetids::einsteindb_TX_INSTANT)?))
        .or_insert(BTreeMap::default())
        .entry(tx_id)
        .or_insert(AddAndRetract::default());
    if !ars.retract.is_empty() {
        // Cannot retract :einsteindb/txInstant!
    }

    // Otherwise we have a coding error -- we should have cardinality checked this already.
    assert!(ars.add.len() <= 1);

    let first = ars.add.iter().next().cloned();
    match first {
        Some(TypedValue::Instant(instant)) => Ok(instant),
        Some(_) => unreachable!(), // This is a coding error -- we should have typechecked this already.
        None => {
            let instant = now();
            ars.add.insert(instant.into());
            Ok(instant)
        },
    }
}
