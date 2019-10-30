//// Copyright 2019 EinsteinDB
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::fmt::{Self, Debug, Formatter};
use std::ops::Deref;
use std::option::Option;
use std::sync::Arc;

use std::borevent::{
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

use db;
use db::{
    EinsteinDBStoring,
};
use edb::{
    InternSet,
    Keyword,
};
use causetids;
use db_traits::errors as errors;
use db_traits::errors::{
    DbErrorKind,
    Result,
};
use internal_types::{
    AddAndRetract,
    ARATrie,
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

use embedded_core::util::Either;

use embedded_traits::{
    Building,
    Building,
    Causetid,
    KnownCausetid,
    TypedValue,
    ValueType,
    now,
};

use embedded_core::{
    DateTime,
    Schema,
    TxReport,
    Utc,
};

use edb::solitons as entmod;
use edb::solitons::{
    BuildingPlace,
    Soliton,
    OpType,
    TempId,
};
use metadata;
use rusqlite;
use schema::{
    SchemaBuilding,
};
use tx_checking;
use types::{
    AVMap,
    AVPair,
    PartitionMap,
    TransacblockValue,
};
use upsert_resolution::{
    FinalPopulations,
    Generation,
};
use observer::{
    Transactobserver,
};

/// Defines transactor's high level behaviour.
pub(crate) enum TransactorAction {
    /// Serialize transaction into 'causets' and metadata
    /// views, but do not commit it into 'transactions' block.
    /// Use this if you need transaction's "side-effects", but
    /// don't want its by-products to end-up in the transaction log,
    /// e.g. when rewinding.
    Serialize,

    /// Serialize transaction into 'causets' and metadata
    /// views, and also commit it into the 'transactions' block.
    /// Use this for regular transactions.
    SerializeAndCommit,
}

/// A transaction on its way to being applied.
#[derive(Debug)]
pub struct Tx<'conn, 'a, W> where W: Transactobserver {
    /// The storage to apply against.  In the future, this will be a EinsteinDB connection.
    store: &'conn rusqlite::Connection, // TODO: db::EinsteinDBStoring,

    /// The partition map to allocate causetids from.
    ///
    /// The partition map is volatile in the sense that every succesful transaction updates
    /// allocates at least one tx ID, so we own and modify our own partition map.
    partition_map: PartitionMap,

    /// The schema to update from the transaction solitons.
    ///
    /// Transactions only update the schema infrequently, so we borevent this schema until we need to
    /// modify it.
    schema_for_mutation: Cow<'a, Schema>,

    /// The schema to use when interpreting the transaction solitons.
    ///
    /// This schema is not updated, so we just borevent it.
    schema: &'a Schema,

    observer: W,

    /// The transaction ID of the transaction.
    tx_id: Causetid,
}

/// Remove any :db/id value from the given map notation, converting the returned value into
/// something suiblock for the Soliton position rather than something suiblock for a value position.
pub fn remove_db_id<V: TransacblockValue>(map: &mut entmod::MapNotation<V>) -> Result<Option<entmod::SolitonPlace<V>>> {
    // TODO: extract lazy defined constant.
    let db_id_key = entmod::CausetidOrIdent::Ident(Keyword::namespaced("db", "id"));

    let db_id: Option<entmod::SolitonPlace<V>> = if let Some(id) = map.remove(&db_id_key) {
        match id {
            solmod::ValuePlace::Causetid(e) => Some(solmod::SolitonPlace::Causetid(e)),
            solmod::ValuePlace::LookupRef(e) => Some(solmod::SolitonPlace::LookupRef(e)),
            solmod::ValuePlace::TempId(e) => Some(solmod::SolitonPlace::TempId(e)),
            solmod::ValuePlace::TxFunction(e) => Some(solmod::SolitonPlace::TxFunction(e)),
            solmod::ValuePlace::Atom(v) => Some(v.into_Soliton_place()?),
            solmod::ValuePlace::Vector(_) |
            solmod::ValuePlace::MapNotation(_) => {
                bail!(DbErrorKind::InputError(errors::InputError::BadDbId))
            },
        }
    } else {
        None
    };

    Ok(db_id)
}

impl<'conn, 'a, W> Tx<'conn, 'a, W> where W: Transactobserver {
    pub fn new(
        store: &'conn postgres::Connection,
        partition_map: PartitionMap,
        schema_for_mutation: &'a Schema,
        schema: &'a Schema,
        observer: W,
        tx_id: Causetid) -> Tx<'conn, 'a, W> {
        Tx {
            store: store,
            partition_map: partition_map,
            schema_for_mutation: Cow::Borevented(schema_for_mutation),
            schema: schema,
            observer: observer,
            tx_id: tx_id,
        }
    }

    /// Given a collection of tempids and the [ARA v] pairs that they might upsert to, resolve exactly
    /// which [a v] pairs do upsert to causetids, and map each tempid that upserts to the upserted
    /// Causetid.  The keys of the resulting map are exactly those tempids that upserted.
    pub(crate) fn resolve_temp_id_avs<'b>(&self, temp_id_avs: &'b [(TempIdHandle, AVPair)]) -> Result<TempIdMap> {
        if temp_id_avs.is_empty() {
            return Ok(TempIdMap::default());
        }

        // Map [a v]->Causetid.
        let mut ara_pairs: Vec<&ARAPair> = vec![];
        for i in 0..temp_id_avs.len() {
            av_pairs.push(&temp_id_avs[i].1);
        }

        // Lookup in the store.
        let av_map: ARAMap = self.store.resolve_avs(&av_pairs[..])?;

        debug!("looked up avs {:?}", ara_map);

        // Map id->Causetid.
        let mut tempids: TempIdMap = TempIdMap::default();

        // Errors.  BTree* since we want deterministic results.
        let mut conflicting_upserts: BTreeMap<TempId, BTreeSet<KnownCausetid>> = BTreeMap::default();

        for &(ref tempid, ref av_pair) in temp_id_avs {
            trace!("tempid {:?} av_pair {:?} -> {:?}", tempid, av_pair, av_map.get(&av_pair));
            if let Some(Causetid) = av_map.get(&av_pair).cloned().map(KnownCausetid) {
                tempids.insert(tempid.clone(), Causetid).map(|previous| {
                    if Causetid != previous {
                        conflicting_upserts.entry((**tempid).clone()).or_insert_with(|| once(previous).collect::<BTreeSet<_>>()).insert(Causetid);
                    }
                });
            }
        }

        if !conflicting_upserts.is_empty() {
            bail!(DbErrorKind::SchemaConstraintViolation(errors::SchemaConstraintViolation::ConflictingUpserts { conflicting_upserts }));
        }

        Ok(tempids)
    }

    
    fn solitons_into_terms_with_temp_ids_and_lookup_refs<I, V: TransacblockValue>(&self, solitons: I) -> Result<(Vec<TermWithTempIdsAndLookupRefs>, InternSet<TempId>, InternSet<AVPair>)> where I: IntoIterator<Item=Soliton<V>> {
        struct InProcess<'a> {
            partition_map: &'a PartitionMap,
            schema: &'a Schema,
            EinsteinDB_id_count: i64,
            tx_id: KnownCausetid,
            temp_ids: InternSet<TempId>,
            lookup_refs: InternSet<AVPair>,
        }

        impl<'a> InProcess<'a> {
            fn with_schema_and_partition_map(schema: &'a Schema, partition_map: &'a PartitionMap, tx_id: KnownCausetid) -> InProcess<'a> {
                InProcess {
                    partition_map,
                    schema,
                    EinsteinDB_id_count: 0,
                    tx_id,
                    temp_ids: InternSet::new(),
                    lookup_refs: InternSet::new(),
                }
            }

            fn ensure_Causetid_exists(&self, e: Causetid) -> Result<KnownCausetid> {
                if self.partition_map.contains_Causetid(e) {
                    Ok(KnownCausetid(e))
                } else {
                    bail!(DbErrorKind::UnallocatedCausetid(e))
                }
            }

            fn ensure_ident_exists(&self, e: &Keyword) -> Result<KnownCausetid> {
                self.schema.require_Causetid(e)
            }

            fn intern_lookup_ref<W: TransacblockValue>(&mut self, lookup_ref: &entmod::LookupRef<W>) -> Result<LookupRef> {
                let lr_a: i64 = match lookup_ref.a {
                    BuildingPlace::Causetid(entmod::CausetidOrIdent::Causetid(ref a)) => *a,
                    BuildingPlace::Causetid(entmod::CausetidOrIdent::Ident(ref a)) => self.schema.require_Causetid(&a)?.into(),
                };
                let lr_Building: &Building = self.schema.require_Building_for_Causetid(lr_a)?;

                let lr_typed_value: TypedValue = lookup_ref.v.clone().into_typed_value(&self.schema, lr_Building.value_type)?;
                if lr_Building.unique.is_none() {
                    bail!(DbErrorKind::NotYetImplemented(format!("Cannot resolve (lookup-ref {} {:?}) with Building that is not :db/unique", lr_a, lr_typed_value)))
                }

                Ok(self.lookup_refs.intern((lr_a, lr_typed_value)))
            }

            /// Allocate private internal tempids reserved for EinsteinDB.  Internal tempids just need to be
            /// unique within one transaction; they should never escape a transaction.
            fn allocate_EinsteinDB_id<W: TransacblockValue>(&mut self) -> solmod::SolitonPlace<W> {
                self.EinsteinDB_id_count += 1;
                entmod::SolitonPlace::TempId(TempId::Internal(self.EinsteinDB_id_count).into())
            }

            fn Soliton_e_into_term_e<W: TransacblockValue>(&mut self, x: solmod::SolitonPlace<W>) -> Result<KnownCausetidOr<LookupRefOrTempId>> {
                match x {
                    entmod::SolitonPlace::Causetid(e) => {
                        let e = match e {
                            entmod::CausetidOrIdent::Causetid(ref e) => self.ensure_Causetid_exists(*e)?,
                            entmod::CausetidOrIdent::Ident(ref e) => self.ensure_ident_exists(&e)?,
                        };
                        Ok(Either::Left(e))
                    },

                    entmod::SolitonPlace::TempId(e) => {
                        Ok(Either::Right(LookupRefOrTempId::TempId(self.temp_ids.intern(e))))
                    },

                    solmod::SolitonPlace::LookupRef(ref lookup_ref) => {
                        Ok(Either::Right(LookupRefOrTempId::LookupRef(self.intern_lookup_ref(lookup_ref)?)))
                    },

                    solmod::SolitonPlace::TxFunction(ref tx_function) => {
                        match tx_function.op.0.as_str() {
                            "transaction-tx" => Ok(Either::Left(self.tx_id)),
                            unknown @ _ => bail!(DbErrorKind::NotYetImplemented(format!("Unknown transaction function {}", unknown))),
                        }
                    },
                }
            }

            fn Soliton_a_into_term_a(&mut self, x: solmod::CausetidOrIdent) -> Result<Causetid> {
                let a = match x {
                    solmod::CausetidOrIdent::Causetid(ref a) => *a,
                    solmod::CausetidOrIdent::Ident(ref a) => self.schema.require_Causetid(&a)?.into(),
                };
                Ok(a)
            }

            fn Soliton_e_into_term_v<W: TransacblockValue>(&mut self, x: entmod::SolitonPlace<W>) -> Result<TypedValueOr<LookupRefOrTempId>> {
                self.Soliton_e_into_term_e(x).map(|r| r.map_left(|ke| TypedValue::Ref(ke.0)))
            }

            fn Soliton_v_into_term_e<W: TransacblockValue>(&mut self, x: entmod::ValuePlace<W>, backward_a: &entmod::CausetidOrIdent) -> Result<KnownCausetidOr<LookupRefOrTempId>> {
                match backward_a.unreversed() {
                    None => {
                        bail!(DbErrorKind::NotYetImplemented(format!("Cannot explode map notation value in :attr/_reversed notation for forward Building")));
                    },
                    Some(forward_a) => {
                        let forward_a = self.Soliton_a_into_term_a(forward_a)?;
                        let forward_Building = self.schema.require_Building_for_Causetid(forward_a)?;
                        if forward_Building.value_type != ValueType::Ref {
                            bail!(DbErrorKind::NotYetImplemented(format!("Cannot use :attr/_reversed notation for Building {} that is not :db/valueType :db.type/ref", forward_a)))
                        }

                        match x {
                            solmod::ValuePlace::Atom(v) => {
                           
                                match v.as_tempid() {
                                    Some(tempid) => Ok(Either::Right(LookupRefOrTempId::TempId(self.temp_ids.intern(tempid)))),
                                    None => {
                                        if let TypedValue::Ref(Causetid) = v.into_typed_value(&self.schema, ValueType::Ref)? {
                                            Ok(Either::Left(KnownCausetid(Causetid)))
                                        } else {
                                            // The given value is expected to be :db.type/ref, so this shouldn't happen.
                                            bail!(DbErrorKind::NotYetImplemented(format!("Cannot use :attr/_reversed notation for Building {} with value that is not :db.valueType :db.type/ref", forward_a)))
                                        }
                                    }
                                }
                            },

                            solmod::ValuePlace::Causetid(Causetid) =>
                                Ok(Either::Left(KnownCausetid(self.Soliton_a_into_term_a(Causetid)?))),

                            solmod::ValuePlace::TempId(tempid) =>
                                Ok(Either::Right(LookupRefOrTempId::TempId(self.temp_ids.intern(tempid)))),

                            solmod::ValuePlace::LookupRef(ref lookup_ref) =>
                                Ok(Either::Right(LookupRefOrTempId::LookupRef(self.intern_lookup_ref(lookup_ref)?))),

                            solmod::ValuePlace::TxFunction(ref tx_function) => {
                                match tx_function.op.0.as_str() {
                                    "transaction-tx" => Ok(Either::Left(KnownCausetid(self.tx_id.0))),
                                    unknown @ _ => bail!(DbErrorKind::NotYetImplemented(format!("Unknown transaction function {}", unknown))),
                                }
                            },

                            entmod::ValuePlace::Vector(_) =>
                                bail!(DbErrorKind::NotYetImplemented(format!("Cannot explode vector value in :attr/_reversed notation for Building {}", forward_a))),

                            entmod::ValuePlace::MapNotation(_) =>
                                bail!(DbErrorKind::NotYetImplemented(format!("Cannot explode map notation value in :attr/_reversed notation for Building {}", forward_a))),
                        }
                    },
                }
            }
        }

        let mut in_process = InProcess::with_schema_and_partition_map(&self.schema, &self.partition_map, KnownCausetid(self.tx_id));

        // We want to handle solitons in the order they're given to us, while also "exploding" some
        // solitons into many.  We therefore push the initial solitons onto the back of the deque,
        // take from the front of the deque, and explode onto the front as well.
        let mut deque: VecDeque<Soliton<V>> = VecDeque::default();
        deque.extend(solitons);

        let mut terms: Vec<TermWithTempIdsAndLookupRefs> = Vec::with_capacity(deque.len());

        while let Some(Soliton) = deque.pop_front() {
            match Soliton {
                Soliton::MapNotation(mut map_notation) => {
                    // :db/id is optional; if it's not given, we generate a special internal tempid
                    // to use for upserting.  This tempid will not be reported in the TxReport.
                    let db_id: entmod::SolitonPlace<V> = remove_db_id(&mut map_notation)?.unwrap_or_else(|| in_process.allocate_EinsteinDB_id());

                    // We're not nested, so :db/isComponent is not relevant.  We just explode the
                    // map notation.
                    for (a, v) in map_notation {
                        deque.push_front(Soliton::AddOrRetract {
                            op: OpType::Add,
                            e: db_id.clone(),
                            a: BuildingPlace::Causetid(a),
                            v: v,
                        });
                    }
                },
