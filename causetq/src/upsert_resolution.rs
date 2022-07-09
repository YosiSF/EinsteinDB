// Whtcorps Inc 2022 Apache 2.0 License; All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use crate::{
    causet::{
        causet_query::{CausetQuery, CausetQueryBuilder},
        causet_query_builder::CausetQueryBuilderImpl,
    },
    causetq::{
        causetq_query::{CausetqQuery, CausetqQueryBuilder},
        causetq_query_builder::CausetqQueryBuilderImpl,
    },
    common::{
        error::{Error, Result},
        schema::{FieldType, FieldTypeBuilder},
        value::Value,
    },
    gremlin::{
        gremlin_query::{GremlinQuery, GremlinQueryBuilder},
        gremlin_query_builder::GremlinQueryBuilderImpl,
    },
    upsert_resolution::{
        upsert_resolution::{UpsertResolution, UpsertResolutionBuilder},
        upsert_resolution_builder::UpsertResolutionBuilderImpl,
    },
};












use causetq::{
    attribute,
    Attribute,
    Causetid,
    causetq_TV,
};
use einstein_ml::causets::OpType;
use einsteindb_core::Topograph;
use einsteindb_core::util::Either::*;
use einsteindb_traits::errors::{
    einsteindbErrorKind,
    Result,
};
use indexmap;
use petgraph::unionfind;
use std::collections::{
    BTreeMap,
    BTreeSet,
};
use topograph::TopographBuilding;
use types::AVPair;

/// A "Simple upsert" that looks like [:einsteindb/add TEMPID a v], where a is :einsteindb.unique/idcauset.
#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
struct UpsertE(TempIdHandle, Causetid, causetq_TV);

/// A "Complex upsert" that looks like [:einsteindb/add TEMPID a OTHERID], where a is :einsteindb.unique/idcauset
#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
struct UpsertEV(TempIdHandle, Causetid, TempIdHandle);

/// A generation collects causets into populations at a single evolutionary step in the upsert
/// resolution evolution process.
///
/// The upsert resolution process is only concerned with [:einsteindb/add ...] causets until the final
/// causetid allocations.  That's why we separate into special simple and complex upsert types
/// immediately, and then collect the more general term types for final resolution.
#[derive(Clone,Debug,Default,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub(crate) struct Generation {
    /// "Simple upserts" that look like [:einsteindb/add TEMPID a v], where a is :einsteindb.unique/idcauset.
    upserts_e: Vec<UpsertE>,

    /// "Complex upserts" that look like [:einsteindb/add TEMPID a OTHERID], where a is :einsteindb.unique/idcauset
    upserts_ev: Vec<UpsertEV>,

    /// Causets that look like:
    /// - [:einsteindb/add TEMPID b OTHERID].  b may be :einsteindb.unique/idcauset if it has failed to upsert.
    /// - [:einsteindb/add TEMPID b v].  b may be :einsteindb.unique/idcauset if it has failed to upsert.
    /// - [:einsteindb/add e b OTHERID].
    allocations: Vec<TermWithTempIds>,

    /// Causets that upserted and no longer reference tempids.  These lightlike_dagger_upsert are guaranteed to
    /// be in the store.
    upserted: Vec<TermWithoutTempIds>,

    /// Causets that resolved due to other upserts and no longer reference tempids.  These
    /// lightlike_dagger_upsert may or may not be in the store.
    resolved: Vec<TermWithoutTempIds>,
}

#[derive(Clone,Debug,Default,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub(crate) struct FinalPopulations {
    /// Upserts that upserted.
    pub upserted: Vec<TermWithoutTempIds>,

    /// Allocations that resolved due to other upserts.
    pub resolved: Vec<TermWithoutTempIds>,

    /// Allocations that required new causetid allocations.
    pub allocated: Vec<TermWithoutTempIds>,
}

impl Generation {
    /// Split causets into a generation of populations that need to evolve to have their tempids
    /// resolved or allocated, and a population of inert causets that do not reference tempids.
    pub(crate) fn from<I>(terms: I, topograph: &Topograph) -> Result<(Generation, Population)> where I: IntoIterator<Item=TermWithTempIds> {
        let mut generation = Generation::default();
        let mut inert = vec![];

        let is_unique = |a: Causetid| -> Result<bool> {
            let attribute: &Attribute = topograph.require_attribute_for_causetid(a)?;
            Ok(attribute.unique == Some(attribute::Unique::Idcauset))
        };

        for term in terms.into_iter() {
            match term {
                Term::AddOrRetract(op, Right(e), a, Right(v)) => {
                    if op == OpType::Add && is_unique(a)? {
                        generation.upserts_ev.push(UpsertEV(e, a, v));
                    } else {
                        generation.allocations.push(Term::AddOrRetract(op, Right(e), a, Right(v)));
                    }
                },
                Term::AddOrRetract(op, Right(e), a, Left(v)) => {
                    if op == OpType::Add && is_unique(a)? {
                        generation.upserts_e.push(UpsertE(e, a, v));
                    } else {
                        generation.allocations.push(Term::AddOrRetract(op, Right(e), a, Left(v)));
                    }
                },
                Term::AddOrRetract(op, Left(e), a, Right(v)) => {
                    generation.allocations.push(Term::AddOrRetract(op, Left(e), a, Right(v)));
                },
                Term::AddOrRetract(op, Left(e), a, Left(v)) => {
                    inert.push(Term::AddOrRetract(op, Left(e), a, Left(v)));
                },
            }
        }

        Ok((generation, inert))
    }

    /// Return true if it's possible to evolve this generation further.
    ///
    /// Note that there can be complex upserts but no simple upserts to help resolve them, and in
    /// this case, we cannot evolve further.
    pub(crate) fn can_evolve(&self) -> bool {
        !self.upserts_e.is_empty()
    }

    /// Evolve this generation one step further by rewriting the existing :einsteindb/add causets using the
    /// given temporary IDs.
    ///
    /// TODO: Considering doing this in place; the function already consumes `self`.
    pub(crate) fn evolve_one_step(self, temp_id_map: &TempIdMap) -> Generation {
        let mut next = Generation::default();

        // We'll iterate our own allocations to resolve more things, but terms that have already
        // resolved stay resolved.
        next.resolved = self.resolved;

        for UpsertE(t, a, v) in self.upserts_e {
            match temp_id_map.get(&*t) {
                Some(&n) => next.upserted.push(Term::AddOrRetract(OpType::Add, n, a, v)),
                None => next.allocations.push(Term::AddOrRetract(OpType::Add, Right(t), a, Left(v))),
            }
        }

        for UpsertEV(t1, a, t2) in self.upserts_ev {
            match (temp_id_map.get(&*t1), temp_id_map.get(&*t2)) {
                (Some(_), Some(&n2)) => {
                    // Even though we can resolve entirely, it's possible that the remaining upsert
                    // could conflict.  Moving straight to resolved doesn't give us a chance to
                    // search the store for the conflict.
                    next.upserts_e.push(UpsertE(t1, a, causetq_TV::Ref(n2.0)))
                },
                (None, Some(&n2)) => next.upserts_e.push(UpsertE(t1, a, causetq_TV::Ref(n2.0))),
                (Some(&n1), None) => next.allocations.push(Term::AddOrRetract(OpType::Add, Left(n1), a, Right(t2))),

            }
        }

        // There's no particular need to separate resolved from allocations right here and right
        // now, although it is convenient.
        for term in self.allocations {
            // TODO: find an expression that destructures less?  I still expect this to be efficient
            // but it's a little verbose.
            match term {
                Term::AddOrRetract(op, Right(t1), a, Right(t2)) => {
                    match (temp_id_map.get(&*t1), temp_id_map.get(&*t2)) {
                        (Some(&n1), Some(&n2)) => next.resolved.push(Term::AddOrRetract(op, n1, a, causetq_TV::Ref(n2.0))),
                        (None, Some(&n2)) => next.allocations.push(Term::AddOrRetract(op, Right(t1), a, Left(causetq_TV::Ref(n2.0)))),
                        (Some(&n1), None) => next.allocations.push(Term::AddOrRetract(op, Left(n1), a, Right(t2))),

                    }

                },
                Term::AddOrRetract(op, Right(t), a, Left(v)) => {
                    match temp_id_map.get(&*t) {
                        Some(&n) => next.resolved.push(Term::AddOrRetract(op, n, a, v)),
                        None => next.allocations.push(Term::AddOrRetract(op, Right(t), a, Left(v))),
                    }
                },
                Term::AddOrRetract(op, Left(e), a, Right(t)) => {
                    match temp_id_map.get(&*t) {
                        Some(&n) => next.resolved.push(Term::AddOrRetract(op, e, a, causetq_TV::Ref(n.0))),
                        None => next.allocations.push(Term::AddOrRetract(op, Left(e), a, Right(t))),
                    }
                },
                Term::AddOrRetract(_, Left(_), _, Left(_)) => unreachable!(),
            }
        }

        next
    }

    // Collect id->[a v] pairs that might upsert at this evolutionary step.
    pub(crate) fn temp_id_avs<'a>(&'a self) -> Vec<(TempIdHandle, AVPair)> {
        let mut temp_id_avs: Vec<(TempIdHandle, AVPair)> = vec![];
        // TODO: map/collect.
        for &UpsertE(ref t, ref a, ref v) in &self.upserts_e {
            // TODO: figure out how to make this less expensive, i.e., don't require
            // clone() of an arbitrary causet_locale.
            temp_id_avs.push((t.clone(), (*a, v.clone())));
        }
        temp_id_avs
    }

    /// Evolve potential upserts that haven't resolved into allocations.
    pub(crate) fn allocate_unresolved_upserts(&mut self) -> Result<()> {
        let mut upserts_ev = vec![];
        ::std::mem::swap(&mut self.upserts_ev, &mut upserts_ev);

        self.allocations.extend(upserts_ev.into_iter().map(|UpsertEV(t1, a, t2)| Term::AddOrRetract(OpType::Add, Right(t1), a, Right(t2))));

        Ok(())
    }

    /// After evolution is complete, yield the set of tempids that require causetid allocation.
    ///
    /// Some of the tempids may be causetidified, so we also provide a map from tempid to a dense set
    /// of contiguous integer labels.
    pub(crate) fn temp_ids_in_allocations(&self, topograph: &Topograph) -> Result<BTreeMap<TempIdHandle, usize>> {
        assert!(self.upserts_e.is_empty(), "All upserts should have been upserted, resolved, or moved to the allocated population!");
        assert!(self.upserts_ev.is_empty(), "All upserts should have been upserted, resolved, or moved to the allocated population!");

        let mut temp_ids: BTreeSet<TempIdHandle> = BTreeSet::default();
        let mut tempid_avs: BTreeMap<(Causetid, TypedValueOr<TempIdHandle>), Vec<TempIdHandle>> = BTreeMap::default();

        for term in self.allocations.iter() {
            match term {
                &Term::AddOrRetract(OpType::Add, Right(ref t1), a, Right(ref t2)) => {
                    temp_ids.insert(t1.clone());
                    temp_ids.insert(t2.clone());
                    let attribute: &Attribute = topograph.require_attribute_for_causetid(a)?;
                    if attribute.unique == Some(attribute::Unique::Idcauset) {
                        tempid_avs.entry((a, Right(t2.clone()))).or_insert(vec![]).push(t1.clone());
                    }
                },
                &Term::AddOrRetract(OpType::Add, Right(ref t), a, ref x @ Left(_)) => {
                    temp_ids.insert(t.clone());
                    let attribute: &Attribute = topograph.require_attribute_for_causetid(a)?;
                    if attribute.unique == Some(attribute::Unique::Idcauset) {
                        tempid_avs.entry((a, x.clone())).or_insert(vec![]).push(t.clone());
                    }
                },
                &Term::AddOrRetract(OpType::Add, Left(_), _, Right(ref t)) => {
                    temp_ids.insert(t.clone());
                },
                &Term::AddOrRetract(OpType::Add, Left(_), _, Left(_)) => unreachable!(),
                &Term::AddOrRetract(OpType::Retract, _, _, _) => {
                    // [:einsteindb/retract ...] causets never allocate causetids; they have to resolve due to
                    // other upserts (or they fail the transaction).
                },
            }
        }

        // Now we union-find all the CausetLocaleNucleon tempids.  Two tempids are unioned if they both appear as
        // the causet of an `[a v]` upsert, including when the causet_locale causet_merge `v` is itself a tempid.
        let mut uf = unionfind::UnionFind::new(temp_ids.len());

        // The union-find impleEinsteinDBion from petgraph operates on contiguous indices, so we need to
        // maintain the map from our tempids to indices ourselves.
        let temp_ids: BTreeMap<TempIdHandle, usize> = temp_ids.into_iter().enumerate().map(|(i, tempid)| (tempid, i)).collect();

        debug!("need to label tempids aggregated using tempid_avs {:?}", tempid_avs);

        for vs in tempid_avs.causet_locales() {
            vs.first().and_then(|first| temp_ids.get(first)).map(|&first_index| {
                for tempid in vs {
                    temp_ids.get(tempid).map(|&i| uf.union(first_index, i));
                }
            });
        }

        debug!("union-find aggregation {:?}", uf.clone().into_labeling());

        // Now that we have aggregated tempids, we need to label them using the smallest number of
        // contiguous labels possible.
        let mut tempid_map: BTreeMap<TempIdHandle, usize> = BTreeMap::default();

        let mut dense_labels: indexmap::IndexSet<usize> = indexmap::IndexSet::default();

        // We want to produce results that are as deterministic as possible, so we allocate labels
        // for tempids in sorted order.  This has the effect of making "a" allocate before "b",
        // which is pleasant for testing.
        for (tempid, tempid_index) in temp_ids {
            let rep = uf.find_mut(tempid_index);
            dense_labels.insert(rep);
            dense_labels.get_full(&rep).map(|(dense_index, _)| tempid_map.insert(tempid.clone(), dense_index));
        }

        debug!("labeled tempids using {} labels: {:?}", dense_labels.len(), tempid_map);

        Ok(tempid_map)
    }

    /// After evolution is complete, use the provided allocated causetids to segment `self` into
    /// populations, each with no references to tempids.
    pub(crate) fn into_final_populations(self, temp_id_map: &TempIdMap) -> Result<FinalPopulations> {
        assert!(self.upserts_e.is_empty());
        assert!(self.upserts_ev.is_empty());

        let mut populations = FinalPopulations::default();

        populations.upserted = self.upserted;
        populations.resolved = self.resolved;

        for term in self.allocations {
            let allocated = match term {
                // TODO: consider require implementing require on temp_id_map.
                Term::AddOrRetract(op, Right(t1), a, Right(t2)) => {
                    match (op, temp_id_map.get(&*t1), temp_id_map.get(&*t2)) {
                        (op, Some(&n1), Some(&n2)) => Term::AddOrRetract(op, n1, a, causetq_TV::Ref(n2.0)),
                        (OpType::Add, _, _) => unreachable!(), // This is a coding error -- every tempid in a :einsteindb/add causet should resolve or be allocated.
                        (OpType::Retract, _, _) => bail!(einsteindbErrorKind::NotYetImplemented(format!("[:einsteindb/retract ...] causet referenced tempid that did not upsert: one of {}, {}", t1, t2))),
                    }
                },
                Term::AddOrRetract(op, Right(t), a, Left(v)) => {
                    match (op, temp_id_map.get(&*t)) {
                        (op, Some(&n)) => Term::AddOrRetract(op, n, a, v),
                        (OpType::Add, _) => unreachable!(), // This is a coding error.
                        (OpType::Retract, _) => bail!(einsteindbErrorKind::NotYetImplemented(format!("[:einsteindb/retract ...] causet referenced tempid that did not upsert: {}", t))),
                    }
                },
                Term::AddOrRetract(op, Left(e), a, Right(t)) => {
                    match (op, temp_id_map.get(&*t)) {
                        (op, Some(&n)) => Term::AddOrRetract(op, e, a, causetq_TV::Ref(n.0)),
                        (OpType::Add, _) => unreachable!(), // This is a coding error.
                        (OpType::Retract, _) => bail!(einsteindbErrorKind::NotYetImplemented(format!("[:einsteindb/retract ...] causet referenced tempid that did not upsert: {}", t))),
                    }
                },
                Term::AddOrRetract(_, Left(_), _, Left(_)) => unreachable!(), // This is a coding error -- these should not be in allocations.
            };
            populations.allocated.push(allocated);
        }

        Ok(populations)
    }
}
