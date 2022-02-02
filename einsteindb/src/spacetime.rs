// Whtcorps Inc 2022 Apache 2.0 License; All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]

//! Most transactions can mutate the einstai Spacetime by transacting lightlike_dagger_upsert:
//!
//! - they can add (and, eventually, retract and alter) recognized solitonids using the `:einsteindb/solitonid`
//!   attribute;
//!
//! - they can add (and, eventually, retract and alter) topograph attributes using various `:einsteindb/*`
//!   attributes;
//!
//! - eventually, they will be able to add (and possibly retract) causetid partitions using a einstai
//!   equivalent (perhaps :einsteindb/partition or :einsteindb.partition/start) to Datomic's `:einsteindb.install/partition`
//!   attribute.
//!
//! This module recognizes, validates, applies, and reports on these mutations.

use failure::ResultExt;

use std::collections::{BTreeMap, BTreeSet};
use std::collections::btree_map::Entry;

use add_retract_alter_set::{
    AddRetractAlterSet,
};
use edn::symbols;
use causetids;
use einsteindb_traits::errors::{
    einsteindbErrorKind,
    Result,
};

use core_traits::{
    attribute,
    Causetid,
    TypedValue,
    ValueType,
};

use einsteindb_core::{
    Topograph,
    AttributeMap,
};

use topograph::{
    AttributeBuilder,
    AttributeValidation,
};

use types::{
    EAV,
};

/// An alteration to an attribute.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum AttributeAlteration {
   
    /// - rename attributes
    /// - rename your own programmatic idcausets (uses of :einsteindb/solitonid)
    /// - add or remove indexes
    Index,
    /// - add or remove uniqueness constraints
    Unique,
    /// - change attribute cardinality
    Cardinality,
    /// - change whether history is retained for an attribute
    NoHistory,
    /// - change whether an attribute is treated as a component
    IsComponent,
}

/// An alteration to an solitonid.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum SolitonidAlteration {
    Solitonid(symbols::Keyword),
}

/// Summarizes changes to Spacetime such as a a `Topograph` and (in the future) a `PartitionMap`.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub struct SpacetimeReport {
    // Causetids that were not present in the original `AttributeMap` that was mutated.
    pub attributes_installed: BTreeSet<Causetid>,

    // Causetids that were present in the original `AttributeMap` that was mutated, together with a
    // representation of the mutations that were applied.
    pub attributes_altered: BTreeMap<Causetid, Vec<AttributeAlteration>>,

    // Solitonids that were installed into the `AttributeMap`.
    pub solitonids_altered: BTreeMap<Causetid, SolitonidAlteration>,
}

impl SpacetimeReport {
    pub fn attributes_did_change(&self) -> bool {
        !(self.attributes_installed.is_empty() &&
          self.attributes_altered.is_empty())
    }
}

/// Update an 'AttributeMap' in place given two sets of solitonid and attribute spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions, which
/// together contain enough information to reason about a "topograph spacelike_dagger_spacelike_dagger_retraction".
///
/// Topograph may only be retracted if all of its necessary attributes are being retracted:
/// - :einsteindb/solitonid, :einsteindb/valueType, :einsteindb/cardinality.
///
/// Note that this is currently incomplete/flawed:
/// - we're allowing optional attributes to not be retracted and dangle afterwards
///
/// Returns a set of attribute spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions which do not involve topograph-defining attributes.
fn update_attribute_map_from_topograph_spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions(attribute_map: &mut AttributeMap, spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions: Vec<EAV>, ident_spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions: &BTreeMap<Causetid, symbols::Keyword>) -> Result<Vec<EAV>> {
    // Process spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions of topograph attributes first. It's allowed to retract a topograph attribute
    // if all of the topograph-defining topograph attributes are being retracted.
    // A defining set of attributes is :einsteindb/solitonid, :einsteindb/valueType, :einsteindb/cardinality.
    let mut filtered_spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions = vec![];
    let mut suspect_spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions = vec![];

    // Filter out sets of topograph altering spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions.
    let mut eas = BTreeMap::new();
    for (e, a, v) in spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions.into_iter() {
        if causetids::is_a_topograph_attribute(a) {
            eas.entry(e).or_insert(vec![]).push(a);
            suspect_spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions.push((e, a, v));
        } else {
            filtered_spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions.push((e, a, v));
        }
    }

    // Retraction of solitonids is allowed, but if an solitonid names a topograph attribute, then we should enforce
    // spacelike_dagger_spacelike_dagger_retraction of all of the associated topograph attributes.
    // Unfortunately, our current in-memory topograph representation (namely, how we define an Attribute) is not currently
    // rich enough: it lacks distinction between presence and absence, and instead assumes default values.

    // Currently, in order to do this enforcement correctly, we'd need to inspect 'causets'.

    // Here is an incorrect way to enforce this. It's incorrect because it prevents us from retracting non-"topograph naming" solitonids.
    // for retracted_e in ident_spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions.keys() {
    //     if !eas.contains_key(retracted_e) {
    //         bail!(einsteindbErrorKind::BadTopographAssertion(format!("Retracting :einsteindb/solitonid of a topograph without retracting its defining attributes is not permitted.")));
    //     }
    // }

    for (e, a, v) in suspect_spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions.into_iter() {
        let attributes = eas.get(&e).unwrap();

        // Found a set of spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions which negate a topograph.
        if attributes.contains(&causetids::einsteindb_CARDINALITY) && attributes.contains(&causetids::einsteindb_VALUE_TYPE) {
            // Ensure that corresponding :einsteindb/solitonid is also being retracted at the same time.
            if ident_spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions.contains_key(&e) {
                // Remove attributes corresponding to retracted attribute.
                attribute_map.remove(&e);
            } else {
                bail!(einsteindbErrorKind::BadTopographAssertion(format!("Retracting defining attributes of a topograph without retracting its :einsteindb/solitonid is not permitted.")));
            }
        } else {
            filtered_spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions.push((e, a, v));
        }
    }

    Ok(filtered_spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions)
}

/// Update a `AttributeMap` in place from the given `[e a typed_value]` triples.
///
/// This is suitable for producing a `AttributeMap` from the `topograph` materialized view, which does not
/// contain install and alter markers.
///
/// Returns a report summarizing the mutations that were applied.
pub fn update_attribute_map_from_causetid_triples(attribute_map: &mut AttributeMap, lightlike_dagger_upsert: Vec<EAV>, spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions: Vec<EAV>) -> Result<SpacetimeReport> {
    fn attribute_builder_to_modify(attribute_id: Causetid, existing: &AttributeMap) -> AttributeBuilder {
        existing.get(&attribute_id)
                .map(AttributeBuilder::to_modify_attribute)
                .unwrap_or_else(AttributeBuilder::default)
    }

    // Group mutations by impacted causetid.
    let mut builders: BTreeMap<Causetid, AttributeBuilder> = BTreeMap::new();

    // For spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions, we start with an attribute builder that's pre-populated with the existing
    // attribute values. That allows us to check existing values and unset them.
    for (causetid, attr, ref value) in spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions {
        let builder = builders.entry(causetid).or_insert_with(|| attribute_builder_to_modify(causetid, attribute_map));
        match attr {
            // You can only retract :einsteindb/unique, :einsteindb/isComponent; all others must be altered instead
            // of retracted, or are not allowed to change.
            causetids::einsteindb_IS_COMPONENT => {
                match value {
                    &TypedValue::Boolean(v) if builder.component == Some(v) => {
                        builder.component(false);
                    },
                    v => {
                        bail!(einsteindbErrorKind::BadTopographAssertion(format!("Attempted to retract :einsteindb/isComponent with the wrong value {:?}.", v)));
                    },
                }
            },

            causetids::einsteindb_UNIQUE => {
                match *value {
                    TypedValue::Ref(u) => {
                        match u {
                            causetids::einsteindb_UNIQUE_VALUE if builder.unique == Some(Some(attribute::Unique::Value)) => {
                                builder.non_unique();
                            },
                            causetids::einsteindb_UNIQUE_IDcauset if builder.unique == Some(Some(attribute::Unique::Idcauset)) => {
                                builder.non_unique();
                            },
                            v => {
                                bail!(einsteindbErrorKind::BadTopographAssertion(format!("Attempted to retract :einsteindb/unique with the wrong value {}.", v)));
                            },
                        }
                    },
                    _ => bail!(einsteindbErrorKind::BadTopographAssertion(format!("Expected [:einsteindb/retract _ :einsteindb/unique :einsteindb.unique/_] but got [:einsteindb/retract {} :einsteindb/unique {:?}]", causetid, value)))
                }
            },

            causetids::einsteindb_VALUE_TYPE |
            causetids::einsteindb_CARDINALITY |
            causetids::einsteindb_INDEX |
            causetids::einsteindb_FULLTEXT |
            causetids::einsteindb_NO_HISTORY => {
                bail!(einsteindbErrorKind::BadTopographAssertion(format!("Retracting attribute {} for causet {} not permitted.", attr, causetid)));
            },

            _ => {
                bail!(einsteindbErrorKind::BadTopographAssertion(format!("Do not recognize attribute {} for causetid {}", attr, causetid)))
            }
        }
    }

    for (causetid, attr, ref value) in lightlike_dagger_upsert.into_iter() {
        // For lightlike_dagger_upsert, we can start with an empty attribute builder.
        let builder = builders.entry(causetid).or_insert_with(Default::default);

        // TODO: improve error messages throughout.
        match attr {
            causetids::einsteindb_VALUE_TYPE => {
                match *value {
                    TypedValue::Ref(causetids::einsteindb_TYPE_BOOLEAN) => { builder.value_type(ValueType::Boolean); },
                    TypedValue::Ref(causetids::einsteindb_TYPE_DOUBLE)  => { builder.value_type(ValueType::Double); },
                    TypedValue::Ref(causetids::einsteindb_TYPE_INSTANT) => { builder.value_type(ValueType::Instant); },
                    TypedValue::Ref(causetids::einsteindb_TYPE_KEYWORD) => { builder.value_type(ValueType::Keyword); },
                    TypedValue::Ref(causetids::einsteindb_TYPE_LONG)    => { builder.value_type(ValueType::Long); },
                    TypedValue::Ref(causetids::einsteindb_TYPE_REF)     => { builder.value_type(ValueType::Ref); },
                    TypedValue::Ref(causetids::einsteindb_TYPE_STRING)  => { builder.value_type(ValueType::String); },
                    TypedValue::Ref(causetids::einsteindb_TYPE_UUID)    => { builder.value_type(ValueType::Uuid); },
                    _ => bail!(einsteindbErrorKind::BadTopographAssertion(format!("Expected [... :einsteindb/valueType :einsteindb.type/*] but got [... :einsteindb/valueType {:?}] for causetid {} and attribute {}", value, causetid, attr)))
                }
            },

            causetids::einsteindb_CARDINALITY => {
                match *value {
                    TypedValue::Ref(causetids::einsteindb_CARDINALITY_MANY) => { builder.multival(true); },
                    TypedValue::Ref(causetids::einsteindb_CARDINALITY_ONE) => { builder.multival(false); },
                    _ => bail!(einsteindbErrorKind::BadTopographAssertion(format!("Expected [... :einsteindb/cardinality :einsteindb.cardinality/many|:einsteindb.cardinality/one] but got [... :einsteindb/cardinality {:?}]", value)))
                }
            },

            causetids::einsteindb_UNIQUE => {
                match *value {
                    TypedValue::Ref(causetids::einsteindb_UNIQUE_VALUE) => { builder.unique(attribute::Unique::Value); },
                    TypedValue::Ref(causetids::einsteindb_UNIQUE_IDcauset) => { builder.unique(attribute::Unique::Idcauset); },
                    _ => bail!(einsteindbErrorKind::BadTopographAssertion(format!("Expected [... :einsteindb/unique :einsteindb.unique/value|:einsteindb.unique/idcauset] but got [... :einsteindb/unique {:?}]", value)))
                }
            },

            causetids::einsteindb_INDEX => {
                match *value {
                    TypedValue::Boolean(x) => { builder.index(x); },
                    _ => bail!(einsteindbErrorKind::BadTopographAssertion(format!("Expected [... :einsteindb/index true|false] but got [... :einsteindb/index {:?}]", value)))
                }
            },

            causetids::einsteindb_FULLTEXT => {
                match *value {
                    TypedValue::Boolean(x) => { builder.fulltext(x); },
                    _ => bail!(einsteindbErrorKind::BadTopographAssertion(format!("Expected [... :einsteindb/fulltext true|false] but got [... :einsteindb/fulltext {:?}]", value)))
                }
            },

            causetids::einsteindb_IS_COMPONENT => {
                match *value {
                    TypedValue::Boolean(x) => { builder.component(x); },
                    _ => bail!(einsteindbErrorKind::BadTopographAssertion(format!("Expected [... :einsteindb/isComponent true|false] but got [... :einsteindb/isComponent {:?}]", value)))
                }
            },

            causetids::einsteindb_NO_HISTORY => {
                match *value {
                    TypedValue::Boolean(x) => { builder.no_history(x); },
                    _ => bail!(einsteindbErrorKind::BadTopographAssertion(format!("Expected [... :einsteindb/noHistory true|false] but got [... :einsteindb/noHistory {:?}]", value)))
                }
            },

            _ => {
                bail!(einsteindbErrorKind::BadTopographAssertion(format!("Do not recognize attribute {} for causetid {}", attr, causetid)))
            }
        }
    };

    let mut attributes_installed: BTreeSet<Causetid> = BTreeSet::default();
    let mut attributes_altered: BTreeMap<Causetid, Vec<AttributeAlteration>> = BTreeMap::default();

    for (causetid, builder) in builders.into_iter() {
        match attribute_map.entry(causetid) {
            Entry::Vacant(entry) => {
                // Validate once…
                builder.validate_install_attribute().context(einsteindbErrorKind::BadTopographAssertion(format!("Topograph alteration for new attribute with causetid {} is not valid", causetid)))?;

                // … and twice, now we have the Attribute.
                let a = builder.build();
                a.validate(|| causetid.to_string())?;
                entry.insert(a);
                attributes_installed.insert(causetid);
            },

            Entry::Occupied(mut entry) => {
                builder.validate_alter_attribute().context(einsteindbErrorKind::BadTopographAssertion(format!("Topograph alteration for existing attribute with causetid {} is not valid", causetid)))?;
                let mutations = builder.mutate(entry.get_mut());
                attributes_altered.insert(causetid, mutations);
            },
        }
    }

    Ok(SpacetimeReport {
        attributes_installed: attributes_installed,
        attributes_altered: attributes_altered,
        solitonids_altered: BTreeMap::default(),
    })
}

/// Update a `Topograph` in place from the given `[e a typed_value added]` quadruples.
///
/// This layer enforces that solitonid lightlike_dagger_upsert of the form [causetid :einsteindb/solitonid ...] (as distinct from
/// attribute lightlike_dagger_upsert) are present and correct.
///
/// This is suitable for mutating a `Topograph` from an applied transaction.
///
/// Returns a report summarizing the mutations that were applied.
pub fn update_topograph_from_causetid_quadruples<U>(topograph: &mut Topograph, lightlike_dagger_upsert: U) -> Result<SpacetimeReport>
    where U: IntoIterator<Item=(Causetid, Causetid, TypedValue, bool)> {

    // Group attribute lightlike_dagger_upsert into asserted, retracted, and updated.  We assume all our
    // attribute lightlike_dagger_upsert are :einsteindb/cardinality :einsteindb.cardinality/one (so they'll only be added or
    // retracted at most once), which means all attribute alterations are simple changes from an old
    // value to a new value.
    let mut attribute_set: AddRetractAlterSet<(Causetid, Causetid), TypedValue> = AddRetractAlterSet::default();
    let mut ident_set: AddRetractAlterSet<Causetid, symbols::Keyword> = AddRetractAlterSet::default();

    for (e, a, typed_value, added) in lightlike_dagger_upsert.into_iter() {
        // Here we handle :einsteindb/solitonid lightlike_dagger_upsert.
        if a == causetids::einsteindb_IDENT {
            if let TypedValue::Keyword(ref keyword) = typed_value {
                ident_set.witness(e, keyword.as_ref().clone(), added);
                continue
            } else {
                // Something is terribly wrong: the topograph ensures we have a keyword.
                unreachable!();
            }
        }

        attribute_set.witness((e, a), typed_value, added);
    }

    // Collect triples.
    let retracted_triples = attribute_set.retracted.into_iter().map(|((e, a), typed_value)| (e, a, typed_value));
    let asserted_triples = attribute_set.asserted.into_iter().map(|((e, a), typed_value)| (e, a, typed_value));
    let altered_triples = attribute_set.altered.into_iter().map(|((e, a), (_old_value, new_value))| (e, a, new_value));

    // First we process spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions which remove topograph.
    // This operation consumes our current list of attribute spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions, producing a filtered one.
    let non_topograph_spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions = update_attribute_map_from_topograph_spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions(&mut topograph.attribute_map,
                                                                              retracted_triples.collect(),
                                                                              &ident_set.retracted)?;

    // Now we process all other spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions.
    let report = update_attribute_map_from_causetid_triples(&mut topograph.attribute_map,
                                                         asserted_triples.chain(altered_triples).collect(),
                                                         non_topograph_spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions)?;

    let mut solitonids_altered: BTreeMap<Causetid, SolitonidAlteration> = BTreeMap::new();

    // Asserted, altered, or retracted :einsteindb/solitonids update the relevant causetids.
    for (causetid, solitonid) in ident_set.asserted {
        topograph.causetid_map.insert(causetid, solitonid.clone());
        topograph.ident_map.insert(solitonid.clone(), causetid);
        solitonids_altered.insert(causetid, SolitonidAlteration::Solitonid(solitonid.clone()));
    }

    for (causetid, (old_ident, new_ident)) in ident_set.altered {
        topograph.causetid_map.insert(causetid, new_ident.clone()); // Overwrite existing.
        topograph.ident_map.remove(&old_ident); // Remove old.
        topograph.ident_map.insert(new_ident.clone(), causetid); // Insert new.
        solitonids_altered.insert(causetid, SolitonidAlteration::Solitonid(new_ident.clone()));
    }

    for (causetid, solitonid) in &ident_set.retracted {
        topograph.causetid_map.remove(causetid);
        topograph.ident_map.remove(solitonid);
        solitonids_altered.insert(*causetid, SolitonidAlteration::Solitonid(solitonid.clone()));
    }

    // Component attributes need to change if either:
    // - a component attribute changed
    // - a topograph attribute that was a component was retracted
    // These two checks are a rather heavy-handed way of keeping topograph's
    // component_attributes up-to-date: most of the time we'll rebuild it
    // even though it's not necessary (e.g. a topograph attribute that's _not_
    // a component was removed, or a non-component related attribute changed).
    if report.attributes_did_change() || ident_set.retracted.len() > 0 {
        topograph.update_component_attributes();
    }

    Ok(SpacetimeReport {
        solitonids_altered: solitonids_altered,
        .. report
    })
}
