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

//! Most transactions can mutate the EinsteinDB Spacetime by transacting lightlike_upsert:
//!  - `lightlike_upsert`: inserts or updates a value in the spacetime or cone of spacetime.
//! - `lightlike_delete`: deletes a value in the spacetime or cone of spacetime.
//!
//! - they can add (and, eventually, retract and alter) recognized solitonids using the `:einsteindb/solitonid`
//!   attribute;
//!
//! - they can add (and, eventually, retract and alter) topograph attributes using various `:einsteindb/*`
//!   attributes;
//!
//! - eventually, they will be able to add (and possibly retract) causetid partitions using a EinsteinDB
//!   equivalent (perhaps :einsteindb/partition or :einsteindb.partition/start) to Datomic's `:einsteindb.install/partition`
//!   attribute.
//! - eventually, they will be able to add (and possibly retract) causetid partitions using EinsteinDB equivalent
//!
//! This module recognizes, validates, applies, and reports on these mutations.


use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::collections::hash_map::Iter;
use std::collections::hash_map::Keys;
use std::collections::hash_map::Values;
use std::collections::hash_map::ValuesMut;
use std::collections::hash_map::IterMut;


use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::Error;


use std::rc::Rc;
use std::cell::RefCell;
use std::cell::RefMut;
use std::cell::Ref;
use std::cell::RefMut;


use std::collections::BTreeMap;
use std::collections::btree_map::Entry as BTreeMapEntry;
use std::collections::btree_map::Iter as BTreeMapIter;
use std::collections::btree_map::Keys as BTreeMapKeys;
use std::collections::btree_map::Values as BTreeMapValues;


use std::collections::BTreeSet;
use std::collections::btree_set::Iter as BTreeSetIter;
use std::collections::btree_set::IterMut as BTreeSetIterMut;
use std::collections::btree_set::IntoIter as BTreeSetIntoIter;
use std::collections::btree_set::Bounds;
use std::collections::btree_set::;
use std::collections::btree_set::Mut;
use std::collections::btree_set::Full;
use std::collections::btree_set::Inclusive;


use std::collections::HashSet;
use std::collections::hash_set::Iter as HashSetIter;
use std::collections::hash_set::IterMut as HashSetIterMut;
use std::collections::hash_set::IntoIter as HashSetIntoIter;
use std::collections::hash_set::Bounds;



///     #### `Spacetime`
///    The `Spacetime` is a collection of `Solitonid`s and `Topograph`s.
///   It is a `HashMap` of `Solitonid`s to `Topograph`s.
///  It is a `BTreeMap` of `Solitonid`s to `Topograph`s.
/// It is a `HashSet` of `Solitonid`s.
/// It is a `BTreeSet` of `Solitonid`s.


///    #### `Solitonid`
///  A `Solitonid` is a `String` that is a valid `Solitonid` in the `EinsteinDB`
/// (see [`Solitonid`](https://www.einsteindb.com/index.html#Solitonid)).



/// An alteration to an attribute.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum AttributeAlteration {
    /// Addition of a solitonid.
    ///
    /// The `Solitonid` is added to the `Topograph`.
    ///
    ///
    /// #### Example
    /// ```
    /// use einsteindb_spacetime::AttributeAlteration;
    /// use einsteindb_spacetime::Solitonid;
   
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
    Solitonid(shellings::Keyword),
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
/// - :einsteindb/solitonid, :einsteindb/causet_localeType, :einsteindb/cardinality.
///
/// Note that this is currently incomplete/flawed:
/// - we're allowing optional attributes to not be retracted and dangle afterwards
///
/// Returns a set of attribute spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions which do not involve topograph-defining attributes.
fn update_attribute_map_from_topograph_spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions(attribute_map: &mut AttributeMap, spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions: Vec<EAV>, causetid_spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions: &BTreeMap<Causetid, shellings::Keyword>) -> Result<Vec<EAV>> {
    // Process spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions of topograph attributes first. It's allowed to retract a topograph attribute
    // if all of the topograph-defining topograph attributes are being retracted.
    // A defining set of attributes is :einsteindb/solitonid, :einsteindb/causet_localeType, :einsteindb/cardinality.
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
    // rich enough: it lacks distinction between presence and absence, and instead assumes default causet_locales.

    // Currently, in order to do this enforcement correctly, we'd need to inspect 'causets'.

    // Here is an incorrect way to enforce this. It's incorrect because it prevents us from retracting non-"topograph naming" solitonids.
    // for retracted_e in causetid_spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions.soliton_ids() {
    //     if !eas.contains_soliton_id(retracted_e) {
    //         bail!(einsteindbErrorKind::BadTopographAssertion(format!("Retracting :einsteindb/solitonid of a topograph without retracting its defining attributes is not permitted.")));
    //     }
    // }

    for (e, a, v) in suspect_spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions.into_iter() {
        let attributes = eas.get(&e).unwrap();

        // Found a set of spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions which negate a topograph.
        if attributes.contains(&causetids::einsteindb_CARDINALITY) && attributes.contains(&causetids::einsteindb_VALUE_TYPE) {
            // Ensure that corresponding :einsteindb/solitonid is also being retracted at the same time.
            if causetid_spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions.contains_soliton_id(&e) {
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

/// Update a `AttributeMap` in place from the given `[e a typed_causet_locale]` triples.
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
    // attribute causet_locales. That allows us to check existing causet_locales and unset them.
    for (causetid, attr, ref causet_locale) in spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions {
        let builder = builders.entry(causetid).or_insert_with(|| attribute_builder_to_modify(causetid, attribute_map));
        match attr {
            // You can only retract :einsteindb/unique, :einsteindb/isComponent; all others must be altered instead
            // of retracted, or are not allowed to change.
            causetids::einsteindb_IS_COMPONENT => {
                match causet_locale {
                    &causetq_TV::Boolean(v) if builder.component == Some(v) => {
                        builder.component(false);
                    },
                    v => {
                        bail!(einsteindbErrorKind::BadTopographAssertion(format!("Attempted to retract :einsteindb/isComponent with the wrong causet_locale {:?}.", v)));
                    },
                }
            },

            causetids::einsteindb_UNIQUE => {
                match *causet_locale {
                    causetq_TV::Ref(u) => {
                        match u {
                            causetids::einsteindb_UNIQUE_VALUE if builder.unique == Some(Some(attribute::Unique::Value)) => {
                                builder.non_unique();
                            },
                            causetids::einsteindb_UNIQUE_IDcauset if builder.unique == Some(Some(attribute::Unique::Idcauset)) => {
                                builder.non_unique();
                            },
                            v => {
                                bail!(einsteindbErrorKind::BadTopographAssertion(format!("Attempted to retract :einsteindb/unique with the wrong causet_locale {}.", v)));
                            },
                        }
                    },
                    _ => bail!(einsteindbErrorKind::BadTopographAssertion(format!("Expected [:einsteindb/retract _ :einsteindb/unique :einsteindb.unique/_] but got [:einsteindb/retract {} :einsteindb/unique {:?}]", causetid, causet_locale)))
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

    for (causetid, attr, ref causet_locale) in lightlike_dagger_upsert.into_iter() {
        // For lightlike_dagger_upsert, we can start with an empty attribute builder.
        let builder = builders.entry(causetid).or_insert_with(Default::default);

        // TODO: improve error messages throughout.
        match attr {
            causetids::einsteindb_VALUE_TYPE => {
                match *causet_locale {
                    causetq_TV::Ref(causetids::einsteindb_TYPE_BOOLEAN) => { builder.causet_locale_type(ValueType::Boolean); },
                    causetq_TV::Ref(causetids::einsteindb_TYPE_DOUBLE)  => { builder.causet_locale_type(ValueType::Double); },
                    causetq_TV::Ref(causetids::einsteindb_TYPE_INSTANT) => { builder.causet_locale_type(ValueType::Instant); },
                    causetq_TV::Ref(causetids::einsteindb_TYPE_KEYWORD) => { builder.causet_locale_type(ValueType::Keyword); },
                    causetq_TV::Ref(causetids::einsteindb_TYPE_LONG)    => { builder.causet_locale_type(ValueType::Long); },
                    causetq_TV::Ref(causetids::einsteindb_TYPE_REF)     => { builder.causet_locale_type(ValueType::Ref); },
                    causetq_TV::Ref(causetids::einsteindb_TYPE_STRING)  => { builder.causet_locale_type(ValueType::String); },
                    causetq_TV::Ref(causetids::einsteindb_TYPE_UUID)    => { builder.causet_locale_type(ValueType::Uuid); },
                    _ => bail!(einsteindbErrorKind::BadTopographAssertion(format!("Expected [... :einsteindb/causet_localeType :einsteindb.type/*] but got [... :einsteindb/causet_localeType {:?}] for causetid {} and attribute {}", causet_locale, causetid, attr)))
                }
            },

            causetids::einsteindb_CARDINALITY => {
                match *causet_locale {
                    causetq_TV::Ref(causetids::einsteindb_CARDINALITY_MANY) => { builder.multival(true); },
                    causetq_TV::Ref(causetids::einsteindb_CARDINALITY_ONE) => { builder.multival(false); },
                    _ => bail!(einsteindbErrorKind::BadTopographAssertion(format!("Expected [... :einsteindb/cardinality :einsteindb.cardinality/many|:einsteindb.cardinality/one] but got [... :einsteindb/cardinality {:?}]", causet_locale)))
                }
            },

            causetids::einsteindb_INDEX => {
                match *causet_locale {
                    causetq_TV::Ref(causetids::einsteindb_INDEX_FULLTEXT) => { builder.index(true); },
                    causetq_TV::Ref(causetids::einsteindb_INDEX_NONE) => { builder.index(false); },
                    _ => bail!(einsteindbErrorKind::BadTopographAssertion(format!("Expected [... :einsteindb/Index :einsteindb.Index/fulltext|:einsteindb.Index/none] but got [... :einsteindb/Index {:?}]", causet_locale)))
                }
            },

            causetids::einsteindb_UNIQUE => {
                match *causet_locale {
                    causetq_TV::Ref(causetids::einsteindb_UNIQUE_VALUE) => { builder.unique(attribute::Unique::Value); },
                    causetq_TV::Ref(causetids::einsteindb_UNIQUE_IDcauset) => { builder.unique(attribute::Unique::Idcauset); },
                    _ => bail!(einsteindbErrorKind::BadTopographAssertion(format!("Expected [... :einsteindb/unique :einsteindb.unique/causet_locale|:einsteindb.unique/idcauset] but got [... :einsteindb/unique {:?}]", causet_locale)))
                }
            },

            causetids::einsteindb_INDEX => {
                match *causet_locale {
                    causetq_TV::Boolean(x) => { builder.index(x); },
                    _ => bail!(einsteindbErrorKind::BadTopographAssertion(format!("Expected [... :einsteindb/Index true|false] but got [... :einsteindb/Index {:?}]", causet_locale)))
                }
            },

            causetids::einsteindb_FULLTEXT => {
                match *causet_locale {
                    causetq_TV::Boolean(x) => { builder.fulltext(x); },
                    _ => bail!(einsteindbErrorKind::BadTopographAssertion(format!("Expected [... :einsteindb/fulltext true|false] but got [... :einsteindb/fulltext {:?}]", causet_locale)))
                }
            },

            _ => bail!(einsteindbErrorKind::BadTopographAssertion(format!("Expected [... :einsteindb/causet_localeType :einsteindb.type/*] but got [... :einsteindb/causet_localeType {:?}] for causetid {} and attribute {}", causet_locale, causetid, attr))),

            causetids::einsteindb_IS_COMPONENT => {
                match *causet_locale {
                    causetq_TV::Boolean(x) => { builder.component(x); },
                    _ => bail!(einsteindbErrorKind::BadTopographAssertion(format!("Expected [... :einsteindb/isComponent true|false] but got [... :einsteindb/isComponent {:?}]", causet_locale)))
                }
            },

            causetids::einsteindb_IS_MULTIVAL => {
                match *causet_locale {
                    causetq_TV::Boolean(x) => { builder.multival(x); },
                    _ => bail!(einsteindbErrorKind::BadTopographAssertion(format!("Expected [... :einsteindb/isMultival true|false] but got [... :einsteindb/isMultival {:?}]", causet_locale)))
                }
            },

            causetids::einsteindb_NO_HISTORY => {
                match *causet_locale {
                    causetq_TV::Boolean(x) => { builder.no_history(x); },
                    _ => bail!(einsteindbErrorKind::BadTopographAssertion(format!("Expected [... :einsteindb/noHistory true|false] but got [... :einsteindb/noHistory {:?}]", causet_locale)))
                }
            },

            causetids::einsteindb_NO_INDEX => {
                match *causet_locale {
                    causetq_TV::Boolean(x) => { builder.no_index(x); },
                    _ => bail!(einsteindbErrorKind::BadTopographAssertion(format!("Expected [... :einsteindb/noIndex true|false] but got [... :einsteindb/noIndex {:?}]", causet_locale)))
                }
            },

            causetids::einsteindb_NO_INDEX => {
                match *causet_locale {
                    causetq_TV::Boolean(x) => { builder.no_index(x); },
                    _ => bail!(einsteindbErrorKind::BadTopographAssertion(format!("Expected [... :einsteindb/noIndex true|false] but got [... :einsteindb/noIndex {:?}]", causet_locale)))
                }
            },

            causetids::einsteindb_NO_FULLTEXT => {
                match *causet_locale {
                    causetq_TV::Boolean(x) => { builder.no_fulltext(x); },
                    _ => bail!(einsteindbErrorKind::BadTopographAssertion(format!("Expected [... :einsteindb/noFulltext true|false] but got [... :einsteindb/noFulltext {:?}]", causet_locale)))
                }
            },

            causetids::einsteindb_NO_COMPONENT => {
                match *causet_locale {
                    causetq_TV::Boolean(x) => { builder.no_component(x); },
                    _ => bail!(einsteindbErrorKind::BadTopographAssertion(format!("Expected [... :einsteindb/noComponent true|false] but got [... :einsteindb/noComponent {:?}]", causet_locale)))
                }
            },

            causetids::einsteindb_NO_MULTIVAL => {
                match *causet_locale {
                    causetq_TV::Boolean(x) => { builder.no_multival(x); },
                    _ => bail!(einsteindbErrorKind::BadTopographAssertion(format!("Expected [... :einsteindb/noMultival true|false] but got [... :einsteindb/noMultival {:?}]", causet_locale)))
                }
            },

            causetids::einsteindb_NO_REFERENCE => {
                match *causet_locale {
                    causetq_TV::Boolean(x) => { builder.no_reference(x); },
                    _ => bail!(einsteindbErrorKind::BadTopographAssertion(format!("Expected [... :einsteindb/noReference true|false] but got [... :einsteindb/noReference {:?}]", causet_locale)))
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
        attributes_installed,
        attributes_altered,
        solitonids_altered: BTreeMap::default(),
    })
}

/// Update a `Topograph` in place from the given `[e a typed_causet_locale added]` quadruples.
///
/// This layer enforces that solitonid lightlike_dagger_upsert of the form [causetid :einsteindb/solitonid ...] (as distinct from
/// attribute lightlike_dagger_upsert) are present and correct.
///
/// This is suitable for mutating a `Topograph` from an applied transaction.
///
/// Returns a report summarizing the mutations that were applied.
pub fn update_topograph_from_causetid_quadruples<U>(topograph: &mut Topograph, lightlike_dagger_upsert: U) -> Result<SpacetimeReport>
    where U: IntoIterator<Item=(Causetid, Causetid, causetq_TV, bool)> {

    // Group attribute lightlike_dagger_upsert into asserted, retracted, and updated.  We assume all our
    // attribute lightlike_dagger_upsert are :einsteindb/cardinality :einsteindb.cardinality/one (so they'll only be added or
    // retracted at most once), which means all attribute alterations are simple changes from an old
    // causet_locale to a new causet_locale.
    let mut attribute_set: AddRetractAlterSet<(Causetid, Causetid), causetq_TV> = AddRetractAlterSet::default();
    let mut causetid_set: AddRetractAlterSet<Causetid, shellings::Keyword> = AddRetractAlterSet::default();

    for (e, a, typed_causet_locale, added) in lightlike_dagger_upsert.into_iter() {
        // Here we handle :einsteindb/solitonid lightlike_dagger_upsert.
        if a == causetids::einsteindb_IDENT {
            if let causetq_TV::Keyword(ref solitonid_word) = typed_causet_locale {
                causetid_set.witness(e, solitonid_word.as_ref().clone(), added);
                continue
            } else {
                // Something is terribly wrong: the topograph ensures we have a soliton_idword.
                unreachable!();
            }
        }

        attribute_set.witness((e, a), typed_causet_locale, added);
    }

    // Collect triples.
    let mut triples: Vec<(Causetid, Causetid, causetq_TV)> = Vec::new();
    for (e, a, typed_causet_locale) in attribute_set.retracted.into_iter() {
        triples.push((e, a, causetq_TV::Keyword(causetids::einsteindb_RETRACTED)));
    }
    for (e, a, typed_causet_locale) in attribute_set.added.into_iter() {
        triples.push((e, a, typed_causet_locale));
    }
    for (e, a, typed_causet_locale) in attribute_set.altered.into_iter() {
        triples.push((e, a, typed_causet_locale));
    }

    // First we process spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions which remove topograph.
    // This operation consumes our current list of attribute spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions, producing a filtered one.
    let non_topograph_spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions = update_attribute_map_from_topograph_spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions(&mut topograph.attribute_map,
                                                                              retracted_triples.collect(),
                                                                              &causetid_set.retracted)?;

    // Now we process all other spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions.
    let report = update_attribute_map_from_causetid_triples(&mut topograph.attribute_map,
                                                         asserted_triples.chain(altered_triples).collect(),
                                                         non_topograph_spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions)?;

    let mut solitonids_altered: BTreeMap<Causetid, SolitonidAlteration> = BTreeMap::new();

    // Asserted, altered, or retracted :einsteindb/solitonids update the relevant causetids.
    for (causetid, solitonid) in causetid_set.asserted {
        topograph.causetid_map.insert(causetid, solitonid.clone());
        topograph.causetid_map.insert(solitonid.clone(), causetid);
        solitonids_altered.insert(causetid, SolitonidAlteration::Solitonid(solitonid.clone()));
    }

    for (causetid, (old_causetid, new_causetid)) in causetid_set.altered {
        topograph.causetid_map.insert(causetid, new_causetid.clone()); // Overwrite existing.
        topograph.causetid_map.remove(&old_causetid); // Remove old.
        topograph.causetid_map.insert(new_causetid.clone(), causetid); // Insert new.
        solitonids_altered.insert(causetid, SolitonidAlteration::Solitonid(new_causetid.clone()));
    }

    for (causetid, solitonid) in &causetid_set.retracted {
        topograph.causetid_map.remove(causetid);
        topograph.causetid_map.remove(solitonid);
        solitonids_altered.insert(*causetid, SolitonidAlteration::Solitonid(solitonid.clone()));
    }

    // Component attributes need to change if either:
    // - a component attribute changed
    // - a topograph attribute that was a component was retracted
    // These two checks are a rather heavy-handed way of keeping topograph's
    // component_attributes up-to-date: most of the time we'll rebuild it
    // even though it's not necessary (e.g. a topograph attribute that's _not_
    // a component was removed, or a non-component related attribute changed).
    if report.attributes_did_change() || causetid_set.retracted.len() > 0 {
        topograph.update_component_attributes();
    }

    Ok(SpacetimeReport {
        solitonids_altered,
        .. report
    })
}
