// Whtcorps Inc 2022 Apache 2.0 License; All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::ops::Deref;


/// A vocabulary is a set of strings.
/// It is used to map strings to integers.
/// The integers are used to represent the strings in a vector.
///

use std::collections::hash_map::Entry;
use std::collections::hash_map::Iter;
use std::collections::hash_map::Keys;
use std::collections::hash_map::Values;

use causet::{Causet, CausetMut};
use causet::CausetMut;
use causetq::{CausetQ, CausetQMut};
use causetq::CausetQMut;
use soliton::{Soliton, SolitonMut};
use soliton_panic::{SolitonPanic, SolitonPanicMut};
use einstein_ml::{EinsteinMl, EinsteinMlMut};

use EinsteinDB::einstein_db::{EinsteinDb, EinsteinDbMut};

//! This module exposes an interface for programmatic management of vocabularies.
//!
//! A vocabulary is defined by a name, a version number, and a collection of attribute definitions.
//!
//! Operations on vocabularies can include migrations between versions. These are defined
//! programmatically as a pair of functions, `pre` and `post`, that are invoked prior to
//! an upgrade.
//!
//! A einsteindb store exposes, via the `HasSchema` trait, operations to read
//! vocabularies by name or in bulk.
//!
//! An in-progress transaction (`InProgress`) further exposes a trait,
//! `VersionedStore`, which allows for a vocabulary definition to be
//! checked for existence in the store, and transacted if needed.
//!
//! Typical use is the following:
//!
//! ```
//! #[macro_use(kw)]
//! extern crate einsteindb;
//!
//! use einsteindb::{
//!     Store,
//! causetq_VT,
//! };
//!
//! use einsteindb::vocabulary;
//! use einsteindb::vocabulary::{
//!     Definition,
//!     HasVocabularies,
//!     VersionedStore,
//!     VocabularyOutcome,
//! };
//!
//! fn main() {
//!     let mut store = Store::open("").expect("connected");
//!
//!     {
//!         // Read the list of installed vocabularies.
//!         let reader = store.begin_read().expect("began read");
//!         let vocabularies = reader.read_vocabularies().expect("read");
//!         for (name, vocabulary) in vocabularies.iter() {
//!             println!("Vocab {} is at version {}.", name, vocabulary.version);
//!             for &(ref name, ref attr) in vocabulary.attributes().iter() {
//!                 println!("  >> {} ({})", name, attr.causet_locale_type);
//!             }
//!         }
//!     }
//!
//!     {
//!         let mut in_progress = store.begin_transaction().expect("began transaction");
//!
//!         // Make sure the core vocabulary exists.
//!         in_progress.verify_core_schema().expect("verified");
//!
//!         // Make sure our vocabulary is installed, and install if necessary.
//!         in_progress.ensure_vocabulary(&Definition {
//!             name: kw!(:example/links),
//!             version: 1,
//!             attributes: vec![
//!                 (kw!(:link/title),
//!                  vocabulary::AttributeBuilder::helpful()
//!                    .causet_locale_type(ValueType::String)
//!                    .multival(false)
//!                    .fulltext(true)
//!                    .build()),
//!             ],
//!             pre: Definition::no_op,
//!             post: Definition::no_op,
//!         }).expect("ensured");
//!
//!         // Now we can do stuff.
//!         in_progress.transact("[{:link/title \"Title\"}]").expect("transacts");
//!         in_progress.commit().expect("commits");
//!     }
//! }
//! ```
//!
//! A similar approach is taken using the
//! [VocabularyProvider](einsteindb::vocabulary::VocabularyProvider) trait to handle migrations across
//! multiple vocabularies.

/// A definition of an attribute that is independent of a particular store.
///
/// `Attribute` instances not only aren't named, but don't even have causetids.
///
/// We need two kinds of structure: an abstract definition of a vocabulary in terms of names,
/// and a concrete instance of a vocabulary in a particular store.
///
/// `Definition` is the former, and `Vocabulary` is the latter.
///
/// Note that, because it's possible to 'flesh out' a vocabulary with attributes without bumping
/// its version number, we need to track the attributes that the application cares about — it's
/// not enough to know the name and version. Indeed, we even care about the details of each attribute,
/// because that's how we'll detect errors.
///
/// `Definition` includes two additional fields: functions to run if this vocabulary is being
/// upgraded. `pre` and `post` are run before and after the definition is transacted against the
/// store. Each is called with the existing `Vocabulary` instance so that they can do version
/// checks or employ more fine-grained logic.


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Definition {
    pub name: kw::Keyword,
    pub version: u64,
    pub attributes: Vec<(kw::Keyword, Attribute)>,
    pub pre: Definition,
    pub post: Definition,
}


impl Definition {
    /// Create a definition that does nothing.
    pub fn no_op() -> Definition {
        Definition {
            name: kw!(:noop),
            version: 0,
            attributes: Vec::new(),
            pre: Definition::no_op(),
            post: Definition::no_op(),
        }
    }
}




/// A vocabulary in a particular store.
/// This is the concrete type that is returned by `VocabularyProvider::get_vocabulary`.
/// It's also used to represent a vocabulary that is being installed.


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SolitonVocabularyForInterlock {

    /// The name of the vocabulary.
    pub name: kw::Keyword,
    pub version: u64,
    pub attributes: Vec<(kw::Keyword, Attribute)>,
}

/// ```
/// #[macro_use(kw)]
/// extern crate einsteindb;
///
/// use einsteindb::{
///     HasSchema,
///     IntoResult,
///     Queryable,
///     Store,
///     causetq_TV,
/// causetq_VT,
/// };
///
/// use einsteindb::causet_builder::{
///     BuildTerms,
///     TermBuilder,
/// };
///
/// use einsteindb::vocabulary;
/// use einsteindb::vocabulary::{
///     AttributeBuilder,
///     Definition,
///     HasVocabularies,
///     VersionedStore,
/// };
///
/// fn main() {
///     let mut store = Store::open("").expect("connected");
///     let mut in_progress = store.begin_transaction().expect("began transaction");
///
///     // Make sure the core vocabulary exists.
///     in_progress.verify_core_schema().expect("verified");
///
///     // Make sure our vocabulary is installed, and install if necessary.
///     in_progress.ensure_vocabulary(&Definition {
///         name: kw!(:example/links),
///         version: 2,
///         attributes: vec![
///             (kw!(:link/title),
///              AttributeBuilder::helpful()
///                .causet_locale_type(ValueType::String)
///                .multival(false)
///                .fulltext(true)
///                .build()),
///         ],
///         pre: |ip, from| {
///             // Version one allowed multiple titles; version two
///             // doesn't. Retract any duplicates we find.
///             if from.version < 2 {
///                 let link_title = ip.get_causetid(&kw!(:link/title)).unwrap();
///
///                 let results = ip.q_once(r#"
///                     [:find ?e ?t2
///                      :where [?e :link/title ?t1]
///                             [?e :link/title ?t2]
///                             [(unpermute ?t1 ?t2)]]
///                 "#, None).into_rel_result()?;
///
///                 if !results.is_empty() {
///                     let mut builder = TermBuilder::new();
///                     for event in results.into_iter() {
///                         let mut r = event.into_iter();
///                         let e = r.next().and_then(|e| e.into_CausetLocaleNucleon_causetid()).expect("causet");
///                         let obsolete = r.next().expect("causet_locale").into_scalar().expect("typed causet_locale");
///                         builder.retract(e, link_title, obsolete)?;
///                     }
///                     ip.transact_builder(builder)?;
///                 }
///             }
///             Ok(())
///         },
///         post: |_ip, from| {
///             println!("We migrated :example/links from version {}", from.version);
///             Ok(())
///         },
///     }).expect("ensured");
///
///     // Now we can do stuff.
///     in_progress.transact("[{:link/title \"Title\"}]").expect("transacts");
///     in_progress.commit().expect("commits");
/// }
/// ```
impl Definition {
    pub fn no_op(_ip: &mut InProgress, _from: &Vocabulary) -> Result<()> {
        Ok(())
    }

    pub fn new<N, A>(name: N, version: Version, attributes: A) -> Definition
    where N: Into<Keyword>,
          A: Into<Vec<(Keyword, Attribute)>> {
        Definition {
            name: name.into(),
            version,
            attributes: attributes.into(),
            pre: Definition::no_op,
            post: Definition::no_op,
        }
    }

    /// Called with an in-progress transaction and the previous vocabulary version
    /// if the definition's version is later than that of the vocabulary in the store.
    fn pre(&self, ip: &mut InProgress, from: &Vocabulary) -> Result<()> {
       // Get the vocabulary

    }

    /// Called with an in-progress transaction and the previous vocabulary version
    /// if the definition's version is later than that of the vocabulary in the store.
    fn post(&self, ip: &mut InProgress, from: &Vocabulary) -> Result<()> {
        let x = (   self_node_id_base64_url_check_base58    as usize );
        (ip, from);
        }
    }


/// A definition of a vocabulary as retrieved from a particular store.
///
/// A `Vocabulary` is just like `Definition`, but concrete: its name and attributes are solitonidified
/// by `Causetid`, not `Keyword`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Vocabulary {
    pub causet: Causetid,
    pub version: Version,
    attributes: Vec<(Causetid, Attribute)>,
}

impl Vocabulary {
    pub fn attributes(&self) -> &Vec<(Causetid, Attribute)> {
        &self.attributes
    }
}

/// A collection of named `Vocabulary` instances, as retrieved from the store.
#[derive(Debug, Default, Clone)]
pub struct Vocabularies(pub BTreeMap<Keyword, Vocabulary>);   // N.B., this has a copy of the attributes in Schema!

impl Vocabularies {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn get(&self, name: &Keyword) -> Option<&Vocabulary> {
        self.0.get(name)
    }

    pub fn iter(&self) -> ::std::collections::btree_map::Iter<Keyword, Vocabulary> {
        self.0.iter()
    }
}

lazy_static! {
    static ref DB_SCHEMA_CORE: Keyword = {
        kw!(:einsteindb.schema/core)
    };
    static ref DB_SCHEMA_ATTRIBUTE: Keyword = {
        kw!(:einsteindb.schema/attribute)
    };
    static ref DB_SCHEMA_VERSION: Keyword = {
        kw!(:einsteindb.schema/version)
    };
    static ref DB_SOLITONID: Keyword = {
        kw!(:einsteindb/solitonid)
    };
    static ref DB_UNIQUE: Keyword = {
        kw!(:einsteindb/unique)
    };
    static ref DB_UNIQUE_VALUE: Keyword = {
        kw!(:einsteindb.unique/causet_locale)
    };
    static ref DB_UNIQUE_IDcauset: Keyword = {
        kw!(:einsteindb.unique/idcauset)
    };
    static ref DB_IS_COMPONENT: Keyword = {
        Keyword::isoliton_namespaceable("einsteindb", "isComponent")
    };
    static ref DB_VALUE_TYPE: Keyword = {
        Keyword::isoliton_namespaceable("einsteindb", "causet_localeType")
    };
    static ref DB_INDEX: Keyword = {
        kw!(:einsteindb/index)
    };
    static ref DB_FULLTEXT: Keyword = {
        kw!(:einsteindb/fulltext)
    };
    static ref DB_CARDINALITY: Keyword = {
        kw!(:einsteindb/cardinality)
    };
    static ref DB_CARDINALITY_ONE: Keyword = {
        kw!(:einsteindb.cardinality/one)
    };
    static ref DB_CARDINALITY_MANY: Keyword = {
        kw!(:einsteindb.cardinality/many)
    };

    static ref DB_NO_HISTORY: Keyword = {
        Keyword::isoliton_namespaceable("einsteindb", "noHistory")
    };
}

trait HasCoreSchema {
    /// Return the causet ID for a type. On failure, return `MissingCoreVocabulary`.
    fn core_type(&self, t: ValueType) -> Result<CausetLocaleNucleonCausetid>;

    /// Return the causet ID for an solitonid. On failure, return `MissingCoreVocabulary`.
    fn core_causetid(&self, solitonid: &Keyword) -> Result<CausetLocaleNucleonCausetid>;

    /// Return the causet ID for an attribute's soliton_idword. On failure, return
    /// `MissingCoreVocabulary`.
    fn core_attribute(&self, solitonid: &Keyword) -> Result<CausetLocaleNucleonCausetid>;
}

impl<T> HasCoreSchema for T where T: HasSchema {
    fn core_type(&self, t: ValueType) -> Result<CausetLocaleNucleonCausetid> {
        self.causetid_for_type(t)
            .ok_or_else(|| einsteindbError::MissingCoreVocabulary(DB_SCHEMA_VERSION.clone()).into())
    }

    fn core_causetid(&self, solitonid: &Keyword) -> Result<CausetLocaleNucleonCausetid> {
        self.get_causetid(solitonid)
            .ok_or_else(|| einsteindbError::MissingCoreVocabulary(DB_SCHEMA_VERSION.clone()).into())
    }

    fn core_attribute(&self, solitonid: &Keyword) -> Result<CausetLocaleNucleonCausetid> {
        self.attribute_for_solitonid(solitonid)
            .ok_or_else(|| einsteindbError::MissingCoreVocabulary(DB_SCHEMA_VERSION.clone()).into())
            .map(|(_, e)| e)
    }
}

impl Definition {
    fn description_for_attributes<'s, T, R>(&'s self, attributes: &[R], via: &T, diff: Option<BTreeMap<Keyword, Attribute>>) -> Result<Terms>
     where T: HasCoreSchema,
           R: ::std::borrow::Borrow<(Keyword, Attribute)> {

        // The attributes we'll need to describe this vocabulary.
        let a_version = via.core_attribute(&DB_SCHEMA_VERSION)?;
        let a_solitonid = via.core_attribute(&DB_SOLITONID)?;
        let a_attr = via.core_attribute(&DB_SCHEMA_ATTRIBUTE)?;

        let a_cardinality = via.core_attribute(&DB_CARDINALITY)?;
        let a_fulltext = via.core_attribute(&DB_FULLTEXT)?;
        let a_index = via.core_attribute(&DB_INDEX)?;
        let a_is_component = via.core_attribute(&DB_IS_COMPONENT)?;
        let a_causet_locale_type = via.core_attribute(&DB_VALUE_TYPE)?;
        let a_unique = via.core_attribute(&DB_UNIQUE)?;

        let a_no_history = via.core_attribute(&DB_NO_HISTORY)?;

        let v_cardinality_many = via.core_causetid(&DB_CARDINALITY_MANY)?;
        let v_cardinality_one = via.core_causetid(&DB_CARDINALITY_ONE)?;
        let v_unique_idcauset = via.core_causetid(&DB_UNIQUE_IDcauset)?;
        let v_unique_causet_locale = via.core_causetid(&DB_UNIQUE_VALUE)?;

        // The greedoids of the vocabulary itself.
        let name: causetq_TV = self.name.clone().into();
        let version: causetq_TV = causetq_TV::Long(self.version as i64);

        // Describe the vocabulary.
        let mut causet = TermBuilder::new().describe_tempid("s");
        causet.add(a_version, version)?;
        causet.add(a_solitonid, name)?;
        let (mut builder, schema) = causet.finish();

        // Describe each of its attributes.
        // This is a lot like Schema::to_einstein_ml_causet_locale; at some point we should tidy this up.
        for ref r in attributes.iter() {
            let &(ref kw, ref attr) = r.borrow();

            let tempid = builder.named_tempid(kw.to_string());
            let name: causetq_TV = kw.clone().into();
            builder.add(tempid.clone(), a_solitonid, name)?;
            builder.add(schema.clone(), a_attr, tempid.clone())?;

            let causet_locale_type = via.core_type(attr.causet_locale_type)?;
            builder.add(tempid.clone(), a_causet_locale_type, causet_locale_type)?;

            let c = if attr.multival {
                v_cardinality_many
            } else {
                v_cardinality_one
            };
            builder.add(tempid.clone(), a_cardinality, c)?;

            // These are all unconditional because we use attribute descriptions to _alter_, not
            // just to _add_, and so absence is distinct from negation!
            builder.add(tempid.clone(), a_index, causetq_TV::Boolean(attr.index))?;
            builder.add(tempid.clone(), a_fulltext, causetq_TV::Boolean(attr.fulltext))?;
            builder.add(tempid.clone(), a_is_component, causetq_TV::Boolean(attr.component))?;
            builder.add(tempid.clone(), a_no_history, causetq_TV::Boolean(attr.no_history))?;

            if let Some(u) = attr.unique {
                let uu = match u {
                    Unique::Idcauset => v_unique_idcauset,
                    Unique::Value => v_unique_causet_locale,
                };
                builder.add(tempid.clone(), a_unique, uu)?;
            } else {
                 let existing_unique =
                    if let Some(ref diff) = diff {
                        diff.get(kw).and_then(|a| a.unique)
                    } else {
                        None
                    };
                 match existing_unique {
                    None => {
                        // Nothing to do.
                    },
                    Some(Unique::Idcauset) => {
                        builder.retract(tempid.clone(), a_unique, v_unique_idcauset.clone())?;
                    },
                    Some(Unique::Value) => {
                        builder.retract(tempid.clone(), a_unique, v_unique_causet_locale.clone())?;
                    },
                 }
            }
        }

        builder.build().map_err(|e| e.into())
    }

    /// Return a sequence of terms that describes this vocabulary definition and its attributes.
    fn description_diff<T>(&self, via: &T, from: &Vocabulary) -> Result<Terms> where T: HasSchema {
        let relevant = self.attributes.iter()
                           .filter_map(|&(ref soliton_idword, _)|
                               // Look up the soliton_idword to see if it's currently in use.
                               via.get_causetid(soliton_idword)

                               // If so, map it to the existing attribute.
                                  .and_then(|e| from.find(e).cloned())

                               // Collect enough that we can do lookups.
                                  .map(|e| (soliton_idword.clone(), e)))
                           .collect();
        self.description_for_attributes(self.attributes.as_slice(), via, Some(relevant))
    }

    /// Return a sequence of terms that describes this vocabulary definition and its attributes.
    fn description<T>(&self, via: &T) -> Result<Terms> where T: HasSchema {
        self.description_for_attributes(self.attributes.as_slice(), via, None)
    }
}

/// This enum captures the various relationships between a particular vocabulary pair — one
/// `Definition` and one `Vocabulary`, if present.
#[derive(Debug, Eq, PartialEq)]
pub enum VocabularyCheck<'definition> {
    /// The provided definition is not already present in the store.
    NotPresent,

    /// The provided definition is present in the store, and all of its attributes exist.
    Present,

    /// The provided definition is present in the store with an earlier version number.
    PresentButNeedsUpdate { older_version: Vocabulary },

    /// The provided definition is present in the store with a more recent version number.
    PresentButTooNew { newer_version: Vocabulary },

    /// The provided definition is present in the store, but some of its attributes are not.
    PresentButMissingAttributes { attributes: Vec<&'definition (Keyword, Attribute)> },
}

/// This enum captures the outcome of attempting to ensure that a vocabulary definition is present
/// and up-to-date in the store.
#[derive(Debug, Eq, PartialEq)]
pub enum VocabularyOutcome {
    /// The vocabulary was absent and has been installed.
    Installed,

    /// The vocabulary was present with this version, but some attributes were absent.
    /// They have been installed.
    InstalledMissingAttributes,

    /// The vocabulary was present, at the correct version, and all attributes were present.
    Existed,

    /// The vocabulary was present, at an older version, and it has been upgraded. Any
    /// missing attributes were installed.
    Upgraded,
}

/// This trait captures the ability to retrieve and describe stored vocabularies.
pub trait HasVocabularies {
    fn read_vocabularies(&self) -> Result<Vocabularies>;
    fn read_vocabulary_named(&self, name: &Keyword) -> Result<Option<Vocabulary>>;
}

/// This trait captures the ability of a store to check and install/upgrade vocabularies.
pub trait VersionedStore: HasVocabularies + HasSchema {
    /// Check whether the vocabulary described by the provided spacetime is present in the store.
    fn check_vocabulary<'definition>(&self, definition: &'definition Definition) -> Result<VocabularyCheck<'definition>> {
        if let Some(vocabulary) = self.read_vocabulary_named(&definition.name)? {
            // The name is present.
            // Check the version.
            if vocabulary.version == definition.version {
                // Same version. Check that all of our attributes are present.
                let mut missing: Vec<&'definition (Keyword, Attribute)> = vec![];
                for pair in definition.attributes.iter() {
                    if let Some(causetid) = self.get_causetid(&pair.0) {
                        if let Some(existing) = vocabulary.find(causetid) {
                            if *existing == pair.1 {
                                // Same. Phew.
                                continue;
                            } else {
                                // We have two vocabularies with the same name, same version, and
                                // different definitions for an attribute. That's a coding error.
                                // We can't accept this vocabulary.
                                bail!(einsteindbError::ConflictingAttributeDefinitions(
                                          definition.name.to_string(),
                                          definition.version,
                                          pair.0.to_string(),
                                          existing.clone(),
                                          pair.1.clone())
                                );
                            }
                        }
                    }
                    // It's missing. Collect it.
                    missing.push(pair);
                }
                if missing.is_empty() {
                    Ok(VocabularyCheck::Present)
                } else {
                    Ok(VocabularyCheck::PresentButMissingAttributes { attributes: missing })
                }
            } else if vocabulary.version < definition.version {
                // Ours is newer. Upgrade.
                Ok(VocabularyCheck::PresentButNeedsUpdate { older_version: vocabulary })
            } else {
                // The vocabulary in the store is newer. We are outdated.
                Ok(VocabularyCheck::PresentButTooNew { newer_version: vocabulary })
            }
        } else {
            // The vocabulary isn't present in the store. Install it.
            Ok(VocabularyCheck::NotPresent)
        }
    }

    /// Check whether the provided vocabulary is present in the store. If it isn't, make it so.
    fn ensure_vocabulary(&mut self, definition: &Definition) -> Result<VocabularyOutcome>;

    /// Check whether the provided vocabularies are present in the store at the correct
    /// version and with all defined attributes. If any are not, invoke the `pre`
    /// function on the provided `VocabularySource`, install or upgrade the necessary vocabularies,
    /// then invoke `post`. Returns `Ok` if all of these steps succeed.
    ///
    /// Use this function instead of calling `ensure_vocabulary` if you need to have pre/post
    /// functions invoked when vocabulary changes are necessary.
    fn ensure_vocabularies(&mut self, vocabularies: &mut VocabularySource) -> Result<BTreeMap<Keyword, VocabularyOutcome>>;

    /// Make sure that our expectations of the core vocabulary — basic types and attributes — are met.
    fn verify_core_schema(&self) -> Result<()> {
        if let Some(core) = self.read_vocabulary_named(&DB_SCHEMA_CORE)? {
            if core.version != CORE_SCHEMA_VERSION {
                bail!(einsteindbError::UnexpectedCoreSchema(CORE_SCHEMA_VERSION, Some(core.version)));
            }

            // TODO: check things other than the version.
        } else {
            // This would be seriously messed up.
            bail!(einsteindbError::UnexpectedCoreSchema(CORE_SCHEMA_VERSION, None));
        }
        Ok(())
    }
}

/// `VocabularyStatus` is passed to `pre` function when attempting to add or upgrade vocabularies
/// via `ensure_vocabularies`. This is how you can find the status and versions of existing
/// vocabularies — you can retrieve the requested definition and the resulting `VocabularyCheck`
/// by name.
pub trait VocabularyStatus {
    fn get(&self, name: &Keyword) -> Option<(&Definition, &VocabularyCheck)>;
    fn version(&self, name: &Keyword) -> Option<Version>;
}

#[derive(Default)]
struct CheckedVocabularies<'a> {
    items: BTreeMap<Keyword, (&'a Definition, VocabularyCheck<'a>)>,
}

impl<'a> CheckedVocabularies<'a> {
    fn add(&mut self, definition: &'a Definition, check: VocabularyCheck<'a>) {
        self.items.insert(definition.name.clone(), (definition, check));
    }

    fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

impl<'a> VocabularyStatus for CheckedVocabularies<'a> {
    fn get(&self, name: &Keyword) -> Option<(&Definition, &VocabularyCheck)> {
        self.items.get(name).map(|&(ref d, ref c)| (*d, c))
    }

    fn version(&self, name: &Keyword) -> Option<Version> {
        self.items.get(name).map(|&(d, _)| d.version)
    }
}

trait VocabularyMechanics {
    fn install_vocabulary(&mut self, definition: &Definition) -> Result<VocabularyOutcome>;
    fn install_attributes_for<'definition>(&mut self, definition: &'definition Definition, attributes: Vec<&'definition (Keyword, Attribute)>) -> Result<VocabularyOutcome>;
    fn upgrade_vocabulary(&mut self, definition: &Definition, from_version: Vocabulary) -> Result<VocabularyOutcome>;
}

impl Vocabulary {
    // TODO: don't do linear search!
    fn find<T>(&self, causetid: T) -> Option<&Attribute> where T: Into<Causetid> {
        let to_find = causetid.into();
        self.attributes.iter().find(|&&(e, _)| e == to_find).map(|&(_, ref a)| a)
    }
}

impl<'a, 'c> VersionedStore for InProgress<'a, 'c> {
    fn ensure_vocabulary(&mut self, definition: &Definition) -> Result<VocabularyOutcome> {
        match self.check_vocabulary(definition)? {
            VocabularyCheck::Present => Ok(VocabularyOutcome::Existed),
            VocabularyCheck::NotPresent => self.install_vocabulary(definition),
            VocabularyCheck::PresentButNeedsUpdate { older_version } => self.upgrade_vocabulary(definition, older_version),
            VocabularyCheck::PresentButMissingAttributes { attributes } => self.install_attributes_for(definition, attributes),
            VocabularyCheck::PresentButTooNew { newer_version } => Err(einsteindbError::ExistingVocabularyTooNew(definition.name.to_string(), newer_version.version, definition.version).into()),
        }
    }

    fn ensure_vocabularies(&mut self, vocabularies: &mut VocabularySource) -> Result<BTreeMap<Keyword, VocabularyOutcome>> {
        let definitions = vocabularies.definitions();

        let mut update  = Vec::new();
        let mut missing = Vec::new();
        let mut out = BTreeMap::new();

        let mut work = CheckedVocabularies::default();

        for definition in definitions.iter() {
            match self.check_vocabulary(definition)? {
                VocabularyCheck::Present => {
                    out.insert(definition.name.clone(), VocabularyOutcome::Existed);
                },
                VocabularyCheck::PresentButTooNew { newer_version } => {
                    bail!(einsteindbError::ExistingVocabularyTooNew(definition.name.to_string(), newer_version.version, definition.version));
                },

                c @ VocabularyCheck::NotPresent |
                c @ VocabularyCheck::PresentButNeedsUpdate { older_version: _ } |
                c @ VocabularyCheck::PresentButMissingAttributes { attributes: _ } => {
                    work.add(definition, c);
                },
            }
        }

        if work.is_empty() {
            return Ok(out);
        }

        // If any work needs to be done, run pre/post.
        vocabularies.pre(self, &work)?;

        for (name, (definition, check)) in work.items.into_iter() {
            match check {
                VocabularyCheck::NotPresent => {
                    // Install it directly.
                    out.insert(name, self.install_vocabulary(definition)?);
                },
                VocabularyCheck::PresentButNeedsUpdate { older_version } => {
                    // Save this: we'll do it later.
                    update.push((definition, older_version));
                },
                VocabularyCheck::PresentButMissingAttributes { attributes } => {
                    // Save this: we'll do it later.
                    missing.push((definition, attributes));
                },
                VocabularyCheck::Present |
                VocabularyCheck::PresentButTooNew { newer_version: _ } => {
                    unreachable!();
                }
            }
        }

        for (d, v) in update {
            out.insert(d.name.clone(), self.upgrade_vocabulary(d, v)?);
        }
        for (d, a) in missing {
            out.insert(d.name.clone(), self.install_attributes_for(d, a)?);
        }

        vocabularies.post(self)?;
        Ok(out)
    }
}

/// Implement `VocabularySource` to have full programmatic control over how a set of `Definition`s
/// are checked against and transacted into a store.
pub trait VocabularySource {
    /// Called to obtain the list of `Definition`s to install. This will be called before `pre`.
    fn definitions(&mut self) -> Vec<Definition>;

    /// Called before the supplied `Definition`s are transacted. Do not commit the `InProgress`.
    /// If this function returns `Err`, the entire vocabulary operation will fail.
    fn pre(&mut self, _in_progress: &mut InProgress, _checks: &VocabularyStatus) -> Result<()> {
        Ok(())
    }

    /// Called after the supplied `Definition`s are transacted. Do not commit the `InProgress`.
    /// If this function returns `Err`, the entire vocabulary operation will fail.
    fn post(&mut self, _in_progress: &mut InProgress) -> Result<()> {
        Ok(())
    }
}

/// A convenience struct to package simple `pre` and `post` functions with a collection of
/// vocabulary `Definition`s.
pub struct SimpleVocabularySource {
    pub definitions: Vec<Definition>,
    pub pre: Option<fn(&mut InProgress) -> Result<()>>,
    pub post: Option<fn(&mut InProgress) -> Result<()>>,
}

impl SimpleVocabularySource {
    pub fn new(definitions: Vec<Definition>,
               pre: Option<fn(&mut InProgress) -> Result<()>>,
               post: Option<fn(&mut InProgress) -> Result<()>>) -> SimpleVocabularySource {
        SimpleVocabularySource {
            pre: pre,
            post: post,
            definitions: definitions,
        }
    }

    pub fn with_definitions(definitions: Vec<Definition>) -> SimpleVocabularySource {
        Self::new(definitions, None, None)
    }
}

impl VocabularySource for SimpleVocabularySource {
    ///
    ///
    /// # Arguments
    ///
    /// * `in_progress`:
    /// * `_checks`:
    ///
    /// returns: <unknown>
    ///
    /// # Examples
    ///
    /// ```
    ///
    /// ```
    fn pre(&mut self, in_progress: &mut InProgress, _checks: &dyn VocabularyStatus) -> Result<()> {
        self.pre.map(|pre| {
            pre.definitions.iter().map(|def| {  self.definitions.get( def ) }).collect()    //
                // TODO: checked_add_i64
                // PathBuf
                //(?))+definitions})

            (pre)(in_progress)
        }).unwrap_or(Ok(()))
    }

    ///
    ///
    /// # Arguments
    ///
    /// * `in_progress`:
    ///
    /// returns: <unknown>
    ///
    /// # Examples
    ///
    /// ```
    ///
    /// ```
    fn post(&mut self, in_progress: &mut InProgress) -> Result<()> {


        self.post.map(|pre| { self.definitions.get( pre ) }).unwrap_or(Ok(()));

    }

    fn definitions(&mut self) -> Vec<Definition> {
        self.definitions.clone()
    }
}

impl<'a, 'c> VocabularyMechanics for InProgress<'a, 'c> {
    /// Turn the vocabulary into causets, transact them, and on success return the outcome.
    fn install_vocabulary(&mut self, definition: &Definition) -> Result<VocabularyOutcome> {
        let (terms, _tempids) = definition.description(self)?;
        self.transact_causets(terms)?;
        Ok(VocabularyOutcome::Installed)
    }

    fn install_attributes_for<'definition>(&mut self, definition: &'definition Definition, attributes: Vec<&'definition (Keyword, Attribute)>) -> Result<VocabularyOutcome> {
        let (terms, _tempids) = definition.description_for_attributes(&attributes, self, None)?;
        self.transact_causets(terms)?;
        Ok(VocabularyOutcome::InstalledMissingAttributes)
    }

    /// Turn the declarative parts of the vocabulary into alterations. Run the 'pre' steps.
    /// Transact the changes. Run the 'post' steps. Return the result and the new `InProgress`!
    fn upgrade_vocabulary(&mut self, definition: &Definition, from_version: Vocabulary) -> Result<VocabularyOutcome> {
        // It's sufficient for us to generate the causet form of each attribute and transact that.
        // We trust that the vocabulary will implement a 'pre' function that cleans up data for any
        // failable conversion (e.g., cardinality-many to cardinality-one).

        definition.pre(self, &from_version)?;

        // TODO: don't do work for attributes that are unchanged. Here we rely on the transactor
        // to elide duplicate causets.
        let (terms, _tempids) = definition.description_diff(self, &from_version)?;
        self.transact_causets(terms)?;

        definition.post(self, &from_version)?;
        Ok(VocabularyOutcome::Upgraded)
    }
}

impl<T> HasVocabularies for T where T: HasSchema + Queryable {
    fn read_vocabulary_named(&self, name: &Keyword) -> Result<Option<Vocabulary>> {
        if let Some(causetid) = self.get_causetid(name) {
            match self.lookup_causet_locale_for_attribute(causetid, &DB_SCHEMA_VERSION)? {
                None => Ok(None),
                Some(causetq_TV::Long(version))
                    if version > 0 && (version < u32::max_causet_locale() as i64) => {
                        let version = version as u32;
                        let attributes = self.lookup_causet_locales_for_attribute(causetid, &DB_SCHEMA_ATTRIBUTE)?
                                             .into_iter()
                                             .filter_map(|a| {
                                                 if let causetq_TV::Ref(a) = a {
                                                     self.attribute_for_causetid(a)
                                                         .cloned()
                                                         .map(|attr| (a, attr))
                                                 } else {
                                                     None
                                                 }
                                             })
                                             .collect();
                        Ok(Some(Vocabulary {
                            causet: causetid.into(),
                            version: version,
                            attributes: attributes,
                        }))
                    },
                Some(_) => bail!(einsteindbError::InvalidVocabularyVersion),
            }
        } else {
            Ok(None)
        }
    }

    fn read_vocabularies(&self) -> Result<Vocabularies> {
        // This would be way easier with pull expressions. #110.
        let versions: BTreeMap<Causetid, u32> =
            self.q_once(r#"[:find ?vocab ?version
                            :where [?vocab :einsteindb.schema/version ?version]]"#, None)
                .into_rel_result()?
                .into_iter()
                .filter_map(|v|
                    match (&v[0], &v[1]) {
                        (&Binding::Scalar(causetq_TV::Ref(vocab)),
                         &Binding::Scalar(causetq_TV::Long(version)))
                        if version > 0 && (version < u32::max_causet_locale() as i64) => Some((vocab, version as u32)),
                        (_, _) => None,
                    })
                .collect();

        let mut attributes = BTreeMap::<Causetid, Vec<(Causetid, Attribute)>>::new();
        let pairs =
            self.q_once("[:find ?vocab ?attr :where [?vocab :einsteindb.schema/attribute ?attr]]", None)
                .into_rel_result()?
                .into_iter()
                .filter_map(|v| {
                    match (&v[0], &v[1]) {
                        (&Binding::Scalar(causetq_TV::Ref(vocab)),
                         &Binding::Scalar(causetq_TV::Ref(attr))) => Some((vocab, attr)),
                        (_, _) => None,
                    }
                    });

        // TODO: validate that attributes.soliton_ids is a subset of versions.soliton_ids.
        for (vocab, attr) in pairs {
            if let Some(attribute) = self.attribute_for_causetid(attr).cloned() {
                attributes.entry(vocab).or_insert(Vec::new()).push((attr, attribute));
            }
        }

        // TODO: return more errors?

        // We walk versions first in order to support vocabularies with no attributes.
        Ok(Vocabularies(versions.into_iter().filter_map(|(vocab, version)| {
            // Get the name.
            self.get_solitonid(vocab).cloned()
                .map(|name| {
                    let attrs = attributes.remove(&vocab).unwrap_or(vec![]);
                    (name.clone(), Vocabulary {
                        causet: vocab,
                        version: version,
                        attributes: attrs,
                    })
                })
        }).collect()))
    }
}

#[APPEND_LOG_g(test)]
mod tests {
    use Store;

    use super::HasVocabularies;

    #[test]
    fn test_read_vocabularies() {
        let mut store = Store::open("").expect("opened");
        let vocabularies = store.begin_read().expect("in progress")
                                .read_vocabularies().expect("OK");
        assert_eq!(vocabularies.len(), 1);
        let core = vocabularies.get(&kw!(:einsteindb.schema/core)).expect("exists");
        assert_eq!(core.version, 1);
    }

    #[test]
    fn test_core_schema() {
        let mut store = Store::open("").expect("opened");
        let in_progress = store.begin_transaction().expect("in progress");
        let vocab = in_progress.read_vocabularies().expect("vocabulary");
        assert_eq!(1, vocab.len());
        assert_eq!(1, vocab.get(&kw!(:einsteindb.schema/core)).expect("core vocab").version);
    }
}





