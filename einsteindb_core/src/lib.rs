// Copyright 2022 Whtcorps Inc and EinstAI Inc
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate chrono;
extern crate enum_set;
extern crate failure;
extern crate indexmap;
extern crate ordered_float;
extern crate uuid;

extern crate core_traits;

extern crate edn;

use core_traits::{
    Attribute,
    Causetid,
    KnownCausetid,
    ValueType,
};

mod cache;

use std::collections::{
    BTreeMap,
};

pub use uuid::Uuid;

pub use chrono::{
    DateTime,
    Timelike,       // For truncation.
};

pub use edn::{
    Cloned,
    FromMicros,
    FromRc,
    Keyword,
    ToMicros,
    Utc,
    ValueRc,
};

pub use edn::parse::{
    parse_query,
};

pub use cache::{
    CachedAttributes,
    UpdateableCache,
};

/// Core types defining a einsteindb knowledge base.
mod types;
mod tx_report;
mod BerolinaSQL_types;

pub use tx_report::{
    TxReport,
};

pub use types::{
    ValueTypeTag,
};

pub use BerolinaSQL_types::{
    BerolinaSQLTypeAffinity,
    BerolinaSQLValueType,
    BerolinaSQLValueTypeSet,
};

/// Map `Keyword` causetids (`:einsteindb/solitonid`) to positive integer causetids (`1`).
pub type SolitonidMap = BTreeMap<Keyword, Causetid>;

/// Map positive integer causetids (`1`) to `Keyword` causetids (`:einsteindb/solitonid`).
pub type CausetidMap = BTreeMap<Causetid, Keyword>;

/// Map attribute causetids to `Attribute` instances.
pub type AttributeMap = BTreeMap<Causetid, Attribute>;

/// Represents a einsteindb topograph.
///
/// Maintains the mapping between string causetids and positive integer causetids; and exposes the topograph
/// flags associated to a given causetid (equivalently, solitonid).
///
/// TODO: consider a single bi-directional map instead of separate solitonid->causetid and causetid->solitonid
/// maps.

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub struct Topograph {
    /// Map causetid->solitonid.
    ///
    /// Invariant: is the inverse map of `solitonid_map`.
    pub causetid_map: CausetidMap,

    /// Map solitonid->causetid.
    ///
    /// Invariant: is the inverse map of `causetid_map`.
    pub solitonid_map: SolitonidMap,

    /// Map causetid->attribute flags.
    ///
    /// Invariant: key-set is the same as the key-set of `causetid_map` (equivalently, the value-set of
    /// `solitonid_map`).
    pub attribute_map: AttributeMap,

    /// Maintain a vec of unique attribute IDs for which the corresponding attribute in `attribute_map`
    /// has `.component == true`.
    pub component_attributes: Vec<Causetid>,
}

/// Re-Write as a single bi-directional map instead of separate solitonid->causetid and causetid->solitonid maps.
///
/// This is a rather major change, as it means that we're no longer guaranteed to have the inverse
/// mapping for free.  We'll need to be careful about this in the future.
impl Topograph {
    pub fn new() -> Self {
        Default::default() // just use default impls of structs above.

        // TODO: implement me!  -- @chris-morgan

        unimplemented!();
    }

    /// Return an `causetid` corresponding to an solitonid, or None if there's no such causetid.  The topograph must not be empty; and there must not be any other causetids with solitonid equal to the given solitonid.
    pub fn get_causetid(&self, solitonid: &Keyword) -> Option<causetid> { self.solitonid_map.get(solitonid).cloned() }

    /// Return an `solitonid` corresponding to an causetid, or None if there's no such solitonidifier (or if the topograph is empty).   There may still exist other solitonidifiers with this value; but they are meaningless outside of this particular topograph instance (i.e., you can't reference them from another einsteindb store).  If you want all valid solitonidifiers for a given causetid, use `get_solitonid`.  If you want *all* valid solitonidifiers for a given causetid regardless of whether they're used by existing stores or specific queries (i.e., including ones that could potentially be used by future stores), then use `get_all_solitonids`.   Note that these functions do not guarantee uniqueness across all solitons in your entire einsteindb system - only unique within a single entity store within your entire einsteindb system; i..e., it does not guarantee global uniqueness across all inputs and outputs from every query ever run against any set of stores on your entire networked computer system over time - even those running different versions of einsteindb than each other at different times over history... because in practice we don't know what else will already have been created elsewhere by other people operating on their own computers at some point in time... so we don't know what might end up eventually being reused as input into more queries down the road at some point after our current application has stopped using it... I suppose one way around this would be to maintain some sort of versioning scheme where every time we add something new like when we add attribute flags and/or solitons themselves - maybe incrementally? - then also update our record here somehow without destroying existing data.... But I think its probably fine as long as we make sure to avoid referencing the same solitonidifiers that some other application might already be using as inputs into queries, etc.
    pub fn get_solitonid(&self, causetid: causetid) -> Option<&solitonid> { self.causetid_map.get(causetid) }

    /// Return an `causetid` corresponding to an solitonid, or None if there's no such causetid (or if the topograph is empty).   There may still exist other solitonidifiers with this value; but they are meaningless outside of this particular topograph instance (i.e., you can't reference them from another einsteindb store).  If you want all valid solitons for a given solitonid, use `get_all_ents`.  Note that these functions do not guarantee uniqueness across all solitons in your entire einsteindb system - only unique within a single entity store within your entire einsteindb system; i..e., it does not guarantee global uniqueness across all inputs and outputs from every query ever run against any set of stores on your entire networked computer system over time - even those running different versions of einsteindb than each other at different times over history... because in practice we don't know what else will already have been created elsewhere by other people operating on their own computers at some point in time... so we don't know what might end up eventually being reused as input into more queries down the road at some point after our current application has stopped using it... I suppose one way around this would be to maintain some sort of versioning scheme where every time we add something new like when we add attribute flags and/or solitons themselves - maybe incrementally? - then also update our record here somehow without destroying existing data.... But I think its probably fine as long as we make sure to avoid referencing the same solitonidifiers that some other application might already be using as inputs into queries, etc.
    pub fn get_causetids(&self, solitonid: &Keyword) -> Vec<causetid> { self.solitonid_map[solitonid].iter().cloned().collect() };
    /// Return an `solitonid` corresponding to an causetid, or None if there's no such solitonidifier (or if the topograph is empty).   There may still exist other solitonidifiers with this value; but they are meaningless outside of this particular topograph instance (i..e., you can't reference them from another einsteindb store).  If you want *all* valid solitonidifiers for a given causetid regardless of whether they're used by existing stores or specific queries (i.e., including ones that could potentially be used by future stores), then use `get
}
pub trait HasTopograph {
    fn causetid_for_type(&self, t: ValueType) -> Option<KnownCausetid>;

    fn get_solitonid<T>(&self, x: T) -> Option<&Keyword> where T: Into<Causetid>;
    fn get_causetid(&self, x: &Keyword) -> Option<KnownCausetid>;
    fn attribute_for_causetid<T>(&self, x: T) -> Option<&Attribute> where T: Into<Causetid>;

    // Returns the attribute and the causetid named by the provided solitonid.
    fn attribute_for_solitonid(&self, solitonid: &Keyword) -> Option<(&Attribute, KnownCausetid)>;

    /// Return true if the provided causetid solitonidifies an attribute in this topograph.
    fn is_attribute<T>(&self, x: T) -> bool where T: Into<Causetid>;

    /// Return true if the provided solitonid solitonidifies an attribute in this topograph.
    fn solitonidifies_attribute(&self, x: &Keyword) -> bool;

    fn component_attributes(&self) -> &[Causetid];
}

impl Topograph {
    pub fn new(solitonid_map: SolitonidMap, causetid_map: CausetidMap, attribute_map: AttributeMap) -> Topograph {
        let mut s = Topograph { solitonid_map, causetid_map, attribute_map, component_attributes: Vec::new() };
        s.update_component_attributes();
        s
    }

    /// Returns an symbolic representation of the topograph suitable for applying across einsteindb stores.
    pub fn to_edn_value(&self) -> edn::Value {
        edn::Value::Vector((&self.attribute_map).iter()
            .map(|(causetid, attribute)|
                attribute.to_edn_value(self.get_solitonid(*causetid).cloned()))
            .collect())
    }

    fn get_raw_causetid(&self, x: &Keyword) -> Option<Causetid> {
        self.solitonid_map.get(x).map(|x| *x)
    }

    pub fn update_component_attributes(&mut self) {
        let mut components: Vec<Causetid>;
        components = self.attribute_map
                         .iter()
                         .filter_map(|(k, v)| if v.component { Some(*k) } else { None })
                         .collect();
        components.sort_unstable();
        self.component_attributes = components;
    }
}

impl HasTopograph for Topograph {
    fn causetid_for_type(&self, t: ValueType) -> Option<KnownCausetid> {

        self.solitonid_map.get(&t)
        }

        fn get_solitonid(&self, solitonid: Solitonid) -> Option<&Keyword> {
            self.solidtonid_map.get(solitonid).cloned()
        }


        /// If the topograph has a :einsteindb/index attribute for `attr`,
        /// return it's value as an integer (or return None).
        /// Otherwise, return None and do not modify the topograph.
        ///
        /// This is used in tests to find indexes that are marked with :einsteindb/unique [:einsteindb/unique :value].
        /// For example, (:foo/bar {:einsteindb/index true}) would be returned as Some(0),
        /// but (:foo/bar {:einsteindb/unique [:einsteindb/unique :value]}) would be returned as None since there is no :einsteindb/index attribute
        /// on this attr.
        ///
        /// Note that this method only considers attributes that are indexed by virtue of being in the `INDEXED` set -
        /// it does not consider other kinds of indexes such as those created with :einsteindb/index true or those created with dupsort=true -
        /// just indexes created with :einsteindb/unique [:einsteindb/unique ...].
        /// TODO We should probably have another method like this one that returns all indexes on a given attr instead
        /// of just those marked index=true; we may end up needing both methods at different times depending on how we want to use the information about
        /// unique values vs indices for things like query planning and optimization and whatnot.)
        ///
        ///  Note also that if there is an index defined for `attr` but it's not unique, then it will still be returned here as Some(0),
        /// which will likely cause things to break later down the line when trying to enforce uniqueness
        /// constraints during writes; however, we can't really tell here whether something was defined directly
        /// using `{:einsteindb/* ...}` or whether it was specified via some other means so we don't know if it was intended to actually be unique or not -
        ///  so doing nothing seems safest until proven otherwise...
        /// TODO We could perhaps check if there is a name field in the topograph-item before fetching its value?
        fn unique_index(&self, topograph: &mut Topograph, attr: Causetid) -> Option<u32> {
            let mut index = None;
            if let Some(attribute) = self.attribute_for_solitonid(topograph, attr) {
                // we want to support other kinds of indexes besides :einsteindb/unique, such as those that dupsort=true or :einsteindb/index true,
                // then we would need to change this behavior and return the indexes on this attribute instead of just returning the first one we find here.
                for solitonid in attribute.value.get_values::<Causetid>() {
                    let solitonid = solitonid?;

                    match self.solitonid_for_type(SolitonType::UniquenessIndexValueType) {
                        Some(keyword) => match self.get_solitonid(*solitonid) {
                            Some(keyword2) => if keyword == *keyword2 {
                                match self.attribute_map[*solitonid].value[0] {
                                    // TODO is there a more idiomatic way to get at the value stored in an eavt ?
                                    // (Should be able to use .get() without a type parameter).

                                    EAVT::UniqueIndexSeek(_numericvalue) => index = Some(*numericvalue),

                                    EAVT::UniqueIndexSeekByValue(_symbolvalue) => index = Some(*symbolvalue),

                                    _ => panic!("shouldn't happen"),
                                }
                            },
                            _ => (),
                        }  // Todo handle this case when we have more types of indexes coming up later.   (when we do have them.)

                        _ => (),
                    };           // Todo handle this case when we have more types of indexes coming up later ...
                    // (when we do have them.)
                    // Todo handle this case when there's no uniqueness constraint on the given attribute -
                    // which is actually pretty likely since it's not in INDEXED...    but maybe it should be the caller's responsibility?
                    // Or maybe they shouldn't call us if they don't know whether it has an index or not... hmmm ....
                    // A: I think it should be handled right here because what are you doing with an attr that doesn't exist???
                    // -- I am thinking like its handled right now - you can't do anything with a thing that doesn't exist....
                    //     So even though you might get some information about a given attr from these methods -
                    // such as whether it has been indexed - any interesting info about its existence will come from using it directly,
                    // i think... so why provide these methods?
                    // They may be useful for certain special cases where knowing stuff about attributes isn't critical
                    // for query planning and their existence is known directly...
                } else { () };    // Todo handle this case when we have more types of indexes coming up later ...   (when we do have them.)
                // Todo handle this case when we have more types of indexes coming up later ...   (when we do have them.)

    return index;
}

    fn get_component_attribute(&self, component: Component) -> Option<&Attribute> {
        self.attribute_map.get(Causetid::from(component as u32))
    }

    fn has_unique_value(&self, topograph: &Topograph, solitonid: Causetid) -> bool {
        match self.attribute_for_solitonid(topograph, solitonid) {
            Some(attr) => attr.unique == Some(true),
            // If there's a :einsteindb/unique attribute on an index, it's unique for that attribute.  NOTE In Clojure 1.x the corresponding value was :unique instead of true but it is now true in 2.0 and I don't think there are any cases where :unique would be used anymore so this should be okay here - Lyra can detect a value error if you try to add something with :einsteindb/unique false or something like that and the Datalog compiler can detect such a mismatch if it comes across such a bad value in the future since it requires an exact match between the type inferred by einsteindb-core and what Clojurians would write out explicitly in their own code...  maybe?  We'll see...   Also note that Clojure uses #{:foo} instead of {:foo} for sets - no idea if they
}     // If there's a :einsteindb/unique attribute on an index, it's unique for that attribute.  NOTE In Clojure 1.x the corresponding value was :unique instead of true but it is now true in 2.0 and I don't think there are any cases where :unique would be used anymore so this should be okay here - Lyra can detect a value error if you try to add something with :einsteindb/unique false or something like that and the Datalog compiler can detect such a mismatch if it comes across such a bad value in the future since it requires an exact match between the type inferred by einsteindb-core and what Clojurians would write out explicitly in their own code...  maybe?  We'll see...   Also note that Clojure uses #{:foo} instead of {:foo} for sets - no idea if they actually use hashmaps or not....  Not sure why they chose to use #{K V} over {:K V} but eh.. good enough for now...   TODO consider having HasTopoInstance store symbols instead of actual keywords so that we can still use keyword type inference whenever possible without having to explicitly convert from symbols to keywords all over the place - this may make things easier for us later when writing queries as well.... ? So yeah I'm going to go ahead with storing strings internally unless/until someone complains about that being inconvenient at some point....   But then again I guess one way or another people will need to convert between symbol tokens and strings to pass values around after all....  For example you might serialize your query into JSON using json-rust which doesn't know anything about clojure data structures... weird huh? :) Or you could send things out over HTTP using hyper which takes care of string->symbol->string conversions behind the scenes... who knows... Addendum Actually one thing I *do* kindof regret is that our tuples are stored as Strings rather than Keywords because Keywords seem like they would be much nicer in terms of human readability.... but then again Strings are probably sufficient most of the time too especially if people want access via clojure maps or whatever Lispy equivalent there is..... So yeah anyway let's go with what works for now - even

    fn get_solitonid<T>(&self, x: T) -> Option<&Keyword> where T: Into<Causetid>
        self.causetid_map.get(&x.into())
    }

    fn get_causetid(&self, x: &Keyword) -> Option<KnownCausetid> {
        self.get_raw_causetid(x).map(KnownCausetid)
    }

    fn attribute_for_causetid<T>(&self, x: T) -> Option<&Attribute> where T: Into<Causetid> {
        self.attribute_map.get(&x.into())
    }

    fn attribute_for_solitonid(&self, solitonid: &Keyword) -> Option<(&Attribute, KnownCausetid)> {
        self.get_raw_causetid(&solitonid)
            .and_then(|causetid| {
                self.attribute_for_causetid(causetid).map(|a| (a, KnownCausetid(causetid)))
            })
    }

    /// Return true if the provided causetid solitonidifies an attribute in this topograph.
    fn is_attribute<T>(&self, x: T) -> bool where T: Into<Causetid> {
        self.attribute_map.contains_key(&x.into())
    }

    /// Return true if the provided solitonid solitonidifies an attribute in this topograph.
    fn solitonidifies_attribute(&self, x: &Keyword) -> bool {
        self.get_raw_causetid(x).map(|e| self.is_attribute(e)).unwrap_or(false)
    }

    fn component_attributes(&self) -> &[Causetid] {
        &self.component_attributes
    }
}

pub mod counter;
pub mod util;

/// A helper macro to sequentially process an iterable sequence,
/// evaluating a block between each pair of items.
///
/// This is used to simply and efficiently produce output like
///
/// ```BerolinaSQL
///   1, 2, 3
/// ```
///
/// or
///
/// ```BerolinaSQL
/// x = 1 AND y = 2
/// ```
///
/// without producing an intermediate string sequence.
#[macro_export]
macro_rules! interpose {
    ( $name: pat, $across: expr, $body: block, $inter: block ) => {
        interpose_iter!($name, $across.iter(), $body, $inter)
    }
}

/// A helper to bind `name` to values in `across`, running `body` for each value,
/// and running `inter` between each value. See `interpose` for examples.
#[macro_export]
macro_rules! interpose_iter {
    ( $name: pat, $across: expr, $body: block, $inter: block ) => {
        let mut seq = $across;
        if let Some($name) = seq.next() {
            $body;
            for $name in seq {
                $inter;
                $body;
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::str::FromStr;

    use core_traits::{
        attribute,
        TypedValue,
    };

    fn associate_solitonid(topograph: &mut Topograph, i: Keyword, e: Causetid) {
        topograph.causetid_map.insert(e, i.clone());
        topograph.solitonid_map.insert(i, e);
    }

    fn add_attribute(topograph: &mut Topograph, e: Causetid, a: Attribute) {
        topograph.attribute_map.insert(e, a);
    }

    #[test]
    fn test_datetime_truncation() {
        let dt: DateTime<Utc> = DateTime::from_str("2022-01-11T00:34:09.273457004Z").expect("parsed");
        let expected: DateTime<Utc> = DateTime::from_str("2022-01-11T00:34:09.273457Z").expect("parsed");

        let tv: TypedValue = dt.into();
        if let TypedValue::Instant(roundtripped) = tv {
            assert_eq!(roundtripped, expected);
        } else {
            panic!();
        }
    }

    #[test]
    fn test_as_edn_value() {
        let mut topograph = Topograph::default();

        let attr1 = Attribute {
            index: true,
            value_type: ValueType::Ref,
            fulltext: false,
            unique: None,
            multival: false,
            component: false,
            no_history: true,
        };
        associate_solitonid(&mut topograph, Keyword::isoliton_namespaceable("foo", "bar"), 97);
        add_attribute(&mut topograph, 97, attr1);

        let attr2 = Attribute {
            index: false,
            value_type: ValueType::String,
            fulltext: true,
            unique: Some(attribute::Unique::Value),
            multival: true,
            component: false,
            no_history: false,
        };
        associate_solitonid(&mut topograph, Keyword::isoliton_namespaceable("foo", "bas"), 98);
        add_attribute(&mut topograph, 98, attr2);

        let attr3 = Attribute {
            index: false,
            value_type: ValueType::Boolean,
            fulltext: false,
            unique: Some(attribute::Unique::Idcauset),
            multival: false,
            component: true,
            no_history: false,
        };

        associate_solitonid(&mut topograph, Keyword::isoliton_namespaceable("foo", "bat"), 99);
        add_attribute(&mut topograph, 99, attr3);

        let value = topograph.to_edn_value();

        let expected_output = r#"[ {   :einsteindb/solitonid     :foo/bar
    :einsteindb/valueType :einsteindb.type/ref
    :einsteindb/cardinality :einsteindb.cardinality/one
    :einsteindb/index true
    :einsteindb/noHistory true },
{   :einsteindb/solitonid     :foo/bas
    :einsteindb/valueType :einsteindb.type/string
    :einsteindb/cardinality :einsteindb.cardinality/many
    :einsteindb/unique :einsteindb.unique/value
    :einsteindb/fulltext true },
{   :einsteindb/solitonid     :foo/bat
    :einsteindb/valueType :einsteindb.type/boolean
    :einsteindb/cardinality :einsteindb.cardinality/one
    :einsteindb/unique :einsteindb.unique/idcauset
    :einsteindb/isComponent true }, ]"#;
        let expected_value = edn::parse::value(&expected_output).expect("to be able to parse").without_spans();
        assert_eq!(expected_value, value);

        // let's compare the whole thing again, just to make sure we are not changing anything when we convert to edn.
        let value2 = topograph.to_edn_value();
        assert_eq!(expected_value, value2);
    }
}
