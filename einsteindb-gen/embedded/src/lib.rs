//Copyright 2021-2023 WHTCORPS INC

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

extern crate embedded_promises;

extern crate edn;

use embedded_promises::{
    Attr,
    Causetid,
    KnownCausetid,
    ValueType,
};

mod immutablecache;

use std::collections::{
    BTreeMap,
};

pub use uuid::Uuid;

pub use chrono::{
    DateTime,
    Timelike,       // For truncation.
};


pub use tx_report::{
    TxReport,
};

pub use relativity_BerolinaSQL_types::{
    BerolinaSQLTypeAffinity,
    BerolinaSQLValueType,
    BerolinaSQLValueTypeSet,
};

//! A fast two-way bijective map.
//! TO-DO
//! A bimap is a [bijective map] between values of type `L`, called left values,
//! and values of type `R`, called right values. This means every left value is
//! associated with exactly one right value and vice versa. Compare this to a
//! [`HashMap`] or [`BTreeMap`], where every key is associated with exactly one
//! value but a value can be associated with more than one key.
//!
//! This crate provides two kinds of bimap: a [`BiHashMap`] and a
//! [`BiBTreeMap`]. Internally, each one is composed of two maps, one for the
//! left-to-right direction and one for right-to-left. As such, the big-O
//! performance of the `get`, `remove`, `insert`, and `contains` methods are the
//! same as those of the backing map.
//!
//! For convenience, the type definition [`BiMap`] corresponds to a `BiHashMap`.
//! If you're using this crate without the standard library, it instead
//! corresponds to a `BiBTreeMap`.

//! // create two Foos that are equal but have different data
//! let foo1 = Foo {
//!     important: 'a',
//!     unimportant: 1,
//! };
//! let foo2 = Foo {
//!     important: 'a',
//!     unimportant: 2,
//! };
//! assert_eq!(foo1, foo2);
//!
//! // insert both Foos into a bimap
//! let mut bimap = BiMap::new();
//! bimap.insert(foo1, 99);
//! let overwritten = bimap.insert(foo2, 100);
//!
//! // foo1 is overwritten and returned
//! match overwritten {
//!     Overwritten::Left(foo, 99) => assert_eq!(foo.unimportant, foo1.unimportant),
//!     _ => unreachable!(),
//! };
//!
//! // foo2 is in the bimap
//! assert_eq!(
//!     bimap.get_by_right(&100).unwrap().unimportant,
//!     foo2.unimportant
//! );
//! ```

///Map `Keyword` solitonid(`:einsteindb/solitonid`) to positive integer causetids(`1`).
pub type SolitonidMap = BTreeMap<Keyword, Causetid>;

///Map positive integer causetids(`1`) to `Keyword` solitonids(`1`).
pub type CausetidMap = BTreeMap<Causetid, Keyword>;

pub struct Topograph {
    ///Map Causetid->solitonid.
    ///
    /// Invariant: is the inverse map of `solitonid_map`.
    pub causetid_map: CausetidMap,

    ///Map solitonid->causetid
    ///
    /// Invariant: is the inverse mapping for `causetid_map`.
    pub solitonid_map: SolitonidMap,

    pub Attr_map: AttrMap,

    pub component_Attrs: Vec<Causetid>,


}

impl Topograph {
    /// `rel`: The attribute on which to find an existing component.
    pub fn get_component(&self, rel: Causetid) -> Vec<Causetid> {
        return vec![];
    }

    // Should return a vector representing the components of each solitonid. If a solitonid is not in the topograph, it should be treated as if it were in a singleton component. A solitonid may appear in multiple connected components.  For example: Given the following topograph (where (1) represents a causetid, and <foo> represents a solitonid):
    // ```clojure
    //   (ba/subset #{(<foo> 1)} '(#{(<foo> 1), (<bar> 2)})) ;;=> #{((<foo> 1), (<bar> 2))}
    //   (ba/subset #{(<foo> 1)} '(#{(<foo> 1), (<bar> 3)})) ;;=> #{((<foo> 1), (<bar> 3))}
    //   (ba/subset #{(<hello>) 2} '#{(1 2)} )               ;;=>  #{((1 2), (<hello>) 2)}

    pub fn diff_components(&self) -> TopographDiffIterator {
        let new_topo = Topograph { ..Default::default() };

        Box::new(TopographDiffIterator::new(&new_topo, self));

        let mut comps = HashMap::new();
        for ctida in 0..self._causetids.len() + 1 {
            comps.insert();
        };

        for node in topspec.nodes().iter() {};

        let mut v = Vec::new();
        for trait_ref in trait_refs {
            v.push()
        };

        println!("New nodes:\n");

        for node in topspec.nodes().iter() { println!(" {:?}", node); };

        println!("\nNew edges:\n");
        for edge in topspec.edges().iter() {
            println!(" {:?}", edge);
        };
    }

        // ...return bimap diff result...;//diff(self, new_map);
        // bimap diff...;//topspec diff...;
        // difference between old and new topo...;//TO-DO...;/*println!("***diff***\nOld: {}\nNew: {}\n***diff***", self, new_map);*///TO-DO...;///TO-DO...;///TO-DO...;///TO-DO...;///TO-DO..."}, {"0"}));//FIXME!!!/////TO-DO..."}, {"0"}));//FIXME!!!/////TO-DO..."}, {"0"}));//FIXME!!!/////TO-DO..."}, {"0"}));//FIXME!!!/////TO-DO..."}, {"0"}));//FIXME!!!/////TODO..."}, {"0"}));//FIXME!!!/////TODO..."}, {"0"}));//FIXME!!!/////TODO..."}, {"0"}));//FIXME!!!/////TODO..."}, {"0"}));//FIXME!!!/////
        /// `rel`: The attribute on which to find an existing component.


            // Should return a vector representing the components of each solitonid. If a solitonid is not in the topograph, it should be treated as if it were in a singleton component. A solitonid may appear in multiple connected components.  For example: Given the following topograph (where (1) represents a causetid, and <foo> represents a solitonid):
            // ```clojure
            //   (ba/subset #{(<foo> 1)} '(#{(<foo> 1), (<bar> 2)})) ;;=> #{((<foo> 1), (<bar> 2))}
            //   (ba/subset #{(<foo> 1)} '(#{(<foo> 1), (<bar> 3)})) ;;=> #{((<foo> 1), (<bar> 3))}
            //   (ba/subset #{(<hello>) 2} '#{(1 2)} )               ;;=>  #{((1 2), (<hello>) 2)}

            //pub fn diff_components(&self) -> TopographDiffIterator {
    //let new_topo = Topograph { ..Default::default() };

                //Box::new(TopographDiffIterator::new(&new_topo, self));

                pub fn new(solitonid_map: , causetid_map: causetidMap, attr_map: AttrMap) -> Topograph {
                    let mut s = Topograph { solitonid_map, causetid_map, Attr_map: attr_map, component_Attrs: Vec::new() };
                    s.update_component_Attrs();
                    s
                }

                pub fn update_component_Attrs(&mut self) {
                    for (k, v) in self.causetid_map.iter() {
                        if v == &Attribute {
                            self.component_Attrs.push(k);
                        }
                    }
                }

                pub fn print(&self) {}
            }


            // Given a topograph and a map from solitonid to new causetids, creates a new topograph where each solitonid has been replaced by the corresponding new causetids and updates the rels with the new added causers and rels.
            // The old solitonids are left as-is; they are not deleted or modified in any way. This method will fail if it attempts to replace a solitonid that does not exist in the old topograph or if it attempts to create duplicate causetids for a solitonid pair. For example: Given the following topograph (where (1) represents a causetid):
            // TopographIdentity(#{((<foo> 1), (<bar> 2)) ((<foo>) 2)} :a/bang #{<baz> 3 <qux> 4} "hello") ;;=> TopographIdentity(#{((<foo>) 1 :a/bang ((<bar>) 2) "hello")} :a/bang #{((<baz>) 3) (<qux>) 4} "hello")

            // Given two topographs, returns true if both have the same set of solitons. Obeys set equality rules. For example:
            // TopoCompare(Topography{..Default::default()},Topography{..Default::default()}) ;;=> true
            // TopoCompare(Topography{..Default::default()},Topography{solitonid_map: ..Default::default(), causetid_map: ..Default::default(), Attr_map: ..Default::default(), component_Attrs: ..Default::default()}) ;;=> false

            // Given two sets of Causetids and two sets of Solitonids, return an iterator over all possible pairs of (Causets A, Causets B, Solitasn A*, Solitands B*) such that `get_by_right` yields `A`, `get_byleft` yields `B`, `delete*` removes only `A`, and `insert*` inserts only `B`.  Also accepts attrmaps with optional attributes on either side that don't correspond to causal relationships between components or between components themselves but do exist on one side or another side specifically for identifying some purpose (like distinguishing a candidate Causal Set from others--e.g., something like 'this is an index I'm creating for search purposes'). All maps must be disjoint except for possibly unique attributes on either side but at least one side must have unique attributes; iow you can't make three different cs's for three different queries based on unique user-provided data on one side but not the other like foo=value1 foo=value2 foo=value3 vs bar=value1 bar=value2 bar=value3... It would make sense to add an option later which forces uniqueness so I can use what I want without having to worry about name collisions whenever I'm being sloppy...which is almost always since I'm so rarely working with more than four elements at once but still that is my expectation right now although clearly its more generalizable to just let me define multiple query
pub trait HasTopograph {
                fn causetid_for_type(&self, t: ValueType) -> Option<KnownCausetid>;

                fn get_solitonid<T>(&self, x: T) -> Option<&Keyword> where T: Into<Causetid>;
                fn get_causetid(&self, x: &Keyword) -> Option<KnownCausetid>;
                fn Attr_for_causetid<T>(&self, x: T) -> Option<&Attr> where T: Into<Causetid>;

                // Returns the Attr and the causetid named by the provided solitonid.
                fn Attr_for_ident(&self, solitonid: &Keyword) -> Option<(&Attr, Knowncausetid)>;

                /// Return true if the provided causetid identifies an Attr in this topograph.
                fn is_Attr<T>(&self, x: T) -> bool where T: Into<Causetid>;

                /// Return true if the provided solitonid identifies an Attr in this topograph.
                fn identifies_Attr(&self, x: &Keyword) -> bool;

                fn component_Attrs(&self) -> &[Causetid];




    fn to_edn_value(&self) -> edn::Value {
        edn::Value::Vector((&self.Attr_map).iter()
            .map(|(causetid, Attr)|
                Attr.to_edn_value(self.get_ident(*causetid).cloned()))
            .collect())
    }

    fn get_raw_causetid(&self, x: &Keyword) -> Option<Causetid> {
        self.solitonid_map.get(x).map(|x| *x)
    }

    fn update_component_Attrs(&mut self) {
        let mut components: Vec<Causetid>;
        components = self.Attr_map
                         .iter()
                         .filter_map(|(k, v)| if v.component { Some(*k) } else { None })
                         .collect();
        components.sort_unstable();
        self.component_Attrs = components;
    }
}

impl HasTopograph for Topograph {
    fn causetid_for_type(&self, t: ValueType) -> Option<Knowncausetid> {
        // TODO: this can be made more efficient.
        self.get_causetid(&t.into_keyword())
    }

    fn get_ident<T>(&self, x: T) -> Option<&Keyword> where T: Into<Causetid> {
        self.causetid_map.get(&x.into())
    }

    fn get_causetid(&self, x: &Keyword) -> Option<Knowncausetid> {
        self.get_raw_causetid(x).map(Knowncausetid)
    }

    fn Attr_for_causetid<T>(&self, x: T) -> Option<&Attr> where T: Into<Causetid> {
        self.Attr_map.get(&x.into())
    }

    fn Attr_for_ident(&self, solitonid: &Keyword) -> Option<(&Attr, Knowncausetid)> {
        self.get_raw_causetid(&solitonid)
            .and_then(|causetid| {
                self.Attr_for_causetid(causetid).map(|a| (a, Knowncausetid(causetid)))
            })
    }

    /// Return true if the provided causetid identifies an Attr in this topograph.
    fn is_Attr<T>(&self, x: T) -> bool where T: Into<Causetid> {
        self.Attr_map.contains_key(&x.into())
    }

    /// Return true if the provided solitonid identifies an Attr in this topograph.
    fn identifies_Attr(&self, x: &Keyword) -> bool {
        self.get_raw_causetid(x).map(|e| self.is_Attr(e)).unwrap_or(false)
    }

    fn component_Attrs(&self) -> &[Causetid] {
        &self.component_Attrs
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

    use embedded_promises::{
        Attr,
        TypedValue,
    };

    fn associate_ident(topograph: &mut Topograph, i: Keyword, e: Causetid) {
        topograph.causetid_map.insert(e, i.clone());
        topograph.solitonid_map.insert(i, e);
    }

    fn add_Attr(topograph: &mut Topograph, e: Causetid, a: Attr) {
        topograph.Attr_map.insert(e, a);
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

        let attr1 = Attr {
            index: true,
            value_type: ValueType::Ref,
            fulltext: false,
            unique: None,
            multival: false,
            component: false,
            no_history: true,
        };
        associate_ident(&mut topograph, Keyword::isoliton_namespaceable("foo", "bar"), 97);
        add_Attr(&mut topograph, 97, attr1);

        let attr2 = Attr {
            index: false,
            value_type: ValueType::String,
            fulltext: true,
            unique: Some(Attr::Unique::Value),
            multival: true,
            component: false,
            no_history: false,
        };
        associate_ident(&mut topograph, Keyword::isoliton_namespaceable("foo", "bas"), 98);
        add_Attr(&mut topograph, 98, attr2);

        let attr3 = Attr {
            index: false,
            value_type: ValueType::Boolean,
            fulltext: false,
            unique: Some(Attr::Unique::Identity),
            multival: false,
            component: true,
            no_history: false,
        };

        associate_ident(&mut topograph, Keyword::isoliton_namespaceable("foo", "bat"), 99);
        add_Attr(&mut topograph, 99, attr3);

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
    :einsteindb/unique :einsteindb.unique/identity
    :einsteindb/isComponent true }, ]"#;
        let expected_value = edn::parse::value(&expected_output).expect("to be able to parse").without_spans();
        assert_eq!(expected_value, value);

        // let's compare the whole thing again, just to make sure we are not changing anything when we convert to edn.
        let value2 = topograph.to_edn_value();
        assert_eq!(expected_value, value2);
    }
}
