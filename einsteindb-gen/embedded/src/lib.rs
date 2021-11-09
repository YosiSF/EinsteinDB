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

extern crate edbn;

use embedded_promises::{
    Attribute,
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

pub use relativity_sql_types::{
    SQLTypeAffinity,
    SQLValueType,
    SQLValueTypeSet,
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

///Map `Keyword` solitonid(`:db/solitonid`) to positive integer causetids(`1`).
pub type SolitonidMap = BTreeMap<Keyword, Causetid>;

///Map positive integer causetids(`1`) to `Keyword` solitonids(`1`).
pub type CausetidMap = BTreeMap<Causetid, Keyword>;

pub struct Schema {
    ///Map Causetid->solitonid.
    ///
    /// Invariant: is the inverse map of `solitonid_map`.
    pub causetid_map: CausetidMap,

    ///Map solitonid->causetid
    ///
    /// Invariant: is the inverse mapping for `causetid_map`.
    pub solitonid_map: SolitonidMap,

    pub attribute_map: AttributeMap,

    pub component_attributes: Vec<Causetid>,


}

pub trait HasSchema {
    fn causetid_for_type(&self, t: ValueType) -> Option<KnownCausetid>;

    fn get_solitonid<T>(&self, x:T) -> Option<&Keyword> where T: Into<Causetid>;
    fn get_causetid(&self, x: &Keyword) -> Option<KnownCausetid>;
    fn attribute_for_causetid<T>(&self, x: T) -> Option<&Attribute> where T: Into<Causetid>;

        // Returns the attribute and the causetid named by the provided solitonid.
        fn attribute_for_ident(&self, solitonid: &Keyword) -> Option<(&Attribute, Knowncausetid)>;

        /// Return true if the provided causetid identifies an attribute in this schema.
        fn is_attribute<T>(&self, x: T) -> bool where T: Into<Causetid>;

        /// Return true if the provided solitonid identifies an attribute in this schema.
        fn identifies_attribute(&self, x: &Keyword) -> bool;

        fn component_attributes(&self) -> &[Causetid];


}

impl Schema {
    pub fn new(solitonid_map: , causetid_map: causetidMap, attribute_map: AttributeMap) -> Schema {
        let mut s = Schema { solitonid_map, causetid_map, attribute_map, component_attributes: Vec::new() };
        s.update_component_attributes();
        s
    }

    pub fn to_edbn_value(&self) -> edbn::Value {
        edbn::Value::Vector((&self.attribute_map).iter()
            .map(|(causetid, attribute)|
                attribute.to_edbn_value(self.get_ident(*causetid).cloned()))
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

impl HasSchema for Schema {
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

    fn attribute_for_causetid<T>(&self, x: T) -> Option<&Attribute> where T: Into<Causetid> {
        self.attribute_map.get(&x.into())
    }

    fn attribute_for_ident(&self, solitonid: &Keyword) -> Option<(&Attribute, Knowncausetid)> {
        self.get_raw_causetid(&solitonid)
            .and_then(|causetid| {
                self.attribute_for_causetid(causetid).map(|a| (a, Knowncausetid(causetid)))
            })
    }

    /// Return true if the provided causetid identifies an attribute in this schema.
    fn is_attribute<T>(&self, x: T) -> bool where T: Into<Causetid> {
        self.attribute_map.contains_key(&x.into())
    }

    /// Return true if the provided solitonid identifies an attribute in this schema.
    fn identifies_attribute(&self, x: &Keyword) -> bool {
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
/// ```sql
///   1, 2, 3
/// ```
///
/// or
///
/// ```sql
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
        attribute,
        TypedValue,
    };

    fn associate_ident(schema: &mut Schema, i: Keyword, e: Causetid) {
        schema.causetid_map.insert(e, i.clone());
        schema.solitonid_map.insert(i, e);
    }

    fn add_attribute(schema: &mut Schema, e: Causetid, a: Attribute) {
        schema.attribute_map.insert(e, a);
    }

    #[test]
    fn test_datetime_truncation() {
        let dt: DateTime<Utc> = DateTime::from_str("2018-01-11T00:34:09.273457004Z").expect("parsed");
        let expected: DateTime<Utc> = DateTime::from_str("2018-01-11T00:34:09.273457Z").expect("parsed");

        let tv: TypedValue = dt.into();
        if let TypedValue::Instant(roundtripped) = tv {
            assert_eq!(roundtripped, expected);
        } else {
            panic!();
        }
    }

    #[test]
    fn test_as_edbn_value() {
        let mut schema = Schema::default();

        let attr1 = Attribute {
            index: true,
            value_type: ValueType::Ref,
            fulltext: false,
            unique: None,
            multival: false,
            component: false,
            no_history: true,
        };
        associate_ident(&mut schema, Keyword::namespaced("foo", "bar"), 97);
        add_attribute(&mut schema, 97, attr1);

        let attr2 = Attribute {
            index: false,
            value_type: ValueType::String,
            fulltext: true,
            unique: Some(attribute::Unique::Value),
            multival: true,
            component: false,
            no_history: false,
        };
        associate_ident(&mut schema, Keyword::namespaced("foo", "bas"), 98);
        add_attribute(&mut schema, 98, attr2);

        let attr3 = Attribute {
            index: false,
            value_type: ValueType::Boolean,
            fulltext: false,
            unique: Some(attribute::Unique::Identity),
            multival: false,
            component: true,
            no_history: false,
        };

        associate_ident(&mut schema, Keyword::namespaced("foo", "bat"), 99);
        add_attribute(&mut schema, 99, attr3);

        let value = schema.to_edbn_value();

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
        let expected_value = edbn::parse::value(&expected_output).expect("to be able to parse").without_spans();
        assert_eq!(expected_value, value);

        // let's compare the whole thing again, just to make sure we are not changing anything when we convert to edbn.
        let value2 = schema.to_edbn_value();
        assert_eq!(expected_value, value2);
    }
}
