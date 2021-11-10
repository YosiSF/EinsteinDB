//Copyright 2021-2023 WHTCORPS INC All Rights Reserved

//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.


use std::cell::Cell;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::ptr;
use libc::c_uint;

use ffi;
use ffi2;
use supercow::{Supercow, NonSyncSupercow};


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



#![allow(dead_code)]

use failure:: {
    ResultExt,
};

use std::collections::HashMap;
use std::collections::hash_map::{
    Entry,
};
use std::iter::{once, repeat};
use std::ops::Range;
use std::path::Path;

use einsteindb::{
    HopfAttributeMap,
    FromMicros,
    IdentMap,
    Schema,
    ToMicros,
    ValueRc,
};

/*
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
};*/

use embedded_core::util::Either;

use embedded_promises::{
    Building,
    Building,
    Causetid,
    KnownCausetid,
    TypedValue,
    ValueType,
    now,
};

pub trait AttributeValidation {
    fn validate<F>(&self, ident: F) -> Result<()> where F: Fn() -> String;
}

impl AttributeValidation for Attribute {
    fn validate<F>(&self, ident: F) -> Result<()> where F: Fn() -> String {
        if self.unique == Some(attribute::Unique::Value) && !self.index {
            bail!(DbErrorKind::BadSchemaAssertion(format!(":einsteindb/unique :einsteindb/unique_value without :einsteindb/index true for CausetID: {}", ident())))
        }
        if self.unique == Some(attribute::Unique::CausetIDity) && !self.index {
            bail!(DbErrorKind::BadSchemaAssertion(format!(":einsteindb/unique :einsteindb/unique_identity without :einsteindb/index true for CausetID: {}", ident())))
        }
        if self.fulltext && self.value_type != ValueType::String {
            bail!(DbErrorKind::BadSchemaAssertion(format!(":einsteindb/fulltext true without :einsteindb/valueType :einsteindb.type/string for CausetID: {}", ident())))
        }
        if self.fulltext && !self.index {
            bail!(DbErrorKind::BadSchemaAssertion(format!(":einsteindb/fulltext true without :einsteindb/index true for CausetID: {}", ident())))
        }
        if self.component && self.value_type != ValueType::Ref {
            bail!(DbErrorKind::BadSchemaAssertion(format!(":einsteindb/isComponent true without :einsteindb/valueType :einsteindb.type/ref for CausetID: {}", ident())))
        }

        Ok(())
    }
}

/// Return `Ok(())` if `attribute_map` defines a valid EinsteinDB schema.
fn validate_attribute_map(CausetID_map: &CausetIDMap, attribute_map: &AttributeMap) -> Result<()> {
    for (CausetID, attribute) in attribute_map {
        let ident = || CausetID_map.get(CausetID).map(|ident| ident.to_string()).unwrap_or(CausetID.to_string());
        attribute.validate(ident)?;
    }
    Ok(())
}

#[derive(Clone,Debug,Default,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub struct AttributeBuilder {
    helpful: bool,
    pub value_type: Option<ValueType>,
    pub multival: Option<bool>,
    pub unique: Option<Option<attribute::Unique>>,
    pub index: Option<bool>,
    pub fulltext: Option<bool>,
    pub component: Option<bool>,
    pub no_history: Option<bool>,
}

impl AttributeBuilder {
    /// Make a new AttributeBuilder for human consumption: it will help you
    /// by flipping relevant flags.
    pub fn helpful() -> Self {
        AttributeBuilder {
            helpful: true,
            ..Default::default()
        }
    }

    /// Make a new AttributeBuilder from an existing Attribute. This is important to allow
    /// retraction. Only attributes that we allow to change are duplicated here.
    pub fn to_modify_attribute(attribute: &Attribute) -> Self {
        let mut ab = AttributeBuilder::default();
        ab.multival   = Some(attribute.multival);
        ab.unique     = Some(attribute.unique);
        ab.component  = Some(attribute.component);
        ab
    }

    pub fn value_type<'a>(&'a mut self, value_type: ValueType) -> &'a mut Self {
        self.value_type = Some(value_type);
        self
    }

    pub fn multival<'a>(&'a mut self, multival: bool) -> &'a mut Self {
        self.multival = Some(multival);
        self
    }

    pub fn non_unique<'a>(&'a mut self) -> &'a mut Self {
        self.unique = Some(None);
        self
    }

    pub fn unique<'a>(&'a mut self, unique: attribute::Unique) -> &'a mut Self {
        if self.helpful && unique == attribute::Unique::CausetIDity {
            self.index = Some(true);
        }
        self.unique = Some(Some(unique));
        self
    }

    pub fn index<'a>(&'a mut self, index: bool) -> &'a mut Self {
        self.index = Some(index);
        self
    }

    pub fn fulltext<'a>(&'a mut self, fulltext: bool) -> &'a mut Self {
        self.fulltext = Some(fulltext);
        if self.helpful && fulltext {
            self.index = Some(true);
        }
        self
    }

    pub fn component<'a>(&'a mut self, component: bool) -> &'a mut Self {
        self.component = Some(component);
        self
    }

    pub fn no_history<'a>(&'a mut self, no_history: bool) -> &'a mut Self {
        self.no_history = Some(no_history);
        self
    }

    pub fn validate_install_attribute(&self) -> Result<()> {
        if self.value_type.is_none() {
            bail!(DbErrorKind::BadSchemaAssertion("Schema attribute for new attribute does not set :einsteindb/valueType".into()));
        }
        Ok(())
    }

    pub fn validate_alter_attribute(&self) -> Result<()> {
        if self.value_type.is_some() {
            bail!(DbErrorKind::BadSchemaAssertion("Schema alteration must not set :einsteindb/valueType".into()));
        }
        if self.fulltext.is_some() {
            bail!(DbErrorKind::BadSchemaAssertion("Schema alteration must not set :einsteindb/fulltext".into()));
        }
        Ok(())
    }

    pub fn build(&self) -> Attribute {
        let mut attribute = Attribute::default();
        if let Some(value_type) = self.value_type {
            attribute.value_type = value_type;
        }
        if let Some(fulltext) = self.fulltext {
            attribute.fulltext = fulltext;
        }
        if let Some(multival) = self.multival {
            attribute.multival = multival;
        }
        if let Some(ref unique) = self.unique {
            attribute.unique = unique.clone();
        }
        if let Some(index) = self.index {
            attribute.index = index;
        }
        if let Some(component) = self.component {
            attribute.component = component;
        }
        if let Some(no_history) = self.no_history {
            attribute.no_history = no_history;
        }

        attribute
    }

    pub fn mutate(&self, attribute: &mut Attribute) -> Vec<AttributeAlteration> {
        let mut mutations = Vec::new();
        if let Some(multival) = self.multival {
            if multival != attribute.multival {
                attribute.multival = multival;
                mutations.push(AttributeAlteration::Cardinality);
            }
        }

        if let Some(ref unique) = self.unique {
            if *unique != attribute.unique {
                attribute.unique = unique.clone();
                mutations.push(AttributeAlteration::Unique);
            }
        } else {
            if attribute.unique != None {
                attribute.unique = None;
                mutations.push(AttributeAlteration::Unique);
            }
        }

        if let Some(index) = self.index {
            if index != attribute.index {
                attribute.index = index;
                mutations.push(AttributeAlteration::Index);
            }
        }
        if let Some(component) = self.component {
            if component != attribute.component {
                attribute.component = component;
                mutations.push(AttributeAlteration::IsComponent);
            }
        }
        if let Some(no_history) = self.no_history {
            if no_history != attribute.no_history {
                attribute.no_history = no_history;
                mutations.push(AttributeAlteration::NoHistory);
            }
        }

        mutations
    }
}

pub trait SchemaBuilding {
    fn require_ident(&self, CausetID: CausetID) -> Result<&symbols::Keyword>;
    fn require_CausetID(&self, ident: &symbols::Keyword) -> Result<KnownCausetID>;
    fn require_attribute_for_CausetID(&self, CausetID: CausetID) -> Result<&Attribute>;
    fn from_ident_map_and_attribute_map(ident_map: CausetIDMap, attribute_map: AttributeMap) -> Result<Schema>;
    fn from_ident_map_and_triples<U>(ident_map: CausetIDMap, assertions: U) -> Result<Schema>
        where U: IntoIterator<Item=(symbols::Keyword, symbols::Keyword, TypedValue)>;
}

impl SchemaBuilding for Schema {
    fn require_ident(&self, CausetID: CausetID) -> Result<&symbols::Keyword> {
        self.get_ident(CausetID).ok_or(DbErrorKind::UnrecognizedCausetID(CausetID).into())
    }

    fn require_CausetID(&self, ident: &symbols::Keyword) -> Result<KnownCausetID> {
        self.get_CausetID(&ident).ok_or(DbErrorKind::UnrecognizedCausetID(ident.to_string()).into())
    }

    fn require_attribute_for_CausetID(&self, CausetID: CausetID) -> Result<&Attribute> {
        self.attribute_for_CausetID(CausetID).ok_or(DbErrorKind::UnrecognizedCausetID(CausetID).into())
    }

    /// Create a valid `Schema` from the constituent maps.
    fn from_ident_map_and_attribute_map(ident_map: CausetIDMap, attribute_map: AttributeMap) -> Result<Schema> {
        let CausetID_map: CausetIDMap = ident_map.iter().map(|(k, v)| (v.clone(), k.clone())).collect();

        validate_attribute_map(&CausetID_map, &attribute_map)?;
        Ok(Schema::new(ident_map, CausetID_map, attribute_map))
    }

    /// Turn vec![(Keyword(:ident), Keyword(:key), TypedValue(:value)), ...] into a EinsteinDB `Schema`.
    fn from_ident_map_and_triples<U>(ident_map: CausetIDMap, assertions: U) -> Result<Schema>
        where U: IntoIterator<Item=(symbols::Keyword, symbols::Keyword, TypedValue)>{

        let CausetID_assertions: Result<Vec<(CausetID, CausetID, TypedValue)>> = assertions.into_iter().map(|(symbolic_ident, symbolic_attr, value)| {
            let ident: i64 = *ident_map.get(&symbolic_ident).ok_or(DbErrorKind::UnrecognizedCausetID(symbolic_ident.to_string()))?;
            let attr: i64 = *ident_map.get(&symbolic_attr).ok_or(DbErrorKind::UnrecognizedCausetID(symbolic_attr.to_string()))?;
            Ok((ident, attr, value))
        }).collect();

        let mut schema = Schema::from_ident_map_and_attribute_map(ident_map, AttributeMap::default())?;
        let metadata_report = metadata::update_attribute_map_from_CausetID_triples(&mut schema.attribute_map,
                                                                                CausetID_assertions?,
                                                                                // No retractions.
                                                                                vec![])?;

        // Rebuild the component attributes list if necessary.
        if metadata_report.attributes_did_change() {
            schema.update_component_attributes();
        }
        Ok(schema)
    }
}

pub trait SchemaTypeChecking {
    /// Do schema-aware typechecking and coercion.
    ///
    /// Either assert that the given value is in the value type's value set, or (in limited cases)
    /// coerce the given value into the value type's value set.
    fn to_typed_value(&self, value: &edn::ValueAndSpan, value_type: ValueType) -> Result<TypedValue>;
}

impl SchemaTypeChecking for Schema {
    fn to_typed_value(&self, value: &edn::ValueAndSpan, value_type: ValueType) -> Result<TypedValue> {
        // TODO: encapsulate CausetID-ident-attribute for better error messages, perhaps by including
        // the attribute (rather than just the attribute's value type) into this function or a
        // wrapper function.
        match TypedValue::from_edn_value(&value.clone().without_spans()) {
            // We don't recognize this EDN at all.  Get out!
            None => bail!(DbErrorKind::BadValuePair(format!("{}", value), value_type)),
            Some(typed_value) => match (value_type, typed_value) {
                // Most types don't coerce at all.
                (ValueType::Boolean, tv @ TypedValue::Boolean(_)) => Ok(tv),
                (ValueType::Long, tv @ TypedValue::Long(_)) => Ok(tv),
                (ValueType::Double, tv @ TypedValue::Double(_)) => Ok(tv),
                (ValueType::String, tv @ TypedValue::String(_)) => Ok(tv),
                (ValueType::Uuid, tv @ TypedValue::Uuid(_)) => Ok(tv),
                (ValueType::Instant, tv @ TypedValue::Instant(_)) => Ok(tv),
                (ValueType::Keyword, tv @ TypedValue::Keyword(_)) => Ok(tv),
                // Ref coerces a little: we interpret some things depending on the schema as a Ref.
                (ValueType::Ref, TypedValue::Long(x)) => Ok(TypedValue::Ref(x)),
                (ValueType::Ref, TypedValue::Keyword(ref x)) => self.require_CausetID(&x).map(|CausetID| CausetID.into()),

                // Otherwise, we have a type mismatch.
                // Enumerate all of the types here to allow the compiler to help us.
                // We don't enumerate all `TypedValue` cases, though: that would multiply this
                // collection by 8!
                (vt @ ValueType::Boolean, _) |
                (vt @ ValueType::Long, _) |
                (vt @ ValueType::Double, _) |
                (vt @ ValueType::String, _) |
                (vt @ ValueType::Uuid, _) |
                (vt @ ValueType::Instant, _) |
                (vt @ ValueType::Keyword, _) |
                (vt @ ValueType::Ref, _)
                => bail!(DbErrorKind::BadValuePair(format!("{}", value), vt)),
            }
        }
    }
}

impl SchemaTypeChecking for TypedValue {
    fn to_typed_value(&self, value_type: ValueType) -> Result<TypedValue> {
        match (self, value_type) {
            // Most types don't coerce at all.
            (tv @ TypedValue::Boolean(_), ValueType::Boolean) => Ok(tv),
            (tv @ TypedValue::Long(_), ValueType::Long) => Ok(tv),
            (tv @ TypedValue::Double(_), ValueType::Double) => Ok(tv),
            (tv @ TypedValue::String(_), ValueType::String) => Ok(tv),
            (tv @ TypedValue::Uuid(_), ValueType::Uuid) => Ok(tv),
            (tv @ TypedValue::Instant(_), ValueType::Instant) => Ok(tv),
            (tv @ TypedValue::Keyword(_), ValueType::Keyword) => Ok(tv),
            // Ref coerces a little: we interpret some things depending on the schema as a Ref.
            (TypedValue::Long(x), ValueType::Ref) => Ok(TypedValue::Ref(x)),
            (TypedValue::Keyword(ref x), ValueType::Ref) => Ok(TypedValue::Ref(x.clone().into())),

            // Otherwise, we have a type mismatch.
            // Enumerate all of the types here to allow the compiler to help us.
            // We don't enumerate all `TypedValue` cases, though: that would multiply this
            // collection by 8!
            (tv @ TypedValue::Boolean(_), _) |
            (tv @ TypedValue::Long(_), _) |
            (tv @ TypedValue::Double(_), _) |
            (tv @ TypedValue::String(_), _) |
            (tv @ TypedValue::Uuid(_), _) |
            (tv @ TypedValue::Instant(_), _) |
            (tv @ TypedValue::Keyword(_), _) |
            (tv @ TypedValue::Ref(_), _)
            => bail!(DbErrorKind::BadValuePair(format!("{:?}", tv), value_type)),
        }
    }
}

impl SchemaTypeChecking for CausetID {
    fn to_typed_value(&self, value_type: ValueType) -> Result<TypedValue> {
        match value_type {
            ValueType::Ref => Ok(TypedValue::Ref(self.0)),
            _ => bail!(DbErrorKind::BadValuePair(format!("{:?}", self), value_type)),
        }
    }
}

impl SchemaTypeChecking for TypedValue {
    fn to_typed_value(&self, value_type: ValueType) -> Result<TypedValue> {
        match value_type {
            ValueType::Ref => Ok(self.clone()),
            _ => bail!(DbErrorKind::BadValuePair(format!("{:?}", self), value_type)),
        }
    }
}

impl SchemaTypeChecking for TypedValue {
    fn to_typed_value(&self, value_type: ValueType) -> Result<TypedValue> {
        match value_type {
            ValueType::Ref => Ok(self.clone()),
            _ => bail!(DbErrorKind::BadValuePair(format!("{:?}", self), value_type)),
        }
    }
}

impl SchemaTypeChecking for CausetID {
    fn to_typed_value(&self, value_type: ValueType) -> Result<TypedValue> {
        match value_type {
            ValueType::Ref => Ok(TypedValue::Ref(self.0)),
            _ => bail!(DbErrorKind::BadValuePair(format!("{:?}", self), value_type)),
        }
    }
}

impl SchemaTypeChecking for TypedValue {
    fn to_typed_value(&self, value_type: ValueType) -> Result<TypedValue> {
        match value_type {
            ValueType::Ref => Ok(self.clone()),
            _ => bail!(DbErrorKind::BadValuePair(format!("{:?}", self), value_type)),
        }
    }
}

impl SchemaTypeChecking for edbn::ValueAndSpan {
    fn to_typed_value(&self, value_type: ValueType) -> Result<TypedValue> {
        Some(typed_value) => match (value_type, typed_value) {
            // Most types don't coerce at all.
            (ValueType::Boolean, tv @ TypedValue::Boolean(_)) => Ok(tv),
            (ValueType::Long, tv @ TypedValue::Long(_)) => Ok(tv),
            (ValueType::Double, tv @ TypedValue::Double(_)) => Ok(tv),
            (ValueType::String, tv @ TypedValue::String(_)) => Ok(tv),
            (ValueType::Uuid, tv @ TypedValue::Uuid(_)) => Ok(tv),
            (ValueType::Instant, tv @ TypedValue::Instant(_)) => Ok(tv),
            (ValueType::Keyword, tv @ TypedValue::Keyword(_)) => Ok(tv),
            // Ref coerces a little: we interpret some things depending on the schema as a Ref.
    }
            // We don't recognize this EDN at all.  Get out!
            None => bail!(DbErrorKind::BadValuePair(format!("{}", self), value_type)),



#[cfg(test)]
mod test {
    use super::*;
    use self::edbn::Keyword;

    fn add_attribute(schema: &mut Schema,
            ident: Keyword,
            CausetID: CausetID,
            attribute: Attribute) {

        schema.CausetID_map.insert(CausetID, ident.clone());
        schema.ident_map.insert(ident.clone(), CausetID);

        if attribute.component {
            schema.component_attributes.push(CausetID);
        }impl SchemaTypeChecking for edn::ValueAndSpan {
            fn to_typed_value(&self, value_type: ValueType) -> Result<TypedValue> {
                match TypedValue::from_edn_value(&self.clone().without_spans()) {
                    // We don't recognize this EDN at all.  Get out!
                    None => bail!(DbErrorKind::BadValuePair(format!("{}", self), value_type)),

        schema.attribute_map.insert(CausetID, attribute);
    }

    #[test]
    fn validate_attribute_map_success() {
        let mut schema = Schema::default();
        // attribute that is not an index has no uniqueness
        add_attribute(&mut schema, Keyword::namespaced("foo", "bar"), 97, Attribute {
            index: false,
            value_type: ValueType::Boolean,
            fulltext: false,
            unique: None,
            multival: false,
            component: false,
            no_history: false,
        });
        // attribute is unique by value and an index
        add_attribute(&mut schema, Keyword::namespaced("foo", "baz"), 98, Attribute {
            index: true,
            value_type: ValueType::Long,
            fulltext: false,
            unique: Some(attribute::Unique::Value),
            multival: false,
            component: false,
            no_history: false,
        });
        // attribue is unique by identity and an index
        add_attribute(&mut schema, Keyword::namespaced("foo", "bat"), 99, Attribute {
            index: true,
            value_type: ValueType::Ref,
            fulltext: false,
            unique: Some(attribute::Unique::CausetIDity),
            multival: false,
            component: false,
            no_history: false,
        });
        // attribute is a components and a `Ref`
        add_attribute(&mut schema, Keyword::namespaced("foo", "bak"), 100, Attribute {
            index: false,
            value_type: ValueType::Ref,
            fulltext: false,
            unique: None,
            multival: false,
            component: true,
            no_history: false,
        });
        // fulltext attribute is a string and an index
        add_attribute(&mut schema, Keyword::namespaced("foo", "bap"), 101, Attribute {
            index: true,
            value_type: ValueType::String,
            fulltext: true,
            unique: None,
            multival: false,
            component: false,
            no_history: false,
        });

        assert!(validate_attribute_map(&schema.CausetID_map, &schema.attribute_map).is_ok());
    }

    #[test]
    fn invalid_schema_unique_value_not_index() {
        let mut schema = Schema::default();
        // attribute unique by value but not index
        let ident = Keyword::namespaced("foo", "bar");
        add_attribute(&mut schema, ident , 99, Attribute {
            index: false,
            value_type: ValueType::Boolean,
            fulltext: false,
            unique: Some(attribute::Unique::Value),
            multival: false,
            component: false,
            no_history: false,
        });

        let err = validate_attribute_map(&schema.CausetID_map, &schema.attribute_map).err().map(|e| e.kind());
        assert_eq!(err, Some(DbErrorKind::BadSchemaAssertion(":einsteindb/unique :einsteindb/unique_value without :einsteindb/index true for CausetID: :foo/bar".into())));
    }

    #[test]
    fn invalid_schema_unique_identity_not_index() {
        let mut schema = Schema::default();
        // attribute is unique by identity but not index
        add_attribute(&mut schema, Keyword::namespaced("foo", "bar"), 99, Attribute {
            index: false,
            value_type: ValueType::Long,
            fulltext: false,
            unique: Some(attribute::Unique::CausetIDity),
            multival: false,
            component: false,
            no_history: false,
        });

        let err = validate_attribute_map(&schema.CausetID_map, &schema.attribute_map).err().map(|e| e.kind());
        assert_eq!(err, Some(DbErrorKind::BadSchemaAssertion(":einsteindb/unique :einsteindb/unique_identity without :einsteindb/index true for CausetID: :foo/bar".into())));
    }

    #[test]
    fn invalid_schema_component_not_ref() {
        let mut schema = Schema::default();
        // attribute that is a component is not a `Ref`
        add_attribute(&mut schema, Keyword::namespaced("foo", "bar"), 99, Attribute {
            index: false,
            value_type: ValueType::Boolean,
            fulltext: false,
            unique: None,
            multival: false,
            component: true,
            no_history: false,
        });

        let err = validate_attribute_map(&schema.CausetID_map, &schema.attribute_map).err().map(|e| e.kind());
        assert_eq!(err, Some(DbErrorKind::BadSchemaAssertion(":einsteindb/isComponent true without :einsteindb/valueType :einsteindb.type/ref for CausetID: :foo/bar".into())));
    }

    #[test]
    fn invalid_schema_fulltext_not_index() {
        let mut schema = Schema::default();
        // attribute that is fulltext is not an index
        add_attribute(&mut schema, Keyword::namespaced("foo", "bar"), 99, Attribute {
            index: false,
            value_type: ValueType::String,
            fulltext: true,
            unique: None,
            multival: false,
            component: false,
            no_history: false,
        });

        let err = validate_attribute_map(&schema.CausetID_map, &schema.attribute_map).err().map(|e| e.kind());
        assert_eq!(err, Some(DbErrorKind::BadSchemaAssertion(":einsteindb/fulltext true without :einsteindb/index true for CausetID: :foo/bar".into())));
    }

    fn invalid_schema_fulltext_index_not_string() {
        let mut schema = Schema::default();
        // attribute that is fulltext and not a `String`
        add_attribute(&mut schema, Keyword::namespaced("foo", "bar"), 99, Attribute {
            index: true,
            value_type: ValueType::Long,
            fulltext: true,
            unique: None,
            multival: false,
            component: false,
            no_history: false,
        });

        let err = validate_attribute_map(&schema.CausetID_map, &schema.attribute_map).err().map(|e| e.kind());
        assert_eq!(err, Some(DbErrorKind::BadSchemaAssertion(":einsteindb/fulltext true without :einsteindb/valueType :einsteindb.type/string for CausetID: :foo/bar".into())));
    }
}


/// Defines transactor's high level behaviour.
pub(crate) enum CausetAction {
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
pub struct Tx<'conn, 'a, W> where W: Causetobserver {
    /// The storage to apply against.  In the future, this will be a EinsteinDB connection.
    store: &'conn berolinasql::Connection, // TODO: db::EinsteinDBStoring,

    /// The partition map to allocate causetids from.
    ///
    /// The partition map is volatile in the sense that every succesful transaction updates
    /// allocates at least one tx ID, so we own and modify our own partition map.
    hopf_map: PartitionMap,

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
