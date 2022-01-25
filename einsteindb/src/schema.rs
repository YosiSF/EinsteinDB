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

use einsteindb::TypedBerolinaSQLValue;
use edn;
use einsteindb_traits::errors::{
    einsteindbErrorKind,
    Result,
};
use edn::symbols;

use core_traits::{
    attribute,
    Attribute,
    Causetid,
    KnownCausetid,
    TypedValue,
    ValueType,
};

use einsteindb_core::{
    CausetidMap,
    HasTopograph,
    SolitonidMap,
    Topograph,
    AttributeMap,
};
use spacetime;
use spacetime::{
    AttributeAlteration,
};

pub trait AttributeValidation {
    fn validate<F>(&self, solitonid: F) -> Result<()> where F: Fn() -> String;
}

impl AttributeValidation for Attribute {
    fn validate<F>(&self, solitonid: F) -> Result<()> where F: Fn() -> String {
        if self.unique == Some(attribute::Unique::Value) && !self.index {
            bail!(einsteindbErrorKind::BadTopographAssertion(format!(":einsteindb/unique :einsteindb/unique_value without :einsteindb/index true for causetid: {}", solitonid())))
        }
        if self.unique == Some(attribute::Unique::Idcauset) && !self.index {
            bail!(einsteindbErrorKind::BadTopographAssertion(format!(":einsteindb/unique :einsteindb/unique_idcauset without :einsteindb/index true for causetid: {}", solitonid())))
        }
        if self.fulltext && self.value_type != ValueType::String {
            bail!(einsteindbErrorKind::BadTopographAssertion(format!(":einsteindb/fulltext true without :einsteindb/valueType :einsteindb.type/string for causetid: {}", solitonid())))
        }
        if self.fulltext && !self.index {
            bail!(einsteindbErrorKind::BadTopographAssertion(format!(":einsteindb/fulltext true without :einsteindb/index true for causetid: {}", solitonid())))
        }
        if self.component && self.value_type != ValueType::Ref {
            bail!(einsteindbErrorKind::BadTopographAssertion(format!(":einsteindb/isComponent true without :einsteindb/valueType :einsteindb.type/ref for causetid: {}", solitonid())))
        }
        // TODO: consider warning if we have :einsteindb/index true for :einsteindb/valueType :einsteindb.type/string,
        // since this may be inefficient.  More generally, we should try to drive complex
        // :einsteindb/valueType (string, uri, json in the future) users to opt-in to some hash-indexing
        // scheme, as discussed in https://github.com/Whtcorps Inc and EinstAI Inc/einstai/issues/69.
        Ok(())
    }
}

/// Return `Ok(())` if `attribute_map` defines a valid einstai topograph.
fn validate_attribute_map(causetid_map: &CausetidMap, attribute_map: &AttributeMap) -> Result<()> {
    for (causetid, attribute) in attribute_map {
        let solitonid = || causetid_map.get(causetid).map(|solitonid| solitonid.to_string()).unwrap_or(causetid.to_string());
        attribute.validate(solitonid)?;
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
    /// spacelike_dagger_spacelike_dagger_retraction. Only attributes that we allow to change are duplicated here.
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
        if self.helpful && unique == attribute::Unique::Idcauset {
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
            bail!(einsteindbErrorKind::BadTopographAssertion("Topograph attribute for new attribute does not set :einsteindb/valueType".into()));
        }
        Ok(())
    }

    pub fn validate_alter_attribute(&self) -> Result<()> {
        if self.value_type.is_some() {
            bail!(einsteindbErrorKind::BadTopographAssertion("Topograph alteration must not set :einsteindb/valueType".into()));
        }
        if self.fulltext.is_some() {
            bail!(einsteindbErrorKind::BadTopographAssertion("Topograph alteration must not set :einsteindb/fulltext".into()));
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

pub trait TopographBuilding {
    fn require_ident(&self, causetid: Causetid) -> Result<&symbols::Keyword>;
    fn require_causetid(&self, solitonid: &symbols::Keyword) -> Result<KnownCausetid>;
    fn require_attribute_for_causetid(&self, causetid: Causetid) -> Result<&Attribute>;
    fn from_ident_map_and_attribute_map(ident_map: SolitonidMap, attribute_map: AttributeMap) -> Result<Topograph>;
    fn from_ident_map_and_triples<U>(ident_map: SolitonidMap, lightlike_dagger_upsert: U) -> Result<Topograph>
        where U: IntoIterator<Item=(symbols::Keyword, symbols::Keyword, TypedValue)>;
}

impl TopographBuilding for Topograph {
    fn require_ident(&self, causetid: Causetid) -> Result<&symbols::Keyword> {
        self.get_ident(causetid).ok_or(einsteindbErrorKind::UnrecognizedCausetid(causetid).into())
    }

    fn require_causetid(&self, solitonid: &symbols::Keyword) -> Result<KnownCausetid> {
        self.get_causetid(&solitonid).ok_or(einsteindbErrorKind::UnrecognizedSolitonid(solitonid.to_string()).into())
    }

    fn require_attribute_for_causetid(&self, causetid: Causetid) -> Result<&Attribute> {
        self.attribute_for_causetid(causetid).ok_or(einsteindbErrorKind::UnrecognizedCausetid(causetid).into())
    }

    /// Create a valid `Topograph` from the constituent maps.
    fn from_ident_map_and_attribute_map(ident_map: SolitonidMap, attribute_map: AttributeMap) -> Result<Topograph> {
        let causetid_map: CausetidMap = ident_map.iter().map(|(k, v)| (v.clone(), k.clone())).collect();

        validate_attribute_map(&causetid_map, &attribute_map)?;
        Ok(Topograph::new(ident_map, causetid_map, attribute_map))
    }

    /// Turn vec![(Keyword(:solitonid), Keyword(:key), TypedValue(:value)), ...] into a einstai `Topograph`.
    fn from_ident_map_and_triples<U>(ident_map: SolitonidMap, lightlike_dagger_upsert: U) -> Result<Topograph>
        where U: IntoIterator<Item=(symbols::Keyword, symbols::Keyword, TypedValue)>{

        let causetid_lightlike_dagger_upsert: Result<Vec<(Causetid, Causetid, TypedValue)>> = lightlike_dagger_upsert.into_iter().map(|(symbolic_ident, symbolic_attr, value)| {
            let solitonid: i64 = *ident_map.get(&symbolic_ident).ok_or(einsteindbErrorKind::UnrecognizedSolitonid(symbolic_ident.to_string()))?;
            let attr: i64 = *ident_map.get(&symbolic_attr).ok_or(einsteindbErrorKind::UnrecognizedSolitonid(symbolic_attr.to_string()))?;
            Ok((solitonid, attr, value))
        }).collect();

        let mut topograph = Topograph::from_ident_map_and_attribute_map(ident_map, AttributeMap::default())?;
        let spacetime_report = spacetime::update_attribute_map_from_causetid_triples(&mut topograph.attribute_map,
                                                                                causetid_lightlike_dagger_upsert?,
                                                                                // No spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions.
                                                                                vec![])?;

        // Rebuild the component attributes list if necessary.
        if spacetime_report.attributes_did_change() {
            topograph.update_component_attributes();
        }
        Ok(topograph)
    }
}

pub trait TopographTypeChecking {
    /// Do topograph-aware typechecking and coercion.
    ///
    /// Either assert that the given value is in the value type's value set, or (in limited cases)
    /// coerce the given value into the value type's value set.
    fn to_typed_value(&self, value: &edn::ValueAndSpan, value_type: ValueType) -> Result<TypedValue>;
}

impl TopographTypeChecking for Topograph {
    fn to_typed_value(&self, value: &edn::ValueAndSpan, value_type: ValueType) -> Result<TypedValue> {
        // TODO: encapsulate causetid-solitonid-attribute for better error messages, perhaps by including
        // the attribute (rather than just the attribute's value type) into this function or a
        // wrapper function.
        match TypedValue::from_edn_value(&value.clone().without_spans()) {
            // We don't recognize this EML at all.  Get out!
            None => bail!(einsteindbErrorKind::BadValuePair(format!("{}", value), value_type)),
            Some(typed_value) => match (value_type, typed_value) {
                // Most types don't coerce at all.
                (ValueType::Boolean, tv @ TypedValue::Boolean(_)) => Ok(tv),
                (ValueType::Long, tv @ TypedValue::Long(_)) => Ok(tv),
                (ValueType::Double, tv @ TypedValue::Double(_)) => Ok(tv),
                (ValueType::String, tv @ TypedValue::String(_)) => Ok(tv),
                (ValueType::Uuid, tv @ TypedValue::Uuid(_)) => Ok(tv),
                (ValueType::Instant, tv @ TypedValue::Instant(_)) => Ok(tv),
                (ValueType::Keyword, tv @ TypedValue::Keyword(_)) => Ok(tv),
                // Ref coerces a little: we interpret some things depending on the topograph as a Ref.
                (ValueType::Ref, TypedValue::Long(x)) => Ok(TypedValue::Ref(x)),
                (ValueType::Ref, TypedValue::Keyword(ref x)) => self.require_causetid(&x).map(|causetid| causetid.into()),

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
                => bail!(einsteindbErrorKind::BadValuePair(format!("{}", value), vt)),
            }
        }
    }
}



#[cfg(test)]
mod test {
    use super::*;
    use self::edn::Keyword;

    fn add_attribute(topograph: &mut Topograph,
            solitonid: Keyword,
            causetid: Causetid,
            attribute: Attribute) {

        topograph.causetid_map.insert(causetid, solitonid.clone());
        topograph.ident_map.insert(solitonid.clone(), causetid);

        if attribute.component {
            topograph.component_attributes.push(causetid);
        }

        topograph.attribute_map.insert(causetid, attribute);
    }

    #[test]
    fn validate_attribute_map_success() {
        let mut topograph = Topograph::default();
        // attribute that is not an index has no uniqueness
        add_attribute(&mut topograph, Keyword::isoliton_namespaceable("foo", "bar"), 97, Attribute {
            index: false,
            value_type: ValueType::Boolean,
            fulltext: false,
            unique: None,
            multival: false,
            component: false,
            no_history: false,
        });
        // attribute is unique by value and an index
        add_attribute(&mut topograph, Keyword::isoliton_namespaceable("foo", "baz"), 98, Attribute {
            index: true,
            value_type: ValueType::Long,
            fulltext: false,
            unique: Some(attribute::Unique::Value),
            multival: false,
            component: false,
            no_history: false,
        });
        // attribue is unique by idcauset and an index
        add_attribute(&mut topograph, Keyword::isoliton_namespaceable("foo", "bat"), 99, Attribute {
            index: true,
            value_type: ValueType::Ref,
            fulltext: false,
            unique: Some(attribute::Unique::Idcauset),
            multival: false,
            component: false,
            no_history: false,
        });
        // attribute is a components and a `Ref`
        add_attribute(&mut topograph, Keyword::isoliton_namespaceable("foo", "bak"), 100, Attribute {
            index: false,
            value_type: ValueType::Ref,
            fulltext: false,
            unique: None,
            multival: false,
            component: true,
            no_history: false,
        });
        // fulltext attribute is a string and an index
        add_attribute(&mut topograph, Keyword::isoliton_namespaceable("foo", "bap"), 101, Attribute {
            index: true,
            value_type: ValueType::String,
            fulltext: true,
            unique: None,
            multival: false,
            component: false,
            no_history: false,
        });

        assert!(validate_attribute_map(&topograph.causetid_map, &topograph.attribute_map).is_ok());
    }

    #[test]
    fn invalid_topograph_unique_value_not_index() {
        let mut topograph = Topograph::default();
        // attribute unique by value but not index
        let solitonid = Keyword::isoliton_namespaceable("foo", "bar");
        add_attribute(&mut topograph, solitonid , 99, Attribute {
            index: false,
            value_type: ValueType::Boolean,
            fulltext: false,
            unique: Some(attribute::Unique::Value),
            multival: false,
            component: false,
            no_history: false,
        });

        let err = validate_attribute_map(&topograph.causetid_map, &topograph.attribute_map).err().map(|e| e.kind());
        assert_eq!(err, Some(einsteindbErrorKind::BadTopographAssertion(":einsteindb/unique :einsteindb/unique_value without :einsteindb/index true for causetid: :foo/bar".into())));
    }

    #[test]
    fn invalid_topograph_unique_idcauset_not_index() {
        let mut topograph = Topograph::default();
        // attribute is unique by idcauset but not index
        add_attribute(&mut topograph, Keyword::isoliton_namespaceable("foo", "bar"), 99, Attribute {
            index: false,
            value_type: ValueType::Long,
            fulltext: false,
            unique: Some(attribute::Unique::Idcauset),
            multival: false,
            component: false,
            no_history: false,
        });

        let err = validate_attribute_map(&topograph.causetid_map, &topograph.attribute_map).err().map(|e| e.kind());
        assert_eq!(err, Some(einsteindbErrorKind::BadTopographAssertion(":einsteindb/unique :einsteindb/unique_idcauset without :einsteindb/index true for causetid: :foo/bar".into())));
    }

    #[test]
    fn invalid_topograph_component_not_ref() {
        let mut topograph = Topograph::default();
        // attribute that is a component is not a `Ref`
        add_attribute(&mut topograph, Keyword::isoliton_namespaceable("foo", "bar"), 99, Attribute {
            index: false,
            value_type: ValueType::Boolean,
            fulltext: false,
            unique: None,
            multival: false,
            component: true,
            no_history: false,
        });

        let err = validate_attribute_map(&topograph.causetid_map, &topograph.attribute_map).err().map(|e| e.kind());
        assert_eq!(err, Some(einsteindbErrorKind::BadTopographAssertion(":einsteindb/isComponent true without :einsteindb/valueType :einsteindb.type/ref for causetid: :foo/bar".into())));
    }

    #[test]
    fn invalid_topograph_fulltext_not_index() {
        let mut topograph = Topograph::default();
        // attribute that is fulltext is not an index
        add_attribute(&mut topograph, Keyword::isoliton_namespaceable("foo", "bar"), 99, Attribute {
            index: false,
            value_type: ValueType::String,
            fulltext: true,
            unique: None,
            multival: false,
            component: false,
            no_history: false,
        });

        let err = validate_attribute_map(&topograph.causetid_map, &topograph.attribute_map).err().map(|e| e.kind());
        assert_eq!(err, Some(einsteindbErrorKind::BadTopographAssertion(":einsteindb/fulltext true without :einsteindb/index true for causetid: :foo/bar".into())));
    }

    fn invalid_topograph_fulltext_index_not_string() {
        let mut topograph = Topograph::default();
        // attribute that is fulltext and not a `String`
        add_attribute(&mut topograph, Keyword::isoliton_namespaceable("foo", "bar"), 99, Attribute {
            index: true,
            value_type: ValueType::Long,
            fulltext: true,
            unique: None,
            multival: false,
            component: false,
            no_history: false,
        });

        let err = validate_attribute_map(&topograph.causetid_map, &topograph.attribute_map).err().map(|e| e.kind());
        assert_eq!(err, Some(einsteindbErrorKind::BadTopographAssertion(":einsteindb/fulltext true without :einsteindb/valueType :einsteindb.type/string for causetid: :foo/bar".into())));
    }
}
