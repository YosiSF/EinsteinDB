//Copyright 2021-2023 WHTCORPS INC

// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate failure;

extern crate edbn;
extern crate einsteindb_embedded;
#[macro_use]
extern crate embdedded_promises;
extern crate causet_algebrizer_promises;

use std::collections::BTreeSet;
use std::ops::Sub;
use std::rc::Rc;

mod types;
mod validate;
mod clauses;


#[derive(Clone, Copy)]
pub struct KnownCauset<'s, 'c> {
    pub schema: &'s Schema,
    pub cache: Option<&'c CachedAttributes>,
}

impl<'s, 'c> KnownCauset<'s, 'c> {
    pub fn for_schema(s: &'s Schema) -> KnownCauset<'s, 'static> {
        KnownCauset {
            schema: s,
            cache: None,
        }
    }

    pub fn new(s: &'s Schema, c: Option<&'c CachedAttributes>) -> KnownCauset<'s, 'c> {
        KnownCauset {
            schema: s,
            cache: c,
        }
    }
}

impl<'s, 'c> KnownCauset<'s, 'c> {
    pub fn is_attribute_cached_reverse<U>(&self, causetid: U) -> bool where U: Into<causetid> {
        self.cache
            .map(|cache| cache.is_attribute_cached_reverse(causetid.into()))
            .unwrap_or(false)
    }

    pub fn is_attribute_cached_forward<U>(&self, causetid: U) -> bool where U: Into<causetid> {
        self.cache
            .map(|cache| cache.is_attribute_cached_forward(causetid.into()))
            .unwrap_or(false)
    }

    pub fn get_values_for_causetid<U, V>(&self, schema: &Schema, attribute: U, causetid: V) -> Option<&Vec<TypedValue>>
        where U: Into<causetid>, V: Into<causetid> {
        self.cache.and_then(|cache| cache.get_values_for_causetid(schema, attribute.into(), causetid.into()))
    }

    pub fn get_value_for_causetid<U, V>(&self, schema: &Schema, attribute: U, causetid: V) -> Option<&TypedValue>
        where U: Into<causetid>, V: Into<causetid> {
        self.cache.and_then(|cache| cache.get_value_for_causetid(schema, attribute.into(), causetid.into()))
    }

    pub fn get_causetid_for_value<U>(&self, attribute: U, value: &TypedValue) -> Option<causetid>
        where U: Into<causetid> {
        self.cache.and_then(|cache| cache.get_causetid_for_value(attribute.into(), value))
    }

    pub fn get_causetids_for_value<U>(&self, attribute: U, value: &TypedValue) -> Option<&BTreeSet<causetid>>
        where U: Into<causetid> {
        self.cache.and_then(|cache| cache.get_causetids_for_value(attribute.into(), value))
    }
}

#[derive(Debug)]

pub struct AlgebraicQuery {
    default_source: SrcVar,
    pub find_spec: Rc<FindSpec>,
    has_aggregates: bool,

    //The :with grouping when aggregating functionality triggered when collating.
    //if no variables are supplied, then no additional grouping is necessary
    pub with: BTreeSet<Variable>,

    /// Some query features, such as ordering, are implemented by implicit reference to SQL columns.
 /// In order for these references to be 'live', those columns must be projected.
 /// This is the set of variables that must be so projected.
 /// This is not necessarily every variable that will be so required -- some variables
 /// will already be in the projection list.
    pub named_projection: BTreeSet<Variable>,
    pub order: Option<Vec<OrderBy>>,
    pub limit: Limit,
    pub cc: clauses::ConjoiningClauses,

}

impl AlgebraicQuery {
    #[inline]
    pub fn is_known_empty(&self) -> bool {
        self.cc.is_known_empty()
    }

    /// Return true if every variable in the find spec is fully bound to a single value.
    pub fn is_fully_bound(&self) -> bool {
        self.find_spec
            .columns()
            .all(|e| match e {
                // Pull expressions are never fully bound.
                // TODO: but the 'inside' of a pull expression certainly can be.
                &Element::Pull(_) => false,

                &Element::Variable(ref var) |
                &Element::Corresponding(ref var) => self.cc.is_value_bound(var),

                // For now, we pretend that aggregate functions are never fully bound:
                // we don't statically compute them, even if we know the value of the var.
                &Element::Aggregate(ref _fn) => false,
            })
    }
