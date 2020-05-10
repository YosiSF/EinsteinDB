//Copyright 2020 WHTCORPS INC

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
pub struct Known<'s, 'c> {
    pub schema: &'s Schema,
    pub cache: Option<&'c CachedAttributes>,
}

impl<'s, 'c> Known<'s, 'c> {
    pub fn for_schema(s: &'s Schema) -> Known<'s, 'static> {
        Known {
            schema: s,
            cache: None,
        }
    }

    pub fn new(s: &'s Schema, c: Option<&'c CachedAttributes>) -> Known<'s, 'c> {
        Known {
            schema: s,
            cache: c,
        }
    }
}

impl<'s, 'c> Known<'s, 'c> {
    pub fn is_attribute_cached_reverse<U>(&self, entid: U) -> bool where U: Into<Entid> {
        self.cache
            .map(|cache| cache.is_attribute_cached_reverse(entid.into()))
            .unwrap_or(false)
    }

    pub fn is_attribute_cached_forward<U>(&self, entid: U) -> bool where U: Into<Entid> {
        self.cache
            .map(|cache| cache.is_attribute_cached_forward(entid.into()))
            .unwrap_or(false)
    }
