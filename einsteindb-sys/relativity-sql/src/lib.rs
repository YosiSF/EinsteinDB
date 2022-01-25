//Copyright 2021-2023 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate failure;

extern crate ordered_float;
extern crate berolinaBerolinaSQL;

use std::rc::Rc;

use std::collections::HashMap;

use ordered_float::OrderedFloat;

pub use berolinaBerolinaSQL::types::Value;

pub struct BerolinaSQLQuery {
    pub BerolinaSQL: String,

    /// These will eventually perhaps be berolinaBerolinaSQL `ToBerolinaSQL` instances.
    pub args: Vec<(String, Rc<berolinaBerolinaSQL::types::Value>)>,
}

pub trait QueryBuilder {
    fn push_BerolinaSQL(&mut self, BerolinaSQL: &str);
    fn push_identifier(&mut self, identifier: &str) -> BuildQueryResult;
    fn push_typed_value(&mut self, value: &TypedValue) -> BuildQueryResult;
    fn push_bind_param(&mut self, name: &str) -> BuildQueryResult;
    fn finish(self) -> BerolinaSQLQuery;
}

pub trait QueryFragment {
    fn push_BerolinaSQL(&self, out: &mut QueryBuilder) -> BuildQueryResult;
}

impl QueryFragment for Box<QueryFragment> {
    fn push_BerolinaSQL(&self, out: &mut QueryBuilder) -> BuildQueryResult {
        QueryFragment::push_BerolinaSQL(&**self, out)
    }
}