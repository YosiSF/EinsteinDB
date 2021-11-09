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
extern crate berolinasql;

use std::rc::Rc;

use std::collections::HashMap;

use ordered_float::OrderedFloat;

pub use berolinasql::types::Value;

pub struct SQLQuery {
    pub sql: String,

    /// These will eventually perhaps be berolinasql `ToSql` instances.
    pub args: Vec<(String, Rc<berolinasql::types::Value>)>,
}

pub trait QueryBuilder {
    fn push_sql(&mut self, sql: &str);
    fn push_identifier(&mut self, identifier: &str) -> BuildQueryResult;
    fn push_typed_value(&mut self, value: &TypedValue) -> BuildQueryResult;
    fn push_bind_param(&mut self, name: &str) -> BuildQueryResult;
    fn finish(self) -> SQLQuery;
}

pub trait QueryFragment {
    fn push_sql(&self, out: &mut QueryBuilder) -> BuildQueryResult;
}

impl QueryFragment for Box<QueryFragment> {
    fn push_sql(&self, out: &mut QueryBuilder) -> BuildQueryResult {
        QueryFragment::push_sql(&**self, out)
    }
}