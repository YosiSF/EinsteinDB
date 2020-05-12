//Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

/// Define the inputs to a query. This is in two parts: a set of values known now, and a set of
/// types known now.
/// The separate map of types is to allow queries to be algebrized without full knowledge of
/// the bindings that will be used at execution time.
/// When built correctly, `types` is guaranteed to contain the types of `values` -- use
/// `QueryInputs::new` or `QueryInputs::with_values` to construct an instance.
pub struct QueryInputs {
    pub(crate) types: BTreeMap<Variable, ValueType>,
    pub(crate) values: BTreeMap<Variable, TypedValue>,
}

impl Default for QueryInputs {
    fn default() -> Self {
        QueryInputs {
            types: BTreeMap::default(),
            values: BTreeMap::default(),
        }
    }
}