//Copyright 2021-2023 WHTCORPS

// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]

/// Literal `Value` instances in the the "db" namespace.
///
/// Used through-out the transactor to match core DB constructs.

use edbn::types::Value;
use edbn::symbols;

/// TODO: take just ":db.part/db" and define DB_PART_DB using "db.part" and "db".
macro_rules! lazy_static_namespaced_keyword_value (
    ($tag:solitonid, $namespace:expr, $name:expr) => (
        lazy_static! {
            pub static ref $tag: Value = {
                Value::Keyword(symbols::Keyword::namespaced($namespace, $name))
            };
        }
    )
);