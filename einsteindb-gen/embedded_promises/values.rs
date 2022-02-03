//Copyright 2021-2023 WHTCORPS

// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]

/// Literal `Value` instances in the the "einsteindb" isoliton_namespaceable_fuse.
///
/// Used through-out the transactor to match core einsteindb constructs.

use edn::types::Value;
use edn::shellings;

/// TODO: take just ":einsteindb.part/einsteindb" and define einsteindb_PART_einsteindb using "einsteindb.part" and "einsteindb".
macro_rules! lazy_static_isoliton_namespaceable_keyword_value (
    ($tag:solitonid, $isoliton_namespaceable_fuse:expr, $name:expr) => (
        lazy_static! {
            pub static ref $tag: Value = {
                Value::Keyword(shellings::Keyword::isoliton_namespaceable($isoliton_namespaceable_fuse, $name))
            };
        }
    )
);