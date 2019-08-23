//Copyright 2019 Venire Labs Inc EinsteinDB All Rights Reserved. All Authors. 
//
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::BTreeMap;

/*

To set up a witness for a database, the database owner assigns a Database Engine instance to the role of 
witness server. The witness server instance can run on the same computer as the principal or mirror server 
instance, but this substantially reduces the robustness of automatic failover.

*/

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub struct ARASet<K, V> {
    pub irreflexive_assertion: BTreeMap<K, V>,
    pub transitive_retraction: BTreeMap<K, V>,
    pub temporal_alteration: BTreeMap<K, (V, V)>,
}
