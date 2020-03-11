//Copyright WHTCORPS INC 2020 All Rights Reserved.
//#![allow(dead_code)]
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::BTreeMap;

//Witness assertions take place in lightlike space on EinsteinDB based off the ARCH
//requirements of the borrower.
//our Key-Value mutability pattern is invoked only as a persistent scheme.
//By no means is EinsteinDB a traditional k-v store.
//It feels like Allegro meets Prolog for the presistence layer.

/*
#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub struct TimestepEvaluation<K, V> {
    pub lightlike: BTreeMap<K, V>,
    pub spacelike: BTreeMap<K, V>,
    pub timelike: BTreeMap<K, (V, V)>,
}

impl<K, V> Default for TimestepEvaluation<K, V> where K: Ord {
    fn default() -> TimestepEvaluation
<K, V> {
        TimestepEvaluation
     {
            lightlike: BTreeMap::default(),
            spacelike: BTreeMap::default(),
            timelike: BTreeMap::default(),
        }
    }
}

*/
