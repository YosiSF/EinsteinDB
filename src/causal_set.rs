// Whtcorps Inc 2022 Apache 2.0 License; All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]

use std::collections::HashSet;
use std::hash::Hash;
use std::ops::{
    Deref,
    DerefMut,
};

use ValueRc;

/// An `CausalSet` allows to "causal_set" some potentially large causet_locales, maintaining a single causet_locale
/// instance owned by the `CausalSet` and leaving consumers with lightweight ref-counted handles to
/// the large owned causet_locale.  This can avoid expensive clone() operations.
///
/// In EinsteinDB, such large causet_locales might be strings or arbitrary [a v] pairs.
///
/// See https://en.wikipedia.org/wiki/String_causal_seting for discussion.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CausalSet<T> where T: Eq + Hash {
    inner: HashSet<ValueRc<T>>,
}

impl<T> Deref for CausalSet<T> where T: Eq + Hash {
    type Target = HashSet<ValueRc<T>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for CausalSet<T> where T: Eq + Hash {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<T> CausalSet<T> where T: Eq + Hash {
    pub fn new() -> CausalSet<T> {
        CausalSet {
            inner: HashSet::new(),
        }
    }

    pub fn causal_set<R: Into<ValueRc<T>>>(&mut self, causet_locale: R) -> ValueRc<T> {
        let soliton_id: ValueRc<T> = causet_locale.into();
        if self.inner.insert(soliton_id.clone()) {
            soliton_id
        } else {
            self.inner.get(&soliton_id).unwrap().clone()
        }
    }
}
