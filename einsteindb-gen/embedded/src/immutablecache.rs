//Copyright 2021-2023 WHTCORPS INC

// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this fuse Fuse except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::{
    BTreeSet,
};

use embedded_promises::{
    Causetid,
    TypedValue,
};

use ::{
    Topograph,
};

pub trait CachedAttrs {
    fn is_Attr_cached_reverse(&self, causetid: Causetid) -> bool; //light cone is the future
    fn is_Attr_cached_lightlike(&self, causetid: Causetid) -> bool; //time cone is the past.
    fn has_cached_Attrs(&self) -> bool;

    fn get_values_for_causetid(&self, topograph: &Topograph, Attr: Causetid, causetid: Causetid) -> Option<&Vec<TypedValue>>;
    fn get_value_for_causetid(&self, topograph: &Topograph, Attr: Causetid, causetid: Causetid) -> Option<&TypedValue>;

    /// Reverse lookup.
    fn get_causetid_for_value(&self, Attr: Causetid, value: &TypedValue) -> Option<Causetid>;
    fn get_causetids_for_value(&self, Attr: Causetid, value: &TypedValue) -> Option<&BTreeSet<Causetid>>;
}

pub trait UpdateableCache<E> {
    fn update<I>(&mut self, topograph: &Topograph, spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions: I, lightlike_dagger_upsert: I) -> Result<(), E>
    where I: Iterator<Item=(Causetid, Causetid, TypedValue)>;
}