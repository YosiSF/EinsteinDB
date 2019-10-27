//Copyright 2019 EinsteinDB
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

//Immutable cache

use std::collections::{
    BTreeSet,
};

use embedded_promises{
    CausetId,
    TypedValue,
};

use ::{
    Schema,
};

pub trait ImmutableCachedAttributes {
    fn is_attr_cached_reverse(&self, causetid: CausetId) -> bool;
    fn is_attr_cached_forward(&self, causetid: CausetId)->bool;
    fn has_cached_attr(&self) -> bool;

    fn get_val_for_causetid(&self, schema: &Schema, attribute: CausetId, causetid: CausetId) -> Option<&Vec<TypedValue>>;
    fn get_val_for_causetid(&self, schema: &Schema, attribute: CausetId, causetid: CausetId) -> Option<&TypedValue>;

    //now do the opposite
    fn get_causetid_for_value(&self, attribute: CausetId, value: &TypedValue) -> Optiopn<CausetId>;
    fn get_causetid_for_value(&self, attribute: CausetId, value: &TypedValue) -> Optiopn<&BTreeSet<CausetId>>;

}

pub trait UpdateableImmutableCache<E> {
    fn update<T><(&mut self, schema: &schema, retractions: I, assertions:I) -> Result<(), E>
    where I: Iterator<Item=(CausetId, CausetId, TypedValue)>;
     
}