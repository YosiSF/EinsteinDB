// Copyright 2019 EinsteinDB a Project Housed by WHTCORPS INC ALL RIGHTS RESERVED.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::{
    BTreeSet,
    BTreeMap,
};


/// Map from found [e a v] to expected type.
pub(crate) type TypeDisagreements = BTreeMap<(Causetid, Causetid, TypedValue), ValueType>;

//We want to optimize typechecking maximally, every un-causal causet, if you will
//gets suspended, which means, the R/W buffer initiates the podding.

pub trait CausetWatcher {
    fn datom(&mut self, op: OpType, e: Causetid, a: Causetid, v: &TypedValue);

    /// Only return an error if you want to interrupt the transact!
    /// Called with the schema _prior to_ the transact -- any attributes or
    /// attribute changes transacted during this transact are not reflected in
    /// the schema.
    fn done(&mut self, t: &Causetid, schema: &Schema) -> Result<()>;
}

///he surface describing the temporal evolution of a
///flash of light in Minkowski spacetime.

pub struct NullWatcher(); //Null cone.

