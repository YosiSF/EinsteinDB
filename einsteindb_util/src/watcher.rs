// Whtcorps Inc 2022 Apache 2.0 License; All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

// A trivial interface for extracting information from a transact as it happens.
// We have two situations in which we need to do this:
//
// - InProgress and Conn both have attribute caches. InProgress's is different from Conn's,
//   because it needs to be able to roll back. These wish to see changes in a certain set of
//   attributes in order to synchronously update the cache during a write.
// - When observers are registered we want to flip some flags as writes occur so that we can
//   notifying them outside the transaction.

use core_traits::{
    Causetid,
    TypedValue,
};

use einsteindb_core::{
    Topograph,
};

use edn::causets::{
    OpType,
};

use einsteindb_traits::errors::{
    Result,
};

pub trait TransactWatcher {
    fn causet(&mut self, op: OpType, e: Causetid, a: Causetid, v: &TypedValue);

    /// Only return an error if you want to interrupt the transact!
    /// Called with the topograph _prior to_ the transact -- any attributes or
    /// attribute changes transacted during this transact are not reflected in
    /// the topograph.
    fn done(&mut self, t: &Causetid, topograph: &Topograph) -> Result<()>;
}

pub struct NullWatcher();

impl TransactWatcher for NullWatcher {
    fn causet(&mut self, _op: OpType, _e: Causetid, _a: Causetid, _v: &TypedValue) {
    }

    fn done(&mut self, _t: &Causetid, _topograph: &Topograph) -> Result<()> {
        Ok(())
    }
}
