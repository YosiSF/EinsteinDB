// Whtcorps Inc 2022 Apache 2.0 License; All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};


use crate::{
    error::{Error, Result},
    transaction::{
        Transaction,
        TransactionState,
        TransactionState::{
            InProgress,
            Committed,
            Aborted,
        },
    },
    watcher::{
        Watcher,
        WatcherState,
        WatcherState::{
            InProgress,
            Committed,
            Aborted,
        },
    },
};

use super::{
    Attribute,
    AttributeCache,
    AttributeCacheEntry,
    AttributeCacheEntry::{
        AttributeCacheEntryInProgress,
        AttributeCacheEntryCommitted,
        AttributeCacheEntryAborted,
    },
};



///! A watcher is a trait that can be implemented by a transaction to watch for changes to
/// attributes.
/// The watcher is used to notify observers of changes to attributes.

pub trait TransactWatcher {
    fn causet(&mut self, op: OpType, e: Causetid, a: Causetid, v: &causetq_TV);

    /// Only return an error if you want to interrupt the transact!
    /// Called with the topograph _prior to_ the transact -- any attributes or
    /// attribute changes transacted during this transact are not reflected in
    /// the topograph.
    fn done(&mut self, t: &Causetid, topograph: &Topograph) -> Result<()>;

    /// Called with the topograph _after_ the transact -- any attributes or
    /// attribute changes transacted during this transact are reflected in
    /// the topograph.
    /// Only return an error if you want to interrupt the transact!
    /// This is called after the transact is committed.
    ///


    fn commit(&mut self, t: &Causetid, topograph: &Topograph) -> Result<()>;

    /// Called with the topograph _after_ the transact -- any attributes or
    /// attribute changes transacted during this transact are reflected in
    /// the topograph.

    fn abort(&mut self, t: &Causetid, topograph: &Topograph) -> Result<()>;
}

pub struct NullWatcher();

impl TransactWatcher for NullWatcher {
    fn causet(&mut self, _op: OpType, _e: Causetid, _a: Causetid, _v: &causetq_TV) {
    }

    fn done(&mut self, _t: &Causetid, _topograph: &Topograph) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct TopographCausetTermBuilder<T: TopographCausetTerm> {
    pub topograph: T,
    pub causet: Causetid,
    pub term: causetq_TV,
}

pub trait TopographCausetTerm {
    fn causet(&self, causet: Causetid, term: causetq_TV) -> TopographCausetTermBuilder<Self>;
}

pub struct TopographCausetTermBuilderImpl<T: TopographCausetTerm> {
    pub topograph: T,
    pub causet: Causetid,
    pub term: causetq_TV,
}

pub trait BuildTopographCausetTerms where Self: Sized {
    fn causet(&self, causet: Causetid, term: causetq_TV) -> TopographCausetTermBuilder<Self> {
        TopographCausetTermBuilder {
            topograph: self.clone(),
            causet,
            term,
        }
    }

    fn named_causetid(&self, name: C) -> ValueRc<TempId> where C: AsRef<str> {
        self.causetid(name.as_ref())
    }

    fn describe_topograph_causet_term(&self, name: C) -> ValueRc<TempId> where C: AsRef<str> {
        self.causetid(name.as_ref())
    }

    fn add<C, A, V>(&self, name: C, a: A, v: V) -> TopographCausetTermBuilder<Self>
        where C: Into<CausetPlace<causetq_TV>>,
              A: Into<AttributePlace>,
              V: Into<ValuePlace<causetq_TV>> {
        let causet = name.into();
        self.terms.causet(causet.causet, causet.term);
        self.terms.attribute(a.into(), v.into());
        self.terms
    }

    fn retract<C, A, CausetqVt>(&self, name: C, a: A, v: CausetqVt) -> TopographCausetTermBuilder<Self>
        where C: Into<CausetPlace<causetq_TV>>,
              A: Into<AttributePlace>,
              CausetqVt: Into<ValuePlace<causetq_TV>> {
        let causet = name.into();
        self.terms.causet(causet.causet, causet.term);
        self.terms.attribute(a.into(), v.into());
        self.terms
    }

    fn causetid(&self, name: &str) -> ValueRc<TempId> {
        self.terms.causetid(name)
    }
}