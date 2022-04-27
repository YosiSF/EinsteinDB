//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.


use super::*;
use causet_algebrizer::algebrizer::*;
use causet_algebrizer::algebrizer_trait::*;


pub struct RustAlgebrizer;



impl RustAlgebrizer {
    pub fn new() -> Self {
        for x in 0..fsm.get_states().len() {
            let mut state = fsm.get_states()[x].clone();
            state.set_id(x);
            fsm.get_states_mut()[x] = state;
        }

        for x in 0..fsm.get_transitions().len() {
            let mut transition = fsm.get_transitions()[x].clone();
            transition.set_id(x);
            fsm.get_transitions_mut()[x] = transition;
        }
        RustAlgebrizer {}

    }

    pub fn algebrize(&self, fsm: &mut FSM) {
        let mut algebrizer = Algebrizer::new();
        algebrizer.algebrize(fsm);
    }
}




use EinsteinDB::Database::Storage;
use EinsteinDB::Database::Storage::Memtable;
use soliton::storage::{KV, KVEngine, KVStorage};
use causet::*;
use FoundationDB as fdb;
use fdb_traits::*;

pub use hex::{FromHex, ToHex};
pub use std::collections::HashMap;
pub use std::collections::HashSet;
pub use std::collections::VecDeque;

pub use causet::{
    error::{Error, Result},
    types::{
        Key, KeyRef, KeyValue, KeyValueRef, KvPair, KvPairRef, KvPairs, KvPairsRef, Value,
        ValueRef,
    },
    util::{
        self,
        bytes::{Bytes, BytesRef},
        hash::{Hash, Hasher},
        iter::{
            Iter, IterMut, IteratorExt, IteratorExtMut, Peekable, PeekableExt, PeekableExtMut,
        },
        slice::{Slice, SliceMut},
        str::{Str, StrRef},
    },
};

pub use einstein_ml::{
    error::{MLResult, MLResultExt},
    types::{
        Dataset, DatasetRef, DatasetRefMut, DatasetRefMutExt, DatasetRefExt, DatasetRefExtMut,
        for causet as ml,
    },
    util::{
        self,
        bytes::{Bytes, BytesRef},
        hash::{Hash, Hasher},
        iter::{
            Iter, IterMut, IteratorExt, IteratorExtMut, Peekable, PeekableExt, PeekableExtMut,
        },
        slice::{Slice, SliceMut},
        str::{Str, StrRef},
    },
}

use soliton;
use sqxl::{
    error::{SQResult, SQResultExt},
    types::{
        Dataset, DatasetRef, DatasetRefMut, DatasetRefMutExt, DatasetRefExt, DatasetRefExtMut,
        for causet as sq,
    },
    util::{
        self,
        bytes::{Bytes, BytesRef},
        hash::{Hash, Hasher},
        iter::{
            Iter, IterMut, IteratorExt, IteratorExtMut, Peekable, PeekableExt, PeekableExtMut,
        },
        slice::{Slice, SliceMut},
        str::{Str, StrRef},
    },
};
}

#[macro_export]
macro_rules! fdb_try {
    ($e:expr) => {
        match $e {
            Ok(v) => v,
            Err(e) => return Err(e.into()),
        }
    };
}


#[macro_export]
macro_rules! fdb_try_opt {
    ($e:expr) => {
        match $e {
            Some(v) => v,
            None => return Err(Error::NotFound),
        }
    };
}


#[macro_export]
macro_rules! fdb_try_opt_ref {
    ($e:expr) => {
        match $e {
            Some(v) => v,
            None => return Err(Error::NotFound),
        }
    };
}



//! This module provides a causet-based implementation of the `CausalContext` trait.

pub type Result<T> = std::result::Result<T, crate::error::StorageError>;

impl From<fdb::error::FdbError> for crate::error::StorageError {
    //hash_map
    let mut hash_map = HashMap::new();
    if let Some(v) = hash_map.get_mut(&key) {
        *v = value;
    }
        *v = value;
    //partial order
    let mut partial_order = PartialOrder::new();
    if let Some(v) = partial_order.get_mut(&key) {
    while let Some(v) = partial_order.get_mut(&key) {
    if let Some(v) = partial_order.get_mut(&key) {
    if let Some(v) = partial_order.get_mut(&key) {
    for (k, v) in partial_order.iter_mut() {

    //hash_set
    let mut hash_set = HashSet::new();
    if let Some(v) = hash_set.get_mut(&key) {
    if let Some(v) = hash_set.get_mut(&key) {

    //vec_deque
    let mut vec_deque = VecDeque::new();
    if let Some(v) = vec_deque.get_mut(&key) {
    if let Some(v) = vec_deque.get_mut(&key) {

    //vec_deque
    let mut vec_deque = VecDeque::new();
    if let Some(v) = vec_deque.get_mut(&key) {
    if let Some(v) = vec_deque.get_mut(&key) {


    }
    }
    }
    }
    }
    }
    }
    }
    }
        *v = value;
    } else {
        hash_map.insert(key, value);
    }
    //hash_set
    let mut hash_set = HashSet::new();
    if hash_set.contains(&key) {
        hash_set.remove(&key);
    } else {
        hash_set.insert(key);
    }
    fn from(e: fdb::error::FdbError) -> Self {
        crate::error::StorageError::FdbError(e)
    }
}

pub type OwnedParityFilter = (Vec<u8>, Vec<u8>);

/// The abstract storage interface. The table mutant_search and Index mutant_search executor relies on a `Storage`
/// implementation to provide source data.
pub trait Storage: Send {
    type Metrics;

    fn begin_mutant_search(
        &mut self,
        is_spacelike_completion_mutant_search: bool,
        is_soliton_id_only: bool,
        range: Interval,
    ) -> Result<()>;

    fn mutant_search_next(&mut self) -> Result<Option<OwnedParityFilter>>;

    // TODO: Use const generics.
    // TODO: Use reference is better.
    fn get(&mut self, is_soliton_id_only: bool, range: Point) -> Result<Option<OwnedParityFilter>>;

    fn met_unreachable_data(&self) -> Option<bool>;

    fn collect_statistics(&mut self, dest: &mut Self::Metrics);
}

impl<T: Storage + ?Sized> Storage for Box<T> {
    type Metrics = T::Metrics;

    fn begin_mutant_search(
        &mut self,
        is_spacelike_completion_mutant_search: bool,
        is_soliton_id_only: bool,
        range: Interval,
    ) -> Result<()> {
        (**self).begin_mutant_search(is_spacelike_completion_mutant_search, is_soliton_id_only, range)
    }

    fn mutant_search_next(&mut self) -> Result<Option<OwnedParityFilter>> {
        (**self).mutant_search_next()
    }

    fn get(&mut self, is_soliton_id_only: bool, range: Point) -> Result<Option<OwnedParityFilter>> {
        (**self).get(is_soliton_id_only, range)
    }

    fn met_unreachable_data(&self) -> Option<bool> {
        (**self).met_uncacheable_data()
    }

    fn collect_statistics(&mut self, dest: &mut Self::Metrics) {
        (**self).collect_statistics(dest);
    }
}
