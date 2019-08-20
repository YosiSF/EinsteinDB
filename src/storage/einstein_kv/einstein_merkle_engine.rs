//Copyright Venire Labs Inc 2019

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.



use std::collections::BTreeMap;
use std::collections::Bound::{self, Excluded, Included, Unbounded};
use std::default::Default;
use std::fmt::{self, Debug, Display, Formatter};
use std::ops::RangeBounds;
use std::sync::{Arc, RwLock};

use engine::IterOption;
use engine::{CfName, CF_DEFAULT, CF_LOCK, CF_WRITE};
use ekvproto::ekvpcpb::Context;

use crate::storage::kv::{
    Callback as EngineCallback, CbContext, Cursor, Engine, Error as EngineError, Iterator, Modify,
    Result as EngineResult, ScanMode, Snapshot,
};
use crate::storage::{Key, Value};

type RwLockTree = RwLock<BTreeMap<Key, Value>>;

//The Einstein Merkle Engine is an in-memory data read
//Excellent for swift testing.
#[derive(Clone)]
pub struct EinsteinMerkleEngine {
    cf_names: Vec<CfName>,
    cf_contents: Vec<Arc<RwLockTree>>,
}

impl EinsteinMerkleEngine {
    pub fn new(cfs: &[CfName]) -> Self {
        let mut cf_names = vec![];
        let mut cf_contents = vec![];

        // create default cf if missing
        if cfs.iter().find(|&&c| c == CF_DEFAULT).is_none() {
            //push default configurations if missing
            cf_names.push(CF_DEFAULT);
            //We're pushing the new BTreeMap state without unwrapping
            cf_contents.push(Arc::new(RwLock::new(BTreeMap::new())))
        }

        //Find the location of the iterable instance of configuration niblles.
        for cf in cfs.iter() {
            cf_names.push(*cf);
            cf_contents.push(Arc::new(RwLock::new(BTreeMap::new())))
        }

        Self {
            cf_names,
            cf_contents,
        }
    }

    pub fn get_cf(&self, cf: CfName) -> Arc<RwLockTree> {
        let index = self
            .cf_names
            .iter()
            .position(|&c| c == cf)
            .expect("CF not exist!");
        self.cf_contents[index].clone()
    }
}

impl Default for EinsteinMerkleEngine {
    fn default() -> Self {
        let cfs = &[CF_WRITE, CF_DEFAULT, CF_];
        Self::new(cfs)
    }
}

impl Engine for EinsteinMerkleEngine {
    type Snap = BTreeEngineSnapshot;

    fn async_write(
        &self,
        _ctx: &Context,
        modifies: Vec<Modify>,
        cb: EngineCallback<()>,
    ) -> EngineResult<()> {
        if modifies.is_empty() {
            return Err(EngineError::EmptyRequest);
        }
        cb((CbContext::new(), write_modifies(&self, modifies)));

        Ok(())
    }
    /// warning: It returns a fake snapshot whose content will be affected by the later modifies!
    fn async_snapshot(&self, _ctx: &Context, cb: EngineCallback<Self::Snap>) -> EngineResult<()> {
        cb((CbContext::new(), Ok(BTreeEngineSnapshot::new(&self))));
        Ok(())
    }
}

impl Display for EinsteinMerkleEngine {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "EinsteinMerkleEngine",)
    }
}

impl Debug for EnsteinMerkleEngine {
    // TODO: Provide more debug info.
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "EinsteinMerkleEngine",)
    }
}
