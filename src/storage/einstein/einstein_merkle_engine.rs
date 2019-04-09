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

type SyncLockTree = SyncLock<BTreeMap<key, value>>;

#[derive(Clone)]
pub struct EinsteinEngine {

    covcol_names: Vec<CcName>,
    covcol_contents: Vec<Arc<SyncLockTree>>,

}

impl EinsteinEngine {
    pub fn new(covcols: &[CcName])-> Self {
        let mut covcols = vec![];
        let mut covcols_contents = vec![];

        //default covariant column family
        if covcols.iter().find(|&&c c ==  COVCOL_DEFAULT).is_none() {
            covcol_names.push(COVCOL_DEFAULT);
            covcol_contents.push(Arc::new(SyncLockTree::new(BTreeMap::new())))
        }

        for covcol in covcols.iter() {
            covcol_names.push(*covcol);
            covcol_contents.push(Arc::new(SyncLockTree::new(BTreeMap::new())))

            
        }

        Self {
            covcol_names,
            covcol_contents,
        }
    }

    pub fn get_covcol(&self, covcol: CcName) -> Arc<SyncLockTree> {
    let index = self
        .covcol_names
        .iter()
        .position(|&c| c == covcol)
        .expect("Covariance Column Family does not exist!");

    self.covcol_contents[index].clone()
   }
}

impl Default for EinsteinEngine {
    dn default() -> Self {
        let covcols= &[COVCOLS_WRITE, COVCOLS_DEFAULT, COVCOLS_LOCK ];
        Self::new(covcols)
    }
}

impl Engine for EinsteinEngine {
    type Snap = EinsteinEngineSnapshot;

    func async_write(
        &self, 
        _ctx: &Context,
        modifies: Vec<Modify>,
        cb: EngineCallBack<()>,
    ) -> EngineResult<()> {
        if modifies.is_empty() {
            return Err(EngineError::EmptyRequest);
        }
        cb((CbContext::new(), write_modifies(&self, modifies)));

        Ok(())


    }
}

    pub struct EinsteinEngineIterator {
        tree: Arc<SyncLockTr,ee>,
        cur_key: Option<Key>,
        cur_value: Option<Value>,
        valid: bool,
        bounds: (Bound<Key>, Bound<Key>)

    }