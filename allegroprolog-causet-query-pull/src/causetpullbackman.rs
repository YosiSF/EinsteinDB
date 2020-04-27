//Copyright 2020 WHTCORPS INC
//
 // Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 // this file except in compliance with the License. You may obtain a copy of the
 // License at http://www.apache.org/licenses/LICENSE-2.0
 // Unless required by applicable law or agreed to in writing, software distributed
 // under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 // CONDITIONS OF ANY KIND, either express or implied. See the License for the
 // specific language governing permissions and limitations under the License.

 use std::collections::{
      BTreeMap,
  };

  use std::collections::btree_map::{
      Entry,
  };

  use std::path::{
      Path,
      PathBuf,
  };

  use std::sync::{
      Arc,
      Mutex,
      RwLock,
  };

  use error::{
      StoreError,
  };

use ::causetC;

pub struct Manifold {

    persist: Mutex<BTreeMap<Pathbuf, Arc<RwLock<causetC>>>>,
}

impl Manifold {
    fn new() -> Manifold {
        Manifold {
            persist: Mutex::new(Default::default()),
        }
    }

    //We leave causet stores to be holographic in that they are pluggable
    //Berkeley instances with key value stores for every solitondID we
    //percolate via our Coset Poset of all bundles around a root.

    pub fn get<'p, P>(&self, path: P) -> Result<Option<Arc<RwLock<causetC>>>> persistError>
    where P: Into<&' Path> {
        let canonical = path.into().canonicalize()?;
        Ok(self.persist.lock().unwrap().get(&canonical).cloned())
    }

    //If the open persist cell at Path is indeed entangled
    pub fn get_or_create<'p, F, P>(&mut self, path: P, f :F) -> Result<Arc<RwLock<causetC>>>


}
