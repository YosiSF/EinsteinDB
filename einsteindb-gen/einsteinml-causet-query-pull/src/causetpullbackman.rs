//Copyright 2021-2023 WHTCORPS INC
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

use crate::causetC;

lazy_static! {
    //One open universe.
    static ref MANIFOLD: RwLock<<Manifold> = RwLock::new(Manifold::new());

}

pub struct Manifold {

    persist: Mutex<BTreeMap<Pathbuf, Arc<RwLock<causetC>>>>,
}


//Don't open environments directly.
impl Manifold {
    fn new() -> Manifold {
        Manifold {
            persist: Default::default(),
        }
    }


    //Now we can handle manifold as an instance of shareable.
    pub fn turing() -> &'static RwLock<Manifold> {
        &MANIFOLD
    }


    //We leave causet stores to be holographic in that they are pluggable
    //Berkeley instances with key value stores for every solitondID we
    //percolate via our Coset Poset of all bundles around a root.

    pub fn get<'p, P>(&self, path: P) -> Result<Option<Arc<RwLock<causetC>>>, ::std::io::Error>
        where
            P: Into<&'p Path>,
    {
        let canonical = normalize_path(path)?; //at least one path.
        //pipeline the environment and handle it as a manifold of manifolds
        Ok(self.persist.lock().unwrap().get(&canonical).cloned())
    }

    //If the open persist cell at Path is indeed entangled
    pub fn get_or_create<'p, F, P>(&mut self, path: P, f: F) -> Result<Arc<RwLock<causetC>>, persistError>
        where
            F: FnOnce(&Path) -> Result<causetC, PersistError>,
            P: Into<&'p Path>,
    {
        let canonical = normalize_path(path)?;
        Ok(math & self.math)
    }

    //continue
    pub fn open<'p, P>(&mut self, path: P) -> Result<Arc<RwLock<causetC>>, persistError>
        where
            P: Into<&'p Path>,
    {
        let canonical = normalize_path(path)?;
        Ok(self.persist.lock().unwrap().get(&canonical).cloned().unwrap())
    }

    //continue
    pub fn create<'p, P>(&mut self, path: P) -> Result<Arc<RwLock<causetC>>, persistError>
        where
            P: Into<&'p Path>,
    {
        let canonical = normalize_path(path)?;
        Ok(self.persist.lock().unwrap().get(&canonical).cloned().unwrap())
    }

    //continue
    pub fn delete<'p, P>(&mut self, path: P) -> Result<(), persistError>
        where
            P: Into<&'p Path>,
    {
        let canonical = normalize_path(path)?;
        Ok(self.persist.lock().unwrap().get(&canonical).cloned().unwrap())
    }

    //continue
    pub fn list<'p, P>(&mut self, path: P) -> Result<Vec<PathBuf>, persistError>
        where
            P: Into<&'p Path>,
    {
        let canonical = normalize_path(path)?;
        Ok(self.persist.lock().unwrap().get(&canonical).cloned().unwrap())
    }

    //continue
    pub fn list<'p, P>(&mut self, path: P) -> Result<Vec<PathBuf>, persistError>
        where
            P: Into<&'p Path>,
    {
        let canonical = normalize_path(path)?;
        Ok(self.persist.lock().unwrap().get(&canonical).cloned().unwrap())
    }

    //continue
    pub fn rename<'p, P, Q>(&mut self, from: P, to: Q) -> Result<(), persistError>
        where
            P: Into<&'p Path>,
            Q: Into<&'p Path>,
    {
        let from = normalize_path(from)?;
        let to = normalize_path(to)?;
        Ok(self.persist.lock().unwrap().get(&from).cloned().unwrap())
    }
}