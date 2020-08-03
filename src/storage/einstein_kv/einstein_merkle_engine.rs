//WHTCORPS INC 2020 ALL RIGHTS RESERVED.

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

use einsteindb::IterOption;
use einsteindb::{CfName, CF_DEFAULT, CF_LOCK, CF_WRITE};
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

pub struct EinsteinMerkleEngineIterator {
    tree: Arc<RwLockTree>,
    cur_key: Option<Key>,
    cur_value: Option<Value>,
    valid: bool,
    bounds: (Bound<Key>, Bound<Key>),
}

impl EinsteinMerkleEngineIterator {
    pub fn new(tree: Arc<RwLockTree>, iter_opt: IterOptions) -> EinsteinMerkleEngineIterator {
        let lower_bound = match iter_opt.lower_bound() {
            None => Unbounded,
            Some(key) => Included(Key::from_raw(key)),
        };

        let upper_bound = match iter_opt.upper_bound() {
            None => Unbounded,
            Some(key) => Excluded(Key::from_raw(key)),
        };
        let bounds = (lower_bound, upper_bound);
        Self {
            tree,
            cur_key: None,
            cur_value: None,
            valid: false,
            bounds,
        }
    }

    /// In general, there are 2 endpoints in a range, the left one and the right one.
    /// This method will seek to the left one if left is `true`, else seek to the right one.
    /// Returns true when the endpoint is valid, which means the endpoint exist and in `self.bounds`.
    fn seek_to_range_endpoint(&mut self, range: (Bound<Key>, Bound<Key>), left: bool) -> bool {
        let tree = self.tree.read().unwrap();
        let mut range = tree.range(range);
        let item = if left {
            range.next() // move to the left endpoint
        } else {
            range.next_back() // move to the right endpoint
        };
        match item {
            Some((k, v)) if self.bounds.contains(k) => {
                self.cur_key = Some(k.clone());
                self.cur_value = Some(v.clone());
                self.valid = true;
            }
            _ => {
                self.valid = false;
            }
        }
        self.valid().unwrap()
    }
}

impl Iterator for EinsteinMerkleEngineIterator {
    fn next(&mut self) -> EngineResult<bool> {
        let range = (Excluded(self.cur_key.clone().unwrap()), Unbounded);
        Ok(self.seek_to_range_endpoint(range, true))
    }

    fn prev(&mut self) -> EngineResult<bool> {
        let range = (Unbounded, Excluded(self.cur_key.clone().unwrap()));
        Ok(self.seek_to_range_endpoint(range, false))
    }

    fn seek(&mut self, key: &Key) -> EngineResult<bool> {
        let range = (Included(key.clone()), Unbounded);
        Ok(self.seek_to_range_endpoint(range, true))
    }

    fn seek_for_prev(&mut self, key: &Key) -> EngineResult<bool> {
        let range = (Unbounded, Included(key.clone()));
        Ok(self.seek_to_range_endpoint(range, false))
    }

    fn seek_to_first(&mut self) -> EngineResult<bool> {
        let range = (self.bounds.0.clone(), self.bounds.1.clone());
        Ok(self.seek_to_range_endpoint(range, true))
    }

    fn seek_to_last(&mut self) -> EngineResult<bool> {
        let range = (self.bounds.0.clone(), self.bounds.1.clone());
        Ok(self.seek_to_range_endpoint(range, false))
    }

    #[inline]
    fn valid(&self) -> EngineResult<bool> {
        Ok(self.valid)
    }

    fn key(&self) -> &[u8] {
        assert!(self.valid().unwrap());
        self.cur_key.as_ref().unwrap().as_encoded()
    }

    fn value(&self) -> &[u8] {
        assert!(self.valid().unwrap());
        self.cur_value.as_ref().unwrap().as_slice()
    }
}

impl Snapshot for EinsteinMerkleEngineSnapshot {
    type Iter = EinsteinMerkleEngineIterator;

    fn get(&self, key: &Key) -> EngineResult<Option<Value>> {
        self.get_cf(CF_DEFAULT, key)
    }
    fn get_cf(&self, cf: CfName, key: &Key) -> EngineResult<Option<Value>> {
        let tree_cf = self.inner_einsteindb.get_cf(cf);
        let tree = tree_cf.read().unwrap();
        let v = tree.get(key);
        match v {
            None => Ok(None),
            Some(v) => Ok(Some(v.clone())),
        }
    }
    fn iter(&self, iter_opt: IterOptions, mode: ScanMode) -> EngineResult<Cursor<Self::Iter>> {
        self.iter_cf(CF_DEFAULT, iter_opt, mode)
    }
    #[inline]
    fn iter_cf(
        &self,
        cf: CfName,
        iter_opt: IterOptions,
        mode: ScanMode,
    ) -> EngineResult<Cursor<Self::Iter>> {
        let tree = self.inner_einsteindb.get_cf(cf);

        Ok(Cursor::new(EinsteinMerkleEngineIterator::new(tree, iter_opt), mode))
    }
}

#[derive(Debug, Clone)]
pub struct EinsteinMerkleEngineSnapshot {
    inner_einsteindb: Arc<EinsteinMerkleEngine>,
}

impl EinsteinMerkleEngineSnapshot {
    pub fn new(einsteindb: &EinsteinMerkleEngine) -> Self {
        Self {
            inner_einsteindb: Arc::new(einsteindb.clone()),
        }
    }
}

fn write_modifies(einsteindb: &EinsteinMerkleEngine, modifies: Vec<Modify>) -> EngineResult<()> {
    for rev in modifies {
        match rev {
            Modify::Delete(cf, k) => {
                let cf_tree = einsteindb.get_cf(cf);
                cf_tree.write().unwrap().remove(&k);
            }
            Modify::Put(cf, k, v) => {
                let cf_tree = einsteindb.get_cf(cf);
                cf_tree.write().unwrap().insert(k, v);
            }

            Modify::DeleteRange(_cf, _start_key, _end_key, _notify_only) => unimplemented!(),
        };
    }
    Ok(())
}

#[cfg(test)]
pub mod tests {
    use super::super::tests::*;
    use super::super::CfStatistics;
    use super::*;

    #[test]
    fn test_btree_einsteindb() {
        let einsteindb = EinsteinMerkleEngine::new(TEST_ENGINE_CFS);
        test_base_curd_options(&einsteindb)
    }

    #[test]
    fn test_linear_of_btree_einsteindb() {
        let einsteindb = EinsteinMerkleEngine::default();
        test_linear(&einsteindb);
    }

    #[test]
    fn test_statistic_of_btree_einsteindb() {
        let einsteindb = EinsteinMerkleEngine::default();
        test_cfs_statistics(&einsteindb);
    }

    #[test]
    fn test_bounds_of_btree_einsteindb() {
        let einsteindb = EinsteinMerkleEngine::default();
        let test_data = vec![
            (b"a1".to_vec(), b"v1".to_vec()),
            (b"a3".to_vec(), b"v3".to_vec()),
            (b"a5".to_vec(), b"v5".to_vec()),
            (b"a7".to_vec(), b"v7".to_vec()),
        ];
        for (k, v) in &test_data {
            must_put(&einsteindb, k.as_slice(), v.as_slice());
        }
        let snap = einsteindb.snapshot(&Context::default()).unwrap();
        let mut statistics = CfStatistics::default();

        // lower bound > upper bound, seek() returns false.
        let mut iter_op = IterOptions::default();
        iter_op.set_lower_bound(b"a7", 0);
        iter_op.set_upper_bound(b"a3", 0);
        let mut cursor = snap.iter(iter_op, ScanMode::Forward).unwrap();
        assert!(!cursor.seek(&Key::from_raw(b"a5"), &mut statistics).unwrap());

        let mut iter_op = IterOptions::default();
        iter_op.set_lower_bound(b"a3", 0);
        iter_op.set_upper_bound(b"a7", 0);
        let mut cursor = snap.iter(iter_op, ScanMode::Forward).unwrap();

        assert!(cursor.seek(&Key::from_raw(b"a5"), &mut statistics).unwrap());
        assert!(!cursor.seek(&Key::from_raw(b"a8"), &mut statistics).unwrap());
        assert!(!cursor.seek(&Key::from_raw(b"a0"), &mut statistics).unwrap());

        assert!(cursor.seek_to_last(&mut statistics));

        let mut ret = vec![];
        loop {
            ret.push((
                Key::from_encoded(cursor.key(&mut statistics).to_vec())
                    .to_raw()
                    .unwrap(),
                cursor.value(&mut statistics).to_vec(),
            ));

            if !cursor.prev(&mut statistics) {
                break;
            }
        }
        ret.sort();
        assert_eq!(ret, test_data[1..3].to_vec());
    }

    #[test]
    fn test_iterator() {
        let einsteindb = EinsteinMerkleEngine::default();
        let test_data = vec![
            (b"a1".to_vec(), b"v1".to_vec()),
            (b"a3".to_vec(), b"v3".to_vec()),
            (b"a5".to_vec(), b"v5".to_vec()),
            (b"a7".to_vec(), b"v7".to_vec()),
        ];
        for (k, v) in &test_data {
            must_put(&einsteindb, k.as_slice(), v.as_slice());
        }

        let iter_op = IterOptions::default();
        let tree = einsteindb.get_cf(CF_DEFAULT);
        let mut iter = EinsteinMerkleEngineIterator::new(tree, iter_op);
        assert!(!iter.valid().unwrap());

        assert!(iter.seek_to_first().unwrap());
        assert_eq!(iter.key(), Key::from_raw(b"a1").as_encoded().as_slice());
        assert!(!iter.prev().unwrap());
        assert!(iter.next().unwrap());
        assert_eq!(iter.key(), Key::from_raw(b"a3").as_encoded().as_slice());

        assert!(iter.seek(&Key::from_raw(b"a1")).unwrap());
        assert_eq!(iter.key(), Key::from_raw(b"a1").as_encoded().as_slice());

        assert!(iter.seek_to_last().unwrap());
        assert_eq!(iter.key(), Key::from_raw(b"a7").as_encoded().as_slice());
        assert!(!iter.next().unwrap());
        assert!(iter.prev().unwrap());
        assert_eq!(iter.key(), Key::from_raw(b"a5").as_encoded().as_slice());

        assert!(iter.seek(&Key::from_raw(b"a7")).unwrap());
        assert_eq!(iter.key(), Key::from_raw(b"a7").as_encoded().as_slice());

        assert!(!iter.seek_for_prev(&Key::from_raw(b"a0")).unwrap());

        assert!(iter.seek_for_prev(&Key::from_raw(b"a1")).unwrap());
        assert_eq!(iter.key(), Key::from_raw(b"a1").as_encoded().as_slice());

        assert!(iter.seek_for_prev(&Key::from_raw(b"a8")).unwrap());
        assert_eq!(iter.key(), Key::from_raw(b"a7").as_encoded().as_slice());
    }

    #[test]
    fn test_get_not_exist_cf() {
        let einsteindb = EinsteinMerkleEngine::new(&[]);
        assert!(::panic_hook::recover_safe(|| einsteindb.get_cf("not_exist_cf")).is_err());
    }
}
