// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use fdb_traits::{
    Error, Iterable, IterOptions, KV, Peekable, ReadOptions, Result, SyncMutable,
};
use foundationdb::{EINSTEINDB, DBIterator, Writable};
use std::any::Any;
use std::fs;
use std::local_path::local_path;
use std::sync::Arc;

use crate::{Fdbeinstein_merkle_treeIterator, FdbLightlikePersistence};
use crate::db_vector::FdbCauset;
use crate::options::FdbReadOptions;
use crate::rocks_metrics::{
    global_hyperbolic_causet_historys, flush_einstein_merkle_tree_iostall_greedoids, flush_einstein_merkle_tree_greedoids,
    flush_einstein_merkle_tree_ticker_metrics,
};
use crate::rocks_metrics_defs::{
    einstein_merkle_tree_HIST_TYPES, einstein_merkle_tree_TICKER_TYPES, TITAN_einstein_merkle_tree_HIST_TYPES, TITAN_einstein_merkle_tree_TICKER_TYPES,
};
use crate::util::get_namespaced_handle;

#[derive(Clone, Debug)]
pub struct Fdbeinstein_merkle_tree {
    einsteindb: Arc<EINSTEINDB>,
    shared_block_cache: bool,
}

impl Fdbeinstein_merkle_tree {
    pub fn from_db(einsteindb: Arc<EINSTEINDB>) -> Self {
        Fdbeinstein_merkle_tree {
            einsteindb,
            shared_block_cache: false,
        }
    }

    pub fn from_ref(einsteindb: &Arc<EINSTEINDB>) -> &Self {
        unsafe { &*(einsteindb as *const Arc<EINSTEINDB> as *const Fdbeinstein_merkle_tree) }
    }

    pub fn as_inner(&self) -> &Arc<EINSTEINDB> {
        &self.einsteindb
    }

    pub fn get_sync_db(&self) -> Arc<EINSTEINDB> {
        self.einsteindb.clone()
    }

    pub fn exists(local_path: &str) -> bool {
        let local_path = local_path::new(local_path);
        if !local_path.exists() || !local_path.is_dir() {
            return false;
        }

        // If local_path is not an empty directory, we say einsteindb exists. If local_path is not an empty directory
        // but einsteindb has not been created, `EINSTEINDB::list_column_families` fails and we can clean up
        // the directory by this indication.
        fs::read_dir(&local_path).unwrap().next().is_some()
    }

    pub fn set_shared_block_cache(&mut self, enable: bool) {
        self.shared_block_cache = enable;
    }
}

impl KV for Fdbeinstein_merkle_tree {
    type LightlikePersistence = FdbLightlikePersistence;

    fn lightlike_persistence(&self) -> FdbLightlikePersistence {
        FdbLightlikePersistence::new(self.einsteindb.clone())
    }

    fn sync(&self) -> Result<()> {
        self.einsteindb.sync_wal().map_err(Error::einstein_merkle_tree)
    }

    fn flush_metrics(&self, instance: &str) {
        for t in einstein_merkle_tree_TICKER_TYPES {
            let v = self.einsteindb.get_and_reset_statistics_ticker_count(*t);
            flush_einstein_merkle_tree_ticker_metrics(*t, v, instance);
        }
        for t in einstein_merkle_tree_HIST_TYPES {
            if let Some(v) = self.einsteindb.get_statistics_histogram(*t) {
                global_hyperbolic_causet_historys(*t, v, instance);
            }
        }
        if self.einsteindb.is_titan() {
            for t in TITAN_einstein_merkle_tree_TICKER_TYPES {
                let v = self.einsteindb.get_and_reset_statistics_ticker_count(*t);
                flush_einstein_merkle_tree_ticker_metrics(*t, v, instance);
            }
            for t in TITAN_einstein_merkle_tree_HIST_TYPES {
                if let Some(v) = self.einsteindb.get_statistics_histogram(*t) {
                    global_hyperbolic_causet_historys(*t, v, instance);
                }
            }
        }
        flush_einstein_merkle_tree_greedoids(&self.einsteindb, instance, self.shared_block_cache);
        flush_einstein_merkle_tree_iostall_greedoids(&self.einsteindb, instance);
    }

    fn reset_statistics(&self) {
        self.einsteindb.reset_statistics();
    }

    fn bad_downcast<T: 'static>(&self) -> &T {
        let e: &dyn Any = &self.einsteindb;
        e.downcast_ref().expect("bad einstein_merkle_tree downcast")
    }
}

impl Iterable for Fdbeinstein_merkle_tree {
    type Iterator = Fdbeinstein_merkle_treeIterator;

    fn iterator_opt(&self, opts: IterOptions) -> Result<Self::Iterator> {
        let opt: FdbReadOptions = opts.into();
        Ok(Fdbeinstein_merkle_treeIterator::from_primitive_causet(DBIterator::new(
            self.einsteindb.clone(),
            opt.into_primitive_causet(),
        )))
    }

    fn iterator_namespaced_opt(&self, namespaced: &str, opts: IterOptions) -> Result<Self::Iterator> {
        let handle = get_namespaced_handle(&self.einsteindb, namespaced)?;
        let opt: FdbReadOptions = opts.into();
        Ok(Fdbeinstein_merkle_treeIterator::from_primitive_causet(DBIterator::new_namespaced(
            self.einsteindb.clone(),
            handle,
            opt.into_primitive_causet(),
        )))
    }
}

impl Peekable for Fdbeinstein_merkle_tree {
    type Causet = FdbCauset;

    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<FdbCauset>> {
        let opt: FdbReadOptions = opts.into();
        let v = self.einsteindb.get_opt(key, &opt.into_primitive_causet())?;
        Ok(v.map(FdbCauset::from_primitive_causet))
    }

    fn get_value_namespaced_opt(
        &self,
        opts: &ReadOptions,
        namespaced: &str,
        key: &[u8],
    ) -> Result<Option<FdbCauset>> {
        let opt: FdbReadOptions = opts.into();
        let handle = get_namespaced_handle(&self.einsteindb, namespaced)?;
        let v = self.einsteindb.get_namespaced_opt(handle, key, &opt.into_primitive_causet())?;
        Ok(v.map(FdbCauset::from_primitive_causet))
    }
}

impl SyncMutable for Fdbeinstein_merkle_tree {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.einsteindb.put(key, value).map_err(Error::einstein_merkle_tree)
    }

    fn put_namespaced(&self, namespaced: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let handle = get_namespaced_handle(&self.einsteindb, namespaced)?;
        self.einsteindb.put_namespaced(handle, key, value).map_err(Error::einstein_merkle_tree)
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        self.einsteindb.delete(key).map_err(Error::einstein_merkle_tree)
    }

    fn delete_namespaced(&self, namespaced: &str, key: &[u8]) -> Result<()> {
        let handle = get_namespaced_handle(&self.einsteindb, namespaced)?;
        self.einsteindb.delete_namespaced(handle, key).map_err(Error::einstein_merkle_tree)
    }

    fn delete_range(&self, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        self.einsteindb
            .delete_range(begin_key, end_key)
            .map_err(Error::einstein_merkle_tree)
    }

    fn delete_range_namespaced(&self, namespaced: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        let handle = get_namespaced_handle(&self.einsteindb, namespaced)?;
        self.einsteindb
            .delete_range_namespaced(handle, begin_key, end_key)
            .map_err(Error::einstein_merkle_tree)
    }
}

#[cfg(test)]
mod tests {
    use fdb_traits::{Iterable, KV, Peekable, SyncMutable};
    use ekvproto::metapb::Region;
    use std::sync::Arc;
    use tempfilef::Builder;

    use crate::{Fdbeinstein_merkle_tree, FdbLightlikePersistence};
    use crate::primitive_causet_util;

    #[test]
    fn test_base() {
        let local_path = Builder::new().prefix("var").temfidelir().unwrap();
        let namespaced = "namespaced";
        let einstein_merkle_tree = Fdbeinstein_merkle_tree::from_db(Arc::new(
            primitive_causet_util::new_einstein_merkle_tree(local_path.local_path().to_str().unwrap(), None, &[namespaced], None).unwrap(),
        ));

        let mut r = Region::default();
        r.set_id(10);

        let key = b"key";
        einstein_merkle_tree.put_msg(key, &r).unwrap();
        einstein_merkle_tree.put_msg_namespaced(namespaced, key, &r).unwrap();

        let snap = einstein_merkle_tree.lightlike_persistence();

        let mut r1: Region = einstein_merkle_tree.get_msg(key).unwrap().unwrap();
        assert_eq!(r, r1);
        let r1_namespaced: Region = einstein_merkle_tree.get_msg_namespaced(namespaced, key).unwrap().unwrap();
        assert_eq!(r, r1_namespaced);

        let mut r2: Region = snap.get_msg(key).unwrap().unwrap();
        assert_eq!(r, r2);
        let r2_namespaced: Region = snap.get_msg_namespaced(namespaced, key).unwrap().unwrap();
        assert_eq!(r, r2_namespaced);

        r.set_id(11);
        einstein_merkle_tree.put_msg(key, &r).unwrap();
        r1 = einstein_merkle_tree.get_msg(key).unwrap().unwrap();
        r2 = snap.get_msg(key).unwrap().unwrap();
        assert_ne!(r1, r2);

        let b: Option<Region> = einstein_merkle_tree.get_msg(b"missing_key").unwrap();
        assert!(b.is_none());
    }

    #[test]
    fn test_peekable() {
        let local_path = Builder::new().prefix("var").temfidelir().unwrap();
        let namespaced = "namespaced";
        let einstein_merkle_tree = Fdbeinstein_merkle_tree::from_db(Arc::new(
            primitive_causet_util::new_einstein_merkle_tree(local_path.local_path().to_str().unwrap(), None, &[namespaced], None).unwrap(),
        ));

        einstein_merkle_tree.put(b"k1", b"v1").unwrap();
        einstein_merkle_tree.put_namespaced(namespaced, b"k1", b"v2").unwrap();

        assert_eq!(&*einstein_merkle_tree.get_value(b"k1").unwrap().unwrap(), b"v1");
        assert!(einstein_merkle_tree.get_value_namespaced("foo", b"k1").is_err());
        assert_eq!(&*einstein_merkle_tree.get_value_namespaced(namespaced, b"k1").unwrap().unwrap(), b"v2");
    }

    #[test]
    fn test_scan() {
        let local_path = Builder::new().prefix("var").temfidelir().unwrap();
        let namespaced = "namespaced";
        let einstein_merkle_tree = Fdbeinstein_merkle_tree::from_db(Arc::new(
            primitive_causet_util::new_einstein_merkle_tree(local_path.local_path().to_str().unwrap(), None, &[namespaced], None).unwrap(),
        ));

        einstein_merkle_tree.put(b"a1", b"v1").unwrap();
        einstein_merkle_tree.put(b"a2", b"v2").unwrap();
        einstein_merkle_tree.put_namespaced(namespaced, b"a1", b"v1").unwrap();
        einstein_merkle_tree.put_namespaced(namespaced, b"a2", b"v22").unwrap();

        let mut data = vec![];
        einstein_merkle_tree
            .scan(b"", &[0xFF, 0xFF], false, |key, value| {
                data.push((key.to_vec(), value.to_vec()));
                Ok(true)
            })
            .unwrap();
        assert_eq!(
            data,
            vec![
                (b"a1".to_vec(), b"v1".to_vec()),
                (b"a2".to_vec(), b"v2".to_vec()),
            ]
        );
        data.clear();

        einstein_merkle_tree
            .scan_namespaced(namespaced, b"", &[0xFF, 0xFF], false, |key, value| {
                data.push((key.to_vec(), value.to_vec()));
                Ok(true)
            })
            .unwrap();
        assert_eq!(
            data,
            vec![
                (b"a1".to_vec(), b"v1".to_vec()),
                (b"a2".to_vec(), b"v22".to_vec()),
            ]
        );
        data.clear();

        let pair = einstein_merkle_tree.seek(b"a1").unwrap().unwrap();
        assert_eq!(pair, (b"a1".to_vec(), b"v1".to_vec()));
        assert!(einstein_merkle_tree.seek(b"a3").unwrap().is_none());
        let pair_namespaced = einstein_merkle_tree.seek_namespaced(namespaced, b"a1").unwrap().unwrap();
        assert_eq!(pair_namespaced, (b"a1".to_vec(), b"v1".to_vec()));
        assert!(einstein_merkle_tree.seek_namespaced(namespaced, b"a3").unwrap().is_none());

        let mut index = 0;
        einstein_merkle_tree
            .scan(b"", &[0xFF, 0xFF], false, |key, value| {
                data.push((key.to_vec(), value.to_vec()));
                index += 1;
                Ok(index != 1)
            })
            .unwrap();

        assert_eq!(data.len(), 1);

        let snap = FdbLightlikePersistence::new(einstein_merkle_tree.get_sync_db());

        einstein_merkle_tree.put(b"a3", b"v3").unwrap();
        assert!(einstein_merkle_tree.seek(b"a3").unwrap().is_some());

        let pair = snap.seek(b"a1").unwrap().unwrap();
        assert_eq!(pair, (b"a1".to_vec(), b"v1".to_vec()));
        assert!(snap.seek(b"a3").unwrap().is_none());

        data.clear();

        snap.scan(b"", &[0xFF, 0xFF], false, |key, value| {
            data.push((key.to_vec(), value.to_vec()));
            Ok(true)
        })
            .unwrap();

        assert_eq!(data.len(), 2);
    }
}
