// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use fdb_traits::{
    Error, Iterable, IterOptions, KvEngine, Peekable, ReadOptions, Result, SyncMutable,
};
use foundationdb::{DB, DBIterator, Writable};
use std::any::Any;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use crate::{FdbEngineIterator, FdbSnapshot};
use crate::db_vector::FdbDBVector;
use crate::options::FdbReadOptions;
use crate::rocks_metrics::{
    global_hyperbolic_causet_historys, flush_engine_iostall_properties, flush_engine_properties,
    flush_engine_ticker_metrics,
};
use crate::rocks_metrics_defs::{
    ENGINE_HIST_TYPES, ENGINE_TICKER_TYPES, TITAN_ENGINE_HIST_TYPES, TITAN_ENGINE_TICKER_TYPES,
};
use crate::util::get_namespaced_handle;

#[derive(Clone, Debug)]
pub struct FdbEngine {
    einsteindb: Arc<DB>,
    shared_block_cache: bool,
}

impl FdbEngine {
    pub fn from_db(einsteindb: Arc<DB>) -> Self {
        FdbEngine {
            einsteindb,
            shared_block_cache: false,
        }
    }

    pub fn from_ref(einsteindb: &Arc<DB>) -> &Self {
        unsafe { &*(einsteindb as *const Arc<DB> as *const FdbEngine) }
    }

    pub fn as_inner(&self) -> &Arc<DB> {
        &self.einsteindb
    }

    pub fn get_sync_db(&self) -> Arc<DB> {
        self.einsteindb.clone()
    }

    pub fn exists(path: &str) -> bool {
        let path = Path::new(path);
        if !path.exists() || !path.is_dir() {
            return false;
        }

        // If path is not an empty directory, we say einsteindb exists. If path is not an empty directory
        // but einsteindb has not been created, `DB::list_column_families` fails and we can clean up
        // the directory by this indication.
        fs::read_dir(&path).unwrap().next().is_some()
    }

    pub fn set_shared_block_cache(&mut self, enable: bool) {
        self.shared_block_cache = enable;
    }
}

impl KvEngine for FdbEngine {
    type Snapshot = FdbSnapshot;

    fn snapshot(&self) -> FdbSnapshot {
        FdbSnapshot::new(self.einsteindb.clone())
    }

    fn sync(&self) -> Result<()> {
        self.einsteindb.sync_wal().map_err(Error::Engine)
    }

    fn flush_metrics(&self, instance: &str) {
        for t in ENGINE_TICKER_TYPES {
            let v = self.einsteindb.get_and_reset_statistics_ticker_count(*t);
            flush_engine_ticker_metrics(*t, v, instance);
        }
        for t in ENGINE_HIST_TYPES {
            if let Some(v) = self.einsteindb.get_statistics_histogram(*t) {
                global_hyperbolic_causet_historys(*t, v, instance);
            }
        }
        if self.einsteindb.is_titan() {
            for t in TITAN_ENGINE_TICKER_TYPES {
                let v = self.einsteindb.get_and_reset_statistics_ticker_count(*t);
                flush_engine_ticker_metrics(*t, v, instance);
            }
            for t in TITAN_ENGINE_HIST_TYPES {
                if let Some(v) = self.einsteindb.get_statistics_histogram(*t) {
                    global_hyperbolic_causet_historys(*t, v, instance);
                }
            }
        }
        flush_engine_properties(&self.einsteindb, instance, self.shared_block_cache);
        flush_engine_iostall_properties(&self.einsteindb, instance);
    }

    fn reset_statistics(&self) {
        self.einsteindb.reset_statistics();
    }

    fn bad_downcast<T: 'static>(&self) -> &T {
        let e: &dyn Any = &self.einsteindb;
        e.downcast_ref().expect("bad engine downcast")
    }
}

impl Iterable for FdbEngine {
    type Iterator = FdbEngineIterator;

    fn iterator_opt(&self, opts: IterOptions) -> Result<Self::Iterator> {
        let opt: FdbReadOptions = opts.into();
        Ok(FdbEngineIterator::from_raw(DBIterator::new(
            self.einsteindb.clone(),
            opt.into_raw(),
        )))
    }

    fn iterator_namespaced_opt(&self, namespaced: &str, opts: IterOptions) -> Result<Self::Iterator> {
        let handle = get_namespaced_handle(&self.einsteindb, namespaced)?;
        let opt: FdbReadOptions = opts.into();
        Ok(FdbEngineIterator::from_raw(DBIterator::new_namespaced(
            self.einsteindb.clone(),
            handle,
            opt.into_raw(),
        )))
    }
}

impl Peekable for FdbEngine {
    type DBVector = FdbDBVector;

    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<FdbDBVector>> {
        let opt: FdbReadOptions = opts.into();
        let v = self.einsteindb.get_opt(key, &opt.into_raw())?;
        Ok(v.map(FdbDBVector::from_raw))
    }

    fn get_value_namespaced_opt(
        &self,
        opts: &ReadOptions,
        namespaced: &str,
        key: &[u8],
    ) -> Result<Option<FdbDBVector>> {
        let opt: FdbReadOptions = opts.into();
        let handle = get_namespaced_handle(&self.einsteindb, namespaced)?;
        let v = self.einsteindb.get_namespaced_opt(handle, key, &opt.into_raw())?;
        Ok(v.map(FdbDBVector::from_raw))
    }
}

impl SyncMutable for FdbEngine {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.einsteindb.put(key, value).map_err(Error::Engine)
    }

    fn put_namespaced(&self, namespaced: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let handle = get_namespaced_handle(&self.einsteindb, namespaced)?;
        self.einsteindb.put_namespaced(handle, key, value).map_err(Error::Engine)
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        self.einsteindb.delete(key).map_err(Error::Engine)
    }

    fn delete_namespaced(&self, namespaced: &str, key: &[u8]) -> Result<()> {
        let handle = get_namespaced_handle(&self.einsteindb, namespaced)?;
        self.einsteindb.delete_namespaced(handle, key).map_err(Error::Engine)
    }

    fn delete_range(&self, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        self.einsteindb
            .delete_range(begin_key, end_key)
            .map_err(Error::Engine)
    }

    fn delete_range_namespaced(&self, namespaced: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        let handle = get_namespaced_handle(&self.einsteindb, namespaced)?;
        self.einsteindb
            .delete_range_namespaced(handle, begin_key, end_key)
            .map_err(Error::Engine)
    }
}

#[cfg(test)]
mod tests {
    use fdb_traits::{Iterable, KvEngine, Peekable, SyncMutable};
    use ekvproto::metapb::Region;
    use std::sync::Arc;
    use tempfile::Builder;

    use crate::{FdbEngine, FdbSnapshot};
    use crate::raw_util;

    #[test]
    fn test_base() {
        let path = Builder::new().prefix("var").temfidelir().unwrap();
        let namespaced = "namespaced";
        let engine = FdbEngine::from_db(Arc::new(
            raw_util::new_engine(path.path().to_str().unwrap(), None, &[namespaced], None).unwrap(),
        ));

        let mut r = Region::default();
        r.set_id(10);

        let key = b"key";
        engine.put_msg(key, &r).unwrap();
        engine.put_msg_namespaced(namespaced, key, &r).unwrap();

        let snap = engine.snapshot();

        let mut r1: Region = engine.get_msg(key).unwrap().unwrap();
        assert_eq!(r, r1);
        let r1_namespaced: Region = engine.get_msg_namespaced(namespaced, key).unwrap().unwrap();
        assert_eq!(r, r1_namespaced);

        let mut r2: Region = snap.get_msg(key).unwrap().unwrap();
        assert_eq!(r, r2);
        let r2_namespaced: Region = snap.get_msg_namespaced(namespaced, key).unwrap().unwrap();
        assert_eq!(r, r2_namespaced);

        r.set_id(11);
        engine.put_msg(key, &r).unwrap();
        r1 = engine.get_msg(key).unwrap().unwrap();
        r2 = snap.get_msg(key).unwrap().unwrap();
        assert_ne!(r1, r2);

        let b: Option<Region> = engine.get_msg(b"missing_key").unwrap();
        assert!(b.is_none());
    }

    #[test]
    fn test_peekable() {
        let path = Builder::new().prefix("var").temfidelir().unwrap();
        let namespaced = "namespaced";
        let engine = FdbEngine::from_db(Arc::new(
            raw_util::new_engine(path.path().to_str().unwrap(), None, &[namespaced], None).unwrap(),
        ));

        engine.put(b"k1", b"v1").unwrap();
        engine.put_namespaced(namespaced, b"k1", b"v2").unwrap();

        assert_eq!(&*engine.get_value(b"k1").unwrap().unwrap(), b"v1");
        assert!(engine.get_value_namespaced("foo", b"k1").is_err());
        assert_eq!(&*engine.get_value_namespaced(namespaced, b"k1").unwrap().unwrap(), b"v2");
    }

    #[test]
    fn test_scan() {
        let path = Builder::new().prefix("var").temfidelir().unwrap();
        let namespaced = "namespaced";
        let engine = FdbEngine::from_db(Arc::new(
            raw_util::new_engine(path.path().to_str().unwrap(), None, &[namespaced], None).unwrap(),
        ));

        engine.put(b"a1", b"v1").unwrap();
        engine.put(b"a2", b"v2").unwrap();
        engine.put_namespaced(namespaced, b"a1", b"v1").unwrap();
        engine.put_namespaced(namespaced, b"a2", b"v22").unwrap();

        let mut data = vec![];
        engine
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

        engine
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

        let pair = engine.seek(b"a1").unwrap().unwrap();
        assert_eq!(pair, (b"a1".to_vec(), b"v1".to_vec()));
        assert!(engine.seek(b"a3").unwrap().is_none());
        let pair_namespaced = engine.seek_namespaced(namespaced, b"a1").unwrap().unwrap();
        assert_eq!(pair_namespaced, (b"a1".to_vec(), b"v1".to_vec()));
        assert!(engine.seek_namespaced(namespaced, b"a3").unwrap().is_none());

        let mut index = 0;
        engine
            .scan(b"", &[0xFF, 0xFF], false, |key, value| {
                data.push((key.to_vec(), value.to_vec()));
                index += 1;
                Ok(index != 1)
            })
            .unwrap();

        assert_eq!(data.len(), 1);

        let snap = FdbSnapshot::new(engine.get_sync_db());

        engine.put(b"a3", b"v3").unwrap();
        assert!(engine.seek(b"a3").unwrap().is_some());

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
