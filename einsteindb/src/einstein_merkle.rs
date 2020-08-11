use std::any::Any;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use einsteindb_promises::{
    Error, IterOptions, Iterable, KvEngine, Peekable, ReadOptions, Result, SyncMutable,
};
use einstein_merkle::{DBIterator, Writable, DB};

use crate::db_vector::einstein_merkleVector;
use crate::options::EinsteinMerkleReadOptions;
use crate::einstein_merkle_metrics::{
    flush_einsteindb_histogram_metrics, flush_einsteindb_iostall_properties, flush_einsteindb_properties,
    flush_einsteindb_ticker_metrics,
};
use crate::einstein_merkle_metrics_defs::{
    ENGINE_HIST_TYPES, ENGINE_TICKER_TYPES, EINSTEIN_MERKLE_ENGINE_HIST_TYPES, EINSTEIN_ MERKLE_ENGINE_TICKER_TYPES,
};
use crate::util::get_cf_handle;
use crate::{EinsteinMerkleEngineIterator, EinsteinMerkleSnapshot};

#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct EinsteinMerkleEngine(Arc<DB>);

impl EinsteinMerkleEngine {
    pub fn from_db(db: Arc<DB>) -> Self {
        EinsteinMerkleEngine(db)
    }

    pub fn from_ref(db: &Arc<DB>) -> &Self {
        unsafe { &*(db as *const Arc<DB> as *const EinsteinMerkleEngine) }
    }

    pub fn as_inner(&self) -> &Arc<DB> {
        &self.0
    }

    pub fn get_sync_db(&self) -> Arc<DB> {
        self.0.clone()
    }

    pub fn exists(path: &str) -> bool {
        let path = Path::new(path);
        if !path.exists() || !path.is_dir() {
            return false;
        }

        // If path is not an empty directory, we say db exists. If path is not an empty directory
        // but db has not been created, `DB::list_column_families` fails and we can clean up
        // the directory by this indication.
        fs::read_dir(&path).unwrap().next().is_some()
    }
}

impl KvEngine for EinsteinMerkleEngine {
    type Snapshot = EinsteinMerkleSnapshot;

    fn snapshot(&self) -> EinsteinMerkleSnapshot {
        EinsteinMerkleSnapshot::new(self.0.clone())
    }

    fn sync(&self) -> Result<()> {
        self.0.sync_wal().map_err(Error::Engine)
    }

    fn flush_metrics(&self, instance: &str, shared_block_cache: bool) {
        for t in ENGINE_TICKER_TYPES {
            let v = self.0.get_and_reset_statistics_ticker_count(*t);
            flush_einsteindb_ticker_metrics(*t, v, instance);
        }
        for t in ENGINE_HIST_TYPES {
            if let Some(v) = self.0.get_statistics_histogram(*t) {
                flush_einsteindb_histogram_metrics(*t, v, instance);
            }
        }
        if self.0.is_titan() {
            for t in EINSTEIN_ MERKLE_ENGINE_TICKER_TYPES {
                let v = self.0.get_and_reset_statistics_ticker_count(*t);
                flush_einsteindb_ticker_metrics(*t, v, instance);
            }
            for t in EINSTEIN_ MERKLE_ENGINE_HIST_TYPES {
                if let Some(v) = self.0.get_statistics_histogram(*t) {
                    flush_einsteindb_histogram_metrics(*t, v, instance);
                }
            }
        }
        flush_einsteindb_properties(&self.0, instance, shared_block_cache);
        flush_einsteindb_iostall_properties(&self.0, instance);
    }

    fn reset_statistics(&self) {
        self.0.reset_statistics();
    }

    fn bad_downcast<T: 'static>(&self) -> &T {
        let e: &dyn Any = &self.0;
        e.downcast_ref().expect("bad einsteindb downcast")
    }
}

impl Iterable for EinsteinMerkleEngine {
    type Iterator = EinsteinMerkleEngineIterator;

    fn iterator_opt(&self, opts: IterOptions) -> Result<Self::Iterator> {
        let opt: EinsteinMerkleReadOptions = opts.into();
        Ok(EinsteinMerkleEngineIterator::from_raw(DBIterator::new(
            self.0.clone(),
            opt.into_raw(),
        )))
    }

    fn iterator_cf_opt(&self, brane: &str, opts: IterOptions) -> Result<Self::Iterator> {
        let handle = get_cf_handle(&self.0, brane)?;
        let opt: EinsteinMerkleReadOptions = opts.into();
        Ok(EinsteinMerkleEngineIterator::from_raw(DBIterator::new_cf(
            self.0.clone(),
            handle,
            opt.into_raw(),
        )))
    }
}

impl Peekable for EinsteinMerkleEngine {
    type DBVector = einstein_merkleVector;

    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<einstein_merkleVector>> {
        let opt: EinsteinMerkleReadOptions = opts.into();
        let v = self.0.get_opt(key, &opt.into_raw())?;
        Ok(v.map(einstein_merkleVector::from_raw))
    }

    fn get_value_cf_opt(
        &self,
        opts: &ReadOptions,
        brane: &str,
        key: &[u8],
    ) -> Result<Option<einstein_merkleVector>> {
        let opt: EinsteinMerkleReadOptions = opts.into();
        let handle = get_cf_handle(&self.0, brane)?;
        let v = self.0.get_cf_opt(handle, key, &opt.into_raw())?;
        Ok(v.map(einstein_merkleVector::from_raw))
    }
}

impl SyncMutable for EinsteinMerkleEngine {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.0.put(key, value).map_err(Error::Engine)
    }

    fn put_cf(&self, brane: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let handle = get_cf_handle(&self.0, brane)?;
        self.0.put_cf(handle, key, value).map_err(Error::Engine)
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        self.0.delete(key).map_err(Error::Engine)
    }

    fn delete_cf(&self, brane: &str, key: &[u8]) -> Result<()> {
        let handle = get_cf_handle(&self.0, brane)?;
        self.0.delete_cf(handle, key).map_err(Error::Engine)
    }

    fn delete_range_cf(&self, brane: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        let handle = get_cf_handle(&self.0, brane)?;
        self.0
            .delete_range_cf(handle, begin_key, end_key)
            .map_err(Error::Engine)
    }
}

#[cfg(test)]
mod tests {
    use crate::raw_util;
    use einsteindb_promises::{Iterable, KvEngine, Peekable, SyncMutable};
    use ekvproto::metapb::Region;
    use std::sync::Arc;
    use tempfile::Builder;

    use crate::{EinsteinMerkleEngine, EinsteinMerkleSnapshot};

    #[test]
    fn test_base() {
        let path = Builder::new().prefix("var").temFIDelir().unwrap();
        let brane = "brane";
        let einsteindb = EinsteinMerkleEngine::from_db(Arc::new(
            raw_util::new_einsteindb(path.path().to_str().unwrap(), None, &[brane], None).unwrap(),
        ));

        let mut r = Region::default();
        r.set_id(10);

        let key = b"key";
        einsteindb.put_msg(key, &r).unwrap();
        einsteindb.put_msg_cf(brane, key, &r).unwrap();

        let snap = einsteindb.snapshot();

        let mut r1: Region = einsteindb.get_msg(key).unwrap().unwrap();
        assert_eq!(r, r1);
        let r1_cf: Region = einsteindb.get_msg_cf(brane, key).unwrap().unwrap();
        assert_eq!(r, r1_cf);

        let mut r2: Region = snap.get_msg(key).unwrap().unwrap();
        assert_eq!(r, r2);
        let r2_cf: Region = snap.get_msg_cf(brane, key).unwrap().unwrap();
        assert_eq!(r, r2_cf);

        r.set_id(11);
        einsteindb.put_msg(key, &r).unwrap();
        r1 = einsteindb.get_msg(key).unwrap().unwrap();
        r2 = snap.get_msg(key).unwrap().unwrap();
        assert_ne!(r1, r2);

        let b: Option<Region> = einsteindb.get_msg(b"missing_key").unwrap();
        assert!(b.is_none());
    }

    #[test]
    fn test_peekable() {
        let path = Builder::new().prefix("var").temFIDelir().unwrap();
        let brane = "brane";
        let einsteindb = EinsteinMerkleEngine::from_db(Arc::new(
            raw_util::new_einsteindb(path.path().to_str().unwrap(), None, &[brane], None).unwrap(),
        ));

        einsteindb.put(b"k1", b"v1").unwrap();
        einsteindb.put_cf(brane, b"k1", b"v2").unwrap();

        assert_eq!(&*einsteindb.get_value(b"k1").unwrap().unwrap(), b"v1");
        assert!(einsteindb.get_value_cf("foo", b"k1").is_err());
        assert_eq!(&*einsteindb.get_value_cf(brane, b"k1").unwrap().unwrap(), b"v2");
    }

    #[test]
    fn test_scan() {
        let path = Builder::new().prefix("var").temFIDelir().unwrap();
        let brane = "brane";
        let einsteindb = EinsteinMerkleEngine::from_db(Arc::new(
            raw_util::new_einsteindb(path.path().to_str().unwrap(), None, &[brane], None).unwrap(),
        ));

        einsteindb.put(b"a1", b"v1").unwrap();
        einsteindb.put(b"a2", b"v2").unwrap();
        einsteindb.put_cf(brane, b"a1", b"v1").unwrap();
        einsteindb.put_cf(brane, b"a2", b"v22").unwrap();

        let mut data = vec![];
        einsteindb
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

        einsteindb
            .scan_cf(brane, b"", &[0xFF, 0xFF], false, |key, value| {
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

        let pair = einsteindb.seek(b"a1").unwrap().unwrap();
        assert_eq!(pair, (b"a1".to_vec(), b"v1".to_vec()));
        assert!(einsteindb.seek(b"a3").unwrap().is_none());
        let pair_cf = einsteindb.seek_cf(brane, b"a1").unwrap().unwrap();
        assert_eq!(pair_cf, (b"a1".to_vec(), b"v1".to_vec()));
        assert!(einsteindb.seek_cf(brane, b"a3").unwrap().is_none());

        let mut index = 0;
        einsteindb
            .scan(b"", &[0xFF, 0xFF], false, |key, value| {
                data.push((key.to_vec(), value.to_vec()));
                index += 1;
                Ok(index != 1)
            })
            .unwrap();

        assert_eq!(data.len(), 1);

        let snap = EinsteinMerkleSnapshot::new(einsteindb.get_sync_db());

        einsteindb.put(b"a3", b"v3").unwrap();
        assert!(einsteindb.seek(b"a3").unwrap().is_some());

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
