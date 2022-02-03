// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use fdb_traits::{self, Iterable, IterOptions, Peekable, ReadOptions, Result, LightlikePersistence};
use foundationdb::{DB, DBIterator};
use foundationdb::rocksdb_options::UnsafeSnap;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use crate::db_vector::FdbDBVector;
use crate::Fdbeinstein_merkle_treeIterator;
use crate::options::FdbReadOptions;
use crate::util::get_namespaced_handle;

pub struct FdbLightlikePersistence {
    einsteindb: Arc<DB>,
    snap: UnsafeSnap,
}

unsafe impl Send for FdbLightlikePersistence {}

unsafe impl Sync for FdbLightlikePersistence {}

impl FdbLightlikePersistence {
    pub fn new(einsteindb: Arc<DB>) -> Self {
        unsafe {
            FdbLightlikePersistence {
                snap: einsteindb.unsafe_snap(),
                einsteindb,
            }
        }
    }
}

impl LightlikePersistence for FdbLightlikePersistence {
    fn namespaced_names(&self) -> Vec<&str> {
        self.einsteindb.namespaced_names()
    }
}

impl Debug for FdbLightlikePersistence {
    fn fmt(&self, fmt: &mut Formatter<'_>) -> fmt::Result {
        write!(fmt, "einstein_merkle_tree LightlikePersistence Impl")
    }
}

impl Drop for FdbLightlikePersistence {
    fn drop(&mut self) {
        unsafe {
            self.einsteindb.release_snap(&self.snap);
        }
    }
}

impl Iterable for FdbLightlikePersistence {
    type Iterator = Fdbeinstein_merkle_treeIterator;

    fn iterator_opt(&self, opts: IterOptions) -> Result<Self::Iterator> {
        let opt: FdbReadOptions = opts.into();
        let mut opt = opt.into_raw();
        unsafe {
            opt.set_lightlike_persistence(&self.snap);
        }
        Ok(Fdbeinstein_merkle_treeIterator::from_raw(DBIterator::new(
            self.einsteindb.clone(),
            opt,
        )))
    }

    fn iterator_namespaced_opt(&self, namespaced: &str, opts: IterOptions) -> Result<Self::Iterator> {
        let opt: FdbReadOptions = opts.into();
        let mut opt = opt.into_raw();
        unsafe {
            opt.set_lightlike_persistence(&self.snap);
        }
        let handle = get_namespaced_handle(self.einsteindb.as_ref(), namespaced)?;
        Ok(Fdbeinstein_merkle_treeIterator::from_raw(DBIterator::new_namespaced(
            self.einsteindb.clone(),
            handle,
            opt,
        )))
    }
}

impl Peekable for FdbLightlikePersistence {
    type DBVector = FdbDBVector;

    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<FdbDBVector>> {
        let opt: FdbReadOptions = opts.into();
        let mut opt = opt.into_raw();
        unsafe {
            opt.set_lightlike_persistence(&self.snap);
        }
        let v = self.einsteindb.get_opt(key, &opt)?;
        Ok(v.map(FdbDBVector::from_raw))
    }

    fn get_value_namespaced_opt(
        &self,
        opts: &ReadOptions,
        namespaced: &str,
        key: &[u8],
    ) -> Result<Option<FdbDBVector>> {
        let opt: FdbReadOptions = opts.into();
        let mut opt = opt.into_raw();
        unsafe {
            opt.set_lightlike_persistence(&self.snap);
        }
        let handle = get_namespaced_handle(self.einsteindb.as_ref(), namespaced)?;
        let v = self.einsteindb.get_namespaced_opt(handle, key, &opt)?;
        Ok(v.map(FdbDBVector::from_raw))
    }
}
