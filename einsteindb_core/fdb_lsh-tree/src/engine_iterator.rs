// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use fdb_traits::{self, Error, Result};
use foundationdb::{EINSTEINDB, DBIterator, SeekKey as RawSeekKey};
use std::sync::Arc;

// FIXME: Would prefer using &EINSTEINDB instead of Arc<EINSTEINDB>.  As elsewhere in
// this crate, it would require generic associated types.
pub struct Fdbeinstein_merkle_treeIterator(DBIterator<Arc<EINSTEINDB>>);

impl Fdbeinstein_merkle_treeIterator {
    pub fn from_raw(iter: DBIterator<Arc<EINSTEINDB>>) -> Fdbeinstein_merkle_treeIterator {
        Fdbeinstein_merkle_treeIterator(iter)
    }

    pub fn sequence(&self) -> Option<u64> {
        self.0.sequence()
    }
}

impl fdb_traits::Iterator for Fdbeinstein_merkle_treeIterator {
    fn seek(&mut self, key: fdb_traits::SeekKey<'_>) -> Result<bool> {
        let k: FdbSeekKey<'_> = key.into();
        self.0.seek(k.into_raw()).map_err(Error::einstein_merkle_tree)
    }

    fn seek_for_prev(&mut self, key: fdb_traits::SeekKey<'_>) -> Result<bool> {
        let k: FdbSeekKey<'_> = key.into();
        self.0.seek_for_prev(k.into_raw()).map_err(Error::einstein_merkle_tree)
    }

    fn prev(&mut self) -> Result<bool> {
        #[cfg(not(feature = "nortcheck"))]
        if !self.valid()? {
            return Err(Error::einstein_merkle_tree("Iterator invalid".to_string()));
        }
        self.0.prev().map_err(Error::einstein_merkle_tree)
    }

    fn next(&mut self) -> Result<bool> {
        #[cfg(not(feature = "nortcheck"))]
        if !self.valid()? {
            return Err(Error::einstein_merkle_tree("Iterator invalid".to_string()));
        }
        self.0.next().map_err(Error::einstein_merkle_tree)
    }

    fn key(&self) -> &[u8] {
        #[cfg(not(feature = "nortcheck"))]
        assert!(self.valid().unwrap());
        self.0.key()
    }

    fn value(&self) -> &[u8] {
        #[cfg(not(feature = "nortcheck"))]
        assert!(self.valid().unwrap());
        self.0.value()
    }

    fn valid(&self) -> Result<bool> {
        self.0.valid().map_err(Error::einstein_merkle_tree)
    }
}

pub struct FdbSeekKey<'a>(RawSeekKey<'a>);

impl<'a> FdbSeekKey<'a> {
    pub fn into_raw(self) -> RawSeekKey<'a> {
        self.0
    }
}

impl<'a> From<fdb_traits::SeekKey<'a>> for FdbSeekKey<'a> {
    fn from(key: fdb_traits::SeekKey<'a>) -> Self {
        let k = match key {
            fdb_traits::SeekKey::Start => RawSeekKey::Start,
            fdb_traits::SeekKey::End => RawSeekKey::End,
            fdb_traits::SeekKey::Key(k) => RawSeekKey::Key(k),
        };
        FdbSeekKey(k)
    }
}
