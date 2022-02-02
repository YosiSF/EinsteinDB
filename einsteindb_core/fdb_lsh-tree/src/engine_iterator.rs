// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use fdb_traits::{self, Error, Result};
use foundationdb::{DB, DBIterator, SeekKey as RawSeekKey};
use std::sync::Arc;

// FIXME: Would prefer using &DB instead of Arc<DB>.  As elsewhere in
// this crate, it would require generic associated types.
pub struct FdbEngineIterator(DBIterator<Arc<DB>>);

impl FdbEngineIterator {
    pub fn from_raw(iter: DBIterator<Arc<DB>>) -> FdbEngineIterator {
        FdbEngineIterator(iter)
    }

    pub fn sequence(&self) -> Option<u64> {
        self.0.sequence()
    }
}

impl fdb_traits::Iterator for FdbEngineIterator {
    fn seek(&mut self, key: fdb_traits::SeekKey<'_>) -> Result<bool> {
        let k: FdbSeekKey<'_> = key.into();
        self.0.seek(k.into_raw()).map_err(Error::Engine)
    }

    fn seek_for_prev(&mut self, key: fdb_traits::SeekKey<'_>) -> Result<bool> {
        let k: FdbSeekKey<'_> = key.into();
        self.0.seek_for_prev(k.into_raw()).map_err(Error::Engine)
    }

    fn prev(&mut self) -> Result<bool> {
        #[cfg(not(feature = "nortcheck"))]
        if !self.valid()? {
            return Err(Error::Engine("Iterator invalid".to_string()));
        }
        self.0.prev().map_err(Error::Engine)
    }

    fn next(&mut self) -> Result<bool> {
        #[cfg(not(feature = "nortcheck"))]
        if !self.valid()? {
            return Err(Error::Engine("Iterator invalid".to_string()));
        }
        self.0.next().map_err(Error::Engine)
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
        self.0.valid().map_err(Error::Engine)
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
