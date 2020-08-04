use std::sync::Arc;

use einsteindb_promises::{self, Error, Result};
use lmdb::{DBIterator, SeekKey as RawSeekKey, DB};

// FIXME: Would prefer using &DB instead of Arc<DB>.  As elsewhere in
// this crate, it would require generic associated types.
pub struct LmdbEngineIterator(DBIterator<Arc<DB>>);

impl LmdbEngineIterator {
    pub fn from_raw(iter: DBIterator<Arc<DB>>) -> LmdbEngineIterator {
        LmdbEngineIterator(iter)
    }
}

impl einsteindb_promises::Iterator for LmdbEngineIterator {
    fn seek(&mut self, key: einsteindb_promises::SeekKey) -> Result<bool> {
        let k: LmdbSeekKey = key.into();
        self.0.seek(k.into_raw()).map_err(Error::Engine)
    }

    fn seek_for_prev(&mut self, key: einsteindb_promises::SeekKey) -> Result<bool> {
        let k: LmdbSeekKey = key.into();
        self.0.seek_for_prev(k.into_raw()).map_err(Error::Engine)
    }

    fn prev(&mut self) -> Result<bool> {
        self.0.prev().map_err(Error::Engine)
    }

    fn next(&mut self) -> Result<bool> {
        self.0.next().map_err(Error::Engine)
    }

    fn key(&self) -> &[u8] {
        self.0.key()
    }

    fn value(&self) -> &[u8] {
        self.0.value()
    }

    fn valid(&self) -> Result<bool> {
        self.0.valid().map_err(Error::Engine)
    }
}

pub struct LmdbSeekKey<'a>(RawSeekKey<'a>);

impl<'a> LmdbSeekKey<'a> {
    pub fn into_raw(self) -> RawSeekKey<'a> {
        self.0
    }
}

impl<'a> From<einsteindb_promises::SeekKey<'a>> for LmdbSeekKey<'a> {
    fn from(key: einsteindb_promises::SeekKey<'a>) -> Self {
        let k = match key {
            einsteindb_promises::SeekKey::Start => RawSeekKey::Start,
            einsteindb_promises::SeekKey::End => RawSeekKey::End,
            einsteindb_promises::SeekKey::Key(k) => RawSeekKey::Key(k),
        };
        LmdbSeekKey(k)
    }
}
