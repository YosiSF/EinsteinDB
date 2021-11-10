use std::sync::Arc;

use einsteindb_promises::{self, Error, Result};
use foundationdb::{DBIterator, SeekKey as RawSeekKey, DB};


pub struct FoundationdbEngineIterator(DBIterator<Arc<DB>>);

impl FoundationdbEngineIterator {
    pub fn from_raw(iter: DBIterator<Arc<DB>>) -> FoundationdbEngineIterator {
        FoundationdbEngineIterator(iter)
    }
}

impl einsteindb_promises::Iterator for FoundationdbEngineIterator {
    fn seek(&mut self, key: einsteindb_promises::SeekKey) -> Result<bool> {
        let k: FoundationdbSeekKey = key.into();
        self.0.seek(k.into_raw()).map_err(Error::Engine)
    }

    fn seek_for_prev(&mut self, key: einsteindb_promises::SeekKey) -> Result<bool> {
        let k: FoundationdbSeekKey = key.into();
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


/*

Here's what the below class is doing:
1. The `into_raw` method converts the `SeekKey` into a `RawSeekKey`
2. The `from` method converts the `SeekKey` into a `FoundationdbSeekKey`
3. The `into_raw` method converts the `FoundationdbSeekKey` into a `RawSeekKey`

This is a bit of a hack, but it works.

The `into_raw` method is implemented by the `From` trait.

The `from` method is implemented by the `Into` trait.

*/
pub struct FoundationdbSeekKey<'a>(RawSeekKey<'a>);

impl<'a> FoundationdbSeekKey<'a> {
    pub fn into_raw(self) -> RawSeekKey<'a> {
        self.0
    }
}

impl<'a> From<einsteindb_promises::SeekKey<'a>> for FoundationdbSeekKey<'a> {
    fn from(key: einsteindb_promises::SeekKey<'a>) -> Self {
        let k = match key {
            einsteindb_promises::SeekKey::Start => RawSeekKey::Start,
            einsteindb_promises::SeekKey::End => RawSeekKey::End,
            einsteindb_promises::SeekKey::Key(k) => RawSeekKey::Key(k),
        };
        FoundationdbSeekKey(k);
    }
}

//use generic associated types to rewrite the function above using &DB instead of Arc<DB>
//impl<'a> Iterator for FoundationdbEngineIterator<'a> {
//    type Item = (Vec<u8>, Vec<u8>);
//
//    fn next(&mut self) -> Option<Self::Item> {
//        self.0.next()
//    }
//}

    impl Iterator for FoundationdbEngineIterator {
        type Item = (Vec<u8>, Vec<u8>);

        fn next(&mut self) -> Option<Self::Item> {
            self.0.next()
        }
    }

    impl einsteindb_promises::Iterator for FoundationdbEngineIterator {
        fn seek(&mut self, seek_key: SeekKey) -> Result<()> {
            match seek_key {
                SeekKey::Start => self.0.seek(RawSeekKey::Start),
                SeekKey::Key(key) => self.0.seek(RawSeekKey::Key(&key)),
                SeekKey::End => self.0.seek(RawSeekKey::End),
            }
            Ok(())
        }

        fn seek_for_prev(&mut self, seek_key: SeekKey) -> Result<()> {
            match seek_key {
                SeekKey::Start => self.0.seek(RawSeekKey::Start),
                SeekKey::Key(key) => self.0.seek(RawSeekKey::Key(&key)),
                SeekKey::End => self.0.seek(RawSeekKey::End),
            }
            Ok(())
        }

        fn prev(&mut self) -> Result<()> {
            self.0.prev()
        }

        fn next_no_dup_check(&mut self) -> Result<()> {
            self.0.next()
        }

        fn validate_key(&self, _: &[u8]) -> Result<()> {
            Ok(())
        }

        fn validate_value(&self, _: &[u8]) -> Result<()> {
            Ok(())
        }

        fn current_key(&self) -> &[u8] {
            self.0.key()
        }

        fn current_value(&self) -> &[u8] {
            self.0.value()
        }
    }
    impl einsteindb_promises::SeekKeyCodec for FoundationdbEngineIterator {
        fn encode_seek_key(&mut self, key: &[u8]) -> Result<()> {
            self.0.seek(RawSeekKey::Key(key))
        }
    }

    impl einsteindb_promises::Error for Error {}

    impl einsteindb_promises::EngineIterator for FoundationdbEngineIterator {
        fn encode_seek_key(&mut self, key: &[u8]) -> Result<()> {
            self.0.seek(RawSeeKey::Key(key))
        }
    }
    impl einsteindb_promises::Error for Error {}

    impl einsteindb_promises::EngineIterator for FoundationdbEngineIterator {
        fn new(db: Arc<DB>, readopts: ReadOptions) -> Result<FoundationdbEngineIterator> {
            let iter = db.iter(readopts);
            Ok(FoundationdbEngineIterator::from_raw(iter))
        }

        fn new_cf(
            db: Arc<DB>,
            cf_handle: &CFHandle,
            readopts: ReadOptions,
        ) -> Result<FoundationdbEngineIterator> {
            let iter = db.iter_cf(cf_handle, readopts);
            Ok(FoundationdbEngineIterator::from_raw(iter))
        }
    }


