// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.


use einsteindb_util::keybuilder::KeyBuilder;

use crate::*;

/// A byteseq indicating where an iterator "seek" operation should stop.
pub enum SeekKey<'a> {
    Start,
    End,
    Key(&'a [u8]),
}


pub trait Iterator: Send {

    fn seek(&mut self, key: SeekKey<'_>) -> Result<bool>;

    /// Move the iterator to a specific key.
    ///
    /// For the difference between this method and `seek`,
    /// see the documentation for `seek`.
    ///
    /// # Returns
    ///
    /// `true` if seeking succeeded and the iterator is valid,
    /// `false` if seeking failed and the iterator is invalid.
    fn seek_for_prev(&mut self, key: SeekKey<'_>) -> Result<bool>;

    /// Short for `seek(SeekKey::Start)`.
    fn seek_to_first(&mut self) -> Result<bool> {
        self.seek(SeekKey::Start)
    }

    /// Short for `seek(SeekKey::End)`.
    fn seek_to_last(&mut self) -> Result<bool> {
        self.seek(SeekKey::End)
    }

    /// Move a valid iterator to the previous key.
    ///
    /// # Panics
    ///
    /// If the iterator is invalid
    fn prev(&mut self) -> Result<bool>;

    /// Move a valid iterator to the next key.
    ///
    /// # Panics
    ///
    /// If the iterator is invalid
    fn next(&mut self) -> Result<bool>;

    /// Retrieve the current key.
    ///
    /// # Panics
    ///
    /// If the iterator is invalid
    fn key(&self) -> &[u8];

    /// Retrieve the current value.
    ///
    /// # Panics
    ///
    /// If the iterator is invalid
    fn value(&self) -> &[u8];

    /// Returns `true` if the iterator points to a `key`/`value` pair.
    fn valid(&self) -> Result<bool>;
}

pub trait Iterable {
    type Iterator: Iterator;

    fn iterator_opt(&self, opts: IterOptions) -> Result<Self::Iterator>;
    fn iterator_namespaced_opt(&self, namespaced: &str, opts: IterOptions) -> Result<Self::Iterator>;

    fn iterator(&self) -> Result<Self::Iterator> {
        self.iterator_opt(IterOptions::default())
    }

    fn iterator_namespaced(&self, namespaced: &str) -> Result<Self::Iterator> {
        self.iterator_namespaced_opt(namespaced, IterOptions::default())
    }

    /// scan the key between start_key(inclusive) and end_key(exclusive),
    /// the upper bound is omitted if end_key is empty
    fn scan<F>(&self, start_key: &[u8], end_key: &[u8], fill_cache: bool, f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let start = KeyBuilder::from_slice(start_key, FILE_CAUSET_PREFIX_LEN_FLUSH, 0);
        let end =
            (!end_key.is_empty()).then(|| KeyBuilder::from_slice(end_key, FILE_CAUSET_PREFIX_LEN_FLUSH, 0));
        let iter_opt = IterOptions::new(Some(start), end, fill_cache);
        scan_impl(self.iterator_opt(iter_opt)?, start_key, f)
    }

    // like `scan`, only on a specific column family.
    fn scan_namespaced<F>(
        &self,
        namespaced: &str,
        start_key: &[u8],
        end_key: &[u8],
        fill_cache: bool,
        f: F,
    ) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let start = KeyBuilder::from_slice(start_key, FILE_CAUSET_PREFIX_LEN_FLUSH, 0);
        let end =
            (!end_key.is_empty()).then(|| KeyBuilder::from_slice(end_key, FILE_CAUSET_PREFIX_LEN_FLUSH, 0));
        let iter_opt = IterOptions::new(Some(start), end, fill_cache);
        scan_impl(self.iterator_namespaced_opt(namespaced, iter_opt)?, start_key, f)
    }

    // Seek the first key >= given key, if not found, return None.
    fn seek(&self, key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let mut iter = self.iterator()?;
        if iter.seek(SeekKey::Key(key))? {
            let (k, v) = (iter.key().to_vec(), iter.value().to_vec());
            return Ok(Some((k, v)));
        }
        Ok(None)
    }

    // Seek the first key >= given key, if not found, return None.
    fn seek_namespaced(&self, namespaced: &str, key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let mut iter = self.iterator_namespaced(namespaced)?;
        if iter.seek(SeekKey::Key(key))? {
            return Ok(Some((iter.key().to_vec(), iter.value().to_vec())));
        }
        Ok(None)
    }
}

fn scan_impl<Iter, F>(mut it: Iter, start_key: &[u8], mut f: F) -> Result<()>
where
    Iter: Iterator,
    F: FnMut(&[u8], &[u8]) -> Result<bool>,
{
    let mut remained = it.seek(SeekKey::Key(start_key))?;
    while remained {
        remained = f(it.key(), it.value())? && it.next()?;
    }
    Ok(())
}

impl<'a> From<&'a [u8]> for SeekKey<'a> {
    fn from(bs: &'a [u8]) -> SeekKey<'a> {
        SeekKey::Key(bs)
    }
}

/// Collect all items of `it` into a vector, generally used for tests.
///
/// # Panics
///
/// If any errors occur during iterator.
pub fn collect<I: Iterator>(mut it: I) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut v = Vec::new();
    let mut it_valid = it.valid().unwrap();
    while it_valid {
        let kv = (it.key().to_vec(), it.value().to_vec());
        v.push(kv);
        it_valid = it.next().unwrap();
    }
    v
}
