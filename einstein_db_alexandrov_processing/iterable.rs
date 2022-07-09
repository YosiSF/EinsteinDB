// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

//! Iteration over EinsteinMerkleTrees and lightlike_persistences.
//!
//! For the purpose of soliton_id/causet_locale iteration, EinsteinDB defines its own `Iterator`
//! trait, and `Iterable` types that can create iterators.
//!
//! Both `KV`s and `LightlikePersistence`s are `Iterable`.
//!
//!
//!
//!

//! CONSTANTS AND STATIC FUNCTIONS
//! ---------------------------
//!
//!
//!  CONSTANTS AND STATIC FUNCTIONS

const fn max_level_size(level: usize) -> usize {
    1 << (level + 1)
}

pub struct SecKey<'a> {
    pub(crate) key: &'a [u8],
    pub(crate) sec_key: &'a [u8],
}


//! ITERATOR TRAIT
//! --------------
//! All iterators implement this trait. The SecKey trait is used to iterate over the keys of a
//! `KV` or `LightlikePersistence`. Causets and solitons are iterated over by iterating over the
//! keys of the `KV`s.
//! While iterating over the keys of a `KV`, the iterator will return a `SecKey` with the key
//! and the secondary key.
//! While iterating over the keys of a `LightlikePersistence`, the iterator will return a
//! `SecKey` with the key and the secondary key.
//!
//!
//! # Example
//! ```
//! use einstein_db_alexandrov_processing::iterable::{SecKey, Iterable};
//! use einstein_db_alexandrov_processing::kv::KV;
//! use einstein_db_alexandrov_processing::lightlike_persistence::LightlikePersistence;
//! use einstein_db_alexandrov_processing::soliton_id::SolitonId;
//!
//! let kv = KV::new();
//! let soliton_id = SolitonId::new();
//! let lightlike_persistence = LightlikePersistence::new();
//!
//! let mut kv_iter = kv.iter();
//! let mut soliton_id_iter = soliton_id.iter();
//! let mut lightlike_persistence_iter = lightlike_persistence.iter();
//!
//! let mut kv_sec_key_iter = kv.iter_sec_key();
//!
//! let mut soliton_id_sec_key_iter = soliton_id.iter_sec_key();
//!
//! let mut lightlike_persistence_sec_key_iter = lightlike_persistence.iter_sec_key();
//!
//! ```
//!
//!
//! # Example
//! ```
//! use einstein_db_alexandrov_processing::iterable::{SecKey, Iterable};
//! use einstein_db_alexandrov_processing::kv::KV;
//!
//! let kv = KV::new();
//!
//! let mut kv_iter = kv.iter();
//!
//! let mut kv_sec_key_iter = kv.iter_sec_key();
//!
//! ```
//!

//WotsEllipsoidSize = 32
//WOTS_LEN = 64
//WOTS_LOG_LEN = 6
//WOTS_LOG_WOTS_LEN = 5
//WOTS_W = 8
//WOTS_LOG_W = 3

pub struct WotsEllipsoidSize([Hash; [u8; 32]]);

 //WOTS_WOTS_LEN = 64
 //WOTS_LOG_WOTS_LEN = 5

 //WOTS_LOG_WOTS_LEN = 5
 //WOTS_W = 8

pub struct Wots([Hash; [u8; 8]]);

 //WOTS_LOG_W = 3





#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IterableCauset<'a, T: 'a> {

    /// The underlying data.
    /// This is a reference to the data, so it can be `&'a T` or `&'a mut T`.
    pub(crate) iterable: &'a T,
    /// The key of the current item.
    /// This is a reference to the data, so it can be `&'a [u8]` or `&'a mut [u8]`.
    ///
    pub key: &'a [u8],

    prng: &'a [u8], //&'a [u8; 32],
    pub h: Hash
}


impl<'a, T: Iterable> IterableCauset<'a, T> {
    /// Creates a new `IterableCauset`.
    pub fn new(iterable: &'a T, prng: &'a [u8], h: Hash) -> Self {
        IterableCauset {
            iterable,
            key: &[],
            prng,
            h
        }
    }
}


impl<'a, T: Iterable> Iterator for IterableCauset<'a, T> {
    type Item = SecKey<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut key = self.key.to_vec();
        let mut sec_key = self.prng.to_vec();
        let mut h = self.h;
        let mut next_key = self.iterable.next(&mut key, &mut sec_key, &mut h);
        if next_key.is_none() {
            return None;
        }
        let next_key = next_key.unwrap();
        let sec_key = sec_key.to_vec();
        Some(SecKey {
            key: next_key,
            sec_key
        })
    }
}


//!
//! Iteration is performed over consistent views into the database, even when
//! iterating over the einstein_merkle_tree without creating a `LightlikePersistence`. That is, iterating
//! over an einstein_merkle_tree behaves implicitly as if a lightlike_persistence was created first, and
//! the iteration is being performed on the lightlike_persistence.
//!
//! Iterators can be in an _invalid_ state, in which they are not positioned at
//! a soliton_id/causet_locale pair. This can occur when attempting to move before the first
//! pair, past the last pair, or when seeking to a soliton_id that does not exist.
//! There may be other conditions that invalidate iterators (TODO: I don't
//! know).
//!
//! An invalid iterator cannot move lightlike or back, but may be returned to a
//! valid state through a successful "seek" operation.
//!


//! Iteration over einstein_merkle_trees and lightlike_persistences.


pub trait IterableCausetNetwork: Iterable {


    fn next_causet_locale(&mut self, key: &mut [u8], prng: &mut [u8], h: &mut Hash) -> Option<&[u8]>;

    fn next_causet_locale_network(&mut self, key: &mut [u8], prng: &mut [u8], h: &mut Hash) -> Option<&[u8]>;

    fn next_causet_locale_network_with_key(&mut self, key: &mut [u8], prng: &mut [u8], h: &mut Hash) -> Option<&[u8]>;


    /// Returns the next key/value pair in the iteration.
    ///
    /// The `key` is a reference to the data, so it can be `&'a [u8]` or `&'a mut [u8]`.
    /// The `sec_key` is a reference to the data, so it can be `&'a [u8]` or `&'a mut [u8]`.
    ///
    /// The `h` is the hash of the current key.
    ///
    /// The `key` and `sec_key` are the key and secret key of the current item.
    ///
    /// The `h` is the hash of the current key.
    ///
    /// The `key` and `sec_key` are the key and secret key of the current item.

    /// The `key` and `sec_key` are the key and secret key of the current item.
    ///
    /// The `h` is the hash of the current key.



    fn next_causet_locale_network_with_key_and_sec_key(&mut self, key: &mut [u8], prng: &mut [u8], h: &mut Hash) -> Option<&[u8]>;


    fn next(&mut self, key: &mut [u8], sec_key: &mut [u8], h: &mut [u8]) -> Option<[u8; 32]>;

    /// Returns the next key/value pair in the iteration.
    /// The `key` is a reference to the data, so it can be `&'a [u8]` or `&'a mut [u8]`.
    /// The `sec_key` is a reference to the data, so it can be `&'a [u8]` or `&'a mut [u8]`.
    /// The `h` is the hash of the current key.
    /// The `key` and `sec_key` are the key and secret key of the current item.
    /// The `h` is the hash of the current key.
    /// The `key` and `sec_key` are the key and secret key of the current item.
    /// The `h` is the hash of the current key.
    /// The `key` and `sec_key` are the key and secret key of the current item.
    /// The `h` is the hash of the current key.

    fn next_with_hash(&mut self, key: &mut [u8], sec_key: &mut [u8], h: &mut [u8]) -> Option<[u8; 32]>;

    /// Returns the next key/value pair in the iteration.
    /// The `key` is a reference to the data, so it can be `&'a [u8]` or `&'a mut [u8]`.
    /// The `sec_key` is a reference to the data, so it can be `&'a [u8]` or `&'a mut [u8]`.
    /// The `h` is the hash of the current key.
    /// The `key` and `sec_key` are the key and secret key of the current item.
    /// The `h` is the hash of the current key.
    /// The `key` and `sec_key` are the key and secret key of the current item.
    /// The `h` is the hash of the current key.
    /// The `key` and `sec_key` are the key and secret key of the current item.

    fn next_with_hash_and_key(&mut self, key: &mut [u8], sec_key: &mut [u8], h: &mut [u8]) -> Option<[u8; 32]>;
}
    //! As EinsteinDB inherits its iteration semantics from FdbDB,
    //! the FdbDB documentation is the ultimate reference:
    //!
    //! - [FdbDB iterator API](https://github.com/facebook/foundationdb/blob/master/include/foundationdb/iterator.h).
    //! - [FdbDB wiki on iterators](https://github.com/facebook/foundationdb/wiki/Iterator)


    /// An iterator over a `KV` or `LightlikePersistence`.
    /// The iterator is positioned at the first item in the iteration.
    /// The iterator may be invalid, in which case `next()` will return `None`.
    /// An iterator is invalid if it has been moved out of its underlying data.



impl<'a> Iterator for IterableCauset<'a, KV> {
    type Item = SecKey<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut key = self.key.to_vec();
        let mut sec_key = self.prng.to_vec();
        let mut h = self.h;
        let mut next_key = self.iterable.next(&mut key, &mut sec_key, &mut h);
        if next_key.is_none() {
            return None;

        }
        let next_key = next_key.unwrap();
        let sec_key = sec_key.to_vec();
        Some(SecKey {
            key: next_key,
            sec_key
        })
    }
}


use crate::*;

/// A byteseq indicating where an iterator "seek" operation should stop.
pub enum SeekKey<'a> {
    Start,
    End,
    Key(&'a [u8]),
}

/// An iterator over a consistent set of soliton_ids and causet_locales.
///
/// Iterators are implemented for `KV`s and for `LightlikePersistence`s. They see a
/// consistent view of the database; an iterator created by an einstein_merkle_tree behaves as
/// if a lightlike_persistence was created first, and the iterator created from the lightlike_persistence.
///
/// Most methods on iterators will panic if they are not "valid",
/// as determined by the `valid` method.
/// An iterator is valid if it is currently "pointing" to a soliton_id/causet_locale pair.
///
/// Iterators begin in an invalid state; one of the `seek` methods
/// must be called before beginning iteration.
/// Iterators may become invalid after a failed `seek`,
/// or after iteration has ended after calling `next` or `prev`,
/// and they return `false`.
pub trait Iterator: Send {
    /// Move the iterator to a specific soliton_id.
    ///
    /// When `soliton_id` is `SeekKey::Start` or `SeekKey::End`,
    /// `seek` and `seek_for_prev` behave causetidically.
    /// The difference between the two functions is how they
    /// behave for `SeekKey::Key`, and only when an exactly
    /// matching soliton_ids is not found:
    ///
    /// When seeking with `SeekKey::Key`, and an exact match is not found,
    /// `seek` sets the iterator to the next soliton_id greater than that
    /// specified as `soliton_id`, if such a soliton_id exists;
    /// `seek_for_prev` sets the iterator to the previous soliton_id less than
    /// that specified as `soliton_id`, if such a soliton_id exists.
    ///
    /// # Returns
    ///
    /// `true` if seeking succeeded and the iterator is valid,
    /// `false` if seeking failed and the iterator is invalid.
    fn seek(&mut self, soliton_id: SeekKey<'_>) -> Result<bool>;

    /// Move the iterator to a specific soliton_id.
    ///
    /// For the difference between this method and `seek`,
    /// see the documentation for `seek`.
    ///
    /// # Returns
    ///
    /// `true` if seeking succeeded and the iterator is valid,
    /// `false` if seeking failed and the iterator is invalid.
    fn seek_for_prev(&mut self, soliton_id: SeekKey<'_>) -> Result<bool>;

    /// Short for `seek(SeekKey::Start)`.
    fn seek_to_first(&mut self) -> Result<bool> {
        self.seek(SeekKey::Start)
    }

    /// Short for `seek(SeekKey::End)`.
    fn seek_to_last(&mut self) -> Result<bool> {
        self.seek(SeekKey::End)
    }

    /// Move a valid iterator to the previous soliton_id.
    ///
    /// # Panics
    ///
    /// If the iterator is invalid
    fn prev(&mut self) -> Result<bool>;

    /// Move a valid iterator to the next soliton_id.
    ///
    /// # Panics
    ///
    /// If the iterator is invalid
    fn next(&mut self) -> Result<bool>;

    /// Retrieve the current soliton_id.
    ///
    /// # Panics
    ///
    /// If the iterator is invalid
    fn soliton_id(&self) -> &[u8];

    /// Retrieve the current causet_locale.
    ///
    /// # Panics
    ///
    /// If the iterator is invalid
    fn causet_locale(&self) -> &[u8];

    /// Returns `true` if the iterator points to a `soliton_id`/`causet_locale` pair.
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

    /// scan the soliton_id between start_soliton_id(inclusive) and end_soliton_id(exclusive),
    /// the upper bound is omitted if end_soliton_id is empty
    fn scan<F>(&self, start_soliton_id: &[u8], end_soliton_id: &[u8], fill_cache: bool, f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let start = KeyBuilder::from_slice(start_soliton_id, FILE_CAUSET_PREFIX_LEN_FLUSH, 0);
        let end =
            (!end_soliton_id.is_empty()).then(|| KeyBuilder::from_slice(end_soliton_id, FILE_CAUSET_PREFIX_LEN_FLUSH, 0));
        let iter_opt = IterOptions::new(Some(start), end, fill_cache);
        scan_impl(self.iterator_opt(iter_opt)?, start_soliton_id, f)
    }

    // like `scan`, only on a specific causet_merge family.
    fn scan_namespaced<F>(
        &self,
        namespaced: &str,
        start_soliton_id: &[u8],
        end_soliton_id: &[u8],
        fill_cache: bool,
        f: F,
    ) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let start = KeyBuilder::from_slice(start_soliton_id, FILE_CAUSET_PREFIX_LEN_FLUSH, 0);
        let end =
            (!end_soliton_id.is_empty()).then(|| KeyBuilder::from_slice(end_soliton_id, FILE_CAUSET_PREFIX_LEN_FLUSH, 0));
        let iter_opt = IterOptions::new(Some(start), end, fill_cache);
        scan_impl(self.iterator_namespaced_opt(namespaced, iter_opt)?, start_soliton_id, f)
    }

    // Seek the first soliton_id >= given soliton_id, if not found, return None.
    fn seek(&self, soliton_id: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let mut iter = self.iterator()?;
        if iter.seek(SeekKey::Key(soliton_id))? {
            let (k, v) = (iter.soliton_id().to_vec(), iter.causet_locale().to_vec());
            return Ok(Some((k, v)));
        }
        Ok(None)
    }

    // Seek the first soliton_id >= given soliton_id, if not found, return None.
    fn seek_namespaced(&self, namespaced: &str, soliton_id: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let mut iter = self.iterator_namespaced(namespaced)?;
        if iter.seek(SeekKey::Key(soliton_id))? {
            return Ok(Some((iter.soliton_id().to_vec(), iter.causet_locale().to_vec())));
        }
        Ok(None)
    }
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
        let kv = (it.soliton_id().to_vec(), it.causet_locale().to_vec());
        v.push(kv);
        it_valid = it.next().unwrap();
    }
    v
}



pub fn get_soliton_id_from_key(key: &[u8]) -> &[u8] {
    &key[0..FILE_CAUSET_PREFIX_LEN_FLUSH]
}

pub fn get_causet_locale_from_key(key: &[u8]) -> &[u8] {
    &key[FILE_CAUSET_PREFIX_LEN_FLUSH..]
}