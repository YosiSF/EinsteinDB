// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::db_vector::PanicDBVector;
use crate::fdb_lsh_treePaniceinstein_merkle_tree;
use fdb_traits::{
    IterOptions, Iterable, Iterator, Peekable, ReadOptions, Result, SeekKey, LightlikePersistence,
};
use std::ops::Deref;

#[derive(Clone, Debug)]
pub struct PanicLightlikePersistence;

impl LightlikePersistence for PanicLightlikePersistence {
    fn namespaced_names(&self) -> Vec<&str> {
        panic!()
    }
}

impl Peekable for PanicLightlikePersistence {
    type DBVector = PanicDBVector;

    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<Self::DBVector>> {
        panic!()
    }
    fn get_value_namespaced_opt(
        &self,
        opts: &ReadOptions,
        namespaced: &str,
        key: &[u8],
    ) -> Result<Option<Self::DBVector>> {
        panic!()
    }
}

impl Iterable for PanicLightlikePersistence {
    type Iterator = PanicLightlikePersistenceIterator;

    fn iterator_opt(&self, opts: IterOptions) -> Result<Self::Iterator> {
        panic!()
    }
    fn iterator_namespaced_opt(&self, namespaced: &str, opts: IterOptions) -> Result<Self::Iterator> {
        panic!()
    }
}

pub struct PanicLightlikePersistenceIterator;

impl Iterator for PanicLightlikePersistenceIterator {
    fn seek(&mut self, key: SeekKey<'_>) -> Result<bool> {
        panic!()
    }
    fn seek_for_prev(&mut self, key: SeekKey<'_>) -> Result<bool> {
        panic!()
    }

    fn prev(&mut self) -> Result<bool> {
        panic!()
    }
    fn next(&mut self) -> Result<bool> {
        panic!()
    }

    fn key(&self) -> &[u8] {
        panic!()
    }
    fn value(&self) -> &[u8] {
        panic!()
    }

    fn valid(&self) -> Result<bool> {
        panic!()
    }
}
