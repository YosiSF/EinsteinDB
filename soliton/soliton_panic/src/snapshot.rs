// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use fdb_traits::{
    Iterable, Iterator, IterOptions, LightlikePersistence, Peekable, ReadOptions, Result, SeekKey,
};
use std::ops::Deref;

use crate::db_vector::PanicCauset;
use crate::fdb_lsh_treePaniceinstein_merkle_tree;

#[derive(Clone, Debug)]
pub struct PanicLightlikePersistence;

impl LightlikePersistence for PanicLightlikePersistence {
    fn namespaced_names(&self) -> Vec<&str> {
        panic!()
    }
}

impl Peekable for PanicLightlikePersistence {
    type Causet = PanicCauset;

    fn get_causet_locale_opt(&self, opts: &ReadOptions, soliton_id: &[u8]) -> Result<Option<Self::Causet>> {
        panic!()
    }
    fn get_causet_locale_namespaced_opt(
        &self,
        opts: &ReadOptions,
        namespaced: &str,
        soliton_id: &[u8],
    ) -> Result<Option<Self::Causet>> {
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
    fn seek(&mut self, soliton_id: SeekKey<'_>) -> Result<bool> {
        panic!()
    }
    fn seek_for_prev(&mut self, soliton_id: SeekKey<'_>) -> Result<bool> {
        panic!()
    }

    fn prev(&mut self) -> Result<bool> {
        panic!()
    }
    fn next(&mut self) -> Result<bool> {
        panic!()
    }

    fn soliton_id(&self) -> &[u8] {
        panic!()
    }
    fn causet_locale(&self) -> &[u8] {
        panic!()
    }

    fn valid(&self) -> Result<bool> {
        panic!()
    }
}
