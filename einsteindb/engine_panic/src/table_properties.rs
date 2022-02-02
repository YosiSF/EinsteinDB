// Copyright 2021 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::fdb_lsh_treePanicEngine;
use fdb_traits::{Range, Result};

pub struct UserCollectedProperties;
impl fdb_traits::UserCollectedProperties for UserCollectedProperties {
    fn get(&self, _: &[u8]) -> Option<&[u8]> {
        None
    }
    fn approximate_size_and_keys(&self, _: &[u8], _: &[u8]) -> Option<(usize, usize)> {
        None
    }
}

pub struct TablePropertiesCollection;
impl fdb_traits::TablePropertiesCollection for TablePropertiesCollection {
    type UserCollectedProperties = UserCollectedProperties;
    fn iter_user_collected_properties<F>(&self, _: F)
    where
        F: FnMut(&Self::UserCollectedProperties) -> bool,
    {
    }
}

impl fdb_traits::TablePropertiesExt for PanicEngine {
    type TablePropertiesCollection = TablePropertiesCollection;
    fn table_properties_collection(
        &self,
        namespaced: &str,
        ranges: &[Range<'_>],
    ) -> Result<Self::TablePropertiesCollection> {
        panic!()
    }
}
