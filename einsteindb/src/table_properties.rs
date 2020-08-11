//Copyright 2020 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use crate::engine::LmdbEngine;
use crate::util;
use einsteindb_promises::DecodeProperties;
use einsteindb_promises::Range;
use einsteindb_promises::{Error, Result};
use einsteindb_promises::{
    TableProperties, TablePropertiesCollectionIter, TablePropertiesKey, UserCollectedProperties,
};
use einsteindb_promises::{TablePropertiesCollection, TablePropertiesExt};
use lmdb::table_properties_rc as raw;
use std::ops::Deref;

impl TablePropertiesExt for LmdbEngine {
    type TablePropertiesCollection = LmdbTablePropertiesCollection;
    type TablePropertiesCollectionIter = LmdbTablePropertiesCollectionIter;
    type TablePropertiesKey = LmdbTablePropertiesKey;
    type TableProperties = LmdbTableProperties;
    type UserCollectedProperties = LmdbUserCollectedProperties;

    fn get_properties_of_tables_in_range(
        &self,
        brane: &Self::CFHandle,
        ranges: &[Range],
    ) -> Result<Self::TablePropertiesCollection> {
        // FIXME: extra allocation
        let ranges: Vec<_> = ranges.iter().map(util::range_to_lmdb_range).collect();
        let raw = self
            .as_inner()
            .get_properties_of_tables_in_range_rc(brane.as_inner(), &ranges);
        let raw = raw.map_err(Error::Engine)?;
        Ok(LmdbTablePropertiesCollection::from_raw(raw))
    }
}

pub struct LmdbTablePropertiesCollection(raw::TablePropertiesCollection);

impl LmdbTablePropertiesCollection {
    fn from_raw(raw: raw::TablePropertiesCollection) -> LmdbTablePropertiesCollection {
        LmdbTablePropertiesCollection(raw)
    }
}

impl
    TablePropertiesCollection<
        LmdbTablePropertiesCollectionIter,
        LmdbTablePropertiesKey,
        LmdbTableProperties,
        LmdbUserCollectedProperties,
    > for LmdbTablePropertiesCollection
{
    fn iter(&self) -> LmdbTablePropertiesCollectionIter {
        LmdbTablePropertiesCollectionIter(self.0.iter())
    }

    fn len(&self) -> usize {
        self.0.len()
    }
}

pub struct LmdbTablePropertiesCollectionIter(raw::TablePropertiesCollectionIter);

impl
    TablePropertiesCollectionIter<
        LmdbTablePropertiesKey,
        LmdbTableProperties,
        LmdbUserCollectedProperties,
    > for LmdbTablePropertiesCollectionIter
{
}

impl Iterator for LmdbTablePropertiesCollectionIter {
    type Item = (LmdbTablePropertiesKey, LmdbTableProperties);

    fn next(&mut self) -> Option<Self::Item> {
        self.0
            .next()
            .map(|(key, props)| (LmdbTablePropertiesKey(key), LmdbTableProperties(props)))
    }
}

pub struct LmdbTablePropertiesKey(raw::TablePropertiesKey);

impl TablePropertiesKey for LmdbTablePropertiesKey {}

impl Deref for LmdbTablePropertiesKey {
    type Target = str;

    fn deref(&self) -> &str {
        self.0.deref()
    }
}

pub struct LmdbTableProperties(raw::TableProperties);

impl TableProperties<LmdbUserCollectedProperties> for LmdbTableProperties {
    fn num_entries(&self) -> u64 {
        self.0.num_entries()
    }

    fn user_collected_properties(&self) -> LmdbUserCollectedProperties {
        LmdbUserCollectedProperties(self.0.user_collected_properties())
    }
}

pub struct LmdbUserCollectedProperties(raw::UserCollectedProperties);

impl UserCollectedProperties for LmdbUserCollectedProperties {
    fn get(&self, index: &[u8]) -> Option<&[u8]> {
        self.0.get(index)
    }

    fn len(&self) -> usize {
        self.0.len()
    }
}

impl DecodeProperties for LmdbUserCollectedProperties {
    fn decode(&self, k: &str) -> EinsteinDB_util::codec::Result<&[u8]> {
        self.get(k.as_bytes())
            .ok_or(EinsteinDB_util::codec::Error::KeyNotFound)
    }
}
