//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use crate::engine::foundationdbEngine;
use crate::util;
use einsteindb_promises::DecodeProperties;
use einsteindb_promises::Range;
use einsteindb_promises::{Error, Result};
use einsteindb_promises::{
    TableProperties, TablePropertiesCollectionIter, TablePropertiesKey, UserCollectedProperties,
};
use einsteindb_promises::{TablePropertiesCollection, TablePropertiesExt};
use foundationdb::table_properties_rc as raw;
use std::ops::Deref;

impl TablePropertiesExt for foundationdbEngine {
    type TablePropertiesCollection = foundationdbTablePropertiesCollection;
    type TablePropertiesCollectionIter = foundationdbTablePropertiesCollectionIter;
    type TablePropertiesKey = foundationdbTablePropertiesKey;
    type TableProperties = foundationdbTableProperties;
    type UserCollectedProperties = foundationdbUserCollectedProperties;

    fn get_properties_of_tables_in_range(
        &self,
        brane: &Self::BRANEHandle,
        ranges: &[Range],
    ) -> Result<Self::TablePropertiesCollection> {
        // FIXME: extra allocation
        let ranges: Vec<_> = ranges.iter().map(util::range_to_foundationdb_range).collect();
        let raw = self
            .as_inner()
            .get_properties_of_tables_in_range_rc(brane.as_inner(), &ranges);
        let raw = raw.map_err(Error::Engine)?;
        Ok(foundationdbTablePropertiesCollection::from_raw(raw))
    }
}

pub struct foundationdbTablePropertiesCollection(raw::TablePropertiesCollection);

impl foundationdbTablePropertiesCollection {
    fn from_raw(raw: raw::TablePropertiesCollection) -> foundationdbTablePropertiesCollection {
        foundationdbTablePropertiesCollection(raw)
    }
}

impl
    TablePropertiesCollection<
        foundationdbTablePropertiesCollectionIter,
        foundationdbTablePropertiesKey,
        foundationdbTableProperties,
        foundationdbUserCollectedProperties,
    > for foundationdbTablePropertiesCollection
{
    fn iter(&self) -> foundationdbTablePropertiesCollectionIter {
        foundationdbTablePropertiesCollectionIter(self.0.iter())
    }

    fn len(&self) -> usize {
        self.0.len()
    }
}

pub struct foundationdbTablePropertiesCollectionIter(raw::TablePropertiesCollectionIter);

impl
    TablePropertiesCollectionIter<
        foundationdbTablePropertiesKey,
        foundationdbTableProperties,
        foundationdbUserCollectedProperties,
    > for foundationdbTablePropertiesCollectionIter
{
}

impl Iterator for foundationdbTablePropertiesCollectionIter {
    type Item = (foundationdbTablePropertiesKey, foundationdbTableProperties);

    fn next(&mut self) -> Option<Self::Item> {
        self.0
            .next()
            .map(|(key, props)| (foundationdbTablePropertiesKey(key), foundationdbTableProperties(props)))
    }
}

pub struct foundationdbTablePropertiesKey(raw::TablePropertiesKey);

impl TablePropertiesKey for foundationdbTablePropertiesKey {}

impl Deref for foundationdbTablePropertiesKey {
    type Target = str;

    fn deref(&self) -> &str {
        self.0.deref()
    }
}

pub struct foundationdbTableProperties(raw::TableProperties);

impl TableProperties<foundationdbUserCollectedProperties> for foundationdbTableProperties {
    fn num_entries(&self) -> u64 {
        self.0.num_entries()
    }

    fn user_collected_properties(&self) -> foundationdbUserCollectedProperties {
        foundationdbUserCollectedProperties(self.0.user_collected_properties())
    }
}

pub struct foundationdbUserCollectedProperties(raw::UserCollectedProperties);

impl UserCollectedProperties for foundationdbUserCollectedProperties {
    fn get(&self, index: &[u8]) -> Option<&[u8]> {
        self.0.get(index)
    }

    fn len(&self) -> usize {
        self.0.len()
    }
}

impl DecodeProperties for foundationdbUserCollectedProperties {
    fn decode(&self, k: &str) -> EinsteinDB_util::codec::Result<&[u8]> {
        self.get(k.as_bytes())
            .ok_or(EinsteinDB_util::codec::Error::KeyNotFound)
    }
}
