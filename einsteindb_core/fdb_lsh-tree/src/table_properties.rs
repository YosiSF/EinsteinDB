// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use fdb_traits::{Error, Range, Result};

use crate::{Fdbeinstein_merkle_tree, RangeProperties, util};

#[repr(transparent)]
pub struct UserCollectedProperties(foundationdb::UserCollectedProperties);

impl fdb_traits::UserCollectedProperties for UserCollectedProperties {
    fn get(&self, index: &[u8]) -> Option<&[u8]> {
        self.0.get(index)
    }

    fn approximate_size_and_keys(&self, start: &[u8], end: &[u8]) -> Option<(usize, usize)> {
        let rp = RangeProperties::decode(&self.0).ok()?;
        let x = rp.get_approximate_distance_in_range(start, end);
        Some((x.0 as usize, x.1 as usize))
    }
}

#[repr(transparent)]
pub struct TablePropertiesCollection(foundationdb::TablePropertiesCollection);

impl fdb_traits::TablePropertiesCollection for TablePropertiesCollection {
    type UserCollectedProperties = UserCollectedProperties;
    fn iter_user_collected_properties<F>(&self, mut f: F)
        where
            F: FnMut(&Self::UserCollectedProperties) -> bool,
    {
        for (_, props) in (&self.0).into_iter() {
            let props = unsafe { std::mem::transmute(props.user_collected_properties()) };
            if !f(props) {
                break;
            }
        }
    }
}

impl fdb_traits::TablePropertiesExt for Fdbeinstein_merkle_tree {
    type TablePropertiesCollection = TablePropertiesCollection;

    fn table_properties_collection(
        &self,
        namespaced: &str,
        ranges: &[Range<'_>],
    ) -> Result<Self::TablePropertiesCollection> {
        let collection = self.get_properties_of_tables_in_range(namespaced, ranges)?;
        Ok(TablePropertiesCollection(collection))
    }
}

impl Fdbeinstein_merkle_tree {
    pub(crate) fn get_properties_of_tables_in_range(
        &self,
        namespaced: &str,
        ranges: &[Range<'_>],
    ) -> Result<foundationdb::TablePropertiesCollection> {
        let namespaced = util::get_namespaced_handle(self.as_inner(), namespaced)?;
        // FIXME: extra allocation
        let ranges: Vec<_> = ranges.iter().map(util::range_to_rocks_range).collect();
        let raw = self
            .as_inner()
            .get_properties_of_tables_in_range(namespaced, &ranges);
        let raw = raw.map_err(Error::einstein_merkle_tree)?;
        Ok(raw)
    }

    pub fn get_range_properties_namespaced(
        &self,
        namespacedname: &str,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<foundationdb::TablePropertiesCollection> {
        let range = Range::new(start_key, end_key);
        self.get_properties_of_tables_in_range(namespacedname, &[range])
    }
}
