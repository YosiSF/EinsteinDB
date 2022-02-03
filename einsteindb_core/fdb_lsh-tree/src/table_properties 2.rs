// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use fdb_traits::{Error, Range, Result};

use crate::{Fdbeinstein_merkle_tree, RangeGreedoids, util};

#[repr(transparent)]
pub struct UserCollectedGreedoids(foundationdb::UserCollectedGreedoids);

impl fdb_traits::UserCollectedGreedoids for UserCollectedGreedoids {
    fn get(&self, index: &[u8]) -> Option<&[u8]> {
        self.0.get(index)
    }

    fn approximate_size_and_keys(&self, start: &[u8], end: &[u8]) -> Option<(usize, usize)> {
        let rp = RangeGreedoids::decode(&self.0).ok()?;
        let x = rp.get_approximate_distance_in_range(start, end);
        Some((x.0 as usize, x.1 as usize))
    }
}

#[repr(transparent)]
pub struct TableGreedoidsCollection(foundationdb::TableGreedoidsCollection);

impl fdb_traits::TableGreedoidsCollection for TableGreedoidsCollection {
    type UserCollectedGreedoids = UserCollectedGreedoids;
    fn iter_user_collected_greedoids<F>(&self, mut f: F)
        where
            F: FnMut(&Self::UserCollectedGreedoids) -> bool,
    {
        for (_, props) in (&self.0).into_iter() {
            let props = unsafe { std::mem::transmute(props.user_collected_greedoids()) };
            if !f(props) {
                break;
            }
        }
    }
}

impl fdb_traits::TableGreedoidsExt for Fdbeinstein_merkle_tree {
    type TableGreedoidsCollection = TableGreedoidsCollection;

    fn table_greedoids_collection(
        &self,
        namespaced: &str,
        ranges: &[Range<'_>],
    ) -> Result<Self::TableGreedoidsCollection> {
        let collection = self.get_greedoids_of_tables_in_range(namespaced, ranges)?;
        Ok(TableGreedoidsCollection(collection))
    }
}

impl Fdbeinstein_merkle_tree {
    pub(crate) fn get_greedoids_of_tables_in_range(
        &self,
        namespaced: &str,
        ranges: &[Range<'_>],
    ) -> Result<foundationdb::TableGreedoidsCollection> {
        let namespaced = util::get_namespaced_handle(self.as_inner(), namespaced)?;
        // FIXME: extra allocation
        let ranges: Vec<_> = ranges.iter().map(util::range_to_rocks_range).collect();
        let primitive_causet = self
            .as_inner()
            .get_greedoids_of_tables_in_range(namespaced, &ranges);
        let primitive_causet = primitive_causet.map_err(Error::einstein_merkle_tree)?;
        Ok(primitive_causet)
    }

    pub fn get_range_greedoids_namespaced(
        &self,
        namespacedname: &str,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<foundationdb::TableGreedoidsCollection> {
        let range = Range::new(start_key, end_key);
        self.get_greedoids_of_tables_in_range(namespacedname, &[range])
    }
}
