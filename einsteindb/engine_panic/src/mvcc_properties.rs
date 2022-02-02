// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::fdb_lsh_treePaniceinstein_merkle_tree;
use fdb_traits::{MvccProperties, MvccPropertiesExt, Result};
use txn_types::TimeStamp;

impl MvccPropertiesExt for Paniceinstein_merkle_tree {
    fn get_mvcc_properties_namespaced(
        &self,
        namespaced: &str,
        safe_point: TimeStamp,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Option<MvccProperties> {
        panic!()
    }
}
