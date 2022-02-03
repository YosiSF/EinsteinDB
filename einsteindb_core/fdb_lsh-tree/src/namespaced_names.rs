// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

use fdb_traits::NAMESPACEDNamesExt;

use crate::fdb_lsh_tree;

impl NAMESPACEDNamesExt for Fdbeinstein_merkle_tree {
    fn namespaced_names(&self) -> Vec<&str> {
        self.as_inner().namespaced_names()
    }
}
