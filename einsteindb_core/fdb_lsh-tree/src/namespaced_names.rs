// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

use fdb_traits::NAMESPACEDNamesExt;

use crate::fdb_lsh_treeFdbEngine;

impl NAMESPACEDNamesExt for FdbEngine {
    fn namespaced_names(&self) -> Vec<&str> {
        self.as_inner().namespaced_names()
    }
}
