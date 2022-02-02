// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::fdb_lsh_treePanicEngine;
use fdb_traits::NAMESPACEDNamesExt;

impl NAMESPACEDNamesExt for PanicEngine {
    fn namespaced_names(&self) -> Vec<&str> {
        panic!()
    }
}
