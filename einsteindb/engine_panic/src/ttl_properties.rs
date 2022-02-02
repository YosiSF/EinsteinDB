// Copyright 2021 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::fdb_lsh_treePanicEngine;
use fdb_traits::{Result, TtlProperties, TtlPropertiesExt};

impl TtlPropertiesExt for PanicEngine {
    fn get_range_ttl_properties_namespaced(
        &self,
        namespaced: &str,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<Vec<(String, TtlProperties)>> {
        panic!()
    }
}
