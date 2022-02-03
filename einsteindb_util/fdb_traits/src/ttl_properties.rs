// Copyright 2021 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::errors::Result;

#[derive(Debug, Default)]
pub struct TtlProperties {
    pub max_expire_ts: u64,
    pub min_expire_ts: u64,
}

pub trait TtlPropertiesExt {
    fn get_range_ttl_properties_namespaced(
        &self,
        namespaced: &str,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<Vec<(String, TtlProperties)>>;
}
