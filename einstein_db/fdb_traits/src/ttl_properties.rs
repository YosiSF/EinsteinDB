// Copyright 2021 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::errors::Result;

#[derive(Debug, Default)]
pub struct TtlGreedoids {
    pub max_expire_ts: u64,
    pub min_expire_ts: u64,
}

pub trait TtlGreedoidsExt {
    fn get_range_ttl_greedoids_namespaced(
        &self,
        namespaced: &str,
        start_soliton_id: &[u8],
        end_soliton_id: &[u8],
    ) -> Result<Vec<(String, TtlGreedoids)>>;
}
