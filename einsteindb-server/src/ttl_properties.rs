/// Copyright 2021 EinsteinDB Project Authors. Licensed under Apache-2.0.
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///    http://www.apache.org/licenses/LICENSE-2.0
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
/// ----------------------------------------------------------------------------
/// @author     <> @CavHack @jedisct1 @kamilskurz @rukzuk @tomaslazdik @slushie

use crate::errors::Result;
use crate::storage::{
    engine::{
        Engine,
        EngineIterator,
        EngineIteratorOptions,
        EngineIteratorOptionsBuilder,
    },
    snapshot::{
        Snapshot,
        SnapshotIterator,
        SnapshotIteratorOptions,
        SnapshotIteratorOptionsBuilder,
    },
};

#[derive(Debug, Clone)]
pub struct CompactOptions {
    pub causetq_upstream_interlock_threshold: u64,
    pub causetq_upstream_interlock_compaction_interval: u64,
    pub causetq_upstream_interlock_compaction_threshold: u64,
    pub block_size: u64,
    pub block_cache_size: u64,
    pub block_cache_shard_bits: u8,
    pub enable_bloom_filter: bool,
    pub enable_indexing: bool,
    pub index_block_size: u64,
    pub index_block_cache_size: u64,
    pub index_block_cache_shard_bits: u8,
    pub index_block_restart_interval: u64,
}


impl CompactOptions {
    pub fn new() -> Self {
        CompactOptions {
            causetq_upstream_interlock_threshold: 0,
            causetq_upstream_interlock_compaction_interval: 0,
            causetq_upstream_interlock_compaction_threshold: 0,
            block_size: 0,
            block_cache_size: 0,
            block_cache_shard_bits: 0,
            enable_bloom_filter: false,
            enable_indexing: false,
            index_block_size: 0,
            index_block_cache_size: 0,
            index_block_cache_shard_bits: 0,
            index_block_restart_interval: 0,
        }
    }
}


/// `Compact` is a trait that provides compacting operations.

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


///! A trait for engines that support TTL.
/// This trait is used by the `TtlManager` to determine if an engine supports TTL.
/// The `TtlManager` will use this trait to determine if an engine supports TTL.
/// If an engine supports TTL, it will be used by the `TtlManager` to manage TTL.
/// If an engine does not support TTL, the `TtlManager` will not use it.


pub fn json_get_string_value(json: &str, key: &str) -> Result<String> {
    let json_object = json.parse::<serde_json::Value>()?;
    let value = json_object.get(key).ok_or_else(|| {
        format!("{} not found in json", key).into()
    })?;
    Ok(value.as_str().ok_or_else(|| {
        format!("{} is not a string", key).into()
    })?.to_string())
}


pub fn json_get_u64_value(json: &str, key: &str) -> Result<u64> {
    let json_object = json.parse::<serde_json::Value>()?;
    let value = json_object.get(key).ok_or_else(|| {
        format!("{} not found in json", key).into()
    })?;
    Ok(value.as_u64().ok_or_else(|| {
        format!("{} is not a u64", key).into()
    })?)
}


pub fn json_get_i64_value(json: &str, key: &str) -> Result<i64> {
    let json_object = json.parse::<serde_json::Value>()?;
    let value = json_object.get(key).ok_or_else(|| {
        format!("{} not found in json", key).into()
    })?;
    Ok(value.as_i64().ok_or_else(|| {
        format!("{} is not a i64", key).into()
    })?)
}






