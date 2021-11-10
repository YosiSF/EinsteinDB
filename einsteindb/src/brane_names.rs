//Copyright 2021-2023 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use crate::db_options::foundationdbEinsteinDBOptions;
use einsteindb_promises::BlackBraneOptions;
use einstein_merkle::BlackBraneOptions as RawBRANEOptions;

#[derive(Clone)]
pub struct foundationdbBlackBraneOptions(RawBRANEOptions);

impl foundationdbBlackBraneOptions {
    pub fn from_raw(raw: RawBRANEOptions) -> foundationdbBlackBraneOptions {
        foundationdbBlackBraneOptions(raw)
    }

    pub fn into_raw(self) -> RawBRANEOptions {
        self.0
    }
}

impl BlackBraneOptions for foundationdbBlackBraneOptions {
    type EinstenDBOptions = foundationdbEinsteinDBOptions;

    fn new() -> Self {
        foundationdbBlackBraneOptions::from_raw(RawBRANEOptions::new())
    }

    fn get_level_zero_slowdown_writes_trigger(&self) -> u32 {
        self.0.get_level_zero_slowdown_writes_trigger()
    }

    fn get_level_zero_stop_writes_trigger(&self) -> u32 {
        self.0.get_level_zero_stop_writes_trigger()
    }

    fn get_soft_pending_compaction_bytes_limit(&self) -> u64 {
        self.0.get_soft_pending_compaction_bytes_limit()
    }

    fn get_hard_pending_compaction_bytes_limit(&self) -> u64 {
        self.0.get_hard_pending_compaction_bytes_limit()
    }

    fn get_block_cache_capacity(&self) -> u64 {
        self.0.get_block_cache_capacity()
    }

    fn set_block_cache_capacity(&self, capacity: u64) -> Result<(), String> {
        self.0.set_block_cache_capacity(capacity)
    }

    fn set_Einstendb_options(&mut self, opts: &Self::EinstenDBOptions) {
        self.0.set_Einstendb_options(opts.as_raw())
    }

    fn get_target_file_size_base(&self) -> u64 {
        self.0.get_target_file_size_base()
    }

    fn get_disable_auto_compactions(&self) -> bool {
        self.0.get_disable_auto_compactions()
    }
}
