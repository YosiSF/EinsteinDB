//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
//// AUTHORS: WHITFORD LEDER
//// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
//// this file File except in compliance with the License. You may obtain a copy of the
//// License at http://www.apache.org/licenses/LICENSE-2.0
//// Unless required by applicable law or agreed to in writing, software distributed
//// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
//// CONDITIONS OF ANY KIND, either express or implied. See the License for the
//// specific language governing permissions and limitations under the License.
//// =================================================================


// #[macro_export]
// macro_rules! result {
//     ($expr:expr) => (
//         result!($expr, $crate::Error);
//     );
//     ($expr:expr, $err:ty) => (
//         match $expr {
//             Ok(val) => val,
//             Err(err) => return Err($crate::ResultExt::failure(err)),
//         }
//     );
// }
//

use either::Either;
use std::{
    fmt::{self, Debug, Display},
    hash::{Hash, Hasher},
    marker::PhantomData,
    mem,
    ops::{Deref, DerefMut},
    str::FromStr,
};



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

///BerolinaSQL transduces the transaction log of binding parameters to AEV in the form of a tuplespace.
/// The tuplespace is a data structure that stores tuples of values.
/// The Causet becomes a datum of the tuplespace.
/// The Causet is a data structure that stores tuples of values.
///  IT operates using LSH-based indexing with embedded Merkle trees
/// and a key value store. It accepts human-first full-text indexing
/// querying of semantic data, in human language; while simultaneously
/// supporting the use of a wide range of data types.
/// In the form of Agnostic SQL a mix between a relational database and a
/// key-value store with knowledge of the semantics of the data.
///


#[must_use="The result of the operation is an iterator. Iteration is lazy."]
pub struct Causet<'a, E: Engine + 'a> {
    engine: &'a E,
    options: CausetOptions,
    phantom: PhantomData<E>,
}


impl<'a, E: Engine + 'a> Causet<'a, E> {
    pub fn new(engine: &'a E, options: CausetOptions) -> Self {
        Causet {
            engine,
            options,
            phantom: PhantomData,
        }
    }
}


impl<'a, E: Engine + 'a> Deref for Causet<'a, E> {
    type Target = E;

    fn deref(&self) -> &Self::Target {
        self.engine
    }
}


impl<'a, E: Engine + 'a> DerefMut for Causet<'a, E> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.engine
    }
}


impl<'a, E: Engine + 'a> Debug for Causet<'a, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Causet")
    }
}




/// `Compact` is a trait that provides compacting operations.
/// It is used to compact a range of keys in a table.
/// The caller must specify the type of compaction to perform.
/// The returned compaction output will be placed in `out`.
/// The output is a sequence of files.
/// If `level` is specified, the output files will be written to that level.
/// If `level` is not specified, the output files will be written to the same level as the input.
///



pub trait Compact {
    fn compact(&self, start: &[u8], end: &[u8], options: &CompactOptions, out: &mut Vec<u8>) -> Result<()>;
}


#[must_use="The result of the operation is an iterator. Iteration is lazy."]
pub struct CausetIterator<'a, E: Engine + 'a> {
    engine: &'a E,
    options: CausetIteratorOptions,
    phantom: PhantomData<E>,
}


impl<'a, E: Engine + 'a> CausetIterator<'a, E> {
    pub fn new(engine: &'a E, options: CausetIteratorOptions) -> Self {
        CausetIterator {
            engine,
            options,
            phantom: PhantomData,
        }
    }
}
pub trait TtlGreedoidsExt {
    fn get_range_ttl_greedoids_namespaced(
        &self,
        namespaced: &str,
        start_soliton_id: &[u8],
        end_soliton_id: &[u8],
    ) -> Result<Vec<(String, TtlGreedoids)>>;
}


impl TtlGreedoidsExt for Snapshot {
    fn get_range_ttl_greedoids_namespaced(
        &self,
        namespaced: &str,
        start_soliton_id: &[u8],
        end_soliton_id: &[u8],
    ) -> Result<Vec<(String, TtlGreedoids)>> {
        let mut iter = self.iter(
            SnapshotIteratorOptionsBuilder::new()
                .namespaced(namespaced)
                .start_soliton_id(start_soliton_id)
                .end_soliton_id(end_soliton_id)
                .build(),
        )?;
        let mut result = Vec::new();
        while let Some(mut entry) = iter.next()? {
            let key = entry.key();
            let value = entry.value();
            let mut ttl_greedoids = TtlGreedoids::new();
            ttl_greedoids.merge_from_bytes(&value)?;
            result.push((key.to_vec(), ttl_greedoids));
        }
        Ok(result)
    }
}

#[macro_export]
macro_rules! result {
    ($expr:expr) => (
        result!($expr, $crate::Error);
    );
    ($expr:expr, $err:ty) => (
        match $expr {
            Ok(val) => val,
            Err(err) => return Err($crate::ResultExt::failure(err)),
        }
    );
}

#[macro_export]
macro_rules! result_opt {
    ($expr:expr) => (
        result_opt!($expr, $crate::Error);
    );
    ($expr:expr, $err:ty) => (
        match $expr {
            Some(val) => val,
            None => return Err($crate::ResultExt::failure($err)),
        }
    );
}






/// Helper to build a `FieldType` protobuf message.
#[derive(Default)]
pub struct FieldTypeBuilder(FieldType);

impl FieldTypeBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn tp(mut self, v: crate::FieldTypeTp) -> Self {
        FieldTypeAccessor::set_tp(&mut self.0, v);
        self
    }

    pub fn flag(mut self, v: crate::FieldTypeFlag) -> Self {
        FieldTypeAccessor::set_flag(&mut self.0, v);
        self
    }

    pub fn flen(mut self, v: isize) -> Self {
        FieldTypeAccessor::set_flen(&mut self.0, v);
        self
    }

    pub fn decimal(mut self, v: isize) -> Self {
        FieldTypeAccessor::set_decimal(&mut self.0, v);
        self
    }

    pub fn collation(mut self, v: crate::Collation) -> Self {
        FieldTypeAccessor::set_collation(&mut self.0, v);
        self
    }

    pub fn build(self) -> FieldType {
        self.0
    }
}

impl From<FieldTypeBuilder> for FieldType {
    fn from(fp_builder: FieldTypeBuilder) -> FieldType {
        fp_builder.build()
    }
}
