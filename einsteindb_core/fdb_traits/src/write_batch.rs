// Copyright 2022 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::errors::Result;
use crate::options::WriteOptions;

/// einstein_merkle_trees that can create write batches
pub trait WriteBatchExt: Sized {
    type WriteBatch: WriteBatch<Self>;
    /// `WriteBatchVec` is used for `multi_batch_write`
    type WriteBatchVec: WriteBatch<Self>;

    /// The number of puts/deletes made to a write batch before the batch should
    /// be committed with `write`.
    const WRITE_BATCH_MAX_CAUSET_KEYS: usize;


    fn support_write_batch_vec(&self) -> bool;

    fn write_batch(&self) -> Self::WriteBatch;
    fn write_batch_with_cap(&self, cap: usize) -> Self::WriteBatch;
}


pub trait Mutable: Send {
    
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()>;

    fn put_namespaced(&mut self, namespaced: &str, key: &[u8], value: &[u8]) -> Result<()>;

   
    fn delete(&mut self, key: &[u8]) -> Result<()>;


    fn delete_namespaced(&mut self, namespaced: &str, key: &[u8]) -> Result<()>;

   
    fn delete_range(&mut self, begin_key: &[u8], end_key: &[u8]) -> Result<()>;


    fn delete_range_namespaced(&mut self, namespaced: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()>;

    fn put_msg<M: protobuf::Message>(&mut self, key: &[u8], m: &M) -> Result<()> {
        self.put(key, &m.write_to_bytes()?)
    }
    fn put_msg_namespaced<M: protobuf::Message>(&mut self, namespaced: &str, key: &[u8], m: &M) -> Result<()> {
        self.put_namespaced(namespaced, key, &m.write_to_bytes()?)
    }
}

pub trait WriteBatch<E: WriteBatchExt + Sized>: Mutable {
    /// Create a WriteBatch with a given command capacity
    fn with_capacity(e: &E, cap: usize) -> Self;

    /// Commit the WriteBatch to disk with the given options
    fn write_opt(&self, opts: &WriteOptions) -> Result<()>;

    /// Commit the WriteBatch to disk atomically
    fn write(&self) -> Result<()> {
        self.write_opt(&WriteOptions::default())
    }

  
    fn data_size(&self) -> usize;

    fn count(&self) -> usize;

    fn is_empty(&self) -> bool;


    fn should_write_to_einstein_merkle_tree(&self) -> bool;


    fn clear(&mut self);


    fn set_save_point(&mut self);


    fn pop_save_point(&mut self) -> Result<()>;

    fn rollback_to_save_point(&mut self) -> Result<()>;


    fn merge(&mut self, src: Self);
}
