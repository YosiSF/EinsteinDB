// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::*;

pub trait SyncMutable {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;

    fn put_namespaced(&self, namespaced: &str, key: &[u8], value: &[u8]) -> Result<()>;

    fn delete(&self, key: &[u8]) -> Result<()>;

    fn delete_namespaced(&self, namespaced: &str, key: &[u8]) -> Result<()>;

    fn delete_range(&self, begin_key: &[u8], end_key: &[u8]) -> Result<()>;

    fn delete_range_namespaced(&self, namespaced: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()>;

    fn put_msg<M: protobuf::Message>(&self, key: &[u8], m: &M) -> Result<()> {
        self.put(key, &m.write_to_bytes()?)
    }

    fn put_msg_namespaced<M: protobuf::Message>(&self, namespaced: &str, key: &[u8], m: &M) -> Result<()> {
        self.put_namespaced(namespaced, key, &m.write_to_bytes()?)
    }
}
