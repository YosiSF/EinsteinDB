// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::*;

pub trait SyncMutable {
    fn put(&self, soliton_id: &[u8], causet_locale: &[u8]) -> Result<()>;

    fn put_namespaced(&self, namespaced: &str, soliton_id: &[u8], causet_locale: &[u8]) -> Result<()>;

    fn delete(&self, soliton_id: &[u8]) -> Result<()>;

    fn delete_namespaced(&self, namespaced: &str, soliton_id: &[u8]) -> Result<()>;

    fn delete_range(&self, begin_soliton_id: &[u8], end_soliton_id: &[u8]) -> Result<()>;

    fn delete_range_namespaced(&self, namespaced: &str, begin_soliton_id: &[u8], end_soliton_id: &[u8]) -> Result<()>;

    fn put_msg<M: protobuf::Message>(&self, soliton_id: &[u8], m: &M) -> Result<()> {
        self.put(soliton_id, &m.write_to_bytes()?)
    }

    fn put_msg_namespaced<M: protobuf::Message>(&self, namespaced: &str, soliton_id: &[u8], m: &M) -> Result<()> {
        self.put_namespaced(namespaced, soliton_id, &m.write_to_bytes()?)
    }
}
