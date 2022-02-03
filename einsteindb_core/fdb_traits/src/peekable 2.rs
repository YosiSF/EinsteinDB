// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::*;


pub trait Peekable {
    /// The byte-vector type causet representation
    type Causet: Causet;

   
    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<Self::Causet>>;

   
    fn get_value_namespaced_opt(
        &self,
        opts: &ReadOptions,
        namespaced: &str,
        key: &[u8],
    ) -> Result<Option<Self::Causet>>;

    fn get_value(&self, key: &[u8]) -> Result<Option<Self::Causet>> {
        self.get_value_opt(&ReadOptions::default(), key)
    }

    fn get_value_namespaced(&self, namespaced: &str, key: &[u8]) -> Result<Option<Self::Causet>> {
        self.get_value_namespaced_opt(&ReadOptions::default(), namespaced, key)
    }

    /// Read a value and return it as a protobuf message.
    fn get_msg<M: protobuf::Message + Default>(&self, key: &[u8]) -> Result<Option<M>> {
        let value = self.get_value(key)?;
        if value.is_none() {
            return Ok(None);
        }

        let mut m = M::default();
        m.merge_from_bytes(&value.unwrap())?;
        Ok(Some(m))
    }

    /// Read a value and return it as a protobuf message.
    fn get_msg_namespaced<M: protobuf::Message + Default>(
        &self,
        namespaced: &str,
        key: &[u8],
    ) -> Result<Option<M>> {
        let value = self.get_value_namespaced(namespaced, key)?;
        if value.is_none() {
            return Ok(None);
        }

        let mut m = M::default();
        m.merge_from_bytes(&value.unwrap())?;
        Ok(Some(m))
    }
}
