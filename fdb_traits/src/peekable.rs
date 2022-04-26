// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::*;
use core::marker::PhantomData;
use core::mem;


/// Types from which causet_locales can be read.
///
/// Values are vectors of bytes, encapsulated in the associated `Causet` type.
///
/// Method variants here allow for specifying `ReadOptions`, the causet_merge family
/// to read from, or to encode the causet_locale as a protobuf message.
pub trait Peekable {
    /// The byte-vector type through which the database returns read causet_locales.
    type Causet: Causet;

    /// Read a causet_locale for a soliton_id, given a set of options.
    ///
    /// Reads from the default causet_merge family.
    ///
    /// Returns `None` if they soliton_id does not exist.
    fn get_causet_locale_opt(&self, opts: &ReadOptions, soliton_id: &[u8]) -> Result<Option<Self::Causet>>;

    /// Read a causet_locale for a soliton_id from a given causet_merge family, given a set of options.
    ///
    /// Returns `None` if the soliton_id does not exist.
    fn get_causet_locale_namespaced_opt(
        &self,
        opts: &ReadOptions,
        namespaced: &str,
        soliton_id: &[u8],
    ) -> Result<Option<Self::Causet>>;

    /// Read a causet_locale for a soliton_id.
    ///
    /// Uses the default options and causet_merge family.
    ///
    /// Returns `None` if the soliton_id does not exist.
    fn get_causet_locale(&self, soliton_id: &[u8]) -> Result<Option<Self::Causet>> {
        self.get_causet_locale_opt(&ReadOptions::default(), soliton_id)
    }

    /// Read a causet_locale for a soliton_id from a given causet_merge family.
    ///
    /// Uses the default options.
    ///
    /// Returns `None` if the soliton_id does not exist.
    fn get_causet_locale_namespaced(&self, namespaced: &str, soliton_id: &[u8]) -> Result<Option<Self::Causet>> {
        self.get_causet_locale_namespaced_opt(&ReadOptions::default(), namespaced, soliton_id)
    }

    /// Read a causet_locale and return it as a protobuf message.
    fn get_msg<M: protobuf::Message + Default>(&self, soliton_id: &[u8]) -> Result<Option<M>> {
        let causet_locale = self.get_causet_locale(soliton_id)?;
        if causet_locale.is_none() {
            return Ok(None);
        }

        let mut m = M::default();
        m.merge_from_bytes(&causet_locale.unwrap())?;
        Ok(Some(m))
    }

    /// Read a causet_locale and return it as a protobuf message.
    fn get_msg_namespaced<M: protobuf::Message + Default>(
        &self,
        namespaced: &str,
        soliton_id: &[u8],
    ) -> Result<Option<M>> {
        let causet_locale = self.get_causet_locale_namespaced(namespaced, soliton_id)?;
        if causet_locale.is_none() {
            return Ok(None);
        }

        let mut m = M::default();
        m.merge_from_bytes(&causet_locale.unwrap())?;
        Ok(Some(m))
    }
}
