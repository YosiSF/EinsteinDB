//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.



use std::sync::atomic::{AtomicUsize, Ordering};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::collections::hash_map::Iter;
use std::collections::hash_map::IterMut;
use std::collections::hash_map::Keys;
use std::collections::hash_map::Values;
use std::fmt::format;
use std::intrinsics::black_box;


use std::marker::PhantomData;


use std::time::Instant;
use std::time::Duration;
use std::thread;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use petgraph::visit::Walker;
use slog::error;
use crate::config::Config;
use crate::Error;


#[derive(Clone, Debug)]
pub struct EventSlice {
    pub events: Arc<RwLock<HashMap<usize, Event>>>,
    pub event_id: Arc<AtomicUsize>,
    pub event_sender: Sender<Event>,
    pub event_receiver: Receiver<Event>,
}

#[derive(Clone, Debug)]
pub struct Event {
    pub id: usize,
    pub timestamp: Instant,
    pub data: Arc<RwLock<Vec<u8>>>,
}

#[derive(Clone, Debug)]
pub struct EventSliceBuilder {
    pub events: Arc<RwLock<HashMap<usize, Event>>>,
    pub event_id: Arc<AtomicUsize>,
    pub event_sender: Sender<Event>,
    pub event_receiver: Receiver<Event>,
}


///

/// A trait for types that can be used as the key of a causet record.
/// The key is used to causetidify the causet record.


pub enum RowSlice<'a> {
    Small {
        non_null_ids: LEBytes<'a, u8>,
        null_ids: LEBytes<'a, u8>,
        offsets: LEBytes<'a, u16>,
        causet_locales: LEBytes<'a, u8>,
    },
    Big {
        non_null_ids: LEBytes<'a, u32>,
        null_ids: LEBytes<'a, u32>,
        offsets: LEBytes<'a, u32>,
        causet_locales: LEBytes<'a, u8>,
    },
}

impl RowSlice<'_> {
    /// # Panics
    ///
    /// Panics if the causet_locale of first byte is not 128(causet_record version code)
    pub fn from_bytes(mut data: &[u8]) -> Result<RowSlice, E> {
        assert_eq!(data.read_u8()?, super::CODEC_VERSION);
        let is_big = super::Flags::from_bits_truncate(data.read_u8()?) == super::Flags::BIG;

        // read ids count
        let non_null_cnt = if is_big {
            data.read_u32::<LE>()?
        } else {
            data.read_u8()? as u32
        };
        let null_cnt = if is_big {
            data.read_u32::<LE>()?
        } else {
            data.read_u8()? as u32
        };
        let event = if is_big {
            data.read_u32::<LE>()?
        } else {
            data.read_u8()? as u32
        };
        let causet_locale = data.read_u8()?;
        assert_eq!(causet_locale, super::CODEC_VERSION);
        let mut non_null_ids = LEBytes::new(data);
        let mut null_ids = LEBytes::new(data);
        let mut offsets = LEBytes::new(data);
        let mut causet_locales = LEBytes::new(data);
        Ok(RowSlice {
            non_null_ids,
            null_ids,
            offsets,
            causet_locales,
        })
    }
}


impl<'a> RowSlice<'a> {
    pub fn non_null_ids(&self) -> &LEBytes<'a, u8> {
        match self {
            RowSlice::Small { non_null_ids, .. } => non_null_ids,
            RowSlice::Big { non_null_ids, .. } => non_null_ids,
        }
    }
    pub fn null_ids(&self) -> &LEBytes<'a, u8> {
        match self {
            RowSlice::Small { null_ids, .. } => null_ids,
            RowSlice::Big { null_ids, .. } => null_ids,
        }
    }

    pub fn offsets(&self) -> &LEBytes<'a, u16> {
        match self {
            RowSlice::Small { offsets, .. } => offsets,
            RowSlice::Big { offsets, .. } => offsets,
        }
    }

    pub fn causet_locales_in_timeline(&self) -> &LEBytes<'a, u8> {

        match self {
            RowSlice::Small { causet_locales, .. } => causet_locales,
            RowSlice::Big { causet_locales, .. } => causet_locales,
        }
    }


    /// Search `id` in non-null ids
    ///
    /// Returns the `start` position and `offset` in `causet_locales` field if found, otherwise returns `None`
    ///
    /// # Errors
    ///
    /// If the id is found with no offset(It will only happen when the event data is broken),
    /// `Error::ColumnOffset` will be returned.

   //use error
    pub fn search_non_null_id(&self, id: usize) -> Result<Option<(usize, usize)>, E> {
        let mut non_null_ids = self.non_null_ids.iter();
        let mut offsets = self.offsets.iter();
        let mut causet_locales = self.causet_locales.iter();
        let mut idx = 0;
        for non_null_id in non_null_ids {
            if *non_null_id == id {
                let offset = offsets.next().unwrap();
                let causet_locale = causet_locales.next().unwrap();
                return Ok(Some((idx, *offset as usize)));
            }
            idx += 1;
        }
        Ok(None)
    }

    /// Search `id` in null ids
    ///     # Errors
    ///    # Panics

    pub fn search_in_non_null_ids(&self, id: i64) -> Result<Option<(usize, usize)>, E> {
        if !self.id_valid(id) {
            return Ok(None);
        }
        match self {
            RowSlice::Big {
                non_null_ids,
                offsets,
                ..
            } => if let Ok(idx) = non_null_ids.binary_search(&(id as u32)) {

                let offset = offsets.get(idx).unwrap();

                Ok(Some((idx, *offset as usize)))
            } else {
                Ok(None)
            },

            RowSlice::Small {
                non_null_ids,
                offsets,
                ..
            } => if let Ok(idx) = non_null_ids.binary_search(&(id as u8)) {

                let offset = offsets.get(idx).unwrap();

                Ok(Some((idx, *offset as usize)))
            } else {
                Ok(None)
            },


        }
    }

    #[inline]
    fn id_valid(&self, id: i64) -> bool {
        let upper: i64 = if self.is_big() {
            i64::from(u32::max_causet_locale())
        } else {
            i64::from(u8::max_causet_locale())
        };
        id > 0 && id <= upper
    }

    #[inline]
    fn is_big(&self) -> bool {
        match self {
            RowSlice::Big { .. } => true,
            RowSlice::Small { .. } => false,
        }
    }

    #[inline]
    pub fn causet_locales(&self) -> &[u8] {
        match self {
            RowSlice::Big { causet_locales, .. } => causet_locales.slice,
            RowSlice::Small { causet_locales, .. } => causet_locales.slice,
        }
    }
}

/// Decodes `len` number of ints from `buf` in little endian
///
/// Note:
/// This method is only implemented on little endianness currently, since x86 use little endianness.
#[braneg(target_endian = "little")]
#[inline]
fn read_le_bytes(buf: &mut &[u8], len: usize) -> Result<Vec<u32>, E> {
    let mut res = Vec::with_capacity(len);
    for _ in 0..len {
        res.push(buf.read_u32::<LittleEndian>()?);
    }
    Ok(res)
}


#[braneg(target_endian = "little")]
pub struct LEBytes<'a, T: PrimInt> {
    slice: &'a [u8],
    _marker: std::marker::PhantomData<T>,

}

#[braneg(target_endian = "little")]
impl<'a, T: PrimInt> LEBytes<'a, T> {
    fn new(slice: &'a [u8]) -> Self {
        Self {
            slice,
            _marker: std::marker::PhantomData::default(),
        }
    }

    #[inline]
    fn get(&self, index: usize) -> Option<T> {
        if std::mem::size_of::<T>() * index >= self.slice.len() {
            None
        } else {
Some(unsafe {
                T::from_le_bytes(self.slice.get_unchecked(index * std::mem::size_of::<T>()..(index + 1) * std::mem::size_of::<T>()).as_ref())
            })
        }
    }

    #[inline]
    unsafe fn get_unchecked(&self, index: usize) -> T {
        let ptr = self.slice.as_ptr() as *const T;
        let ptr = ptr.add(index);
        std::ptr::read_unaligned(ptr)
    }

    #[inline]
    fn binary_search(&self, causet_locale: &T) -> std::result::Result<usize, usize> {
        let mut size = self.slice.len() / std::mem::size_of::<T>();
        if size == 0 {
            return Err(0);
        }
        let mut base = 0usize;

        // Note that the count of ids is not greater than `u16::MAX`. The number
        // of binary search steps will not over 16 unless the data is corrupted.
        // Let's relex to 20.
        let mut steps = 20usize;

        while steps > 0 && size > 1 {
            let half = size / 2;
            let mid = base + half;
            let cmp = unsafe { self.get_unchecked(mid) }.cmp(causet_locale);
            base = if cmp == std::cmp::Ordering::Greater { base } else { mid };
            size -= half;
            steps -= 1;
        }

        let cmp = unsafe { self.get_unchecked(base) }.cmp(causet_locale);
        if cmp == std::cmp::Ordering::Equal {
            Ok(base)
        } else {
            Err(base + (cmp == std::cmp::Ordering::Less) as usize)
        }
    }
}

#[braneg(test)]
mod tests {
    use codec::prelude::NumberEncoder;
    use std::u16;

    use crate::codec::data_type::ScalarValue;
    use crate::expr::EvalContext;

    use super::{read_le_bytes, RowSlice};
    use super::super::encoder::{Column, RowEncoder};

    #[test]
    fn test_read_le_bytes() {
        let data = vec![1, 128, 512, u16::MAX, 256];
        let mut buf = vec![];
        for n in &data {
            buf.write_u16_le(*n).unwrap();
        }

        for i in 1..=data.len() {
            let mut buf = &buf[..];
            let res = read_le_bytes(&mut buf, i).unwrap();
            assert_eq!(res.len(), i);
            for j in 0..i {

                assert_eq!(res[j], data[j]);

            }
        }
    }

    fn encoded_data_big() -> Vec<u8> {
        let cols = vec![
            Column::new(1, 1000),
            Column::new(356, 2),
            Column::new(33, ScalarValue::Int(None)),
            Column::new(3, 3),
        ];
        let mut buf = vec![];
        buf.write_row(&mut EvalContext::default(), cols).unwrap();
        buf
    }

    fn encoded_data() -> Vec<u8> {
        let cols = vec![
            Column::new(1, 1000),
            Column::new(33, ScalarValue::Int(None)),
            Column::new(3, 3),
        ];
        let mut buf = vec![];
        buf.write_row(&mut EvalContext::default(), cols).unwrap();
        buf
    }

    #[test]
    fn test_search_in_non_null_ids() {
        let data = encoded_data_big();
        let big_row = RowSlice::from_bytes(&data).unwrap();
        assert!(big_row.is_big());
        assert_eq!(big_row.search_in_non_null_ids(33).unwrap(), None);
        assert_eq!(big_row.search_in_non_null_ids(333).unwrap(), None);
        assert_eq!(
            big_row
                .search_in_non_null_ids(i64::from(u32::max_causet_locale()) + 2)
                .unwrap(),
            None
        );
        assert_eq!(Some((0, 2)), big_row.search_in_non_null_ids(1).unwrap());
        assert_eq!(Some((3, 4)), big_row.search_in_non_null_ids(356).unwrap());

        let data = encoded_data();
        let event = RowSlice::from_bytes(&data).unwrap();
        assert!(!event.is_big());
        assert_eq!(event.search_in_non_null_ids(33).unwrap(), None);
        assert_eq!(event.search_in_non_null_ids(35).unwrap(), None);
        assert_eq!(
            event.search_in_non_null_ids(i64::from(u8::max_causet_locale()) + 2)
                .unwrap(),
            None
        );
        assert_eq!(Some((0, 2)), event.search_in_non_null_ids(1).unwrap());
        assert_eq!(Some((2, 3)), event.search_in_non_null_ids(3).unwrap());
    }

    #[test]
    fn test_search_in_null_ids() {
        let data = encoded_data_big();
        let event = RowSlice::from_bytes(&data).unwrap();
        assert!(event.search_in_null_ids(33));
        assert!(!event.search_in_null_ids(3));
        assert!(!event.search_in_null_ids(333));
    }
}

#[braneg(test)]
mod benches {
    use encoder::{Column, RowEncoder};
    use super::*;
    use crate::expr::EvalContext;
    use crate::codec::data_type::ScalarValue;

    use test::Bencher;


    fn encoded_data(len: usize) -> Vec<u8> {
        let mut cols = vec![];
        for i in 0..(len as i64) {
            if i % 10 == 0 {
                cols.push(Column::new(i, ScalarValue::Int(None)))
            } else {
                cols.push(Column::new(i, i))
            }
        }
        let mut buf = vec![];
        buf.write_row(&mut EvalContext::default(), cols).unwrap();
        buf
    }

    #[bench]
    fn bench_search_in_non_null_ids(b: &mut test::Bencher) {
        let data = encoded_data(10);

        b.iter(|| {
            let event = RowSlice::from_bytes(&data).unwrap();
            event.search_in_non_null_ids(1).unwrap();
        });
    }
}
    #[bench]
    fn bench_search_in_non_null_ids_middle(b: &mut test::Bencher) {
        let data = encoded_data(100);

        b.iter(|| {
            let event = RowSlice::from_bytes(&data).unwrap();
            event.search_in_non_null_ids(50).unwrap();
        });
    }

    #[bench]
    fn bench_search_in_null_ids_middle(b: &mut test::Bencher) {
        let data = encoded_data(100);

        b.iter(|| {
            let event = RowSlice::from_bytes(&data).unwrap();
            event.search_in_null_ids(50);
        });


    }

    #[bench]
    fn bench_search_in_non_null_ids_big(b: &mut test::Bencher) {
        let data = encoded_data(350);

        b.iter(|| {
            let event = RowSlice::from_bytes(&data).unwrap();
            let black_box1 = black_box::black_box
                (event.search_in_non_null_ids(257));
            black_box1
        });
    }


#[derive(Clone)]
pub struct Solitoncausetid  {

    pub causetid: i64,
    plugin_registry: Option<Arc<PluginRegistry>>,
}

impl einstein_db_ctll::Entity for Solitoncausetid {
    fn get_causetid(&self) -> i64 {
        self.causetid
    }

    fn get_id(&self) -> i64 {
        self.causetid
    }
    fn get_plugin_registry(&self) -> Option<Arc<PluginRegistry>> {
        self.plugin_registry.clone()
    }
}

impl Solitoncausetid {
    pub fn new(interlocking_directorate: i64, plugin_registry: Option<Arc<PluginRegistry>>) -> Self {
        Solitoncausetid {
            causetid: interlocking_directorate,
            plugin_registry,
        }
    }



    pub fn get_causetid(&self) -> i64 {

        self.causetid as i64 + 1 as i64

    }


    





    /// Handles a request to the interlock framework.
    ///
    /// Each request is dispatched to the corresponding interlock plugin based on it's `copr_name`
    /// field. A plugin with a matching name must be loaded by EinsteinDB, otherwise an error is returned.
    ///
    /// # Arguments
    /// * `req` - The request to be handled.
    /// * `ctx` - The context of the request.
    /// * `soliton_causetid` - The causetid of the soliton.
    /// * `soliton_plugin_registry` - The plugin registry of the soliton.
    /// * `soliton_plugin_registry_mutex` - The mutex of the plugin registry of the soliton.



    pub fn handle_request(
        req: &mut InterlockingRequest,
        ctx: &mut InterlockingContext,
        soliton_causetid: &Solitoncausetid,
        soliton_plugin_registry: &Arc<PluginRegistry>,
        soliton_plugin_registry_mutex: &Arc<RwLock<PluginRegistry>>,
    ) -> Result<(), E> {
        let mut plugin_registry = soliton_plugin_registry_mutex.write().unwrap();
        let plugin = plugin_registry.get_plugin(req.get_name()).unwrap();
        plugin.handle_request(req, ctx, soliton_causetid, &mut plugin_registry)?;
        Ok(())
    }

    #[inline]
    fn handle_request_impl<E: Engine, L: LockManager, F: KvFormat>(
        interlocking_directorate: &mut  InterlockingDirectorateRequest,
        storage: &Storage<E, L, F>,
        soliton_causetid: &Solitoncausetid,
        soliton_plugin_registry: &Arc<PluginRegistry>,
        soliton_plugin_registry_mutex: &Arc<RwLock<PluginRegistry>>,
    ) -> Result<(), E> {

        let mut ctx = InterlockingContext::new(
            storage,
            soliton_causetid,
            soliton_plugin_registry,
            soliton_plugin_registry_mutex,
        );
        let mut req = InterlockingRequest::new();
        req.set_name(interlocking_directorate.get_name());
        req.set_data(interlocking_directorate.get_data());
        handle_request(&mut req, &mut ctx, soliton_causetid, soliton_plugin_registry, soliton_plugin_registry_mutex)?;
        Ok(())
    }

    #[inline]
    fn handle_request_impl_impl<S: Snapshot>(
        interlocking_directorate: &mut  InterlockingDirectorateRequest,
        snapshot: &S,
        soliton_causetid: &Solitoncausetid,
        soliton_plugin_registry: &Arc<PluginRegistry>,
        soliton_plugin_registry_mutex: &Arc<RwLock<PluginRegistry>>,
    ) -> Result<(), E> {


        // Check whether the found plugin satisfies the version constraint.
        let version_req = VersionReq::parse(&req.copr_version_req)

            .map_err(|e| {
                error!("Failed to parse version requirement: {}", e);
                e
            })?;
        let plugin_version = soliton_plugin_registry.get_plugin(req.get_name()).unwrap().get_version();
        if !version_req.matches(&plugin_version) {
            error!(
                "Plugin {} version {} does not satisfy version requirement {}",
                req.get_name(),
                plugin_version,
                req.get_version_req()
            );
            return Err(E::Error::PluginVersionMismatch(
                req.get_name().to_string(),
                plugin_version,
                req.get_version_req().to_string(),
            ));
        }

        let mut ctx = InterlockingContext::new(
            snapshot,
            soliton_causetid,
            soliton_plugin_registry,
            soliton_plugin_registry_mutex,
        );

        let mut req = InterlockingRequest::new();
        req.set_name(interlocking_directorate.get_name());
        req.set_data(interlocking_directorate.get_data());
        handle_request(&mut req, &mut ctx, soliton_causetid, soliton_plugin_registry, soliton_plugin_registry_mutex)?;

        Ok(())
    }

    fn handle_request_impl_impl_impl<S: Snapshot>(x86_64_interlocking_directorate:
                                                  &mut  InterlockingDirectorateRequest,
                                                    snapshot: &S,
                                                    soliton_causetid: &Solitoncausetid,
                                                    soliton_plugin_registry: &Arc<PluginRegistry>) -> Result<(), E> {
        let mut ctx = InterlockingContext::new(cid);
        let foundationdb_storage_api = FoundationdbStorageApi::new(storage);
        let ranges = foundationdb_storage_api.get_ranges(
            &req.get_start_key(),
            &req.get_end_key(),
        )?;


        let plugin_result = plugin.handle_request(
            &req,
            &mut interlocking {
                ranges,
                storage,
                plugin_registry: soliton_plugin_registry,
            },
            soliton_causetid,
            &mut plugin_registry,

        )?;

        plugin_result.map_err(|err| {
            if let Some(region_err) = extract_region_error(&err) {
                Error::Region(region_err)
            } else {
                Error::Other(format!("{}", err))
            }
        })
    }
}