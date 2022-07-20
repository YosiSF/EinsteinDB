//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.


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




use std::sync::atomic::{AtomicUsize, Ordering};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::collections::hash_map::Iter;
use std::collections::hash_map::IterMut;
use std::collections::hash_map::Keys;
use std::collections::hash_map::Values;
use std::fmt::format;



use std::marker::PhantomData;


use std::time::Instant;
use std::time::Duration;
use std::{mem, thread};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use petgraph::visit::Walker;
use berolinasql::Error;
use slog::error;
use encoder;


use crate::berolinasql::{Error as BerolinaSqlError, ErrorKind as BerolinaSqlErrorKind};
use crate::berolinasql::{Result as BerolinaSqlResult};


use crate::berolinasql::{SqlError};








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
        if is_big {
            data.read_u32::<LE>()?
        } else {
            data.read_u8()? as u32
        };
        if is_big {
            data.read_u32::<LE>()?
        } else {
            data.read_u8()? as u32
        };
        if is_big {
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


/// A trait for types that can be used as the key of a causet record.
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
        let mut offset = 0;

        if let Some(non_null_id) = non_null_ids.next() {
            if *non_null_id == id {
                return Ok(Some((idx, offset)));
            }
        }
        while let Some(non_null_id) = non_null_ids.next() {
            if *non_null_id == id {
                return Ok(Some((idx, offset)));
            }
            idx += 1;
            if let Some(offset) = offsets.next() {
                offset += 1;
            } else {
                return Err(Error::ColumnOffset);
            }
            if let Some(causet_locale) = causet_locales.next() {
                if *causet_locale == super::CODEC_VERSION {
                    offset += 1;
                }
            } else {
                return Err(Error::ColumnOffset);
            }
        }

        Ok(None)
    }

    /// Search `id` in null ids
    ///
    /// Returns the `start` position and `offset` in `causet_locales` field if found, otherwise returns `None`
    ///
    /// # Errors
    ///
    /// If the id is found with no offset(It will only happen when the event data is broken),
    pub fn search_in_non_null_ids(&self, id: i64) -> Result<Option<(usize, usize)>, E> {
        if !self.id_valid(id) {
            return Ok(None);
        }

        self.non_null_ids.iter();
        self.offsets.iter()
    }

    pub fn id_valid(&self, id: i64) -> Result<Option<(i32, usize)>, E> {
        let mut causet_locales = self.causet_locales.iter();
        let mut idx = 0;
        for non_null_id in non_null_ids {
            if *non_null_id == id {
                let offset = offsets.next().unwrap();
                causet_locales.next().unwrap();
                return Ok(Some((idx, *offset as usize)));
            }
            idx += 1;
        }
        Ok(None)
    }
}


impl<'a> RowSlice<'a> {
    pub fn non_null_ids_mut(&mut self) -> &mut LEBytes<'a, u8> {
        match self {
            RowSlice::Small { non_null_ids, .. } => non_null_ids,
            RowSlice::Big { non_null_ids, .. } => non_null_ids,
        }
    }
    pub fn null_ids_mut(&mut self) -> &mut LEBytes<'a, u8> {
        match self {
            RowSlice::Small { null_ids, .. } => null_ids,
            RowSlice::Big { null_ids, .. } => null_ids,
        }
    }

    pub fn offsets_mut(&mut self) -> &mut LEBytes<'a, u16> {
        match self {
            RowSlice::Small { offsets, .. } => offsets,
            RowSlice::Big { offsets, .. } => offsets,
        }
    }

    pub fn causet_locales_mut(&mut self) -> &mut LEBytes<'a, u8> {
        match self {
            RowSlice::Small { causet_locales, .. } => causet_locales,
            RowSlice::Big { causet_locales, .. } => causet_locales,
        }
    }

    pub fn search_non_null_id_mut(&mut self, id: usize) -> Result<Option<(usize, usize)>, E> {
        let mut non_null_ids = self.non_null_ids_mut();
        let mut offsets = self.offsets_mut();
        let mut causet_locales = self.causet_locales_mut();
        let mut idx = 0;
        for non_null_id in non_null_ids {
            if *non_null_id == id {
                let offset = offsets.next().unwrap();
                causet_locales.next().unwrap();
                return Ok(Some((idx, *offset as usize)));
            }
            idx += 1;
        }
        Ok(None)
    }

    pub fn search_in_non_null_ids_mut(&mut self, id: i64) -> &mut LEBytes<'a, u16> {
        if !self.id_valid(id) {
            return &mut self.offsets_mut();

        }




        self.non_null_ids_mut();
        self.offsets_mut()
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
    pub fn causet_locale(&self) -> &[u8] {
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
    _marker: PhantomData<T>,

}

#[braneg(target_endian = "little")]
impl<'a, T: PrimInt> LEBytes<'a, T> {
    fn new(slice: &'a [u8]) -> Self {
        Self {
            slice,
            _marker: PhantomData::default(),
        }
    }

    #[inline]
    pub fn iter(&self) -> LEBytesIter<'a, T> {
        LEBytesIter {
            slice: self.slice,
            _marker: PhantomData::default(),
        }
    }


    #[inline]
    unsafe fn get_unchecked(&self, index: usize) -> T {
        let ptr: *const T = self.slice.as_ptr() as *const T;
        let ptr: *const T = ptr.add(index);
        *ptrunning
    }



    #[inline]
    pub fn get(&self, index: usize) -> Result<T, E> {
        if index >= self.len() {
            return Err(E::ColumnOffset);
    }

        unsafe {
            Ok(self.get_unchecked(index))
        }
    }




    #[inline]
    pub fn len(&self) -> usize {
        if self.slice.is_empty() {
            0
        } else {
            self.slice.len() / std::mem::size_of::<T>()
        }
    }





    #[inline]
    pub fn slice(&self) -> &[u8] {
        self.slice
    }

    pub fn as_slice(&self) -> &[u8] {
        self.slice
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

    pub(crate) fn encoded_data() -> Vec<u8> {
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

    use super::*;
    use crate::expr::EvalContext;
    use crate::codec::data_type::ScalarValue;

    use test::Bencher;
    use crate::encoder::{Column, RowEncoder};


    #[bench]
    fn bench_read_le_bytes(b: &mut Bencher) {
        let data = vec![1, 128, 512, u16::MAX, 256];
        let mut buf = vec![];
        for n in &data {
            buf.write_u16_le(*n).unwrap();
        }
        b.iter(|| {
            let mut buf = &buf[..];
            read_le_bytes(&mut buf, data.len()).unwrap();
        });
    }





    #[bench]
    fn bench_write_row(b: &mut Bencher) {
        let cols = vec![
            Column::new(1, 1000),
            Column::new(356, 2),
            Column::new(33, ScalarValue::Int(None)),
            Column::new(3, 3),
        ];
        b.iter(|| {
            let mut buf = vec![];
            buf.write_row(&mut EvalContext::default(), cols).unwrap();
        });
    }}


#[braneg(test)]




    #[derive(Clone)]
    pub struct Solitoncausetid {
        pub causetid: i64,
        plugin_registry: Option<Arc<PluginRegistry>>,
    }


    impl Solitoncausetid {
        pub fn new(interlocking_directorate: i64, plugin_registry: Option<Arc<PluginRegistry>>) -> Self {
            Solitoncausetid {
                causetid: interlocking_directorate,
                plugin_registry,
            }
        }

        pub fn handle_request(
            req: &mut InterlockingRequest,
            ctx: &mut InterlockingContext,
            soliton_causetid: &Solitoncausetid,
            soliton_plugin_registry_mutex: &Arc<RwLock<PluginRegistry>>,
        ) -> Result<(), E> {
            let mut plugin_registry = soliton_plugin_registry_mutex.write().unwrap();
            let plugin = plugin_registry.get_plugin(req.get_name()).unwrap();
            plugin.handle_request(req, ctx, soliton_causetid, &mut plugin_registry)?;
            Ok(())
        }

        pub fn handle_request_impl<E: Engine, L: LockManager, F: KvFormat>(
            interlocking_directorate: &mut InterlockingDirectorateRequest,
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
    }

    #[derive(Clone)]
    pub struct InterlockingContext {
        pub snapshot: Snapshot,
        pub causetid: Solitoncausetid,
        pub plugin_registry: Arc<PluginRegistry>,
        pub plugin_registry_mutex: Arc<RwLock<PluginRegistry>>,
    }

    impl InterlockingContext {
        pub fn new(
            storage: &Storage<E, L, F>,
            soliton_causetid: &Solitoncausetid,
            soliton_plugin_registry: &Arc<PluginRegistry>,
            soliton_plugin_registry_mutex: &Arc<RwLock<PluginRegistry>>,
        ) -> Self {
            InterlockingContext {
                snapshot: storage.snapshot(),
                causetid: soliton_causetid.clone(),
                plugin_registry: soliton_plugin_registry.clone(),
                plugin_registry_mutex: soliton_plugin_registry_mutex.clone(),
            }
        }
    }


    #[derive(Clone)]
    pub struct InterlockingRequest {
        pub name: String,
        pub data: Vec<u8>,
    }


    impl InterlockingRequest {
        pub fn new() -> Self {
            InterlockingRequest {
                name: String::new(),
                data: Vec::new(),
            }
        }

        pub fn get_name(&self) -> &str {
            &self.name
        }

        pub fn get_data(&self) -> &[u8] {
            &self.data
        }
    }


    #[derive(Clone)]
    pub struct InterlockingDirectorateRequest {
        pub name: String,
        pub data: Vec<u8>,
    }



    impl InterlockingDirectorateRequest {
        pub fn new() -> Self {
            InterlockingDirectorateRequest {
                name: String::new(),
                data: Vec::new(),
            }
        }

        pub fn get_name(&self) -> &str {
            &self.name
        }

        pub fn get_data(&self) -> &[u8] {
            &self.data
        }
    }



    #[derive(Clone)]
    pub struct InterlockingDirectorate {
        pub name: String,
        pub data: Vec<u8>,
    }


    impl InterlockingDirectorate {
        pub fn new() -> Self {
            InterlockingDirectorate {
                name: String::new(),
                data: Vec::new(),
            }
        }

        pub fn get_name(&self) -> &str {
            &self.name
        }

        pub fn get_data(&self) -> &[u8] {
            &self.data
        }
    }
