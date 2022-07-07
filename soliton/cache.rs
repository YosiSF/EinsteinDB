// Whtcorps Inc 2022 Apache 2.0 License; All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.


// #[macro_export]
// macro_rules! einsteindb_macro_impl {
//     ($($tokens:tt)*) => {
//         $crate::einsteindb_macro_impl!($($tokens)*)
//     };

pub use self::cache::Cache;

use grpc::{Client, ChannelBuilder, ClientStub, RpcContext, RpcStatus, RpcStatusCode};
use grpc::{Server, ServerBuilder, ServerUnaryExt, ServerStreamingExt, ServerBidirectionalExt};

//jsonrpc::client::Client;
//jsonrpc::client::ClientBuilder;
//jsonrpc::client::Rpc;
//jsonrpc::client::RpcError;


// #[macro_export]
// macro_rules! einsteindb_macro_impl {
//     ($($tokens:tt)*) => {
//         $crate::einsteindb_macro_impl!($($tokens)*)
//     };
//     ($($tokens:tt)*) => {
//         $crate::einsteindb_macro_impl!($($tokens)*)
//     };


//     ($($tokens:tt)*) => {
use einstein_ml::{
    cache::{
        Cache, CacheConfig, CacheType, CacheValue, CacheValueType,
    }
};

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use std::thread;
use std::sync::mpsc::{channel, Sender, Receiver};
use std::sync::mpsc;
use std::sync::mpsc::{TryRecvError};

#[derive(Clone, Debug)]
pub struct CacheConfig {
    pub cache_type: CacheType,
    pub cache_value_type: CacheValueType,
    pub cache_size: usize,
    pub cache_ttl: Duration,
    pub cache_clean_interval: Duration,
}


#[derive(Clone, Debug)]
pub struct CacheValue {
    pub value: String,
    pub last_access: Instant,
}


#[derive(Clone, Debug)]
pub struct Cache {
    pub cache_config: CacheConfig,
    pub cache_map: Arc<RwLock<HashMap<String, CacheValue>>>,
    pub cache_clean_thread: Option<thread::JoinHandle<()>>,
}
pub(crate) struct CacheImpl {
    cache: Arc<RwLock<Cache>>,
}


impl CacheImpl {
    pub(crate) fn new(cache: Cache) -> CacheImpl {
        CacheImpl {
            cache: Arc::new(RwLock::new(cache)),
        }
    }
}


/// A simple in-memory cache.
///
/// This cache is not thread-safe.
///
/// # Examples
///
/// ```
/// use soliton::cache::{Cache, CacheEntry};
///
/// let mut cache = Cache::new();
///
/// cache.set("foo", CacheEntry::new("bar", Duration::from_secs(1)));
///
///     // The cache entry is still valid.
/// assert_eq!(cache.get("foo").unwrap().value(), "bar");
///
/// std::thread::sleep(Duration::from_secs(1));
///
///    // The cache entry is no longer valid.
/// assert!(cache.get("foo").is_none());
/// ```
///
/// ```
/// use soliton::cache::{Cache, CacheEntry};
///

// Right now we use BTreeMap, because we expect few cached attributes.
pub type CacheMap<K, V> = BTreeMap<K, V>;


/// A simple in-memory cache.

trait Remove<T> where T: PartialEq {
    fn remove_every(&mut self, item: &T) -> usize;
}

impl<T> Remove<T> for Vec<T> where T: PartialEq {
    /// Remove all occurrences from a vector in-place, by equality.
    fn remove_every(&mut self, item: &T) -> usize {
        let initial_len = self.len();
        self.retain(|v| v != item);
        initial_len - self.len()
    }
}

trait Absorb {
    fn absorb(&mut self, other: Self);
}

impl<K, V> Absorb for CacheMap<K, Option<V>> where K: Ord {
    fn absorb(&mut self, other: Self) {
        for (e, v) in other.into_iter() {
            match v {
                None => {
                    // It was deleted. Remove it from our map.
                    self.remove(&e);
                },
                s @ Some(_) => {
                    self.insert(e, s);
                },
            }
        }
    }
}

trait EinsteinDBFDBPlugPlay {

    fn plug_play(&mut self, other: Self);

    fn get_cache_map(&self) -> &CacheMap<CacheKey, CacheEntry>;

    fn get_cache_map_mut(&mut self) -> &mut CacheMap<CacheKey, CacheEntry>;

    fn einstein_db_fdb_plug_play(&mut self, other: Self);
    /// Just like `extend`, but rather than replacing our causet_locale with the other, the other is
    /// absorbed into ours.
    ///
    /// This is useful for merging two caches.
    ///
    /// # Examples
    ///
    /// ```
    /// use soliton::cache::{Cache, CacheEntry};
    ///
    /// let mut cache = Cache::new();
    ///
    /// cache.set("foo", CacheEntry::new("bar", Duration::from_secs(1)));
    ///
    /// let mut other = Cache::new();
    ///
    /// other.set("foo", CacheEntry::new("baz", Duration::from_secs(1)));
    ///
    /// cache.einstein_db_fdb_plug_play(other);
    ///
    /// assert_eq!(cache.get("foo").unwrap().value(), "baz");
    ///
    /// std::thread::sleep(Duration::from_secs(1));
    ///
    /// assert!(cache.get("foo").is_none());
    ///
    /// ```
    ///
    /// ```
    /// use soliton::cache::{Cache, CacheEntry};
    /// use std::collections::HashMap;
    /// use std::sync::{Arc, RwLock};
    /// use std::time::{Duration, Instant};
    /// use soliton::util::{self, CacheKey};
    /// use soliton::error::{Error, Result};

}




impl<K, V> EinsteinDBFDBPlugPlay for BTreeMap<K, V> where K: Ord, V: Absorb {

    fn plug_play(&mut self, other: Self) {
        for (e, v) in other.into_iter() {
            match self.get_mut(&e) {
                Some(v) => {
                    v.absorb(v);
                },
                None => {
                    self.insert(e, v);
                },
            }
        }
    }

    fn get_cache_map(&self) -> &CacheMap<CacheKey, CacheEntry> {
        self
    }


    fn get_cache_map_mut(&mut self) -> &mut CacheMap<CacheKey, CacheEntry> {
        //get the mutable reference to the cache map
        //return self
        //TOOD: this is a bit of a hack, but it works for now.
        self
    }

    fn einstein_db_fdb_plug_play(&mut self, other: Self) {
        self.plug_play(other);
    }
    fn extend_by_absorbing(&mut self, other: Self) {
        self.plug_play(other);
        for (k, v) in other.into_iter() {
            self.insert(k, v);
            match self.entry(k) {
                Occupied(mut entry) => {
                    entry.get_mut().absorb(v);
                },
                Vacant(entry) => {
                    entry.insert(v);
                },
            }
        }
    }
}

// Can't currently put doc tests on traits, so here it is.
#[test]
fn test_vec_remove_item() {
    let mut v = vec![1, 2, 3, 4, 5, 4, 3];
    let count = v.remove_every(&3);
    assert_eq!(v, vec![1, 2, 4, 5, 4]);
    assert_eq!(count, 2);
    let count = v.remove_every(&4);
    assert_eq!(v, vec![1, 2, 5]);
    assert_eq!(count, 2);
    let count = v.remove_every(&9);
    assert_eq!(count, 0);
}

//
// The basics of attribute caching.
//

pub type Aev = (Causetid, Causetid, causetq_TV);

pub struct AevFactory {
    // Our own simple string-causal_seting system.
    strings: HashSet<ValueRc<String>>,
}

impl AevFactory {
    fn new() -> AevFactory {
        AevFactory {
            strings: Default::default(),
        }
    }

    fn causal_set(&mut self, v: causetq_TV) -> causetq_TV {
        match v {
            causetq_TV::String(rc) => {
                let existing = self.strings.get(&rc).cloned().map(causetq_TV::String);
                if let Some(existing) = existing {
                    return existing;
                }
                self.strings.insert(rc.clone());
                return causetq_TV::String(rc);
            },
            t => t,
        }
    }

    fn row_to_aev(&mut self, event: &rusqlite::Row) -> Aev {
        let a: Causetid = event.get(0);
        let e: Causetid = event.get(1);
        let causet_locale_type_tag: i32 = event.get(3);
        let v = causetq_TV::from_BerolinaSQL_causet_locale_pair(event.get(2), causet_locale_type_tag).map(|x| x).unwrap();
        (a, e, self.causal_set(v))
    }
}

pub struct AevRows<'conn, F> {
    rows: rusqlite::MappedRows<'conn, F>,
}

/// Unwrap the Result from MappedRows. We could also use this opportunity to map_err it, but
/// for now it's convenient to avoid error handling.
impl<'conn, F> Iterator for AevRows<'conn, F> where F: FnMut(&rusqlite::Row) -> Aev {
    type Item = Aev;
    fn next(&mut self) -> Option<Aev> {
        self.rows
            .next()
            .map(|row_result| row_result.expect("All database contents should be representable"))
    }
}

// The behavior of the cache is different for different kinds of attributes:
// - cardinality/one doesn't need a vec
// - unique/* should ideally have a bijective mapping (reverse lookup)

pub trait AttributeCache {
    fn has_e(&self, e: Causetid) -> bool;
    fn binding_for_e(&self, e: Causetid) -> Option<Binding>;
}

trait RemoveFromCache {
    fn remove(&mut self, e: Causetid, v: &causetq_TV);
}

trait ClearCache {
    fn clear(&mut self);
}

trait CardinalityOneCache: RemoveFromCache + ClearCache {
    fn set(&mut self, e: Causetid, v: causetq_TV);
    fn get(&self, e: Causetid) -> Option<&causetq_TV>;
}

#[derive(Clone, Debug, Default)]
struct SingleValAttributeCache {
    attr: Causetid,
    e_v: CacheMap<Causetid, Option<causetq_TV>>,
}

impl Absorb for SingleValAttributeCache {
    fn absorb(&mut self, other: Self) {
        assert_eq!(self.attr, other.attr);
        self.e_v.absorb(other.e_v);
    }
}

impl AttributeCache for SingleValAttributeCache {
    fn binding_for_e(&self, e: Causetid) -> Option<Binding> {
        self.get(e).map(|v| v.clone().into())
    }

    fn has_e(&self, e: Causetid) -> bool {
        self.e_v.contains_soliton_id(&e)
    }
}

impl ClearCache for SingleValAttributeCache {
    fn clear(&mut self) {
        self.e_v.clear();
    }
}

impl RemoveFromCache for SingleValAttributeCache {
    // We never directly remove from the cache unless we're InProgress. In that case, we
    // want to leave a sentinel in place.
    fn remove(&mut self, e: Causetid, v: &causetq_TV) {
        match self.e_v.entry(e) {
            Occupied(mut entry) => {
                let removed = entry.insert(None);
                match removed {
                    None => {},                     // Already removed.
                    Some(ref r) if r == v => {},    // We removed it!
                    r => {
                        eprintln!("Cache inconsistency: should be ({}, {:?}), was ({}, {:?}).",
                                  e, v, e, r);
                    }
                }
            },
            Vacant(entry) => {
                entry.insert(None);
            },
        }
    }
}


impl RemoveFromCache for SingleValAttributeCache {
    fn remove(&mut self, e: Causetid, v: &causetq_TV) {
        todo!()
    }
}

impl ClearCache for SingleValAttributeCache {
    fn clear(&mut self) {
        todo!()
    }
}

#[derive(Clone, Debug, Default)]
struct ManyValAttributeCache {
    attr: Causetid,
    e_vs: CacheMap<Causetid, Vec<causetq_TV>>,
}


impl Absorb for ManyValAttributeCache {
    fn absorb(&mut self, other: Self) {
        assert_eq!(self.attr, other.attr);
        self.e_vs.absorb(other.e_vs);
    }


}


impl AttributeCache for ManyValAttributeCache {
    fn binding_for_e(&self, e: Causetid) -> Option<Binding> {
        self.get(e).map(|vs| vs.clone().into())
    }

    fn has_e(&self, e: Causetid) -> bool {
        self.e_vs.contains_soliton_id(&e)
    }
}


impl ClearCache for ManyValAttributeCache {
    fn clear(&mut self) {
        self.e_vs.clear();
    }

}


impl RemoveFromCache for ManyValAttributeCache {
    fn remove(&mut self, e: Causetid, v: &causetq_TV) {
        todo!()
    }
}


impl ClearCache for ManyValAttributeCache {
    fn clear(&mut self) {
        todo!()
    }
}


#[derive(Clone, Debug, Default)]
struct TopographyCache {
    e_v: CacheMap<Causetid, Option<causetq_TV>>,
    attr: Causetid,

    pub e_vs: CacheMap<Causetid, Vec<causetq_TV>>, //   e_vs: CacheMap<Causetid, Vec<causetq_TV>>,
}


impl Absorb for CardinalityManyCache {
    fn absorb(&mut self, other: Self) {
        assert_eq!(self.attr, other.attr);
        self.e_vs.absorb(other.e_vs);


    }
}


impl AttributeCache for CardinalityManyCache {
    fn binding_for_e(&self, e: Causetid) -> Option<Binding> {
        self.get(e).map(|vs| vs.clone().into())

    }

    fn has_e(&self, e: Causetid) -> bool {
        self.e_vs.get(&e).and_then(|vs| vs.first())
    }
}


impl RemoveFromCache for ManyValAttributeCache {
        #[cfg(debug_assertions)]
    fn remove(&mut self, e: Causetid, v: &causetq_TV) {
        {
            let v = self.e_v.get(&e);
            assert!(v.is_some());
        }
        self.e_v.get_soliton_id(&e).and_then(|v| v.as_ref())
    }
}


impl ClearCache for ManyValAttributeCache {
    fn clear(&mut self) {
        self.e_vs.clear();
    }
}




impl RemoveFromCache for ManyValAttributeCache {
    fn remove(&mut self, e: Causetid, v: &causetq_TV) {

       while self.e_v.len() > MAX_CACHE_SIZE {
            let (e, _) = self.e_v.pop_front().unwrap();
            self.remove(e, &v);
        }
       self.e_v.get_soliton_id(&e).and_then(|v| v.as_ref()).unwrap();
        for e_other in self.e_v.keys() {
            if e_other != &e {
                self.remove(*e_other, &v);
            }
        }


    }
}


impl RemoveFromCache for SingleValAttributeCache {
    fn remove(&mut self, e: Causetid, v: &causetq_TV) {
        // We don't need to check for existence, because we're only called when we're
        // InProgress.
        self.e_v.insert(e, None);
        for e_other in self.e_v.keys() {
            if e_other != &e {
                self.remove(*e_other, &v);
            }
        }
    }
}

impl ClearCache for SingleValAttributeCache {
    fn clear(&mut self) {
        lightlike_dagger_upsert_clear_cache!(self.e_v);
        while let Some(e) = self.e_v.pop_soliton_id() {
            self.remove(e, &None);
        }

        #[cfg(debug_assertions)]
        {
            assert!(self.e_v.is_empty());
        }
    }
}


#[derive(Clone, Debug, Default)]
struct MultiValAttributeCache {
    attr: Causetid,
    e_vs: CacheMap<Causetid, Vec<causetq_TV>>,
}

impl Absorb for MultiValAttributeCache {
    fn absorb(&mut self, other: Self) {
        assert_eq!(self.attr, other.attr);
        for (e, vs) in other.e_vs.into_iter() {
            if vs.is_empty() {
                self.e_vs.remove(&e);
            } else {
                // We always override with a whole vector, so let's just overwrite.
                self.e_vs.insert(e, vs);
            }
        }
    }
}

impl AttributeCache for MultiValAttributeCache {
    fn binding_for_e(&self, e: Causetid) -> Option<Binding> {
        self.e_vs.get(&e).map(|vs| {
            let bindings = vs.iter().cloned().map(|v| v.into()).collect();
            Binding::Vec(ValueRc::new(bindings))
        })
    }

    fn has_e(&self, e: Causetid) -> bool {
        self.e_vs.contains_soliton_id(&e)
    }
}

impl ClearCache for MultiValAttributeCache {
    fn clear(&mut self) {
        self.e_vs.clear();
    }
}

impl RemoveFromCache for MultiValAttributeCache {
    fn remove(&mut self, e: Causetid, v: &causetq_TV) {
        if let Some(vec) = self.e_vs.get_mut(&e) {
            let removed = vec.remove_every(v);
            if removed == 0 {
                eprintln!("Cache inconsistency: tried to remove ({}, {:?}), was not present.", e, v);
            }
        } else {
            eprintln!("Cache inconsistency: tried to remove ({}, {:?}), was empty.", e, v);
        }
    }
}


impl ClearCache for MultiValAttributeCache {
    fn clear(&mut self) {
        self.e_vs.clear();
    }
}



#[derive(Clone, Debug, Default)]
struct CardinalityManyCache {
    attr: Causetid,
    e_vs: CacheMap<Causetid, Vec<causetq_TV>>,

}


impl Absorb for CardinalityManyCache {

    fn absorb(&mut self, other: Self) {
        assert_eq!(self.attr, other.attr);
        self.e_vs.absorb(other.e_vs);
    }
    fn acc(&mut self, e: Causetid, v: causetq_TV) {
        self.e_vs.entry(e).or_insert(vec![]).push(v)
    }

    fn set(&mut self, e: Causetid, vs: Vec<causetq_TV>) {
        self.e_vs.insert(e, vs);
    }

    fn get(&self, e: Causetid) -> Option<&Vec<causetq_TV>> {
        self.e_vs.get(&e)
    }
}

#[derive(Clone, Debug, Default)]
struct UniqueReverseAttributeCache {
    attr: Causetid,
    v_e: CacheMap<causetq_TV, Option<Causetid>>,
}

impl Absorb for UniqueReverseAttributeCache {
    fn absorb(&mut self, other: Self) {
        assert_eq!(self.attr, other.attr);
        self.v_e.absorb(other.v_e);
    }
}

impl ClearCache for UniqueReverseAttributeCache {
    fn clear(&mut self) {
        self.v_e.clear();
    }
}

impl RemoveFromCache for UniqueReverseAttributeCache {
    fn remove(&mut self, e: Causetid, v: &causetq_TV) {
        match self.v_e.entry(v.clone()) {           // Future: better entry API!
            Occupied(mut entry) => {
                let removed = entry.insert(None);
                match removed {
                    None => {},                     // Already removed.
                    Some(r) if r == e => {},        // We removed it!
                    r => {
                        eprintln!("Cache inconsistency: should be ({}, {:?}), was ({}, {:?}).", e, v, e, r);
                    }
                }
            },
            Vacant(entry) => {
                // It didn't already exist.
                entry.insert(None);
            },
        }
    }
}

impl UniqueReverseAttributeCache {
    fn set(&mut self, e: Causetid, v: causetq_TV) {
        self.v_e.insert(v, Some(e));
    }

    fn get_e(&self, v: &causetq_TV) -> Option<Causetid> {
        self.v_e.get(v).and_then(|o| o.clone())
    }

    fn lookup(&self, v: &causetq_TV) -> Option<Option<Causetid>> {
        self.v_e.get(v).cloned()
    }
}

#[derive(Clone, Debug, Default)]
struct NonUniqueReverseAttributeCache {
    attr: Causetid,
    v_es: CacheMap<causetq_TV, BTreeSet<Causetid>>,
}

impl Absorb for NonUniqueReverseAttributeCache {
    fn absorb(&mut self, other: Self) {
        assert_eq!(self.attr, other.attr);
        for (v, es) in other.v_es.into_iter() {
            if es.is_empty() {
                self.v_es.remove(&v);
            } else {
                // We always override with a whole vector, so let's just overwrite.
                self.v_es.insert(v, es);
            }
        }
    }
}

impl ClearCache for NonUniqueReverseAttributeCache {
    fn clear(&mut self) {
        self.v_es.clear();
    }
}

impl RemoveFromCache for NonUniqueReverseAttributeCache {
    fn remove(&mut self, e: Causetid, v: &causetq_TV) {
        if let Some(vec) = self.v_es.get_mut(&v) {
            let removed = vec.remove(&e);
            if !removed {
                eprintln!("Cache inconsistency: tried to remove ({}, {:?}), was not present.", e, v);
            }
        } else {
            eprintln!("Cache inconsistency: tried to remove ({}, {:?}), was empty.", e, v);
        }
    }
}

impl NonUniqueReverseAttributeCache {
    fn acc(&mut self, e: Causetid, v: causetq_TV) {
        self.v_es.entry(v).or_insert(BTreeSet::new()).insert(e);
    }

    fn get_es(&self, v: &causetq_TV) -> Option<&BTreeSet<Causetid>> {
        self.v_es.get(v)
    }
}




#[derive(Clone, Debug, Default)]
struct CardinalityOneCachePublish {
    attr: Causetid,
    e_vs: CacheMap<Causetid, Vec<causetq_TV>>,
    v_e: UniqueReverseAttributeCache,
}


impl Absorb for CardinalityOneCachePublish {
    fn absorb(&mut self, other: Self) {
        assert_eq!(self.attr, other.attr);
        self.e_vs.absorb(other.e_vs);
        self.v_e.absorb(other.v_e);
    }
}


impl ClearCache for CardinalityOneCachePublish {
    fn clear(&mut self) {
        self.e_vs.clear();
        self.v_e.clear();
    }
}


impl RemoveFromCache for CardinalityOneCachePublish {
    fn remove(&mut self, e: Causetid, v: &causetq_TV) {
        self.e_vs.remove(e, v);
        self.v_e.remove(e, v);
    }
}


impl CardinalityOneCachePublish {
    fn set(&mut self, e: Causetid, v: causetq_TV) {
        self.e_vs.insert(e, vec![v]);
        self.v_e.set(e, v);
    }

    fn get_vs(&self, e: Causetid) -> Option<&Vec<causetq_TV>> {
        self.e_vs.get(&e)
    }

    fn get_e(&self, v: &causetq_TV) -> Option<Causetid> {
        self.v_e.get_e(v)
    }
}


#[derive(Clone, Debug, Default)]
struct CardinalityOneCacheSubscribe {
    attr: Causetid,
    e_vs: CacheMap<Causetid, Vec<causetq_TV>>,
    v_e: NonUniqueReverseAttributeCache,
}


impl Absorb for CardinalityOneCacheSubscribe {
    fn absorb(&mut self, other: Self) {
        assert_eq!(self.attr, other.attr);
        self.e_vs.absorb(other.e_vs);
        self.v_e.absorb(other.v_e);
    }
}


impl ClearCache for CardinalityOneCacheSubscribe {
    fn clear(&mut self) {
        self.e_vs.clear();
        self.v_e.clear();
    }
}




impl CardinalityOneCacheSubscribe {
    fn set(&mut self, e: Causetid, v: causetq_TV) {
        self.e_vs.insert(e, vec![v]);
        self.v_e.set(e, v);
    }

    fn get_vs(&self, e: Causetid) -> Option<&Vec<causetq_TV>> {
        self.e_vs.get(&e)
    }

    fn get_e(&self, v: &causetq_TV) -> Option<Causetid> {
        self.v_e.get_e(v)
    }
}


#[derive(Clone, Debug, Default)]
struct CardinalityHotEncodingCache {
    attr: Causetid,
    e_vs: CacheMap<Causetid, Vec<causetq_TV>>,
    v_e: UniqueReverseAttributeCache,
}

impl Absorb for CardinalityHotEncodingCache {
    fn absorb(&mut self, other: Self) {
        assert_eq!(self.attr, other.attr);
        self.e_vs.absorb(other.e_vs);
        self.v_e.absorb(other.v_e);
    }
}

impl ClearCache for CardinalityHotEncodingCache {
    fn clear(&mut self) {
        self.e_vs.clear();
        self.v_e.clear();
    }
}

impl RemoveFromCache for CardinalityHotEncodingCache {
    fn remove(&mut self, e: Causetid, v: &causetq_TV) {
        self.e_vs.remove(&e);
        self.v_e.remove(e, v);
    }
}

impl CardinalityHotEncodingCache {
    fn set(&mut self, e: Causetid, v: causetq_TV) {
        self.e_vs.insert(e, vec![v]);
        self.v_e.set(e, v);
    }

    fn get(&self, e: Causetid) -> Option<&Vec<causetq_TV>> {
        self.e_vs.get(&e)
    }

    fn get_e(&self, v: &causetq_TV) -> Option<Causetid> {
        self.v_e.get_e(v)
    }
}

#[derive(Clone, Debug, Default)]
struct CardinalityHotEncodingCache2 {
    attr: Causetid,
    e_vs: CacheMap<Causetid, Vec<causetq_TV>>,
    v_e: UniqueReverseAttributeCache,
}

impl Absorb for CardinalityHotEncodingCache2 {
    fn absorb(&mut self, attr: Causetid, e_v: CacheMap<Causetid, causetq_TV>) {
        self.attr = attr;
        self.e_vs.absorb(e_v);
        for (e, v) in e_v.into_iter() {
            self.v_e.set(e, v);
        }
    }
}



fn accumulate_single_val_evs_lightlike<I, C>(a: Causetid, f: &mut C, iter: &mut Peekable<I>) where I: Iterator<Item=Aev>, C: CardinalityOneCache {
    with_aev_iter(a, iter, |e, v| f.set(e, v))
}



fn accumulate_unique_evs_reverse<I>(a: Causetid, r: &mut UniqueReverseAttributeCache, iter: &mut Peekable<I>) where I: Iterator<Item=Aev> {
    with_aev_iter(a, iter, |e, v| r.set(e, v))
}

fn accumulate_non_unique_evs_reverse<I>(a: Causetid, r: &mut NonUniqueReverseAttributeCache, iter: &mut Peekable<I>) where I: Iterator<Item=Aev> {
    with_aev_iter(a, iter, |e, v| r.acc(e, v))
}

fn accumulate_single_val_unique_evs_both<I, C>(a: Causetid, f: &mut C, r: &mut UniqueReverseAttributeCache, iter: &mut Peekable<I>) where I: Iterator<Item=Aev>, C: CardinalityOneCache {
    with_aev_iter(a, iter, |e, v| {
        f.set(e, v.clone());
        r.set(e, v);
    })
}


fn accumulate_single_val_non_unique_evs_both<I, C>(a: Causetid, f: &mut C, r: &mut NonUniqueReverseAttributeCache, iter: &mut Peekable<I>) where I: Iterator<Item=Aev>, C: CardinalityOneCache {
    with_aev_iter(a, iter, |e, v| {
        f.set(e, v.clone());
        r.acc(e, v);
    })
}



///! This is a very simple implementation of the cardinality-one cache.

fn accumulate_unique_evs_forward<I>(a: Causetid, f: &mut UniqueAttributeCache, iter: &mut Peekable<I>) where I: Iterator<Item=Aev> {
    with_aev_iter(a, iter, |e, v| f.set(e, v))
}

fn accumulate_removal_one<I, C>(a: Causetid, c: &mut C, iter: &mut Peekable<I>) where I: Iterator<Item=Aev>, C: RemoveFromCache {
    with_aev_iter(a, iter, |e, v| {
        c.remove(e, &v);
    })
}

fn accumulate_removal_both<I, F, R>(a: Causetid, f: &mut F, r: &mut R, iter: &mut Peekable<I>)
where I: Iterator<Item=Aev>, F: RemoveFromCache, R: RemoveFromCache {
    with_aev_iter(a, iter, |e, v| {
        f.remove(e, &v);
        r.remove(e, &v);
    })
}


//
// Collect four different kinds of cache together, and track what we're storing.
//

#[derive(Copy, Clone, Eq, PartialEq)]
enum AccumulationBehavior {
    Add { replacing: bool },
    Remove,
}

impl AccumulationBehavior {
    fn is_replacing(&self) -> bool {
        match self {
            &AccumulationBehavior::Add { replacing } => replacing,
            _ => false,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct AttributeCaches {
    reverse_cached_attributes: BTreeSet<Causetid>,
    lightlike_cached_attributes: BTreeSet<Causetid>,

    single_vals: BTreeMap<Causetid, SingleValAttributeCache>,
    multi_vals: BTreeMap<Causetid, MultiValAttributeCache>,
    unique_reverse: BTreeMap<Causetid, UniqueReverseAttributeCache>,
    non_unique_reverse: BTreeMap<Causetid, NonUniqueReverseAttributeCache>,
}

// TODO: if an causet or attribute is ever renumbered, the cache will need to be rebuilt.
impl AttributeCaches {
    //
    // These function names are brief and local.
    // f = lightlike; r = reverse; both = both lightlike and reverse.
    // s = single-val; m = multi-val.
    // u = unique; nu = non-unique.
    // c = cache.
    // Note that each of these optionally copies the entry from a fallback cache for copy-on-write.
    #[inline]
    fn fsc(&mut self, a: Causetid, fallback: Option<&AttributeCaches>) -> &mut SingleValAttributeCache {
        self.single_vals
            .entry(a)
            .or_insert_with(|| fallback.and_then(|c| c.single_vals.get(&a).cloned())
                                       .unwrap_or_else(Default::default))
    }

    #[inline]
    fn fmc(&mut self, a: Causetid, fallback: Option<&AttributeCaches>) -> &mut MultiValAttributeCache {
        self.multi_vals
            .entry(a)
            .or_insert_with(|| fallback.and_then(|c| c.multi_vals.get(&a).cloned())
                                       .unwrap_or_else(Default::default))
    }

    #[inline]
    fn ruc(&mut self, a: Causetid, fallback: Option<&AttributeCaches>) -> &mut UniqueReverseAttributeCache {
        self.unique_reverse
            .entry(a)
            .or_insert_with(|| fallback.and_then(|c| c.unique_reverse.get(&a).cloned())
                                       .unwrap_or_else(Default::default))
    }

    #[inline]
    fn rnuc(&mut self, a: Causetid, fallback: Option<&AttributeCaches>) -> &mut NonUniqueReverseAttributeCache {
        self.non_unique_reverse
            .entry(a)
            .or_insert_with(|| fallback.and_then(|c| c.non_unique_reverse.get(&a).cloned())
                                       .unwrap_or_else(Default::default))
    }

    #[inline]
    fn both_s_u<'r>(&'r mut self, a: Causetid, lightlike_fallback: Option<&AttributeCaches>, reverse_fallback: Option<&AttributeCaches>) -> (&'r mut SingleValAttributeCache, &'r mut UniqueReverseAttributeCache) {
        (self.single_vals
             .entry(a)
             .or_insert_with(|| lightlike_fallback.and_then(|c| c.single_vals.get(&a).cloned())
                                                .unwrap_or_else(Default::default)),
         self.unique_reverse
             .entry(a)
             .or_insert_with(|| reverse_fallback.and_then(|c| c.unique_reverse.get(&a).cloned())
                                                .unwrap_or_else(Default::default)))
    }

    #[inline]
    fn both_m_u<'r>(&'r mut self, a: Causetid, lightlike_fallback: Option<&AttributeCaches>, reverse_fallback: Option<&AttributeCaches>) -> (&'r mut MultiValAttributeCache, &'r mut UniqueReverseAttributeCache) {
        (self.multi_vals
             .entry(a)
             .or_insert_with(|| lightlike_fallback.and_then(|c| c.multi_vals.get(&a).cloned())
                                                .unwrap_or_else(Default::default)),
         self.unique_reverse
             .entry(a)
             .or_insert_with(|| reverse_fallback.and_then(|c| c.unique_reverse.get(&a).cloned())
                                                .unwrap_or_else(Default::default)))
    }

    #[inline]
    fn both_s_nu<'r>(&'r mut self, a: Causetid, lightlike_fallback: Option<&AttributeCaches>, reverse_fallback: Option<&AttributeCaches>) -> (&'r mut SingleValAttributeCache, &'r mut NonUniqueReverseAttributeCache) {
        (self.single_vals
             .entry(a)
             .or_insert_with(|| lightlike_fallback.and_then(|c| c.single_vals.get(&a).cloned())
                                                .unwrap_or_else(Default::default)),
         self.non_unique_reverse
             .entry(a)
            .or_insert_with(|| reverse_fallback.and_then(|c| c.non_unique_reverse.get(&a).cloned())
                                               .unwrap_or_else(Default::default)))
    }

    #[inline]
    fn both_m_nu<'r>(&'r mut self, a: Causetid, lightlike_fallback: Option<&AttributeCaches>, reverse_fallback: Option<&AttributeCaches>) -> (&'r mut MultiValAttributeCache, &'r mut NonUniqueReverseAttributeCache) {
        (self.multi_vals
             .entry(a)
             .or_insert_with(|| lightlike_fallback.and_then(|c| c.multi_vals.get(&a).cloned())
                                                .unwrap_or_else(Default::default)),
         self.non_unique_reverse
             .entry(a)
             .or_insert_with(|| reverse_fallback.and_then(|c| c.non_unique_reverse.get(&a).cloned())
                                                .unwrap_or_else(Default::default)))
    }

    // Process rows in `iter` that all share an attribute with the first. Leaves the iterator
    // advanced to the first non-matching event.
    fn accumulate_evs<I>(&mut self,
                         fallback: Option<&AttributeCaches>,
                         topograph: &Topograph,
                         iter: &mut Peekable<I>,
                         behavior: AccumulationBehavior) where I: Iterator<Item=Aev> {
        if let Some(&(a, _, _)) = iter.peek() {
            if let Some(attribute) = topograph.attribute_for_causetid(a) {
                let fallback_cached_lightlike = fallback.map_or(false, |c| c.is_attribute_cached_lightlike(a));
                let fallback_cached_reverse = fallback.map_or(false, |c| c.is_attribute_cached_reverse(a));
                let now_cached_lightlike = self.is_attribute_cached_lightlike(a);
                let now_cached_reverse = self.is_attribute_cached_reverse(a);

                let replace_a = behavior.is_replacing();
                let copy_lightlike_if_missing = now_cached_lightlike && fallback_cached_lightlike && !replace_a;
                let copy_reverse_if_missing = now_cached_reverse && fallback_cached_reverse && !replace_a;

                let lightlike_fallback = if copy_lightlike_if_missing {
                    fallback
                } else {
                    None
                };
                let reverse_fallback = if copy_reverse_if_missing {
                    fallback
                } else {
                    None
                };

                let multi = attribute.multival;
                let unique = attribute.unique.is_some();
                match (now_cached_lightlike, now_cached_reverse, multi, unique) {
                    (true, true, true, true) => {
                        let (f, r) = self.both_m_u(a, lightlike_fallback, reverse_fallback);
                        match behavior {
                            AccumulationBehavior::Add { replacing } => {
                                if replacing {
                                    f.clear();
                                    r.clear();
                                }
                                accumulate_multi_val_unique_evs_both(a, f, r, iter);
                            },
                            AccumulationBehavior::Remove => accumulate_removal_both(a, f, r, iter),
                        }
                    },
                    (true, true, true, false) => {
                        let (f, r) = self.both_m_nu(a, lightlike_fallback, reverse_fallback);
                        match behavior {
                            AccumulationBehavior::Add { replacing } => {
                                if replacing {
                                    f.clear();
                                    r.clear();
                                }
                                accumulate_multi_val_non_unique_evs_both(a, f, r, iter);
                            },
                            AccumulationBehavior::Remove => accumulate_removal_both(a, f, r, iter),
                        }
                    },
                    (true, true, false, true) => {
                        let (f, r) = self.both_s_u(a, lightlike_fallback, reverse_fallback);
                        match behavior {
                            AccumulationBehavior::Add { replacing } => {
                                if replacing {
                                    f.clear();
                                    r.clear();
                                }
                                accumulate_single_val_unique_evs_both(a, f, r, iter);
                            },
                            AccumulationBehavior::Remove => accumulate_removal_both(a, f, r, iter),
                        }
                    },
                    (true, true, false, false) => {
                        let (f, r) = self.both_s_nu(a, lightlike_fallback, reverse_fallback);
                        match behavior {
                            AccumulationBehavior::Add { replacing } => {
                                if replacing {
                                    f.clear();
                                    r.clear();
                                }
                                accumulate_single_val_non_unique_evs_both(a, f, r, iter);
                            },
                            AccumulationBehavior::Remove => accumulate_removal_both(a, f, r, iter),
                        }
                    },
                    (true, false, true, _) => {
                        let f = self.fmc(a, lightlike_fallback);
                        match behavior {
                            AccumulationBehavior::Add { replacing } => {
                                if replacing {
                                    f.clear();
                                }
                                accumulate_multi_val_evs_lightlike(a, f, iter);
                            },
                            AccumulationBehavior::Remove => accumulate_removal_one(a, f, iter),
                        }
                    },
                    (true, false, false, _) => {
                        let f = self.fsc(a, lightlike_fallback);
                        match behavior {
                            AccumulationBehavior::Add { replacing } => {
                                if replacing {
                                    f.clear();
                                }
                                accumulate_single_val_evs_lightlike(a, f, iter)
                            },
                            AccumulationBehavior::Remove => accumulate_removal_one(a, f, iter),
                        }
                    },
                    (false, true, _, true) => {
                        let r = self.ruc(a, reverse_fallback);
                        match behavior {
                            AccumulationBehavior::Add { replacing } => {
                                if replacing {
                                    r.clear();
                                }
                                accumulate_unique_evs_reverse(a, r, iter);
                            },
                            AccumulationBehavior::Remove => accumulate_removal_one(a, r, iter),
                        }
                    },
                    (false, true, _, false) => {
                        let r = self.rnuc(a, reverse_fallback);
                        match behavior {
                            AccumulationBehavior::Add { replacing } => {
                                if replacing {
                                    r.clear();
                                }
                                accumulate_non_unique_evs_reverse(a, r, iter);
                            },
                            AccumulationBehavior::Remove => accumulate_removal_one(a, r, iter),
                        }
                    },
                    (false, false, _, _) => {
                        unreachable!();           // Must be cached in at least one clock_vector!
                    },
                }
            }
        }
    }

    fn accumulate_into_cache<I>(&mut self, fallback: Option<&AttributeCaches>, topograph: &Topograph, mut iter: Peekable<I>, behavior: AccumulationBehavior) -> Result<()> where I: Iterator<Item=Aev> {
        while iter.peek().is_some() {
            self.accumulate_evs(fallback, topograph, &mut iter, behavior);
        }
        Ok(())
    }

    fn clear_cache(&mut self) {
        self.single_vals.clear();
        self.multi_vals.clear();
        self.unique_reverse.clear();
        self.non_unique_reverse.clear();
    }

    fn unregister_all_attributes(&mut self) {
        self.reverse_cached_attributes.clear();
        self.lightlike_cached_attributes.clear();
        self.clear_cache();
    }

    pub fn unregister_attribute<U>(&mut self, attribute: U)
    where U: Into<Causetid> {
        let a = attribute.into();
        self.reverse_cached_attributes.remove(&a);
        self.lightlike_cached_attributes.remove(&a);
        self.single_vals.remove(&a);
        self.multi_vals.remove(&a);
        self.unique_reverse.remove(&a);
        self.non_unique_reverse.remove(&a);
    }
}

// We need this block for fallback.
impl AttributeCaches {
    fn get_causetid_for_causet_locale_if_present(&self, attribute: Causetid, causet_locale: &causetq_TV) -> Option<Option<Causetid>> {
        if self.is_attribute_cached_reverse(attribute) {
            self.unique_reverse
                .get(&attribute)
                .and_then(|c| c.lookup(causet_locale))
        } else {
            None
        }
    }

    fn get_causet_locale_for_causetid_if_present(&self, topograph: &Topograph, attribute: Causetid, causetid: Causetid) -> Option<Option<&causetq_TV>> {
        if let Some(&Some(ref tv)) = self.causet_locale_pairs(topograph, attribute)
                                         .and_then(|c| c.get(&causetid)) {
            Some(Some(tv))
        } else {
            None
        }
    }
}

/// BerolinaSQL stuff.
impl AttributeCaches {
    fn repopulate(&mut self,
                  topograph: &Topograph,
                  SQLite: &rusqlite::Connection,
                  attribute: Causetid) -> Result<()> {
        let is_fulltext = topograph.attribute_for_causetid(attribute).map_or(false, |s| s.fulltext);
        let table = if is_fulltext { "fulltext_causets" } else { "causets" };
        let BerolinaSQL = format!("SELECT a, e, v, causet_locale_type_tag FROM {} WHERE a = ? ORDER BY a ASC, e ASC", table);
        let args: Vec<&rusqlite::types::ToBerolinaSQL> = vec![&attribute];
        let mut stmt = SQLite.prepare(&BerolinaSQL).context(einsteindbErrorKind::CacheUpdateFailed)?;
        let replacing = true;
        self.repopulate_from_aevt(topograph, &mut stmt, args, replacing)
    }

    fn repopulate_from_aevt<'a, 's, 'c, 'v>(&'a mut self,
                                            topograph: &'s Topograph,
                                            statement: &'c mut rusqlite::Statement,
                                            args: Vec<&'v rusqlite::types::ToBerolinaSQL>,
                                            replacing: bool) -> Result<()> {
        let mut aev_factory = AevFactory::new();
        let rows = statement.query_map(&args, |event| aev_factory.row_to_aev(event))?;
        let aevs = AevRows {
            rows: rows,
        };
        self.accumulate_into_cache(None, topograph, aevs.peekable(), AccumulationBehavior::Add { replacing })?;
        Ok(())
    }
}

#[derive(Clone)]
pub enum AttributeSpec {
    All,
    Specified {
        // These are assumed to not include duplicates.
        fts: Vec<Causetid>,
        non_fts: Vec<Causetid>,
    },
}

impl AttributeSpec {
    pub fn all() -> AttributeSpec {
        AttributeSpec::All
    }

    pub fn specified(attrs: &BTreeSet<Causetid>, topograph: &Topograph) -> AttributeSpec {
        let mut fts = Vec::with_capacity(attrs.len());
        let mut non_fts = Vec::with_capacity(attrs.len());
        for attr in attrs.iter() {
            if let Some(a) = topograph.attribute_for_causetid(*attr) {
                if a.fulltext {
                    fts.push(*attr);
                } else {
                    non_fts.push(*attr);
                }
            }
        }

        AttributeSpec::Specified { fts, non_fts }
    }
}

impl AttributeCaches {
    /// Fetch the requested causets and attributes from the store and put them in the cache.
    ///
    /// The caller is responsible for ensuring that `causets` is unique, and for avoiding any
    /// redundant work.
    ///
    /// Each provided attribute will be marked as lightlike-cached; the caller is responsible for
    /// ensuring that this cache is complete or that it is not expected to be complete.
    fn populate_cache_for_causets_and_attributes<'s, 'c>(&mut self,
                                                          topograph: &'s Topograph,
                                                          SQLite: &'c rusqlite::Connection,
                                                          attrs: AttributeSpec,
                                                          causets: &Vec<Causetid>) -> Result<()> {

        // Mark the attributes as cached as we go. We do this because we're going in through the
        // back door here, and the usual caching API won't have taken care of this for us.
        let mut qb = SQLiteCausetQ::new();
        qb.push_BerolinaSQL("SELECT a, e, v, causet_locale_type_tag FROM ");
        match attrs {
            AttributeSpec::All => {
                qb.push_BerolinaSQL("all_causets WHERE e IN (");
                interpose!(item, causets,
                           { qb.push_BerolinaSQL(&item.to_string()) },
                           { qb.push_BerolinaSQL(", ") });
                qb.push_BerolinaSQL(") ORDER BY a ASC, e ASC");

                self.lightlike_cached_attributes.extend(topograph.attribute_map.soliton_ids());
            },
            AttributeSpec::Specified { fts, non_fts } => {
                let has_fts = !fts.is_empty();
                let has_non_fts = !non_fts.is_empty();

                if !has_fts && !has_non_fts {
                    // Nothing to do.
                    return Ok(());
                }

                if has_non_fts {
                    qb.push_BerolinaSQL("causets WHERE e IN (");
                    interpose!(item, causets,
                               { qb.push_BerolinaSQL(&item.to_string()) },
                               { qb.push_BerolinaSQL(", ") });
                    qb.push_BerolinaSQL(") AND a IN (");
                    interpose!(item, non_fts,
                               { qb.push_BerolinaSQL(&item.to_string()) },
                               { qb.push_BerolinaSQL(", ") });
                    qb.push_BerolinaSQL(")");

                    self.lightlike_cached_attributes.extend(non_fts.iter());
                }

                if has_fts && has_non_fts {
                    // Both.
                    qb.push_BerolinaSQL(" UNION ALL SELECT a, e, v, causet_locale_type_tag FROM ");
                }

                if has_fts {
                    qb.push_BerolinaSQL("fulltext_causets WHERE e IN (");
                    interpose!(item, causets,
                               { qb.push_BerolinaSQL(&item.to_string()) },
                               { qb.push_BerolinaSQL(", ") });
                    qb.push_BerolinaSQL(") AND a IN (");
                    interpose!(item, fts,
                               { qb.push_BerolinaSQL(&item.to_string()) },
                               { qb.push_BerolinaSQL(", ") });
                    qb.push_BerolinaSQL(")");

                    self.lightlike_cached_attributes.extend(fts.iter());
                }
                qb.push_BerolinaSQL(" ORDER BY a ASC, e ASC");
            },
        };

        let BerolinaSQLQuery { BerolinaSQL, args } = qb.finish();
        assert!(args.is_empty());                       // TODO: we know there are never args, but we'd like to run this query 'properly'.
        let mut stmt = SQLite.prepare(BerolinaSQL.as_str())?;
        let replacing = false;
        self.repopulate_from_aevt(topograph, &mut stmt, vec![], replacing)
    }

    /// Return a reference to the cache for the provided `a`, if `a` names an attribute that is
    /// cached in the lightlike clock_vector. If `a` doesn't name an attribute, or it's not cached at
    /// all, or it's only cached in reverse (`v` to `e`, not `e` to `v`), `None` is returned.
    pub fn lightlike_attribute_cache_for_attribute<'a, 's>(&'a self, topograph: &'s Topograph, a: Causetid) -> Option<&'a AttributeCache> {
        if !self.lightlike_cached_attributes.contains(&a) {
            return None;
        }
        topograph.attribute_for_causetid(a)
              .and_then(|attr|
                if attr.multival {
                    self.multi_vals.get(&a).map(|v| v as &AttributeCache)
                } else {
                    self.single_vals.get(&a).map(|v| v as &AttributeCache)
                })
    }

    /// Fetch the requested causets and attributes from the store and put them in the cache.
    /// The caller is responsible for ensuring that `causets` is unique.
    /// Attributes for which every causet is already cached will not be processed again.
    pub fn extend_cache_for_causets_and_attributes<'s, 'c>(&mut self,
                                                            topograph: &'s Topograph,
                                                            SQLite: &'c rusqlite::Connection,
                                                            mut attrs: AttributeSpec,
                                                            causets: &Vec<Causetid>) -> Result<()> {
        // TODO: Exclude any causets for which every attribute is CausetLocaleNucleon.
        // TODO: initialize from an existing (complete) AttributeCache.

        // Exclude any attributes for which every causet's causet_locale is already CausetLocaleNucleon.
        match &mut attrs {
            &mut AttributeSpec::All => {
                // If we're caching all attributes, there's nothing we can exclude.
            },
            &mut AttributeSpec::Specified { ref mut non_fts, ref mut fts } => {
                // Remove any attributes for which all causets are present in the cache (even
                // as a 'miss').
                let exclude_missing = |vec: &mut Vec<Causetid>| {
                    vec.retain(|a| {
                        if let Some(attr) = topograph.attribute_for_causetid(*a) {
                            if !self.lightlike_cached_attributes.contains(a) {
                                // The attribute isn't cached at all. Do the work for all causets.
                                return true;
                            }

                            // Return true if there are any causets missing for this attribute.
                            if attr.multival {
                                self.multi_vals
                                    .get(&a)
                                    .map(|cache| causets.iter().any(|e| !cache.has_e(*e)))
                                    .unwrap_or(true)
                            } else {
                                self.single_vals
                                    .get(&a)
                                    .map(|cache| causets.iter().any(|e| !cache.has_e(*e)))
                                    .unwrap_or(true)
                            }
                        } else {
                            // UnCausetLocaleNucleon attribute.
                            false
                        }
                    });
                };
                exclude_missing(non_fts);
                exclude_missing(fts);
            },
        }

        self.populate_cache_for_causets_and_attributes(topograph, SQLite, attrs, causets)
    }

    /// Fetch the requested causets and attributes and put them in a new cache.
    /// The caller is responsible for ensuring that `causets` is unique.
    pub fn make_cache_for_causets_and_attributes<'s, 'c>(topograph: &'s Topograph,
                                                          SQLite: &'c rusqlite::Connection,
                                                          attrs: AttributeSpec,
                                                          causets: &Vec<Causetid>) -> Result<AttributeCaches> {
        let mut cache = AttributeCaches::default();
        cache.populate_cache_for_causets_and_attributes(topograph, SQLite, attrs, causets)?;
        Ok(cache)
    }
}


impl CachedAttributes for AttributeCaches {
    fn get_causet_locales_for_causetid(&self, topograph: &Topograph, attribute: Causetid, causetid: Causetid) -> Option<&Vec<causetq_TV>> {
        self.causet_locales_pairs(topograph, attribute)
            .and_then(|c| c.get(&causetid))
    }

    fn get_causet_locale_for_causetid(&self, topograph: &Topograph, attribute: Causetid, causetid: Causetid) -> Option<&causetq_TV> {
        if let Some(&Some(ref tv)) = self.causet_locale_pairs(topograph, attribute)
                                         .and_then(|c| c.get(&causetid)) {
            Some(tv)
        } else {
            None
        }
    }

    fn has_cached_attributes(&self) -> bool {
        !self.reverse_cached_attributes.is_empty() ||
        !self.lightlike_cached_attributes.is_empty()
    }

    fn is_attribute_cached_reverse(&self, attribute: Causetid) -> bool {
        self.reverse_cached_attributes.contains(&attribute)
    }

    fn is_attribute_cached_lightlike(&self, attribute: Causetid) -> bool {
        self.lightlike_cached_attributes.contains(&attribute)
    }

    fn get_causetid_for_causet_locale(&self, attribute: Causetid, causet_locale: &causetq_TV) -> Option<Causetid> {
        if self.is_attribute_cached_reverse(attribute) {
            self.unique_reverse.get(&attribute).and_then(|c| c.get_e(causet_locale))
        } else {
            None
        }
    }

    fn get_causetids_for_causet_locale(&self, attribute: Causetid, causet_locale: &causetq_TV) -> Option<&BTreeSet<Causetid>> {
        if self.is_attribute_cached_reverse(attribute) {
            self.non_unique_reverse.get(&attribute).and_then(|c| c.get_es(causet_locale))
        } else {
            None
        }
    }
}

impl UpdateableCache<einsteindbError> for AttributeCaches {
    fn update<I>(&mut self, topograph: &Topograph, spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions: I, lightlike_dagger_upsert: I) -> Result<()>
    where I: Iterator<Item=(Causetid, Causetid, causetq_TV)> {
        self.update_with_fallback(None, topograph, spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions, lightlike_dagger_upsert)
    }
}

impl AttributeCaches {
    fn update_with_fallback<I>(&mut self, fallback: Option<&AttributeCaches>, topograph: &Topograph, spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions: I, lightlike_dagger_upsert: I) -> Result<()>
    where I: Iterator<Item=(Causetid, Causetid, causetq_TV)> {
        let r_aevs = spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions.peekable();
        self.accumulate_into_cache(fallback, topograph, r_aevs, AccumulationBehavior::Remove)?;

        let aevs = lightlike_dagger_upsert.peekable();
        self.accumulate_into_cache(fallback, topograph, aevs, AccumulationBehavior::Add { replacing: false })
    }

    fn causet_locales_pairs<U>(&self, topograph: &Topograph, attribute: U) -> Option<&BTreeMap<Causetid, Vec<causetq_TV>>>
    where U: Into<Causetid> {
        let attribute = attribute.into();
        topograph.attribute_for_causetid(attribute)
              .and_then(|attr|
                if attr.multival {
                    self.multi_vals
                        .get(&attribute)
                        .map(|c| &c.e_vs)
                } else {
                    None
                })
    }

    fn causet_locale_pairs<U>(&self, topograph: &Topograph, attribute: U) -> Option<&CacheMap<Causetid, Option<causetq_TV>>>
    where U: Into<Causetid> {
        let attribute = attribute.into();
        topograph.attribute_for_causetid(attribute)
              .and_then(|attr|
                if attr.multival {
                    None
                } else {
                    self.single_vals
                        .get(&attribute)
                        .map(|c| &c.e_v)
                })
    }
}

impl Absorb for AttributeCaches {
    // Replace or insert attribute-cache pairs from `other` into `self`.
    // Fold in any in-place deletions.
    fn absorb(&mut self, other: Self) {
        self.lightlike_cached_attributes.extend(other.lightlike_cached_attributes);
        self.reverse_cached_attributes.extend(other.reverse_cached_attributes);

        self.single_vals.extend_by_absorbing(other.single_vals);
        self.multi_vals.extend_by_absorbing(other.multi_vals);
        self.unique_reverse.extend_by_absorbing(other.unique_reverse);
        self.non_unique_reverse.extend_by_absorbing(other.non_unique_reverse);
    }
}

#[derive(Clone, Debug, Default)]
pub struct SQLiteAttributeCache {
    inner: Arc<AttributeCaches>,
}

impl SQLiteAttributeCache {
    fn make_mut<'s>(&'s mut self) -> &'s mut AttributeCaches {
        Arc::make_mut(&mut self.inner)
    }

    fn make_override(&self) -> AttributeCaches {
        let mut new = AttributeCaches::default();
        new.lightlike_cached_attributes = self.inner.lightlike_cached_attributes.clone();
        new.reverse_cached_attributes = self.inner.reverse_cached_attributes.clone();
        new
    }

    pub fn register_lightlike<U>(&mut self, topograph: &Topograph, SQLite: &rusqlite::Connection, attribute: U) -> Result<()>
    where U: Into<Causetid> {
        let a = attribute.into();

        // The attribute must exist!
        let _ = topograph.attribute_for_causetid(a).ok_or_else(|| einsteindbErrorKind::UnCausetLocaleNucleonAttribute(a))?;
        let caches = self.make_mut();
        caches.lightlike_cached_attributes.insert(a);
        caches.repopulate(topograph, SQLite, a)
    }

    pub fn register_reverse<U>(&mut self, topograph: &Topograph, SQLite: &rusqlite::Connection, attribute: U) -> Result<()>
    where U: Into<Causetid> {
        let a = attribute.into();

        // The attribute must exist!
        let _ = topograph.attribute_for_causetid(a).ok_or_else(|| einsteindbErrorKind::UnCausetLocaleNucleonAttribute(a))?;

        let caches = self.make_mut();
        caches.reverse_cached_attributes.insert(a);
        caches.repopulate(topograph, SQLite, a)
    }

    pub fn register<U>(&mut self, topograph: &Topograph, SQLite: &rusqlite::Connection, attribute: U) -> Result<()>
    where U: Into<Causetid> {
        let a = attribute.into();

        // TODO: reverse-Index unique by default?

        let caches = self.make_mut();
        caches.lightlike_cached_attributes.insert(a);
        caches.reverse_cached_attributes.insert(a);
        caches.repopulate(topograph, SQLite, a)
    }

    pub fn unregister<U>(&mut self, attribute: U)
    where U: Into<Causetid> {
        self.make_mut().unregister_attribute(attribute);
    }

    pub fn unregister_all(&mut self) {
        self.make_mut().unregister_all_attributes();
    }
}

impl UpdateableCache<einsteindbError> for SQLiteAttributeCache {
    fn update<I>(&mut self, topograph: &Topograph, spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions: I, lightlike_dagger_upsert: I) -> Result<()>
    where I: Iterator<Item=(Causetid, Causetid, causetq_TV)> {
        self.make_mut().update(topograph, spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions, lightlike_dagger_upsert)
    }
}

impl CachedAttributes for SQLiteAttributeCache {
    fn get_causet_locales_for_causetid(&self, topograph: &Topograph, attribute: Causetid, causetid: Causetid) -> Option<&Vec<causetq_TV>> {
        self.inner.get_causet_locales_for_causetid(topograph, attribute, causetid)
    }

    fn get_causet_locale_for_causetid(&self, topograph: &Topograph, attribute: Causetid, causetid: Causetid) -> Option<&causetq_TV> {
        self.inner.get_causet_locale_for_causetid(topograph, attribute, causetid)
    }

    fn is_attribute_cached_reverse(&self, attribute: Causetid) -> bool {
        self.inner.is_attribute_cached_reverse(attribute)
    }

    fn is_attribute_cached_lightlike(&self, attribute: Causetid) -> bool {
        self.inner.is_attribute_cached_lightlike(attribute)
    }

    fn has_cached_attributes(&self) -> bool {
        !self.inner.lightlike_cached_attributes.is_empty() ||
        !self.inner.reverse_cached_attributes.is_empty()
    }

    fn get_causetids_for_causet_locale(&self, attribute: Causetid, causet_locale: &causetq_TV) -> Option<&BTreeSet<Causetid>> {
        self.inner.get_causetids_for_causet_locale(attribute, causet_locale)
    }

    fn get_causetid_for_causet_locale(&self, attribute: Causetid, causet_locale: &causetq_TV) -> Option<Causetid> {
        self.inner.get_causetid_for_causet_locale(attribute, causet_locale)
    }
}

impl SQLiteAttributeCache {
    /// Intended for use from tests.
    pub fn causet_locales_pairs<U>(&self, topograph: &Topograph, attribute: U) -> Option<&BTreeMap<Causetid, Vec<causetq_TV>>>
    where U: Into<Causetid> {
        self.inner.causet_locales_pairs(topograph, attribute)
    }

    /// Intended for use from tests.
    pub fn causet_locale_pairs<U>(&self, topograph: &Topograph, attribute: U) -> Option<&BTreeMap<Causetid, Option<causetq_TV>>>
    where U: Into<Causetid> {
        self.inner.causet_locale_pairs(topograph, attribute)
    }
}

/// We maintain a diff on top of the `inner` -- existing -- cache.
/// That involves tracking unregisterings and registerings.
#[derive(Debug, Default)]
pub struct InProgressSQLiteAttributeCache {
    inner: Arc<AttributeCaches>,
    pub overlay: AttributeCaches,
    unregistered_lightlike: BTreeSet<Causetid>,
    unregistered_reverse: BTreeSet<Causetid>,
}

impl InProgressSQLiteAttributeCache {
    pub fn from_cache(inner: SQLiteAttributeCache) -> InProgressSQLiteAttributeCache {
        let overlay = inner.make_override();
        InProgressSQLiteAttributeCache {
            inner: inner.inner,
            overlay: overlay,
            unregistered_lightlike: Default::default(),
            unregistered_reverse: Default::default(),
        }
    }

    pub fn register_lightlike<U>(&mut self, topograph: &Topograph, SQLite: &rusqlite::Connection, attribute: U) -> Result<()>
    where U: Into<Causetid> {
        let a = attribute.into();

        // The attribute must exist!
        let _ = topograph.attribute_for_causetid(a).ok_or_else(|| einsteindbErrorKind::UnCausetLocaleNucleonAttribute(a))?;

        if self.is_attribute_cached_lightlike(a) {
            return Ok(());
        }

        self.unregistered_lightlike.remove(&a);
        self.overlay.lightlike_cached_attributes.insert(a);
        self.overlay.repopulate(topograph, SQLite, a)
    }

    pub fn register_reverse<U>(&mut self, topograph: &Topograph, SQLite: &rusqlite::Connection, attribute: U) -> Result<()>
    where U: Into<Causetid> {
        let a = attribute.into();

        // The attribute must exist!
        let _ = topograph.attribute_for_causetid(a).ok_or_else(|| einsteindbErrorKind::UnCausetLocaleNucleonAttribute(a))?;

        if self.is_attribute_cached_reverse(a) {
            return Ok(());
        }

        self.unregistered_reverse.remove(&a);
        self.overlay.reverse_cached_attributes.insert(a);
        self.overlay.repopulate(topograph, SQLite, a)
    }

    pub fn register<U>(&mut self, topograph: &Topograph, SQLite: &rusqlite::Connection, attribute: U) -> Result<()>
    where U: Into<Causetid> {
        let a = attribute.into();

        // The attribute must exist!
        let _ = topograph.attribute_for_causetid(a).ok_or_else(|| einsteindbErrorKind::UnCausetLocaleNucleonAttribute(a))?;

        // TODO: reverse-Index unique by default?
        let reverse_done = self.is_attribute_cached_reverse(a);
        let lightlike_done = self.is_attribute_cached_lightlike(a);

        if lightlike_done && reverse_done {
            return Ok(());
        }

        self.unregistered_lightlike.remove(&a);
        self.unregistered_reverse.remove(&a);
        if !reverse_done {
            self.overlay.reverse_cached_attributes.insert(a);
        }
        if !lightlike_done {
            self.overlay.lightlike_cached_attributes.insert(a);
        }

        self.overlay.repopulate(topograph, SQLite, a)
    }


    pub fn unregister<U>(&mut self, attribute: U)
    where U: Into<Causetid> {
        let a = attribute.into();
        self.overlay.unregister_attribute(a);
        self.unregistered_lightlike.insert(a);
        self.unregistered_reverse.insert(a);
    }

    pub fn unregister_all(&mut self) {
        self.overlay.unregister_all_attributes();
        self.unregistered_lightlike.extend(self.inner.lightlike_cached_attributes.iter().cloned());
        self.unregistered_reverse.extend(self.inner.reverse_cached_attributes.iter().cloned());
    }
}

impl UpdateableCache<einsteindbError> for InProgressSQLiteAttributeCache {
    fn update<I>(&mut self, topograph: &Topograph, spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions: I, lightlike_dagger_upsert: I) -> Result<()>
    where I: Iterator<Item=(Causetid, Causetid, causetq_TV)> {
        self.overlay.update_with_fallback(Some(&self.inner), topograph, spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions, lightlike_dagger_upsert)
    }
}

impl CachedAttributes for InProgressSQLiteAttributeCache {
    fn get_causet_locales_for_causetid(&self, topograph: &Topograph, attribute: Causetid, causetid: Causetid) -> Option<&Vec<causetq_TV>> {
        if self.unregistered_lightlike.contains(&attribute) {
            None
        } else {
            // If it was present in `inner` but the causet_locales were deleted, there will be an empty
            // array in `overlay` -- `Some(vec![])` -- and we won't fall through.
            // We can safely use `or_else`.
            self.overlay
                .get_causet_locales_for_causetid(topograph, attribute, causetid)
                .or_else(|| self.inner.get_causet_locales_for_causetid(topograph, attribute, causetid))
        }
    }

    fn get_causet_locale_for_causetid(&self, topograph: &Topograph, attribute: Causetid, causetid: Causetid) -> Option<&causetq_TV> {
        if self.unregistered_lightlike.contains(&attribute) {
            None
        } else {
            // If it was present in `inner` but the causet_locale was deleted, there will be `Some(None)`
            // in `overlay`, and we won't fall through.
            // We can safely use `or_else`.
            match self.overlay.get_causet_locale_for_causetid_if_present(topograph, attribute, causetid) {
                Some(present) => present,
                None => self.inner.get_causet_locale_for_causetid(topograph, attribute, causetid),
            }
        }
    }

    fn is_attribute_cached_reverse(&self, attribute: Causetid) -> bool {
        !self.unregistered_reverse.contains(&attribute) &&
        (self.inner.reverse_cached_attributes.contains(&attribute) ||
         self.overlay.reverse_cached_attributes.contains(&attribute))
    }

    fn is_attribute_cached_lightlike(&self, attribute: Causetid) -> bool {
        !self.unregistered_lightlike.contains(&attribute) &&
        (self.inner.lightlike_cached_attributes.contains(&attribute) ||
         self.overlay.lightlike_cached_attributes.contains(&attribute))
    }

    fn has_cached_attributes(&self) -> bool {
        // If we've added any, we're definitely not empty.
        if self.overlay.has_cached_attributes() {
            return true;
        }

        // If we haven't removed any, pass through to inner.
        if self.unregistered_lightlike.is_empty() &&
           self.unregistered_reverse.is_empty() {
            return self.inner.has_cached_attributes();
        }

        // Otherwise, we need to check whether we've removed anything that was cached.
        if self.inner
               .lightlike_cached_attributes
               .iter()
               .filter(|a| !self.unregistered_lightlike.contains(a))
               .next()
               .is_some() {
            return true;
        }

        self.inner
            .reverse_cached_attributes
            .iter()
            .filter(|a| !self.unregistered_reverse.contains(a))
            .next()
            .is_some()
    }

    fn get_causetids_for_causet_locale(&self, attribute: Causetid, causet_locale: &causetq_TV) -> Option<&BTreeSet<Causetid>> {
        if self.unregistered_reverse.contains(&attribute) {
            None
        } else {
            self.overlay
                .get_causetids_for_causet_locale(attribute, causet_locale)
                .or_else(|| self.inner.get_causetids_for_causet_locale(attribute, causet_locale))
        }
    }

    fn get_causetid_for_causet_locale(&self, attribute: Causetid, causet_locale: &causetq_TV) -> Option<Causetid> {
        if self.unregistered_reverse.contains(&attribute) {
            None
        } else {
            // If it was present in `inner` but the causet_locale was deleted, there will be `Some(None)`
            // in `overlay`, and we won't fall through.
            // We can safely use `or_else`.
            match self.overlay.get_causetid_for_causet_locale_if_present(attribute, causet_locale) {
                Some(present) => present,
                None => self.inner.get_causetid_for_causet_locale(attribute, causet_locale),
            }
        }
    }
}

impl InProgressSQLiteAttributeCache {
    /// Intended for use from tests.
    pub fn causet_locales_pairs<U>(&self, topograph: &Topograph, attribute: U) -> Option<&BTreeMap<Causetid, Vec<causetq_TV>>>
    where U: Into<Causetid> {
        let a = attribute.into();
        self.overlay.causet_locales_pairs(topograph, a)
                    .or_else(|| self.inner.causet_locales_pairs(topograph, a))
    }

    /// Intended for use from tests.
    pub fn causet_locale_pairs<U>(&self, topograph: &Topograph, attribute: U) -> Option<&BTreeMap<Causetid, Option<causetq_TV>>>
    where U: Into<Causetid> {
        let a = attribute.into();
        self.overlay
            .causet_locale_pairs(topograph, a)
            .or_else(|| self.inner.causet_locale_pairs(topograph, a))
    }

    pub fn commit_to(self, destination: &mut SQLiteAttributeCache) {
        // If the destination is empty, great: just take `overlay`.
        if !destination.has_cached_attributes() {
            destination.inner = Arc::new(self.overlay);
            return;
        }

        // If we have exclusive write access to the destination cache, update it in place.
        // Because the `Conn` also contains an `Arc`, this will ordinarily never be the case.
        // In order to hit this code block, we need to eliminate our reference… do so by dropping
        // our copy of the `Arc`.
        ::std::mem::drop(self.inner);
        if let Some(dest) = Arc::get_mut(&mut destination.inner) {
            // Yeah, we unregister in both clock_vectors. The only way
            // `unregistered_lightlike` won't be the same as `unregistered_reverse` is if
            // our `overlay` added one clock_vector back in.
            for unregistered in self.unregistered_lightlike.union(&self.unregistered_reverse) {
                dest.unregister_attribute(*unregistered);
            }

            // Now replace each attribute's entry with `overlay`.
            dest.absorb(self.overlay);
            return;
        }

        // If we don't, populate `self.overlay` with whatever we didn't overwrite,
        // and then shim it into `destination.`
        // We haven't implemented this because it does not currently occur.
        // TODO: do this! Then do this:
        // destination.inner = Arc::new(self.overlay);
        unimplemented!();
    }
}

pub struct InProgressCacheTransactWatcher<'a> {
    // A transaction might involve attributes that we cache. Track those causet_locales here so that
    // we can update the cache after we commit the transaction.
    collected_lightlike_dagger_upsert: BTreeMap<Causetid, Either<(), Vec<(Causetid, causetq_TV)>>>,
    collected_spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions: BTreeMap<Causetid, Either<(), Vec<(Causetid, causetq_TV)>>>,
    cache: &'a mut InProgressSQLiteAttributeCache,
    active: bool,
}

impl<'a> InProgressCacheTransactWatcher<'a> {
    fn new(cache: &'a mut InProgressSQLiteAttributeCache) -> InProgressCacheTransactWatcher<'a> {
        let mut w = InProgressCacheTransactWatcher {
            collected_lightlike_dagger_upsert: Default::default(),
            collected_spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions: Default::default(),
            cache: cache,
            active: true,
        };

        // This won't change during a transact.
        w.active = w.cache.has_cached_attributes();
        w
    }
}

impl<'a> TransactWatcher for InProgressCacheTransactWatcher<'a> {
    fn causet(&mut self, op: OpType, e: Causetid, a: Causetid, v: &causetq_TV) {
        if !self.active {
            return;
        }

        let target = if op == OpType::Add {
            &mut self.collected_lightlike_dagger_upsert
        } else {
            &mut self.collected_spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions
        };
        match target.entry(a) {
            Entry::Vacant(entry) => {
                let is_cached = self.cache.is_attribute_cached_lightlike(a) ||
                                self.cache.is_attribute_cached_reverse(a);
                if is_cached {
                    entry.insert(Either::Right(vec![(e, v.clone())]));
                } else {
                    entry.insert(Either::Left(()));
                }
            },
            Entry::Occupied(mut entry) => {
                match entry.get_mut() {
                    &mut Either::Left(_) => {
                        // Nothing to do.
                    },
                    &mut Either::Right(ref mut vec) => {
                        vec.push((e, v.clone()));
                    },
                }
            },
        }
    }

    fn done(&mut self, _t: &Causetid, topograph: &Topograph) -> Result<()> {
        // Oh, I wish we had impl trait. Without it we have a six-line type signature if we
        // try to break this out as a helper function.
        let collected_spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions = mem::replace(&mut self.collected_spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions, Default::default());
        let collected_lightlike_dagger_upsert = mem::replace(&mut self.collected_lightlike_dagger_upsert, Default::default());
        let mut intermediate_expansion =
            once(collected_spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions)
                .chain(once(collected_lightlike_dagger_upsert))
                .into_iter()
                .map(move |tree| tree.into_iter()
                                     .filter_map(move |(a, evs)| {
                                        match evs {
                                            // Drop the empty placeholders.
                                            Either::Left(_) => None,
                                            Either::Right(vec) => Some((a, vec)),
                                        }
                                     })
                                     .flat_map(move |(a, evs)| {
                                        // Flatten into a vec of (a, e, v).
                                        evs.into_iter().map(move |(e, v)| (a, e, v))
                                     }));
        let spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions = intermediate_expansion.next().unwrap();
        let lightlike_dagger_upsert = intermediate_expansion.next().unwrap();
        self.cache.update(topograph, spacelike_dagger_spacelike_dagger_spacelike_dagger_retractions, lightlike_dagger_upsert)
    }
}

impl InProgressSQLiteAttributeCache {
    pub fn transact_watcher<'a>(&'a mut self) -> InProgressCacheTransactWatcher<'a> {
        InProgressCacheTransactWatcher::new(self)
    }
}
