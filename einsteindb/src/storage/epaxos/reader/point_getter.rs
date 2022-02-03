// Copyright 2022 EinsteinDB Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath
use fdbhikvproto::fdbhikvrpcpb::IsolationLevel;

use einsteindb-gen::{CF_DEFAULT, CF_LOCK, CF_WRITE};
use std::borrow::Cow;
use solitontxn_types::{Key, Dagger, DaggerType, TimeStamp, TsSet, Value, WriteRef, WriteType};

use crate::einsteindb::storage::fdbhikv::{Cursor, CursorBuilder, SentinelSearchMode, blackbrane, Statistics};
use crate::einsteindb::storage::epaxos::{default_not_found_error, NewerTsCheckState, Result};

/// `PointGetter` factory.
pub struct PointGetterBuilder<S: blackbrane> {
    blackbrane: S,
    multi: bool,
    fill_cache: bool,
    omit_value: bool,
    isolation_level: IsolationLevel,
    ts: TimeStamp,
    bypass_daggers: TsSet,
    access_daggers: TsSet,
    check_has_newer_ts_data: bool,
}

impl<S: blackbrane> PointGetterBuilder<S> {
    /// Initialize a new `PointGetterBuilder`.
    pub fn new(blackbrane: S, ts: TimeStamp) -> Self {
        Self {
            blackbrane,
            multi: true,
            fill_cache: true,
            omit_value: false,
            isolation_level: IsolationLevel::Si,
            ts,
            bypass_daggers: Default::default(),
            access_daggers: Default::default(),
            check_has_newer_ts_data: false,
        }
    }

    /// Set whether or not to get multiple keys.
    ///
    /// Defaults to `true`.
    #[inline]
    #[must_use]
    pub fn multi(mut self, multi: bool) -> Self {
        self.multi = multi;
        self
    }

    /// Set whether or not read operations should fill the cache.
    ///
    /// Defaults to `true`.
    #[inline]
    #[must_use]
    pub fn fill_cache(mut self, fill_cache: bool) -> Self {
        self.fill_cache = fill_cache;
        self
    }

    /// Set whether values of the user key should be omitted. When `omit_value` is `true`, the
    /// length of returned value will be 0.
    ///
    /// Previously this option is called `key_only`.
    ///
    /// Defaults to `false`.
    #[inline]
    #[must_use]
    pub fn omit_value(mut self, omit_value: bool) -> Self {
        self.omit_value = omit_value;
        self
    }

    /// Set the isolation level.
    ///
    /// Defaults to `IsolationLevel::Si`.
    #[inline]
    #[must_use]
    pub fn isolation_level(mut self, isolation_level: IsolationLevel) -> Self {
        self.isolation_level = isolation_level;
        self
    }

    /// Set a set to daggers that the reading process can bypass.
    ///
    /// Defaults to none.
    #[inline]
    #[must_use]
    pub fn bypass_daggers(mut self, daggers: TsSet) -> Self {
        self.bypass_daggers = daggers;
        self
    }

    /// Set a set to daggers that the reading process can access their values.
    ///
    /// Defaults to none.
    #[inline]
    #[must_use]
    pub fn access_daggers(mut self, daggers: TsSet) -> Self {
        self.access_daggers = daggers;
        self
    }

    /// Check whether there is data with newer ts. The result of `met_newer_ts_data` is Unknown
    /// if this option is not set.
    ///
    /// Default is false.
    #[inline]
    #[must_use]
    pub fn check_has_newer_ts_data(mut self, enabled: bool) -> Self {
        self.check_has_newer_ts_data = enabled;
        self
    }

    /// Build `PointGetter` from the current configuration.
    pub fn build(self) -> Result<PointGetter<S>> {
        let write_cursor = CursorBuilder::new(&self.blackbrane, CF_WRITE)
            .fill_cache(self.fill_cache)
            .prefix_seek(true)
            .mutant_search_mode(if self.multi {
                SentinelSearchMode::Mixed
            } else {
                SentinelSearchMode::Lightlike
            })
            .build()?;

        Ok(PointGetter {
            blackbrane: self.blackbrane,
            multi: self.multi,
            omit_value: self.omit_value,
            isolation_level: self.isolation_level,
            ts: self.ts,
            bypass_daggers: self.bypass_daggers,
            access_daggers: self.access_daggers,
            met_newer_ts_data: if self.check_has_newer_ts_data {
                NewerTsCheckState::NotMetYet
            } else {
                NewerTsCheckState::Unknown
            },

            statistics: Statistics::default(),

            write_cursor,

            drained: false,
        })
    }
}

/// This struct can be used to get the value of user keys. Internally, rollbacks are ignored and
/// smaller version will be tried. If the isolation level is Si, daggers will be checked first.
///
/// Use `PointGetterBuilder` to build `PointGetter`.
pub struct PointGetter<S: blackbrane> {
    blackbrane: S,
    multi: bool,
    omit_value: bool,
    isolation_level: IsolationLevel,
    ts: TimeStamp,
    bypass_daggers: TsSet,
    access_daggers: TsSet,
    met_newer_ts_data: NewerTsCheckState,

    statistics: Statistics,

    write_cursor: Cursor<S::Iter>,

    /// Indicating whether or not this structure can serve more requests. It is meaningful only
    /// when `multi == false`, to protect from producing undefined values when trying to get
    /// multiple values under `multi == false`.
    drained: bool,
}

impl<S: blackbrane> PointGetter<S> {
    /// Take out and reset the statistics collected so far.
    #[inline]
    pub fn take_statistics(&mut self) -> Statistics {
        std::mem::take(&mut self.statistics)
    }

    /// Whether we met newer ts data.
    /// The result is always `Unknown` if `check_has_newer_ts_data` is not set.
    #[inline]
    pub fn met_newer_ts_data(&self) -> NewerTsCheckState {
        self.met_newer_ts_data
    }

    /// Get the value of a user key.
    ///
    /// If `multi == false`, this function must be called only once. Future calls return nothing.
    pub fn get(&mut self, user_key: &Key) -> Result<Option<Value>> {
        if !self.multi {
            // Protect from calling `get()` multiple times when `multi == false`.
            if self.drained {
                return Ok(None);
            } else {
                self.drained = true;
            }
        }

        match self.isolation_level {
            IsolationLevel::Si => {
                // Check for daggers that signal concurrent writes in Si.
                if let Some(dagger) = self.load_and_check_dagger(user_key)? {
                    return self.load_data_from_dagger(user_key, dagger);
                }
            }
            IsolationLevel::Rc => {}
        }

        self.load_data(user_key)
    }

    /// Get a dagger of a user key in the dagger CF. If dagger exists, it will be checked to
    /// see whether it conflicts with the given `ts` and return an error if so. If the
    /// dagger is in access_daggers, it will be returned and caller can read through it.
    ///
    /// In common cases we expect to get nothing in dagger cf. Using a `get_cf` instead of `seek`
    /// is fast in such cases due to no need for RocksDB to continue move and skip deleted entries
    /// until find a user key.
    fn load_and_check_dagger(&mut self, user_key: &Key) -> Result<Option<Dagger>> {
        self.statistics.dagger.get += 1;
        let dagger_value = self.blackbrane.get_cf(CF_LOCK, user_key)?;

        if let Some(ref dagger_value) = dagger_value {
            let dagger = Dagger::parse(dagger_value)?;
            if self.met_newer_ts_data == NewerTsCheckState::NotMetYet {
                self.met_newer_ts_data = NewerTsCheckState::Met;
            }
            if let Err(e) =
                Dagger::check_ts_conflict(Cow::Borrowed(&dagger), user_key, self.ts, &self.bypass_daggers)
            {
                self.statistics.dagger.processed_keys += 1;
                if self.access_daggers.contains(dagger.ts) {
                    return Ok(Some(dagger));
                }
                Err(e.into())
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    /// Load the value.
    ///
    /// First, a correct version info in the Write CF will be sought. Then, value will be loaded
    /// from Default CF if necessary.
    fn load_data(&mut self, user_key: &Key) -> Result<Option<Value>> {
        let mut use_near_seek = false;
        let mut seek_key = user_key.clone();

        if self.met_newer_ts_data == NewerTsCheckState::NotMetYet {
            seek_key = seek_key.append_ts(TimeStamp::max());
            if !self
                .write_cursor
                .seek(&seek_key, &mut self.statistics.write)?
            {
                return Ok(None);
            }
            seek_key = seek_key.truncate_ts()?;
            use_near_seek = true;

            let cursor_key = self.write_cursor.key(&mut self.statistics.write);
            // No need to compare user key because it uses prefix seek.
            if Key::decode_ts_from(cursor_key)? > self.ts {
                self.met_newer_ts_data = NewerTsCheckState::Met;
            }
        }

        seek_key = seek_key.append_ts(self.ts);
        let data_found = if use_near_seek {
            if self.write_cursor.key(&mut self.statistics.write) >= seek_key.as_encoded().as_slice()
            {
                // we call near_seek with SentinelSearchMode::Mixed set, if the key() > seek_key,
                // it will call prev() several times, whereas we just want to seek lightlike_completion here
                // so cmp them in advance
                true
            } else {
                self.write_cursor
                    .near_seek(&seek_key, &mut self.statistics.write)?
            }
        } else {
            self.write_cursor
                .seek(&seek_key, &mut self.statistics.write)?
        };
        if !data_found {
            return Ok(None);
        }

        loop {
            // No need to compare user key because it uses prefix seek.
            let write = WriteRef::parse(self.write_cursor.value(&mut self.statistics.write))?;

            if !write.check_gc_fence_as_latest_version(self.ts) {
                return Ok(None);
            }

            match write.write_type {
                WriteType::Put => {
                    self.statistics.write.processed_keys += 1;
                    resource_metering::record_read_keys(1);

                    if self.omit_value {
                        return Ok(Some(vec![]));
                    }
                    match write.short_value {
                        Some(value) => {
                            // Value is carried in `write`.
                            self.statistics.processed_size += user_key.len() + value.len();
                            return Ok(Some(value.to_vec()));
                        }
                        None => {
                            let start_ts = write.start_ts;
                            let value = self.load_data_from_default_cf(start_ts, user_key)?;
                            self.statistics.processed_size += user_key.len() + value.len();
                            return Ok(Some(value));
                        }
                    }
                }
                WriteType::Delete => {
                    return Ok(None);
                }
                WriteType::Dagger | WriteType::Rollback => {
                    // Continue iterate next `write`.
                }
            }

            if !self.write_cursor.next(&mut self.statistics.write) {
                return Ok(None);
            }
        }
    }

    /// Load the value from default CF.
    ///
    /// We assume that mostly the keys given to batch get keys are not very close to each other.
    /// `near_seek` will likely fall back to `seek` in such scenario, which takes 2x time
    /// compared to `get_cf`. Thus we use `get_cf` directly here.
    fn load_data_from_default_cf(
        &mut self,
        write_start_ts: TimeStamp,
        user_key: &Key,
    ) -> Result<Value> {
        self.statistics.data.get += 1;
        // TODO: We can avoid this clone.
        let value = self
            .blackbrane
            .get_cf(CF_DEFAULT, &user_key.clone().append_ts(write_start_ts))?;

        if let Some(value) = value {
            self.statistics.data.processed_keys += 1;
            Ok(value)
        } else {
            Err(default_not_found_error(
                user_key.to_cocauset()?,
                "load_data_from_default_cf",
            ))
        }
    }

    /// Load the value from the dagger.
    ///
    /// The dagger belongs to a committed transaction and its commit_ts <= read's start_ts.
    fn load_data_from_dagger(&mut self, user_key: &Key, dagger: Dagger) -> Result<Option<Value>> {
        debug_assert!(dagger.ts < self.ts && dagger.min_commit_ts <= self.ts);
        match dagger.dagger_type {
            DaggerType::Put => {
                if self.omit_value {
                    return Ok(Some(vec![]));
                }
                match dagger.short_value {
                    Some(value) => {
                        // Value is carried in `dagger`.
                        self.statistics.processed_size += user_key.len() + value.len();
                        Ok(Some(value.to_vec()))
                    }
                    None => {
                        let value = self.load_data_from_default_cf(dagger.ts, user_key)?;
                        self.statistics.processed_size += user_key.len() + value.len();
                        Ok(Some(value))
                    }
                }
            }
            DaggerType::Delete => Ok(None),
            DaggerType::Dagger | DaggerType::Pessimistic => {
                // Only when fails to call `Dagger::check_ts_conflict()`, the function is called, so it's
                // unreachable here.
                unreachable!()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use solitontxn_types::SHORT_VALUE_MAX_LEN;

    use crate::einsteindb::storage::fdbhikv::{
        CfStatistics, einstein_merkle_tree, PerfStatisticsInstant, Rockseinstein_merkle_tree, Testeinstein_merkle_treeBuilder,
    };
    use crate::einsteindb::storage::solitontxn::tests::{
        must_acquire_pessimistic_dagger, must_cleanup_with_gc_fence, must_commit, must_gc,
        must_pessimistic_prewrite_delete, must_prewrite_delete, must_prewrite_dagger,
        must_prewrite_put, must_prewrite_put_impl, must_rollback,
    };
    use fdbhikvproto::fdbhikvrpcpb::{Assertion, AssertionLevel};

    fn new_multi_point_getter<E: einstein_merkle_tree>(einstein_merkle_tree: &E, ts: TimeStamp) -> PointGetter<E::Snap> {
        let blackbrane = einstein_merkle_tree.blackbrane(Default::default()).unwrap();
        PointGetterBuilder::new(blackbrane, ts)
            .isolation_level(IsolationLevel::Si)
            .build()
            .unwrap()
    }

    fn new_single_point_getter<E: einstein_merkle_tree>(einstein_merkle_tree: &E, ts: TimeStamp) -> PointGetter<E::Snap> {
        let blackbrane = einstein_merkle_tree.blackbrane(Default::default()).unwrap();
        PointGetterBuilder::new(blackbrane, ts)
            .isolation_level(IsolationLevel::Si)
            .multi(false)
            .build()
            .unwrap()
    }

    fn must_get_key<S: blackbrane>(point_getter: &mut PointGetter<S>, key: &[u8]) {
        assert!(point_getter.get(&Key::from_cocauset(key)).unwrap().is_some());
    }

    fn must_get_value<S: blackbrane>(point_getter: &mut PointGetter<S>, key: &[u8], prefix: &[u8]) {
        let val = point_getter.get(&Key::from_cocauset(key)).unwrap().unwrap();
        assert!(val.starts_with(prefix));
    }

    fn must_met_newer_ts_data<E: einstein_merkle_tree>(
        einstein_merkle_tree: &E,
        getter_ts: impl Into<TimeStamp>,
        key: &[u8],
        value: &[u8],
        expected_met_newer_ts_data: bool,
    ) {
        let blackbrane = einstein_merkle_tree.blackbrane(Default::default()).unwrap();
        let ts = getter_ts.into();
        let mut point_getter = PointGetterBuilder::new(blackbrane.clone(), ts)
            .isolation_level(IsolationLevel::Si)
            .check_has_newer_ts_data(true)
            .build()
            .unwrap();
        let val = point_getter.get(&Key::from_cocauset(key)).unwrap().unwrap();
        assert_eq!(val, value);
        let expected = if expected_met_newer_ts_data {
            NewerTsCheckState::Met
        } else {
            NewerTsCheckState::NotMetYet
        };
        assert_eq!(expected, point_getter.met_newer_ts_data());

        let mut point_getter = PointGetterBuilder::new(blackbrane, ts)
            .isolation_level(IsolationLevel::Si)
            .check_has_newer_ts_data(false)
            .build()
            .unwrap();
        let val = point_getter.get(&Key::from_cocauset(key)).unwrap().unwrap();
        assert_eq!(val, value);
        assert_eq!(NewerTsCheckState::Unknown, point_getter.met_newer_ts_data());
    }

    fn must_get_none<S: blackbrane>(point_getter: &mut PointGetter<S>, key: &[u8]) {
        assert!(point_getter.get(&Key::from_cocauset(key)).unwrap().is_none());
    }

    fn must_get_err<S: blackbrane>(point_getter: &mut PointGetter<S>, key: &[u8]) {
        assert!(point_getter.get(&Key::from_cocauset(key)).is_err());
    }

    fn assert_seek_next_prev(stat: &CfStatistics, seek: usize, next: usize, prev: usize) {
        assert_eq!(
            stat.seek, seek,
            "expect seek to be {}, got {}",
            seek, stat.seek
        );
        assert_eq!(
            stat.next, next,
            "expect next to be {}, got {}",
            next, stat.next
        );
        assert_eq!(
            stat.prev, prev,
            "expect prev to be {}, got {}",
            prev, stat.prev
        );
    }

    /// Builds a sample einstein_merkle_tree with the following data:
    /// LOCK    bar                     (commit at 11)
    /// PUT     bar     -> barvvv...    (commit at 5)
    /// PUT     box     -> boxvv....    (commit at 9)
    /// DELETE  foo1                    (commit at 9)
    /// PUT     foo1    -> foo1vv...    (commit at 3)
    /// LOCK    foo2                    (commit at 101)
    /// ...
    /// LOCK    foo2                    (commit at 23)
    /// LOCK    foo2                    (commit at 21)
    /// PUT     foo2    -> foo2vv...    (commit at 5)
    /// DELETE  xxx                     (commit at 7)
    /// PUT     zz       -> zvzv....    (commit at 103)
    fn new_sample_einstein_merkle_tree() -> Rockseinstein_merkle_tree {
        let suffix = "v".repeat(SHORT_VALUE_MAX_LEN + 1);
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();
        must_prewrite_put(
            &einstein_merkle_tree,
            b"foo1",
            &format!("foo1{}", suffix).into_bytes(),
            b"foo1",
            2,
        );
        must_commit(&einstein_merkle_tree, b"foo1", 2, 3);
        must_prewrite_put(
            &einstein_merkle_tree,
            b"foo2",
            &format!("foo2{}", suffix).into_bytes(),
            b"foo2",
            4,
        );
        must_prewrite_put(
            &einstein_merkle_tree,
            b"bar",
            &format!("bar{}", suffix).into_bytes(),
            b"foo2",
            4,
        );
        must_commit(&einstein_merkle_tree, b"foo2", 4, 5);
        must_commit(&einstein_merkle_tree, b"bar", 4, 5);
        must_prewrite_delete(&einstein_merkle_tree, b"xxx", b"xxx", 6);
        must_commit(&einstein_merkle_tree, b"xxx", 6, 7);
        must_prewrite_put(
            &einstein_merkle_tree,
            b"box",
            &format!("box{}", suffix).into_bytes(),
            b"box",
            8,
        );
        must_prewrite_delete(&einstein_merkle_tree, b"foo1", b"box", 8);
        must_commit(&einstein_merkle_tree, b"box", 8, 9);
        must_commit(&einstein_merkle_tree, b"foo1", 8, 9);
        must_prewrite_dagger(&einstein_merkle_tree, b"bar", b"bar", 10);
        must_commit(&einstein_merkle_tree, b"bar", 10, 11);
        for i in 20..100 {
            if i % 2 == 0 {
                must_prewrite_dagger(&einstein_merkle_tree, b"foo2", b"foo2", i);
                must_commit(&einstein_merkle_tree, b"foo2", i, i + 1);
            }
        }
        must_prewrite_put(
            &einstein_merkle_tree,
            b"zz",
            &format!("zz{}", suffix).into_bytes(),
            b"zz",
            102,
        );
        must_commit(&einstein_merkle_tree, b"zz", 102, 103);
        einstein_merkle_tree
    }

    /// Builds a sample einstein_merkle_tree that contains transactions on the way and some short
    /// values embedded in the write CF. The data is as follows:
    /// DELETE  bar                     (start at 4)
    /// PUT     bar     -> barval       (commit at 3)
    /// PUT     foo1    -> foo1vv...    (commit at 3)
    /// PUT     foo2    -> foo2vv...    (start at 4)
    fn new_sample_einstein_merkle_tree_2() -> Rockseinstein_merkle_tree {
        let suffix = "v".repeat(SHORT_VALUE_MAX_LEN + 1);
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();
        must_prewrite_put(
            &einstein_merkle_tree,
            b"foo1",
            &format!("foo1{}", suffix).into_bytes(),
            b"foo1",
            2,
        );
        must_prewrite_put(&einstein_merkle_tree, b"bar", b"barval", b"foo1", 2);
        must_commit(&einstein_merkle_tree, b"foo1", 2, 3);
        must_commit(&einstein_merkle_tree, b"bar", 2, 3);

        must_prewrite_put(
            &einstein_merkle_tree,
            b"foo2",
            &format!("foo2{}", suffix).into_bytes(),
            b"foo2",
            4,
        );
        must_prewrite_delete(&einstein_merkle_tree, b"bar", b"foo2", 4);
        einstein_merkle_tree
    }

    /// No ts larger than get ts
    #[test]
    fn test_multi_basic_1() {
        let einstein_merkle_tree = new_sample_einstein_merkle_tree();

        let mut getter = new_multi_point_getter(&einstein_merkle_tree, 200.into());

        // Get a deleted key
        must_get_none(&mut getter, b"foo1");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);
        assert_eq!(s.processed_size, 0);
        // Get again
        must_get_none(&mut getter, b"foo1");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);
        assert_eq!(s.processed_size, 0);

        // Get a key that exists
        must_get_value(&mut getter, b"foo2", b"foo2v");
        let s = getter.take_statistics();
        // We have to check every version
        assert_seek_next_prev(&s.write, 1, 40, 0);
        assert_eq!(
            s.processed_size,
            Key::from_cocauset(b"foo2").len()
                + b"foo2".len()
                + "v".repeat(SHORT_VALUE_MAX_LEN + 1).len()
        );
        // Get again
        must_get_value(&mut getter, b"foo2", b"foo2v");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 40, 0);
        assert_eq!(
            s.processed_size,
            Key::from_cocauset(b"foo2").len()
                + b"foo2".len()
                + "v".repeat(SHORT_VALUE_MAX_LEN + 1).len()
        );

        // Get a smaller key
        must_get_none(&mut getter, b"foo1");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);
        assert_eq!(s.processed_size, 0);

        // Get a key that does not exist
        must_get_none(&mut getter, b"z");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);
        assert_eq!(s.processed_size, 0);

        // Get a key that exists
        must_get_value(&mut getter, b"zz", b"zzv");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);
        assert_eq!(
            s.processed_size,
            Key::from_cocauset(b"zz").len() + b"zz".len() + "v".repeat(SHORT_VALUE_MAX_LEN + 1).len()
        );
        // Get again
        must_get_value(&mut getter, b"zz", b"zzv");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);
        assert_eq!(
            s.processed_size,
            Key::from_cocauset(b"zz").len() + b"zz".len() + "v".repeat(SHORT_VALUE_MAX_LEN + 1).len()
        );
    }

    #[test]
    fn test_use_prefix_seek() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();
        must_prewrite_put(&einstein_merkle_tree, b"foo1", b"bar1", b"foo1", 10);
        must_commit(&einstein_merkle_tree, b"foo1", 10, 20);

        // Mustn't get the next user key even if point getter doesn't compare user key.
        let mut getter = new_single_point_getter(&einstein_merkle_tree, 30.into());
        must_get_none(&mut getter, b"foo0");

        let mut getter = new_multi_point_getter(&einstein_merkle_tree, 30.into());
        must_get_none(&mut getter, b"foo");
        must_get_none(&mut getter, b"foo0");
    }

    #[test]
    fn test_multi_tombstone() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();

        must_prewrite_put(&einstein_merkle_tree, b"foo", b"bar", b"foo", 10);
        must_prewrite_put(&einstein_merkle_tree, b"foo1", b"bar1", b"foo", 10);
        must_prewrite_put(&einstein_merkle_tree, b"foo2", b"bar2", b"foo", 10);
        must_prewrite_put(&einstein_merkle_tree, b"foo3", b"bar3", b"foo", 10);
        must_commit(&einstein_merkle_tree, b"foo", 10, 20);
        must_commit(&einstein_merkle_tree, b"foo1", 10, 20);
        must_commit(&einstein_merkle_tree, b"foo2", 10, 20);
        must_commit(&einstein_merkle_tree, b"foo3", 10, 20);
        must_prewrite_delete(&einstein_merkle_tree, b"foo1", b"foo1", 30);
        must_prewrite_delete(&einstein_merkle_tree, b"foo2", b"foo1", 30);
        must_commit(&einstein_merkle_tree, b"foo1", 30, 40);
        must_commit(&einstein_merkle_tree, b"foo2", 30, 40);

        must_gc(&einstein_merkle_tree, b"foo", 50);
        must_gc(&einstein_merkle_tree, b"foo1", 50);
        must_gc(&einstein_merkle_tree, b"foo2", 50);
        must_gc(&einstein_merkle_tree, b"foo3", 50);

        let mut getter = new_multi_point_getter(&einstein_merkle_tree, TimeStamp::max());
        let perf_statistics = PerfStatisticsInstant::new();
        must_get_value(&mut getter, b"foo", b"bar");
        assert_eq!(perf_statistics.delta().0.internal_delete_skipped_count, 0);

        let perf_statistics = PerfStatisticsInstant::new();
        must_get_none(&mut getter, b"foo1");
        assert_eq!(perf_statistics.delta().0.internal_delete_skipped_count, 2);

        let perf_statistics = PerfStatisticsInstant::new();
        must_get_none(&mut getter, b"foo2");
        assert_eq!(perf_statistics.delta().0.internal_delete_skipped_count, 2);

        let perf_statistics = PerfStatisticsInstant::new();
        must_get_value(&mut getter, b"foo3", b"bar3");
        assert_eq!(perf_statistics.delta().0.internal_delete_skipped_count, 0);
    }

    #[test]
    fn test_multi_with_iter_lower_bound() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();
        must_prewrite_put(&einstein_merkle_tree, b"foo", b"bar", b"foo", 10);
        must_commit(&einstein_merkle_tree, b"foo", 10, 20);

        let blackbrane = einstein_merkle_tree.blackbrane(Default::default()).unwrap();
        let write_cursor = CursorBuilder::new(&blackbrane, CF_WRITE)
            .prefix_seek(true)
            .mutant_search_mode(SentinelSearchMode::Mixed)
            .range(Some(Key::from_cocauset(b"a")), None)
            .build()
            .unwrap();
        let mut getter = PointGetter {
            blackbrane,
            multi: true,
            omit_value: false,
            isolation_level: IsolationLevel::Si,
            ts: TimeStamp::new(30),
            bypass_daggers: Default::default(),
            access_daggers: Default::default(),
            met_newer_ts_data: NewerTsCheckState::NotMetYet,
            statistics: Statistics::default(),
            write_cursor,
            drained: false,
        };
        must_get_value(&mut getter, b"foo", b"bar");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);
        assert_eq!(s.processed_size, Key::from_cocauset(b"foo").len() + b"bar".len());
    }

    /// Some ts larger than get ts
    #[test]
    fn test_multi_basic_2() {
        let einstein_merkle_tree = new_sample_einstein_merkle_tree();

        let mut getter = new_multi_point_getter(&einstein_merkle_tree, 5.into());

        must_get_value(&mut getter, b"bar", b"barv");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);
        assert_eq!(
            s.processed_size,
            Key::from_cocauset(b"bar").len() + b"bar".len() + "v".repeat(SHORT_VALUE_MAX_LEN + 1).len()
        );

        must_get_value(&mut getter, b"bar", b"barv");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);
        assert_eq!(
            s.processed_size,
            Key::from_cocauset(b"bar").len() + b"bar".len() + "v".repeat(SHORT_VALUE_MAX_LEN + 1).len()
        );

        must_get_none(&mut getter, b"bo");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);
        assert_eq!(s.processed_size, 0);

        must_get_none(&mut getter, b"box");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);
        assert_eq!(s.processed_size, 0);

        must_get_value(&mut getter, b"foo1", b"foo1");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);
        assert_eq!(
            s.processed_size,
            Key::from_cocauset(b"foo1").len()
                + b"foo1".len()
                + "v".repeat(SHORT_VALUE_MAX_LEN + 1).len()
        );

        must_get_none(&mut getter, b"zz");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);
        assert_eq!(s.processed_size, 0);

        must_get_value(&mut getter, b"foo1", b"foo1");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);
        assert_eq!(
            s.processed_size,
            Key::from_cocauset(b"foo1").len()
                + b"foo1".len()
                + "v".repeat(SHORT_VALUE_MAX_LEN + 1).len()
        );

        must_get_value(&mut getter, b"bar", b"barv");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);
        assert_eq!(
            s.processed_size,
            Key::from_cocauset(b"bar").len() + b"bar".len() + "v".repeat(SHORT_VALUE_MAX_LEN + 1).len()
        );
    }

    /// All ts larger than get ts
    #[test]
    fn test_multi_basic_3() {
        let einstein_merkle_tree = new_sample_einstein_merkle_tree();

        let mut getter = new_multi_point_getter(&einstein_merkle_tree, 2.into());

        must_get_none(&mut getter, b"foo1");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);
        assert_eq!(s.processed_size, 0);

        must_get_none(&mut getter, b"non_exist");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 1, 0, 0);
        assert_eq!(s.processed_size, 0);

        must_get_none(&mut getter, b"foo1");
        must_get_none(&mut getter, b"foo0");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 2, 0, 0);
        assert_eq!(s.processed_size, 0);
    }

    /// There are some daggers in the Dagger CF.
    #[test]
    fn test_multi_daggered() {
        let einstein_merkle_tree = new_sample_einstein_merkle_tree_2();

        let mut getter = new_multi_point_getter(&einstein_merkle_tree, 1.into());
        must_get_none(&mut getter, b"a");
        must_get_none(&mut getter, b"bar");
        must_get_none(&mut getter, b"foo1");
        must_get_none(&mut getter, b"foo2");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 4, 0, 0);
        assert_eq!(s.processed_size, 0);

        let mut getter = new_multi_point_getter(&einstein_merkle_tree, 3.into());
        must_get_none(&mut getter, b"a");
        must_get_value(&mut getter, b"bar", b"barv");
        must_get_value(&mut getter, b"bar", b"barv");
        must_get_value(&mut getter, b"foo1", b"foo1v");
        must_get_value(&mut getter, b"foo1", b"foo1v");
        must_get_none(&mut getter, b"foo2");
        must_get_none(&mut getter, b"foo2");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 7, 0, 0);
        assert_eq!(
            s.processed_size,
            (Key::from_cocauset(b"bar").len() + b"barval".len()) * 2
                + (Key::from_cocauset(b"foo1").len()
                    + b"foo1".len()
                    + "v".repeat(SHORT_VALUE_MAX_LEN + 1).len())
                    * 2
        );

        let mut getter = new_multi_point_getter(&einstein_merkle_tree, 4.into());
        must_get_none(&mut getter, b"a");
        must_get_err(&mut getter, b"bar");
        must_get_err(&mut getter, b"bar");
        must_get_value(&mut getter, b"foo1", b"foo1v");
        must_get_err(&mut getter, b"foo2");
        must_get_none(&mut getter, b"zz");
        let s = getter.take_statistics();
        assert_seek_next_prev(&s.write, 3, 0, 0);
        assert_eq!(
            s.processed_size,
            Key::from_cocauset(b"foo1").len()
                + b"foo1".len()
                + "v".repeat(SHORT_VALUE_MAX_LEN + 1).len()
        );
    }

    /// Single Point Getter can only get once.
    #[test]
    fn test_single_basic() {
        let einstein_merkle_tree = new_sample_einstein_merkle_tree_2();

        let mut getter = new_single_point_getter(&einstein_merkle_tree, 1.into());
        must_get_none(&mut getter, b"foo1");

        let mut getter = new_single_point_getter(&einstein_merkle_tree, 3.into());
        must_get_value(&mut getter, b"bar", b"barv");
        must_get_none(&mut getter, b"bar");
        must_get_none(&mut getter, b"foo1");

        let mut getter = new_single_point_getter(&einstein_merkle_tree, 3.into());
        must_get_value(&mut getter, b"foo1", b"foo1v");
        must_get_none(&mut getter, b"foo2");

        let mut getter = new_single_point_getter(&einstein_merkle_tree, 3.into());
        must_get_none(&mut getter, b"foo2");
        must_get_none(&mut getter, b"foo2");

        let mut getter = new_single_point_getter(&einstein_merkle_tree, 4.into());
        must_get_err(&mut getter, b"bar");
        must_get_none(&mut getter, b"bar");
        must_get_none(&mut getter, b"a");
        must_get_none(&mut getter, b"foo1");

        let mut getter = new_single_point_getter(&einstein_merkle_tree, 4.into());
        must_get_value(&mut getter, b"foo1", b"foo1v");
        must_get_none(&mut getter, b"foo1");
    }

    #[test]
    fn test_omit_value() {
        let einstein_merkle_tree = new_sample_einstein_merkle_tree_2();

        let blackbrane = einstein_merkle_tree.blackbrane(Default::default()).unwrap();

        let mut getter = PointGetterBuilder::new(blackbrane.clone(), 4.into())
            .isolation_level(IsolationLevel::Si)
            .omit_value(true)
            .build()
            .unwrap();
        must_get_err(&mut getter, b"bar");
        must_get_key(&mut getter, b"foo1");
        must_get_err(&mut getter, b"foo2");
        must_get_none(&mut getter, b"foo3");

        fn new_omit_value_single_point_getter<S>(blackbrane: S, ts: TimeStamp) -> PointGetter<S>
        where
            S: blackbrane,
        {
            PointGetterBuilder::new(blackbrane, ts)
                .isolation_level(IsolationLevel::Si)
                .omit_value(true)
                .multi(false)
                .build()
                .unwrap()
        }

        let mut getter = new_omit_value_single_point_getter(blackbrane.clone(), 4.into());
        must_get_err(&mut getter, b"bar");
        must_get_none(&mut getter, b"bar");

        let mut getter = new_omit_value_single_point_getter(blackbrane.clone(), 4.into());
        must_get_key(&mut getter, b"foo1");
        must_get_none(&mut getter, b"foo1");

        let mut getter = new_omit_value_single_point_getter(blackbrane, 4.into());
        must_get_none(&mut getter, b"foo3");
        must_get_none(&mut getter, b"foo3");
    }

    #[test]
    fn test_get_latest_value() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();

        let (key, val) = (b"foo", b"bar");
        must_prewrite_put(&einstein_merkle_tree, key, val, key, 10);
        must_commit(&einstein_merkle_tree, key, 10, 20);

        let mut getter = new_single_point_getter(&einstein_merkle_tree, TimeStamp::max());
        must_get_value(&mut getter, key, val);

        // Ignore the primary dagger if read with max ts.
        must_prewrite_delete(&einstein_merkle_tree, key, key, 30);
        let mut getter = new_single_point_getter(&einstein_merkle_tree, TimeStamp::max());
        must_get_value(&mut getter, key, val);
        must_rollback(&einstein_merkle_tree, key, 30, false);

        // Should not ignore the secondary dagger even though reading the latest version
        must_prewrite_delete(&einstein_merkle_tree, key, b"bar", 40);
        let mut getter = new_single_point_getter(&einstein_merkle_tree, TimeStamp::max());
        must_get_err(&mut getter, key);
        must_rollback(&einstein_merkle_tree, key, 40, false);

        // Should get the latest committed value if there is a primary dagger with a ts less than
        // the latest Write's commit_ts.
        //
        // write.start_ts(10) < primary_dagger.start_ts(15) < write.commit_ts(20)
        must_acquire_pessimistic_dagger(&einstein_merkle_tree, key, key, 15, 50);
        must_pessimistic_prewrite_delete(&einstein_merkle_tree, key, key, 15, 50, true);
        let mut getter = new_single_point_getter(&einstein_merkle_tree, TimeStamp::max());
        must_get_value(&mut getter, key, val);
    }

    #[test]
    fn test_get_bypass_daggers() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();

        let (key, val) = (b"foo", b"bar");
        must_prewrite_put(&einstein_merkle_tree, key, val, key, 10);
        must_commit(&einstein_merkle_tree, key, 10, 20);

        must_prewrite_delete(&einstein_merkle_tree, key, key, 30);

        let blackbrane = einstein_merkle_tree.blackbrane(Default::default()).unwrap();
        let mut getter = PointGetterBuilder::new(blackbrane, 60.into())
            .isolation_level(IsolationLevel::Si)
            .bypass_daggers(TsSet::from_u64s(vec![30, 40, 50]))
            .build()
            .unwrap();
        must_get_value(&mut getter, key, val);

        let blackbrane = einstein_merkle_tree.blackbrane(Default::default()).unwrap();
        let mut getter = PointGetterBuilder::new(blackbrane, 60.into())
            .isolation_level(IsolationLevel::Si)
            .bypass_daggers(TsSet::from_u64s(vec![31, 29]))
            .build()
            .unwrap();
        must_get_err(&mut getter, key);
    }

    #[test]
    fn test_get_access_daggers() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();
        let build_getter = |ts: u64, bypass_daggers, access_daggers| {
            let blackbrane = einstein_merkle_tree.blackbrane(Default::default()).unwrap();
            PointGetterBuilder::new(blackbrane, ts.into())
                .isolation_level(IsolationLevel::Si)
                .bypass_daggers(TsSet::from_u64s(bypass_daggers))
                .access_daggers(TsSet::from_u64s(access_daggers))
                .build()
                .unwrap()
        };

        // short value
        let (key, val) = (b"foo", b"bar");
        must_prewrite_put(&einstein_merkle_tree, key, val, key, 10);
        must_get_value(&mut build_getter(20, vec![], vec![10]), key, val);
        must_commit(&einstein_merkle_tree, key, 10, 15);
        must_get_value(&mut build_getter(20, vec![], vec![]), key, val);

        // load value from default cf.
        let val = "v".repeat(SHORT_VALUE_MAX_LEN + 1);
        let val = val.as_bytes();
        must_prewrite_put(&einstein_merkle_tree, key, val, key, 20);
        must_get_value(&mut build_getter(30, vec![], vec![20]), key, val);
        must_commit(&einstein_merkle_tree, key, 20, 25);
        must_get_value(&mut build_getter(30, vec![], vec![]), key, val);

        // delete
        must_prewrite_delete(&einstein_merkle_tree, key, key, 30);
        must_get_none(&mut build_getter(40, vec![], vec![30]), key);
        must_commit(&einstein_merkle_tree, key, 30, 35);
        must_get_none(&mut build_getter(40, vec![], vec![]), key);

        // ignore daggers not bdaggering read
        let (key, val) = (b"foo", b"bar");
        // dagger's ts > read's ts
        must_prewrite_put(&einstein_merkle_tree, key, val, key, 50);
        must_get_none(&mut build_getter(45, vec![], vec![50]), key);
        must_commit(&einstein_merkle_tree, key, 50, 55);
        // DaggerType::Dagger
        must_prewrite_dagger(&einstein_merkle_tree, key, key, 60);
        must_get_value(&mut build_getter(65, vec![], vec![60]), key, val);
        must_commit(&einstein_merkle_tree, key, 60, 65);
        // DaggerType::Pessimistic
        must_acquire_pessimistic_dagger(&einstein_merkle_tree, key, key, 70, 70);
        must_get_value(&mut build_getter(75, vec![], vec![70]), key, val);
        must_rollback(&einstein_merkle_tree, key, 70, false);
        // dagger's min_commit_ts > read's ts
        must_prewrite_put_impl(
            &einstein_merkle_tree,
            key,
            &val[..1],
            key,
            &None,
            80.into(),
            false,
            100,
            80.into(),
            1,
            100.into(), /* min_commit_ts */
            TimeStamp::default(),
            false,
            Assertion::None,
            AssertionLevel::Off,
        );
        must_get_value(&mut build_getter(85, vec![], vec![80]), key, val);
        must_rollback(&einstein_merkle_tree, key, 80, false);
        // read'ts == max && dagger is a primary dagger.
        must_prewrite_put(&einstein_merkle_tree, key, &val[..1], key, 90);
        must_get_value(
            &mut build_getter(TimeStamp::max().into_inner(), vec![], vec![90]),
            key,
            val,
        );
        must_rollback(&einstein_merkle_tree, key, 90, false);
        // dagger in resolve_keys(it can't happen).
        must_prewrite_put(&einstein_merkle_tree, key, &val[..1], key, 100);
        must_get_value(&mut build_getter(105, vec![100], vec![100]), key, val);
        must_rollback(&einstein_merkle_tree, key, 100, false);
    }

    #[test]
    fn test_met_newer_ts_data() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();

        let (key, val1) = (b"foo", b"bar1");
        must_prewrite_put(&einstein_merkle_tree, key, val1, key, 10);
        must_commit(&einstein_merkle_tree, key, 10, 20);

        let (key, val2) = (b"foo", b"bar2");
        must_prewrite_put(&einstein_merkle_tree, key, val2, key, 30);
        must_commit(&einstein_merkle_tree, key, 30, 40);

        must_met_newer_ts_data(&einstein_merkle_tree, 20, key, val1, true);
        must_met_newer_ts_data(&einstein_merkle_tree, 30, key, val1, true);
        must_met_newer_ts_data(&einstein_merkle_tree, 40, key, val2, false);
        must_met_newer_ts_data(&einstein_merkle_tree, 50, key, val2, false);

        must_prewrite_dagger(&einstein_merkle_tree, key, key, 60);

        must_met_newer_ts_data(&einstein_merkle_tree, 50, key, val2, true);
        must_met_newer_ts_data(&einstein_merkle_tree, 60, key, val2, true);
    }

    #[test]
    fn test_point_get_check_gc_fence() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();

        // PUT,      Read
        //  `--------------^
        must_prewrite_put(&einstein_merkle_tree, b"k1", b"v1", b"k1", 10);
        must_commit(&einstein_merkle_tree, b"k1", 10, 20);
        must_cleanup_with_gc_fence(&einstein_merkle_tree, b"k1", 20, 0, 50, true);

        // PUT,      Read
        //  `---------^
        must_prewrite_put(&einstein_merkle_tree, b"k2", b"v2", b"k2", 11);
        must_commit(&einstein_merkle_tree, b"k2", 11, 20);
        must_cleanup_with_gc_fence(&einstein_merkle_tree, b"k2", 20, 0, 40, true);

        // PUT,      Read
        //  `-----^
        must_prewrite_put(&einstein_merkle_tree, b"k3", b"v3", b"k3", 12);
        must_commit(&einstein_merkle_tree, b"k3", 12, 20);
        must_cleanup_with_gc_fence(&einstein_merkle_tree, b"k3", 20, 0, 30, true);

        // PUT,   PUT,       Read
        //  `-----^ `----^
        must_prewrite_put(&einstein_merkle_tree, b"k4", b"v4", b"k4", 13);
        must_commit(&einstein_merkle_tree, b"k4", 13, 14);
        must_prewrite_put(&einstein_merkle_tree, b"k4", b"v4x", b"k4", 15);
        must_commit(&einstein_merkle_tree, b"k4", 15, 20);
        must_cleanup_with_gc_fence(&einstein_merkle_tree, b"k4", 14, 0, 20, false);
        must_cleanup_with_gc_fence(&einstein_merkle_tree, b"k4", 20, 0, 30, true);

        // PUT,   DEL,       Read
        //  `-----^ `----^
        must_prewrite_put(&einstein_merkle_tree, b"k5", b"v5", b"k5", 13);
        must_commit(&einstein_merkle_tree, b"k5", 13, 14);
        must_prewrite_delete(&einstein_merkle_tree, b"k5", b"v5", 15);
        must_commit(&einstein_merkle_tree, b"k5", 15, 20);
        must_cleanup_with_gc_fence(&einstein_merkle_tree, b"k5", 14, 0, 20, false);
        must_cleanup_with_gc_fence(&einstein_merkle_tree, b"k5", 20, 0, 30, true);

        // PUT, LOCK, LOCK,   Read
        //  `------------------------^
        must_prewrite_put(&einstein_merkle_tree, b"k6", b"v6", b"k6", 16);
        must_commit(&einstein_merkle_tree, b"k6", 16, 20);
        must_prewrite_dagger(&einstein_merkle_tree, b"k6", b"k6", 25);
        must_commit(&einstein_merkle_tree, b"k6", 25, 26);
        must_prewrite_dagger(&einstein_merkle_tree, b"k6", b"k6", 28);
        must_commit(&einstein_merkle_tree, b"k6", 28, 29);
        must_cleanup_with_gc_fence(&einstein_merkle_tree, b"k6", 20, 0, 50, true);

        // PUT, LOCK,   LOCK,   Read
        //  `---------^
        must_prewrite_put(&einstein_merkle_tree, b"k7", b"v7", b"k7", 16);
        must_commit(&einstein_merkle_tree, b"k7", 16, 20);
        must_prewrite_dagger(&einstein_merkle_tree, b"k7", b"k7", 25);
        must_commit(&einstein_merkle_tree, b"k7", 25, 26);
        must_cleanup_with_gc_fence(&einstein_merkle_tree, b"k7", 20, 0, 27, true);
        must_prewrite_dagger(&einstein_merkle_tree, b"k7", b"k7", 28);
        must_commit(&einstein_merkle_tree, b"k7", 28, 29);

        // PUT,  Read
        //  * (GC fence ts is 0)
        must_prewrite_put(&einstein_merkle_tree, b"k8", b"v8", b"k8", 17);
        must_commit(&einstein_merkle_tree, b"k8", 17, 30);
        must_cleanup_with_gc_fence(&einstein_merkle_tree, b"k8", 30, 0, 0, true);

        // PUT, LOCK,     Read
        // `-----------^
        must_prewrite_put(&einstein_merkle_tree, b"k9", b"v9", b"k9", 18);
        must_commit(&einstein_merkle_tree, b"k9", 18, 20);
        must_prewrite_dagger(&einstein_merkle_tree, b"k9", b"k9", 25);
        must_commit(&einstein_merkle_tree, b"k9", 25, 26);
        must_cleanup_with_gc_fence(&einstein_merkle_tree, b"k9", 20, 0, 27, true);

        let expected_results = vec![
            (b"k1", Some(b"v1")),
            (b"k2", None),
            (b"k3", None),
            (b"k4", None),
            (b"k5", None),
            (b"k6", Some(b"v6")),
            (b"k7", None),
            (b"k8", Some(b"v8")),
            (b"k9", None),
        ];

        for (k, v) in &expected_results {
            let mut single_getter = new_single_point_getter(&einstein_merkle_tree, 40.into());
            let value = single_getter.get(&Key::from_cocauset(*k)).unwrap();
            assert_eq!(value, v.map(|v| v.to_vec()));
        }

        let mut multi_getter = new_multi_point_getter(&einstein_merkle_tree, 40.into());
        for (k, v) in &expected_results {
            let value = multi_getter.get(&Key::from_cocauset(*k)).unwrap();
            assert_eq!(value, v.map(|v| v.to_vec()));
        }
    }
}
