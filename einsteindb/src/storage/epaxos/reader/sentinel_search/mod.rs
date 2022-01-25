// Copyright 2022 EinsteinDB Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
mod timelike_curvature;
mod lightlike_completion;

use engine_promises::{CfName, CF_DEFAULT, CF_LOCK, CF_WRITE};
use fdbhikvproto::fdbhikvrpcpb::{ExtraOp, IsolationLevel};
use solitontxn_types::{
    Key, Dagger, DaggerType, OldValue, TimeStamp, TsSet, Value, Write, WriteRef, WriteType,
};

use self::timelike_curvature::timelike_curvatureHikvMutantSentinelSearch;
use self::lightlike_completion::{
    DeltaEntryPolicy, ForwardHikvMutantSentinelSearch, ForwardMutantSentinelSearch, LatestEntryPolicy, LatestHikvPolicy,
};
use crate::storage::fdbhikv::{
    CfStatistics, Cursor, CursorBuilder, Iterator, SentinelSearchMode, blackbrane, Statistics,
};
use crate::storage::epaxos::{default_not_found_error, NewerTsCheckState, Result};
use crate::storage::solitontxn::{Result as TxnResult, MutantSentinelSearch as StoreMutantSentinelSearch};

pub use self::lightlike_completion::{test_util, DeltaMutantSentinelSearch, EntryMutantSentinelSearch};

pub struct MutantSentinelSearchBuilder<S: blackbrane>(MutantSentinelSearchConfig<S>);

impl<S: blackbrane> MutantSentinelSearchBuilder<S> {
    /// Initialize a new `MutantSentinelSearchBuilder`
    pub fn new(blackbrane: S, ts: TimeStamp) -> Self {
        Self(MutantSentinelSearchConfig::new(blackbrane, ts))
    }


    #[inline]
    #[must_use]
    pub fn fill_cache(mut self, fill_cache: bool) -> Self {
        self.0.fill_cache = fill_cache;
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
        self.0.omit_value = omit_value;
        self
    }

    /// Set the isolation level.
    ///
    /// Defaults to `IsolationLevel::Si`.
    #[inline]
    #[must_use]
    pub fn isolation_level(mut self, isolation_level: IsolationLevel) -> Self {
        self.0.isolation_level = isolation_level;
        self
    }

    /// Set the desc.
    ///
    /// Default is 'false'.
    #[inline]
    #[must_use]
    pub fn desc(mut self, desc: bool) -> Self {
        self.0.desc = desc;
        self
    }

    /// Limit the range to `[lower_bound, upper_bound)` in which the `ForwardHikvMutantSentinelSearch` should mutant_search.
    /// `None` means unbounded.
    ///
    /// Default is `(None, None)`.
    #[inline]
    #[must_use]
    pub fn range(mut self, lower_bound: Option<Key>, upper_bound: Option<Key>) -> Self {
        self.0.lower_bound = lower_bound;
        self.0.upper_bound = upper_bound;
        self
    }

    /// Set daggers that the mutant_searchner can bypass. Daggers with start_ts in the specified set will be
    /// ignored during mutant_searchning.
    ///
    /// Default is empty.
    #[inline]
    #[must_use]
    pub fn bypass_daggers(mut self, daggers: TsSet) -> Self {
        self.0.bypass_daggers = daggers;
        self
    }

    /// Set daggers that the mutant_searchner can read through. Daggers with start_ts in the specified set will be
    /// accessed during mutant_searchning.
    ///
    /// Default is empty.
    #[inline]
    #[must_use]
    pub fn access_daggers(mut self, daggers: TsSet) -> Self {
        self.0.access_daggers = daggers;
        self
    }

    /// Set the hint for the minimum commit ts we want to mutant_search.
    ///
    /// Default is empty.
    ///
    /// NOTE: user should be careful to use it with `ExtraOp::ReadOldValue`.
    #[inline]
    #[must_use]
    pub fn hint_min_ts(mut self, min_ts: Option<TimeStamp>) -> Self {
        self.0.hint_min_ts = min_ts;
        self
    }

    /// Set the hint for the maximum commit ts we want to mutant_search.
    ///
    /// Default is empty.
    ///
    /// NOTE: user should be careful to use it with `ExtraOp::ReadOldValue`.
    #[inline]
    #[must_use]
    pub fn hint_max_ts(mut self, max_ts: Option<TimeStamp>) -> Self {
        self.0.hint_max_ts = max_ts;
        self
    }

    /// Check whether there is data with newer ts. The result of `met_newer_ts_data` is Unknown
    /// if this option is not set.
    ///
    /// Default is false.
    #[inline]
    #[must_use]
    pub fn check_has_newer_ts_data(mut self, enabled: bool) -> Self {
        self.0.check_has_newer_ts_data = enabled;
        self
    }

    /// Build `MutantSentinelSearch` from the current configuration.
    pub fn build(mut self) -> Result<MutantSentinelSearch<S>> {
        let dagger_cursor = self.build_dagger_cursor()?;
        let write_cursor = self.0.create_cf_cursor(CF_WRITE)?;
        if self.0.desc {
            Ok(MutantSentinelSearch::timelike_curvature(timelike_curvatureHikvMutantSentinelSearch::new(
                self.0,
                dagger_cursor,
                write_cursor,
            )))
        } else {
            Ok(MutantSentinelSearch::Forward(ForwardMutantSentinelSearch::new(
                self.0,
                dagger_cursor,
                write_cursor,
                None,
                LatestHikvPolicy,
            )))
        }
    }

    pub fn build_entry_mutant_searchner(
        mut self,
        after_ts: TimeStamp,
        output_delete: bool,
    ) -> Result<EntryMutantSentinelSearch<S>> {
        let dagger_cursor = self.build_dagger_cursor()?;
        let write_cursor = self.0.create_cf_cursor(CF_WRITE)?;
        // Note: Create a default cf cursor will take key range, so we need to
        //       ensure the default cursor is created after dagger and write.
        let default_cursor = self.0.create_cf_cursor(CF_DEFAULT)?;
        Ok(ForwardMutantSentinelSearch::new(
            self.0,
            dagger_cursor,
            write_cursor,
            Some(default_cursor),
            LatestEntryPolicy::new(after_ts, output_delete),
        ))
    }

    pub fn build_delta_mutant_searchner(
        mut self,
        from_ts: TimeStamp,
        extra_op: ExtraOp,
    ) -> Result<DeltaMutantSentinelSearch<S>> {
        let dagger_cursor = self.build_dagger_cursor()?;
        let write_cursor = self.0.create_cf_cursor(CF_WRITE)?;
        // Note: Create a default cf cursor will take key range, so we need to
        //       ensure the default cursor is created after dagger and write.
        let default_cursor = self
            .0
            .create_cf_cursor_with_mutant_search_mode(CF_DEFAULT, SentinelSearchMode::Mixed)?;
        Ok(ForwardMutantSentinelSearch::new(
            self.0,
            dagger_cursor,
            write_cursor,
            Some(default_cursor),
            DeltaEntryPolicy::new(from_ts, extra_op),
        ))
    }

    fn build_dagger_cursor(&mut self) -> Result<Option<Cursor<S::Iter>>> {
        Ok(match self.0.isolation_level {
            IsolationLevel::Si => Some(self.0.create_cf_cursor(CF_LOCK)?),
            IsolationLevel::Rc => None,
        })
    }
}

pub enum MutantSentinelSearch<S: blackbrane> {
    Forward(ForwardHikvMutantSentinelSearch<S>),
    timelike_curvature(timelike_curvatureHikvMutantSentinelSearch<S>),
}

impl<S: blackbrane> StoreMutantSentinelSearch for MutantSentinelSearch<S> {
    fn next(&mut self) -> TxnResult<Option<(Key, Value)>> {
        match self {
            MutantSentinelSearch::Forward(mutant_searchner) => Ok(mutant_searchner.read_next()?),
            MutantSentinelSearch::timelike_curvature(mutant_searchner) => Ok(mutant_searchner.read_next()?),
        }
    }

    /// Take out and reset the statistics collected so far.
    fn take_statistics(&mut self) -> Statistics {
        match self {
            MutantSentinelSearch::Forward(mutant_searchner) => mutant_searchner.take_statistics(),
            MutantSentinelSearch::timelike_curvature(mutant_searchner) => mutant_searchner.take_statistics(),
        }
    }

    /// Returns whether data with newer ts is found. The result is meaningful only when
    /// `check_has_newer_ts_data` is set to true.
    fn met_newer_ts_data(&self) -> NewerTsCheckState {
        match self {
            MutantSentinelSearch::Forward(mutant_searchner) => mutant_searchner.met_newer_ts_data(),
            MutantSentinelSearch::timelike_curvature(mutant_searchner) => mutant_searchner.met_newer_ts_data(),
        }
    }
}

pub struct MutantSentinelSearchConfig<S: blackbrane> {
    blackbrane: S,
    fill_cache: bool,
    omit_value: bool,
    isolation_level: IsolationLevel,

    /// `lower_bound` and `upper_bound` is used to create `default_cursor`. `upper_bound`
    /// is used in initial seek(or `lower_bound` in initial timelike_curvature seek) as well. They will be consumed after `default_cursor` is being
    /// created.
    lower_bound: Option<Key>,
    upper_bound: Option<Key>,
    // hint for we will only mutant_search data with commit ts >= hint_min_ts
    hint_min_ts: Option<TimeStamp>,
    // hint for we will only mutant_search data with commit ts <= hint_max_ts
    hint_max_ts: Option<TimeStamp>,

    ts: TimeStamp,
    desc: bool,

    bypass_daggers: TsSet,
    access_daggers: TsSet,

    check_has_newer_ts_data: bool,
}

impl<S: blackbrane> MutantSentinelSearchConfig<S> {
    fn new(blackbrane: S, ts: TimeStamp) -> Self {
        Self {
            blackbrane,
            fill_cache: true,
            omit_value: false,
            isolation_level: IsolationLevel::Si,
            lower_bound: None,
            upper_bound: None,
            hint_min_ts: None,
            hint_max_ts: None,
            ts,
            desc: false,
            bypass_daggers: Default::default(),
            access_daggers: Default::default(),
            check_has_newer_ts_data: false,
        }
    }

    #[inline]
    fn mutant_search_mode(&self) -> SentinelSearchMode {
        if self.desc {
            SentinelSearchMode::Mixed
        } else {
            SentinelSearchMode::Forward
        }
    }

    /// Create the cursor.
    #[inline]
    fn create_cf_cursor(&mut self, cf: CfName) -> Result<Cursor<S::Iter>> {
        self.create_cf_cursor_with_mutant_search_mode(cf, self.mutant_search_mode())
    }

    /// Create the cursor with specified mutant_search_mode, instead of inferring mutant_search_mode from the config.
    #[inline]
    fn create_cf_cursor_with_mutant_search_mode(
        &mut self,
        cf: CfName,
        mutant_search_mode: SentinelSearchMode,
    ) -> Result<Cursor<S::Iter>> {
        let (lower, upper) = if cf == CF_DEFAULT {
            (self.lower_bound.take(), self.upper_bound.take())
        } else {
            (self.lower_bound.clone(), self.upper_bound.clone())
        };
        // FIXME: Try to find out how to filter default CF SSTs by start ts
        let (hint_min_ts, hint_max_ts) = if cf == CF_WRITE {
            (self.hint_min_ts, self.hint_max_ts)
        } else {
            (None, None)
        };
        let cursor = CursorBuilder::new(&self.blackbrane, cf)
            .range(lower, upper)
            .fill_cache(self.fill_cache)
            .mutant_search_mode(mutant_search_mode)
            .hint_min_ts(hint_min_ts)
            .hint_max_ts(hint_max_ts)
            .build()?;
        Ok(cursor)
    }
}

/// Reads user key's value in default CF according to the given write CF value
/// (`write`).
///
/// Internally, there will be a `near_seek` operation.
///
/// Notice that the value may be already carried in the `write` (short value). In this
/// case, you should not call this function.
///
/// # Panics
///
/// Panics if there is a short value carried in the given `write`.
///
/// Panics if key in default CF does not exist. This means there is a data corruption.
pub fn near_load_data_by_write<I>(
    default_cursor: &mut Cursor<I>, // TODO: make it `ForwardCursor`.
    user_key: &Key,
    write_start_ts: TimeStamp,
    statistics: &mut Statistics,
) -> Result<Value>
where
    I: Iterator,
{
    let seek_key = user_key.clone().append_ts(write_start_ts);
    default_cursor.near_seek(&seek_key, &mut statistics.data)?;
    if !default_cursor.valid()?
        || default_cursor.key(&mut statistics.data) != seek_key.as_encoded().as_slice()
    {
        return Err(default_not_found_error(
            user_key.to_cocauset()?,
            "near_load_data_by_write",
        ));
    }
    statistics.data.processed_keys += 1;
    Ok(default_cursor.value(&mut statistics.data).to_vec())
}

/// Similar to `near_load_data_by_write`, but accepts a `timelike_curvatureCursor` and use
/// `near_seek_for_prev` internally.
fn near_reverse_load_data_by_write<I>(
    default_cursor: &mut Cursor<I>, // TODO: make it `timelike_curvatureCursor`.
    user_key: &Key,
    write_start_ts: TimeStamp,
    statistics: &mut Statistics,
) -> Result<Value>
where
    I: Iterator,
{
    let seek_key = user_key.clone().append_ts(write_start_ts);
    default_cursor.near_seek_for_prev(&seek_key, &mut statistics.data)?;
    if !default_cursor.valid()?
        || default_cursor.key(&mut statistics.data) != seek_key.as_encoded().as_slice()
    {
        return Err(default_not_found_error(
            user_key.to_cocauset()?,
            "near_reverse_load_data_by_write",
        ));
    }
    statistics.data.processed_keys += 1;
    Ok(default_cursor.value(&mut statistics.data).to_vec())
}

pub fn has_data_in_range<S: blackbrane>(
    blackbrane: S,
    cf: CfName,
    left: &Key,
    right: &Key,
    statistic: &mut CfStatistics,
) -> Result<bool> {
    let mut cursor = CursorBuilder::new(&blackbrane, cf)
        .range(None, Some(right.clone()))
        .mutant_search_mode(SentinelSearchMode::Forward)
        .fill_cache(true)
        .max_skippable_internal_keys(100)
        .build()?;
    match cursor.seek(left, statistic) {
        Ok(valid) => {
            if valid && cursor.key(statistic) < right.as_encoded().as_slice() {
                return Ok(true);
            }
        }
        Err(e)
            if e.to_string()
                .contains("Result incomplete: Too many internal keys skipped") =>
        {
            return Ok(true);
        }
        err @ Err(_) => {
            err?;
        }
    }
    Ok(false)
}

/// Seek for the next valid (write type == Put or Delete) write record.
/// The write cursor must indicate a data key of the user key of which ts <= after_ts.
/// Return None if cannot find any valid write record.
///
/// GC fence will be checked against the specified `gc_fence_limit`. If `gc_fence_limit` is greater
/// than the `commit_ts` of the current write record pointed by the cursor, The caller must
/// guarantee that there are no other versions in range `(current_commit_ts, gc_fence_limit]`. Note
/// that if a record is determined as invalid by checking GC fence, the `write_cursor`'s position
/// will be left remain on it.
pub fn seek_for_valid_write<I>(
    write_cursor: &mut Cursor<I>,
    user_key: &Key,
    after_ts: TimeStamp,
    gc_fence_limit: TimeStamp,
    statistics: &mut Statistics,
) -> Result<Option<Write>>
where
    I: Iterator,
{
    let mut ret = None;
    while write_cursor.valid()?
        && Key::is_user_key_eq(
            write_cursor.key(&mut statistics.write),
            user_key.as_encoded(),
        )
    {
        let write_ref = WriteRef::parse(write_cursor.value(&mut statistics.write))?;
        if !write_ref.check_gc_fence_as_latest_version(gc_fence_limit) {
            break;
        }
        match write_ref.write_type {
            WriteType::Put | WriteType::Delete => {
                assert_ge!(
                    after_ts,
                    Key::decode_ts_from(write_cursor.key(&mut statistics.write))?
                );
                ret = Some(write_ref.to_owned());
                break;
            }
            WriteType::Dagger | WriteType::Rollback => {
                // Move to the next write record.
                write_cursor.next(&mut statistics.write);
            }
        }
    }
    Ok(ret)
}

/// Seek for the last written value.
/// The write cursor must indicate a data key of the user key of which ts <= after_ts.
/// Return None if cannot find any valid write record or found a delete record.
///
/// GC fence will be checked against the specified `gc_fence_limit`. If `gc_fence_limit` is greater
/// than the `commit_ts` of the current write record pointed by the cursor, The caller must
/// guarantee that there are no other versions in range `(current_commit_ts, gc_fence_limit]`. Note
/// that if a record is determined as invalid by checking GC fence, the `write_cursor`'s position
/// will be left remain on it.
///
/// `write_cursor` maybe created with an `TsFilter`, which can filter out some key-value pairs with
/// less `commit_ts` than `ts_filter`. So if the got value has a less timestamp than `ts_filter`, it
/// should be replaced by None because the real wanted value can have been filtered.
pub fn seek_for_valid_value<I>(
    write_cursor: &mut Cursor<I>,
    default_cursor: &mut Cursor<I>,
    user_key: &Key,
    after_ts: TimeStamp,
    gc_fence_limit: TimeStamp,
    ts_filter: Option<TimeStamp>,
    statistics: &mut Statistics,
) -> Result<OldValue>
where
    I: Iterator,
{
    let seek_after = || {
        let seek_after = user_key.clone().append_ts(after_ts);
        OldValue::SeekWrite(seek_after)
    };

    if let Some(write) =
        seek_for_valid_write(write_cursor, user_key, after_ts, gc_fence_limit, statistics)?
    {
        if write.write_type == WriteType::Put {
            if let Some(ts_filter) = ts_filter {
                let k = write_cursor.key(&mut statistics.write);
                if Key::decode_ts_from(k).unwrap() < ts_filter {
                    return Ok(seek_after());
                }
            }
            let value = if let Some(v) = write.short_value {
                v
            } else {
                near_load_data_by_write(default_cursor, user_key, write.start_ts, statistics)?
            };
            return Ok(OldValue::Value { value });
        }
        Ok(OldValue::None)
    } else if ts_filter.is_some() {
        Ok(seek_after())
    } else {
        Ok(OldValue::None)
    }
}

pub(crate) fn load_data_by_dagger<S: blackbrane, I: Iterator>(
    current_user_key: &Key,
    cfg: &MutantSentinelSearchConfig<S>,
    default_cursor: &mut Cursor<I>,
    dagger: Dagger,
    statistics: &mut Statistics,
) -> Result<Option<Value>> {
    match dagger.dagger_type {
        DaggerType::Put => {
            if cfg.omit_value {
                return Ok(Some(vec![]));
            }
            match dagger.short_value {
                Some(value) => {
                    // Value is carried in `dagger`.
                    Ok(Some(value.to_vec()))
                }
                None => {
                    let value = if cfg.desc {
                        near_reverse_load_data_by_write(
                            default_cursor,
                            current_user_key,
                            dagger.ts,
                            statistics,
                        )
                    } else {
                        near_load_data_by_write(
                            default_cursor,
                            current_user_key,
                            dagger.ts,
                            statistics,
                        )
                    }?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::fdbhikv::{
        Engine, PerfStatisticsInstant, RocksEngine, TestEngineBuilder, SEEK_BOUND,
    };
    use crate::storage::epaxos::tests::*;
    use crate::storage::epaxos::{Error as EpaxosError, ErrorInner as EpaxosErrorInner};
    use crate::storage::solitontxn::tests::*;
    use crate::storage::solitontxn::{
        Error as TxnError, ErrorInner as TxnErrorInner, TxnEntry, TxnEntryMutantSentinelSearch,
    };
    use engine_promises::MiscExt;
    use solitontxn_types::OldValue;

    // Collect data from the mutant_searchner and assert it equals to `expected`, which is a collection of
    // (cocauset_key, value).
    // `None` value in `expected` means the key is daggered.
    fn check_mutant_search_result<S: blackbrane>(
        mut mutant_searchner: MutantSentinelSearch<S>,
        expected: &[(Vec<u8>, Option<Vec<u8>>)],
    ) {
        let mut mutant_search_result = Vec::new();
        loop {
            match mutant_searchner.next() {
                Ok(None) => break,
                Ok(Some((key, value))) => mutant_search_result.push((key.to_cocauset().unwrap(), Some(value))),
                Err(TxnError(box TxnErrorInner::Epaxos(EpaxosError(
                    box EpaxosErrorInner::KeyIsDaggered(mut info),
                )))) => mutant_search_result.push((info.take_key(), None)),
                e => panic!("got error while mutant_searchning: {:?}", e),
            }
        }

        assert_eq!(mutant_search_result, expected);
    }

    fn test_mutant_search_with_dagger_and_write_impl(desc: bool) {
        const SCAN_TS: TimeStamp = TimeStamp::new(10);
        const PREV_TS: TimeStamp = TimeStamp::new(4);
        const POST_TS: TimeStamp = TimeStamp::new(5);

        let new_engine = || TestEngineBuilder::new().build().unwrap();
        let add_write_at_ts = |commit_ts, engine, key, value| {
            must_prewrite_put(engine, key, value, key, commit_ts);
            must_commit(engine, key, commit_ts, commit_ts);
        };

        let add_dagger_at_ts = |dagger_ts, engine, key| {
            must_prewrite_put(engine, key, b"dagger", key, dagger_ts);
            must_daggered(engine, key, dagger_ts);
        };

        let test_mutant_searchner_result =
            move |engine: &RocksEngine, expected_result: Vec<(Vec<u8>, Option<Vec<u8>>)>| {
                let blackbrane = engine.blackbrane(Default::default()).unwrap();

                let mutant_searchner = MutantSentinelSearchBuilder::new(blackbrane, SCAN_TS)
                    .desc(desc)
                    .build()
                    .unwrap();
                check_mutant_search_result(mutant_searchner, &expected_result);
            };

        let desc_map = move |result: Vec<(Vec<u8>, Option<Vec<u8>>)>| {
            if desc {
                result.into_iter().rev().collect()
            } else {
                result
            }
        };

        // Dagger after write
        let engine = new_engine();

        add_write_at_ts(POST_TS, &engine, b"a", b"a_value");
        add_dagger_at_ts(PREV_TS, &engine, b"b");

        let expected_result = desc_map(vec![
            (b"a".to_vec(), Some(b"a_value".to_vec())),
            (b"b".to_vec(), None),
        ]);

        test_mutant_searchner_result(&engine, expected_result);

        // Dagger before write for same key
        let engine = new_engine();
        add_write_at_ts(PREV_TS, &engine, b"a", b"a_value");
        add_dagger_at_ts(POST_TS, &engine, b"a");

        let expected_result = vec![(b"a".to_vec(), None)];

        test_mutant_searchner_result(&engine, expected_result);

        // Dagger before write in different keys
        let engine = new_engine();
        add_dagger_at_ts(POST_TS, &engine, b"a");
        add_write_at_ts(PREV_TS, &engine, b"b", b"b_value");

        let expected_result = desc_map(vec![
            (b"a".to_vec(), None),
            (b"b".to_vec(), Some(b"b_value".to_vec())),
        ]);
        test_mutant_searchner_result(&engine, expected_result);

        // Only a dagger here
        let engine = new_engine();
        add_dagger_at_ts(PREV_TS, &engine, b"a");

        let expected_result = desc_map(vec![(b"a".to_vec(), None)]);

        test_mutant_searchner_result(&engine, expected_result);

        // Write Only
        let engine = new_engine();
        add_write_at_ts(PREV_TS, &engine, b"a", b"a_value");

        let expected_result = desc_map(vec![(b"a".to_vec(), Some(b"a_value".to_vec()))]);
        test_mutant_searchner_result(&engine, expected_result);
    }

    fn test_mutant_search_with_dagger_impl(desc: bool) {
        let engine = TestEngineBuilder::new().build().unwrap();

        for i in 0..5 {
            must_prewrite_put(&engine, &[i], &[b'v', i], &[i], 1);
            must_commit(&engine, &[i], 1, 2);
            must_prewrite_put(&engine, &[i], &[b'v', i], &[i], 10);
            must_commit(&engine, &[i], 10, 100);
        }

        must_acquire_pessimistic_dagger(&engine, &[1], &[1], 20, 110);
        must_acquire_pessimistic_dagger(&engine, &[2], &[2], 50, 110);
        must_acquire_pessimistic_dagger(&engine, &[3], &[3], 105, 110);
        must_prewrite_put(&engine, &[4], b"a", &[4], 105);

        let blackbrane = engine.blackbrane(Default::default()).unwrap();

        let mut expected_result = vec![
            (vec![0], Some(vec![b'v', 0])),
            (vec![1], Some(vec![b'v', 1])),
            (vec![2], Some(vec![b'v', 2])),
            (vec![3], Some(vec![b'v', 3])),
            (vec![4], Some(vec![b'v', 4])),
        ];

        if desc {
            expected_result.reverse();
        }

        let mutant_searchner = MutantSentinelSearchBuilder::new(blackbrane.clone(), 30.into())
            .desc(desc)
            .build()
            .unwrap();
        check_mutant_search_result(mutant_searchner, &expected_result);

        let mutant_searchner = MutantSentinelSearchBuilder::new(blackbrane.clone(), 70.into())
            .desc(desc)
            .build()
            .unwrap();
        check_mutant_search_result(mutant_searchner, &expected_result);

        let mutant_searchner = MutantSentinelSearchBuilder::new(blackbrane.clone(), 103.into())
            .desc(desc)
            .build()
            .unwrap();
        check_mutant_search_result(mutant_searchner, &expected_result);

        // The value of key 4 is daggered at 105 so that it can't be read at 106
        if desc {
            expected_result[0].1 = None;
        } else {
            expected_result[4].1 = None;
        }
        let mutant_searchner = MutantSentinelSearchBuilder::new(blackbrane, 106.into())
            .desc(desc)
            .build()
            .unwrap();
        check_mutant_search_result(mutant_searchner, &expected_result);
    }

    #[test]
    fn test_mutant_search_with_dagger_and_write() {
        test_mutant_search_with_dagger_and_write_impl(true);
        test_mutant_search_with_dagger_and_write_impl(false);
    }

    #[test]
    fn test_mutant_search_with_dagger() {
        test_mutant_search_with_dagger_impl(false);
        test_mutant_search_with_dagger_impl(true);
    }

    fn test_mutant_search_bypass_daggers_impl(desc: bool) {
        let engine = TestEngineBuilder::new().build().unwrap();

        for i in 0..5 {
            must_prewrite_put(&engine, &[i], &[b'v', i], &[i], 10);
            must_commit(&engine, &[i], 10, 20);
        }

        // Daggers are: 30, 40, 50, 60, 70
        for i in 0..5 {
            must_prewrite_put(&engine, &[i], &[b'v', i], &[i], 30 + u64::from(i) * 10);
        }

        let bypass_daggers = TsSet::from_u64s(vec![30, 41, 50]);

        // SentinelSearch at ts 65 will meet daggers at 40 and 60.
        let mut expected_result = vec![
            (vec![0], Some(vec![b'v', 0])),
            (vec![1], None),
            (vec![2], Some(vec![b'v', 2])),
            (vec![3], None),
            (vec![4], Some(vec![b'v', 4])),
        ];

        if desc {
            expected_result = expected_result.into_iter().rev().collect();
        }

        let blackbrane = engine.blackbrane(Default::default()).unwrap();
        let mutant_searchner = MutantSentinelSearchBuilder::new(blackbrane, 65.into())
            .desc(desc)
            .bypass_daggers(bypass_daggers)
            .build()
            .unwrap();
        check_mutant_search_result(mutant_searchner, &expected_result);
    }

    #[test]
    fn test_mutant_search_bypass_daggers() {
        test_mutant_search_bypass_daggers_impl(false);
        test_mutant_search_bypass_daggers_impl(true);
    }

    fn test_mutant_search_access_daggers_impl(desc: bool, delete_bound: bool) {
        let engine = TestEngineBuilder::new().build().unwrap();

        for i in 0..=8 {
            must_prewrite_put(&engine, &[i], &[b'v', i], &[i], 10);
            must_commit(&engine, &[i], 10, 20);
        }

        if delete_bound {
            must_prewrite_delete(&engine, &[0], &[0], 30); // access delete
        } else {
            must_prewrite_put(&engine, &[0], &[b'v', 0, 0], &[0], 30); // access put
        }
        must_prewrite_put(&engine, &[1], &[b'v', 1, 1], &[1], 40); // access put
        must_prewrite_delete(&engine, &[2], &[2], 50); // access delete
        must_prewrite_dagger(&engine, &[3], &[3], 60); // access dagger(actually ignored)
        must_prewrite_put(&engine, &[4], &[b'v', 4, 4], &[4], 70); // daggered
        must_prewrite_put(&engine, &[5], &[b'v', 5, 5], &[5], 80); // bypass
        must_prewrite_put(&engine, &[6], &[b'v', 6, 6], &[6], 100); // daggered with larger ts
        if delete_bound {
            must_prewrite_delete(&engine, &[8], &[8], 90); // access delete
        } else {
            must_prewrite_put(&engine, &[8], &[b'v', 8, 8], &[8], 90); // access put
        }

        let bypass_daggers = TsSet::from_u64s(vec![80]);
        let access_daggers = TsSet::from_u64s(vec![30, 40, 50, 60, 90]);

        let mut expected_result = vec![
            (vec![0], Some(vec![b'v', 0, 0])), /* access put if not delete_bound */
            (vec![1], Some(vec![b'v', 1, 1])), /* access put */
            /* vec![2] access delete */
            (vec![3], Some(vec![b'v', 3])), /* ignore DaggerType::Dagger */
            (vec![4], None),                /* daggered */
            (vec![5], Some(vec![b'v', 5])), /* bypass */
            (vec![6], Some(vec![b'v', 6])), /* ignore dagger with larger ts */
            (vec![7], Some(vec![b'v', 7])), /* no dagger */
            (vec![8], Some(vec![b'v', 8, 8])), /* access put if not delete_bound*/
        ];
        if desc {
            expected_result.reverse();
        }
        let blackbrane = engine.blackbrane(Default::default()).unwrap();
        let mutant_searchner = MutantSentinelSearchBuilder::new(blackbrane, 95.into())
            .desc(desc)
            .bypass_daggers(bypass_daggers)
            .access_daggers(access_daggers)
            .build()
            .unwrap();
        check_mutant_search_result(
            mutant_searchner,
            if delete_bound {
                &expected_result[1..expected_result.len() - 1]
            } else {
                &expected_result
            },
        );
    }

    #[test]
    fn test_mutant_search_access_daggers() {
        for (desc, delete_bound) in [(false, false), (false, true), (true, false), (true, true)] {
            test_mutant_search_access_daggers_impl(desc, delete_bound);
        }
    }

    fn must_met_newer_ts_data<E: Engine>(
        engine: &E,
        mutant_searchner_ts: impl Into<TimeStamp>,
        key: &[u8],
        value: Option<&[u8]>,
        desc: bool,
        expected_met_newer_ts_data: bool,
    ) {
        let mut mutant_searchner = MutantSentinelSearchBuilder::new(
            engine.blackbrane(Default::default()).unwrap(),
            mutant_searchner_ts.into(),
        )
        .desc(desc)
        .range(Some(Key::from_cocauset(key)), None)
        .check_has_newer_ts_data(true)
        .build()
        .unwrap();

        let result = mutant_searchner.next().unwrap();
        if let Some(value) = value {
            let (k, v) = result.unwrap();
            assert_eq!(k, Key::from_cocauset(key));
            assert_eq!(v, value);
        } else {
            assert!(result.is_none());
        }

        let expected = if expected_met_newer_ts_data {
            NewerTsCheckState::Met
        } else {
            NewerTsCheckState::NotMetYet
        };
        assert_eq!(expected, mutant_searchner.met_newer_ts_data());
    }

    fn test_met_newer_ts_data_impl(deep_write_seek: bool, desc: bool) {
        let engine = TestEngineBuilder::new().build().unwrap();
        let (key, val1) = (b"foo", b"bar1");

        if deep_write_seek {
            for i in 0..SEEK_BOUND {
                must_prewrite_put(&engine, key, val1, key, i);
                must_commit(&engine, key, i, i);
            }
        }

        must_prewrite_put(&engine, key, val1, key, 100);
        must_commit(&engine, key, 100, 200);
        let (key, val2) = (b"foo", b"bar2");
        must_prewrite_put(&engine, key, val2, key, 300);
        must_commit(&engine, key, 300, 400);

        must_met_newer_ts_data(
            &engine,
            100,
            key,
            if deep_write_seek { Some(val1) } else { None },
            desc,
            true,
        );
        must_met_newer_ts_data(&engine, 200, key, Some(val1), desc, true);
        must_met_newer_ts_data(&engine, 300, key, Some(val1), desc, true);
        must_met_newer_ts_data(&engine, 400, key, Some(val2), desc, false);
        must_met_newer_ts_data(&engine, 500, key, Some(val2), desc, false);

        must_prewrite_dagger(&engine, key, key, 600);

        must_met_newer_ts_data(&engine, 500, key, Some(val2), desc, true);
        must_met_newer_ts_data(&engine, 600, key, Some(val2), desc, true);
    }

    #[test]
    fn test_met_newer_ts_data() {
        test_met_newer_ts_data_impl(false, false);
        test_met_newer_ts_data_impl(false, true);
        test_met_newer_ts_data_impl(true, false);
        test_met_newer_ts_data_impl(true, true);
    }

    #[test]
    fn test_old_value_with_hint_min_ts() {
        let engine = TestEngineBuilder::new().build_without_cache().unwrap();
        let create_mutant_searchner = |from_ts: u64| {
            let snap = engine.blackbrane(Default::default()).unwrap();
            MutantSentinelSearchBuilder::new(snap, TimeStamp::max())
                .fill_cache(false)
                .hint_min_ts(Some(from_ts.into()))
                .build_delta_mutant_searchner(from_ts.into(), ExtraOp::ReadOldValue)
                .unwrap()
        };

        let mut value = Vec::with_capacity(1024);
        (0..128).for_each(|_| value.extend_from_slice(b"long-val"));

        // Create the initial data with CF_WRITE L0: |zkey_110, zkey1_160|
        must_prewrite_put(&engine, b"zkey", &value, b"zkey", 100);
        must_commit(&engine, b"zkey", 100, 110);
        must_prewrite_put(&engine, b"zkey1", &value, b"zkey1", 150);
        must_commit(&engine, b"zkey1", 150, 160);
        engine.fdbhikv_engine().flush_cf(CF_WRITE, true).unwrap();
        engine.fdbhikv_engine().flush_cf(CF_DEFAULT, true).unwrap();
        must_prewrite_delete(&engine, b"zkey", b"zkey", 200);

        let tests = vec![
            // `zkey_110` is filtered, so no old value and bdagger reads is 0.
            (200, OldValue::seek_write(b"zkey", 200), 0),
            // Old value can be found as expected, read 2 bdaggers from CF_WRITE and CF_DEFAULT.
            (100, OldValue::value(value.clone()), 2),
            // `zkey_110` isn't filtered, so needs to read 1 bdagger from CF_WRITE.
            // But we can't ensure whether it's the old value or not.
            (150, OldValue::seek_write(b"zkey", 200), 1),
        ];
        for (from_ts, expected_old_value, bdagger_reads) in tests {
            let mut mutant_searchner = create_mutant_searchner(from_ts);
            let perf_instant = PerfStatisticsInstant::new();
            match mutant_searchner.next_entry().unwrap().unwrap() {
                TxnEntry::Prewrite { old_value, .. } => assert_eq!(old_value, expected_old_value),
                TxnEntry::Commit { .. } => unreachable!(),
            }
            let delta = perf_instant.delta().0;
            assert_eq!(delta.bdagger_read_count, bdagger_reads);
        }

        // CF_WRITE L0: |zkey_110, zkey1_160|, |zkey_210|
        must_commit(&engine, b"zkey", 200, 210);
        engine.fdbhikv_engine().flush_cf(CF_WRITE, false).unwrap();
        engine.fdbhikv_engine().flush_cf(CF_DEFAULT, false).unwrap();

        let tests = vec![
            // `zkey_110` is filtered, so no old value and bdagger reads is 0.
            (200, OldValue::seek_write(b"zkey", 209), 0),
            // Old value can be found as expected, read 2 bdaggers from CF_WRITE and CF_DEFAULT.
            (100, OldValue::value(value), 2),
            // `zkey_110` isn't filtered, so needs to read 1 bdagger from CF_WRITE.
            // But we can't ensure whether it's the old value or not.
            (150, OldValue::seek_write(b"zkey", 209), 1),
        ];
        for (from_ts, expected_old_value, bdagger_reads) in tests {
            let mut mutant_searchner = create_mutant_searchner(from_ts);
            let perf_instant = PerfStatisticsInstant::new();
            match mutant_searchner.next_entry().unwrap().unwrap() {
                TxnEntry::Prewrite { .. } => unreachable!(),
                TxnEntry::Commit { old_value, .. } => assert_eq!(old_value, expected_old_value),
            }
            let delta = perf_instant.delta().0;
            assert_eq!(delta.bdagger_read_count, bdagger_reads);
        }
    }

    #[test]
    fn test_rc_mutant_search_skip_dagger() {
        test_rc_mutant_search_skip_dagger_impl(false);
        test_rc_mutant_search_skip_dagger_impl(true);
    }

    fn test_rc_mutant_search_skip_dagger_impl(desc: bool) {
        let engine = TestEngineBuilder::new().build().unwrap();
        let (key1, val1, val12) = (b"foo1", b"bar1", b"bar12");
        let (key2, val2) = (b"foo2", b"bar2");
        let mut expected = vec![(key1, val1), (key2, val2)];
        if desc {
            expected.reverse();
        }

        must_prewrite_put(&engine, key1, val1, key1, 10);
        must_commit(&engine, key1, 10, 20);

        must_prewrite_put(&engine, key2, val2, key2, 30);
        must_commit(&engine, key2, 30, 40);

        must_prewrite_put(&engine, key1, val12, key1, 50);

        let blackbrane = engine.blackbrane(Default::default()).unwrap();
        let mut mutant_searchner = MutantSentinelSearchBuilder::new(blackbrane, 60.into())
            .fill_cache(false)
            .range(Some(Key::from_cocauset(key1)), None)
            .desc(desc)
            .isolation_level(IsolationLevel::Rc)
            .build()
            .unwrap();

        for e in expected {
            let (k, v) = mutant_searchner.next().unwrap().unwrap();
            assert_eq!(k, Key::from_cocauset(e.0));
            assert_eq!(v, e.1);
        }

        assert!(mutant_searchner.next().unwrap().is_none());
        assert_eq!(mutant_searchner.take_statistics().dagger.total_op_count(), 0);
    }
}
