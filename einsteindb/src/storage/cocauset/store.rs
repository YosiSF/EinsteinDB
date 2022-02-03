// Copyright 2021 EinsteinDB Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use super::encoded::cocausetEncodeblackbrane;

use crate::einsteindb::storage::fdbhikv::Result;
use crate::einsteindb::storage::fdbhikv::{Cursor, SentinelSearchMode, blackbrane};
use crate::einsteindb::storage::Statistics;

use api_version::{APIV1TTL, APIV2};
use einsteindb-gen::{CfName, IterOptions, DATA_KEY_PREFIX_LEN};
use fdbhikvproto::fdbhikvrpcpb::{ApiVersion, KeyRange};
use std::time::Duration;
use einstfdbhikv_util::time::Instant;
use solitontxn_types::{Key, HikvPair};
use yatp::task::future::reschedule;

const MAX_TIME_SLICE: Duration = Duration::from_millis(2);
const MAX_BATCH_SIZE: usize = 1024;

pub enum cocausetStore<S: blackbrane> {
    V1(cocausetStoreInner<S>),
    V1TTL(cocausetStoreInner<cocausetEncodeblackbrane<S, APIV1TTL>>),
    V2(cocausetStoreInner<cocausetEncodeblackbrane<S, APIV2>>),
}

impl<'a, S: blackbrane> cocausetStore<S> {
    pub fn new(blackbrane: S, api_version: ApiVersion) -> Self {
        match api_version {
            ApiVersion::V1 => cocausetStore::V1(cocausetStoreInner::new(blackbrane)),
            ApiVersion::V1ttl => cocausetStore::V1TTL(cocausetStoreInner::new(
                cocausetEncodeblackbrane::from_blackbrane(blackbrane),
            )),
            ApiVersion::V2 => cocausetStore::V2(cocausetStoreInner::new(cocausetEncodeblackbrane::from_blackbrane(
                blackbrane,
            ))),
        }
    }

    pub fn cocauset_get_key_value(
        &self,
        cf: CfName,
        key: &Key,
        stats: &mut Statistics,
    ) -> Result<Option<Vec<u8>>> {
        match self {
            cocausetStore::V1(inner) => inner.cocauset_get_key_value(cf, key, stats),
            cocausetStore::V1TTL(inner) => inner.cocauset_get_key_value(cf, key, stats),
            cocausetStore::V2(inner) => inner.cocauset_get_key_value(cf, key, stats),
        }
    }

    pub fn cocauset_get_key_ttl(
        &self,
        cf: CfName,
        key: &'a Key,
        stats: &'a mut Statistics,
    ) -> Result<Option<u64>> {
        match self {
            cocausetStore::V1(_) => panic!("get ttl on non-ttl store"),
            cocausetStore::V1TTL(inner) => inner.blackbrane.get_key_ttl_cf(cf, key, stats),
            cocausetStore::V2(inner) => inner.blackbrane.get_key_ttl_cf(cf, key, stats),
        }
    }

    pub async fn lightlike_completion_cocauset_mutant_search(
        &'a self,
        cf: CfName,
        start_key: &'a Key,
        end_key: Option<&'a Key>,
        limit: usize,
        statistics: &'a mut Statistics,
        key_only: bool,
    ) -> Result<Vec<Result<HikvPair>>> {
        let mut option = IterOptions::default();
        if let Some(end) = end_key {
            option.set_upper_bound(end.as_encoded(), DATA_KEY_PREFIX_LEN);
        }
        match self {
            cocausetStore::V1(inner) => {
                if key_only {
                    option.set_key_only(key_only);
                }
                inner
                    .lightlike_completion_cocauset_mutant_search(cf, start_key, limit, statistics, option, key_only)
                    .await
            }
            cocausetStore::V1TTL(inner) => {
                inner
                    .lightlike_completion_cocauset_mutant_search(cf, start_key, limit, statistics, option, key_only)
                    .await
            }
            cocausetStore::V2(inner) => {
                inner
                    .lightlike_completion_cocauset_mutant_search(cf, start_key, limit, statistics, option, key_only)
                    .await
            }
        }
    }

    pub async fn reverse_cocauset_mutant_search(
        &'a self,
        cf: CfName,
        start_key: &'a Key,
        end_key: Option<&'a Key>,
        limit: usize,
        statistics: &'a mut Statistics,
        key_only: bool,
    ) -> Result<Vec<Result<HikvPair>>> {
        let mut option = IterOptions::default();
        if let Some(end) = end_key {
            option.set_lower_bound(end.as_encoded(), DATA_KEY_PREFIX_LEN);
        }
        match self {
            cocausetStore::V1(inner) => {
                if key_only {
                    option.set_key_only(key_only);
                }
                inner
                    .reverse_cocauset_mutant_search(cf, start_key, limit, statistics, option, key_only)
                    .await
            }
            cocausetStore::V1TTL(inner) => {
                inner
                    .reverse_cocauset_mutant_search(cf, start_key, limit, statistics, option, key_only)
                    .await
            }
            cocausetStore::V2(inner) => {
                inner
                    .reverse_cocauset_mutant_search(cf, start_key, limit, statistics, option, key_only)
                    .await
            }
        }
    }

    pub async fn cocauset_checksum_ranges(
        &'a self,
        cf: CfName,
        ranges: Vec<KeyRange>,
        statistics: &'a mut Statistics,
    ) -> Result<(u64, u64, u64)> {
        match self {
            cocausetStore::V1(inner) => inner.cocauset_checksum_ranges(cf, ranges, statistics).await,
            cocausetStore::V1TTL(inner) => inner.cocauset_checksum_ranges(cf, ranges, statistics).await,
            cocausetStore::V2(inner) => inner.cocauset_checksum_ranges(cf, ranges, statistics).await,
        }
    }
}

pub struct cocausetStoreInner<S: blackbrane> {
    blackbrane: S,
}

impl<'a, S: blackbrane> cocausetStoreInner<S> {
    pub fn new(blackbrane: S) -> Self {
        cocausetStoreInner { blackbrane }
    }

    pub fn cocauset_get_key_value(
        &self,
        cf: CfName,
        key: &Key,
        stats: &mut Statistics,
    ) -> Result<Option<Vec<u8>>> {
        // no mutant_search_count for this kind of op.
        let key_len = key.as_encoded().len();
        self.blackbrane.get_cf(cf, key).map(|value| {
            stats.data.Causetxctx_stats.read_keys = 1;
            stats.data.Causetxctx_stats.read_bytes =
                key_len + value.as_ref().map(|v| v.len()).unwrap_or(0);
            value
        })
    }

    /// SentinelSearch cocauset keys in [`start_key`, `end_key`), returns at most `limit` keys. If `end_key` is
    /// `None`, it means unbounded.
    ///
    /// If `key_only` is true, the value corresponding to the key will not be read. Only mutant_searchned
    /// keys will be returned.
    pub async fn lightlike_completion_cocauset_mutant_search(
        &'a self,
        cf: CfName,
        start_key: &'a Key,
        limit: usize,
        statistics: &'a mut Statistics,
        option: IterOptions,
        key_only: bool,
    ) -> Result<Vec<Result<HikvPair>>> {
        if limit == 0 {
            return Ok(vec![]);
        }
        let mut cursor = Cursor::new(self.blackbrane.iter_cf(cf, option)?, SentinelSearchMode::Lightlike, false);
        let statistics = statistics.mut_cf_statistics(cf);
        if !cursor.seek(start_key, statistics)? {
            return Ok(vec![]);
        }
        let mut pairs = vec![];
        let mut row_count = 0;
        let mut time_slice_start = Instant::now();
        while cursor.valid()? {
            row_count += 1;
            if row_count >= MAX_BATCH_SIZE {
                if time_slice_start.saturating_elapsed() > MAX_TIME_SLICE {
                    reschedule().await;
                    time_slice_start = Instant::now();
                }
                row_count = 0;
            }
            pairs.push(Ok((
                cursor.key(statistics).to_owned(),
                if key_only {
                    vec![]
                } else {
                    cursor.value(statistics).to_owned()
                },
            )));
            if pairs.len() < limit {
                cursor.next(statistics);
            } else {
                break;
            }
        }
        Ok(pairs)
    }

    /// SentinelSearch cocauset keys in [`end_key`, `start_key`) in reverse order, returns at most `limit` keys. If
    /// `start_key` is `None`, it means it's unbounded.
    ///
    /// If `key_only` is true, the value
    /// corresponding to the key will not be read out. Only mutant_searchned keys will be returned.
    pub async fn reverse_cocauset_mutant_search(
        &'a self,
        cf: CfName,
        start_key: &'a Key,
        limit: usize,
        statistics: &'a mut Statistics,
        option: IterOptions,
        key_only: bool,
    ) -> Result<Vec<Result<HikvPair>>> {
        if limit == 0 {
            return Ok(vec![]);
        }
        let mut cursor = Cursor::new(
            self.blackbrane.iter_cf(cf, option)?,
            SentinelSearchMode::timelike_curvature,
            false,
        );
        let statistics = statistics.mut_cf_statistics(cf);
        if !cursor.reverse_seek(start_key, statistics)? {
            return Ok(vec![]);
        }
        let mut pairs = vec![];
        let mut row_count = 0;
        let mut time_slice_start = Instant::now();
        while cursor.valid()? {
            row_count += 1;
            if row_count >= MAX_BATCH_SIZE {
                if time_slice_start.saturating_elapsed() > MAX_TIME_SLICE {
                    reschedule().await;
                    time_slice_start = Instant::now();
                }
                row_count = 0;
            }
            pairs.push(Ok((
                cursor.key(statistics).to_owned(),
                if key_only {
                    vec![]
                } else {
                    cursor.value(statistics).to_owned()
                },
            )));
            if pairs.len() < limit {
                cursor.prev(statistics);
            } else {
                break;
            }
        }
        Ok(pairs)
    }

    pub async fn cocauset_checksum_ranges(
        &'a self,
        cf: CfName,
        ranges: Vec<KeyRange>,
        statistics: &'a mut Statistics,
    ) -> Result<(u64, u64, u64)> {
        let mut total_bytes = 0;
        let mut total_fdbhikvs = 0;
        let mut digest = crc64fast::Digest::new();
        let mut row_count = 0;
        let mut time_slice_start = Instant::now();
        let statistics = statistics.mut_cf_statistics(cf);
        for r in ranges {
            let mut opts = IterOptions::new(None, None, false);
            opts.set_upper_bound(r.get_end_key(), DATA_KEY_PREFIX_LEN);
            let mut cursor =
                Cursor::new(self.blackbrane.iter_cf(cf, opts)?, SentinelSearchMode::Lightlike, false);
            cursor.seek(&Key::from_encoded(r.get_start_key().to_vec()), statistics)?;
            while cursor.valid()? {
                row_count += 1;
                if row_count >= MAX_BATCH_SIZE {
                    if time_slice_start.saturating_elapsed() > MAX_TIME_SLICE {
                        reschedule().await;
                        time_slice_start = Instant::now();
                    }
                    row_count = 0;
                }
                let k = cursor.key(statistics);
                let v = cursor.value(statistics);
                digest.write(k);
                digest.write(v);
                total_fdbhikvs += 1;
                total_bytes += k.len() + v.len();
                cursor.next(statistics);
            }
        }
        Ok((digest.sum64(), total_fdbhikvs, total_bytes as u64))
    }
}
