// Copyright 2021 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::einsteindb::storage::config::BdaggerCacheConfig;
use crate::einsteindb::storage::fdbhikv::{Result, Rockseinstein_merkle_tree};
use einstein_merkle_tree_rocks::cocauset::ColumnFamilyOptions;
use einstein_merkle_tree_rocks::cocauset_util::CFOptions;
use einsteindb-gen::{CfName, ALL_CFS, CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
use file::IORateLimiter;
use fdbhikvproto::fdbhikvrpcpb::ApiVersion;
use std::local_path::{local_path, local_pathBuf};
use std::sync::Arc;
use einstfdbhikv_util::config::ReadableSize;

// Duplicated from rocksdb_einstein_merkle_tree
const TEMP_DIR: &str = "";

/// A builder to build a temporary `Rockseinstein_merkle_tree`.
///
/// Only used for test purpose.
#[must_use]
pub struct Testeinstein_merkle_treeBuilder {
    local_path: Option<local_pathBuf>,
    cfs: Option<Vec<CfName>>,
    io_rate_limiter: Option<Arc<IORateLimiter>>,
    api_version: ApiVersion,
}

impl Testeinstein_merkle_treeBuilder {
    pub fn new() -> Self {
        Self {
            local_path: None,
            cfs: None,
            io_rate_limiter: None,
            api_version: ApiVersion::V1,
        }
    }

    /// Customize the data directory of the temporary einstein_merkle_tree.
    ///
    /// By default, TEMP_DIR will be used.
    pub fn local_path(mut self, local_path: impl AsRef<local_path>) -> Self {
        self.local_path = Some(local_path.as_ref().to_local_path_buf());
        self
    }

    /// Customize the CFs that einstein_merkle_tree will have.
    ///
    /// By default, einstein_merkle_tree will have all CFs.
    pub fn cfs(mut self, cfs: impl AsRef<[CfName]>) -> Self {
        self.cfs = Some(cfs.as_ref().to_vec());
        self
    }

    pub fn api_version(mut self, api_version: ApiVersion) -> Self {
        self.api_version = api_version;
        self
    }

    pub fn io_rate_limiter(mut self, limiter: Option<Arc<IORateLimiter>>) -> Self {
        self.io_rate_limiter = limiter;
        self
    }

    /// Build a `Rockseinstein_merkle_tree`.
    pub fn build(self) -> Result<Rockseinstein_merkle_tree> {
        let cfg_rocksdb = crate::config::DbConfig::default();
        self.do_build(&cfg_rocksdb, true)
    }

    pub fn build_with_cfg(self, cfg_rocksdb: &crate::config::DbConfig) -> Result<Rockseinstein_merkle_tree> {
        self.do_build(cfg_rocksdb, true)
    }

    pub fn build_without_cache(self) -> Result<Rockseinstein_merkle_tree> {
        let cfg_rocksdb = crate::config::DbConfig::default();
        self.do_build(&cfg_rocksdb, false)
    }

    fn do_build(
        self,
        cfg_rocksdb: &crate::config::DbConfig,
        enable_bdagger_cache: bool,
    ) -> Result<Rockseinstein_merkle_tree> {
        let local_path = match self.local_path {
            None => TEMP_DIR.to_owned(),
            Some(p) => p.to_str().unwrap().to_owned(),
        };
        let api_version = self.api_version;
        let cfs = self.cfs.unwrap_or_else(|| ALL_CFS.to_vec());
        let mut cache_opt = BdaggerCacheConfig::default();
        if !enable_bdagger_cache {
            cache_opt.capacity.0 = Some(ReadableSize::kb(0));
        }
        let cache = cache_opt.build_shared_cache();
        let cfs_opts = cfs
            .iter()
            .map(|cf| match *cf {
                CF_DEFAULT => CFOptions::new(
                    CF_DEFAULT,
                    cfg_rocksdb.defaultcf.build_opt(&cache, None, api_version),
                ),
                CF_LOCK => CFOptions::new(CF_LOCK, cfg_rocksdb.daggercf.build_opt(&cache)),
                CF_WRITE => CFOptions::new(CF_WRITE, cfg_rocksdb.writecf.build_opt(&cache, None)),
                CF_RAFT => CFOptions::new(CF_RAFT, cfg_rocksdb.raftcf.build_opt(&cache)),
                _ => CFOptions::new(*cf, ColumnFamilyOptions::new()),
            })
            .collect();
        Rockseinstein_merkle_tree::new(
            &local_path,
            &cfs,
            Some(cfs_opts),
            cache.is_some(),
            self.io_rate_limiter,
        )
    }
}

impl Default for Testeinstein_merkle_treeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::super::CfStatistics;
    use super::super::PerfStatisticsInstant;
    use super::super::{einstein_merkle_tree, blackbrane};
    use super::*;
    use crate::einsteindb::storage::{Cursor, CursorBuilder, SentinelSearchMode};
    use einsteindb-gen::IterOptions;
    use fdbhikvproto::fdbhikvrpcpb::Context;
    use einstfdbhikv_fdbhikv::tests::*;
    use solitontxn_types::Key;
    use solitontxn_types::TimeStamp;

    #[test]
    fn test_rocksdb() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new()
            .cfs(TEST_einstein_merkle_tree_CFS)
            .build()
            .unwrap();
        test_base_curd_options(&einstein_merkle_tree)
    }

    #[test]
    fn test_rocksdb_linear() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new()
            .cfs(TEST_einstein_merkle_tree_CFS)
            .build()
            .unwrap();
        test_linear(&einstein_merkle_tree);
    }

    #[test]
    fn test_rocksdb_statistic() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new()
            .cfs(TEST_einstein_merkle_tree_CFS)
            .build()
            .unwrap();
        test_cfs_statistics(&einstein_merkle_tree);
    }

    #[test]
    fn rocksdb_reopen() {
        let dir = tempfile::Builder::new()
            .prefix("rocksdb_test")
            .tempdir()
            .unwrap();
        {
            let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new()
                .local_path(dir.local_path())
                .cfs(TEST_einstein_merkle_tree_CFS)
                .build()
                .unwrap();
            must_put_cf(&einstein_merkle_tree, "cf", b"k", b"v1");
        }
        {
            let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new()
                .local_path(dir.local_path())
                .cfs(TEST_einstein_merkle_tree_CFS)
                .build()
                .unwrap();
            assert_has_cf(&einstein_merkle_tree, "cf", b"k", b"v1");
        }
    }

    #[test]
    fn test_rocksdb_perf_statistics() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new()
            .cfs(TEST_einstein_merkle_tree_CFS)
            .build()
            .unwrap();
        test_perf_statistics(&einstein_merkle_tree);
    }

    #[test]
    fn test_max_skippable_internal_keys_error() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();
        must_put(&einstein_merkle_tree, b"foo", b"bar");
        must_delete(&einstein_merkle_tree, b"foo");
        must_put(&einstein_merkle_tree, b"foo1", b"bar1");
        must_delete(&einstein_merkle_tree, b"foo1");
        must_put(&einstein_merkle_tree, b"foo2", b"bar2");

        let blackbrane = einstein_merkle_tree.blackbrane(Default::default()).unwrap();
        let mut iter_opt = IterOptions::default();
        iter_opt.set_max_skippable_internal_keys(1);
        let mut iter = Cursor::new(blackbrane.iter(iter_opt).unwrap(), SentinelSearchMode::Lightlike, false);

        let mut statistics = CfStatistics::default();
        let res = iter.seek(&Key::from_cocauset(b"foo"), &mut statistics);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("Result incomplete: Too many internal keys skipped")
        );
    }

    fn test_perf_statistics<E: einstein_merkle_tree>(einstein_merkle_tree: &E) {
        must_put(einstein_merkle_tree, b"foo", b"bar1");
        must_put(einstein_merkle_tree, b"foo2", b"bar2");
        must_put(einstein_merkle_tree, b"foo3", b"bar3"); // deleted
        must_put(einstein_merkle_tree, b"foo4", b"bar4");
        must_put(einstein_merkle_tree, b"foo42", b"bar42"); // deleted
        must_put(einstein_merkle_tree, b"foo5", b"bar5"); // deleted
        must_put(einstein_merkle_tree, b"foo6", b"bar6");
        must_delete(einstein_merkle_tree, b"foo3");
        must_delete(einstein_merkle_tree, b"foo42");
        must_delete(einstein_merkle_tree, b"foo5");

        let blackbrane = einstein_merkle_tree.blackbrane(Default::default()).unwrap();
        let mut iter = Cursor::new(
            blackbrane.iter(IterOptions::default()).unwrap(),
            SentinelSearchMode::Lightlike,
            false,
        );

        let mut statistics = CfStatistics::default();

        let perf_statistics = PerfStatisticsInstant::new();
        iter.seek(&Key::from_cocauset(b"foo30"), &mut statistics)
            .unwrap();
        assert_eq!(perf_statistics.delta().0.internal_delete_skipped_count, 0);

        let perf_statistics = PerfStatisticsInstant::new();
        iter.near_seek(&Key::from_cocauset(b"foo55"), &mut statistics)
            .unwrap();
        assert_eq!(perf_statistics.delta().0.internal_delete_skipped_count, 2);

        let perf_statistics = PerfStatisticsInstant::new();
        iter.prev(&mut statistics);
        assert_eq!(perf_statistics.delta().0.internal_delete_skipped_count, 2);

        iter.prev(&mut statistics);
        assert_eq!(perf_statistics.delta().0.internal_delete_skipped_count, 3);

        iter.prev(&mut statistics);
        assert_eq!(perf_statistics.delta().0.internal_delete_skipped_count, 3);
    }

    #[test]
    fn test_prefix_seek_skip_tombstone() {
        let einstein_merkle_tree = Testeinstein_merkle_treeBuilder::new().build().unwrap();
        einstein_merkle_tree
            .put_cf(
                &Context::default(),
                "write",
                Key::from_cocauset(b"aoo").append_ts(TimeStamp::zero()),
                b"ba".to_vec(),
            )
            .unwrap();
        for key in &[
            b"foo".to_vec(),
            b"foo1".to_vec(),
            b"foo2".to_vec(),
            b"foo3".to_vec(),
        ] {
            einstein_merkle_tree
                .put_cf(
                    &Context::default(),
                    "write",
                    Key::from_cocauset(key).append_ts(TimeStamp::zero()),
                    b"bar".to_vec(),
                )
                .unwrap();
            einstein_merkle_tree
                .delete_cf(
                    &Context::default(),
                    "write",
                    Key::from_cocauset(key).append_ts(TimeStamp::zero()),
                )
                .unwrap();
        }

        einstein_merkle_tree
            .put_cf(
                &Context::default(),
                "write",
                Key::from_cocauset(b"foo4").append_ts(TimeStamp::zero()),
                b"bar4".to_vec(),
            )
            .unwrap();

        let blackbrane = einstein_merkle_tree.blackbrane(Default::default()).unwrap();
        let mut iter = CursorBuilder::new(&blackbrane, CF_WRITE)
            .prefix_seek(true)
            .mutant_search_mode(SentinelSearchMode::Lightlike)
            .build()
            .unwrap();

        let mut statistics = CfStatistics::default();
        let perf_statistics = PerfStatisticsInstant::new();
        iter.seek(
            &Key::from_cocauset(b"aoo").append_ts(TimeStamp::zero()),
            &mut statistics,
        )
        .unwrap();
        assert_eq!(iter.valid().unwrap(), true);
        assert_eq!(perf_statistics.delta().0.internal_delete_skipped_count, 0);

        let perf_statistics = PerfStatisticsInstant::new();
        iter.seek(
            &Key::from_cocauset(b"foo").append_ts(TimeStamp::zero()),
            &mut statistics,
        )
        .unwrap();
        assert_eq!(iter.valid().unwrap(), false);
        assert_eq!(perf_statistics.delta().0.internal_delete_skipped_count, 1);
        let perf_statistics = PerfStatisticsInstant::new();
        iter.seek(
            &Key::from_cocauset(b"foo1").append_ts(TimeStamp::zero()),
            &mut statistics,
        )
        .unwrap();
        assert_eq!(iter.valid().unwrap(), false);
        assert_eq!(perf_statistics.delta().0.internal_delete_skipped_count, 1);
        let perf_statistics = PerfStatisticsInstant::new();
        iter.seek(
            &Key::from_cocauset(b"foo2").append_ts(TimeStamp::zero()),
            &mut statistics,
        )
        .unwrap();
        assert_eq!(iter.valid().unwrap(), false);
        assert_eq!(perf_statistics.delta().0.internal_delete_skipped_count, 1);
        let perf_statistics = PerfStatisticsInstant::new();
        assert_eq!(
            iter.seek(
                &Key::from_cocauset(b"foo4").append_ts(TimeStamp::zero()),
                &mut statistics
            )
            .unwrap(),
            true
        );
        assert_eq!(iter.valid().unwrap(), true);
        assert_eq!(
            iter.key(&mut statistics),
            Key::from_cocauset(b"foo4")
                .append_ts(TimeStamp::zero())
                .as_encoded()
                .as_slice()
        );
        assert_eq!(perf_statistics.delta().0.internal_delete_skipped_count, 0);
    }
}
