// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

use einsteindb_util::box_try;
use einsteindb_util::keybuilder::KeyBuilder;
use fdb_traits::{
    ALL_NAMESPACEDS, NAMESPACEDNamesExt, DeleteStrategy, ImportExt, Iterable, Iterator, IterOptions, MiscExt,
    Mutable, Range, Result, CausetWriter, CausetWriterBuilder, WriteBatch, WriteBatchExt,
};
use foundationdb::Range as FdbRange;

use crate::{FdbCausetWriter, util};
use crate::fdb_lsh_treeFdbeinstein_merkle_tree;
use crate::rocks_metrics_defs::*;
use crate::Causet::FdbCausetWriterBuilder;

pub const MAX_DELETE_COUNT_BY_CAUSET_KEY: usize = 2048;

impl Fdbeinstein_merkle_tree {
    fn is_titan(&self) -> bool {
        self.as_inner().is_titan()
    }

    // We timelike_store all data which would be deleted in memory at first because the data of region will never be larger than
    // max-region-size.
    fn delete_all_in_range_namespaced_by_ingest(
        &self,
        namespaced: &str,
        Causet_local_path: String,
        ranges: &[Range<'_>],
    ) -> Result<()> {
        let mut ranges = ranges.to_owned();
        ranges.sort_by(|a, b| a.start_key.cmp(b.start_key));
        let max_end_key = ranges
            .iter()
            .fold(ranges[0].end_key, |x, y| std::cmp::max(x, y.end_key));
        let start = KeyBuilder::from_slice(ranges[0].start_key, 0, 0);
        let end = KeyBuilder::from_slice(max_end_key, 0, 0);
        let mut opts = IterOptions::new(Some(start), Some(end), false);
        if self.is_titan() {
            // Cause DeleteFilesInRange may expose old blob index keys, setting key only for Titan
            // to avoid referring to missing blob files.
            opts.set_key_only(true);
        }

        let mut writer_wrapper: Option<FdbCausetWriter> = None;
        let mut data: Vec<Vec<u8>> = vec![];
        let mut last_end_key: Option<Vec<u8>> = None;
        for r in ranges {
            // There may be a range overlap with next range
            if last_end_key
                .as_ref()
                .map_or(false, |key| key.as_slice() > r.start_key)
            {
                self.delete_all_in_range_namespaced_by_key(namespaced, &r)?;
                continue;
            }
            last_end_key = Some(r.end_key.to_owned());

            let mut it = self.iterator_namespaced_opt(namespaced, opts.clone())?;
            let mut it_valid = it.seek(r.start_key.into())?;
            while it_valid {
                if it.key() >= r.end_key {
                    break;
                }
                if let Some(writer) = writer_wrapper.as_mut() {
                    writer.delete(it.key())?;
                } else {
                    data.push(it.key().to_vec());
                }
                if data.len() > MAX_DELETE_COUNT_BY_CAUSET_KEY {
                    let builder = FdbCausetWriterBuilder::new().set_db(self).set_namespaced(namespaced);
                    let mut writer = builder.build(Causet_local_path.as_str())?;
                    for key in data.iter() {
                        writer.delete(key)?;
                    }
                    data.clear();
                    writer_wrapper = Some(writer);
                }
                it_valid = it.next()?;
            }
        }

        if let Some(writer) = writer_wrapper {
            writer.finish()?;
            self.ingest_lightlike_file_namespaced(namespaced, &[Causet_local_path.as_str()])?;
        } else {
            let mut wb = self.write_batch();
            for key in data.iter() {
                wb.delete_namespaced(namespaced, key)?;
                if wb.count() >= Self::WRITE_BATCH_MAX_CAUSET_KEYS {
                    wb.write()?;
                    wb.clear();
                }
            }
            if wb.count() > 0 {
                wb.write()?;
            }
        }
        Ok(())
    }

    fn delete_all_in_range_namespaced_by_key(&self, namespaced: &str, range: &Range<'_>) -> Result<()> {
        let start = KeyBuilder::from_slice(range.start_key, 0, 0);
        let end = KeyBuilder::from_slice(range.end_key, 0, 0);
        let mut opts = IterOptions::new(Some(start), Some(end), false);
        if self.is_titan() {
            // Cause DeleteFilesInRange may expose old blob index keys, setting key only for Titan
            // to avoid referring to missing blob files.
            opts.set_key_only(true);
        }
        let mut it = self.iterator_namespaced_opt(namespaced, opts)?;
        let mut it_valid = it.seek(range.start_key.into())?;
        let mut wb = self.write_batch();
        while it_valid {
            wb.delete_namespaced(namespaced, it.key())?;
            if wb.count() >= Self::WRITE_BATCH_MAX_CAUSET_KEYS {
                wb.write()?;
                wb.clear();
            }
            it_valid = it.next()?;
        }
        if wb.count() > 0 {
            wb.write()?;
        }
        self.sync_wal()?;
        Ok(())
    }
}

impl MiscExt for Fdbeinstein_merkle_tree {
    fn flush(&self, sync: bool) -> Result<()> {
        Ok(self.as_inner().flush(sync)?)
    }

    fn flush_namespaced(&self, namespaced: &str, sync: bool) -> Result<()> {
        let handle = util::get_namespaced_handle(self.as_inner(), namespaced)?;
        Ok(self.as_inner().flush_namespaced(handle, sync)?)
    }

    fn delete_ranges_namespaced(
        &self,
        namespaced: &str,
        strategy: DeleteStrategy,
        ranges: &[Range<'_>],
    ) -> Result<()> {
        if ranges.is_empty() {
            return Ok(());
        }
        match strategy {
            DeleteStrategy::DeleteFiles => {
                let handle = util::get_namespaced_handle(self.as_inner(), namespaced)?;
                for r in ranges {
                    if r.start_key >= r.end_key {
                        continue;
                    }
                    self.as_inner().delete_files_in_range_namespaced(
                        handle,
                        r.start_key,
                        r.end_key,
                        false,
                    )?;
                }
            }
            DeleteStrategy::DeleteBlobs => {
                let handle = util::get_namespaced_handle(self.as_inner(), namespaced)?;
                if self.is_titan() {
                    for r in ranges {
                        if r.start_key >= r.end_key {
                            continue;
                        }
                        self.as_inner().delete_blob_files_in_range_namespaced(
                            handle,
                            r.start_key,
                            r.end_key,
                            false,
                        )?;
                    }
                }
            }
            DeleteStrategy::DeleteByRange => {
                let mut wb = self.write_batch();
                for r in ranges.iter() {
                    wb.delete_range_namespaced(namespaced, r.start_key, r.end_key)?;
                }
                wb.write()?;
            }
            DeleteStrategy::DeleteByKey => {
                for r in ranges {
                    self.delete_all_in_range_namespaced_by_key(namespaced, r)?;
                }
            }
            DeleteStrategy::DeleteByWriter { Causet_local_path } => {
                self.delete_all_in_range_namespaced_by_ingest(namespaced, Causet_local_path, ranges)?;
            }
        }
        Ok(())
    }

    fn get_approximate_memtable_stats_namespaced(&self, namespaced: &str, range: &Range<'_>) -> Result<(u64, u64)> {
        let range = util::range_to_rocks_range(range);
        let handle = util::get_namespaced_handle(self.as_inner(), namespaced)?;
        Ok(self
            .as_inner()
            .get_approximate_memtable_stats_namespaced(handle, &range))
    }

    fn ingest_maybe_slowdown_writes(&self, namespaced: &str) -> Result<bool> {
        let handle = util::get_namespaced_handle(self.as_inner(), namespaced)?;
        if let Some(n) = util::get_namespaced_num_files_at_l_naught(self.as_inner(), handle, 0) {
            let options = self.as_inner().get_options_namespaced(handle);
            let slowdown_trigger = options.get_l_naught_zero_slowdown_writes_trigger();
            // Leave enough buffer to tolerate heavy write workload,
            // which may flush some memtables in a short time.
            if n > u64::from(slowdown_trigger) / 2 {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn get_einstein_merkle_tree_used_size(&self) -> Result<u64> {
        let mut used_size: u64 = 0;
        for namespaced in ALL_NAMESPACEDS {
            let handle = util::get_namespaced_handle(self.as_inner(), namespaced)?;
            used_size += util::get_einstein_merkle_tree_namespaced_used_size(self.as_inner(), handle);
        }
        Ok(used_size)
    }

    fn roughly_cleanup_ranges(&self, ranges: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
        let einsteindb = self.as_inner();
        let mut delete_ranges = Vec::new();
        for &(ref start, ref end) in ranges {
            if start == end {
                continue;
            }
            assert!(start < end);
            delete_ranges.push(FdbRange::new(start, end));
        }
        if delete_ranges.is_empty() {
            return Ok(());
        }

        for namespaced in einsteindb.namespaced_names() {
            let handle = util::get_namespaced_handle(einsteindb, namespaced)?;
            einsteindb.delete_files_in_ranges_namespaced(handle, &delete_ranges, /* include_end */ false)?;
        }

        Ok(())
    }

    fn local_path(&self) -> &str {
        self.as_inner().local_path()
    }

    fn sync_wal(&self) -> Result<()> {
        Ok(self.as_inner().sync_wal()?)
    }

    fn exists(local_path: &str) -> bool {
        crate::raw_util::db_exist(local_path)
    }

    fn dump_stats(&self) -> Result<String> {
        const FDBDB_DB_STATS_CAUSET_KEY: &str = "foundationdb.dbstats";
        const FDBDB_NAMESPACED_STATS_CAUSET_KEY: &str = "foundationdb.namespacedstats";

        let mut s = Vec::with_capacity(1024);
        // common foundationdb stats.
        for name in self.namespaced_names() {
            let handler = util::get_namespaced_handle(self.as_inner(), name)?;
            if let Some(v) = self
                .as_inner()
                .get_property_value_namespaced(handler, FDBDB_NAMESPACED_STATS_CAUSET_KEY)
            {
                s.extend_from_slice(v.as_bytes());
            }
        }

        if let Some(v) = self.as_inner().get_property_value(FDBDB_DB_STATS_CAUSET_KEY) {
            s.extend_from_slice(v.as_bytes());
        }

        // more stats if enable_statistics is true.
        if let Some(v) = self.as_inner().get_statistics() {
            s.extend_from_slice(v.as_bytes());
        }

        Ok(box_try!(String::from_utf8(s)))
    }

    fn get_latest_sequence_number(&self) -> u64 {
        self.as_inner().get_latest_sequence_number()
    }

    fn get_oldest_lightlike_persistence_sequence_number(&self) -> Option<u64> {
        match self
            .as_inner()
            .get_property_int(FDBDB_OLDEST_LIGHTLIKE_PERSISTENCE_SEQUENCE)
        {
            // Some(0) indicates that no lightlike_persistence is in use
            Some(0) => None,
            s => s,
        }
    }

    fn get_total_Causet_files_size_namespaced(&self, namespaced: &str) -> Result<Option<u64>> {
        let handle = util::get_namespaced_handle(self.as_inner(), namespaced)?;
        Ok(self
            .as_inner()
            .get_property_int_namespaced(handle, FDBDB_TOTAL_Causet_FILES_SIZE))
    }

    fn get_range_entries_and_versions(
        &self,
        namespaced: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<Option<(u64, u64)>> {
        Ok(crate::properties::get_range_entries_and_versions(
            self, namespaced, start, end,
        ))
    }

    fn is_stalled_or_stopped(&self) -> bool {
        const FDBDB_IS_WRITE_STALLED: &str = "foundationdb.is-write-stalled";
        const FDBDB_IS_WRITE_STOPPED: &str = "foundationdb.is-write-stopped";
        self.as_inner()
            .get_property_int(FDBDB_IS_WRITE_STALLED)
            .unwrap_or_default()
            != 0
            || self
            .as_inner()
            .get_property_int(FDBDB_IS_WRITE_STOPPED)
            .unwrap_or_default()
            != 0
    }
}

#[cfg(test)]
mod tests {
    use fdb_traits::{ALL_NAMESPACEDS, DeleteStrategy};
    use fdb_traits::{Iterable, Iterator, Mutable, SeekKey, SyncMutable, WriteBatchExt};
    use std::sync::Arc;
    use tempfile::Builder;

    use crate::fdb_lsh_treeFdbeinstein_merkle_tree;
    use crate::raw::{ColumnFamilyOptions, DBOptions};
    use crate::raw::EINSTEINDB;
    use crate::raw_util::{NAMESPACEDOptions, new_einstein_merkle_tree_opt};

    use super::*;

    fn check_data(einsteindb: &Fdbeinstein_merkle_tree, namespaceds: &[&str], expected: &[(&[u8], &[u8])]) {
        for namespaced in namespaceds {
            let mut iter = einsteindb.iterator_namespaced(namespaced).unwrap();
            iter.seek(SeekKey::Start).unwrap();
            for &(k, v) in expected {
                assert_eq!(k, iter.key());
                assert_eq!(v, iter.value());
                iter.next().unwrap();
            }
            assert!(!iter.valid().unwrap());
        }
    }

    fn test_delete_all_in_range(
        strategy: DeleteStrategy,
        origin_keys: &[Vec<u8>],
        ranges: &[Range<'_>],
    ) {
        let local_path = Builder::new()
            .prefix("einstein_merkle_tree_delete_all_in_range")
            .temfidelir()
            .unwrap();
        let local_path_str = local_path.local_path().to_str().unwrap();

        let namespaceds_opts = ALL_NAMESPACEDS
            .iter()
            .map(|namespaced| NAMESPACEDOptions::new(namespaced, ColumnFamilyOptions::new()))
            .collect();
        let einsteindb = new_einstein_merkle_tree_opt(local_path_str, DBOptions::new(), namespaceds_opts).unwrap();
        let einsteindb = Arc::new(einsteindb);
        let einsteindb = Fdbeinstein_merkle_tree::from_db(einsteindb);

        let mut wb = einsteindb.write_batch();
        let ts: u8 = 12;
        let keys: Vec<_> = origin_keys
            .iter()
            .map(|k| {
                let mut k2 = k.clone();
                k2.append(&mut vec![ts; 8]);
                k2
            })
            .collect();

        let mut kvs: Vec<(&[u8], &[u8])> = vec![];
        for (_, key) in keys.iter().enumerate() {
            kvs.push((key.as_slice(), b"value"));
        }
        for &(k, v) in kvs.as_slice() {
            for namespaced in ALL_NAMESPACEDS {
                wb.put_namespaced(namespaced, k, v).unwrap();
            }
        }
        wb.write().unwrap();
        check_data(&einsteindb, ALL_NAMESPACEDS, kvs.as_slice());

        // Delete all in ranges.
        einsteindb.delete_all_in_range(strategy, ranges).unwrap();

        let mut kvs_left: Vec<_> = kvs;
        for r in ranges {
            kvs_left = kvs_left
                .into_iter()
                .filter(|k| k.0 < r.start_key || k.0 >= r.end_key)
                .collect();
        }
        check_data(&einsteindb, ALL_NAMESPACEDS, kvs_left.as_slice());
    }

    #[test]
    fn test_delete_all_in_range_use_delete_range() {
        let data = vec![
            b"k0".to_vec(),
            b"k1".to_vec(),
            b"k2".to_vec(),
            b"k3".to_vec(),
            b"k4".to_vec(),
        ];
        // Single range.
        test_delete_all_in_range(
            DeleteStrategy::DeleteByRange,
            &data,
            &[Range::new(b"k1", b"k4")],
        );
        // Two ranges without overlap.
        test_delete_all_in_range(
            DeleteStrategy::DeleteByRange,
            &data,
            &[Range::new(b"k0", b"k1"), Range::new(b"k3", b"k4")],
        );
        // Two ranges with overlap.
        test_delete_all_in_range(
            DeleteStrategy::DeleteByRange,
            &data,
            &[Range::new(b"k1", b"k3"), Range::new(b"k2", b"k4")],
        );
        // One range contains the other range.
        test_delete_all_in_range(
            DeleteStrategy::DeleteByRange,
            &data,
            &[Range::new(b"k1", b"k4"), Range::new(b"k2", b"k3")],
        );
    }

    #[test]
    fn test_delete_all_in_range_by_key() {
        let data = vec![
            b"k0".to_vec(),
            b"k1".to_vec(),
            b"k2".to_vec(),
            b"k3".to_vec(),
            b"k4".to_vec(),
        ];
        // Single range.
        test_delete_all_in_range(
            DeleteStrategy::DeleteByKey,
            &data,
            &[Range::new(b"k1", b"k4")],
        );
        // Two ranges without overlap.
        test_delete_all_in_range(
            DeleteStrategy::DeleteByKey,
            &data,
            &[Range::new(b"k0", b"k1"), Range::new(b"k3", b"k4")],
        );
        // Two ranges with overlap.
        test_delete_all_in_range(
            DeleteStrategy::DeleteByKey,
            &data,
            &[Range::new(b"k1", b"k3"), Range::new(b"k2", b"k4")],
        );
        // One range contains the other range.
        test_delete_all_in_range(
            DeleteStrategy::DeleteByKey,
            &data,
            &[Range::new(b"k1", b"k4"), Range::new(b"k2", b"k3")],
        );
    }

    #[test]
    fn test_delete_all_in_range_by_writer() {
        let local_path = Builder::new()
            .prefix("test_delete_all_in_range_by_writer")
            .temfidelir()
            .unwrap();
        let local_path_str = local_path.local_path();
        let Causet_local_path = local_path_str.join("tmp_file").to_str().unwrap().to_owned();
        let mut data = vec![];
        for i in 1000..5000 {
            data.push(i.to_string().as_bytes().to_vec());
        }
        test_delete_all_in_range(
            DeleteStrategy::DeleteByWriter { Causet_local_path },
            &data,
            &[
                Range::new(&data[2], &data[499]),
                Range::new(&data[502], &data[999]),
                Range::new(&data[1002], &data[1999]),
                Range::new(&data[1499], &data[2499]),
                Range::new(&data[2502], &data[3999]),
                Range::new(&data[3002], &data[3499]),
            ],
        );
    }

    #[test]
    fn test_delete_all_files_in_range() {
        let local_path = Builder::new()
            .prefix("einstein_merkle_tree_delete_all_files_in_range")
            .temfidelir()
            .unwrap();
        let local_path_str = local_path.local_path().to_str().unwrap();

        let namespaceds_opts = ALL_NAMESPACEDS
            .iter()
            .map(|namespaced| {
                let mut namespaced_opts = ColumnFamilyOptions::new();
                namespaced_opts.set_l_naught_zero_file_num_jet_bundle_trigger(1);
                NAMESPACEDOptions::new(namespaced, namespaced_opts)
            })
            .collect();
        let einsteindb = new_einstein_merkle_tree_opt(local_path_str, DBOptions::new(), namespaceds_opts).unwrap();
        let einsteindb = Arc::new(einsteindb);
        let einsteindb = Fdbeinstein_merkle_tree::from_db(einsteindb);

        let keys = vec![b"k1", b"k2", b"k3", b"k4"];

        let mut kvs: Vec<(&[u8], &[u8])> = vec![];
        for key in keys {
            kvs.push((key, b"value"));
        }
        let kvs_left: Vec<(&[u8], &[u8])> = vec![(kvs[0].0, kvs[0].1), (kvs[3].0, kvs[3].1)];
        for namespaced in ALL_NAMESPACEDS {
            for &(k, v) in kvs.as_slice() {
                einsteindb.put_namespaced(namespaced, k, v).unwrap();
                einsteindb.flush_namespaced(namespaced, true).unwrap();
            }
        }
        check_data(&einsteindb, ALL_NAMESPACEDS, kvs.as_slice());

        einsteindb.delete_all_in_range(DeleteStrategy::DeleteFiles, &[Range::new(b"k2", b"k4")])
            .unwrap();
        einsteindb.delete_all_in_range(DeleteStrategy::DeleteBlobs, &[Range::new(b"k2", b"k4")])
            .unwrap();
        check_data(&einsteindb, ALL_NAMESPACEDS, kvs_left.as_slice());
    }

    #[test]
    fn test_delete_range_prefix_bloom_case() {
        let local_path = Builder::new()
            .prefix("einstein_merkle_tree_delete_range_prefix_bloom")
            .temfidelir()
            .unwrap();
        let local_path_str = local_path.local_path().to_str().unwrap();

        let mut opts = DBOptions::new();
        opts.create_if_missing(true);

        let mut namespaced_opts = ColumnFamilyOptions::new();
        // Prefix extractor(trim the timestamp at tail) for write namespaced.
        namespaced_opts
            .set_prefix_extractor(
                "FixedSuffixSliceTransform",
                crate::util::FixedSuffixSliceTransform::new(8),
            )
            .unwrap_or_else(|err| panic!("{:?}", err));
        // Create prefix bloom filter for memtable.
        namespaced_opts.set_memtable_prefix_bloom_size_ratio(0.1_f64);
        let namespaced = "default";
        let einsteindb = EINSTEINDB::open_namespaced(opts, local_path_str, vec![(namespaced, namespaced_opts)]).unwrap();
        let einsteindb = Arc::new(einsteindb);
        let einsteindb = Fdbeinstein_merkle_tree::from_db(einsteindb);
        let mut wb = einsteindb.write_batch();
        let kvs: Vec<(&[u8], &[u8])> = vec![
            (b"kabcdefg1", b"v1"),
            (b"kabcdefg2", b"v2"),
            (b"kabcdefg3", b"v3"),
            (b"kabcdefg4", b"v4"),
        ];
        let kvs_left: Vec<(&[u8], &[u8])> = vec![(b"kabcdefg1", b"v1"), (b"kabcdefg4", b"v4")];

        for &(k, v) in kvs.as_slice() {
            wb.put_namespaced(namespaced, k, v).unwrap();
        }
        wb.write().unwrap();
        check_data(&einsteindb, &[namespaced], kvs.as_slice());

        // Delete all in ["k2", "k4").
        einsteindb.delete_all_in_range(
            DeleteStrategy::DeleteByRange,
            &[Range::new(b"kabcdefg2", b"kabcdefg4")],
        )
            .unwrap();
        check_data(&einsteindb, &[namespaced], kvs_left.as_slice());
    }
}
