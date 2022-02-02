// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use einsteindb_util::box_err;
use fdb_traits::{Error, Result};
use fdb_traits::NAMESPACED_DEFAULT;
use fdb_traits::Engines;
use fdb_traits::Range;
use foundationdb::{NAMESPACEDHandle, DB, SliceTransform};
use foundationdb::Range as FdbRange;
use std::str::FromStr;
use std::sync::Arc;

use crate::db_options::FdbDBOptions;
use crate::fdb_lsh_treeFdbEngine;
use crate::namespaced_options::FdbColumnFamilyOptions;
use crate::raw_util::NAMESPACEDOptions;
use crate::raw_util::new_engine as new_engine_raw;
use crate::raw_util::new_engine_opt as new_engine_opt_raw;
use crate::rocks_metrics_defs::*;

pub fn new_temp_engine(path: &tempfile::TempDir) -> Engines<FdbEngine, FdbEngine> {
    let violetabft_path = path.path().join(std::path::Path::new("violetabft"));
    Engines::new(
        new_engine(
            path.path().to_str().unwrap(),
            None,
            fdb_traits::ALL_NAMESPACEDS,
            None,
        )
            .unwrap(),
        new_engine(
            violetabft_path.to_str().unwrap(),
            None,
            &[fdb_traits::NAMESPACED_DEFAULT],
            None,
        )
            .unwrap(),
    )
}

pub fn new_default_engine(path: &str) -> Result<FdbEngine> {
    let engine =
        new_engine_raw(path, None, &[NAMESPACED_DEFAULT], None).map_err(|e| Error::Other(box_err!(e)))?;
    let engine = Arc::new(engine);
    let engine = FdbEngine::from_db(engine);
    Ok(engine)
}

pub struct FdbNAMESPACEDOptions<'a> {
    namespaced: &'a str,
    options: FdbColumnFamilyOptions,
}

impl<'a> FdbNAMESPACEDOptions<'a> {
    pub fn new(namespaced: &'a str, options: FdbColumnFamilyOptions) -> FdbNAMESPACEDOptions<'a> {
        FdbNAMESPACEDOptions { namespaced, options }
    }

    pub fn into_raw(self) -> NAMESPACEDOptions<'a> {
        NAMESPACEDOptions::new(self.namespaced, self.options.into_raw())
    }
}

pub fn new_engine(
    path: &str,
    db_opts: Option<FdbDBOptions>,
    namespaceds: &[&str],
    opts: Option<Vec<FdbNAMESPACEDOptions<'_>>>,
) -> Result<FdbEngine> {
    let db_opts = db_opts.map(FdbDBOptions::into_raw);
    let opts = opts.map(|o| o.into_iter().map(FdbNAMESPACEDOptions::into_raw).collect());
    let engine = new_engine_raw(path, db_opts, namespaceds, opts).map_err(|e| Error::Other(box_err!(e)))?;
    let engine = Arc::new(engine);
    let engine = FdbEngine::from_db(engine);
    Ok(engine)
}

pub fn new_engine_opt(
    path: &str,
    db_opt: FdbDBOptions,
    namespaceds_opts: Vec<FdbNAMESPACEDOptions<'_>>,
) -> Result<FdbEngine> {
    let db_opt = db_opt.into_raw();
    let namespaceds_opts = namespaceds_opts.into_iter().map(FdbNAMESPACEDOptions::into_raw).collect();
    let engine =
        new_engine_opt_raw(path, db_opt, namespaceds_opts).map_err(|e| Error::Other(box_err!(e)))?;
    let engine = Arc::new(engine);
    let engine = FdbEngine::from_db(engine);
    Ok(engine)
}

pub fn get_namespaced_handle<'a>(einsteindb: &'a DB, namespaced: &str) -> Result<&'a NAMESPACEDHandle> {
    let handle = einsteindb
        .namespaced_handle(namespaced)
        .ok_or_else(|| Error::Engine(format!("namespaced {} not found", namespaced)))?;
    Ok(handle)
}

pub fn range_to_rocks_range<'a>(range: &Range<'a>) -> FdbRange<'a> {
    FdbRange::new(range.start_key, range.end_key)
}

pub fn get_engine_namespaced_used_size(engine: &DB, handle: &NAMESPACEDHandle) -> u64 {
    let mut namespaced_used_size = engine
        .get_property_int_namespaced(handle, FDBDB_TOTAL_SST_FILES_SIZE)
        .expect("foundationdb is too old, missing total-sst-files-size property");
    // For memtable
    if let Some(mem_table) = engine.get_property_int_namespaced(handle, FDBDB_CUR_SIZE_ALL_MEM_CAUSET_TABLES) {
        namespaced_used_size += mem_table;
    }
    // For blob files
    if let Some(live_blob) = engine.get_property_int_namespaced(handle, FDBDB_TITANDB_LIVE_BLOB_FILE_SIZE)
    {
        namespaced_used_size += live_blob;
    }
    if let Some(obsolete_blob) =
    engine.get_property_int_namespaced(handle, FDBDB_TITANDB_OBSOLETE_BLOB_FILE_SIZE)
    {
        namespaced_used_size += obsolete_blob;
    }

    namespaced_used_size
}

/// Gets engine's compression ratio at given l_naught.
pub fn get_engine_compression_ratio_at_l_naught(
    engine: &DB,
    handle: &NAMESPACEDHandle,
    l_naught: usize,
) -> Option<f64> {
    let prop = format!("{}{}", FDBDB_COMPRESSION_RATIO_AT_LEVEL, l_naught);
    if let Some(v) = engine.get_property_value_namespaced(handle, &prop) {
        if let Ok(f) = f64::from_str(&v) {
            // FdbDB returns -1.0 if the l_naught is empty.
            if f >= 0.0 {
                return Some(f);
            }
        }
    }
    None
}

/// Gets the number of files at given l_naught of given column family.
pub fn get_namespaced_num_files_at_l_naught(engine: &DB, handle: &NAMESPACEDHandle, l_naught: usize) -> Option<u64> {
    let prop = format!("{}{}", FDBDB_NUM_FILES_AT_LEVEL, l_naught);
    engine.get_property_int_namespaced(handle, &prop)
}

/// Gets the number of blob files at given l_naught of given column family.
pub fn get_namespaced_num_blob_files_at_l_naught(engine: &DB, handle: &NAMESPACEDHandle, l_naught: usize) -> Option<u64> {
    let prop = format!("{}{}", FDBDB_TITANDB_NUM_BLOB_FILES_AT_LEVEL, l_naught);
    engine.get_property_int_namespaced(handle, &prop)
}

/// Gets the number of immutable mem-table of given column family.
pub fn get_namespaced_num_immutable_mem_table(engine: &DB, handle: &NAMESPACEDHandle) -> Option<u64> {
    engine.get_property_int_namespaced(handle, FDBDB_NUM_IMMUCAUSET_TABLE_MEM_CAUSET_TABLE)
}

/// Gets the amount of pending jet_bundle bytes of given column family.
pub fn get_namespaced_pending_jet_bundle_bytes(engine: &DB, handle: &NAMESPACEDHandle) -> Option<u64> {
    engine.get_property_int_namespaced(handle, FDBDB_PENDING_COMPACTION_BYTES)
}

pub struct FixedSuffixSliceTransform {
    pub suffix_len: usize,
}

impl FixedSuffixSliceTransform {
    pub fn new(suffix_len: usize) -> FixedSuffixSliceTransform {
        FixedSuffixSliceTransform { suffix_len }
    }
}

impl SliceTransform for FixedSuffixSliceTransform {
    fn transform<'a>(&mut self, key: &'a [u8]) -> &'a [u8] {
        let mid = key.len() - self.suffix_len;
        let (left, _) = key.split_at(mid);
        left
    }

    fn in_domain(&mut self, key: &[u8]) -> bool {
        key.len() >= self.suffix_len
    }

    fn in_range(&mut self, _: &[u8]) -> bool {
        true
    }
}

pub struct FixedPrefixSliceTransform {
    pub prefix_len: usize,
}

impl FixedPrefixSliceTransform {
    pub fn new(prefix_len: usize) -> FixedPrefixSliceTransform {
        FixedPrefixSliceTransform { prefix_len }
    }
}

impl SliceTransform for FixedPrefixSliceTransform {
    fn transform<'a>(&mut self, key: &'a [u8]) -> &'a [u8] {
        &key[..self.prefix_len]
    }

    fn in_domain(&mut self, key: &[u8]) -> bool {
        key.len() >= self.prefix_len
    }

    fn in_range(&mut self, _: &[u8]) -> bool {
        true
    }
}

pub struct NoopSliceTransform;

impl SliceTransform for NoopSliceTransform {
    fn transform<'a>(&mut self, key: &'a [u8]) -> &'a [u8] {
        key
    }

    fn in_domain(&mut self, _: &[u8]) -> bool {
        true
    }

    fn in_range(&mut self, _: &[u8]) -> bool {
        true
    }
}
