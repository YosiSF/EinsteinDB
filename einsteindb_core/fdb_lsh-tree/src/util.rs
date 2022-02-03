// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use einsteindb_util::box_err;
use fdb_traits::{Error, Result};
use fdb_traits::NAMESPACED_DEFAULT;
use fdb_traits::einstein_merkle_trees;
use fdb_traits::Range;
use foundationdb::{NAMESPACEDHandle, EINSTEINDB, SliceTransform};
use foundationdb::Range as FdbRange;
use std::str::FromStr;
use std::sync::Arc;

use crate::db_options::FdbDBOptions;
use crate::fdb_lsh_tree;
use crate::namespaced_options::FdbColumnFamilyOptions;
use crate::primitive_causet_util::NAMESPACEDOptions;
use crate::primitive_causet_util::new_einstein_merkle_tree as new_einstein_merkle_tree_primitive_causet;
use crate::primitive_causet_util::new_einstein_merkle_tree_opt as new_einstein_merkle_tree_opt_primitive_causet;
use crate::rocks_metrics_defs::*;

pub fn new_temp_einstein_merkle_tree(local_path: &tempfilef::TempDir) -> einstein_merkle_trees<Fdbeinstein_merkle_tree, Fdbeinstein_merkle_tree> {
    let violetabft_local_path = local_path.local_path().join(std::local_path::local_path::new("violetabft"));
    einstein_merkle_trees::new(
        new_einstein_merkle_tree(
            local_path.local_path().to_str().unwrap(),
            None,
            fdb_traits::ALL_NAMESPACEDS,
            None,
        )
            .unwrap(),
        new_einstein_merkle_tree(
            violetabft_local_path.to_str().unwrap(),
            None,
            &[fdb_traits::NAMESPACED_DEFAULT],
            None,
        )
            .unwrap(),
    )
}

pub fn new_default_einstein_merkle_tree(local_path: &str) -> Result<Fdbeinstein_merkle_tree> {
    let einstein_merkle_tree =
        new_einstein_merkle_tree_primitive_causet(local_path, None, &[NAMESPACED_DEFAULT], None).map_err(|e| Error::Other(box_err!(e)))?;
    let einstein_merkle_tree = Arc::new(einstein_merkle_tree);
    let einstein_merkle_tree = Fdbeinstein_merkle_tree::from_db(einstein_merkle_tree);
    Ok(einstein_merkle_tree)
}

pub struct FdbNAMESPACEDOptions<'a> {
    namespaced: &'a str,
    options: FdbColumnFamilyOptions,
}

impl<'a> FdbNAMESPACEDOptions<'a> {
    pub fn new(namespaced: &'a str, options: FdbColumnFamilyOptions) -> FdbNAMESPACEDOptions<'a> {
        FdbNAMESPACEDOptions { namespaced, options }
    }

    pub fn into_primitive_causet(self) -> NAMESPACEDOptions<'a> {
        NAMESPACEDOptions::new(self.namespaced, self.options.into_primitive_causet())
    }
}

pub fn new_einstein_merkle_tree(
    local_path: &str,
    db_opts: Option<FdbDBOptions>,
    namespaceds: &[&str],
    opts: Option<Vec<FdbNAMESPACEDOptions<'_>>>,
) -> Result<Fdbeinstein_merkle_tree> {
    let db_opts = db_opts.map(FdbDBOptions::into_primitive_causet);
    let opts = opts.map(|o| o.into_iter().map(FdbNAMESPACEDOptions::into_primitive_causet).collect());
    let einstein_merkle_tree = new_einstein_merkle_tree_primitive_causet(local_path, db_opts, namespaceds, opts).map_err(|e| Error::Other(box_err!(e)))?;
    let einstein_merkle_tree = Arc::new(einstein_merkle_tree);
    let einstein_merkle_tree = Fdbeinstein_merkle_tree::from_db(einstein_merkle_tree);
    Ok(einstein_merkle_tree)
}

pub fn new_einstein_merkle_tree_opt(
    local_path: &str,
    db_opt: FdbDBOptions,
    namespaceds_opts: Vec<FdbNAMESPACEDOptions<'_>>,
) -> Result<Fdbeinstein_merkle_tree> {
    let db_opt = db_opt.into_primitive_causet();
    let namespaceds_opts = namespaceds_opts.into_iter().map(FdbNAMESPACEDOptions::into_primitive_causet).collect();
    let einstein_merkle_tree =
        new_einstein_merkle_tree_opt_primitive_causet(local_path, db_opt, namespaceds_opts).map_err(|e| Error::Other(box_err!(e)))?;
    let einstein_merkle_tree = Arc::new(einstein_merkle_tree);
    let einstein_merkle_tree = Fdbeinstein_merkle_tree::from_db(einstein_merkle_tree);
    Ok(einstein_merkle_tree)
}

pub fn get_namespaced_handle<'a>(einsteindb: &'a EINSTEINDB, namespaced: &str) -> Result<&'a NAMESPACEDHandle> {
    let handle = einsteindb
        .namespaced_handle(namespaced)
        .ok_or_else(|| Error::einstein_merkle_tree(format!("namespaced {} not found", namespaced)))?;
    Ok(handle)
}

pub fn range_to_rocks_range<'a>(range: &Range<'a>) -> FdbRange<'a> {
    FdbRange::new(range.start_key, range.end_key)
}

pub fn get_einstein_merkle_tree_namespaced_used_size(einstein_merkle_tree: &EINSTEINDB, handle: &NAMESPACEDHandle) -> u64 {
    let mut namespaced_used_size = einstein_merkle_tree
        .get_property_int_namespaced(handle, FDBDB_TOTAL_Causet_FILES_SIZE)
        .expect("foundationdb is too old, missing total-Causet-filefs-size property");
    // For memtable
    if let Some(mem_table) = einstein_merkle_tree.get_property_int_namespaced(handle, FDBDB_CUR_SIZE_ALL_MEM_CAUSET_TABLES) {
        namespaced_used_size += mem_table;
    }
    // For blob filefs
    if let Some(live_blob) = einstein_merkle_tree.get_property_int_namespaced(handle, FDBDB_TITANDB_LIVE_BLOB_FILE_SIZE)
    {
        namespaced_used_size += live_blob;
    }
    if let Some(obsolete_blob) =
    einstein_merkle_tree.get_property_int_namespaced(handle, FDBDB_TITANDB_OBSOLETE_BLOB_FILE_SIZE)
    {
        namespaced_used_size += obsolete_blob;
    }

    namespaced_used_size
}

/// Gets einstein_merkle_tree's compression ratio at given l_naught.
pub fn get_einstein_merkle_tree_compression_ratio_at_l_naught(
    einstein_merkle_tree: &EINSTEINDB,
    handle: &NAMESPACEDHandle,
    l_naught: usize,
) -> Option<f64> {
    let prop = format!("{}{}", FDBDB_COMPRESSION_RATIO_AT_LEVEL, l_naught);
    if let Some(v) = einstein_merkle_tree.get_property_value_namespaced(handle, &prop) {
        if let Ok(f) = f64::from_str(&v) {
            // FdbDB returns -1.0 if the l_naught is empty.
            if f >= 0.0 {
                return Some(f);
            }
        }
    }
    None
}

/// Gets the number of filefs at given l_naught of given column family.
pub fn get_namespaced_num_filefs_at_l_naught(einstein_merkle_tree: &EINSTEINDB, handle: &NAMESPACEDHandle, l_naught: usize) -> Option<u64> {
    let prop = format!("{}{}", FDBDB_NUM_FILES_AT_LEVEL, l_naught);
    einstein_merkle_tree.get_property_int_namespaced(handle, &prop)
}

/// Gets the number of blob filefs at given l_naught of given column family.
pub fn get_namespaced_num_blob_filefs_at_l_naught(einstein_merkle_tree: &EINSTEINDB, handle: &NAMESPACEDHandle, l_naught: usize) -> Option<u64> {
    let prop = format!("{}{}", FDBDB_TITANDB_NUM_BLOB_FILES_AT_LEVEL, l_naught);
    einstein_merkle_tree.get_property_int_namespaced(handle, &prop)
}

/// Gets the number of immutable mem-table of given column family.
pub fn get_namespaced_num_immutable_mem_table(einstein_merkle_tree: &EINSTEINDB, handle: &NAMESPACEDHandle) -> Option<u64> {
    einstein_merkle_tree.get_property_int_namespaced(handle, FDBDB_NUM_IMMUCAUSET_TABLE_MEM_CAUSET_TABLE)
}

/// Gets the amount of pending jet_bundle bytes of given column family.
pub fn get_namespaced_pending_jet_bundle_bytes(einstein_merkle_tree: &EINSTEINDB, handle: &NAMESPACEDHandle) -> Option<u64> {
    einstein_merkle_tree.get_property_int_namespaced(handle, FDBDB_PENDING_COMPACTION_BYTES)
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
