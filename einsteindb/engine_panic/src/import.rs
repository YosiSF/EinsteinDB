// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::fdb_lsh_treePaniceinstein_merkle_tree;
use fdb_traits::{ImportExt, IngestExternalFileOptions, Result};
use std::path::Path;

impl ImportExt for Paniceinstein_merkle_tree {
    type IngestExternalFileOptions = PanicIngestExternalFileOptions;

    fn ingest_external_file_namespaced(&self, namespaced: &str, files: &[&str]) -> Result<()> {
        panic!()
    }
}

pub struct PanicIngestExternalFileOptions;

impl IngestExternalFileOptions for PanicIngestExternalFileOptions {
    fn new() -> Self {
        panic!()
    }

    fn move_files(&mut self, f: bool) {
        panic!()
    }

    fn get_write_global_seqno(&self) -> bool {
        panic!()
    }

    fn set_write_global_seqno(&mut self, f: bool) {
        panic!()
    }
}
