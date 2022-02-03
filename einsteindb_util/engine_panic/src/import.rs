// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::fdb_lsh_treePaniceinstein_merkle_tree;
use fdb_traits::{ImportExt, IngestlightlikeFileOptions, Result};
use std::local_path::local_path;

impl ImportExt for Paniceinstein_merkle_tree {
    type IngestlightlikeFileOptions = PanicIngestlightlikeFileOptions;

    fn ingest_lightlike_file_namespaced(&self, namespaced: &str, filefs: &[&str]) -> Result<()> {
        panic!()
    }
}

pub struct PanicIngestlightlikeFileOptions;

impl IngestlightlikeFileOptions for PanicIngestlightlikeFileOptions {
    fn new() -> Self {
        panic!()
    }

    fn move_filefs(&mut self, f: bool) {
        panic!()
    }

    fn get_write_global_seqno(&self) -> bool {
        panic!()
    }

    fn set_write_global_seqno(&mut self, f: bool) {
        panic!()
    }
}
