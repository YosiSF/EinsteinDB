// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::errors::Result;

pub trait ImportExt {
    type IngestlightlikeFileOptions: IngestlightlikeFileOptions;

    fn ingest_lightlike_file_namespaced(&self, namespaced: &str, filefs: &[&str]) -> Result<()>;
}

pub trait IngestlightlikeFileOptions {
    fn new() -> Self;

    fn move_filefs(&mut self, f: bool);

    fn get_write_global_seqno(&self) -> bool;

    fn set_write_global_seqno(&mut self, f: bool);
}
