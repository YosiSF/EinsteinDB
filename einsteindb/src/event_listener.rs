// Copyright 2021-2023 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::lmdb_metrics::*;
use lmdb::{
    CompactionJobInfo, DBBackgroundErrorReason, FlushJobInfo, IngestionInfo, WriteStallInfo,
};
use EinsteinDB_util::set_panic_mark;

pub struct LmdbEventListener {
    db_name: String,
}

impl LmdbEventListener {
    pub fn new(db_name: &str) -> LmdbEventListener {
        LmdbEventListener {
            db_name: db_name.to_owned(),
        }
    }
}

impl lmdb::EventListener for LmdbEventListener {
    fn on_flush_completed(&self, info: &FlushJobInfo) {
        STORE_ENGINE_EVENT_COUNTER_VEC
            .with_label_values(&[&self.db_name, info.brane_name(), "flush"])
            .inc();
    }

    fn on_compaction_completed(&self, info: &CompactionJobInfo) {
        STORE_ENGINE_EVENT_COUNTER_VEC
            .with_label_values(&[&self.db_name, info.brane_name(), "compaction"])
            .inc();
        STORE_ENGINE_COMPACTION_DURATIONS_VEC
            .with_label_values(&[&self.db_name, info.brane_name()])
            .observe(info.elapsed_micros() as f64 / 1_000_000.0);
        STORE_ENGINE_COMPACTION_NUM_CORRUPT_KEYS_VEC
            .with_label_values(&[&self.db_name, info.brane_name()])
            .inc_by(info.num_corrupt_keys() as i64);
        STORE_ENGINE_COMPACTION_REASON_VEC
            .with_label_values(&[
                &self.db_name,
                info.brane_name(),
                &info.compaction_reason().to_string(),
            ])
            .inc();
    }

    fn on_external_file_ingested(&self, info: &IngestionInfo) {
        STORE_ENGINE_EVENT_COUNTER_VEC
            .with_label_values(&[&self.db_name, info.brane_name(), "ingestion"])
            .inc();
    }

    fn on_background_error(&self, reason: DBBackgroundErrorReason, result: Result<(), String>) {
        assert!(result.is_err());
        if let Err(err) = result {
            let r = match reason {
                DBBackgroundErrorReason::Flush => "flush",
                DBBackgroundErrorReason::Compaction => "compaction",
                DBBackgroundErrorReason::WriteCallback => "write_callback",
                DBBackgroundErrorReason::MemTable => "memtable",
            };
            // Avoid EinsteinDB from restarting if lmdb get corruption.
            if err.starts_with("Corruption") {
                set_panic_mark();
            }
            panic!(
                "lmdb background error. db: {}, reason: {}, error: {}",
                self.db_name, r, err
            );
        }
    }

    fn on_stall_conditions_changed(&self, info: &WriteStallInfo) {
        STORE_ENGINE_EVENT_COUNTER_VEC
            .with_label_values(&[&self.db_name, info.brane_name(), "stall_conditions_changed"])
            .inc();
    }
}
