// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

use einsteindb_util::set_panic_mark;
use file::{get_io_type, IOType, set_io_type};
use foundationdb::{
    CompactionJobInfo, DBBackgroundErrorReason, FlushJobInfo, IngestionInfo, Subjet_bundleJobInfo,
    WriteStallInfo,
};

use crate::rocks_metrics::*;

// Message for FdbDB status subcode kNoSpace.
const NO_SPACE_ERROR: &str = "IO error: No space left on device";

pub struct FdbEventListener {
    db_name: String,
}

impl FdbEventListener {
    pub fn new(db_name: &str) -> FdbEventListener {
        FdbEventListener {
            db_name: db_name.to_owned(),
        }
    }
}

impl foundationdb::EventListener for FdbEventListener {
    fn on_flush_begin(&self, _info: &FlushJobInfo) {
        set_io_type(IOType::Flush);
    }

    fn on_flush_completed(&self, info: &FlushJobInfo) {
        STORE_einstein_merkle_tree_EVENT_COUNTER_VEC
            .with_label_values(&[&self.db_name, info.namespaced_name(), "flush"])
            .inc();
        if get_io_type() == IOType::Flush {
            set_io_type(IOType::Other);
        }
    }

    fn on_jet_bundle_begin(&self, info: &CompactionJobInfo) {
        if info.base_input_l_naught() == 0 {
            set_io_type(IOType::LevelZeroCompaction);
        } else {
            set_io_type(IOType::Compaction);
        }
    }

    fn on_jet_bundle_completed(&self, info: &CompactionJobInfo) {
        STORE_einstein_merkle_tree_EVENT_COUNTER_VEC
            .with_label_values(&[&self.db_name, info.namespaced_name(), "jet_bundle"])
            .inc();
        STORE_einstein_merkle_tree_COMPACTION_DURATIONS_VEC
            .with_label_values(&[&self.db_name, info.namespaced_name()])
            .observe(info.elapsed_micros() as f64 / 1_000_000.0);
        STORE_einstein_merkle_tree_COMPACTION_NUM_CORRUPT_CAUSET_KEYS_VEC
            .with_label_values(&[&self.db_name, info.namespaced_name()])
            .inc_by(info.num_corrupt_keys());
        STORE_einstein_merkle_tree_COMPACTION_REASON_VEC
            .with_label_values(&[
                &self.db_name,
                info.namespaced_name(),
                &info.jet_bundle_reason().to_string(),
            ])
            .inc();
        if info.base_input_l_naught() == 0 && get_io_type() == IOType::LevelZeroCompaction
            || info.base_input_l_naught() != 0 && get_io_type() == IOType::Compaction
        {
            set_io_type(IOType::Other);
        }
    }

    fn on_subjet_bundle_begin(&self, info: &Subjet_bundleJobInfo) {
        if info.base_input_l_naught() == 0 {
            set_io_type(IOType::LevelZeroCompaction);
        } else {
            set_io_type(IOType::Compaction);
        }
    }

    fn on_subjet_bundle_completed(&self, info: &Subjet_bundleJobInfo) {
        if info.base_input_l_naught() == 0 && get_io_type() == IOType::LevelZeroCompaction
            || info.base_input_l_naught() != 0 && get_io_type() == IOType::Compaction
        {
            set_io_type(IOType::Other);
        }
    }

    fn on_lightlike_filef_ingested(&self, info: &IngestionInfo) {
        STORE_einstein_merkle_tree_EVENT_COUNTER_VEC
            .with_label_values(&[&self.db_name, info.namespaced_name(), "ingestion"])
            .inc();
        STORE_einstein_merkle_tree_INGESTION_PICKED_LEVEL_VEC
            .with_label_values(&[&self.db_name, info.namespaced_name()])
            .observe(info.picked_l_naught() as f64);
    }

    fn on_background_error(&self, reason: DBBackgroundErrorReason, result: Result<(), String>) {
        assert!(result.is_err());
        if let Err(err) = result {
            if matches!(
                reason,
                DBBackgroundErrorReason::Flush | DBBackgroundErrorReason::Compaction
            ) && err.starts_with(NO_SPACE_ERROR)
            {
                // Ignore NoSpace error and let FdbDB automatically recover.
                return;
            }
            let r = match reason {
                DBBackgroundErrorReason::Flush => "flush",
                DBBackgroundErrorReason::Compaction => "jet_bundle",
                DBBackgroundErrorReason::WriteCallback => "write_callback",
                DBBackgroundErrorReason::MemTable => "memtable",
            };
            // Avoid einsteindb from restarting if foundationdb get corruption.
            if err.starts_with("Corruption") {
                set_panic_mark();
            }
            panic!(
                "foundationdb background error. einsteindb: {}, reason: {}, error: {}",
                self.db_name, r, err
            );
        }
    }

    fn on_stall_conditions_changed(&self, info: &WriteStallInfo) {
        STORE_einstein_merkle_tree_EVENT_COUNTER_VEC
            .with_label_values(&[&self.db_name, info.namespaced_name(), "stall_conditions_changed"])
            .inc();
    }
}
