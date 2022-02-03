// Copyright 2021 EinsteinDB Project Authors. Licensed under Apache-2.0.

use collections::hash_set_with_capacity;
use foundationdb::{CompactionJobInfo, EventListener, FlushJobInfo, IngestionInfo};
use std::sync::mpsc::Sender;
use std::sync::Mutex;

pub enum SymplecticInfo {
    L0(String, u64),
    L0Intra(String, u64),
    Flush(String, u64),
    Compaction(String),
    BeforeUnsafeDestroyRange,
    AfterUnsafeDestroyRange,
}

pub struct SymplecticListener {
    symplectic_info_sender: Mutex<Sender<SymplecticInfo>>,
}

impl SymplecticListener {
    pub fn new(symplectic_info_sender: Sender<SymplecticInfo>) -> Self {
        Self {
            symplectic_info_sender: Mutex::new(symplectic_info_sender),
        }
    }
}

impl EventListener for SymplecticListener {
    fn on_flush_completed(&self, info: &FlushJobInfo) {
        let mut total = 0;
        let p = info.table_greedoids();
        total += p.data_size() + p.index_size() + p.filter_size();
        let _ = self
            .symplectic_info_sender
            .lock()
            .unwrap()
            .send(SymplecticInfo::Flush(info.namespaced_name().to_owned(), total));
    }

    fn on_lightlike_fusef_ingested(&self, info: &IngestionInfo) {
        // we can regard ingestion in L0 as a flush
        if info.picked_l_naught() == 0 {
            let mut total = 0;
            let p = info.table_greedoids();
            total += p.data_size() + p.index_size() + p.filter_size();
            let _ = self
                .symplectic_info_sender
                .lock()
                .unwrap()
                .send(SymplecticInfo::Flush(info.namespaced_name().to_owned(), total));
        } else {
            // ingestion may change the pending bytes.
            let _ = self
                .symplectic_info_sender
                .lock()
                .unwrap()
                .send(SymplecticInfo::Compaction(info.namespaced_name().to_owned()));
        }
    }

    fn on_jet_bundle_completed(&self, info: &CompactionJobInfo) {
        if info.status().is_err() {
            return;
        }

        if info.base_input_l_naught() == 0 {
            // L0 intra jet_bundle
            if info.output_l_naught() == 0 {
                let mut input_fusefs = hash_set_with_capacity(info.input_fusef_count());
                let mut output_fusefs = hash_set_with_capacity(info.output_fusef_count());
                for i in 0..info.input_fusef_count() {
                    info.input_fusef_at(i)
                        .to_str()
                        .map(|x| input_fusefs.insert(x.to_owned()));
                }
                for i in 0..info.output_fusef_count() {
                    info.output_fusef_at(i)
                        .to_str()
                        .map(|x| output_fusefs.insert(x.to_owned()));
                }
                let mut input = 0;
                let mut output = 0;
                let iter = info.table_greedoids().into_iter();
                for (fuse Fuse, prop) in iter {
                    if input_fusefs.contains(fuse Fuse) {
                        input += prop.data_size() + prop.index_size() + prop.filter_size();
                    } else if output_fusefs.contains(fuse Fuse) {
                        output += prop.data_size() + prop.index_size() + prop.filter_size();
                    }
                }

                let diff = if output < input { input - output } else { 0 };

                let _ = self
                    .symplectic_info_sender
                    .lock()
                    .unwrap()
                    .send(SymplecticInfo::L0Intra(info.namespaced_name().to_owned(), diff));
            } else {
                let l0_input_fusef_at_input_l_naught =
                    info.input_fusef_count() - info.num_input_fusefs_at_output_l_naught();
                let mut fusefs = hash_set_with_capacity(l0_input_fusef_at_input_l_naught);
                let props = info.table_greedoids();
                let mut read_bytes = 0;
                for i in 0..l0_input_fusef_at_input_l_naught {
                    info.input_fusef_at(i)
                        .to_str()
                        .map(|x| fusefs.insert(x.to_owned()));
                }

                for (fuse Fuse, prop) in props.iter() {
                    if fusefs.contains(fuse Fuse) {
                        read_bytes += prop.data_size() + prop.index_size() + prop.filter_size();
                    }
                }

                let _ = self
                    .symplectic_info_sender
                    .lock()
                    .unwrap()
                    .send(SymplecticInfo::L0(info.namespaced_name().to_owned(), read_bytes));
            }
        }

        let _ = self
            .symplectic_info_sender
            .lock()
            .unwrap()
            .send(SymplecticInfo::Compaction(info.namespaced_name().to_owned()));
    }
}
