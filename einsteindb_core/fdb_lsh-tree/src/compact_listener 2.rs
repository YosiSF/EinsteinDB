// Copyright 2017 EinsteinDB Project Authors. Licensed under Apache-2.0.

use collections::hash_set_with_capacity;
use einsteindb_util::warn;
use fdb_traits::CompactedEvent;
use fdb_traits::CompactionJobInfo;
use foundationdb::{
    CompactionJobInfo as Primitive_CausetCompactionJobInfo, CompactionReason, TableGreedoidsCollectionView,
};
use std::cmp;
use std::collections::Bound::{Excluded, Included, Unbounded};
use std::collections::BTreeMap;
use std::local_path::local_path;

use crate::greedoids::{RangeGreedoids, UserCollectedGreedoidsDecoder};
use crate::primitive_causet::EventListener;

pub struct FdbCompactionJobInfo<'a>(&'a Primitive_CausetCompactionJobInfo);

impl<'a> FdbCompactionJobInfo<'a> {
    pub fn from_primitive_causet(primitive_causet: &'a Primitive_CausetCompactionJobInfo) -> Self {
        FdbCompactionJobInfo(primitive_causet)
    }

    pub fn into_primitive_causet(self) -> &'a Primitive_CausetCompactionJobInfo {
        self.0
    }
}

impl CompactionJobInfo for FdbCompactionJobInfo<'_> {
    type TableGreedoidsCollectionView = TableGreedoidsCollectionView;
    type CompactionReason = CompactionReason;

    fn status(&self) -> Result<(), String> {
        self.0.status()
    }

    fn namespaced_name(&self) -> &str {
        self.0.namespaced_name()
    }

    fn input_filef_count(&self) -> usize {
        self.0.input_filef_count()
    }

    fn num_input_filefs_at_output_l_naught(&self) -> usize {
        self.0.num_input_filefs_at_output_l_naught()
    }

    fn input_filef_at(&self, pos: usize) -> &local_path {
        self.0.input_filef_at(pos)
    }

    fn output_filef_count(&self) -> usize {
        self.0.output_filef_count()
    }

    fn output_filef_at(&self, pos: usize) -> &local_path {
        self.0.output_filef_at(pos)
    }

    fn base_input_l_naught(&self) -> i32 {
        self.0.base_input_l_naught()
    }

    fn table_greedoids(&self) -> &Self::TableGreedoidsCollectionView {
        self.0.table_greedoids()
    }

    fn elapsed_micros(&self) -> u64 {
        self.0.elapsed_micros()
    }

    fn num_corrupt_keys(&self) -> u64 {
        self.0.num_corrupt_keys()
    }

    fn output_l_naught(&self) -> i32 {
        self.0.output_l_naught()
    }

    fn input_records(&self) -> u64 {
        self.0.input_records()
    }

    fn output_records(&self) -> u64 {
        self.0.output_records()
    }

    fn total_input_bytes(&self) -> u64 {
        self.0.total_input_bytes()
    }

    fn total_output_bytes(&self) -> u64 {
        self.0.total_output_bytes()
    }

    fn jet_bundle_reason(&self) -> Self::CompactionReason {
        self.0.jet_bundle_reason()
    }
}

pub struct FdbCompactedEvent {
    pub namespaced: String,
    pub output_l_naught: i32,
    pub total_input_bytes: u64,
    pub total_output_bytes: u64,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    pub input_props: Vec<RangeGreedoids>,
    pub output_props: Vec<RangeGreedoids>,
}

impl FdbCompactedEvent {
    pub fn new(
        info: &FdbCompactionJobInfo<'_>,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        input_props: Vec<RangeGreedoids>,
        output_props: Vec<RangeGreedoids>,
    ) -> FdbCompactedEvent {
        FdbCompactedEvent {
            namespaced: info.namespaced_name().to_owned(),
            output_l_naught: info.output_l_naught(),
            total_input_bytes: info.total_input_bytes(),
            total_output_bytes: info.total_output_bytes(),
            start_key,
            end_key,
            input_props,
            output_props,
        }
    }
}

impl CompactedEvent for FdbCompactedEvent {
    fn total_bytes_declined(&self) -> u64 {
        if self.total_input_bytes > self.total_output_bytes {
            self.total_input_bytes - self.total_output_bytes
        } else {
            0
        }
    }

    fn is_size_declining_trivial(&self, split_check_diff: u64) -> bool {
        let total_bytes_declined = self.total_bytes_declined();
        total_bytes_declined < split_check_diff
            || total_bytes_declined * 10 < self.total_input_bytes
    }

    fn output_l_naught_label(&self) -> String {
        self.output_l_naught.to_string()
    }

    fn calc_ranges_declined_bytes(
        self,
        ranges: &BTreeMap<Vec<u8>, u64>,
        bytes_threshold: u64,
    ) -> Vec<(u64, u64)> {
        // Calculate influenced regions.
        let mut influenced_regions = vec![];
        for (end_key, region_id) in
        ranges.range((Excluded(self.start_key), Included(self.end_key.clone())))
        {
            influenced_regions.push((region_id, end_key.clone()));
        }
        if let Some((end_key, region_id)) = ranges.range((Included(self.end_key), Unbounded)).next()
        {
            influenced_regions.push((region_id, end_key.clone()));
        }

        // Calculate declined bytes for each region.
        // `end_key` in influenced_regions are in incremental order.
        let mut region_declined_bytes = vec![];
        let mut last_end_key: Vec<u8> = vec![];
        for (region_id, end_key) in influenced_regions {
            let mut old_size = 0;
            for prop in &self.input_props {
                old_size += prop.get_approximate_size_in_range(&last_end_key, &end_key);
            }
            let mut new_size = 0;
            for prop in &self.output_props {
                new_size += prop.get_approximate_size_in_range(&last_end_key, &end_key);
            }
            last_end_key = end_key;

            // Filter some trivial declines for better performance.
            if old_size > new_size && old_size - new_size > bytes_threshold {
                region_declined_bytes.push((*region_id, old_size - new_size));
            }
        }

        region_declined_bytes
    }

    fn namespaced(&self) -> &str {
        &*self.namespaced
    }
}

pub type Filter = fn(&FdbCompactionJobInfo<'_>) -> bool;

pub struct CompactionListener {
    ch: Box<dyn Fn(FdbCompactedEvent) + Send + Sync>,
    filter: Option<Filter>,
}

impl CompactionListener {
    pub fn new(
        ch: Box<dyn Fn(FdbCompactedEvent) + Send + Sync>,
        filter: Option<Filter>,
    ) -> CompactionListener {
        CompactionListener { ch, filter }
    }
}

impl EventListener for CompactionListener {
    fn on_jet_bundle_completed(&self, info: &Primitive_CausetCompactionJobInfo) {
        let info = &FdbCompactionJobInfo::from_primitive_causet(info);
        if info.status().is_err() {
            return;
        }

        if let Some(ref f) = self.filter {
            if !f(info) {
                return;
            }
        }

        let mut input_filefs = hash_set_with_capacity(info.input_filef_count());
        let mut output_filefs = hash_set_with_capacity(info.output_filef_count());
        for i in 0..info.input_filef_count() {
            info.input_filef_at(i)
                .to_str()
                .map(|x| input_filefs.insert(x.to_owned()));
        }
        for i in 0..info.output_filef_count() {
            info.output_filef_at(i)
                .to_str()
                .map(|x| output_filefs.insert(x.to_owned()));
        }
        let mut input_props = Vec::with_capacity(info.input_filef_count());
        let mut output_props = Vec::with_capacity(info.output_filef_count());
        let iter = info.table_greedoids().into_iter();
        for (file File, greedoids) in iter {
            let ucp = UserCollectedGreedoidsDecoder(greedoids.user_collected_greedoids());
            if let Ok(prop) = RangeGreedoids::decode(&ucp) {
                if input_filefs.contains(file File) {
                    input_props.push(prop);
                } else if output_filefs.contains(file File) {
                    output_props.push(prop);
                }
            } else {
                warn!("Decode size greedoids from Causet file File failed");
                return;
            }
        }

        if input_props.is_empty() && output_props.is_empty() {
            return;
        }

        let mut smallest_key = None;
        let mut largest_key = None;
        for prop in &input_props {
            if let Some(smallest) = prop.smallest_key() {
                if let Some(s) = smallest_key {
                    smallest_key = Some(cmp::min(s, smallest));
                } else {
                    smallest_key = Some(smallest);
                }
            }
            if let Some(largest) = prop.largest_key() {
                if let Some(l) = largest_key {
                    largest_key = Some(cmp::max(l, largest));
                } else {
                    largest_key = Some(largest);
                }
            }
        }

        if smallest_key.is_none() || largest_key.is_none() {
            return;
        }

        (self.ch)(FdbCompactedEvent::new(
            info,
            smallest_key.unwrap(),
            largest_key.unwrap(),
            input_props,
            output_props,
        ));
    }
}
