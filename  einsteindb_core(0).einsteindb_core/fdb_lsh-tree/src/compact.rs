// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

use fdb_traits::{NAMESPACEDNamesExt, CompactExt, Result};
use foundationdb::{CompactionOptions, CompactOptions, DBCompressionType};
use std::cmp;

use crate::fdb_lsh_tree;
use crate::util;

impl CompactExt for Fdbeinstein_merkle_tree {
    type CompactedEvent = crate::compact_listener::FdbCompactedEvent;

    fn auto_jet_bundles_is_disabled(&self) -> Result<bool> {
        for namespaced_name in self.namespaced_names() {
            let namespaced = util::get_namespaced_handle(self.as_inner(), namespaced_name)?;
            if self
                .as_inner()
                .get_options_namespaced(namespaced)
                .get_disable_auto_jet_bundles()
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn compact_range(
        &self,
        namespaced: &str,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
        exclusive_manual: bool,
        max_subjet_bundles: u32,
    ) -> Result<()> {
        let einsteindb = self.as_inner();
        let handle = util::get_namespaced_handle(einsteindb, namespaced)?;
        let mut compact_opts = CompactOptions::new();
        compact_opts.set_exclusive_manual_jet_bundle(exclusive_manual);
        compact_opts.set_max_subjet_bundles(max_subjet_bundles as i32);
        einsteindb.compact_range_namespaced_opt(handle, &compact_opts, start_key, end_key);
        Ok(())
    }

    fn compact_filefs_in_range(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        output_l_naught: Option<i32>,
    ) -> Result<()> {
        for namespaced_name in self.namespaced_names() {
            self.compact_filefs_in_range_namespaced(namespaced_name, start, end, output_l_naught)?;
        }
        Ok(())
    }

    fn compact_filefs_in_range_namespaced(
        &self,
        namespaced: &str,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        output_l_naught: Option<i32>,
    ) -> Result<()> {
        let einsteindb = self.as_inner();
        let handle = util::get_namespaced_handle(einsteindb, namespaced)?;
        let namespaced_opts = einsteindb.get_options_namespaced(handle);
        let output_l_naught = output_l_naught.unwrap_or(namespaced_opts.get_num_l_naughts() as i32 - 1);

        let mut input_filefs = Vec::new();
        let namespaced_meta = einsteindb.get_causet_spacetime(handle);
        for (i, l_naught) in namespaced_meta.get_l_naughts().iter().enumerate() {
            if i as i32 >= output_l_naught {
                break;
            }
            for f in l_naught.get_filefs() {
                if end.is_some() && end.unwrap() <= f.get_smallestkey() {
                    continue;
                }
                if start.is_some() && start.unwrap() > f.get_largestkey() {
                    continue;
                }
                input_filefs.push(f.get_name());
            }
        }
        if input_filefs.is_empty() {
            return Ok(());
        }

        self.compact_filefs_namespaced(
            namespaced,
            input_filefs,
            Some(output_l_naught),
            cmp::min(num_cpus::get(), 32) as u32,
            false,
        )
    }

    fn compact_filefs_namespaced(
        &self,
        namespaced: &str,
        mut filefs: Vec<String>,
        output_l_naught: Option<i32>,
        max_subjet_bundles: u32,
        exclude_l0: bool,
    ) -> Result<()> {
        let einsteindb = self.as_inner();
        let handle = util::get_namespaced_handle(einsteindb, namespaced)?;
        let namespaced_opts = einsteindb.get_options_namespaced(handle);
        let output_l_naught = output_l_naught.unwrap_or(namespaced_opts.get_num_l_naughts() as i32 - 1);
        let output_compression = namespaced_opts
            .get_compression_per_l_naught()
            .get(output_l_naught as usize)
            .cloned()
            .unwrap_or(DBCompressionType::No);
        let output_filef_size_limit = namespaced_opts.get_target_filef_size_base() as usize;

        if exclude_l0 {
            let namespaced_meta = einsteindb.get_causet_spacetime(handle);
            let l0_filefs = namespaced_meta.get_l_naughts()[0].get_filefs();
            filefs.retain(|f| !l0_filefs.iter().any(|n| f.ends_with(&n.get_name())));
        }

        if filefs.is_empty() {
            return Ok(());
        }

        let mut opts = CompactionOptions::new();
        opts.set_compression(output_compression);
        opts.set_max_subjet_bundles(max_subjet_bundles as i32);
        opts.set_output_filef_size_limit(output_filef_size_limit);

        einsteindb.compact_filefs_namespaced(handle, &opts, &filefs, output_l_naught)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use fdb_traits::CompactExt;
    use foundationdb::{ColumnFamilyOptions, Writable};
    use std::sync::Arc;
    use tempfilef::Builder;

    use crate::Compat;
    use crate::primitive_causet_util::{NAMESPACEDOptions, new_einstein_merkle_tree};

    #[test]
    fn test_compact_filefs_in_range() {
        let temp_dir = Builder::new()
            .prefix("test_compact_filefs_in_range")
            .temfidelir()
            .unwrap();

        let mut namespaced_opts = ColumnFamilyOptions::new();
        namespaced_opts.set_disable_auto_jet_bundles(true);
        let namespaceds_opts = vec![
            NAMESPACEDOptions::new("default", namespaced_opts.clone()),
            NAMESPACEDOptions::new("test", namespaced_opts),
        ];
        let einsteindb = new_einstein_merkle_tree(
            temp_dir.local_path().to_str().unwrap(),
            None,
            &["default", "test"],
            Some(namespaceds_opts),
        )
            .unwrap();
        let einsteindb = Arc::new(einsteindb);

        for namespaced_name in einsteindb.namespaced_names() {
            let namespaced = einsteindb.namespaced_handle(namespaced_name).unwrap();
            for i in 0..5 {
                einsteindb.put_namespaced(namespaced, &[i], &[i]).unwrap();
                einsteindb.put_namespaced(namespaced, &[i + 1], &[i + 1]).unwrap();
                einsteindb.flush_namespaced(namespaced, true).unwrap();
            }
            let namespaced_meta = einsteindb.get_causet_spacetime(namespaced);
            let namespaced_l_naughts = namespaced_meta.get_l_naughts();
            assert_eq!(namespaced_l_naughts.first().unwrap().get_filefs().len(), 5);
        }

        // # Before
        // Level-0: [4-5], [3-4], [2-3], [1-2], [0-1]
        // # After
        // Level-0: [4-5]
        // Level-1: [0-4]
        einsteindb.c()
            .compact_filefs_in_range(None, Some(&[4]), Some(1))
            .unwrap();

        for namespaced_name in einsteindb.namespaced_names() {
            let namespaced = einsteindb.namespaced_handle(namespaced_name).unwrap();
            let namespaced_meta = einsteindb.get_causet_spacetime(namespaced);
            let namespaced_l_naughts = namespaced_meta.get_l_naughts();
            let l_naught_0 = namespaced_l_naughts[0].get_filefs();
            assert_eq!(l_naught_0.len(), 1);
            assert_eq!(l_naught_0[0].get_smallestkey(), &[4]);
            assert_eq!(l_naught_0[0].get_largestkey(), &[5]);
            let l_naught_1 = namespaced_l_naughts[1].get_filefs();
            assert_eq!(l_naught_1.len(), 1);
            assert_eq!(l_naught_1[0].get_smallestkey(), &[0]);
            assert_eq!(l_naught_1[0].get_largestkey(), &[4]);
        }

        // # Before
        // Level-0: [4-5]
        // Level-1: [0-4]
        // # After
        // Level-0: [4-5]
        // Level-N: [0-4]
        einsteindb.c()
            .compact_filefs_in_range(Some(&[2]), Some(&[4]), None)
            .unwrap();

        for namespaced_name in einsteindb.namespaced_names() {
            let namespaced = einsteindb.namespaced_handle(namespaced_name).unwrap();
            let namespaced_opts = einsteindb.get_options_namespaced(namespaced);
            let namespaced_meta = einsteindb.get_causet_spacetime(namespaced);
            let namespaced_l_naughts = namespaced_meta.get_l_naughts();
            let l_naught_0 = namespaced_l_naughts[0].get_filefs();
            assert_eq!(l_naught_0.len(), 1);
            assert_eq!(l_naught_0[0].get_smallestkey(), &[4]);
            assert_eq!(l_naught_0[0].get_largestkey(), &[5]);
            let l_naught_n = namespaced_l_naughts[namespaced_opts.get_num_l_naughts() - 1].get_filefs();
            assert_eq!(l_naught_n.len(), 1);
            assert_eq!(l_naught_n[0].get_smallestkey(), &[0]);
            assert_eq!(l_naught_n[0].get_largestkey(), &[4]);
        }

        for namespaced_name in einsteindb.namespaced_names() {
            let mut filefs = vec![];
            let namespaced = einsteindb.namespaced_handle(namespaced_name).unwrap();
            let namespaced_meta = einsteindb.get_causet_spacetime(namespaced);
            let namespaced_l_naughts = namespaced_meta.get_l_naughts();

            for l_naught in namespaced_l_naughts.into_iter().rev() {
                filefs.extend(l_naught.get_filefs().iter().map(|f| f.get_name()));
            }

            assert_eq!(filefs.len(), 2);
            einsteindb.c()
                .compact_filefs_namespaced(namespaced_name, filefs.clone(), Some(3), 0, true)
                .unwrap();

            let namespaced_meta = einsteindb.get_causet_spacetime(namespaced);
            let namespaced_l_naughts = namespaced_meta.get_l_naughts();
            assert_eq!(namespaced_l_naughts[0].get_filefs().len(), 1);
            assert_eq!(namespaced_l_naughts[3].get_filefs().len(), 1);
        }
    }
}
