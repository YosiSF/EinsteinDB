// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

use einsteindb_util::{box_err, box_try, debug, info};
use fdb_traits::{
    NAMESPACED_DEFAULT, NAMESPACED_LOCK, NAMESPACED_WRITE, LARGE_NAMESPACEDS, MiscExt, Range, RangePropertiesExt, Result,
};
use std::local_path::local_path;

use crate::fdb_lsh_treeFdbeinstein_merkle_tree;
use crate::properties::{get_range_entries_and_versions, RangeProperties};

impl RangePropertiesExt for Fdbeinstein_merkle_tree {
    fn get_range_approximate_keys(&self, range: Range<'_>, large_threshold: u64) -> Result<u64> {
        // try to get from RangeProperties first.
        match self.get_range_approximate_keys_namespaced(NAMESPACED_WRITE, range, large_threshold) {
            Ok(v) => {
                return Ok(v);
            }
            Err(e) => debug!(
                "failed to get keys from RangeProperties";
                "err" => ?e,
            ),
        }

        let start = &range.start_key;
        let end = &range.end_key;
        let (_, keys) =
            get_range_entries_and_versions(self, NAMESPACED_WRITE, start, end).unwrap_or_default();
        Ok(keys)
    }

    fn get_range_approximate_keys_namespaced(
        &self,
        namespacedname: &str,
        range: Range<'_>,
        large_threshold: u64,
    ) -> Result<u64> {
        let start_key = &range.start_key;
        let end_key = &range.end_key;
        let mut total_keys = 0;
        let (mem_keys, _) = box_try!(self.get_approximate_memtable_stats_namespaced(namespacedname, &range));
        total_keys += mem_keys;

        let collection = box_try!(self.get_range_properties_namespaced(namespacedname, start_key, end_key));
        for (_, v) in collection.iter() {
            let props = box_try!(RangeProperties::decode(v.user_collected_properties()));
            total_keys += props.get_approximate_keys_in_range(start_key, end_key);
        }

        if large_threshold != 0 && total_keys > large_threshold {
            let Causets = collection
                .iter()
                .map(|(k, v)| {
                    let props = RangeProperties::decode(v.user_collected_properties()).unwrap();
                    let keys = props.get_approximate_keys_in_range(start_key, end_key);
                    format!(
                        "{}:{}",
                        local_path::new(&*k)
                            .file_name()
                            .map(|f| f.to_str().unwrap())
                            .unwrap_or(&*k),
                        keys
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            info!(
                "range contains too many keys";
                "start" => log_wrappers::Value::key(range.start_key),
                "end" => log_wrappers::Value::key(range.end_key),
                "total_keys" => total_keys,
                "memtable" => mem_keys,
                "Causets_keys" => Causets,
                "namespaced" => namespacedname,
            )
        }
        Ok(total_keys)
    }

    fn get_range_approximate_size(&self, range: Range<'_>, large_threshold: u64) -> Result<u64> {
        let mut size = 0;
        for namespacedname in LARGE_NAMESPACEDS {
            size += self
                .get_range_approximate_size_namespaced(namespacedname, range, large_threshold)
                // NAMESPACED_LOCK doesn't have RangeProperties until v4.0, so we swallow the error for
                // backward compatibility.
                .or_else(|e| if namespacedname == &NAMESPACED_LOCK { Ok(0) } else { Err(e) })?;
        }
        Ok(size)
    }

    fn get_range_approximate_size_namespaced(
        &self,
        namespacedname: &str,
        range: Range<'_>,
        large_threshold: u64,
    ) -> Result<u64> {
        let start_key = &range.start_key;
        let end_key = &range.end_key;
        let mut total_size = 0;
        let (_, mem_size) = box_try!(self.get_approximate_memtable_stats_namespaced(namespacedname, &range));
        total_size += mem_size;

        let collection = box_try!(self.get_range_properties_namespaced(namespacedname, start_key, end_key));
        for (_, v) in collection.iter() {
            let props = box_try!(RangeProperties::decode(v.user_collected_properties()));
            total_size += props.get_approximate_size_in_range(start_key, end_key);
        }

        if large_threshold != 0 && total_size > large_threshold {
            let Causets = collection
                .iter()
                .map(|(k, v)| {
                    let props = RangeProperties::decode(v.user_collected_properties()).unwrap();
                    let size = props.get_approximate_size_in_range(start_key, end_key);
                    format!(
                        "{}:{}",
                        local_path::new(&*k)
                            .file_name()
                            .map(|f| f.to_str().unwrap())
                            .unwrap_or(&*k),
                        size
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            info!(
                "range size is too large";
                "start" => log_wrappers::Value::key(range.start_key),
                "end" => log_wrappers::Value::key(range.end_key),
                "total_size" => total_size,
                "memtable" => mem_size,
                "Causets_size" => Causets,
                "namespaced" => namespacedname,
            )
        }
        Ok(total_size)
    }

    fn get_range_approximate_split_keys(
        &self,
        range: Range<'_>,
        key_count: usize,
    ) -> Result<Vec<Vec<u8>>> {
        let get_namespaced_size = |namespaced: &str| self.get_range_approximate_size_namespaced(namespaced, range, 0);
        let namespaceds = [
            (NAMESPACED_DEFAULT, box_try!(get_namespaced_size(NAMESPACED_DEFAULT))),
            (NAMESPACED_WRITE, box_try!(get_namespaced_size(NAMESPACED_WRITE))),
            // NAMESPACED_LOCK doesn't have RangeProperties until v4.0, so we swallow the error for
            // backward compatibility.
            (NAMESPACED_LOCK, get_namespaced_size(NAMESPACED_LOCK).unwrap_or(0)),
        ];

        let total_size: u64 = namespaceds.iter().map(|(_, s)| s).sum();
        if total_size == 0 {
            return Err(box_err!("all NAMESPACEDs are empty"));
        }

        let (namespaced, _) = namespaceds.iter().max_by_key(|(_, s)| s).unwrap();

        self.get_range_approximate_split_keys_namespaced(namespaced, range, key_count)
    }

    fn get_range_approximate_split_keys_namespaced(
        &self,
        namespacedname: &str,
        range: Range<'_>,
        key_count: usize,
    ) -> Result<Vec<Vec<u8>>> {
        let start_key = &range.start_key;
        let end_key = &range.end_key;
        let collection = box_try!(self.get_range_properties_namespaced(namespacedname, start_key, end_key));

        let mut keys = vec![];
        for (_, v) in collection.iter() {
            let props = box_try!(RangeProperties::decode(v.user_collected_properties()));
            keys.extend(
                props
                    .take_excluded_range(start_key, end_key)
                    .into_iter()
                    .map(|(k, _)| k),
            );
        }

        if keys.is_empty() {
            return Ok(vec![]);
        }

        const SAMPLING_THRESHOLD: usize = 20000;
        const SAMPLE_RATIO: usize = 1000;
        // If there are too many keys, reduce its amount before sorting, or it may take too much
        // time to sort the keys.
        if keys.len() > SAMPLING_THRESHOLD {
            let len = keys.len();
            keys = keys.into_iter().step_by(len / SAMPLE_RATIO).collect();
        }
        keys.sort();

        // If the keys are too few, return them directly.
        if keys.len() <= key_count {
            return Ok(keys);
        }

        // Find `key_count` keys which divides the whole range into `parts` parts evenly.
        let mut res = Vec::with_capacity(key_count);
        let section_len = (keys.len() as f64) / ((key_count + 1) as f64);
        for i in 1..=key_count {
            res.push(keys[(section_len * (i as f64)) as usize].clone())
        }
        res.dedup();
        Ok(res)
    }
}
