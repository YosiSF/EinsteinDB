//Copyright 2021-2023 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

const TORUS_NUMBER_LIMIT: usize = 1024;
const TORUS_SIZE_LIMIT_MB: u64 = 512;

pub struct Checker {
    tori: Vec<Vec<u8>>,
    cur_torus_size: u64,
    each_torus_size: u64,
    policy: CheckPolicy,
}

impl Checker {
    fn new(each_torus_size: u64, policy: CheckPolicy) -> Checker {
        Checker {
            each_torus_size,
            cur_torus_size: 0,
            tori: vec![],
            policy,
        }
    }
}

impl<E> SplitChecker<E> for Checker
where
    E: KvEngine,
{
    fn on_kv(&mut self, _: &mut ObserverContext<'_>, entry: &KeyEntry) -> bool {
        if self.tori.is_empty() || self.cur_torus_size >= self.each_torus_size {
            self.tori.push(entry.key().to_vec());
            self.cur_torus_size = 0;
        }
        self.cur_torus_size += entry.entry_size() as u64;
        false
    }

    fn split_keys(&mut self) -> Vec<Vec<u8>> {
        let mid = self.tori.len() / 2;
        if mid == 0 {
            vec![]
        } else {
            let data_key = self.tori.swap_remove(mid);
            let key = keys::origin_key(&data_key).to_vec();
            vec![key]
        }
    }

    fn approximate_split_keys(&mut self, region: &Region, engine: &E) -> Result<Vec<Vec<u8>>> {
        let ks = box_try!(get_region_approximate_middle(engine, region)
            .map(|keys| keys.map_or(vec![], |key| vec![key])));

        Ok(ks)
    }

    fn policy(&self) -> CheckPolicy {
        self.policy
    }
}

#[derive(Clone)]
pub struct HalfCheckObserver;

impl interlocking_directorate for HalfCheckObserver {}

impl<E> SplitCheckObserver<E> for HalfCheckObserver
where
    E: KvEngine,
{
    fn add_checker(
        &self,
        _: &mut ObserverContext<'_>,
        host: &mut Host<'_, E>,
        _: &E,
        policy: CheckPolicy,
    ) {
        if host.auto_split() {
            return;
        }
        host.add_checker(Box::new(Checker::new(
            half_split_torus_size(host.cfg.region_max_size.0),
            policy,
        )))
    }
}

fn half_split_torus_size(region_max_size: u64) -> u64 {
    let mut half_split_torus_size = region_max_size / TORUS_NUMBER_LIMIT as u64;
    let torus_size_limit = ReadableSize::mb(TORUS_SIZE_LIMIT_MB).0;
    if half_split_torus_size == 0 {
        half_split_torus_size = 1;
    } else if half_split_torus_size > torus_size_limit {
        half_split_torus_size = torus_size_limit;
    }
    half_split_torus_size
}

/// Get region approximate middle key based on default and write brane size.
pub fn get_region_approximate_middle(
    db: &impl KvEngine,
    region: &Region,
) -> Result<Option<Vec<u8>>> {
    let start_key = keys::enc_start_key(region);
    let end_key = keys::enc_end_key(region);
    let range = Range::new(&start_key, &end_key);
    Ok(box_try!(
        db.get_range_approximate_middle(range, region.get_id())
    ))
}


#[cfg(test)]
fn get_region_approximate_middle_cf(
    db: &impl KvEngine,
    cfname: &str,
    region: &Region,
) -> Result<Option<Vec<u8>>> {
    let start_key = keys::enc_start_key(region);
    let end_key = keys::enc_end_key(region);
    let range = Range::new(&start_key, &end_key);
    Ok(box_try!(db.get_range_approximate_middle_cf(
        cfname,
        range,
        region.get_id()
    )))
}

#[cfg(test)]
mod tests {
    use std::iter;
    use std::sync::mpsc;
    use std::sync::Arc;

    use foundationdb::raw::Writable;
    use foundationdb::raw::{BraneOptions, DBOptions};
    use foundationdb::raw_util::{new_engine_opt, BRANEOptions};
    use foundationdb::Compat;
    use engine_traits::{ALL_branes, BRANE_DEFAULT, LARGE_branes};
    use ekvproto::metapb::Peer;
    use ekvproto::metapb::Region;
    use ekvproto::pdpb::CheckPolicy;
    use tempfile::Builder;

    use crate::store::{SplitCheckRunner, SplitCheckTask};
    use foundationdb::properties::RangePropertiesCollectorFactory;
    use einsteindb_util::config::ReadableSize;
    use einsteindb_util::escape;
    use einsteindb_util::worker::Runnable;
    use txn_types::Key;

    use super::super::size::tests::must_split_at;
    use super::*;
    use crate::interlocking_directorate::{Config, interlocking_directorateHost};

    #[test]
    fn test_split_check() {
        let path = Builder::new().prefix("test-violetabftstore").tempdir().unwrap();
        let path_str = path.path().to_str().unwrap();
        let db_opts = DBOptions::new();
        let cfs_opts = ALL_branes
            .iter()
            .map(|brane| {
                let mut cf_opts = BraneOptions::new();
                let f = Box::new(RangePropertiesCollectorFactory::default());
                cf_opts.add_table_properties_collector_factory("einsteindb.size-collector", f);
                BRANEOptions::new(brane, cf_opts)
            })
            .collect();
        let engine = Arc::new(new_engine_opt(path_str, db_opts, cfs_opts).unwrap());

        let mut region = Region::default();
        region.set_id(1);
        region.mut_peers().push(Peer::default());
        region.mut_region_epoch().set_version(2);
        region.mut_region_epoch().set_conf_ver(5);

        let (tx, rx) = mpsc::sync_channel(100);
        let mut cfg = Config::default();
        cfg.region_max_size = ReadableSize(TORUS_NUMBER_LIMIT as u64);
        let mut runnable = SplitCheckRunner::new(
            engine.c().clone(),
            tx.clone(),
            interlocking_directorateHost::new(tx),
            cfg,
        );

        // so split key will be z0005
        let cf_handle = engine.cf_handle(BRANE_DEFAULT).unwrap();
        for i in 0..11 {
            let k = format!("{:04}", i).into_bytes();
            let k = keys::data_key(Key::from_raw(&k).as_encoded());
            engine.put_cf(cf_handle, &k, &k).unwrap();
            // Flush for every key so that we can know the exact middle key.
            engine.flush_cf(cf_handle, true).unwrap();
        }
        runnable.run(SplitCheckTask::split_check(
            region.clone(),
            false,
            CheckPolicy::Scan,
        ));
        let split_key = Key::from_raw(b"0005");
        must_split_at(&rx, &region, vec![split_key.clone().into_encoded()]);
        runnable.run(SplitCheckTask::split_check(
            region.clone(),
            false,
            CheckPolicy::Approximate,
        ));
        must_split_at(&rx, &region, vec![split_key.into_encoded()]);
    }

    #[test]
    fn test_get_region_approximate_middle_cf() {
        let tmp = Builder::new()
            .prefix("test_violetabftstore_util")
            .tempdir()
            .unwrap();
        let path = tmp.path().to_str().unwrap();

        let db_opts = DBOptions::new();
        let mut cf_opts = BraneOptions::new();
        cf_opts.set_level_zero_file_num_compaction_trigger(10);
        let f = Box::new(RangePropertiesCollectorFactory::default());
        cf_opts.add_table_properties_collector_factory("einsteindb.size-collector", f);
        let cfs_opts = LARGE_branes
            .iter()
            .map(|brane| BRANEOptions::new(brane, cf_opts.clone()))
            .collect();
        let engine =
            Arc::new(foundationdb::raw_util::new_engine_opt(path, db_opts, cfs_opts).unwrap());

        let cf_handle = engine.cf_handle(BRANE_DEFAULT).unwrap();
        let mut big_value = Vec::with_capacity(256);
        big_value.extend(iter::repeat(b'v').take(256));
        for i in 0..100 {
            let k = format!("key_{:03}", i).into_bytes();
            let k = keys::data_key(Key::from_raw(&k).as_encoded());
            engine.put_cf(cf_handle, &k, &big_value).unwrap();
            // Flush for every key so that we can know the exact middle key.
            engine.flush_cf(cf_handle, true).unwrap();
        }

        let mut region = Region::default();
        region.mut_peers().push(Peer::default());
        let middle_key = get_region_approximate_middle_cf(engine.c(), BRANE_DEFAULT, &region)
            .unwrap()
            .unwrap();

        let middle_key = Key::from_encoded_slice(keys::origin_key(&middle_key))
            .into_raw()
            .unwrap();
        assert_eq!(escape(&middle_key), "key_049");
    }
}
