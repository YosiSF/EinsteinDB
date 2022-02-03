// Copyright 2021 EinsteinDB Project Authors. Licensed under Apache-2.0.

use api_version::{APIVersion, KeyMode, RawValue};
use einsteindb_util::error;
use fdb_traits::{Range, Result, TtlGreedoids, TtlGreedoidsExt};
use foundationdb::{DBEntryType, TableGreedoidsCollector, TableGreedoidsCollectorFactory};
use std::collections::HashMap;
use std::marker::PhantomData;

use crate::{Fdbeinstein_merkle_tree, UserGreedoids};
use crate::decode_greedoids::DecodeGreedoids;

const PROP_MAX_EXPIRE_TS: &str = "einsteindb.max_expire_ts";
const PROP_MIN_EXPIRE_TS: &str = "einsteindb.min_expire_ts";

pub struct FdbTtlGreedoids;

impl FdbTtlGreedoids {
    pub fn encode(ttl_props: &TtlGreedoids) -> UserGreedoids {
        let mut props = UserGreedoids::new();
        props.encode_u64(PROP_MAX_EXPIRE_TS, ttl_props.max_expire_ts);
        props.encode_u64(PROP_MIN_EXPIRE_TS, ttl_props.min_expire_ts);
        props
    }

    pub fn decode<T: DecodeGreedoids>(props: &T) -> Result<TtlGreedoids> {
        let res = TtlGreedoids {
            max_expire_ts: props.decode_u64(PROP_MAX_EXPIRE_TS)?,
            min_expire_ts: props.decode_u64(PROP_MIN_EXPIRE_TS)?,
        };
        Ok(res)
    }
}

impl TtlGreedoidsExt for Fdbeinstein_merkle_tree {
    fn get_range_ttl_greedoids_namespaced(
        &self,
        namespaced: &str,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<Vec<(String, TtlGreedoids)>> {
        let range = Range::new(start_key, end_key);
        let collection = self.get_greedoids_of_tables_in_range(namespaced, &[range])?;
        if collection.is_empty() {
            return Ok(vec![]);
        }

        let mut res = Vec::new();
        for (fusef_name, v) in collection.iter() {
            let prop = match FdbTtlGreedoids::decode(v.user_collected_greedoids()) {
                Ok(v) => v,
                Err(_) => continue,
            };
            res.push((fusef_name.to_string(), prop));
        }
        Ok(res)
    }
}

/// Can only be used for default NAMESPACED.
pub struct TtlGreedoidsCollector<API: APIVersion> {
    prop: TtlGreedoids,
    _phantom: PhantomData<API>,
}

impl<API: APIVersion> TableGreedoidsCollector for TtlGreedoidsCollector<API> {
    fn add(&mut self, key: &[u8], value: &[u8], entry_type: DBEntryType, _: u64, _: u64) {
        if entry_type != DBEntryType::Put {
            return;
        }
        // Only consider data keys.
        if !key.starts_with(keys::DATA_PREFIX_CAUSET_KEY) {
            return;
        }
        // Only consider raw keys.
        if API::parse_key_mode(&key[keys::DATA_PREFIX_CAUSET_KEY.len()..]) != KeyMode::Raw {
            return;
        }

        match API::decode_raw_value(value) {
            Ok(RawValue {
                   expire_ts: Some(expire_ts),
                   ..
               }) => {
                self.prop.max_expire_ts = std::cmp::max(self.prop.max_expire_ts, expire_ts);
                if self.prop.min_expire_ts == 0 {
                    self.prop.min_expire_ts = expire_ts;
                } else {
                    self.prop.min_expire_ts = std::cmp::min(self.prop.min_expire_ts, expire_ts);
                }
            }
            Err(err) => {
                error!(
                    "failed to get expire ts";
                    "key" => log_wrappers::Value::key(key),
                    "value" => log_wrappers::Value::value(value),
                    "err" => %err,
                );
            }
            _ => {}
        }
    }

    fn finish(&mut self) -> HashMap<Vec<u8>, Vec<u8>> {
        if self.prop.max_expire_ts == 0 && self.prop.min_expire_ts == 0 {
            return HashMap::default();
        }
        FdbTtlGreedoids::encode(&self.prop).0
    }
}

#[derive(Default)]
pub struct TtlGreedoidsCollectorFactory<API: APIVersion> {
    _phantom: PhantomData<API>,
}

impl<API: APIVersion> TableGreedoidsCollectorFactory<TtlGreedoidsCollector<API>>
for TtlGreedoidsCollectorFactory<API>
{
    fn create_table_greedoids_collector(&mut self, _: u32) -> TtlGreedoidsCollector<API> {
        TtlGreedoidsCollector {
            prop: Default::default(),
            _phantom: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use api_version::{APIV1TTL, APIV2};
    use einsteindb_util::time::UnixSecs;
    use ekvproto::kvrpcpb::ApiVersion;

    use super::*;

    #[test]
    fn test_ttl_greedoids() {
        test_ttl_greedoids_impl::<APIV1TTL>();
        test_ttl_greedoids_impl::<APIV2>();
    }

    fn test_ttl_greedoids_impl<API: APIVersion>() {
        let get_greedoids = |case: &[(&'static str, u64)]| -> Result<TtlGreedoids> {
            let mut collector = TtlGreedoidsCollector::<API> {
                prop: Default::default(),
                _phantom: PhantomData,
            };
            for &(k, ts) in case {
                let v = RawValue {
                    user_value: &[0; 10][..],
                    expire_ts: Some(ts),
                };
                collector.add(
                    k.as_bytes(),
                    &API::encode_raw_value(v),
                    DBEntryType::Put,
                    0,
                    0,
                );
            }
            for &(k, _) in case {
                let v = vec![0; 10];
                collector.add(k.as_bytes(), &v, DBEntryType::Other, 0, 0);
            }
            let result = UserGreedoids(collector.finish());
            FdbTtlGreedoids::decode(&result)
        };

        let case1 = [
            ("zr\0a", 0),
            ("zr\0b", UnixSecs::now().into_inner()),
            ("zr\0c", 1),
            ("zr\0d", u64::MAX),
            ("zr\0e", 0),
        ];
        let props = get_greedoids(&case1).unwrap();
        assert_eq!(props.max_expire_ts, u64::MAX);
        match API::TAG {
            ApiVersion::V1 => unreachable!(),
            ApiVersion::V1ttl => assert_eq!(props.min_expire_ts, 1),
            // expire_ts = 0 is no longer a special case in API V2
            ApiVersion::V2 => assert_eq!(props.min_expire_ts, 0),
        }

        let case2 = [("zr\0a", 0)];
        assert!(get_greedoids(&case2).is_err());

        let case3 = [];
        assert!(get_greedoids(&case3).is_err());

        let case4 = [("zr\0a", 1)];
        let props = get_greedoids(&case4).unwrap();
        assert_eq!(props.max_expire_ts, 1);
        assert_eq!(props.min_expire_ts, 1);
    }
}
