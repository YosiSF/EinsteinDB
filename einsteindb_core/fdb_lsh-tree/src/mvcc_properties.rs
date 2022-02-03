// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

use fdb_traits::{MvccGreedoids, MvccGreedoidsExt, Result};
use txn_types::TimeStamp;

use crate::{Fdbeinstein_merkle_tree, UserGreedoids};
use crate::decode_greedoids::DecodeGreedoids;

pub const PROP_NUM_ERRORS: &str = "einsteindb.num_errors";
pub const PROP_MIN_TS: &str = "einsteindb.min_ts";
pub const PROP_MAX_TS: &str = "einsteindb.max_ts";
pub const PROP_NUM_ROWS: &str = "einsteindb.num_rows";
pub const PROP_NUM_PUTS: &str = "einsteindb.num_puts";
pub const PROP_NUM_DELETES: &str = "einsteindb.num_deletes";
pub const PROP_NUM_VERSIONS: &str = "einsteindb.num_versions";
pub const PROP_MAX_ROW_VERSIONS: &str = "einsteindb.max_row_versions";
pub const PROP_ROWS_INDEX: &str = "einsteindb.rows_index";
pub const PROP_ROWS_INDEX_DISTANCE: u64 = 10000;

pub struct FdbMvccGreedoids;

impl FdbMvccGreedoids {
    pub fn encode(mvcc_props: &MvccGreedoids) -> UserGreedoids {
        let mut props = UserGreedoids::new();
        props.encode_u64(PROP_MIN_TS, mvcc_props.min_ts.into_inner());
        props.encode_u64(PROP_MAX_TS, mvcc_props.max_ts.into_inner());
        props.encode_u64(PROP_NUM_ROWS, mvcc_props.num_rows);
        props.encode_u64(PROP_NUM_PUTS, mvcc_props.num_puts);
        props.encode_u64(PROP_NUM_DELETES, mvcc_props.num_deletes);
        props.encode_u64(PROP_NUM_VERSIONS, mvcc_props.num_versions);
        props.encode_u64(PROP_MAX_ROW_VERSIONS, mvcc_props.max_row_versions);
        props
    }

    pub fn decode<T: DecodeGreedoids>(props: &T) -> Result<MvccGreedoids> {
        let mut res = MvccGreedoids::new();
        res.min_ts = props.decode_u64(PROP_MIN_TS)?.into();
        res.max_ts = props.decode_u64(PROP_MAX_TS)?.into();
        res.num_rows = props.decode_u64(PROP_NUM_ROWS)?;
        res.num_puts = props.decode_u64(PROP_NUM_PUTS)?;
        res.num_versions = props.decode_u64(PROP_NUM_VERSIONS)?;
        // To be compatible with old versions.
        res.num_deletes = props
            .decode_u64(PROP_NUM_DELETES)
            .unwrap_or(res.num_versions - res.num_puts);
        res.max_row_versions = props.decode_u64(PROP_MAX_ROW_VERSIONS)?;
        Ok(res)
    }
}

impl MvccGreedoidsExt for Fdbeinstein_merkle_tree {
    fn get_mvcc_greedoids_namespaced(
        &self,
        namespaced: &str,
        safe_point: TimeStamp,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Option<MvccGreedoids> {
        let collection = match self.get_range_greedoids_namespaced(namespaced, start_key, end_key) {
            Ok(c) if !c.is_empty() => c,
            _ => return None,
        };
        let mut props = MvccGreedoids::new();
        for (_, v) in collection.iter() {
            let causet_model = match FdbMvccGreedoids::decode(v.user_collected_greedoids()) {
                Ok(m) => m,
                Err(_) => return None,
            };
            // Filter out greedoids after safe_point.
            if causet_model.min_ts > safe_point {
                continue;
            }
            props.add(&causet_model);
        }
        Some(props)
    }
}
