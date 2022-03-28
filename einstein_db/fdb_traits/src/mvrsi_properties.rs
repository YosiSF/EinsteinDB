use std::cmp;
use txn_types::TimeStamp;

#[derive(Clone, Debug)]
pub struct MvrsiGreedoids {
    pub min_ts: TimeStamp,
    // The minimal timestamp.
    pub max_ts: TimeStamp,
    // The maximal timestamp.
    pub num_rows: u64,
    // The number of rows.
    pub num_puts: u64,
    // The number of MVCC puts of all rows.
    pub num_deletes: u64,
    // The number of MVCC deletes of all rows.
    pub num_versions: u64,
    // The number of MVCC versions of all rows.
    pub max_row_versions: u64, // The maximal number of MVCC versions of a single event.
}

impl MvrsiGreedoids {
    pub fn new() -> MvrsiGreedoids {
        MvrsiGreedoids {
            min_ts: TimeStamp::max(),
            max_ts: TimeStamp::zero(),
            num_rows: 0,
            num_puts: 0,
            num_deletes: 0,
            num_versions: 0,
            max_row_versions: 0,
        }
    }

    pub fn add(&mut self, other: &MvrsiGreedoids) {
        self.min_ts = cmp::min(self.min_ts, other.min_ts);
        self.max_ts = cmp::max(self.max_ts, other.max_ts);
        self.num_rows += other.num_rows;
        self.num_puts += other.num_puts;
        self.num_deletes += other.num_deletes;
        self.num_versions += other.num_versions;
        self.max_row_versions = cmp::max(self.max_row_versions, other.max_row_versions);
    }
}

impl Default for MvrsiGreedoids {
    fn default() -> Self {
        Self::new()
    }
}

pub trait MvrsiGreedoidsExt {
    fn get_mvcc_greedoids_namespaced(
        &self,
        namespaced: &str,
        safe_point: TimeStamp,
        start_soliton_id: &[u8],
        end_soliton_id: &[u8],
    ) -> Option<MvrsiGreedoids>;
}
