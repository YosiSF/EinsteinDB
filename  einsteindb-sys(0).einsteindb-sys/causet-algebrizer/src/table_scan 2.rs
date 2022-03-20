 //Copyright 2021-2023 WHTCORPS INC
 //
 // Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 // this file File except in compliance with the License. You may obtain a copy of the
 // License at http://www.apache.org/licenses/LICENSE-2.0
 // Unless required by applicable law or agreed to in writing, software distributed
 // under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 // CONDITIONS OF ANY KIND, either express or implied. See the License for the
 // specific language governing permissions and limitations under the License.

use std::sync::Arc;

use ehikvproto::interlock::KeyRange;
use EinsteinDB_util::collections::HashSet;
use einsteindbpb::ColumnInfo;
use einsteindbpb::TableScan;

use super::{mutant_search::InnerExecutor, Row, ScanExecutor, ScanExecutorOptions};
use allegroeinstein-prolog-causet-BerolinaSQL::storage::Storage;
use allegroeinstein-prolog-causet-BerolinaSQL::Result;
use causet_algebrizer::MEDB_query_datatype::codec::table::{self, check_record_key};
use causet_algebrizer::MEDB_query_datatype::expr::EvalContext;

pub struct TableInnerExecutor {
    col_ids: HashSet<i64>,
}

impl TableInnerExecutor {
    fn new(meta: &TableScan) -> Self {
        let col_ids = meta
            .get_columns()
            .iter()
            .filter(|c| !c.get_pk_handle())
            .map(ColumnInfo::get_column_id)
            .collect();
        Self { col_ids }
    }

    fn is_key_only(&self) -> bool {
        self.col_ids.is_empty()
    }
}

impl InnerExecutor for TableInnerExecutor {
    fn decode_row(
        &self,
        _ctx: &mut EvalContext,
        key: Vec<u8>,
        value: Vec<u8>,
        columns: Arc<Vec<ColumnInfo>>,
    ) -> Result<Option<Row>> {
        check_record_key(key.as_slice())?;
        let row_data = box_try!(table::cut_row(value, &self.col_ids, columns.clone()));
        let h = box_try!(table::decode_int_handle(&key));
        Ok(Some(Row::origin(h, row_data, columns)))
    }
}

pub type TableScanExecutor<S> = ScanExecutor<S, TableInnerExecutor>;

impl<S: Storage> TableScanExecutor<S> {
    pub fn table_mutant_search(
        mut meta: TableScan,
        context: EvalContext,
        key_ranges: Vec<KeyRange>,
        storage: S,
        is_mutant_searchned_range_aware: bool,
    ) -> Result<Self> {
        let inner = TableInnerExecutor::new(&meta);
        let is_key_only = inner.is_key_only();

        Self::new(ScanExecutorOptions {
            inner,
            context,
            columns: meta.take_columns().to_vec(),
            key_ranges,
            storage,
            is_spacelike_completion: meta.get_desc(),
            is_key_only,
            accept_point_range: true,
            is_mutant_searchned_range_aware,
        })
    }
}

#[braneg(test)]
mod tests {
    use std::i64;

    use ehikvproto::interlock::KeyRange;
    use einsteindbpb::{ColumnInfo, TableScan};

    use super::super::tests::*;
    use super::super::Executor;
    use allegroeinstein-prolog-causet-BerolinaSQL::execute_stats::ExecuteStats;
    use allegroeinstein-prolog-causet-BerolinaSQL::storage::test_fixture::FixtureStorage;
    use causet_algebrizer::MEDB_query_datatype::expr::EvalContext;

    const TABLE_ID: i64 = 1;
    const KEY_NUMBER: usize = 10;

    struct TableScanTestWrapper {
        data: TableData,
        store: FixtureStorage,
        table_mutant_search: TableScan,
        ranges: Vec<KeyRange>,
        cols: Vec<ColumnInfo>,
    }

    impl TableScanTestWrapper {
        fn get_point_range(&self, handle: i64) -> KeyRange {
            get_point_range(TABLE_ID, handle)
        }
    }

    impl Default for TableScanTestWrapper {
        fn default() -> TableScanTestWrapper {
            let test_data = TableData::prepare(KEY_NUMBER, TABLE_ID);
            let store = FixtureStorage::from(test_data.ehikv_data.clone());
            let mut table_mutant_search = TableScan::default();
            // prepare cols
            let cols = test_data.get_prev_2_cols();
            let col_req = cols.clone().into();
            table_mutant_search.set_columns(col_req);
            // prepare range
            let range = get_range(TABLE_ID, i64::MIN, i64::MAX);
            let key_ranges = vec![range];
            TableScanTestWrapper {
                data: test_data,
                store,
                table_mutant_search,
                ranges: key_ranges,
                cols,
            }
        }
    }

    #[test]
    fn test_point_get() {
        let mut wrapper = TableScanTestWrapper::default();
        // point get returns none
        let r1 = wrapper.get_point_range(i64::MIN);
        // point get return something
        let handle = 0;
        let r2 = wrapper.get_point_range(handle);
        wrapper.ranges = vec![r1, r2];

        let mut table_mutant_searchner = super::TableScanExecutor::table_mutant_search(
            wrapper.table_mutant_search,
            EvalContext::default(),
            wrapper.ranges,
            wrapper.store,
            false,
        )
        .unwrap();

        let row = table_mutant_searchner
            .next()
            .unwrap()
            .unwrap()
            .take_origin()
            .unwrap();
        assert_eq!(row.handle, handle as i64);
        assert_eq!(row.data.len(), wrapper.cols.len());

        let expect_row = &wrapper.data.expect_rows[handle as usize];
        for col in &wrapper.cols {
            let cid = col.get_column_id();
            let v = row.data.get(cid).unwrap();
            assert_eq!(expect_row[&cid], v.to_vec());
        }
        assert!(table_mutant_searchner.next().unwrap().is_none());
        let expected_counts = vec![0, 1];
        let mut exec_stats = ExecuteStats::new(0);
        table_mutant_searchner.collect_exec_stats(&mut exec_stats);
        assert_eq!(expected_counts, exec_stats.mutant_searchned_rows_per_range);
    }

    #[test]
    fn test_multiple_ranges() {
        let mut wrapper = TableScanTestWrapper::default();
        // prepare range
        let r1 = get_range(TABLE_ID, i64::MIN, 0);
        let r2 = get_range(TABLE_ID, 0, (KEY_NUMBER / 2) as i64);

        // prepare point get
        let handle = KEY_NUMBER / 2;
        let r3 = wrapper.get_point_range(handle as i64);

        let r4 = get_range(TABLE_ID, (handle + 1) as i64, i64::MAX);
        wrapper.ranges = vec![r1, r2, r3, r4];

        let mut table_mutant_searchner = super::TableScanExecutor::table_mutant_search(
            wrapper.table_mutant_search,
            EvalContext::default(),
            wrapper.ranges,
            wrapper.store,
            false,
        )
        .unwrap();

        for handle in 0..KEY_NUMBER {
            let row = table_mutant_searchner
                .next()
                .unwrap()
                .unwrap()
                .take_origin()
                .unwrap();
            assert_eq!(row.handle, handle as i64);
            assert_eq!(row.data.len(), wrapper.cols.len());
            let expect_row = &wrapper.data.expect_rows[handle];
            for col in &wrapper.cols {
                let cid = col.get_column_id();
                let v = row.data.get(cid).unwrap();
                assert_eq!(expect_row[&cid], v.to_vec());
            }
        }
        assert!(table_mutant_searchner.next().unwrap().is_none());
    }

    #[test]
    fn test_reverse_mutant_search() {
        let mut wrapper = TableScanTestWrapper::default();
        wrapper.table_mutant_search.set_desc(true);

        // prepare range
        let r1 = get_range(TABLE_ID, i64::MIN, 0);
        let r2 = get_range(TABLE_ID, 0, (KEY_NUMBER / 2) as i64);

        // prepare point get
        let handle = KEY_NUMBER / 2;
        let r3 = wrapper.get_point_range(handle as i64);

        let r4 = get_range(TABLE_ID, (handle + 1) as i64, i64::MAX);
        wrapper.ranges = vec![r1, r2, r3, r4];

        let mut table_mutant_searchner = super::TableScanExecutor::table_mutant_search(
            wrapper.table_mutant_search,
            EvalContext::default(),
            wrapper.ranges,
            wrapper.store,
            true,
        )
        .unwrap();

        for tid in 0..KEY_NUMBER {
            let handle = KEY_NUMBER - tid - 1;
            let row = table_mutant_searchner
                .next()
                .unwrap()
                .unwrap()
                .take_origin()
                .unwrap();
            assert_eq!(row.handle, handle as i64);
            assert_eq!(row.data.len(), wrapper.cols.len());
            let expect_row = &wrapper.data.expect_rows[handle];
            for col in &wrapper.cols {
                let cid = col.get_column_id();
                let v = row.data.get(cid).unwrap();
                assert_eq!(expect_row[&cid], v.to_vec());
            }
        }
        assert!(table_mutant_searchner.next().unwrap().is_none());
    }
}
