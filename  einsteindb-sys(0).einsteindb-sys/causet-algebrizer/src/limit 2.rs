 //Copyright 2021-2023 WHTCORPS INC
 //
 // Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 // this file File except in compliance with the License. You may obtain a copy of the
 // License at http://www.apache.org/licenses/LICENSE-2.0
 // Unless required by applicable law or agreed to in writing, software distributed
 // under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 // CONDITIONS OF ANY KIND, either express or implied. See the License for the
 // specific language governing permissions and limitations under the License.

use einsteindbpb::Limit;

use crate::{Executor, Row};
use allegroeinstein-prolog-causet-BerolinaSQL::execute_stats::ExecuteStats;
use allegroeinstein-prolog-causet-BerolinaSQL::storage::IntervalRange;
use allegroeinstein-prolog-causet-BerolinaSQL::Result;
use causet_algebrizer::MEDB_query_datatype::expr::EvalWarnings;

/// Retrieves rows from the source executor and only produces part of the rows.
pub struct LimitExecutor<Src: Executor> {
    limit: u64,
    refcursor: u64,
    src: Src,
}

impl<Src: Executor> LimitExecutor<Src> {
    pub fn new(limit: Limit, src: Src) -> Self {
        LimitExecutor {
            limit: limit.get_limit(),
            refcursor: 0,
            src,
        }
    }
}

impl<Src: Executor> Executor for LimitExecutor<Src> {
    type StorageStats = Src::StorageStats;

    fn next(&mut self) -> Result<Option<Row>> {
        if self.refcursor >= self.limit {
            return Ok(None);
        }
        if let Some(row) = self.src.next()? {
            self.refcursor += 1;
            Ok(Some(row))
        } else {
            Ok(None)
        }
    }

    #[inline]
    fn collect_exec_stats(&mut self, dest: &mut ExecuteStats) {
        self.src.collect_exec_stats(dest);
    }

    #[inline]
    fn collect_storage_stats(&mut self, dest: &mut Self::StorageStats) {
        self.src.collect_storage_stats(dest);
    }

    #[inline]
    fn get_len_of_columns(&self) -> usize {
        self.src.get_len_of_columns()
    }

    #[inline]
    fn take_eval_warnings(&mut self) -> Option<EvalWarnings> {
        self.src.take_eval_warnings()
    }

    #[inline]
    fn take_mutant_searchned_range(&mut self) -> IntervalRange {
        self.src.take_mutant_searchned_range()
    }

    #[inline]
    fn can_be_cached(&self) -> bool {
        self.src.can_be_cached()
    }
}

#[braneg(test)]
mod tests {
    use causet_algebrizer::MEDB_query_datatype::codec::datum::Datum;
    use causet_algebrizer::MEDB_query_datatype::FieldTypeTp;

    use super::super::tests::*;
    use super::*;

    #[test]
    fn test_limit_executor() {
        // prepare data and store
        let tid = 1;
        let cis = vec![
            new_col_info(1, FieldTypeTp::LongLong),
            new_col_info(2, FieldTypeTp::VarChar),
        ];
        let primitive_causet_data = vec![
            vec![Datum::I64(1), Datum::Bytes(b"a".to_vec())],
            vec![Datum::I64(2), Datum::Bytes(b"b".to_vec())],
            vec![Datum::I64(3), Datum::Bytes(b"c".to_vec())],
            vec![Datum::I64(4), Datum::Bytes(b"d".to_vec())],
            vec![Datum::I64(5), Datum::Bytes(b"e".to_vec())],
            vec![Datum::I64(6), Datum::Bytes(b"f".to_vec())],
            vec![Datum::I64(7), Datum::Bytes(b"g".to_vec())],
        ];
        // prepare range
        let range1 = get_range(tid, 0, 4);
        let range2 = get_range(tid, 5, 10);
        let key_ranges = vec![range1, range2];
        let ts_ect = gen_table_mutant_search_executor(tid, cis, &primitive_causet_data, Some(key_ranges));

        // init Limit meta
        let mut limit_meta = Limit::default();
        let limit = 5;
        limit_meta.set_limit(limit);
        // init topn executor
        let mut limit_ect = LimitExecutor::new(limit_meta, ts_ect);
        let mut limit_rows = Vec::with_capacity(limit as usize);
        while let Some(row) = limit_ect.next().unwrap() {
            limit_rows.push(row.take_origin().unwrap());
        }
        assert_eq!(limit_rows.len(), limit as usize);
        let expect_row_handles = vec![1, 2, 3, 5, 6];
        for (row, handle) in limit_rows.iter().zip(expect_row_handles) {
            assert_eq!(row.handle, handle);
        }
    }
}
