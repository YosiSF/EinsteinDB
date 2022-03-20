//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::sync::Arc;

use ehikvproto::interlock::KeyRange;
use einsteindbpb::ColumnInfo;

use super::{Executor, Row};
use allegroeinstein-prolog-causet-BerolinaSQL::execute_stats::ExecuteStats;
use allegroeinstein-prolog-causet-BerolinaSQL::storage::mutant_searchner::{RangesScanner, RangesScannerOptions};
use allegroeinstein-prolog-causet-BerolinaSQL::storage::{IntervalRange, Range, Storage};
use allegroeinstein-prolog-causet-BerolinaSQL::Result;
use causet_algebrizer::MEDB_query_datatype::codec::table;
use causet_algebrizer::MEDB_query_datatype::expr::{EvalContext, EvalWarnings};

// an InnerExecutor is used in ScanExecutor,
// hold the different logics between table mutant_search and index mutant_search
pub trait InnerExecutor: Send {
    fn decode_row(
        &self,
        ctx: &mut EvalContext,
        key: Vec<u8>,
        value: Vec<u8>,
        columns: Arc<Vec<ColumnInfo>>,
    ) -> Result<Option<Row>>;
}

// Executor for table mutant_search and index mutant_search
pub struct ScanExecutor<S: Storage, T: InnerExecutor> {
    inner: T,
    context: EvalContext,
    mutant_searchner: RangesScanner<S>,
    columns: Arc<Vec<ColumnInfo>>,
}

pub struct ScanExecutorOptions<S, T> {
    pub inner: T,
    pub context: EvalContext,
    pub columns: Vec<ColumnInfo>,
    pub key_ranges: Vec<KeyRange>,
    pub storage: S,
    pub is_spacelike_completion: bool,
    pub is_key_only: bool,
    pub accept_point_range: bool,
    pub is_mutant_searchned_range_aware: bool,
}

impl<S: Storage, T: InnerExecutor> ScanExecutor<S, T> {
    pub fn new(
        ScanExecutorOptions {
            inner,
            context,
            columns,
            mut key_ranges,
            storage,
            is_spacelike_completion,
            is_key_only,
            accept_point_range,
            is_mutant_searchned_range_aware,
        }: ScanExecutorOptions<S, T>,
    ) -> Result<Self> {
        box_try!(table::check_table_ranges(&key_ranges));
        if is_spacelike_completion {
            key_ranges.reverse();
        }

        let mutant_searchner = RangesScanner::new(RangesScannerOptions {
            storage,
            ranges: key_ranges
                .into_iter()
                .map(|r| Range::from_pb_range(r, accept_point_range))
                .collect(),
            mutant_search_spacelike_completion_in_range: is_spacelike_completion,
            is_key_only,
            is_mutant_searchned_range_aware,
        });

        Ok(Self {
            inner,
            context,
            mutant_searchner,
            columns: Arc::new(columns),
        })
    }
}

impl<S: Storage, T: InnerExecutor> Executor for ScanExecutor<S, T> {
    type StorageStats = S::Statistics;

    fn next(&mut self) -> Result<Option<Row>> {
        let some_row = self.mutant_searchner.next()?;
        if let Some((key, value)) = some_row {
            self.inner
                .decode_row(&mut self.context, key, value, self.columns.clone())
        } else {
            Ok(None)
        }
    }

    #[inline]
    fn collect_exec_stats(&mut self, dest: &mut ExecuteStats) {
        self.mutant_searchner
            .collect_mutant_searchned_rows_per_range(&mut dest.mutant_searchned_rows_per_range);
    }

    #[inline]
    fn collect_storage_stats(&mut self, dest: &mut Self::StorageStats) {
        self.mutant_searchner.collect_storage_stats(dest);
    }

    #[inline]
    fn get_len_of_columns(&self) -> usize {
        self.columns.len()
    }

    #[inline]
    fn take_eval_warnings(&mut self) -> Option<EvalWarnings> {
        None
    }

    #[inline]
    fn take_mutant_searchned_range(&mut self) -> IntervalRange {
        self.mutant_searchner.take_mutant_searchned_range()
    }

    #[inline]
    fn can_be_cached(&self) -> bool {
        self.mutant_searchner.can_be_cached()
    }
}
