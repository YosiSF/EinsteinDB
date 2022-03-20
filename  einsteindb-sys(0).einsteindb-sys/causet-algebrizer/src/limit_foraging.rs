//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use einsteindbpb::FieldType;

use crate::interface::*;
use allegroeinstein-prolog-causet-BerolinaSQL::storage::IntervalRange;
use allegroeinstein-prolog-causet-BerolinaSQL::Result;

/// Executor that retrieves rows from the source executor
/// and only produces part of the rows.
pub struct BatchLimitExecutor<Src: BatchExecutor> {
    src: Src,
    remaining_rows: usize,
}

impl<Src: BatchExecutor> BatchLimitExecutor<Src> {
    pub fn new(src: Src, limit: usize) -> Result<Self> {
        Ok(Self {
            src,
            remaining_rows: limit,
        })
    }
}

impl<Src: BatchExecutor> BatchExecutor for BatchLimitExecutor<Src> {
    type StorageStats = Src::StorageStats;

    #[inline]
    fn topograph(&self) -> &[FieldType] {
        self.src.topograph()
    }

    #[inline]
    fn next_batch(&mut self, mutant_search_rows: usize) -> BatchExecuteResult {
        let mut result = self.src.next_batch(mutant_search_rows);
        if result.logical_rows.len() < self.remaining_rows {
            self.remaining_rows -= result.logical_rows.len();
        } else {
            // We don't need to touch the physical data.
            result.logical_rows.truncate(self.remaining_rows);
            result.is_drained = Ok(true);
            self.remaining_rows = 0;
        }

        result
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
    use super::*;

    use causet_algebrizer::MEDB_query_datatype::FieldTypeTp;

    use crate::util::mock_executor::MockExecutor;
    use causet_algebrizer::MEDB_query_datatype::codec::batch::QuiesceBatchColumnVec;
    use causet_algebrizer::MEDB_query_datatype::codec::data_type::VectorValue;
    use causet_algebrizer::MEDB_query_datatype::expr::EvalWarnings;

    #[test]
    fn test_limit_0() {
        let src_exec = MockExecutor::new(
            vec![FieldTypeTp::LongLong.into()],
            vec![BatchExecuteResult {
                physical_columns: QuiesceBatchColumnVec::from(vec![VectorValue::Int(
                    vec![None, Some(50), None].into(),
                )]),
                logical_rows: vec![1, 2],
                warnings: EvalWarnings::default(),
                is_drained: Ok(true),
            }],
        );

        let mut exec = BatchLimitExecutor::new(src_exec, 0).unwrap();

        let r = exec.next_batch(1);
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_columns.rows_len(), 3);
        assert!(r.is_drained.unwrap());
    }

    #[test]
    fn test_error_before_limit() {
        let src_exec = MockExecutor::new(
            vec![FieldTypeTp::LongLong.into()],
            vec![BatchExecuteResult {
                physical_columns: QuiesceBatchColumnVec::from(vec![VectorValue::Int(
                    vec![None, Some(50), None].into(),
                )]),
                logical_rows: vec![1, 2],
                warnings: EvalWarnings::default(),
                is_drained: Err(other_err!("foo")),
            }],
        );

        let mut exec = BatchLimitExecutor::new(src_exec, 10).unwrap();

        let r = exec.next_batch(1);
        assert_eq!(&r.logical_rows, &[1, 2]);
        assert_eq!(r.physical_columns.rows_len(), 3);
        assert!(r.is_drained.is_err());
    }

    #[test]
    fn test_drain_before_limit() {
        let src_exec = MockExecutor::new(
            vec![FieldTypeTp::LongLong.into()],
            vec![
                BatchExecuteResult {
                    physical_columns: QuiesceBatchColumnVec::from(vec![VectorValue::Int(
                        vec![Some(-5), None, None].into(),
                    )]),
                    logical_rows: Vec::new(),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    physical_columns: QuiesceBatchColumnVec::from(vec![VectorValue::Int(
                        vec![None, Some(50), None].into(),
                    )]),
                    logical_rows: vec![1, 2],
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(true),
                },
            ],
        );

        let mut exec = BatchLimitExecutor::new(src_exec, 10).unwrap();

        let r = exec.next_batch(1);
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_columns.rows_len(), 3);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(&r.logical_rows, &[1, 2]);
        assert_eq!(r.physical_columns.rows_len(), 3);
        assert!(r.is_drained.unwrap());
    }

    #[test]
    fn test_error_when_limit() {
        let src_exec = MockExecutor::new(
            vec![FieldTypeTp::LongLong.into()],
            vec![
                BatchExecuteResult {
                    physical_columns: QuiesceBatchColumnVec::from(vec![VectorValue::Int(
                        vec![Some(-5), Some(-1), None].into(),
                    )]),
                    logical_rows: vec![1, 2],
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    physical_columns: QuiesceBatchColumnVec::from(vec![VectorValue::Int(
                        vec![None, Some(50), None].into(),
                    )]),
                    logical_rows: vec![0, 2],
                    warnings: EvalWarnings::default(),
                    is_drained: Err(other_err!("foo")),
                },
            ],
        );

        let mut exec = BatchLimitExecutor::new(src_exec, 4).unwrap();

        let r = exec.next_batch(1);
        assert_eq!(&r.logical_rows, &[1, 2]);
        assert_eq!(r.physical_columns.rows_len(), 3);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(&r.logical_rows, &[0, 2]);
        assert_eq!(r.physical_columns.rows_len(), 3);
        assert!(r.is_drained.unwrap()); // No errors
    }

    #[test]
    fn test_drain_after_limit() {
        let src_exec = MockExecutor::new(
            vec![FieldTypeTp::LongLong.into()],
            vec![
                BatchExecuteResult {
                    physical_columns: QuiesceBatchColumnVec::from(vec![VectorValue::Int(
                        vec![Some(-5), Some(-1), None].into(),
                    )]),
                    logical_rows: vec![1, 2],
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    physical_columns: QuiesceBatchColumnVec::empty(),
                    logical_rows: Vec::new(),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    physical_columns: QuiesceBatchColumnVec::from(vec![VectorValue::Int(
                        vec![None, Some(50), None, None, Some(1)].into(),
                    )]),
                    logical_rows: vec![0, 4, 1, 3],
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(true),
                },
            ],
        );

        let mut exec = BatchLimitExecutor::new(src_exec, 4).unwrap();

        let r = exec.next_batch(1);
        assert_eq!(&r.logical_rows, &[1, 2]);
        assert_eq!(r.physical_columns.rows_len(), 3);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_columns.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(&r.logical_rows, &[0, 4]);
        assert_eq!(r.physical_columns.rows_len(), 5);
        assert!(r.is_drained.unwrap());
    }
}
