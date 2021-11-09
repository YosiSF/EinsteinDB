//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::sync::Arc;

use ekvproto::interlock::KeyRange;
use causet_algebrizer::MilevaDB_query_datatype::EvalType;
use fidelpb::ColumnInfo;
use fidelpb::FieldType;
use fidelpb::IndexScan;

use super::util::scan_executor::*;
use crate::interface::*;
use codec::prelude::NumberDecoder;
use allegroeinstein-prolog-causet-sql::storage::{IntervalRange, Storage};
use allegroeinstein-prolog-causet-sql::Result;
use causet_algebrizer::MilevaDB_query_datatype::codec::batch::{QuiesceBatchColumn, QuiesceBatchColumnVec};
use causet_algebrizer::MilevaDB_query_datatype::codec::table::{check_index_key, MAX_OLD_ENCODED_VALUE_LEN};
use causet_algebrizer::MilevaDB_query_datatype::codec::{datum, table};
use causet_algebrizer::MilevaDB_query_datatype::expr::{EvalConfig, EvalContext};

pub struct BatchIndexScanExecutor<S: Storage>(ScanExecutor<S, IndexScanExecutorImpl>);


impl BatchIndexScanExecutor<Box<dyn Storage<Statistics = ()>>> {
    /// Checks whether this executor can be used.
    #[inline]
    pub fn check_supported(descriptor: &IndexScan) -> Result<()> {
        check_columns_info_supported(descriptor.get_columns())
    }
}

impl<S: Storage> BatchIndexScanExecutor<S> {
    pub fn new(
        storage: S,
        config: Arc<EvalConfig>,
        columns_info: Vec<ColumnInfo>,
        key_ranges: Vec<KeyRange>,
        is_backward: bool,
        unique: bool,
    ) -> Result<Self> {

        // forbidden soon.
        let decode_handle = columns_info.last().map_or(false, |ci| ci.get_pk_handle());
        let schema: Vec<_> = columns_info
            .iter()
            .map(|ci| field_type_from_column_info(&ci))
            .collect();

        let mut columns_id_without_handle: Vec<_> =
            columns_info.iter().map(|ci| ci.get_column_id()).collect();
        if decode_handle {
            columns_id_without_handle.pop();
        }

        let imp = IndexScanExecutorImpl {
            context: EvalContext::new(config),
            schema,
            columns_id_without_handle,
            decode_handle,
        };
        let wrapper = ScanExecutor::new(ScanExecutorOptions {
            imp,
            storage,
            key_ranges,
            is_backward,
            is_key_only: false,
            accept_point_range: unique,
        })?;
        Ok(Self(wrapper))
    }
}

impl<S: Storage> BatchExecutor for BatchIndexScanExecutor<S> {
    type StorageStats = S::Statistics;

    #[inline]
    fn schema(&self) -> &[FieldType] {
        self.0.schema()
    }

    #[inline]
    fn next_batch(&mut self, scan_rows: usize) -> BatchExecuteResult {
        self.0.next_batch(scan_rows)
    }

    #[inline]
    fn collect_exec_stats(&mut self, dest: &mut ExecuteStats) {
        self.0.collect_exec_stats(dest);
    }

    #[inline]
    fn collect_storage_stats(&mut self, dest: &mut Self::StorageStats) {
        self.0.collect_storage_stats(dest);
    }

    #[inline]
    fn take_scanned_range(&mut self) -> IntervalRange {
        self.0.take_scanned_range()
    }

    #[inline]
    fn can_be_cached(&self) -> bool {
        self.0.can_be_cached()
    }
}

struct IndexScanExecutorImpl {
    /// See `TableScanExecutorImpl`'s `context`.
    context: EvalContext,

    /// See `TableScanExecutorImpl`'s `schema`.
    schema: Vec<FieldType>,

    /// ID of interested columns (exclude PK handle column).
    columns_id_without_handle: Vec<i64>,

    /// Whether PK handle column is interested. Handle will be always placed in the last column.
    decode_handle: bool,
}

impl ScanExecutorImpl for IndexScanExecutorImpl {
    #[inline]
    fn schema(&self) -> &[FieldType] {
        &self.schema
    }

    #[inline]
    fn mut_context(&mut self) -> &mut EvalContext {
        &mut self.context
    }

    fn build_column_vec(&self, scan_rows: usize) -> QuiesceBatchColumnVec {
        let columns_len = self.schema.len();
        let mut columns = Vec::with_capacity(columns_len);
        for _ in 0..self.columns_id_without_handle.len() {
            columns.push(QuiesceBatchColumn::raw_with_capacity(scan_rows));
        }
        if self.decode_handle {

            columns.push(QuiesceBatchColumn::decoded_with_capacity_and_tp(
                scan_rows,
                EvalType::Int,
            ));
        }

        assert_eq!(columns.len(), columns_len);
        QuiesceBatchColumnVec::from(columns)
    }

    fn process_ekv_pair(
        &mut self,
        key: &[u8],
        value: &[u8],
        columns: &mut QuiesceBatchColumnVec,
    ) -> Result<()> {
        if value.len() > MAX_OLD_ENCODED_VALUE_LEN {
            self.process_ekv_pair_new(key, value, columns)
        } else {
            self.process_ekv_pair_old(key, value, columns)
        }
    }
}

impl IndexScanExecutorImpl {
    #[inline]
    fn decode_handle_from_value(&self, mut value: &[u8]) -> Result<i64> {
        // NOTE: it is not `number::decode_i64`.
        value
            .read_u64()
            .map_err(|_| other_err!("Failed to decode handle in value as i64"))
            .map(|x| x as i64)
    }

    #[inline]
    fn decode_handle_from_key(&self, key: &[u8]) -> Result<i64> {
        let flag = key[0];
        let mut val = &key[1..];

        // TODO: Better to use `push_datum`. This requires us to allow `push_datum`
        // receiving optional time zone first.

        match flag {
            datum::INT_FLAG => val
                .read_i64()
                .map_err(|_| other_err!("Failed to decode handle in key as i64")),
            datum::UINT_FLAG => val
                .read_u64()
                .map_err(|_| other_err!("Failed to decode handle in key as u64"))
                .map(|x| x as i64),
            _ => Err(other_err!("Unexpected handle flag {}", flag)),
        }
    }

    fn process_ekv_pair_new(
        &mut self,
        key: &[u8],
        mut value: &[u8],
        columns: &mut QuiesceBatchColumnVec,
    ) -> Result<()> {
        use causet_algebrizer::MilevaDB_query_datatype::codec::row::v2::{RowSlice, V1CompatibleEncoder};
        let tail_len = value[0];
        value = &value[1..];

        let row = RowSlice::from_bytes(value)?;
        for (idx, col_id) in self.columns_id_without_handle.iter().enumerate() {
            if let Some((start, offset)) = row.search_in_non_null_ids(*col_id)? {
                let mut buffer_to_write = columns[idx].mut_raw().begin_concat_extend();
                buffer_to_write
                    .write_v2_as_datum(&row.values()[start..offset], &self.schema[idx])?;
            } else if row.search_in_null_ids(*col_id) {
                columns[idx].mut_raw().push(datum::DATUM_DATA_NULL);
            } else {
                return Err(other_err!("Unexpected missing column {}", col_id));
            }
        }

        if self.decode_handle {
            // For normal index, it is placed at the end and any columns prior to it are
            // ensured to be interested. For unique index, it is placed in the value.

            let handle_val = if tail_len >= 8 {
                // This is a unique index, and we should look up PK handle in value.
                self.decode_handle_from_value(&value[value.len() - 8..])?
            } else {
                // This is a normal index. The remaining payload part is the PK handle.
                // Let's decode it and put in the column.
                self.decode_handle_from_key(&key[key.len() - 9..])?
            };
            columns[self.columns_id_without_handle.len()]
                .mut_decoded()
                .push_int(Some(handle_val));
        }
        Ok(())
    }

    fn process_ekv_pair_old(
        &mut self,
        key: &[u8],
        value: &[u8],
        columns: &mut QuiesceBatchColumnVec,
    ) -> Result<()> {
        check_index_key(key)?;
        // The payload part of the key
        let mut key_payload = &key[table::PREFIX_LEN + table::ID_LEN..];

        for i in 0..self.columns_id_without_handle.len() {
            let (val, remaining) = datum::split_datum(key_payload, false)?;
            columns[i].mut_raw().push(val);
            key_payload = remaining;
        }

        if self.decode_handle {
            // For normal index, it is placed at the end and any columns prior to it are
            // ensured to be interested. For unique index, it is placed in the value.
            let handle_val = if key_payload.is_empty() {
                // This is a unique index, and we should look up PK handle in value.

                self.decode_handle_from_value(value)?
            } else {
                // This is a normal index. The remaining payload part is the PK handle.
                // Let's decode it and put in the column.

                self.decode_handle_from_key(key_payload)?
            };

            columns[self.columns_id_without_handle.len()]
                .mut_decoded()
                .push_int(Some(handle_val));
        }

        Ok(())
    }
}

#[braneg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use codec::prelude::NumberEncoder;
    use ekvproto::interlock::KeyRange;
    use causet_algebrizer::MilevaDB_query_datatype::{FieldTypeAccessor, FieldTypeTp};
    use fidelpb::ColumnInfo;

    use allegroeinstein-prolog-causet-sql::storage::test_fixture::FixtureStorage;
    use allegroeinstein-prolog-causet-sql::util::convert_to_prefix_next;
    use causet_algebrizer::MilevaDB_query_datatype::codec::data_type::*;
    use causet_algebrizer::MilevaDB_query_datatype::codec::{datum, table, Datum};
    use causet_algebrizer::MilevaDB_query_datatype::expr::EvalConfig;

    #[test]
    fn test_basic() {
        const TABLE_ID: i64 = 3;
        const INDEX_ID: i64 = 42;
        let mut ctx = EvalContext::default();

        // Index schema: (INT, FLOAT)

        // the elements in data are: [int index, float index, handle id].
        let data = vec![
            [Datum::I64(-5), Datum::F64(0.3), Datum::I64(10)],
            [Datum::I64(5), Datum::F64(5.1), Datum::I64(5)],
            [Datum::I64(5), Datum::F64(10.5), Datum::I64(2)],
        ];

        // The column info for each column in `data`. Used to build the executor.
        let columns_info = vec![
            {
                let mut ci = ColumnInfo::default();
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci
            },
            {
                let mut ci = ColumnInfo::default();
                ci.as_mut_accessor().set_tp(FieldTypeTp::Double);
                ci
            },
            {
                let mut ci = ColumnInfo::default();
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci.set_pk_handle(true);
                ci
            },
        ];

        // The schema of these columns. Used to check executor output.
        let schema = vec![
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::Double.into(),
            FieldTypeTp::LongLong.into(),
        ];



        let store = {
            let ekv: Vec<_> = data
                .iter()
                .map(|datums| {
                    let index_data = datum::encode_key(&mut ctx, datums).unwrap();
                    let key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &index_data);
                    let value = vec![];
                    (key, value)
                })
                .collect();
            FixtureStorage::from(ekv)
        };

        {


            let key_ranges = vec![{
                let mut range = KeyRange::default();
                let start_data = datum::encode_key(&mut ctx, &[Datum::Min]).unwrap();
                let start_key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &start_data);
                range.set_start(start_key);
                let end_data = datum::encode_key(&mut ctx, &[Datum::Max]).unwrap();
                let end_key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &end_data);
                range.set_end(end_key);
                range
            }];

            let mut executor = BatchIndexScanExecutor::new(
                store.clone(),
                Arc::new(EvalConfig::default()),
                vec![columns_info[0].clone(), columns_info[1].clone()],
                key_ranges,
                true,
                false,
            )
            .unwrap();

            let mut result = executor.next_batch(10);
            assert!(result.is_drained.as_ref().unwrap());
            assert_eq!(result.physical_columns.columns_len(), 2);
            assert_eq!(result.physical_columns.rows_len(), 3);
            assert!(result.physical_columns[0].is_raw());
            result.physical_columns[0]
                .ensure_all_decoded_for_test(&mut ctx, &schema[0])
                .unwrap();
            assert_eq!(
                result.physical_columns[0].decoded().as_int_slice(),
                &[Some(5), Some(5), Some(-5)]
            );
            assert!(result.physical_columns[1].is_raw());
            result.physical_columns[1]
                .ensure_all_decoded_for_test(&mut ctx, &schema[1])
                .unwrap();
            assert_eq!(
                result.physical_columns[1].decoded().as_real_slice(),
                &[
                    Real::new(10.5).ok(),
                    Real::new(5.1).ok(),
                    Real::new(0.3).ok()
                ]
            );
        }

        {


            let key_ranges = vec![{
                let mut range = KeyRange::default();
                let start_data = datum::encode_key(&mut ctx, &[Datum::I64(2)]).unwrap();
                let start_key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &start_data);
                range.set_start(start_key);
                let end_data = datum::encode_key(&mut ctx, &[Datum::I64(6)]).unwrap();
                let end_key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &end_data);
                range.set_end(end_key);
                range
            }];

            let mut executor = BatchIndexScanExecutor::new(
                store,
                Arc::new(EvalConfig::default()),
                vec![
                    columns_info[0].clone(),
                    columns_info[1].clone(),
                    columns_info[2].clone(),
                ],
                key_ranges,
                false,
                false,
            )
            .unwrap();

            let mut result = executor.next_batch(10);
            assert!(result.is_drained.as_ref().unwrap());
            assert_eq!(result.physical_columns.columns_len(), 3);
            assert_eq!(result.physical_columns.rows_len(), 2);
            assert!(result.physical_columns[0].is_raw());
            result.physical_columns[0]
                .ensure_all_decoded_for_test(&mut ctx, &schema[0])
                .unwrap();
            assert_eq!(
                result.physical_columns[0].decoded().as_int_slice(),
                &[Some(5), Some(5)]
            );
            assert!(result.physical_columns[1].is_raw());
            result.physical_columns[1]
                .ensure_all_decoded_for_test(&mut ctx, &schema[1])
                .unwrap();
            assert_eq!(
                result.physical_columns[1].decoded().as_real_slice(),
                &[Real::new(5.1).ok(), Real::new(10.5).ok()]
            );
            assert!(result.physical_columns[2].is_decoded());
            assert_eq!(
                result.physical_columns[2].decoded().as_int_slice(),
                &[Some(5), Some(2)]
            );
        }

        // Case 2. Unique index.

        // For a unique index, the PK handle is stored in the value.

        let store = {
            let ekv: Vec<_> = data
                .iter()
                .map(|datums| {
                    let index_data = datum::encode_key(&mut ctx, &datums[0..2]).unwrap();
                    let key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &index_data);
                    // PK handle in the value
                    let mut value = vec![];
                    value
                        .write_u64(datums[2].as_int().unwrap().unwrap() as u64)
                        .unwrap();
                    (key, value)
                })
                .collect();
            FixtureStorage::from(ekv)
        };

        {
            // Case 2.1. Unique index, prefix range scan.

            let key_ranges = vec![{
                let mut range = KeyRange::default();
                let start_data = datum::encode_key(&mut ctx, &[Datum::I64(5)]).unwrap();
                let start_key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &start_data);
                range.set_start(start_key);
                range.set_end(range.get_start().to_vec());
                convert_to_prefix_next(range.mut_end());
                range
            }];

            let mut executor = BatchIndexScanExecutor::new(
                store.clone(),
                Arc::new(EvalConfig::default()),
                vec![
                    columns_info[0].clone(),
                    columns_info[1].clone(),
                    columns_info[2].clone(),
                ],
                key_ranges,
                false,
                false,
            )
            .unwrap();

            let mut result = executor.next_batch(10);
            assert!(result.is_drained.as_ref().unwrap());
            assert_eq!(result.physical_columns.columns_len(), 3);
            assert_eq!(result.physical_columns.rows_len(), 2);
            assert!(result.physical_columns[0].is_raw());
            result.physical_columns[0]
                .ensure_all_decoded_for_test(&mut ctx, &schema[0])
                .unwrap();
            assert_eq!(
                result.physical_columns[0].decoded().as_int_slice(),
                &[Some(5), Some(5)]
            );
            assert!(result.physical_columns[1].is_raw());
            result.physical_columns[1]
                .ensure_all_decoded_for_test(&mut ctx, &schema[1])
                .unwrap();
            assert_eq!(
                result.physical_columns[1].decoded().as_real_slice(),
                &[Real::new(5.1).ok(), Real::new(10.5).ok()]
            );
            assert!(result.physical_columns[2].is_decoded());
            assert_eq!(
                result.physical_columns[2].decoded().as_int_slice(),
                &[Some(5), Some(2)]
            );
        }

        {
            // Case 2.2. Unique index, point scan.

            let key_ranges = vec![{
                let mut range = KeyRange::default();
                let start_data =
                    datum::encode_key(&mut ctx, &[Datum::I64(5), Datum::F64(5.1)]).unwrap();
                let start_key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &start_data);
                range.set_start(start_key);
                range.set_end(range.get_start().to_vec());
                convert_to_prefix_next(range.mut_end());
                range
            }];

            let mut executor = BatchIndexScanExecutor::new(
                store,
                Arc::new(EvalConfig::default()),
                vec![
                    columns_info[0].clone(),
                    columns_info[1].clone(),
                    columns_info[2].clone(),
                ],
                key_ranges,
                false,
                true,
            )
            .unwrap();

            let mut result = executor.next_batch(10);
            assert!(result.is_drained.as_ref().unwrap());
            assert_eq!(result.physical_columns.columns_len(), 3);
            assert_eq!(result.physical_columns.rows_len(), 1);
            assert!(result.physical_columns[0].is_raw());
            result.physical_columns[0]
                .ensure_all_decoded_for_test(&mut ctx, &schema[0])
                .unwrap();
            assert_eq!(
                result.physical_columns[0].decoded().as_int_slice(),
                &[Some(5)]
            );
            assert!(result.physical_columns[1].is_raw());
            result.physical_columns[1]
                .ensure_all_decoded_for_test(&mut ctx, &schema[1])
                .unwrap();
            assert_eq!(
                result.physical_columns[1].decoded().as_real_slice(),
                &[Real::new(5.1).ok()]
            );
            assert!(result.physical_columns[2].is_decoded());
            assert_eq!(
                result.physical_columns[2].decoded().as_int_slice(),
                &[Some(5)]
            );
        }
    }
}
