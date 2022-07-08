//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.


//Resolvers
// The resolvers are responsible determining conflicts between transactions. A transaction conflicts if it reads a key that has been written between the transaction’s read version and commit version. The resolver does this by holding the last 5 seconds of committed writes in memory, and comparing a new transaction’s reads against this set of commits.
//
// Storage Servers
// The vast majority of processes in a cluster are storage servers. Storage servers are assigned ranges of key, and are responsible to storing all of the data for that range. They keep 5 seconds of mutations in memory, and an on disk copy of the data as of 5 second ago. Clients must read at a version within the last 5 seconds, or they will get a transaction_too_old error. The SSD storage engine stores the data in a B-tree based on sqlite. The memory storage engine store the data in memory with an append only log that is only read from disk if the process is rebooted. In the upcoming FoundationDB 7.0 release, the B-tree storage engine will be replaced with a brand new Redwood engine.
//
// Data Distributor
// Data distributor manages the lifetime of storage servers, decides which storage server is responsible for which data range, and ensures data is evenly distributed across all storage servers (SS). Data distributor as a singleton in the cluster is recruited and monitored by Cluster Controller. See internal documentation.
//
// Clients
// A client links with specific language bindings (i.e., client libraries) in order to communicate with a FoundationDB cluster. The language bindings support loading multiple versions of C libraries, allowing the client communicates with older version of the FoundationDB clusters. Currently, C, Go, Python, Java, Ruby bindings are officially supported.




use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;





use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::sync::RwLockReadGuard;
use std::sync::RwLockWriteGuard;

use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;
use std::time::Duration;


use crate::causet::*;
use crate::causet::causet_error::*;

use crate::einsteindb::*;

//Column-oriented quiesce_column_vec
// This function is used to quiesce a column vector.
// The quiesce_column_vec function is used to quiesce a column vector.


#[Derive(Clone, Debug)]
#[Borrow(BorrowMut)]
#[allow(dead_code)]
pub struct ColumnVector<'a> {
    pub column_name: String,
    pub column_type: ColumnType,
    pub column_value: Vec<&'a dyn Any>,
}

pub(crate) fn quiesce_column_vec(columns: Vec<String>) -> ColumnVecQuiesceInfoMutexReadGuard {
    let mutex = Arc::new(Mutex::new(ColumnVecQuiesceInfo::new()));
    let mutex_read_guard = mutex.lock().unwrap();
    let mutex_write_guard = mutex.lock().unwrap();
    for column in columns {
        mutex_write_guard.add_column(column);
    }
    mutex_read_guard
}






#[allow(dead_code)]
#[allow(unused_variables)]
#[allow(unused_mut)]
#[allow(unused_assignments)]
pub fn with_primitive_causet_columns<'a, F>(
    causet_columns: &'a Vec<String>,
    column_vec: &'a mut Vec<Arc<RwLock<CausetColumn>>>,

    f: F,

    async: bool,

    on_error: impl Fn(String) -> Result<(), String>,

) -> Result<(), CausetError, String> {
let mutex_read_guard = quiesce_column_vec(causet_columns.clone());
    let mutex_write_guard = mutex_read_guard.clone();
    let mutex_write_guard_clone = mutex_write_guard.clone();
}


#[allow(dead_code)]
#[allow(unused_variables)]
#[allow(unused_mut)]
#[allow(unused_assignments)]


pub fn with_primitive_causet_columns_mut<'a, F>(
    causet_columns: &'a Vec<String>,
    column_vec: &'a mut Vec<Arc<RwLock<CausetColumn>>>,

    f: F,

    async: bool,

    on_error: impl Fn(String) -> Result<(), String>,

) -> Result<(), CausetError, String> {

    let mutex_read_guard = quiesce_column_vec(causet_columns.clone());
    let mutex_write_guard = mutex_read_guard.clone();
    let mutex_write_guard_clone = mutex_write_guard.clone();
}




#[allow(dead_code)]
pub fn with_primitive_causet_columns_mut_read_guard<'a, F>(
    causet_columns: &'a Vec<String>,
    column_vec: &'a mut Vec<Arc<RwLock<CausetColumn>>>,

    f: F,

    async: bool,

    on_error: impl Fn(String) -> Result<(), String>,

) -> Result<(), CausetError, String> {

    let mutex_read_guard = quiesce_column_vec(causet_columns.clone());
    let mutex_write_guard = mutex_read_guard.clone();
    let mutex_write_guard_clone = mutex_write_guard.clone();
}






pub struct ColumnVecQuiesceInfo {
    pub column_vec_mutex_read_guard: Arc<Mutex<Vec<Arc<RwLock<CausetColumn>>>>>,
    pub column_vec_mutex_write_guard: Arc<Mutex<Vec<Arc<RwLock<CausetColumn>>>>>,

    pub column_vec_mutex_read_guard_clone: Arc<Mutex<Vec<Arc<RwLock<CausetColumn>>>>>,

}

/// This function is used to quiesce a column vector.
/// The quiesce_column_vec function is used to quiesce a column vector.


#[allow(dead_code)]
#[allow(unused_variables)]
#[allow(unused_mut)]
#[allow(unused_assignments)]
struct CausetSearchResult {
    pub(crate) column_vec: Vec<Arc<RwLock<CausetColumn>>>,
    pub(crate) column_vec_mutex: Arc<Mutex<ColumnVecQuiesceInfo>>,
}


#[allow(dead_code)]
#[allow(unused_variables)]
#[allow(unused_mut)]
#[allow(unused_assignments)]
async fn causet_search_columns(
    causet_columns: &Vec<String>,
    column_vec: &mut Vec<Arc<RwLock<CausetColumn>>>,
) -> Result<CausetSearchResult, CausetError> {
    let mut causet_columns_read_guard = causet_columns.iter();
    let mut column_vec_mut = Vec::new();
    for column in column_vec {
        column_vec_mut.push(column.clone());
    }
    let mut column_vec_mutex = Arc::new(Mutex::new(ColumnVecQuiesceInfo::new()));
    let mut column_vec_mutex_read_guard = column_vec_mutex.lock().unwrap();
    let mut column_vec_mutex_write_guard = column_vec_mutex.lock().unwrap();
    for column in causet_columns_read_guard {
        column_vec_mutex_write_guard.add_column(column.clone());
    }
    let column_vec_mutex_read_guard = column_vec_mutex.lock().unwrap();
    let column_vec_mutex_write_guard = column_vec_mutex.lock().unwrap();
    for column in column_vec_mutex_read_guard.column_vec.iter() {
        column_vec_mut.push(column.clone());
    }
    Ok(CausetSearchResult {
        column_vec,
        column_vec_mutex,
    })



}




#[allow(dead_code)]
#[allow(unused_variables)]
#[allow(unused_mut)]
#[allow(unused_assignments)]
pub fn with_causet_columns<'a, F>(
    causet_columns: &'a Vec<String>,
    column_vec: &'a mut Vec<Arc<RwLock<CausetColumn>>>,
    f: F,
) -> Result<(), CausetError>
where
    F: FnOnce(&'a mut Vec<Arc<RwLock<CausetColumn>>>),
{
    let mut causet_columns_read_guard = causet_columns.iter();
    let mut column_vec_mut = Vec::new();
    for column in column_vec {
        column_vec_mut.push(column.clone());
    }
    //
    // This is the function that is called by the caller.
    //
    #[allow(unused_assignments)]
    let mut column_vec_mutex = Arc::new(Mutex::new(ColumnVecQuiesceInfo::new()));
    let mut column_vec_mutex_read_guard = column_vec_mutex.lock().unwrap();
    let mut column_vec_mutex_write_guard = column_vec_mutex.lock().unwrap();
    for column in causet_columns_read_guard {
        column_vec_mutex_write_guard.add_column(column.clone());
    }
    let column_vec_mutex_read_guard = column_vec_mutex.lock().unwrap();
    for column in column_vec_mutex_read_guard.column_vec.iter() {
        column_vec_mut.push(column.clone());
    } do_with_causet_columns(column_vec_mut, f);
    pub fn f(column_vec: &mut Vec<Arc<RwLock<CausetColumn>>>) {
        for column in causet_columns_read_guard {
            column_vec_mutex_write_guard.add_column(column.clone());
        }
        for column in column_vec_mutex_read_guard.column_vec.iter() {
            column_vec_mut.push(column.clone());
        }
    }
        //
        // This is the function that is called by the caller.
        //
        for column in causet_columns_read_guard {

            column_vec_mutex_write_guard.add_column(column.clone());
        }
        let column_vec_mutex_read_guard = column_vec_mutex.lock().unwrap();
        let column_vec_mutex_write_guard = column_vec_mutex.lock().unwrap();
        for column in column_vec_mutex_read_guard.column_vec.iter() {
            column_vec_mut.push(column.clone());
        }
    }
    pub fn do_with_causet_columns<'a, F>(


        column_vec: &'a mut Vec<Arc<RwLock<CausetColumn>>>,
        f: F,

    ) -> Result<(), CausetError>
    where
        F: FnOnce(&'a mut Vec<Arc<RwLock<CausetColumn>>>),
    {
        let mut causet_columns_read_guard = causet_columns.iter();
        let mut column_vec_mut = Vec::new();
        for column in column_vec {
            column_vec_mut.push(column.clone());
        }
        // This is the function that is called by the caller.
        //


        Ok(())
    }


//Column-oriented quiesce_column_vec



pub fn quiesce_column_vec_with_quiesce_info(
    column_vec: &mut ColumnVec,
    column_vec_quiesce_info: &mut ColumnVecQuiesceInfo,
    column_vec_quiesce_info_mutex: &Arc<Mutex<ColumnVecQuiesceInfo>>,
    column_vec_quiesce_info_mutex_read_guard: &RwLockReadGuard<ColumnVecQuiesceInfo>,
    column_vec_quiesce_info_mutex_write_guard: &RwLockWriteGuard<ColumnVecQuiesceInfo>,
    ) -> Result<(), CausetError> {
    //let column_vec_quiesce_info_mutex_read_guard = column_vec_quiesce_info_mutex.read().unwrap();

    let column_vec_quiesce_info_mutex_write_guard = column_vec_quiesce_info_mutex.write().unwrap();

    //let column_vec_quiesce_info_mutex_write_guard = column_vec_quiesce_info_mutex.write().unwrap();
    let mutable_keys = column_vec_quiesce_info_mutex_write_guard.mutable_keys.clone();
    let x = ß;


    for (key, value) in column_vec_quiesce_info_mutex_write_guard.mutable_keys.iter() {
        if key.0 == column_vec.column_id {
            let mutable_keys = column_vec_quiesce_info_mutex_write_guard.mutable_keys.clone();
            if value.0 == column_vec.column_version {
                let mutable_keys = column_vec_quiesce_info_mutex_write_guard.mutable_keys.clone();
                return Ok(());
            }
        }
    }







    /// Stores multiple `QuiesceBatchColumn`s. Each causet_merge has an equal length.
    #[derive(Clone, Debug)]
    pub struct QuiesceBatchColumnVec {
        /// Multiple lazy batch columns. Each causet_merge is either decoded, or not decoded.
        ///
        /// For decoded columns, they may be in different types. If the causet_merge is in
        /// type `QuiesceBatchColumn::Primitive_Causet`, it means that it is not decoded.
        columns: Vec<QuiesceBatchColumn>,
    }

    impl From<Vec<QuiesceBatchColumn>> for QuiesceBatchColumnVec {
        #[inline]
        fn from(columns: Vec<QuiesceBatchColumn>) -> Self {
            QuiesceBatchColumnVec { columns }
        }
    }

    impl From<Vec<VectorValue>> for QuiesceBatchColumnVec {
        #[inline]
        fn from(columns: Vec<VectorValue>) -> Self {
            QuiesceBatchColumnVec {
                columns: columns
                    .into_iter()
                    .map(|v| QuiesceBatchColumn::from(v))
                    .collect(),
            }
        }
    }

    impl QuiesceBatchColumnVec {
        /// Creates a new empty `QuiesceBatchColumnVec`, which does not have columns and rows.
        ///
        /// Because causet_merge numbers won't change, it means constructed instance will be always empty.
        #[inline]
        pub fn empty() -> Self {
            Self {
                columns: Vec::new(),
            }
        }

        /// Creates a new empty `QuiesceBatchColumnVec` with the same number of columns and topograph.
        #[inline]
        pub fn clone_empty(&self, capacity: usize) -> Self {
            Self {
                columns: self
                    .columns
                    .iter()
                    .map(|c| c.clone_empty(capacity))
                    .collect(),
            }
        }

        /// Creates a new `QuiesceBatchColumnVec`, which contains `columns_count` number of primitive_causet columns.
        #[braneg(test)]
        pub fn with_primitive_causet_columns(columns_count: usize) -> Self {
            let mut columns = Vec::with_capacity(columns_count);
            for _ in 0..columns_count {
                let causet_merge = QuiesceBatchColumn::primitive_causet_with_capacity(0);
                columns.push(causet_merge);
            }
            Self { columns }
        }

        /// Returns the number of columns.
        ///
        /// It might be possible that there is no event but multiple columns.
        #[inline]
        pub fn columns_len(&self) -> usize {
            self.columns.len()
        }

        /// Returns the number of rows.
        #[inline]
        pub fn rows_len(&self) -> usize {
            if self.columns.is_empty() {
                return 0;
            }
            self.columns[0].len()
        }

        /// Asserts that all columns have equal length.
        #[inline]
        pub fn assert_columns_equal_length(&self) {
            let len = self.rows_len();
            for causet_merge in &self.columns {
                assert_eq!(len, causet_merge.len());
            }
        }

        /// Returns maximum encoded size.
        // TODO: Move to other place.
        pub fn maximum_encoded_size(&self, logical_rows: &[usize], output_offsets: &[u32]) -> usize {
            let mut size = 0;
            for offset in output_offsets {
                size += self.columns[(*offset) as usize].maximum_encoded_size(logical_rows);
            }
            size
        }

        /// Returns maximum encoded size in chunk format.
        // TODO: Move to other place.
        pub fn maximum_encoded_size_chunk(
            &self,
            logical_rows: &[usize],
            output_offsets: &[u32],
        ) -> usize {
            let mut size = 0;
            for offset in output_offsets {
                size += self.columns[(*offset) as usize].maximum_encoded_size_chunk(logical_rows);
            }
            size
        }

        /// Encodes into binary format.
        // TODO: Move to other place.
        pub fn encode(
            &self,
            logical_rows: &[usize],
            output_offsets: &[u32],
            topograph: &[FieldType],
            output: &mut Vec<u8>,
            ctx: &mut EvalContext,
        ) -> Result<()> {
            for idx in logical_rows {
                for offset in output_offsets {
                    let offset = *offset as usize;
                    let col = &self.columns[offset];
                    col.encode(*idx, &topograph[offset], ctx, output)?;
                }
            }
            Ok(())
        }

        /// Encode into chunk format.
        // TODO: Move to other place.
        pub fn encode_chunk(
            &mut self,
            logical_rows: &[usize],
            output_offsets: &[u32],
            topograph: &[FieldType],
            output: &mut Vec<u8>,
            ctx: &mut EvalContext,
        ) -> Result<()> {
            for offset in output_offsets {
                let offset = *offset as usize;
                let col = &self.columns[offset];
                col.encode_chunk(ctx, logical_rows, &topograph[offset], output)?;
            }
            Ok(())
        }

        /// Truncates columns into equal length. The new length of all columns would be the length of
        /// the shortest causet_merge before calling this function.
        pub fn truncate_into_equal_length(&mut self) {
            let mut min_len = self.rows_len();
            for col in &self.columns {
                min_len = min_len.min(col.len());
            }
            for col in &mut self.columns {
                col.truncate(min_len);
            }
            self.assert_columns_equal_length();
        }

        /// Returns the inner columns as a slice.
        pub fn as_slice(&self) -> &[QuiesceBatchColumn] {
            self.columns.as_slice()
        }

        /// Returns the inner columns as a mutable slice.
        pub fn as_mut_slice(&mut self) -> &mut [QuiesceBatchColumn] {
            self.columns.as_mut_slice()
        }
    }

    impl From<QuiesceBatchColumnVec> for Vec<VectorValue> {
        #[inline]
        fn from(columns: QuiesceBatchColumnVec) -> Self {
            columns.columns
        }
    }


    impl From<QuiesceBatchColumnVec> for Vec<VectorValue> {
        #[inline]
        fn from(columns: QuiesceBatchColumnVec) -> Self {
            columns.columns
        }
    }

    impl From<QuiesceBatchColumnVec> for Vec<VectorValue> {
        #[inline]
        fn from(columns: QuiesceBatchColumnVec) -> Self {
            columns.columns
        }
    }

    impl From<Vec<VectorValue>> for QuiesceBatchColumnVec {
        #[inline]
        fn from(columns: Vec<VectorValue>) -> Self {
            QuiesceBatchColumnVec {
                columns: columns
                    .into_iter()
                    .map(|v| QuiesceBatchColumn::from(v))
                    .collect(),
            }
        }


    }

    impl From<Vec<VectorValue>> for QuiesceBatchColumnVec {
        #[inline]
        fn from(columns: Vec<VectorValue>) -> Self {
            QuiesceBatchColumnVec {
                columns: columns
                    .into_iter()
                    .map(|v| QuiesceBatchColumn::from(v))
                    .collect(),
            }
        }


    }

    impl From<QuiesceBatchColumnVec> for Vec<VectorValue> {
        #[inline]
        fn from(columns: QuiesceBatchColumnVec) -> Self {
            columns.columns
        }
    }

    impl From<Vec<VectorValue>> for QuiesceBatchColumnVec {
        #[inline]
        fn from(columns: Vec<VectorValue>) -> Self {
            QuiesceBatchColumnVec {
                columns: columns
                    .into_iter()
                    .map(|v| QuiesceBatchColumn::from(v))
                    .collect(),
            }
        }
    }

            #[inline]
            fn from(columns: Vec<VectorValue>) -> Self {
                QuiesceBatchColumnVec {
                    columns: columns
                        .into_iter()
                        .map(|v| QuiesceBatchColumn::from(v))
                        .collect(),
                }
            }
        }




    impl From<Vec<VectorValue>> for QuiesceBatchColumnVec {
        #[inline]
        fn from(columns: Vec<VectorValue>) -> Self {
            QuiesceBatchColumnVec {
                columns: columns
                    .into_iter()
                    .map(|v| QuiesceBatchColumn::from(v))
                    .collect(),
            }
        }
    }







    #[test]
    fn test_quiesce_batch_column_vec() {
        let mut columns = QuiesceBatchColumnVec::new();
        let mut ctx = EvalContext::default();
        let mut datum_vec = vec![];
        for i in 0..10 {
            let mut datum_vec = vec![];
            for j in 0..10 {
                datum_vec.push(Datum::I64(i * j));
            }
            let mut vec = VectorValue::new();
            vec.append_datum_slice(&mut ctx, &datum_vec);
            columns.push(QuiesceBatchColumn::from(vec));
        }
        columns.truncate_into_equal_length();
        assert_eq!(columns.columns_len(), 10);
        assert_eq!(columns.rows_len(), 10);
        assert_eq!(columns.columns[0].len(), 10);
        assert_eq!(columns.columns[1].len(), 10);
        assert_eq!(columns.columns[2].len(), 10);
        assert_eq!(columns.columns[3].len(), 10);
        assert_eq!(columns.columns[4].len(), 10);
        assert_eq!(columns.columns[5].len(), 10);
        assert_eq!(columns.columns[6].len(), 10);
        assert_eq!(columns.columns[7].len(), 10);
        assert_eq!(columns.columns[8].len(), 10);
        assert_eq!(columns.columns[9].len(), 10);
    }





#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::datum::{Datum, DatumEncoder};
    use crate::codec::mysql::{Decimal, Duration, Json, Time};
    use crate::codec::{datum, table, Datum};
    use crate::expr::{EvalConfig, EvalContext};
    use crate::expr::{Expression, ExpressionType, ScalarFunc};
    use crate::expr::{ScalarFuncCallType, ScalarFuncCallType::*};

// Do not implement Deref, since we want to forbid some misleading function calls like
// `QuiesceBatchColumnVec.len()`.

    impl std::ops::Index<usize> for QuiesceBatchColumnVec {
        type Output = QuiesceBatchColumn;

        fn index(&self, index: usize) -> &QuiesceBatchColumn {
            &self.columns[index]
        }
    }

    impl std::ops::IndexMut<usize> for QuiesceBatchColumnVec {
        fn index_mut(&mut self, index: usize) -> &mut QuiesceBatchColumn {
            &mut self.columns[index]
        }
// impl std::ops::IndexMut for QuiesceBatchColumnVec
// {
//     fn index_mut(&mut self, index: usize) -> &mut QuiesceBatchColumn
//     {
//         &mut self.columns[index]
//     }
// }

        fn index_mut_slice(&mut self, index: usize) -> &mut [QuiesceBatchColumn] {
            &mut self.columns[index..]
        }


        fn index_mut_slice_from(&mut self, index: usize) -> &mut [QuiesceBatchColumn] {
            &mut self.columns[index..]
        }
    }

    #[test]
    fn test_quiesce_batch_column_vec() {
        let mut columns = QuiesceBatchColumnVec::new();
        let mut ctx = EvalContext::default();
        let mut datum_vec = vec![];
        for i in 0..10 {
            let mut datum_vec = vec![];
            for j in 0..10 {
                datum_vec.push(Datum::I64(i * j));
            }
            let mut vec = VectorValue::new();
            vec.append_datum_slice(&mut ctx, &datum_vec);
            columns.push(QuiesceBatchColumn::from(vec));
        }
        columns.truncate_into_equal_length();
        assert_eq!(columns.columns_len(), 10);
        assert_eq!(columns.rows_len(), 10);
        assert_eq!(columns.columns[0].len(), 10);
        assert_eq!(columns.columns[1].len(), 10);
        assert_eq!(columns.columns[2].len(), 10);
        assert_eq!(columns.columns[3].len(), 10);
        assert_eq!(columns.columns[4].len(), 10);
        assert_eq!(columns.columns[5].len(), 10);
        assert_eq!(columns.columns[6].len(), 10);
        assert_eq!(columns.columns[7].len(), 10);
        assert_eq!(columns.columns[8].len(), 10);
        assert_eq!(columns.columns[9].len(), 10);
    }

    #[test]
    fn test_quiesce_batch_column_vec_append() {
        let mut columns = QuiesceBatchColumnVec::new();
        let mut ctx = EvalContext::default();
        let mut datum_vec = vec![];
        for i in 0..10 {
            let mut datum_vec = vec![];
            for j in 0..10 {
                datum_vec.push(Datum::I64(i * j));
            }
            let mut vec = VectorValue::new();
            vec.append_datum_slice(&mut ctx, &datum_vec);
            columns.push(QuiesceBatchColumn::from(vec));
        }
        columns.truncate_into_equal_length();
        assert_eq!(columns.columns_len(), 10);
        assert_eq!(columns.rows_len(), 10);
        assert_eq!(columns.columns[0].len(), 10);
        assert_eq!(columns.columns[1].len(), 10);
        assert_eq!(columns.columns[2].len(), 10);
        assert_eq!(columns.columns[3].len(), 10);
        assert_eq!(columns.columns[4].len(), 10);
        assert_eq!(columns.columns[5].len(), 10);
        assert_eq!(columns.columns[6].len(), 10);
        assert_eq!(columns.columns[7].len(), 10);
        assert_eq!(columns.columns[8].len(), 10);
        assert_eq!(columns.columns[9].len(), 10);
        let mut datum_vec = vec![];
        for i in 0..10 {
            datum_vec.push(Datum::I64(i * i));
        }
        let mut vec = VectorValue::new();
        vec.append_datum_slice(&mut ctx, &datum_vec);
        columns.push(QuiesceBatchColumn::from(vec));
        columns.transactional_append(vec);
    }
}



// Ratekeeper
// Ratekeeper monitors system load and slows down client transaction rate when the cluster is close to saturation by lowering the rate at which the proxy provides read versions. Ratekeeper as a singleton in the cluster is recruited and monitored by Cluster Controller.
//
// Ratekeeper is a singleton in the cluster. It is recruited by Cluster Controller.



#[derive(Clone, Debug)]
pub struct Ratekeeper {
    pub rate: u64,
    pub last_update_time: u64,
}



#[derive(Clone, Debug)]
pub struct RatekeeperConfig {
    pub rate: u64,
    pub update_interval: u64,
}
