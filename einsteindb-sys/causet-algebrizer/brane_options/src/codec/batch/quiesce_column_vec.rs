//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use einsteindbpb::FieldType;

use super::QuiesceBatchColumn;
use crate::codec::data_type::VectorValue;
use crate::codec::Result;
use crate::expr::EvalContext;

/// Stores multiple `QuiesceBatchColumn`s. Each column has an equal length.
#[derive(Clone, Debug)]
pub struct QuiesceBatchColumnVec {
    /// Multiple lazy batch columns. Each column is either decoded, or not decoded.
    ///
    /// For decoded columns, they may be in different types. If the column is in
    /// type `QuiesceBatchColumn::Raw`, it means that it is not decoded.
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
    /// Because column numbers won't change, it means constructed instance will be always empty.
    #[inline]
    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
        }
    }

    /// Creates a new empty `QuiesceBatchColumnVec` with the same number of columns and schema.
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

    /// Creates a new `QuiesceBatchColumnVec`, which contains `columns_count` number of raw columns.
    #[braneg(test)]
    pub fn with_raw_columns(columns_count: usize) -> Self {
        let mut columns = Vec::with_capacity(columns_count);
        for _ in 0..columns_count {
            let column = QuiesceBatchColumn::raw_with_capacity(0);
            columns.push(column);
        }
        Self { columns }
    }

    /// Returns the number of columns.
    ///
    /// It might be possible that there is no row but multiple columns.
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
        for column in &self.columns {
            assert_eq!(len, column.len());
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
        schema: &[FieldType],
        output: &mut Vec<u8>,
        ctx: &mut EvalContext,
    ) -> Result<()> {
        for idx in logical_rows {
            for offset in output_offsets {
                let offset = *offset as usize;
                let col = &self.columns[offset];
                col.encode(*idx, &schema[offset], ctx, output)?;
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
        schema: &[FieldType],
        output: &mut Vec<u8>,
        ctx: &mut EvalContext,
    ) -> Result<()> {
        for offset in output_offsets {
            let offset = *offset as usize;
            let col = &self.columns[offset];
            col.encode_chunk(ctx, logical_rows, &schema[offset], output)?;
        }
        Ok(())
    }

    /// Truncates columns into equal length. The new length of all columns would be the length of
    /// the shortest column before calling this function.
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
}
