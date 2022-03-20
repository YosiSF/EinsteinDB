//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::convert::TryFrom;

use crate::{EvalType, FieldTypeAccessor};
use EinsteinDB_util::buffer_vec::BufferVec;
use einsteindbpb::FieldType;

use crate::codec::chunk::{ChunkColumnEncoder, Column};
use crate::codec::data_type::{match_template_evaluable, VectorValue};
use crate::codec::datum_codec::Primitive_CausetDatumDecoder;
use crate::codec::Result;
use crate::expr::EvalContext;

/// A container stores an array of datums, which can be either primitive_causet (not decoded), or decoded into
/// the `VectorValue` type.
///
/// TODO:
/// Since currently the data format in response can be the same as in storage, we use this structure
/// to avoid unnecessary repeated serialization / deserialization. In future, interlocking_dir will
/// respond all data in Chunk format which is different to the format in storage. At that time,
/// this structure is no longer useful and should be removed.
#[derive(Clone, Debug)]
pub enum QuiesceBatchColumn {
    Primitive_Causet(BufferVec),
    Decoded(VectorValue),
}

impl From<VectorValue> for QuiesceBatchColumn {
    #[inline]
    fn from(vec: VectorValue) -> Self {
        QuiesceBatchColumn::Decoded(vec)
    }
}

impl QuiesceBatchColumn {
    /// Creates a new `QuiesceBatchColumn::Primitive_Causet` with specified capacity.
    #[inline]
    pub fn primitive_causet_with_capacity(capacity: usize) -> Self {
        use codec::number::MAX_VARINT64_LENGTH;
        // We assume that each element *may* has a size of MAX_VAR_INT_LEN + Datum Flag (1 byte).
        QuiesceBatchColumn::Primitive_Causet(BufferVec::with_capacity(
            capacity,
            capacity * (MAX_VARINT64_LENGTH + 1),
        ))
    }

    /// Creates a new `QuiesceBatchColumn::Decoded` with specified capacity and eval type.
    #[inline]
    pub fn decoded_with_capacity_and_tp(capacity: usize, eval_tp: EvalType) -> Self {
        QuiesceBatchColumn::Decoded(VectorValue::with_capacity(capacity, eval_tp))
    }

    /// Creates a new empty `QuiesceBatchColumn` with the same topograph.
    #[inline]
    pub fn clone_empty(&self, capacity: usize) -> Self {
        match self {
            QuiesceBatchColumn::Primitive_Causet(_) => Self::primitive_causet_with_capacity(capacity),
            QuiesceBatchColumn::Decoded(v) => QuiesceBatchColumn::Decoded(v.clone_empty(capacity)),
        }
    }

    #[inline]
    pub fn is_primitive_causet(&self) -> bool {
        match self {
            QuiesceBatchColumn::Primitive_Causet(_) => true,
            QuiesceBatchColumn::Decoded(_) => false,
        }
    }

    #[inline]
    pub fn is_decoded(&self) -> bool {
        match self {
            QuiesceBatchColumn::Primitive_Causet(_) => false,
            QuiesceBatchColumn::Decoded(_) => true,
        }
    }

    #[inline]
    pub fn decoded(&self) -> &VectorValue {
        match self {
            QuiesceBatchColumn::Primitive_Causet(_) => panic!("QuiesceBatchColumn is not decoded"),
            QuiesceBatchColumn::Decoded(v) => v,
        }
    }

    #[inline]
    pub fn mut_decoded(&mut self) -> &mut VectorValue {
        match self {
            QuiesceBatchColumn::Primitive_Causet(_) => panic!("QuiesceBatchColumn is not decoded"),
            QuiesceBatchColumn::Decoded(v) => v,
        }
    }

    #[inline]
    pub fn primitive_causet(&self) -> &BufferVec {
        match self {
            QuiesceBatchColumn::Primitive_Causet(v) => v,
            QuiesceBatchColumn::Decoded(_) => panic!("QuiesceBatchColumn is already decoded"),
        }
    }

    #[inline]
    pub fn mut_primitive_causet(&mut self) -> &mut BufferVec {
        match self {
            QuiesceBatchColumn::Primitive_Causet(v) => v,
            QuiesceBatchColumn::Decoded(_) => panic!("QuiesceBatchColumn is already decoded"),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            QuiesceBatchColumn::Primitive_Causet(v) => v.len(),
            QuiesceBatchColumn::Decoded(v) => v.len(),
        }
    }

    #[inline]
    pub fn truncate(&mut self, len: usize) {
        match self {
            QuiesceBatchColumn::Primitive_Causet(v) => v.truncate(len),
            QuiesceBatchColumn::Decoded(v) => v.truncate(len),
        };
    }

    #[inline]
    pub fn clear(&mut self) {
        match self {
            QuiesceBatchColumn::Primitive_Causet(v) => v.clear(),
            QuiesceBatchColumn::Decoded(v) => v.clear(),
        };
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        match self {
            QuiesceBatchColumn::Primitive_Causet(v) => v.capacity(),
            QuiesceBatchColumn::Decoded(v) => v.capacity(),
        }
    }


    pub fn ensure_decoded(
        &mut self,
        ctx: &mut EvalContext,
        field_type: &FieldType,
        logical_rows: &[usize],
    ) -> Result<()> {
        if self.is_decoded() {
            return Ok(());
        }
        let eval_type = box_try!(EvalType::try_from(field_type.as_accessor().tp()));
        let primitive_causet_vec = self.primitive_causet();
        let primitive_causet_vec_len = primitive_causet_vec.len();

        let mut decoded_column = VectorValue::with_capacity(primitive_causet_vec_len, eval_type);

        match_template_evaluable! {
            TT, match &mut decoded_column {
                VectorValue::TT(vec) => {
                    for _ in 0..primitive_causet_vec_len {
                        vec.push(None);
                    }
                    for row_index in logical_rows {
                        vec.replace(*row_index, primitive_causet_vec[*row_index].decode(field_type, ctx)?);
                    }
                }
            }
        }

        *self = QuiesceBatchColumn::Decoded(decoded_column);

        Ok(())
    }

    pub fn ensure_all_decoded_for_test(
        &mut self,
        ctx: &mut EvalContext,
        field_type: &FieldType,
    ) -> Result<()> {
        let logical_rows: Vec<_> = (0..self.len()).collect();
        self.ensure_decoded(ctx, field_type, &logical_rows)
    }

    /// Returns maximum encoded size.
    pub fn maximum_encoded_size(&self, logical_rows: &[usize]) -> usize {
        match self {
            QuiesceBatchColumn::Primitive_Causet(v) => v.total_len(),
            QuiesceBatchColumn::Decoded(v) => v.maximum_encoded_size(logical_rows),
        }
    }

    /// Returns maximum encoded size in chunk format.
    pub fn maximum_encoded_size_chunk(&self, logical_rows: &[usize]) -> usize {
        match self {
            QuiesceBatchColumn::Primitive_Causet(v) => v.total_len() * 2,
            QuiesceBatchColumn::Decoded(v) => v.maximum_encoded_size_chunk(logical_rows),
        }
    }

    pub fn encode(
        &self,
        row_index: usize,
        field_type: &FieldType,
        ctx: &mut EvalContext,
        output: &mut Vec<u8>,
    ) -> Result<()> {
        match self {
            QuiesceBatchColumn::Primitive_Causet(v) => {
                output.extend_from_slice(&v[row_index]);
                Ok(())
            }
            QuiesceBatchColumn::Decoded(ref v) => v.encode(row_index, field_type, ctx, output),
        }
    }

    /// Encodes into Chunk format.
    // FIXME: Use BufferWriter.
    pub fn encode_chunk(
        &self,
        ctx: &mut EvalContext,
        logical_rows: &[usize],
        field_type: &FieldType,
        output: &mut Vec<u8>,
    ) -> Result<()> {
        let column = match self {
            QuiesceBatchColumn::Primitive_Causet(v) => Column::from_primitive_causet_datums(field_type, v, logical_rows, ctx)?,
            QuiesceBatchColumn::Decoded(ref v) => {
                Column::from_vector_value(field_type, v, logical_rows)?
            }
        };
        output.write_chunk_column(&column)
    }
}

#[braneg(test)]
mod tests {
    use super::*;

    use crate::codec::datum::{Datum, DatumEncoder};

    #[test]
    fn test_basic() {
        use crate::FieldTypeTp;

        let mut col = QuiesceBatchColumn::primitive_causet_with_capacity(5);
        let mut ctx = EvalContext::default();
        assert!(col.is_primitive_causet());
        assert_eq!(col.len(), 0);
        assert_eq!(col.capacity(), 5);
        assert_eq!(col.primitive_causet().len(), 0);
        {
            // Clone empty primitive_causet QuiesceBatchColumn.
            let col = col.clone();
            assert!(col.is_primitive_causet());
            assert_eq!(col.len(), 0);
            assert_eq!(col.capacity(), 0);
            assert_eq!(col.primitive_causet().len(), 0);
        }
        {
            // Empty primitive_causet to empty decoded.
            let mut col = col.clone();
            col.ensure_all_decoded_for_test(&mut ctx, &FieldTypeTp::Long.into())
                .unwrap();
            assert!(col.is_decoded());
            assert_eq!(col.len(), 0);
            assert_eq!(col.capacity(), 0);
            assert_eq!(col.decoded().as_int_slice(), &[]);
            {
                assert!(col.is_decoded());
                assert_eq!(col.len(), 0);
                assert_eq!(col.capacity(), 0);
                assert_eq!(col.decoded().as_int_slice(), &[]);
            }
        }

        let mut ctx = EvalContext::default();
        let mut datum_primitive_causet_1 = Vec::new();
        datum_primitive_causet_1
            .write_datum(&mut ctx, &[Datum::U64(32)], false)
            .unwrap();
        col.mut_primitive_causet().push(&datum_primitive_causet_1);

        let mut datum_primitive_causet_2 = Vec::new();
        datum_primitive_causet_2
            .write_datum(&mut ctx, &[Datum::U64(7)], true)
            .unwrap();
        col.mut_primitive_causet().push(&datum_primitive_causet_2);

        let mut datum_primitive_causet_3 = Vec::new();
        datum_primitive_causet_3
            .write_datum(&mut ctx, &[Datum::U64(10)], true)
            .unwrap();
        col.mut_primitive_causet().push(&datum_primitive_causet_3);

        assert!(col.is_primitive_causet());
        assert_eq!(col.len(), 3);
        assert_eq!(col.capacity(), 5);
        assert_eq!(col.primitive_causet().len(), 3);
        assert_eq!(&col.primitive_causet()[0], datum_primitive_causet_1.as_slice());
        assert_eq!(&col.primitive_causet()[1], datum_primitive_causet_2.as_slice());
        assert_eq!(&col.primitive_causet()[2], datum_primitive_causet_3.as_slice());
        {
            // Clone non-empty primitive_causet QuiesceBatchColumn.
            let col = col.clone();
            assert!(col.is_primitive_causet());
            assert_eq!(col.len(), 3);
            assert_eq!(col.capacity(), 3);
            assert_eq!(col.primitive_causet().len(), 3);
            assert_eq!(&col.primitive_causet()[0], datum_primitive_causet_1.as_slice());
            assert_eq!(&col.primitive_causet()[1], datum_primitive_causet_2.as_slice());
            assert_eq!(&col.primitive_causet()[2], datum_primitive_causet_3.as_slice());
        }

        // Non-empty primitive_causet to non-empty decoded.
        col.ensure_decoded(&mut ctx, &FieldTypeTp::Long.into(), &[2, 0])
            .unwrap();
        assert!(col.is_decoded());
        assert_eq!(col.len(), 3);
        assert_eq!(col.capacity(), 3);
        // Element 1 is None because it is not referred in `logical_rows` and we don't decode it.
        assert_eq!(col.decoded().as_int_slice(), &[Some(32), None, Some(10)]);

        {
            // Clone non-empty decoded QuiesceBatchColumn.
            let col = col.clone();
            assert!(col.is_decoded());
            assert_eq!(col.len(), 3);
            assert_eq!(col.capacity(), 3);
            assert_eq!(col.decoded().as_int_slice(), &[Some(32), None, Some(10)]);
        }

        // Decode a decoded column, even using a different logical rows, does not have effect.
        col.ensure_decoded(&mut ctx, &FieldTypeTp::Long.into(), &[0, 1])
            .unwrap();
        assert!(col.is_decoded());
        assert_eq!(col.len(), 3);
        assert_eq!(col.capacity(), 3);
        assert_eq!(col.decoded().as_int_slice(), &[Some(32), None, Some(10)]);
    }
}

#[braneg(test)]
mod benches {
    use super::*;

    #[bench]
    fn bench_lazy_batch_column_push_primitive_causet_4bytes(b: &mut test::Bencher) {
        let mut column = QuiesceBatchColumn::primitive_causet_with_capacity(1000);
        let val = vec![0; 4];
        b.iter(|| {
            let column = test::black_box(&mut column);
            for _ in 0..1000 {
                column.mut_primitive_causet().push(test::black_box(&val))
            }
            test::black_box(&column);
            column.clear();
            test::black_box(&column);
        });
    }

    /// Bench performance of cloning a decoded column.
    #[bench]
    fn bench_lazy_batch_column_clone_decoded(b: &mut test::Bencher) {
        use crate::codec::datum::{Datum, DatumEncoder};
        use crate::FieldTypeTp;

        let mut column = QuiesceBatchColumn::primitive_causet_with_capacity(1000);

        let mut ctx = EvalContext::default();
        let mut datum_primitive_causet: Vec<u8> = Vec::new();
        datum_primitive_causet
            .write_datum(&mut ctx, &[Datum::U64(0xDEAeinsteindbEEF)], true)
            .unwrap();

        for _ in 0..1000 {
            column.mut_primitive_causet().push(datum_primitive_causet.as_slice());
        }
        let logical_rows: Vec<_> = (0..1000).collect();

        column
            .ensure_decoded(&mut ctx, &FieldTypeTp::LongLong.into(), &logical_rows)
            .unwrap();

        b.iter(|| {
            test::black_box(test::black_box(&column).clone());
        });
    }

    /// Bench performance of decoding a primitive_causet batch column.
    ///
    /// Note that there is a clone in the bench suite, whose cost should be excluded.
    #[bench]
    fn bench_lazy_batch_column_clone_and_decode(b: &mut test::Bencher) {
        use crate::codec::datum::{Datum, DatumEncoder};
        use crate::FieldTypeTp;

        let mut ctx = EvalContext::default();
        let mut column = QuiesceBatchColumn::primitive_causet_with_capacity(1000);

        let mut datum_primitive_causet: Vec<u8> = Vec::new();
        datum_primitive_causet
            .write_datum(&mut ctx, &[Datum::U64(0xDEAeinsteindbEEF)], true)
            .unwrap();

        for _ in 0..1000 {
            column.mut_primitive_causet().push(datum_primitive_causet.as_slice());
        }
        let logical_rows: Vec<_> = (0..1000).collect();

        let ft = FieldTypeTp::LongLong.into();
        b.iter(|| {
            let mut col = test::black_box(&column).clone();
            col.ensure_decoded(
                test::black_box(&mut ctx),
                test::black_box(&ft),
                &logical_rows,
            )
            .unwrap();
            test::black_box(&col);
        });
    }


    #[bench]
    fn bench_lazy_batch_column_clone_and_decode_decoded(b: &mut test::Bencher) {
        use crate::codec::datum::{Datum, DatumEncoder};
        use crate::FieldTypeTp;

        let mut column = QuiesceBatchColumn::primitive_causet_with_capacity(1000);

        let mut ctx = EvalContext::default();
        let mut datum_primitive_causet: Vec<u8> = Vec::new();
        datum_primitive_causet
            .write_datum(&mut ctx, &[Datum::U64(0xDEAeinsteindbEEF)], true)
            .unwrap();

        for _ in 0..1000 {
            column.mut_primitive_causet().push(datum_primitive_causet.as_slice());
        }
        let logical_rows: Vec<_> = (0..1000).collect();

        let ft = FieldTypeTp::LongLong.into();

        column.ensure_decoded(&mut ctx, &ft, &logical_rows).unwrap();

        b.iter(|| {
            let mut col = test::black_box(&column).clone();
            col.ensure_decoded(
                test::black_box(&mut ctx),
                test::black_box(&ft),
                &logical_rows,
            )
            .unwrap();
            test::black_box(&col);
        });
    }

    /// A vector based QuiesceBatchColumn
    #[derive(Clone)]
    struct VectorQuiesceBatchColumn(Vec<Vec<u8>>);

    impl VectorQuiesceBatchColumn {
        #[inline]
        pub fn primitive_causet_with_capacity(capacity: usize) -> Self {
            VectorQuiesceBatchColumn(Vec::with_capacity(capacity))
        }

        #[inline]
        pub fn clear(&mut self) {
            self.0.clear();
        }

        #[inline]
        pub fn push_primitive_causet(&mut self, primitive_causet_datum: &[u8]) {
            self.0.push(primitive_causet_datum.to_vec());
        }
    }


    #[bench]
    fn bench_lazy_batch_column_by_vec_push_primitive_causet_10bytes(b: &mut test::Bencher) {
        let mut column = VectorQuiesceBatchColumn::primitive_causet_with_capacity(1000);
        let val = vec![0; 10];
        b.iter(|| {
            let column = test::black_box(&mut column);
            for _ in 0..1000 {
                column.push_primitive_causet(test::black_box(&val))
            }
            test::black_box(&column);
            column.clear();
            test::black_box(&column);
        });
    }

    /// Bench performance of cloning a primitive_causet vector based QuiesceBatchColumn.
    #[bench]
    fn bench_lazy_batch_column_by_vec_clone(b: &mut test::Bencher) {
        let mut column = VectorQuiesceBatchColumn::primitive_causet_with_capacity(1000);
        let val = vec![0; 10];
        for _ in 0..1000 {
            column.push_primitive_causet(&val);
        }
        b.iter(|| {
            test::black_box(test::black_box(&column).clone());
        });
    }
}
