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
use std::sync::Mutex;
use std::sync::RwLock;
use std::sync::RwLockReadGuard;
use std::sync::RwLockWriteGuard;
use std::sync::{mpsc, Arc as SyncArc};
use std::thread;
use std::time::Duration;
use std::{fmt, io, result, thread};
use std::{fmt::Debug, io::Write};
use std::{io::BufRead, io::BufReader, io::Read};



use EinsteinDB::berolinasql::{self, prelude::*};
use EinsteinDB::causet::{self, kv, kv::{self, Key, Value}, storage::{self, Engine, Snapshot}};
use EinsteinDB::causet::storage::{
    self,
    kv,
    kv::{self, Key, Value},
    storage::{self, Engine, Snapshot},
};


use EinsteinDB_util::buffer_vec::BufferVec;
use EinsteinDB_util::codec::{Error as CodecError, Result as CodecResult};
use EinsteinDB_util::{
    bytes::{self, Bytes},
    io::{self, WriteZeroes},
    number::{self, Number},
    version::{self, Version},
};
use std::{
    error,
    fmt,
    io::{self, Write},
    result,
    str::FromStr,
};


use EinsteinDB::einstein_db_ctl::{self, prelude::*};







#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "{}", _0)]
    Causet(String),
    #[fail(display = "{}", _0)]
    CausetQ(String),
    #[fail(display = "{}", _0)]
    EinsteinML(String),
    #[fail(display = "{}", _0)]
    Gremlin(String),
    #[fail(display = "{}", _0)]
    GremlinQ(String),
    #[fail(display = "{}", _0)]
    GremlinQ2(String),
    #[fail(display = "{}", _0)]
    GremlinQ3(String),
    #[fail(display = "{}", _0)]
    GremlinQ4(String),
    #[fail(display = "{}", _0)]
    GremlinQ5(String),
    #[fail(display = "{}", _0)]
    GremlinQ6(String),
    #[fail(display = "{}", _0)]
    GremlinQ7(String),
    #[fail(display = "{}", _0)]
    GremlinQ8(String),
    #[fail(display = "{}", _0)]
    GremlinQ9(String),
    #[fail(display = "{}", _0)]
    GremlinQ10(String),
    #[fail(display = "{}", _0)]
    GremlinQ11(String),
    #[fail(display = "{}", _0)]
    GremlinQ12(String),
    #[fail(display = "{}", _0)]
    GremlinQ13(String),
    #[fail(display = "{}", _0)]
    GremlinQ14(String),
}

///! A Quiesce Column becomes a causet column.
/// It is a column that is used to quiesce the causet.add_rule(Rule::new("  ".to_string(), "  ".to_string()));
///
///
/// # Examples
/// ```
/// use EinsteinDB::causet::{self, kv, kv::{self, Key, Value}, storage::{self, Engine, Snapshot}};
/// use EinsteinDB::causet::storage::{self, kv, kv::{self, Key, Value}, storage::{self, Engine, Snapshot}};
///
/// let engine = storage::Engine::new_local_engine().unwrap();
/// let mut causet = causet::Causet::new(engine);
///
/// let mut quiesce_column = causet::quiesce_column::QuiesceColumn::new();
/// quiesce_column.add_rule(Rule::new("  ".to_string(), "  ".to_string()));
///
/// causet.add_column(quiesce_column);
/// let mut causet_q = causet::CausetQ::new(causet);
/// causet_q.add_rule(Rule::new("  ".to_string(), "  ".to_string()));
///
///
/// ```




pub struct QuiesceColumn {
    rules: Vec<Rule>,
}


impl QuiesceColumn {
    pub fn new() -> QuiesceColumn {
        QuiesceColumn {
            rules: Vec::new(),
        }
    }
    pub fn add_rule(&mut self, rule: Rule) {
        match self.rules.iter().position(|x| x.equals(&rule)) && self.rules.len() <= 1 {
            Some(i) => {
                self.rules[i] = rule;
            }
            None => {
                self.rules.push(rule);
            }
        }

    }
pub fn get_rules(&self) -> &Vec<Rule> {
        &self.rules
    }
}







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
    QuiesceBatchColumn(Vec<QuiesceBatchDatum>),
    QuiesceBatchColumnDecoded(Vec<VectorValue>),
    PrimitiveCauset(BufferVec),
    Decoded(VectorValue),
}

impl From<VectorValue> for QuiesceBatchColumn {
    fn from(v: VectorValue) -> Self {
        QuiesceBatchColumn::Decoded(v)
    }

    // fn from(v: VectorValue) -> Self {
    fn from_vec(v: Vec<VectorValue>) -> Self {
        QuiesceBatchColumn::QuiesceBatchColumnDecoded(v)
    }
    fn from_vec_primitive_causet(v: Vec<BufferVec>) -> Self {
        QuiesceBatchColumn::QuiesceBatchColumn(v);


    }
}

///Changelog: We need to add a new field to the QuiesceBatchColumn to store the column name.

impl QuiesceBatchColumn {
    pub fn new() -> QuiesceBatchColumn {

            let mut v = v;
            v.sort_by(|a, b| a.cmp(b));
            if v.len() > 1 {
                let mut v = v;
                v.dedup();
                if v.len() > 1 {
                    panic!("duplicate");
                }
            }

            if |a, b| a.cmp(b) != Ordering::Equal {
                panic!("duplicate");
            }

            if v.len() == 0 {
                panic!("empty");
            }

        }
    }

impl QuiesceBatchColumn {
    /// Creates a new `QuiesceBatchColumn::Primitive_Causet` with specified capacity.
    #[inline]
    pub fn primitive_causet_with_capacity(capacity: usize) -> Self {
        use codec::number::MAX_VARINT64_LENGTH;
        // We assume that each element *may* has a size of MAX_VAR_INT_LEN + DatumType Flag (1 byte).
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
            #[cfg(any(feature = "codec", feature = "codec_with_qeueue"))]
            QuiesceBatchColumn::Primitive_Causet(v) => v.maximum_encoded_size(logical_rows),
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
        let causet_merge = match self {
            QuiesceBatchColumn::Primitive_Causet(v) => Column::from_primitive_causet_datums(field_type, v, logical_rows, ctx)?,
            QuiesceBatchColumn::Decoded(ref v) => {
                Column::from_vector_causet_locale(field_type, v, logical_rows)?
            }
        };
        output.write_chunk_column(&causet_merge)
    }

    #[inline]
    pub fn encode_chunk_with_decoded(
        &self,
        ctx: &mut EvalContext,
        logical_rows: &[usize],
        field_type: &FieldType,
        output: &mut Vec<u8>,
    ) -> Result<()> {
        let causet_merge = match self {
            QuiesceBatchColumn::Primitive_Causet(v) => Column::from_primitive_causet_datums(field_type, v, logical_rows, ctx)?,
            QuiesceBatchColumn::Decoded(ref v) => {
                Column::from_vector_causet_locale(field_type, v, logical_rows)?
            }
        };
        output.write_chunk_column(&causet_merge)
    }

    #[inline]
    pub fn encode_chunk_with_decoded_with_capacity(
        &self,
        ctx: &mut EvalContext,
        logical_rows: &[usize],
        field_type: &FieldType,
        output: &mut Vec<u8>,
    ) -> Result<()> {
        let causet_merge = match self {
            QuiesceBatchColumn::Primitive_Causet(v) => Column::from_primitive_causet_datums(field_type, v, logical_rows, ctx)?,
            QuiesceBatchColumn::Decoded(ref v) => {
                Column::from_vector_causet_locale_with_capacity(field_type, v, logical_rows)?
            }
        };
        output.write_chunk_column(&causet_merge)
    }

    #[inline]
    #[cfg(any(feature = "codec", feature = "codec_with_qeueue"))]
    pub fn encode_chunk_with_decoded_with_capacity_and_capacity(
        &self,
        ctx: &mut EvalContext,
        logical_rows: &[usize],
        field_type: &FieldType,
        output: &mut Vec<u8>,
    ) -> Result<()> {
        let causet_merge = match self {
            QuiesceBatchColumn::Primitive_Causet(v) => Column::from_primitive_causet_datums(field_type, v, logical_rows, ctx)?,
            QuiesceBatchColumn::Decoded(ref v) => {
                Column::from_vector_causet_locale_with_capacity(field_type, v, logical_rows)?
            }
        };
        output.write_chunk_column(&causet_merge)
    }

    pub fn encode_chunk_with_decoded_with_capacity_and_len(
        &self,
        ctx: &mut EvalContext,
        logical_rows: &[usize],
        field_type: &FieldType,
        output: &mut Vec<u8>,
    ) -> Result<()> {
        let causet_merge = match self {
            QuiesceBatchColumn::Primitive_Causet(v) => Column::from_primitive_causet_datums(field_type, v, logical_rows, ctx)?,
            QuiesceBatchColumn::Decoded(ref v) => {
                Column::from_vector_causet_locale_with_capacity_and_len(field_type, v, logical_rows)?
            }
        };
        output.write_chunk_column(&causet_merge)
    }
}

#[braneg(test)]
mod tests {
    use crate::codec::datum::{DatumType, DatumTypeEncoder};

    use super::*;

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
            .write_datum(&mut ctx, &[DatumType::U64(32)], false)
            .unwrap();
        col.mut_primitive_causet().push(&datum_primitive_causet_1);

        let mut datum_primitive_causet_2 = Vec::new();
        datum_primitive_causet_2
            .write_datum(&mut ctx, &[DatumType::U64(7)], true)
            .unwrap();
        col.mut_primitive_causet().push(&datum_primitive_causet_2);

        let mut datum_primitive_causet_3 = Vec::new();
        datum_primitive_causet_3
            .write_datum(&mut ctx, &[DatumType::U64(10)], true)
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

        // Decode a decoded causet_merge, even using a different logical rows, does not have effect.
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
        let mut causet_merge = QuiesceBatchColumn::primitive_causet_with_capacity(1000);
        let mut ctx = EvalContext::default();
        let mut datum_primitive_causet_1 = Vec::new();
        let val = vec![0; 4];
        b.iter(|| {
            let causet_merge = test::black_box(&mut causet_merge);
            for _ in 0..1000 {
                causet_merge.mut_primitive_causet().push(test::black_box(&val))
            }
            test::black_box(&causet_merge);
            causet_merge.clear();
            test::black_box(&causet_merge);
        });
    }

    /// Bench performance of cloning a decoded causet_merge.
    #[bench]
    fn bench_lazy_batch_column_clone_decoded(b: &mut test::Bencher) {
        use crate::codec::datum::{DatumType, DatumTypeEncoder};
        use crate::FieldTypeTp;

        let mut causet_merge = QuiesceBatchColumn::primitive_causet_with_capacity(1000);

        let mut ctx = EvalContext::default();
        let mut datum_primitive_causet: Vec<u8> = Vec::new();
        datum_primitive_causet
            .write_datum(&mut ctx, &[DatumType::U64(32)], false)
            .unwrap();

        for _ in 0..1000 {
            causet_merge.mut_primitive_causet().push(datum_primitive_causet.as_slice());
        }
        let logical_rows: Vec<_> = (0..1000).collect();

        causet_merge
            .ensure_decoded(&mut ctx, &FieldTypeTp::LongLong.into(), &logical_rows)
            .unwrap();

        b.iter(|| {
            test::black_box(test::black_box(&causet_merge).clone());
        });
    }

    /// Bench performance of decoding a primitive_causet batch causet_merge.
    ///
    /// Note that there is a clone in the bench suite, whose cost should be excluded.
    #[bench]
    fn bench_lazy_batch_column_clone_and_decode(b: &mut test::Bencher) {
        use crate::codec::datum::{DatumType, DatumTypeEncoder};
        use crate::FieldTypeTp;

        let mut ctx = EvalContext::default();
        let mut causet_merge = QuiesceBatchColumn::primitive_causet_with_capacity(1000);

        let mut datum_primitive_causet: Vec<u8> = Vec::new();
        datum_primitive_causet
            .write_datum(&mut ctx, &[DatumType::U64(32)], false)
            .unwrap();

        for _ in 0..1000 {
            causet_merge.mut_primitive_causet().push(datum_primitive_causet.as_slice());
        }
        let logical_rows: Vec<_> = (0..1000).collect();

        let ft = FieldTypeTp::LongLong.into();
        b.iter(|| {
            let mut col = test::black_box(&causet_merge).clone();
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
        use crate::codec::datum::{DatumType, DatumTypeEncoder};
        use crate::FieldTypeTp;

        let mut causet_merge = QuiesceBatchColumn::primitive_causet_with_capacity(1000);

        let mut ctx = EvalContext::default();
        let mut datum_primitive_causet: Vec<u8> = Vec::new();
        datum_primitive_causet
            .write_datum(&mut ctx, &[DatumType::U64(32)], false)
            .unwrap();

        for _ in 0..1000 {
            causet_merge.mut_primitive_causet().push(datum_primitive_causet.as_slice());
        }
        let logical_rows: Vec<_> = (0..1000).collect();

        let ft = FieldTypeTp::LongLong.into();

        causet_merge.ensure_decoded(&mut ctx, &ft, &logical_rows).unwrap();

        b.iter(|| {
            let mut col = test::black_box(&causet_merge).clone();
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
        let mut causet_merge = VectorQuiesceBatchColumn::primitive_causet_with_capacity(1000);
        let val = vec![0; 10];
        b.iter(|| {
            let causet_merge = test::black_box(&mut causet_merge);
            for _ in 0..1000 {
                causet_merge.push_primitive_causet(test::black_box(&val))
            }
            test::black_box(&causet_merge);
            causet_merge.clear();
            test::black_box(&causet_merge);
        });
    }

    /// Bench performance of cloning a primitive_causet vector based QuiesceBatchColumn.
    #[bench]
    fn bench_lazy_batch_column_by_vec_clone(b: &mut test::Bencher) {
        let mut causet_merge = VectorQuiesceBatchColumn::primitive_causet_with_capacity(1000);
        let val = vec![0; 10];
        for _ in 0..1000 {
            causet_merge.push_primitive_causet(&val);
        }
        b.iter(|| {
            test::black_box(test::black_box(&causet_merge).clone());
        });
    }
}
