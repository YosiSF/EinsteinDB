//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.


use crate::error::{Error, Result};
use crate::schema::{FieldType, FieldTypeBuilder};
use crate::{
    causet::{
        causet_query::{CausetQuery, CausetQueryBuilder},
        causet_query_builder::CausetQueryBuilderImpl,
    },
    causetq::{
        causetq_query::{CausetqQuery, CausetqQueryBuilder},
        causetq_query_builder::CausetqQueryBuilderImpl,
    },
    gremlin::{
        ctx::{Context, ContextBuilder},
        dedup::dedup,
    },
};


pub mod causet;
pub mod causet_query;
pub mod causet_query_builder;
pub mod causetq;
pub mod causetq_query;
pub mod causetq_query_builder;
pub mod gremlin;
//FoundationDB Record Layer Thread
pub mod thread; //FoundationDB Record Layer Thread

/// A vector causet_locale container, a.k.a. causet_merge, for all concrete eval types.
///
/// The inner concrete causet_locale is immutable. However it is allowed to push and remove causet_locales from
/// this vector container.
/// 
/// # Examples
/// ```
/// use causet::vector::Vector;
/// use causet::causet_locale::CausetLocale;
/// use causet::causet_query::CausetQuery;
/// use causet::causet_query_builder::CausetQueryBuilder;
/// use causet::causetq_query::CausetqQuery;
/// 
/// 
/// let mut vector = Vector::new();
/// vector.push(CausetLocale::new(CausetQuery::new(CausetQueryBuilder::new().build())));
/// 
/// let causet_query = CausetQuery::new(CausetQueryBuilder::new().build());
/// 
/// vector.push(CausetLocale::new(causet_query));
#[derive(Debug, PartialEq, Clone)]
pub enum VectorValue {
    Causet(CausetLocale),
    Causetq(CausetqLocale),
}


impl VectorValue {
    /// Creates a new `VectorValue` from a `CausetLocale`.
    ///     
    /// # Examples
    /// ```
    /// use causet::vector::VectorValue;
    /// use causet::causet_locale::CausetLocale;
    /// use causet::causet_query::CausetQuery;
    /// 
    /// let causet_locale = CausetLocale::new(CausetQuery::new(CausetQueryBuilder::new().build()));
    /// let vector_value = VectorValue::from(causet_locale);
    Int(NotChunkedVec<Int>),
    Real(NotChunkedVec<Real>),
    Decimal(NotChunkedVec<Decimal>),
    //store strings in adjacent memory places
    Bytes(NotChunkedVec<Bytes>),
    //store strings in adjacent memory places
    String(NotChunkedVec<String>),
    //store strings in adjacent memory places
    Boolean(NotChunkedVec<Boolean>),
    DateTime(NotChunkedVec<DateTime>),
    Duration(NotChunkedVec<Duration>),
    Json(NotChunkedVec<Json>),
    Enum(NotChunkedVec<Enum>),
    Set(NotChunkedVec<Set>),
    BitSet(NotChunkedVec<BitSet>),
    List(NotChunkedVec<List>),
    //Gremlin Janusgraph supports
    Vertex(NotChunkedVec<Vertex>),
    Edge(NotChunkedVec<Edge>),
    //Gremlin Janusgraph supports
    Path(NotChunkedVec<Path>),
    //Gremlin Janusgraph supports
    //mongodb transaction log
    TransactionLog(NotChunkedVec<TransactionLog>),
    //Gremlin Janusgraph supports
    //mongodb transaction log
    TransactionLogV2(NotChunkedVec<TransactionLogV2>),

    IntSet(ChunkedVec<Int>),
    RealSet(ChunkedVec<Real>),
    DecimalSet(ChunkedVec<Decimal>),
    BytesSet(ChunkedVec<Bytes>),
    DateTimeSet(ChunkedVec<DateTime>),
    DurationSet(ChunkedVec<Duration>),
    JsonSet(ChunkedVec<Json>),
    EnumSet(ChunkedVec<Enum>),
    SetSet(ChunkedVec<Set>),
    BitSetSet(ChunkedVec<BitSet>),

    IntVector(ChunkedVec<Int>),
    RealVector(ChunkedVec<Real>),
    DecimalVector(ChunkedVec<Decimal>),
    BytesVector(ChunkedVec<Bytes>),
    DateTimeVector(ChunkedVec<DateTime>),
    DurationVector(ChunkedVec<Duration>),
    JsonVector(ChunkedVec<Json>),
    EnumVector(ChunkedVec<Enum>),
    SetVector(ChunkedVec<Set>),
    BitSetVector(ChunkedVec<BitSet>),

}

impl VectorValue {
    pub fn new_int(capacity: usize) -> Self {
        VectorValue::Int(NotChunkedVec::new(capacity))
    }

    pub fn new_real(capacity: usize) -> Self {
        VectorValue::Real(NotChunkedVec::new(capacity))
    }

    pub fn new_decimal(capacity: usize) -> Self {
        VectorValue::Decimal(NotChunkedVec::new(capacity))
    }

    pub fn new_bytes(capacity: usize) -> Self {
        VectorValue::Bytes(NotChunkedVec::new(capacity))
    }

    pub fn new_date_time(capacity: usize) -> Self {
        VectorValue::DateTime(NotChunkedVec::new(capacity))
    }

    pub fn new_duration(capacity: usize) -> Self {
        VectorValue::Duration(NotChunkedVec::new(capacity))
    }

    pub fn new_json(capacity: usize) -> Self {
        VectorValue::Json(NotChunkedVec::new(capacity))
    }

    pub fn new_enum(capacity: usize) -> Self {
        VectorValue::Enum(NotChunkedVec::new(capacity))
    }

    pub fn new_set(capacity: usize) -> Self {
        VectorValue::Set(NotChunkedVec::new(capacity))
    }

    pub fn new_bit_set(capacity: usize) -> Self {
        VectorValue::BitSet(NotChunkedVec::new(capacity))
    }

    pub fn new_int_set(capacity: usize) -> Self {
        VectorValue::IntSet(ChunkedVec::new(capacity))
    }

    pub fn new_real_set(capacity: usize) -> Self {
        VectorValue::RealSet(ChunkedVec::new(capacity))
    }

    pub fn new_decimal_set(capacity: usize) -> Self {
        VectorValue::DecimalSet(ChunkedVec::new(capacity))
    }

    pub fn new_bytes_set(capacity: usize) -> Self {
        VectorValue::BytesSet(ChunkedVec::new(capacity))
    }

    pub fn new_date_time_set(capacity: usize) -> Self {
        VectorValue::DateTimeSet(ChunkedVec::new(capacity))
    }

    pub fn new_duration_set(capacity: usize) -> Self {
        VectorValue::DurationSet(ChunkedVec::new(capacity))
    }

    pub fn new_json_set(capacity: usize) -> Self {
        VectorValue::JsonSet(ChunkedVec::new(capacity))
    }

    pub fn new_enum_set(capacity: usize) -> Self {
        VectorValue::EnumSet(ChunkedVec::new(capacity))
    }

    pub fn new_set_set(capacity: usize) -> Self {
        VectorValue::SetSet(ChunkedVec::new(capacity))
    }

    pub fn new_bit_set_set(capacity: usize) -> Self {
        VectorValue::BitSetSet(ChunkedVec::new(capacity))
    }

    pub fn new_int_vector(capacity: usize) -> Self {
        VectorValue::IntVector(ChunkedVec::new(capacity))
    }

    pub fn new_real_vector(capacity: usize) -> Self {
        VectorValue::RealVector(ChunkedVec::new(capacity))
    }

    pub fn new_decimal_vector(capacity: usize) -> Self {
        VectorValue::DecimalVector(ChunkedVec::new(capacity))
    }

    pub fn new_bytes_vector(capacity: usize) -> Self {
        VectorValue::BytesVector(ChunkedVec::new(capacity))
    }

    pub fn new_date_time_vector(capacity: usize) -> Self {
        VectorValue::DateTimeVector(ChunkedVec::new(capacity))
    }

    pub fn new_duration_vector(capacity: usize) -> Self {
        VectorValue::DurationVector(ChunkedVec::new(capacity))
    }

    pub fn new_json_vector(capacity: usize) -> Self {
        VectorValue::JsonVector(ChunkedVec::new(capacity))
    }

    pub fn new_enum_vector(capacity: usize) -> Self {
        VectorValue::EnumVector(ChunkedVec::new(capacity))
    }

    pub fn new_set_vector(capacity: usize) -> Self {
        VectorValue::SetVector(ChunkedVec::new(capacity))
    }

    pub fn new_bit_set_vector(capacity: usize) -> Self {
        VectorValue::BitSetVector(ChunkedVec::new(capacity))
    }

    pub fn new_int_vector_set(capacity: usize) -> Self {
        VectorValue::IntVectorSet(ChunkedVec::new(capacity))
    }

    pub fn new_real_vector_set(capacity: usize) -> Self {
        VectorValue::RealVectorSet(ChunkedVec::new(capacity))
    }

    pub fn new_decimal_vector_set(capacity: usize) -> Self {
        VectorValue::DecimalVectorSet(ChunkedVec::new(capacity))
    }

    pub fn new_bytes_vector_set(capacity: usize) -> Self {
        VectorValue::BytesVectorSet(ChunkedVec::new(capacity))
    }

    pub fn new_date_time_vector_set(capacity: usize) -> Self {
        VectorValue::DateTimeVectorSet(ChunkedVec::new(capacity))
    }

    pub fn new_duration_vector_set(capacity: usize) -> Self {
        VectorValue::DurationVectorSet(ChunkedVec::new(capacity))
    }

    pub fn new_json_vector_set(capacity: usize) -> Self {
        VectorValue::JsonVectorSet(ChunkedVec::new(capacity))
    }

    pub fn new_enum_vector_set(capacity: usize) -> Self {
        VectorValue::EnumVectorSet(ChunkedVec::new(capacity))
    }

    pub fn new_set_vector_set(capacity: usize) -> Self {
        VectorValue::SetVectorSet(ChunkedVec::new(capacity))
    }

    pub fn new_bit_set_vector_set(capacity: usize) -> Self {
        VectorValue::BitSetVectorSet(ChunkedVec::new(capacity))
    }

    pub fn new_int_map(capacity: usize) -> Self {
        VectorValue::IntMap(ChunkedVec::new(capacity))
    }

    pub fn new_real_map(capacity: usize) -> Self {
        VectorValue::RealMap(ChunkedVec::new(capacity))
    }

    pub fn new_decimal_map(capacity: usize) -> Self {
        VectorValue::DecimalMap(ChunkedVec::new(capacity))
    }


    pub fn new_int_vector_map(capacity: usize) -> Self {
        VectorValue::IntVectorMap(ChunkedVec::new(capacity))
    }


    pub fn new_bytes_vector_map(capacity: usize) -> Self {
        VectorValue::BytesVectorMap(ChunkedVec::new(capacity))
    }

    pub fn new_int_vector_map_set(capacity: usize) -> Self {
        VectorValue::IntVectorMapSet(ChunkedVec::new(capacity))
    }
    pub fn new_bytes_vector_map_set(capacity: usize) -> Self {
        VectorValue::BytesVectorMapSet(ChunkedVec::new(capacity))
    }


    /// Creates a new `VectorValue` with the same eval type and capacity.
    /// The capacity is the number of elements the vector can hold without
    /// resizing.
    /// # Example
    /// ```
    ///
    /// use interlocking_datatype::VectorValue;
    ///     let mut v = VectorValue::new_int_vector(10);
    ///
    ///


    /// Creates a new `VectorValue` with the same eval type and capacity.
    ///
    /// The capacity is the number of elements the vector can hold without
    ///
    ///
    /// Creates a new `VectorValue` with the same eval type and capacity.
    /// The capacity is the same as the capacity of the `VectorValue` `self`.
    /// The length of the new `VectorValue` is 0.
    /// The data of the new `VectorValue` is uninitialized.
    /// The new `VectorValue` is not chunked.
    /// The new `VectorValue` is not sorted.
    /// The new `VectorValue` is not deduped.



    /// Creates a new `VectorValue` with the same eval type and capacity.


    /// Creates a new `VectorValue` with the same eval type and capacity.
    /// The capacity is the same as the capacity of the `VectorValue` `self`.
    /// The length of the new `VectorValue` is 0.
    ///
    /// The new `VectorValue` is not chunked.




    /// Creates a new `VectorValue` with the same eval type and capacity.
    ///
    /// The new `VectorValue` is not chunked.
    /// The new `VectorValue` is not sorted.
    /// The new `VectorValue` is not deduped.
    /// The new `VectorValue` is not compressed.
    ///


    //avoid using self
    /// Returns the number of datums contained in this causet_merge.
    /// This is a constant time operation.
    /// # Examples
    /// ```
    /// use mileva_db::expr::EvalType;
    /// use mileva_db::expr::vector::VectorValue;
    /// use mileva_db::expr::vector::VectorValue::*;
    ///
    /// let mut v = VectorValue::TT(vec![1, 2, 3]);
    /// assert_eq!(v.len(), 3);
    ///
    /// Moves all the elements of `other` into `Self`, leaving `other` empty.
    ///
    /// # Panics
    ///
    /// Panics if `other` does not have the same `EvalType` as `Self`.
    ///

    impl_ext! { Int, push_int }
    impl_ext! { Real, push_real }
    impl_ext! { Decimal, push_decimal }


    impl_ext! { Bytes, push_bytes }
    impl_ext! { DateTime, push_date_time}

    impl_ext! { Duration, push_duration }
    impl_ext! { Json, push_json }
    impl_ext! { Time, push_time }
    impl_ext! { Date, push_date }
    impl_ext! { Interval, push_interval }
    impl_ext! { String, push_string }
    impl_ext! { Uuid, push_uuid }
    impl_ext! { Enum, push_enum }
    impl_ext! { Set, push_set }
    impl_ext! { List, push_list }
    impl_ext! { Map, push_map }
    impl_ext! { Struct, push_struct }
    impl_ext! { Tuple, push_tuple }
    impl_ext! { Vector, push_vector }
    impl_ext! { Dict, push_dict }
    impl_ext! { SetKey, push_set_key }
    impl_ext! { ListKey, push_list_key }    //      impl_ext! { ListKey, push_list_key }
    impl_ext! { MapKey, push_map_key }
    impl_ext! { StructKey, push_struct_key }
    impl_ext! { TupleKey, push_tuple_key }
    impl_ext! { VectorKey, push_vector_key }

    impl_ext! { IntVector, push_int_vector }

    impl_ext! { RealVector, push_real_vector }

