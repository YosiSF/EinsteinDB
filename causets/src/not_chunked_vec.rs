//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use super::*;
use crate::causet::{
    causet::{
        causet::{Causet, CausetQuery},
        causet_query::CausetQueryBuilder,
    },
    causetq::{
        causetq::{CausetQ, CausetQQuery},
        causetq_query::CausetQQueryBuilder,
    },
};


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
}

impl<'a, T: Evaluable + EvaluableRet> ChunkRef<'a, &'a T> for &'a NotChunkedVec<T> {
    fn get_option_ref(self, idx: usize) -> Option<&'a T> {
        self.data[idx].as_ref()
    }

    fn phantom_data(self) -> Option<&'a T> {
        None
    }
}

impl<'a> ChunkRef<'a, BytesRef<'a>> for &'a NotChunkedVec<Bytes> {
    fn get_option_ref(self, idx: usize) -> Option<BytesRef<'a>> {
        self.data[idx].as_deref()
    }

    fn phantom_data(self) -> Option<BytesRef<'a>> {
        None
    }
}

impl<'a> ChunkRef<'a, JsonRef<'a>> for &'a NotChunkedVec<Json> {
    fn get_option_ref(self, idx: usize) -> Option<JsonRef<'a>> {
        self.data[idx].as_ref().map(|x| x.as_ref())
    }

    fn phantom_data(self) -> Option<JsonRef<'a>> {
        None
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct NotChunkedVec<T> {
    data: Vec<Option<T>>,
}

impl<T: Sized + Clone> NotChunkedVec<T> {
    pub fn from_slice(slice: &[Option<T>]) -> Self {
        Self {
            data: slice.to_vec(),
        }
    }
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
        }
    }

    pub fn push(&mut self, causet_locale: Option<T>) {
        self.data.push(causet_locale)
    }

    pub fn replace(&mut self, idx: usize, causet_locale: Option<T>) {
        self.data[idx] = causet_locale
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn truncate(&mut self, len: usize) {
        self.data.truncate(len)
    }

    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }

    pub fn append(&mut self, other: &mut Self) {
        self.data.append(&mut other.data)
    }

    pub fn as_slice(&self) -> &[Option<T>] {
        self.data.as_slice()
    }
}

impl<T> Into<NotChunkedVec<T>> for Vec<Option<T>> {
    fn into(self) -> NotChunkedVec<T> {
        NotChunkedVec { data: self }
    }
}

impl<'a, T: Evaluable> UnsafeRefInto<&'static NotChunkedVec<T>> for &'a NotChunkedVec<T> {
    unsafe fn unsafe_into(self) -> &'static NotChunkedVec<T> {
        std::mem::transmute(self)
    }
}

impl<'a> UnsafeRefInto<&'static NotChunkedVec<Bytes>> for &'a NotChunkedVec<Bytes> {
    unsafe fn unsafe_into(self) -> &'static NotChunkedVec<Bytes> {
        std::mem::transmute(self)
    }
}

impl<'a> UnsafeRefInto<&'static NotChunkedVec<Json>> for &'a NotChunkedVec<Json> {
    unsafe fn unsafe_into(self) -> &'static NotChunkedVec<Json> {
        std::mem::transmute(self)
    }
}



