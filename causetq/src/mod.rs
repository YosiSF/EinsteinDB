//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

//! Builder utilities for making type representations. Currently only includes
//! `FieldTypeBuilder` for building the `FieldType` protobuf message.

mod field_type;

pub use self::field_type::FieldTypeBuilder;

//gremlin queries for causetq

mod ctx;
mod dedup; // a deduping function for gremlin queries
//einsteinml lisp
crate use self::ctx::{Context, ContextBuilder};
crate use self::dedup::dedup;


//gremlin queries for causet
//Peek, Obtain Lease, Complete, and Enqueue operations.
mod causet;
mod causet_query;
mod causet_query_builder;
mod causet_query_builder_impl;


//gremlin queries for causetq
//Peek, Obtain Lease, Complete, and Enqueue operations.
mod causetq;
mod causetq_query;
mod causetq_query_builder;











