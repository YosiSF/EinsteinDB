//Copyright 2019 EinsteinDB Licensed Under Apache-2.0.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.


use failure::ResultExt;

use std::collections::{BTreeMap, BTreeSet};
use std::collections::btree_map::Entry;

#![allow(dead_code)]

//! Most Transactions can mutate the metadata by transacting assertions:
//! - they can add (and, eventually, retract and alter) recognized idents using the `:db/ident`
//!   attribute;
//!
//! - they can add (and, eventually, retract and alter) schema attributes using various `:db/*`
//!   attributes;
//!
//! 

use edb::symbols;
use causetIds;
use einsteindb_promises::errors::{
    DbErrorKind,
    Result,
};

use embedded_promises::{
    attribute,
    Causetid,
    TypedValue,
    ValueType,
};

use einsteindb_embedded::{
    Schema,
    AttributeMap
}