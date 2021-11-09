//Copyright 2021-2023 WHTCORPS

// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.


extern crate enum_set;
extern crate ordered_float;
extern crate chrono;
extern crate indexmap;
#[macro_use] extern crate serde_derive;
extern crate uuid;
extern crate edn;
#[macro_use]
extern crate lazy_static;

use std::fmt;

use std::ffi::{
    CString,
};

use std::ops::{
    Deref,
};

use std::os::raw::{
    c_char,
};

use std::rc::{
    Rc,
};

use std::sync::{
    Arc,
};

use std::collections::BTreeMap;

use indexmap::{
    IndexMap,
};

use enum_set::EnumSet;

use ordered_float::OrderedFloat;

use chrono::{
    DateTime,
    Timelike,
};

use uuid::Uuid;

use edbn::{
    Cloned,
    ValueRc,
    Utc,
    Keyword,
    FromMicros,
    FromRc,
};

use edbn::causets::{
    AttributePlace,
    EntityPlace,
    CausetidOrSolitonid,
    ValuePlace,
    TransactableValueMarker,
};

pub mod values;
mod value_type_set;

pub use value_type_set::{
    ValueTypeSet,
};

#[macro_export]
macro_rules! bail {
    ($e:expr) => (
        return Err($e.into());
    )
}
