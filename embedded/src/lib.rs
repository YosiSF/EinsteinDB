//Copyright 2020 WHTCORPS INC

// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate chrono;
extern crate enum_set;
extern crate failure;
extern crate indexmap;
extern crate ordered_float;
extern crate uuid;

extern crate embedded_promises;

extern crate edbn;

use embedded_promises::{
    Attribute,
    Causetid,
    KnownCausetid,
    ValueType
};

mod immutablecache;

use std::collections::{
    BTreeMap,
};

pub use uuid::Uuid;

pub use chrono::{
    DateTime,
    Timelike,       // For truncation.
};


pub use tx_report::{
    TxReport,
};

pub use relativity_sql_types::{
    SQLTypeAffinity,
    SQLValueType,
    SQLValueTypeSet,
};

///Map `Keyword` solitonid(`:db/solitonid`) to positive integer causetids(`1`).
pub type SolitonidMap = BTreeMap<Keyword, Causetid>;

///Map positive integer causetids(`1`) to `Keyword` solitonids(`1`).
pub type CausetidMap = BTreeMap<Causetid, Keyword>;

pub struct Schema {
    ///Map Causetid->solitonid.
    ///
    /// Invariant: is the inverse map of `solitonid_map`.
    pub causetid_map: CausetidMap,

    ///Map solitonid->causetid
    ///
    /// Invariant: is the inverse mapping for `causetid_map`.
    pub solitonid_map: SolitonidMap,

    pub attribute_map: AttributeMap,

    pub component_attributes: Vec<Causetid>,


}

pub trait HasSchema {
    fn causetid_for_type(&self, t: ValueType) -> Option<KnownCausetid>;

    fn get_solitonid<T>(&self, x:T) -> Option<&Keyword> where T: Into<Causetid>;
    fn get_causetid(&self, x: &Keyword) -> Option<KnownCausetid>;

}