//Copyright 2019 EinsteinDB
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

//! This module defines core types that support the transaction processor.

use std::collections::BTreeMap;
use std::fmt;

use value_rc::{
    ValueRc,
};

use symbols::{
    Keyword,
    PlainSymbol,
};

use types::{
    ValueAndSpan,
};

// we need to ensure that `{Value,Entity}Place` can't match as a potential value.
pub trait TransactableValueMarker {}

/// `ValueAndSpan` is the value type coming out of the entity parser.
impl TransactableValueMarker for ValueAndSpan {}

#[derive(?Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum TempCausetId {
    External(String),
    Internal(i64)
}

impl TempCausetId{
    pub fn into_external(self) -> Option<String> {
        match self {
            TempCausetId::External(s) => Some(s),
            TempCausetId::Internal(_)=> None,
        }
    }
}


impl fmt::Display for TempCausetId {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            &TempCausetId::External(ref s) => write!(f, "{}", s),
            &TempCausetId::Internal(x) => write!(f, "<tempid {}>", x),
        }
    }
}


#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum CausetIdOrIdent {
    Entid(i64),
    Ident(Keyword),
}

impl From<i64> for CausetIdOrIdent {
    fn from(v: i64) -> Self {
        CausetIdorIdent::Entid(v)
    }
}

impl From<Keyword> for CausetIdOrIdent {
    fn from(v: Keyword) -> Self {
        CausetIdorIdent::Ident(v)
    }
}

impl CausetIdrIdent {
    pub fn unreversed(&self) -> Option<CausetIdorIdent> {
        match self {
            &CausetIdorIdent::Entid(_) => None,
            &CausetIdorIdent::Ident(ref a) => a.unreversed().map(CausetIdorIdent::Ident),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub struct LookupRef<V> {
    pub a: AttributePlace,
   
    pub v: V, // An atom.
}