//Copyright 2020 WHTCORPS

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

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum TempId {
    External(String),
    Internal(i64),
}

impl TempId {
    pub fn into_external(self) -> Option<String> {
        match self {
            TempId::External(s) => Some(s),
            TempId::Internal(_) => None,
        }
    }
}

impl fmt::Display for TempId {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            &TempId::External(ref s) => write!(f, "{}", s),
            &TempId::Internal(x) => write!(f, "<tempid {}>", x),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum CausetidOrSolitonid {
    Causetid(i64),
    Solitonid(Keyword),
}

impl From<i64> for CausetidOrSolitonid {
    fn from(v: i64) -> Self {
        CausetidOrSolitonid::Causetid(v)
    }
}

impl From<Keyword> for CausetidOrSolitonid {
    fn from(v: Keyword) -> Self {
        CausetidOrSolitonid::Solitonid(v)
    }
}

impl CausetidOrSolitonid {
    pub fn unreversed(&self) -> Option<CausetidOrSolitonid> {
        match self {
            &CausetidOrSolitonid::Causetid(_) => None,
            &CausetidOrSolitonid::Solitonid(ref a) => a.unreversed().map(CausetidOrSolitonid::Solitonid),
        }
    }
}
