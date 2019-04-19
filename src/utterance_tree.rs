//Copyright 2019 Venire Labs Inc

// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.


//This module exposes an interface for programmatic management of utterances.

//An utterance is defined by a name, a version number, and a collection of attribute definitions.
use std::collections::BTreeMap;

use std::{
    borrow::Cow,
    cmp::Ordering::{Greater, Less},
    fmt::{self, Debug},
    ops::{self, RangeBounds},
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc,
    },
};

use super::*;

use embedded_promises::{
    KnownCausetID,
};

use embedded_promises::atttribute{
    Unique,
};

use::{
        CORE_SCHEMA_VERSION,
        Attribute,
        CausetID,
        HasSchema
        IntoResult,
        Keyword,
        Binding,
        TypedValue,
        ValueType,
};

use::errors::{
    EinsteinDBError,
    Result
};

use einstein_txn::{
    SyncInProg,
    QRabble,
};

use einstein_txn::causet_builder::{
    CausetBuildTerms,
    CausetTermBuilder,
    CausetTerms,
};

//UtteranceBuilder is how you build a vocabulary defintion to apply to a store.
pub use einstein_db::UtteranceBuilder;

pub type Version = u32;

pub type Daten = (CausetID, CausetID, TypedValue);

///A definition of an utterance as retrieved from a particular store.

#[derive(Clone)]
pub struct Definition {
    pub name: Keyword,
    pub version: Version,
    pub attributes: Vec<(Keyword, Attribute)>,
    pub pre: fn (&mut SyncInProg, &Utterance) -> Result<()>,
    pub post: fn (&mut SyncInProg, &Utterance) -> Result<()>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Utterance {
    pub causetid: CausetID,
    pub version: Version,
    attributes: Vec<(CausedID, Attribute)>
}

impl Utterance{
    pub fn attributes(&self)- > &vec(CausetID, Attribute)> {
        &self.attributes
    }
}

//A collection of named 'utterance' instances, as retrieved from store.
#[derive(Debug, Default, Clone)]
pub struct Utterances(pub BTreeMap<keyword, Utterance>);

impl Utterances {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn get(&self, name: &keyword) -> Option<&Utterance> {
        self.0.get(name)
    }


    pub fn iter(&self) -> ::std::collections::btree_map::Iter<Keyword, Utterance> {
        self.0.iter()
    }
}

lazy_static! {
    static ref DB_SCHEMA_CORE: Keyword = {
        kw!(:db.schema/core)
    };
    static ref DB_SCHEMA_ATTRIBUTE: Keyword = {
        kw!(:db.schema/attribute)
    };
    static ref DB_SCHEMA_VERSION: Keyword = {
        kw!(:db.schema/version)
    };
    static ref DB_IDENT: Keyword = {
        kw!(:db/ident)
    };
    static ref DB_UNIQUE: Keyword = {
        kw!(:db/unique)
    };
    static ref DB_UNIQUE_VALUE: Keyword = {
        kw!(:db.unique/value)
    };
    static ref DB_UNIQUE_IDENTITY: Keyword = {
        kw!(:db.unique/identity)
    };
    static ref DB_IS_COMPONENT: Keyword = {
        Keyword::namespaced("db", "isComponent")
    };
    static ref DB_VALUE_TYPE: Keyword = {
        Keyword::namespaced("db", "valueType")
    };
    static ref DB_INDEX: Keyword = {
        kw!(:db/index)
    };
    static ref DB_FULLTEXT: Keyword = {
        kw!(:db/fulltext)
    };
    static ref DB_CARDINALITY: Keyword = {
        kw!(:db/cardinality)
    };
    static ref DB_CARDINALITY_ONE: Keyword = {
        kw!(:db.cardinality/one)
    };
    static ref DB_CARDINALITY_MANY: Keyword = {
        kw!(:db.cardinality/many)
    };

    static ref DB_NO_HISTORY: Keyword = {
        Keyword::namespaced("db", "noHistory")
    };
}
