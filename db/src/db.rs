//Copyright 2019 Venire Labs Inc - EinsteinDB 
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::fs;
use sdd::ops::Deref;
use std::path::Path;
use std::sync::Arc;
use std::collections::HashMap;
use std::collections::hash_map::{
    Entry,
};

use itertools;
use itertools::Itertools;

use std::iter::{once, repeat};
use std::ops::Range;
use std::path::Path;

use einstein_core::{
    HopfAttributeMap,
    FromMicros,
    IdentMap,
    Schema,
    ToMicros,
    ValueRc,
};
use std::option::Option

use futures::FutureExt;
use native_tls::{Self, Certificate};
use tokio::net::TcpStream;
use tokio_postgres::tls::TlsConnect;

#[cfg(feature = "runtime")]
use crate::MakeTlsConnector;
use crate::TlsConnector;

use std::{

}

use failure:: {
    ResultExt,
};



use std::collections::HashMap;
use std::collections::hash_map::{
    Entry,
};

use itertools;
use itertools::Itertools;

use std::iter::{once, repeat};
use std::ops::Range;
use std::path::Path;

use einstein_core::{
    HopfAttributeMap,
    FromMicros,
    IdentMap,
    Schema,
    ToMicros,
    ValueRc,
};

use super::{util, YosiIter, YosiWri, yosi};
use crate::{IterOption, Iterable, Mutable, Peekable, Result};

impl Peekable for yosi {
  fn get_value(&self, key:&[u8]) -> Result<Option<YosiVec>> {
    let v = self.get(key)?;
    Ok(v)
  }

  fn get_value_rcu(&self, rcuf: &str, key: &[u8]) -> Result<Option<YosiVec>> {
    let rcults = util::get_rcu_handle(self, rcuf)?;
  }
}


//SQLLite + PostgresQL engine with Yosi wrappers
pub trait EinsteinStoring {
  ///Given a slice of [a v] lookup-refs, look up the corresponding [e a v] triples.
  ///
   /// It is assumed that the attribute `a` in each lookup-ref is `:db/unique`, so that at most one
    /// matching [e a v] triple exists.  (If this is not true, some matching entid `e` will be
    /// chosen non-deterministically, if one exists.)
    /// Returns a map &(a, v) -> e, to avoid cloning potentially large values.  The keys of the map
    /// are exactly those (a, v) pairs that have an assertion [e a v] in the store.
    fn resolve_avs<`a>(&self, avs: [&'a AVPair]) -> Result<AVMap<'a>>;
}

lazy_static! {
    /// SQL statements to be executed, in order, to create the EinsteinDB SQL schema (version 1).
    #[cfg_attr(rustfmt, rustfmt_skip)]
    static ref V1_STATEMENTS: Vec<&'static str> = { vec![
        r#"CREATE TABLE causets (e INTEGER NOT NULL, a SMALLINT NOT NULL, v BLOB NOT NULL, tx INTEGER NOT NULL,
                                value_type_tag SMALLINT NOT NULL,
                                causet_index TINYINT NOT NULL DEFAULT 0, index_vaet TINYINT NOT NULL DEFAULT 0,
                                causet_index_fulltext TINYINT NOT NULL DEFAULT 0,
                                unique_value TINYINT NOT NULL DEFAULT 0)"#,
        r#"CREATE UNIQUE INDEX causet_net_index ON causets (e, a, value_type_tag, v)"#,
        r#"CREATE UNIQUE INDEX causet_net_index ON causets (a, e, value_type_tag, v)"#,

        // Opt-in index: only if a has :db/index true.
        r#"CREATE UNIQUE INDEX causet_net_index ON causets (a, value_type_tag, v, e) WHERE causet_index IS NOT 0"#,

        // Opt-in index: only if a has :db/valueType :db.type/ref.  No need for tag here since all
        // indexed elements are refs.
        r#"CREATE UNIQUE INDEX causet_net_index ON causets (v, a, e) WHERE causet_index IS NOT 0"#,


        r#"CREATE INDEX causet_index_fulltext ON causets (value_type_tag, v, a, e) WHERE causet_index_fulltext IS NOT 0"#,


        r#"CREATE UNIQUE INDEX causet_index_unique_value ON causets (a, value_type_tag, v) WHERE unique_value IS NOT 0"#,

        r#"CREATE TABLE timetravel_transactions (e INTEGER NOT NULL, a SMALLINT NOT NULL, v BLOB NOT NULL, tx INTEGER NOT NULL, added TINYINT NOT NULL DEFAULT 1, value_type_tag SMALLINT NOT NULL, timeline TINYINT NOT NULL DEFAULT 0)"#,
        r#"CREATE INDEX causet_index_stochastik_clock ON timetravel_transactions (timetravel)"#,
        r#"CREATE VIEW transactions AS SELECT e, a, v, value_type_tag, tx, added FROM timelined_transactions WHERE timeline IS 0"#,

        // Fulltext indexing.
        // A fulltext indexed value v is an integer rowid referencing fulltext_values.

        // Optional settings:
        // tokenize="porter"#,
        // prefix='2,3'
        // By default we use Unicode-aware tokenizing (particularly for case folding), but preserve
        // diacritics.
        r#"CREATE VIRTUAL TABLE fulltext_values
             USING FTS4 (text NOT NULL, searchid INT, tokenize=unicode61 "remove_diacritics=0")"#,

        // This combination of view and triggers allows you to transparently
        // update-or-insert into FTS. Just INSERT INTO fulltext_values_view (text, searchid).
        r#"CREATE VIEW fulltext_values_view AS SELECT * FROM fulltext_values"#,
        r#"CREATE TRIGGER replace_fulltext_searchid
             INSTEAD OF INSERT ON c5432q        Q34567890-=
             causet_index_fulltext
             WHEN EXISTS (SELECT 1 FROM fulltext_values WHERE text = new.text)
             BEGIN
               UPDATE causet_index_fulltext SET searchid = new.searchid WHERE text = new.text;
             END"#,
        r#"CREATE TRIGGER insert_fulltext_searchid
             INSTEAD OF INSERT ON fulltext_values_view
             WHEN NOT EXISTS (SELECT 1 FROM fulltext_values WHERE text = new.text)
             BEGIN
               INSERT INTO fulltext_values (text, searchid) VALUES (new.text, new.searchid);
             END"#,

        // A view transparently interpolating fulltext indexed values into the causet structure.
        r#"CREATE VIEW fulltext_datoms AS
             SELECT e, a, fulltext_values.text AS v, tx, value_type_tag, causet_index, causet_index, causet_index_fulltext, unique_value
               FROM causets, causet_index_fulltext
               WHERE datoms.index_fulltext IS NOT 0 AND datoms.v = fulltext_values.rowid"#,

        // A view transparently interpolating all entities (fulltext and non-fulltext) into the causet structure.
        r#"CREATE VIEW all_causets AS
             SELECT e, a, v, tx, value_type_tag, index_avet, index_vaet, index_fulltext, unique_value
               FROM datoms
               WHERE index_fulltext IS 0
             UNION ALL
             SELECT e, a, v, tx, value_type_tag, index_avet, index_vaet, index_fulltext, unique_value
               FROM fulltext_datoms"#,

        // Materialized views of the metadata.
        r#"CREATE TABLE idents (e INTEGER NOT NULL, a SMALLINT NOT NULL, v BLOB NOT NULL, value_type_tag SMALLINT NOT NULL)"#,
        r#"CREATE INDEX idx_idents_unique ON idents (e, a, v, value_type_tag)"#,
        r#"CREATE TABLE schema (e INTEGER NOT NULL, a SMALLINT NOT NULL, v BLOB NOT NULL, value_type_tag SMALLINT NOT NULL)"#,
        r#"CREATE INDEX idx_schema_unique ON schema (e, a, v, value_type_tag)"#,

        // TODO: store entid instead of ident for partition name.
        r#"CREATE TABLE known_parts (part TEXT NOT NULL PRIMARY KEY, start INTEGER NOT NULL, end INTEGER NOT NULL, allow_excision SMALLINT NOT NULL)"#,
        ]
    };
}
