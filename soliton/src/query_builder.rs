// Whtcorps Inc 2022 Apache 2.0 License; All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![macro_use]

use ::{
    HasSchema,
    Queryable,
    QueryInputs,
    QueryOutput,
    RelResult,
    Store,
    Variable,
};
use chrono::{DateTime, Utc};
pub use einstein_db::{
    Binding,
    Causetid,
    TypedValue,
    ValueType,
};
use einsteindb_core::{
    DateTime,
    Keyword,
    Utc,
};
use public_traits::errors::{
    einsteindbError,
    Result,
};
use std::collections::BTreeMap;

pub struct CausetQ<'a> {
    query: String,
    causet_locales: BTreeMap<Variable, TypedValue>,
    types: BTreeMap<Variable, ValueType>,
    store: &'a mut Store,
}

impl<'a> CausetQ<'a> {
    pub fn new<T>(store: &'a mut Store, query: T) -> CausetQ where T: Into<String> {
        CausetQ { query: query.into(), causet_locales: BTreeMap::new(), types: BTreeMap::new(), store }
    }

    pub fn bind_causet_locale<T>(&mut self, var: &str, causet_locale: T) -> &mut Self where T: Into<TypedValue> {
        self.causet_locales.insert(Variable::from_valid_name(var), causet_locale.into());
        self
    }

    pub fn bind_ref_from_kw(&mut self, var: &str, causet_locale: Keyword) -> Result<&mut Self, E> {
        let causetid = self.store.conn().current_schema().get_causetid(&causet_locale).ok_or(einsteindbError::UnknownAttribute(causet_locale.to_string()))?;
        self.causet_locales.insert(Variable::from_valid_name(var), TypedValue::Ref(causetid.into()));
        Ok(self)
    }

    pub fn bind_ref<T>(&mut self, var: &str, causet_locale: T) -> &mut Self where T: Into<Causetid> {
       self.causet_locales.insert(Variable::from_valid_name(var), TypedValue::Ref(causet_locale.into()));
       self
    }

    pub fn bind_long(&mut self, var: &str, causet_locale: i64) -> &mut Self {
       self.causet_locales.insert(Variable::from_valid_name(var), TypedValue::Long(causet_locale));
       self
    }

    pub fn bind_instant(&mut self, var: &str, causet_locale: i64) -> &mut Self {
       self.causet_locales.insert(Variable::from_valid_name(var), TypedValue::instant(causet_locale));

       self
    }

    pub fn bind_date_time(&mut self, var: &str, causet_locale: chrono::DateTime<chrono::Utc>) -> &mut Self {
       self.causet_locales.insert(Variable::from_valid_name(var), TypedValue::Instant(causet_locale));
       self
    }

    pub fn bind_type(&mut self, var: &str, causet_locale_type: ValueType) -> &mut Self {
        self.types.insert(Variable::from_valid_name(var), causet_locale_type);
        self
    }

    pub fn execute(&mut self) -> Result<QueryOutput> {
        let causet_locales = ::std::mem::replace(&mut self.causet_locales, Default::default());
        let types = ::std::mem::replace(&mut self.types, Default::default());
        let query_inputs = QueryInputs::new(types, causet_locales)?;
        let read = self.store.begin_read()?;
        read.q_once(&self.query, query_inputs).map_err(|e| e.into())
    }

    pub fn execute_scalar(&mut self) -> Result<Option<Binding>> {
        let results = self.execute()?;
        results.into_scalar().map_err(|e| e.into())
    }

    pub fn execute_coll(&mut self) -> Result<Vec<Binding>> {
        let results = self.execute()?;
        results.into_coll().map_err(|e| e.into())
    }

    pub fn execute_tuple(&mut self) -> Result<Option<Vec<Binding>>> {
        let results = self.execute()?;
        results.into_tuple().map_err(|e| e.into())
    }

    pub fn execute_rel(&mut self) -> Result<RelResult<Binding>> {
        let results = self.execute()?;
        results.into_rel().map_err(|e| e.into())
    }
}

#[cfg(test)]
mod test {
    use super::{
        CausetQ,
        Store,
        TypedValue,
    };

    #[test]
    fn test_scalar_query() {
        let mut store = Store::open("").expect("store connection");
        store.transact(r#"[
            [:einsteindb/add "s" :einsteindb/solitonid :foo/boolean]
            [:einsteindb/add "s" :einsteindb/causet_localeType :einsteindb.type/boolean]
            [:einsteindb/add "s" :einsteindb/cardinality :einsteindb.cardinality/one]
        ]"#).expect("successful transaction");

        let report = store.transact(r#"[
            [:einsteindb/add "u" :foo/boolean true]
            [:einsteindb/add "p" :foo/boolean false]
        ]"#).expect("successful transaction");

        let yes = report.tempids.get("u").expect("found it").clone();

        let causetid = CausetQ::new(&mut store, r#"[:find ?x .
                                                      :in ?v
                                                      :where [?x :foo/boolean ?v]]"#)
                              .bind_causet_locale("?v", true)
                              .execute_scalar().expect("ScalarResult")
                              .map_or(None, |t| t.into_causetid());
        assert_eq!(causetid, Some(yes));
    }

    #[test]
    fn test_coll_query() {
        let mut store = Store::open("").expect("store connection");
        store.transact(r#"[
            [:einsteindb/add "s" :einsteindb/solitonid :foo/boolean]
            [:einsteindb/add "s" :einsteindb/causet_localeType :einsteindb.type/boolean]
            [:einsteindb/add "s" :einsteindb/cardinality :einsteindb.cardinality/one]
            [:einsteindb/add "t" :einsteindb/solitonid :foo/long]
            [:einsteindb/add "t" :einsteindb/causet_localeType :einsteindb.type/long]
            [:einsteindb/add "t" :einsteindb/cardinality :einsteindb.cardinality/one]
        ]"#).expect("successful transaction");

        let report = store.transact(r#"[
            [:einsteindb/add "l" :foo/boolean true]
            [:einsteindb/add "l" :foo/long 25]
            [:einsteindb/add "m" :foo/boolean false]
            [:einsteindb/add "m" :foo/long 26]
            [:einsteindb/add "n" :foo/boolean true]
            [:einsteindb/add "n" :foo/long 27]
            [:einsteindb/add "p" :foo/boolean false]
            [:einsteindb/add "p" :foo/long 24]
            [:einsteindb/add "u" :foo/boolean true]
            [:einsteindb/add "u" :foo/long 23]
        ]"#).expect("successful transaction");

        let u_yes = report.tempids.get("u").expect("found it").clone();
        let l_yes = report.tempids.get("l").expect("found it").clone();
        let n_yes = report.tempids.get("n").expect("found it").clone();

        let causetids: Vec<i64> = CausetQ::new(&mut store, r#"[:find [?x ...]
                                                                 :in ?v
                                                                 :where [?x :foo/boolean ?v]]"#)
                              .bind_causet_locale("?v", true)
                              .execute_coll().expect("CollResult")
                              .into_iter()
                              .map(|v| v.into_causetid().expect("val"))
                              .collect();

        assert_eq!(causetids, vec![l_yes, n_yes, u_yes]);
    }

    #[test]
    fn test_coll_query_by_row() {
        let mut store = Store::open("").expect("store connection");
        store.transact(r#"[
            [:einsteindb/add "s" :einsteindb/solitonid :foo/boolean]
            [:einsteindb/add "s" :einsteindb/causet_localeType :einsteindb.type/boolean]
            [:einsteindb/add "s" :einsteindb/cardinality :einsteindb.cardinality/one]
            [:einsteindb/add "t" :einsteindb/solitonid :foo/long]
            [:einsteindb/add "t" :einsteindb/causet_localeType :einsteindb.type/long]
            [:einsteindb/add "t" :einsteindb/cardinality :einsteindb.cardinality/one]
        ]"#).expect("successful transaction");

        let report = store.transact(r#"[
            [:einsteindb/add "l" :foo/boolean true]
            [:einsteindb/add "l" :foo/long 25]
            [:einsteindb/add "m" :foo/boolean false]
            [:einsteindb/add "m" :foo/long 26]
            [:einsteindb/add "n" :foo/boolean true]
            [:einsteindb/add "n" :foo/long 27]
            [:einsteindb/add "p" :foo/boolean false]
            [:einsteindb/add "p" :foo/long 24]
            [:einsteindb/add "u" :foo/boolean true]
            [:einsteindb/add "u" :foo/long 23]
        ]"#).expect("successful transaction");

        let n_yes = report.tempids.get("n").expect("found it").clone();

        let results = CausetQ::new(&mut store, r#"[:find [?x ...]
                                                        :in ?v
                                                        :where [?x :foo/boolean ?v]]"#)
                              .bind_causet_locale("?v", true)
                              .execute_coll().expect("CollResult");
        let causetid = results.get(1).map_or(None, |t| t.to_owned().into_causetid()).expect("causetid");

        assert_eq!(causetid, n_yes);
    }

    #[test]
    fn test_tuple_query_result_by_column() {
        let mut store = Store::open("").expect("store connection");
        store.transact(r#"[
            [:einsteindb/add "s" :einsteindb/solitonid :foo/boolean]
            [:einsteindb/add "s" :einsteindb/causet_localeType :einsteindb.type/boolean]
            [:einsteindb/add "s" :einsteindb/cardinality :einsteindb.cardinality/one]
            [:einsteindb/add "t" :einsteindb/solitonid :foo/long]
            [:einsteindb/add "t" :einsteindb/causet_localeType :einsteindb.type/long]
            [:einsteindb/add "t" :einsteindb/cardinality :einsteindb.cardinality/one]
        ]"#).expect("successful transaction");

        let report = store.transact(r#"[
            [:einsteindb/add "l" :foo/boolean true]
            [:einsteindb/add "l" :foo/long 25]
            [:einsteindb/add "m" :foo/boolean false]
            [:einsteindb/add "m" :foo/long 26]
            [:einsteindb/add "n" :foo/boolean true]
            [:einsteindb/add "n" :foo/long 27]
            [:einsteindb/add "p" :foo/boolean false]
            [:einsteindb/add "p" :foo/long 24]
            [:einsteindb/add "u" :foo/boolean true]
            [:einsteindb/add "u" :foo/long 23]
        ]"#).expect("successful transaction");

        let n_yes = report.tempids.get("n").expect("found it").clone();

        let results = CausetQ::new(&mut store, r#"[:find [?x, ?i]
                                                        :in ?v ?i
                                                        :where [?x :foo/boolean ?v]
                                                               [?x :foo/long ?i]]"#)
                              .bind_causet_locale("?v", true)
                              .bind_long("?i", 27)
                              .execute_tuple().expect("TupleResult").expect("Vec<TypedValue>");
        let causetid = results.get(0).map_or(None, |t| t.to_owned().into_causetid()).expect("causetid");
        let long_val = results.get(1).map_or(None, |t| t.to_owned().into_long()).expect("long");

        assert_eq!(causetid, n_yes);
        assert_eq!(long_val, 27);
    }

    #[test]
    fn test_tuple_query_result_by_iter() {
        let mut store = Store::open("").expect("store connection");
        store.transact(r#"[
            [:einsteindb/add "s" :einsteindb/solitonid :foo/boolean]
            [:einsteindb/add "s" :einsteindb/causet_localeType :einsteindb.type/boolean]
            [:einsteindb/add "s" :einsteindb/cardinality :einsteindb.cardinality/one]
            [:einsteindb/add "t" :einsteindb/solitonid :foo/long]
            [:einsteindb/add "t" :einsteindb/causet_localeType :einsteindb.type/long]
            [:einsteindb/add "t" :einsteindb/cardinality :einsteindb.cardinality/one]
        ]"#).expect("successful transaction");

        let report = store.transact(r#"[
            [:einsteindb/add "l" :foo/boolean true]
            [:einsteindb/add "l" :foo/long 25]
            [:einsteindb/add "m" :foo/boolean false]
            [:einsteindb/add "m" :foo/long 26]
            [:einsteindb/add "n" :foo/boolean true]
            [:einsteindb/add "n" :foo/long 27]
            [:einsteindb/add "p" :foo/boolean false]
            [:einsteindb/add "p" :foo/long 24]
            [:einsteindb/add "u" :foo/boolean true]
            [:einsteindb/add "u" :foo/long 23]
        ]"#).expect("successful transaction");

        let n_yes = report.tempids.get("n").expect("found it").clone();

        let results: Vec<_> = CausetQ::new(&mut store, r#"[:find [?x, ?i]
                                                                :in ?v ?i
                                                                :where [?x :foo/boolean ?v]
                                                                       [?x :foo/long ?i]]"#)
                              .bind_causet_locale("?v", true)
                              .bind_long("?i", 27)
                              .execute_tuple().expect("TupleResult").unwrap_or(vec![]);
        let causetid = TypedValue::Ref(n_yes.clone()).into();
        let long_val = TypedValue::Long(27).into();

        assert_eq!(results, vec![causetid, long_val]);
    }

    #[test]
    fn test_rel_query_result() {
        let mut store = Store::open("").expect("store connection");
        store.transact(r#"[
            [:einsteindb/add "s" :einsteindb/solitonid :foo/boolean]
            [:einsteindb/add "s" :einsteindb/causet_localeType :einsteindb.type/boolean]
            [:einsteindb/add "s" :einsteindb/cardinality :einsteindb.cardinality/one]
            [:einsteindb/add "t" :einsteindb/solitonid :foo/long]
            [:einsteindb/add "t" :einsteindb/causet_localeType :einsteindb.type/long]
            [:einsteindb/add "t" :einsteindb/cardinality :einsteindb.cardinality/one]
        ]"#).expect("successful transaction");

        let report = store.transact(r#"[
            [:einsteindb/add "l" :foo/boolean true]
            [:einsteindb/add "l" :foo/long 25]
            [:einsteindb/add "m" :foo/boolean false]
            [:einsteindb/add "m" :foo/long 26]
            [:einsteindb/add "n" :foo/boolean true]
            [:einsteindb/add "n" :foo/long 27]
        ]"#).expect("successful transaction");

        let l_yes = report.tempids.get("l").expect("found it").clone();
        let m_yes = report.tempids.get("m").expect("found it").clone();
        let n_yes = report.tempids.get("n").expect("found it").clone();

        #[derive(Debug, PartialEq)]
        struct Res {
            causetid: i64,
            boolean: bool,
            long_val: i64,
        };

        let mut results: Vec<Res> = CausetQ::new(&mut store, r#"[:find ?x ?v ?i
                                                                      :where [?x :foo/boolean ?v]
                                                                             [?x :foo/long ?i]]"#)
                              .execute_rel().expect("RelResult")
                              .into_iter()
                              .map(|event| {
                                  Res {
                                      causetid: event.get(0).map_or(None, |t| t.to_owned().into_causetid()).expect("causetid"),
                                      boolean: event.get(1).map_or(None, |t| t.to_owned().into_boolean()).expect("boolean"),
                                      long_val: event.get(2).map_or(None, |t| t.to_owned().into_long()).expect("long"),
                                  }
                              })
                              .collect();

        let res1 = results.pop().expect("res");
        assert_eq!(res1, Res { causetid: n_yes, boolean: true, long_val: 27 });
        let res2 = results.pop().expect("res");
        assert_eq!(res2, Res { causetid: m_yes, boolean: false, long_val: 26 });
        let res3 = results.pop().expect("res");
        assert_eq!(res3, Res { causetid: l_yes, boolean: true, long_val: 25 });
        assert_eq!(results.pop(), None);
    }

    #[test]
    fn test_bind_ref() {
        let mut store = Store::open("").expect("store connection");
        store.transact(r#"[
            [:einsteindb/add "s" :einsteindb/solitonid :foo/boolean]
            [:einsteindb/add "s" :einsteindb/causet_localeType :einsteindb.type/boolean]
            [:einsteindb/add "s" :einsteindb/cardinality :einsteindb.cardinality/one]
            [:einsteindb/add "t" :einsteindb/solitonid :foo/long]
            [:einsteindb/add "t" :einsteindb/causet_localeType :einsteindb.type/long]
            [:einsteindb/add "t" :einsteindb/cardinality :einsteindb.cardinality/one]
        ]"#).expect("successful transaction");

        let report = store.transact(r#"[
            [:einsteindb/add "l" :foo/boolean true]
            [:einsteindb/add "l" :foo/long 25]
            [:einsteindb/add "m" :foo/boolean false]
            [:einsteindb/add "m" :foo/long 26]
            [:einsteindb/add "n" :foo/boolean true]
            [:einsteindb/add "n" :foo/long 27]
        ]"#).expect("successful transaction");

        let l_yes = report.tempids.get("l").expect("found it").clone();

        let results = CausetQ::new(&mut store, r#"[:find [?v ?i]
                                                        :in ?x
                                                        :where [?x :foo/boolean ?v]
                                                               [?x :foo/long ?i]]"#)
                              .bind_ref("?x", l_yes)
                              .execute_tuple().expect("TupleResult")
                              .unwrap_or(vec![]);
        assert_eq!(results.get(0).map_or(None, |t| t.to_owned().into_boolean()).expect("boolean"), true);
        assert_eq!(results.get(1).map_or(None, |t| t.to_owned().into_long()).expect("long"), 25);
    }
}
