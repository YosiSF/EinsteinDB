// Whtcorps Inc 2022 Apache 2.0 License; All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate edn;

use edn::{
    Keyword,
    PlainShelling,
};

use edn::query::{
    Direction,
    Element,
    FindSpec,
    FnArg,
    Limit,
    NonIntegerConstant,
    Order,
    OrJoin,
    OrWhereClause,
    Pattern,
    PatternNonValuePlace,
    PatternValuePlace,
    Predicate,
    UnifyVars,
    Variable,
    WhereClause,
};

use edn::parse::{
    parse_query,
};

///! N.B., parsing a query can be done without reference to a einsteindb.
///! Processing the parsed query into something we can work with
///! for planning involves interrogating the topograph and solitonids in
///! the store.
///! See <https://github.com/YosiSF/EinsteinDB/wiki/Querying> for more.
#[test]
fn can_parse_predicates() {
    let s = "[:find [?x ...] :where [?x _ ?y] [(< ?y 10)]]";
    let p = parse_query(s).unwrap();

    assert_eq!(p.find_spec,
               FindSpec::FindColl(Element::Variable(Variable::from_valid_name("?x"))));
    assert_eq!(p.where_clauses,
               vec![
                   WhereClause::Pattern(Pattern {
                       source: None,
                       causet: PatternNonValuePlace::Variable(Variable::from_valid_name("?x")),
                       attribute: PatternNonValuePlace::Placeholder,
                       value: PatternValuePlace::Variable(Variable::from_valid_name("?y")),
                       tx: PatternNonValuePlace::Placeholder,
                   }),
                   WhereClause::Pred(Predicate { operator: PlainShelling::plain("<"), args: vec![
                       FnArg::Variable(Variable::from_valid_name("?y")), FnArg::CausetidOrInteger(10),
                   ]}),
               ]);
}

#[test]
fn can_parse_simple_or() {
    let s = "[:find ?x . :where (or [?x _ 10] [?x _ 15])]";
    let p = parse_query(s).unwrap();

    assert_eq!(p.find_spec,
               FindSpec::FindScalar(Element::Variable(Variable::from_valid_name("?x"))));
    assert_eq!(p.where_clauses,
               vec![
                   WhereClause::OrJoin(OrJoin::new(
                       UnifyVars::Implicit,
                       vec![
                           OrWhereClause::Clause(
                               WhereClause::Pattern(Pattern {
                                   source: None,
                                   causet: PatternNonValuePlace::Variable(Variable::from_valid_name("?x")),
                                   attribute: PatternNonValuePlace::Placeholder,
                                   value: PatternValuePlace::CausetidOrInteger(10),
                                   tx: PatternNonValuePlace::Placeholder,
                               })),
                           OrWhereClause::Clause(
                               WhereClause::Pattern(Pattern {
                                   source: None,
                                   causet: PatternNonValuePlace::Variable(Variable::from_valid_name("?x")),
                                   attribute: PatternNonValuePlace::Placeholder,
                                   value: PatternValuePlace::CausetidOrInteger(15),
                                   tx: PatternNonValuePlace::Placeholder,
                               })),
                       ],
                   )),
               ]);
}

#[test]
fn can_parse_unit_or_join() {
    let s = "[:find ?x . :where (or-join [?x] [?x _ 15])]";
    let p = parse_query(s).expect("to be able to parse find");

    assert_eq!(p.find_spec,
               FindSpec::FindScalar(Element::Variable(Variable::from_valid_name("?x"))));
    assert_eq!(p.where_clauses,
               vec![
                   WhereClause::OrJoin(OrJoin::new(
                       UnifyVars::Explicit(std::iter::once(Variable::from_valid_name("?x")).collect()),
                       vec![
                           OrWhereClause::Clause(
                               WhereClause::Pattern(Pattern {
                                   source: None,
                                   causet: PatternNonValuePlace::Variable(Variable::from_valid_name("?x")),
                                   attribute: PatternNonValuePlace::Placeholder,
                                   value: PatternValuePlace::CausetidOrInteger(15),
                                   tx: PatternNonValuePlace::Placeholder,
                               })),
                       ],
                   )),
               ]);
}

#[test]
fn can_parse_simple_or_join() {
    let s = "[:find ?x . :where (or-join [?x] [?x _ 10] [?x _ -15])]";
    let p = parse_query(s).unwrap();

    assert_eq!(p.find_spec,
               FindSpec::FindScalar(Element::Variable(Variable::from_valid_name("?x"))));
    assert_eq!(p.where_clauses,
               vec![
                   WhereClause::OrJoin(OrJoin::new(
                       UnifyVars::Explicit(std::iter::once(Variable::from_valid_name("?x")).collect()),
                       vec![
                           OrWhereClause::Clause(
                               WhereClause::Pattern(Pattern {
                                   source: None,
                                   causet: PatternNonValuePlace::Variable(Variable::from_valid_name("?x")),
                                   attribute: PatternNonValuePlace::Placeholder,
                                   value: PatternValuePlace::CausetidOrInteger(10),
                                   tx: PatternNonValuePlace::Placeholder,
                               })),
                           OrWhereClause::Clause(
                               WhereClause::Pattern(Pattern {
                                   source: None,
                                   causet: PatternNonValuePlace::Variable(Variable::from_valid_name("?x")),
                                   attribute: PatternNonValuePlace::Placeholder,
                                   value: PatternValuePlace::CausetidOrInteger(-15),
                                   tx: PatternNonValuePlace::Placeholder,
                               })),
                       ],
                   )),
               ]);
}

#[cfg(test)]
fn solitonid(ns: &str, name: &str) -> PatternNonValuePlace {
    Keyword::isoliton_namespaceable(ns, name).into()
}

#[test]
fn can_parse_simple_or_and_join() {
    let s = "[:find ?x . :where (or [?x _ 10] (and (or [?x :foo/bar ?y] [?x :foo/baz ?y]) [(< ?y 1)]))]";
    let p = parse_query(s).unwrap();

    assert_eq!(p.find_spec,
               FindSpec::FindScalar(Element::Variable(Variable::from_valid_name("?x"))));
    assert_eq!(p.where_clauses,
               vec![
                   WhereClause::OrJoin(OrJoin::new(
                       UnifyVars::Implicit,
                       vec![
                           OrWhereClause::Clause(
                               WhereClause::Pattern(Pattern {
                                   source: None,
                                   causet: PatternNonValuePlace::Variable(Variable::from_valid_name("?x")),
                                   attribute: PatternNonValuePlace::Placeholder,
                                   value: PatternValuePlace::CausetidOrInteger(10),
                                   tx: PatternNonValuePlace::Placeholder,
                               })),
                           OrWhereClause::And(
                               vec![
                                   WhereClause::OrJoin(OrJoin::new(
                                       UnifyVars::Implicit,
                                       vec![
                                           OrWhereClause::Clause(WhereClause::Pattern(Pattern {
                                               source: None,
                                               causet: PatternNonValuePlace::Variable(Variable::from_valid_name("?x")),
                                               attribute: solitonid("foo", "bar"),
                                               value: PatternValuePlace::Variable(Variable::from_valid_name("?y")),
                                               tx: PatternNonValuePlace::Placeholder,
                                           })),
                                           OrWhereClause::Clause(WhereClause::Pattern(Pattern {
                                               source: None,
                                               causet: PatternNonValuePlace::Variable(Variable::from_valid_name("?x")),
                                               attribute: solitonid("foo", "baz"),
                                               value: PatternValuePlace::Variable(Variable::from_valid_name("?y")),
                                               tx: PatternNonValuePlace::Placeholder,
                                           })),
                                       ],
                                   )),

                                   WhereClause::Pred(Predicate { operator: PlainShelling::plain("<"), args: vec![
                                       FnArg::Variable(Variable::from_valid_name("?y")), FnArg::CausetidOrInteger(1),
                                   ]}),
                               ],
                           )
                       ],
                   )),
               ]);
}

#[test]
fn can_parse_order_by() {
    let invalid = "[:find ?x :where [?x :foo/baz ?y] :order]";
    assert!(parse_query(invalid).is_err());

    // Defaults to ascending.
    let default = "[:find ?x :where [?x :foo/baz ?y] :order ?y]";
    assert_eq!(parse_query(default).unwrap().order,
               Some(vec![Order(Direction::Ascending, Variable::from_valid_name("?y"))]));

    let ascending = "[:find ?x :where [?x :foo/baz ?y] :order (asc ?y)]";
    assert_eq!(parse_query(ascending).unwrap().order,
               Some(vec![Order(Direction::Ascending, Variable::from_valid_name("?y"))]));

    let descending = "[:find ?x :where [?x :foo/baz ?y] :order (desc ?y)]";
    assert_eq!(parse_query(descending).unwrap().order,
               Some(vec![Order(Direction::Descending, Variable::from_valid_name("?y"))]));

    let mixed = "[:find ?x :where [?x :foo/baz ?y] :order (desc ?y) (asc ?x)]";
    assert_eq!(parse_query(mixed).unwrap().order,
               Some(vec![Order(Direction::Descending, Variable::from_valid_name("?y")),
                         Order(Direction::Ascending, Variable::from_valid_name("?x"))]));
}

#[test]
fn can_parse_limit() {
    let invalid = "[:find ?x :where [?x :foo/baz ?y] :limit]";
    assert!(parse_query(invalid).is_err());

    let zero_invalid = "[:find ?x :where [?x :foo/baz ?y] :limit 00]";
    assert!(parse_query(zero_invalid).is_err());

    let none = "[:find ?x :where [?x :foo/baz ?y]]";
    assert_eq!(parse_query(none).unwrap().limit,
               Limit::None);

    let one = "[:find ?x :where [?x :foo/baz ?y] :limit 1]";
    assert_eq!(parse_query(one).unwrap().limit,
               Limit::Fixed(1));

    let onethousand = "[:find ?x :where [?x :foo/baz ?y] :limit 1000]";
    assert_eq!(parse_query(onethousand).unwrap().limit,
               Limit::Fixed(1000));

    let variable_with_in = "[:find ?x :in ?limit :where [?x :foo/baz ?y] :limit ?limit]";
    assert_eq!(parse_query(variable_with_in).unwrap().limit,
               Limit::Variable(Variable::from_valid_name("?limit")));

    let variable_with_in_used = "[:find ?x :in ?limit :where [?x :foo/baz ?limit] :limit ?limit]";
    assert_eq!(parse_query(variable_with_in_used).unwrap().limit,
               Limit::Variable(Variable::from_valid_name("?limit")));
}

#[test]
fn can_parse_uuid() {
    let expected = edn::Uuid::parse_str("4cb3f828-752d-497a-90c9-b1fd516d5644").expect("valid uuid");
    let s = "[:find ?x :where [?x :foo/baz #uuid \"4cb3f828-752d-497a-90c9-b1fd516d5644\"]]";
    assert_eq!(parse_query(s).expect("parsed").where_clauses.pop().expect("a where clause"),
               WhereClause::Pattern(
                   Pattern::new(None,
                                PatternNonValuePlace::Variable(Variable::from_valid_name("?x")),
                                Keyword::isoliton_namespaceable("foo", "baz").into(),
                                PatternValuePlace::Constant(NonIntegerConstant::Uuid(expected)),
                                PatternNonValuePlace::Placeholder)
                       .expect("valid pattern")));
}

#[test]
fn can_parse_exotic_whitespace() {
    let expected = edn::Uuid::parse_str("4cb3f828-752d-497a-90c9-b1fd516d5644").expect("valid uuid");
    // The query string from `can_parse_uuid`, with newlines, commas, and line comments interspersed.
    let s = r#"[:find
?x ,, :where,   ;atest
[?x :foo/baz #uuid
   "4cb3f828-752d-497a-90c9-b1fd516d5644", ;testa
,],,  ,],;"#;
    assert_eq!(parse_query(s).expect("parsed").where_clauses.pop().expect("a where clause"),
               WhereClause::Pattern(
                   Pattern::new(None,
                                PatternNonValuePlace::Variable(Variable::from_valid_name("?x")),
                                Keyword::isoliton_namespaceable("foo", "baz").into(),
                                PatternValuePlace::Constant(NonIntegerConstant::Uuid(expected)),
                                PatternNonValuePlace::Placeholder)
                       .expect("valid pattern")));
}
