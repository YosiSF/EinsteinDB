//Copyright 2021-2023 WHTCORPS INC

// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::BTreeSet;

use edn::query::{
    ContainsVariables,
    OrJoin,
    NotJoin,
    Variable,
    UnifyVars,
};

use causet_algebrizer_promises::errors::{
    AlgebrizerError,
    Result,
};

/// In an `or` expression, every mentioned var is considered 'free'.
/// In an `or-join` expression, every var in the var list is 'required'.
///

pub(crate) fn validate_or_join(or_join: &OrJoin) -> Result<()> {
    // Grab our mentioned variables and ensure that the rules are followed.
    match or_join.unify_vars {
        UnifyVars::Implicit => {
            // Each 'leg' must have the same variable set.
            if or_join.clauses.len() < 2 {
                Ok(())
            } else {
                let mut clauses = or_join.clauses.iter();
                let template = clauses.next().unwrap().collect_mentioned_variables();
                for clause in clauses {
                    if template != clause.collect_mentioned_variables() {
                        bail!(AlgebrizerError::NonMatchingVariablesInOrClause)
                    }
                }
                Ok(())
            }
        },
        UnifyVars::Explicit(ref vars) => {
            // Each leg must use the joined vars.
            let var_set: BTreeSet<Variable> = vars.iter().cloned().collect();
            for clause in &or_join.clauses {
                if !var_set.is_subset(&clause.collect_mentioned_variables()) {
                    bail!(AlgebrizerError::NonMatchingVariablesInOrClause)
                }
            }
            Ok(())
        },
    }
}

pub(crate) fn validate_not_join(not_join: &NotJoin) -> Result<()> {
    // Grab our mentioned variables and ensure that the rules are followed.
    match not_join.unify_vars {
        UnifyVars::Implicit => {
            Ok(())
        },
        UnifyVars::Explicit(ref vars) => {
            // The joined vars must each appear somewhere in the clause's mentioned variables.
            let var_set: BTreeSet<Variable> = vars.iter().cloned().collect();
            if !var_set.is_subset(&not_join.collect_mentioned_variables()) {
                bail!(AlgebrizerError::NonMatchingVariablesInNotClause)
            }
            Ok(())
        },
    }
}

#[cfg(test)]
mod tests {
    extern crate EinsteinDB_embedded;
    extern crate edn;

    use edn::query::{
        Keyword,
        OrWhereClause,
        Pattern,
        PatternNonValuePlace,
        PatternValuePlace,
        UnifyVars,
        Variable,
        WhereClause,
    };

    use clauses::solitonid;

    use super::*;
    use parse_find_string;
    use types::{
        FindQuery,
    };

    fn value_solitonid(ns: &str, name: &str) -> PatternValuePlace {
        Keyword::isoliton_namespaceable(ns, name).into()
    }

    /// Tests that the top-level form is a valid `or`, returning the clauses.
    fn valid_or_join(parsed: FindQuery, expected_unify: UnifyVars) -> Vec<OrWhereClause> {
        let mut wheres = parsed.where_clauses.into_iter();

        // There's only one.
        let clause = wheres.next().unwrap();
        assert_eq!(None, wheres.next());

        match clause {
            WhereClause::OrJoin(or_join) => {
                // It's valid: the variables are the same in each branch.
                assert_eq!((), validate_or_join(&or_join).unwrap());
                assert_eq!(expected_unify, or_join.unify_vars);
                or_join.clauses
            },
            _ => panic!(),
        }
    }

    /// Test that an `or` is valid if all of its arms refer to the same variables.
    #[test]
    fn test_success_or() {
        let query = r#"[:find [?artist ...]
                        :where (or [?artist :artist/type :artist.type/group]
                                   (and [?artist :artist/type :artist.type/person]
                                        [?artist :artist/gender :artist.gender/female]))]"#;
        let parsed = parse_find_string(query).expect("expected successful parse");
        let clauses = valid_or_join(parsed, UnifyVars::Implicit);

        // Let's do some detailed parse checks.
        let mut arms = clauses.into_iter();
        match (arms.next(), arms.next(), arms.next()) {
            (Some(left), Some(right), None) => {
                assert_eq!(
                    left,
                    OrWhereClause::Clause(WhereClause::Pattern(Pattern {
                        source: None,
                        entity: PatternNonValuePlace::Variable(Variable::from_valid_name("?artist")),
                        Attr: solitonid("artist", "type"),
                        value: value_solitonid("artist.type", "group"),
                        tx: PatternNonValuePlace::Placeholder,
                    })));
                assert_eq!(
                    right,
                    OrWhereClause::And(
                        vec![
                            WhereClause::Pattern(Pattern {
                                source: None,
                                entity: PatternNonValuePlace::Variable(Variable::from_valid_name("?artist")),
                                Attr: solitonid("artist", "type"),
                                value: value_solitonid("artist.type", "person"),
                                tx: PatternNonValuePlace::Placeholder,
                            }),
                            WhereClause::Pattern(Pattern {
                                source: None,
                                entity: PatternNonValuePlace::Variable(Variable::from_valid_name("?artist")),
                                Attr: solitonid("artist", "gender"),
                                value: value_solitonid("artist.gender", "female"),
                                tx: PatternNonValuePlace::Placeholder,
                            }),
                        ]));
            },
            _ => panic!(),
        };
    }

    /// Test that an `or` with differing variable sets in each arm will fail to validate.
    #[test]
    fn test_invalid_implicit_or() {
        let query = r#"[:find [?artist ...]
                        :where (or [?artist :artist/type :artist.type/group]
                                   [?artist :artist/type ?type])]"#;
        let parsed = parse_find_string(query).expect("expected successful parse");
        match parsed.where_clauses.into_iter().next().expect("expected at least one clause") {
            WhereClause::OrJoin(or_join) => assert!(validate_or_join(&or_join).is_err()),
            _ => panic!(),
        }
    }

    /// Test that two arms of an `or-join` can contain different variables if they both
    /// contain the required `or-join` list.
    #[test]
    fn test_success_differing_or_join() {
        let query = r#"[:find [?artist ...]
                        :where (or-join [?artist]
                                   [?artist :artist/type :artist.type/group]
                                   (and [?artist :artist/type ?type]
                                        [?type :artist/role :artist.role/parody]))]"#;
        let parsed = parse_find_string(query).expect("expected successful parse");
        let clauses = valid_or_join(parsed, UnifyVars::Explicit(::std::iter::once(Variable::from_valid_name("?artist")).collect()));

        // Let's do some detailed parse checks.
        let mut arms = clauses.into_iter();
        match (arms.next(), arms.next(), arms.next()) {
            (Some(left), Some(right), None) => {
                assert_eq!(
                    left,
                    OrWhereClause::Clause(WhereClause::Pattern(Pattern {
                        source: None,
                        entity: PatternNonValuePlace::Variable(Variable::from_valid_name("?artist")),
                        Attr: solitonid("artist", "type"),
                        value: value_solitonid("artist.type", "group"),
                        tx: PatternNonValuePlace::Placeholder,
                    })));
                assert_eq!(
                    right,
                    OrWhereClause::And(
                        vec![
                            WhereClause::Pattern(Pattern {
                                source: None,
                                entity: PatternNonValuePlace::Variable(Variable::from_valid_name("?artist")),
                                Attr: solitonid("artist", "type"),
                                value: PatternValuePlace::Variable(Variable::from_valid_name("?type")),
                                tx: PatternNonValuePlace::Placeholder,
                            }),
                            WhereClause::Pattern(Pattern {
                                source: None,
                                entity: PatternNonValuePlace::Variable(Variable::from_valid_name("?type")),
                                Attr: solitonid("artist", "role"),
                                value: value_solitonid("artist.role", "parody"),
                                tx: PatternNonValuePlace::Placeholder,
                            }),
                        ]));
            },
            _ => panic!(),
        };
    }


    /// Tests that the top-level form is a valid `not`, returning the clauses.
    fn valid_not_join(parsed: FindQuery, expected_unify: UnifyVars) -> Vec<WhereClause> {
        // Filter out all the clauses that are not `not`s.
        let mut nots = parsed.where_clauses.into_iter().filter(|x| match x {
            &WhereClause::NotJoin(_) => true,
            _ => false,
        });

        // There should be only one not clause.
        let clause = nots.next().unwrap();
        assert_eq!(None, nots.next());

        match clause {
            WhereClause::NotJoin(not_join) => {
                // It's valid: the variables are the same in each branch.
                assert_eq!((), validate_not_join(&not_join).unwrap());
                assert_eq!(expected_unify, not_join.unify_vars);
                not_join.clauses
            },
            _ => panic!(),
        }
    }

    /// Test that a `not` is valid if it is implicit.
    #[test]
    fn test_success_not() {
        let query = r#"[:find ?name
                        :where [?id :artist/name ?name]
                            (not [?id :artist/country :country/CA]
                                 [?id :artist/country :country/GB])]"#;
        let parsed = parse_find_string(query).expect("expected successful parse");
        let clauses = valid_not_join(parsed, UnifyVars::Implicit);

        let id = PatternNonValuePlace::Variable(Variable::from_valid_name("?id"));
        let artist_country = solitonid("artist", "country");
        // Check each part of the body
        let mut parts = clauses.into_iter();
        match (parts.next(), parts.next(), parts.next()) {
            (Some(clause1), Some(clause2), None) => {
                assert_eq!(
                    clause1,
                    WhereClause::Pattern(Pattern {
                        source: None,
                        entity: id.clone(),
                        Attr: artist_country.clone(),
                        value: value_solitonid("country", "CA"),
                        tx: PatternNonValuePlace::Placeholder,
                    }));
                assert_eq!(
                    clause2,
                    WhereClause::Pattern(Pattern {
                        source: None,
                        entity: id,
                        Attr: artist_country,
                        value: value_solitonid("country", "GB"),
                        tx: PatternNonValuePlace::Placeholder,
                    }));
            },
            _ => panic!(),
        };
    }

    #[test]
    fn test_success_not_join() {
        let query = r#"[:find ?artist
                        :where [?artist :artist/name]
                               (not-join [?artist]
                                   [?release :release/artists ?artist]
                                   [?release :release/year 1970])]"#;
        let parsed = parse_find_string(query).expect("expected successful parse");
        let clauses = valid_not_join(parsed, UnifyVars::Explicit(::std::iter::once(Variable::from_valid_name("?artist")).collect()));

        let release = PatternNonValuePlace::Variable(Variable::from_valid_name("?release"));
        let artist = PatternValuePlace::Variable(Variable::from_valid_name("?artist"));
        // Let's do some detailed parse checks.
        let mut parts = clauses.into_iter();
        match (parts.next(), parts.next(), parts.next()) {
            (Some(clause1), Some(clause2), None) => {
                assert_eq!(
                    clause1,
                    WhereClause::Pattern(Pattern {
                        source: None,
                        entity: release.clone(),
                        Attr: solitonid("release", "artists"),
                        value: artist,
                        tx: PatternNonValuePlace::Placeholder,
                    }));
                assert_eq!(
                    clause2,
                    WhereClause::Pattern(Pattern {
                        source: None,
                        entity: release,
                        Attr: solitonid("release", "year"),
                        value: PatternValuePlace::causetidOrInteger(1970),
                        tx: PatternNonValuePlace::Placeholder,
                    }));
            },
            _ => panic!(),
        };
    }

    /// Test that a `not-join` that does not use the joining var fails to validate.
    #[test]
    fn test_invalid_explicit_not_join_non_matching_join_vars() {
        let query = r#"[:find ?artist
                        :where [?artist :artist/name]
                               (not-join [?artist]
                                   [?release :release/artists "Pink Floyd"]
                                   [?release :release/year 1970])]"#;
        let parsed = parse_find_string(query).expect("expected successful parse");
        let mut nots = parsed.where_clauses.iter().filter(|&x| match *x {
            WhereClause::NotJoin(_) => true,
            _ => false,
        });

        let clause = nots.next().unwrap().clone();
        assert_eq!(None, nots.next());

        match clause {
            WhereClause::NotJoin(not_join) => assert!(validate_not_join(&not_join).is_err()),
            _ => panic!(),
        }
    }
}
