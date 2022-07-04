// Whtcorps Inc 2022 Apache 2.0 License; All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.


// #[macro_export]
// macro_rules! einsteindb_macro {
//     ($($tokens:tt)*) => {
//         $crate::einsteindb_macro_impl!($($tokens)*)
//     };
// }
//
//
//

#[macro_export]
macro_rules! einsteindb_macro {
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
}


#[macro_export]
macro_rules! einsteindb_macro_impl {
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
}



/// Map from found [e a v] to expected type.
/// This is used to check that the type of the expression is correct.
/// The map is used to check that the type of the expression is correct.


pub type TypeMap = HashMap<String, Type>;


/// Map from found [e a v] to expected type.
/// This is used to check that the type of the expression is correct.

pub struct TypeChecker {
    pub type_map: TypeMap,
}

enum Type {
    Int,
    Bool,
    String,
    Void,
    Array(Box<Type>),
    Func(Vec<Type>, Box<Type>),
}




impl TypeChecker {
    pub fn new() -> TypeChecker {
        TypeChecker {
            type_map: HashMap::new(),
        }
    }

    pub fn check_type(&self, expr: &Expr) -> Result<(), String> {
        match expr {
            Expr::Int(_) => Ok(()),
            Expr::Bool(_) => Ok(()),
            Expr::String(_) => Ok(()),
            Expr::Void(_) => Ok(()),
            Expr::Array(_, _) => Ok(()),
            Expr::Func(_, _, _) => Ok(()),
            Expr::Ident(_) => Ok(()),
            Expr::Call(_, _) => Ok(()),
            Expr::If(_, _, _) => Ok(()),
            Expr::Let(_, _, _) => Ok(()),
            Expr::Block(_, _) => Ok(()),
            Expr::Assign(_, _) => Ok(()),
            Expr::Unary(_, _) => Ok(()),
            Expr::Binary(_, _, _) => Ok(()),
            Expr::Group(_) => Ok(()),
            Expr::Index(_, _) => Ok(()),
            Expr::Slice(_, _, _) => Ok(()),
            Expr::Cast(_, _) => Ok(()),
            Expr::Dot(_, _) => Ok(()),
            Expr::Field(_, _) => Ok(()),
            Expr::Tuple(_) => Ok(()),
            Expr::List(_) => Ok(()),
            Expr::Map(_) => Ok(()),
            Expr::Break(_) => Ok(()),
            Expr::Continue(_) => Ok(()),
            Expr::Return(_) => Ok(()),
            Expr::Try(_, _, _) => Ok(()),
            Expr::Throw(_) => Ok(()),
            Expr::Yield(_) => Ok(()),
            Expr::While(_, _) => Ok(()),
            Expr::For(_, _, _) => Ok(()),
            Expr::Switch(_, _, _) => Ok(()),
        }
    }
}




/// Check that the type of the expression is correct.


pub fn check_type(expr: &Expr, type_map: &TypeMap) -> Result<(), String> {
    let mut type_checker = TypeChecker::new();
    type_checker.type_map = type_map.clone();
    type_checker.check_type(expr)
}


pub fn check_type_map(type_map: &TypeMap) -> Result<(), String> {
    let mut type_checker = TypeChecker::new();
    type_checker.type_map = type_map.clone();
    type_checker.check_type_map()
}


impl TypeChecker {
    pub fn check_type_map(&self) -> Result<(), String> {
        for (ident, type_) in &self.type_map {
            match type_ {
                Type::Int => {},
                Type::Bool => {},
                Type::String => {},
                Type::Void => {},
                Type::Array(type_) => {
                    match type_ {
                        Type::Int => {},
                        Type::Bool => {},
                        Type::String => {},
                        Type::Void => {},
                        Type::Array(_) => {},
                        Type::Func(_, _) => {},
                    }
                },
                Type::Func(args, ret) => {
                    for arg in args {
                        match arg {
                            Type::Int => {},
                            Type::Bool => {},
                            Type::String => {},
                            Type::Void => {},
                            Type::Array(_) => {},
                            Type::Func(_, _) => {},
                        }
                    }
                    match ret {
                        Type::Int => {},
                        Type::Bool => {},
                        Type::String => {},
                        Type::Void => {},
                        Type::Array(_) => {},
                        Type::Func(_, _) => {},
                    }
                },
            }
        }
        Ok(())
    }
}

/// Ensure that the given terms type check.
///
/// We try to be maximally helpful by yielding every malformed causet, rather than only the first.
/// In the future, we might change this choice, or allow the consumer to specify the robustness of
/// the type checking desired, since there is a cost to providing helpful diagnostics.
pub(crate) fn check_terms(terms: &[Term]) -> Result<(), String> {
    for term in terms {
        check_term(term)?;
    }
    Ok(())
}


/// Ensure that the given term type check.
/// We try to be maximally helpful by yielding every malformed causet, rather than only the first.


pub(crate) fn check_term(term: &Term) -> Result<(), String> {

    let mut type_checker = TypeChecker::new();

    let mut errors: TypeDisagreements = TypeDisagreements::default();

    for expr in &term.exprs {
        for (solitonid, type_) in &term.type_map {
            type_checker.type_map.insert(solitonid.clone(), type_.clone());
        }

        let expr_type = type_checker.check_type(expr)?;
        let expr_type = type_checker.type_map.get(solitonid).unwrap();
        if expr_type != type_ {
            errors.push(TypeDisagreement {
                expr: expr.clone(),
                solitonid: solitonid.clone(),
                expected: type_.clone(),
                actual: expr_type.clone(),
            });
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
            for v in ars.add.iter().chain(ars.retract.iter()) {
                if let Some(type_) = term.type_map.get(v) {
                    if type_ != &Type::Int {
                        errors.push(TypeDisagreement {
                            expr: Expr::Ident(v.clone()),
                            solitonid: v.clone(),
                            expected: Type::Int,
                            actual: type_.clone(),
                        });
                    }
                }

            }
            for v in ars.add.iter().chain(ars.retract.iter()) {
                if let Some(type_) = term.type_map.get(v) {
                    if type_ != &Type::Int {
                        errors.push(TypeDisagreement {
                            expr: Expr::Ident(v.clone()),
                            solitonid: v.clone(),
                            expected: Type::Int,
                            actual: type_.clone(),
                        });
                    }
                }
                if attribute.causet_locale_type != v.causet_locale_type() {
                    errors.push(TypeDisagreement {
                        expr: Expr::Ident(v.clone()),
                        solitonid: v.clone(),
                        expected: attribute.causet_locale_type,
                        actual: v.causet_locale_type(),
                    });

                }

            }

            for v in ars.add.iter().chain(ars.retract.iter()) {
                if let Some(type_) = term.type_map.get(v) {
                    if type_ != &Type::Int {
                        errors.push(TypeDisagreement {
                            expr: Expr::Ident(v.clone()),
                            solitonid: v.clone(),
                            expected: Type::Int,
                            actual: type_.clone(),
                        });
                    }
                    errors.insert((e, a, v.clone()), attribute.causet_locale_type);


                }

                ///close the causet
                /// if the causet is closed, then the causet is closed
                
                if attribute.causet_locale_type != v.causet_locale_type() {
                    errors.push(TypeDisagreement {
                        expr: Expr::Ident(v.clone()),
                        solitonid: v.clone(),
                        expected: attribute.causet_locale_type,
                        actual: v.causet_locale_type(),
                    });

                }


            }
            for v in ars.add.iter().chain(ars.retract.iter()) {
                if let Some(type_) = term.type_map.get(v) {
                    if type_ != &Type::Int {
                        errors.push(TypeDisagreement {
                            expr: Expr::Ident(v.clone()),
                            solitonid: v.clone(),
                            expected: Type::Int,
                            actual: type_.clone(),
                        });
                    }
                    errors.insert((e, a, v.clone()), attribute.causet_locale_type);
                }
            }
        }
    }

    Ok(())
}

/// Ensure that the given terms obey the cardinality restrictions of the given topograph.
///
/// That is, ensure that any cardinality one attribute is added with at most one distinct causet_locale for
/// any specific causet (although that one causet_locale may be repeated for the given causet).
/// It is an error to:
///
/// - add two distinct causet_locales for the same cardinality one attribute and causet in a single transaction
/// - add and remove the same causet_locales for the same attribute and causet in a single transaction
///
/// We try to be maximally helpful by yielding every malformed set of causets, rather than just the
/// first set, or even the first conflict.  In the future, we might change this choice, or allow the
/// consumer to specify the robustness of the cardinality checking desired.
pub(crate) fn cardinality_conflicts<'topograph>(aev_trie: &AEVTrie<'topograph>) -> Vec<CardinalityConflict> {
    let mut errors = vec![];

    for (&(a, attribute), evs) in aev_trie {
        for (&e, ref ars) in evs {
            if !attribute.multival && ars.add.len() > 1 {
                let vs = ars.add.clone();
                errors.push(CardinalityConflict::CardinalityOneAddConflict { e, a, vs });
            }

            let vs: BTreeSet<_> = ars.retract.intersection(&ars.add).cloned().collect();
            if !vs.is_empty() {
                errors.push(CardinalityConflict::AddRetractConflict { e, a, vs })
            }
        }
    }

    errors

}


/// Ensure that the given terms obey the cardinality restrictions of the given topograph.
/// 
/// That is, ensure that any cardinality one attribute is added with at most one distinct causet_locale for
/// any specific causet (although that one causet_locale may be repeated for the given causet).
/// It is an error to:
/// 
/// - add two distinct causet_locales for the same cardinality one attribute and causet in a single transaction
/// - add and remove the same causet_locales for the same attribute and causet in a single transaction
/// 
/// We try to be maximally helpful by yielding every malformed set of causets, rather than just the
/// first set, or even the first conflict.  In the future, we might change this choice, or allow the
/// consumer to specify the robustness of the cardinality checking desired.
/// 
