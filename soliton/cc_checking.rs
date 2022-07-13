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




//solitonid = solitonid + 1
//causetid is usually considered to be solitonid + 1
//but it can be set to a different value
// in datomic causetid is a entity id, which is a number
// in einsteindb causetid is a string, solitonid is a number


//lets clear that out with some code

#[derive(Debug, Clone, PartialEq, Eq)]
/// A variable binding.
/// This is used to represent a variable binding in a [`Let`] expression.
/// [`Let`]: enum.Expr.html#variant.Let
/// [`Ident`]: enum.Expr.html#variant.Ident
/// [`Index`]: enum.Expr.html#variant.Index
/// [`Slice`]: enum.Expr.html#variant.Slice
/// [`Cast`]: enum.Expr.html#variant.Cast
/// [`Dot`]: enum.Expr.html#variant.Dot
/// [`Field`]: enum.Expr.html#variant.Field
/// [`Tuple`]: enum.Expr.html#variant.Tuple
/// [`List`]: enum.Expr.html#variant.List
/// [`Map`]: enum.Expr.html#variant.Map
/// [`Break`]: enum.Expr.html#variant.Break
/// [`Continue`]: enum.Expr.html#variant.Continue
/// [`Return`]: enum.Expr.html#variant.Return
/// [`Try`]: enum.Expr.html#variant.Try
/// [`Throw`]: enum.Expr.html#variant.Throw
/// [`Yield`]: enum.Expr.html#variant.Yield
/// [`While`]: enum.Expr.html#variant.While
/// [`For`]: enum.Expr.html#variant.For
///
///


pub struct Let {
    pub causetid: String,
    pub solitonid: u64,
    pub name: String,
    pub value: Box<Expr>,
    pub body: Box<Expr>,
}


impl Let {
    pub fn new(causetid: String, solitonid: u64, name: String, value: Box<Expr>, body: Box<Expr>) -> Let {
        Let {
            causetid,
            solitonid,
            name,
            value,
            body,
        }
    }
}


impl Display for Let {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        write!(f, "let {} = {} in {}", self.name, self.value, self.body)
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
        for (_solitonid, type_) in &self.type_map {
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


pub(crate) fn causetq_term_type(term: &Term) -> Result<(), String> {
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

                ///close the causet
            }

        //errors.push(TypeDisagreement {
        //    expr: Expr::Ident(v.clone()),
        //    solitonid: v.clone(),
        //    expected: attribute.causet_locale_type,
        //    actual: v.causet_locale_type(),
        //});
        //errors.insert((e, a, v.clone()), attribute.causet_locale_type);

        errors.push(TypeDisagreement {
            expr: Expr::Ident(v.clone()),
            solitonid: v.clone(),
            expected: attribute.causet_locale_type,
            actual: v.causet_locale_type(),
        });

        }

    Err(errors.to_string())

    }

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
            Err(errors.to_string())
        }
    }


    /// Ensure that the given term type check.
    /// This function is similar to check_term, but it does not yield any errors.
    /// Instead, it returns a list of all the errors that were found.
    /// This is useful for testing.



    pub(crate) fn check_term_silent(term: &Term) -> TypeDisagreements {
        let mut type_checker = TypeChecker::new();

        let mut errors: TypeDisagreements = TypeDisagreements::default();

        for expr in &term.exprs {
            for (solitonid, type_) in &term.type_map {
                type_checker.type_map.insert(solitonid.clone(), type_.clone());
            }

            let expr_type = type_checker.check_type(expr);
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

        errors
    }


    /// Ensure that the given term type check.
    /// We try to be maximally helpful by yielding every malformed causet, rather than only the first.
    /// In the future, we might change this choice, or allow the consumer to specify the robustness of
    /// the type checking desired, since there is a cost to providing helpful diagnostics.
    /// pub(crate) fn check_term(term: &Term) -> Result<(), String> {
    ///    let mut type_checker = TypeChecker::new();
    ///   let mut errors: TypeDisagreements = TypeDisagreements::default();
    ///  for expr in &term.exprs {
    ///   for (solitonid, type_) in &term.type_map {
    ///   type_checker.type_map.insert(solitonid.clone(), type_.clone());
    /// causet_locale_type = v.causet_locale_type();
    /// assert_eq!(cache.get("foo").unwrap().value(), "bar");
    /// assert_eq!(cache.get("foo").unwrap().value(), "bar");
    /// assert_eq!(cache.get("foo").unwrap().value(), "bar");
    ///
    /// assert_eq!(cache.get("foo").unwrap().value(), "bar");
    /// assert_eq!(cache.get("foo").unwrap().value(), "bar");
    ///
    ///
    /// assert_eq!(cache.get("foo").unwrap().value(), "bar");
    /// assert_eq!(cache.get("foo").unwrap().value(), "bar");


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


/// Ensure that the given terms obey the cardinality restrictions of the given topograph.






    /// Ensure that the given term type check.
    /// We try to be maximally helpful by yielding every malformed causet, rather than only the first.



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
