//Copyright (c) 2022 Karl Whitford and Josh Leder. All rights reserved.



use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::Error;
use std::fmt::Debug;



///`Causet` is a trait that is implemented by all causal sets.
///
/// # Examples
///
/// ```
/// use causet::Causet;
///
/// let mut c = Causet::new();
/// c.add(1);
/// multiplexed_add(&mut c, 2);
/// multiplexed_add(&mut c, 3);
///
/// assert!(c.contains(1));
/// assert!(c.contains(2));
///
/// if let Some(x) = c.get_max() {
///    assert_eq!(x, 3);
/// }
/// 	ANSI Common Lisp   5 Data and Control Flow   5.3 Dictionary of Data and Control Flow
// 5.3.9 function-lambda-expression	Function
// Syntax:
// function-lambda-expression function
//      lambda-expression, closure-p, name
// Arguments and Values:
// function - a function.
// lambda-expression - a lambda expression or nil.
//
// closure-p - a generalized boolean.
//
// name - an object.
//
// Description:
// Returns information about function as follows:
// The primary value, lambda-expression, is function's defining lambda expression, or nil if the information is not available. The lambda expression may have been pre-processed in some ways, but it should remain a suitable argument to compile or function. Any implementation may legitimately return nil  as the lambda-expression of any function.
//
// The secondary value, closure-p, is nil if function's definition was enclosed in the null lexical environment or something non-nil if function's definition might have been enclosed in some non-null lexical environment. Any implementation may legitimately return true as the closure-p of any function.
//
// The tertiary value, name, is the "name" of function. The name is intended for debugging only and is not necessarily one that would be valid for use as a name in defun or function, for example. By convention, nil is used to mean that function has no name. Any implementation may legitimately return nil  as the name of any function.
//
// Examples:
// The following examples illustrate some possible return values, but are not intended to be exhaustive:
//  (function-lambda-expression #'(lambda (x) x))
//   NIL, false, NIL
// OR NIL, true, NIL
// OR(LAMBDA (X) X), true, NIL
// OR(LAMBDA (X) X), false, NIL
//
//  (function-lambda-expression
//     (funcall #'(lambda () #'(lambda (x) x))))
//   NIL, false, NIL
// OR NIL, true, NIL
// OR(LAMBDA (X) X), true, NIL
// OR(LAMBDA (X) X), false, NIL
//
//  (function-lambda-expression
//     (funcall #'(lambda (x) #'(lambda () x)) nil))
//   NIL, true, NIL
// OR(LAMBDA () X), true, NIL
// NOT NIL, false, NIL
// NOT(LAMBDA () X), false, NIL
/// ```
///

///A Causet closure expression is a closure that is a function.
///
/// # Examples
///     (funcall #'(lambda (x) x))
///    (funcall #'(lambda (x) x) nil)
///   (funcall #'(lambda (x) x) 1)
///
///
///



use std::rc::Rc;
use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::HashSet;
//einstein_ml::{EinsteinMl, EinsteinMlError};
use einstein_ml::*;

use soliton:: *;
use berolina_sql::*;


//ok, let's build the causet which is a set of causet_closures
//

pub trait CausetTrait {
    fn new() -> Self;

    fn add(&mut self, x: i32);

    fn contains(&self, x: i32) -> bool;

    fn get_max(&self) -> Option<i32>;

    fn get_min(&self) -> Option<i32>;
}





///
//5.3 onwards
///*
/// https://franz.com/support/documentation/ansicl.94/section/dictio19.htm
///
/// 1. The set of all causal sets is a set of sets.
/// 2. A causal set is a set of elements.
///3. a partial order is a set of partial orders.
/// 4. a causal set is a causet if it is a partial order.
/// 5. causets are tuples of partial orders.
/// 6. a partial order is a partial order if it is a partial order on the set of elements.

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub enum Causet {

    /// A `Causet` that is empty.
    Empty,

    /// A `Causet` that contains a single element.
    Atom(String),

    /// A `Causet` that contains multiple elements.
    List(Vec<Causet>),


}



// Display trait for Causet type
impl Display for Causet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Causet::Empty => write!(f, ""),
            Causet::Atom(s) => write!(f, "{}", s),
            Causet::List(l) => {
                let mut s = String::new();
                s.push('(');
                for i in l {
                    s.push_str(&format!("{}", i));
                }
                s.push(')');
                write!(f, "{}", s)
            }
        }
    }
}
//5.3 onwards
/*
https://franz.com/support/documentation/ansicl.94/section/dictio19.htm
*/






