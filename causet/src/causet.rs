//Copyright (c) 2022 Karl Whitford and Josh Leder. All rights reserved.
// Copyright (c) 2020 The EinsteinDB Authors
// Distributed under the MIT software license, see the accompanying
// file LICENSE or https://www.opensource.org/licenses/mit-license.php.

////////////////////////////////////////////////////////////////////////////////


use einstein_ml::*;
use einstein_ml::lisp::*;
use einstein_ml::lisp::lisp_types::*;
use super::*;

use std::rc::Rc;
use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::HashSet;
//einstein_ml::{EinsteinMl, EinsteinMlError};
use einstein_ml::*;

use soliton:: *;
use berolina_sql::*;
use berolina_sql::sql_types::*;
use berolina_sql::sql_types::sql_types::*;


use std::collections::HashMap;
use std::collections::HashSet;
use std::rc::Rc;
use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::HashSet;


use std::rc::Rc;
use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::HashSet;
use std::rc::Rc;
use std::cell::RefCell;



///time
/// time_stamp


use std::rc::Rc;
use std::cell::RefCell;
use std::collections::HashMap;


use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::Error;
use std::fmt::Debug;
use std::fmt::Result;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};




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

//ok, let's build the causet which is a set of causet_closures
//

pub trait CausetTrait {

    fn new() -> Self;

    fn add(&mut self, x: i32);

    fn contains(&self, x: i32) -> bool;

    fn get_max(&self) -> Option<i32>;

    fn get_min(&self) -> Option<i32>;
}






///A Causet closure expression is a closure that is a function.
/// # Examples
///    (funcall #'(lambda (x) x))
///  (funcall #'(lambda (x) x) nil)
/// (funcall #'(lambda (x) x) 1)
/// # Examples
///    (funcall #'(lambda (x) x))
/// (funcall #'(lambda (x) x) nil)
/// (funcall #'(lambda (x) x) 1)
/// # Examples
///   (funcall #'(lambda (x) x))
/// (funcall #'(lambda (x) x) nil)
/// (funcall #'(lambda (x) x) 1)
///
///
///




pub trait CausetClosureTrait {
    fn new() -> Self;
    fn call(&self, x: i32) -> i32;
}


pub struct CausetClosure {
    pub closure: Rc<RefCell<LispClosure>>,
}

pub struct PosetClosure {
    pub closure: Rc<RefCell<LispClosure>>,
}




pub struct Poset {
    pub closure: Rc<RefCell<LispClosure>>,
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
}   // Display trait for Causet type

// Display trait for Causet type




use EinsteinDB::berolinasql::{
    berolina_sql::sql_types::sql_types::*,

    self,
    db::{self, DB},
    error::{self, Error},
    options::{self, Options},
    types::{self, Type},
    util::{self, Key},
};


/// Causets shall follow the following proposition : Linearizability, a widely-accepted correctness property
/// for shared objects, is grounded in classical physics.
/// Its definition assumes a total temporal order over invocation and response events, which is tantamount to assuming
/// the existence
/// of a global clock that determines the time of each event. By contrast, according to Einstein’s theory of relativity, there can be no global clock: time itself is relative. For example, given two events A and B, one observer may perceive A occurring before B, another may perceive B occurring before A, and yet another may perceive A and B occurring simultaneously,with respect to local time.


///We'll compose two different L1-L3 virtualized semver asymptotes of EinsteinDB replicas using berolinaSQL, Causet, and Causetq
/// as the intermediate interpretation and join-optimizer. Thusly, we shall incorporate the notion of an sRDMA isolated and opinionated namespaced superspace for both user and admin.
/// What distinguishes the concept of a causet, to say a tuplespace; is the result of additional bit signatures to the XOR of the primary key and the secondary key.
/// The primary key is the primary key of the underlying database table. The secondary key is the secondary key of the underlying database table.
/// We use Lamport virtual clocks to bring about a metric which tells us the throughput of the causet.
/// WE name two different timelines as being lightlike and timelike. This is because we want to be able to compare the throughput of two causets.
/// In a relativistically linearizable bolt-on consistency model, the causet is lightlike.
/// In a non-relativistically linearizable bolt-on consistency model, the causet is timelike.
/// We shall use the following naming convention for causet:



#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TimelikeCauset {
    pub primary_key: String,
    pub secondary_key: String,
    pub value: String,
    pub timestamp: i64,
}

pub enum LamportClock {
    Lightlike,
    Timelike,
}





/// causet_<lightlike or timelike>_<primary key>_<secondary key>
/// The primary key is the primary key of the underlying database table. The secondary key is the secondary key of the underlying database table.
/// We use Lamport virtual clocks to bring about a metric which tells us the throughput of the causet.
/// WE name two different timelines as being lightlike and timelike. This is because we want to be able to compare the throughput of two causets.
/// In a relativistically linearizable bolt-on consistency model, the causet is lightlike.
/// In a non-relativistically linearizable bolt-on consistency model, the causet is timelike.
/// We shall use the following naming convention for causet:
///


///We'll compose two different L1-L3 virtualized semver asymptotes of EinsteinDB replicas using berolinaSQL, Causet, and Causetq
/// as the intermediate interpretation and join-optimizer. Thusly, we shall incorporate the notion of an sRDMA isolated and opinionated namespaced superspace for both user and admin.



#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ClosedtimelikeCauset<T> {
    pub primary_key: T,
    pub secondary_key: T,
    pub lamport_clock: Arc<Mutex<LamportClock>>,
    pub last_update: Arc<Mutex<Instant>>,
    pub value: Arc<Mutex<T>>,
}


impl<T> ClosedtimelikeCauset<T> {
    pub fn new(primary_key: T, secondary_key: T, value: T) -> ClosedtimelikeCauset<T> {
        ClosedtimelikeCauset {
            primary_key,
            secondary_key,
            lamport_clock: Arc::new(Mutex::new(LamportClock::Lightlike)),
            last_update: Arc::new(Mutex::new(Instant::now())),
            value: Arc::new(Mutex::new(value)),
        }
    }
}


impl<T> ClosedtimelikeCauset<T> {
    pub fn get_value(&self) -> T {
        self.value.lock().unwrap().clone()
    }
}


impl<T> ClosedtimelikeCauset<T> {
    pub fn get_primary_key(&self) -> T {
        self.primary_key.clone()
    }
}



///!// 5.3.9 function-lambda-expression	Function
// // Syntax:
// // function-lambda-expression function
// //      lambda-expression, closure-p, name
// // Arguments and Values:
// // function - a function.
// // lambda-expression - a lambda expression or nil.
// //
// // closure-p - a generalized boolean.
// //
// // name - an object.
// //
// // Description:
// // Returns information about function as follows:
// // The primary value, lambda-expression, is function's defining lambda expression, or nil if the information is not available. The lambda expression may have been pre-processed in some ways, but it should remain a suitable argument to compile or function. Any implementation may legitimately return nil  as the lambda-expression of any function.
// //
// // The secondary value, closure-p, is nil if function's definition was enclosed in the null lexical environment or something non-nil if function's definition might have been enclosed in some non-null lexical environment. Any implementation may legitimately return true as the closure-p of any function.
// //
// // The tertiary value, name, is the "name" of function. The name is intended for debugging only and is not necessarily one that would be valid for use as a name in defun or function, for example. By convention, nil is used to mean that function has no name. Any implementation may legitimately return nil  as the name of any function.
// //
// // Examples:
// // The following examples illustrate some possible return values, but are not intended to be exhaustive:
// //  (function-lambda-expression #'(lambda (x) x))
// //   NIL, false, NIL
// // OR NIL, true, NIL
// // OR(LAMBDA (X) X), true, NIL
// // OR(LAMBDA (X) X), false, NIL
// //
// //  (function-lambda-expression
// //     (funcall #'(lambda () #'(lambda (x) x))))
// //   NIL, false, NIL
// // OR NIL, true, NIL
// // OR(LAMBDA (X) X), true, NIL
// // OR(LAMBDA (X) X), false, NIL
// //
// //  (function-lambda-expression
// //     (funcall #'(lambda (x) #'(lambda () x)) nil))
// //   NIL, true, NIL
// // OR(LAMBDA () X), true, NIL
// // NOT NIL, false, NIL
// // NOT(LAMBDA () X), false, NIL
// /// ```
///!CausetQ embeds itself into an Einstein_ML Clojar like manner.
/// We shall use the following naming convention for causet:
/// causet_<lightlike or timelike>_<primary key>_<secondary key>_<secondary key>
/// The primary key is the primary key of the underlying database table. The secondary key is the secondary key of the underlying database table. The
/// We use Lamport virtual clocks to bring about a metric which tells us the throughput of the causet.
/// WE name two different timelines as being lightlike and timelike. This is because we want to be able to compare the throughput of two causets.
/// In a relativistically linearizable bolt-on consistency model, the causet is lightlike.
///
/// In a non-relativistically linearizable bolt-on consistency model, the causet is timelike.



#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ClosedtimelikeCausetq<T> {
    pub primary_key: T,
    pub secondary_key: T,
    pub lamport_clock: Arc<Mutex<LamportClock>>,
    pub last_update: Arc<Mutex<Instant>>,
    pub value: Arc<Mutex<T>>,
}

pub fn causet_lightlike_primary_key_secondary_key_secondary_key<T>(primary_key: T, secondary_key: T, value: T) -> ClosedtimelikeCausetq<T> {
    ClosedtimelikeCausetq {
        primary_key,
        secondary_key,
        lamport_clock: Arc::new(Mutex::new(LamportClock::Lightlike)),
        last_update: Arc::new(Mutex::new(Instant::now())),
        value: Arc::new(Mutex::new(value)),
    }
}

///
/// The result you get from a 'rel' query, like:
///
/// ```einstein_ml::sql_types::RelResult<ClosedtimelikeCausetq<T>>```
/// is a vector of causetqs
///
/// The result you get from a 'rel' query, like:
/// ```einstein_ml::sql_types::RelResult<ClosedtimelikeCausetq<T>>```
/// this is a causetq or a vector of causetqs, in other words. A Q-Causet is a causet with a Q-Lamport clock.
/// A relativistic partial order of causetqs is a partial order of causetqs with a relativistic Lamport clock.
/// [:find ?person ?name :where [?person :person/name ?name] [?person :person/age ?age]]
///
/// example of a causetq:
///  :where [?person :person/name ?name]]
/// [?person :person/age ?age]]
///
/// ```
///
/// example of a causetq:
/// :where [?person :person/name ?name]]
/// Set<ClosedtimelikeCausetq<T>>
/// The result you get from a 'rel' query, like:
/// ```einstein_ml::sql_types::RelResult<ClosedtimelikeCausetq<T>>```
/// this is a causetq or a vector of causetqs, in other words. A Q-Causet is a causet with a Q-Lamport clock.

///
/// There are three ways to get data out of a `RelResult`:
/// - By iterating over rows as slices. Use `result.rows()`. This is efficient and is
///   recommended in two cases:
///   1. If you don't need to take ownership of the resulting values (e.g., you're comparing
///      or making a modified clone).
///   2. When the data you're retrieving is cheap to clone. All scalar values are relatively
///      cheap: they're either small values or `Rc`.
/// - By direct reference to a row by index, using `result.row(i)`. This also returns
///   a reference.
/// - By consuming the results using `into_iter`. This allocates short-lived vectors,
///   but gives you ownership of the enclosed `TypedValue`s.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RelResult<T> {
    pub width: usize,
    pub values: Vec<T>,
}


impl<T> RelResult<T> {
    pub fn new(width: usize, values: Vec<T>) -> RelResult<T> {
        RelResult {
            width,
            values,
        }
    }
}




impl<T> RelResult<T> {
    pub fn rows(&self) -> impl Iterator<Item = &[TypedValue]> {
        self.values.iter().map(|row| row.as_slice())
    }
}


impl<T> RelResult<T> {
    pub fn row(&self, i: usize) -> &[TypedValue] {
        self.values.get(i).unwrap().as_slice()
    }
}


impl<T> RelResult<T> {
    pub fn into_iter(self) -> impl Iterator<Item = T> {
        self.values.into_iter()
    }
}

///
/// The result you get from a 'rel' query, like:

pub type StructuredRelResult = RelResult<Binding>;


/// A causetq is a causet with a Q-Lamport clock.


pub type BoltOnTuples = Vec<(BoltOnTuple, BoltOnTuple)>;


pub type BoltOnTuple = (BoltOnTuplePrimaryKey, BoltOnTupleSecondaryKey);


impl<T> RelResult<T> {
    pub fn empty(width: usize) -> RelResult<T> {
        RelResult {
            width: width,
            values: Vec::new(),
        }
    }

    pub fn from_tuples(tuples: BoltOnTuples) -> RelResult<T> {
        let mut values = Vec::new();
        for (primary_key, secondary_key) in tuples {
            values.push(BoltOnTuple::new(primary_key, secondary_key));
        }
        RelResult {
            width: 2,
            values: values,
        }
    }

    pub fn from_tuples_with_width(tuples: BoltOnTuples, width: usize) -> RelResult<T> {
        let mut values = Vec::new();
        for (primary_key, secondary_key) in tuples {
            values.push(BoltOnTuple::new(primary_key, secondary_key));
        }
        RelResult {
            width: width,
            values: values,
        }
    }

    pub fn from_tuples_with_width_and_values(width: usize, values: Vec<T>) -> RelResult<T> {
        RelResult {
            width: width,
            values: values,
        }
    }

    pub fn from_tuples_with_width_and_values_and_lamport_clock(width: usize, values: Vec<T>, lamport_clock: Arc<Mutex<LamportClock>>) -> RelResult<T> {
        let rel_result = RelResult {
            width: width,
            values: values,

        };
        rel_result
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn row_count(&self) -> usize {
        self.values.len() / self.width
    }

    pub fn rows(&self) -> ::std::slice::Chunks<T> {
        // TODO: Nightly-only API `exact_chunks`. #47115.
        self.values.chunks(self.width)
    }

    pub fn row(&self, index: usize) -> Option<&[T]> {
        let end = self.width * (index + 1);
        if end > self.values.len() {
            None
        } else {
            let start = self.width * index;
            Some(&self.values[start..end])
        }
    }
}



pub fn interlocking_async_wait_free_o_naught_key<T>(primary_key: T, secondary_key: T, value: T) -> ClosedtimelikeCausetq<T> {

  switch_to_timelike_causet(primary_key, secondary_key, value)(<ClosedtimelikeCausetq<T> as CosetPoset>::new(primary_key, secondary_key, value))
}





    // ClosedtimelikeCausetq {
    //     primary_key,
    //     secondary_key,
    //     lamport_clock: Arc::new(Mutex::new(LamportClock::Lightlike)),
    //     last_update: Arc::new(Mutex::new(Instant::now())),
    //     value: Arc::new(Mutex::new(value)),
//     // }
//     ClosedtimelikeCausetq {
//         primary_key,
//         secondary_key,
//         lamport_clock: Arc::new(Mutex::new(LamportClock::Lightlike)),
//         last_update: Arc::new(Mutex::new(Instant::now())),
//         value: Arc::new(Mutex::new(value)),
//     }
// }

impl<T> Causet<T> {
    pub fn new(primary_key: T, secondary_key: T, value: T) -> Causet<T> {
        Causet {
            primary_key,
            secondary_key,
            lamport_clock: Arc::new(Mutex::new(LamportClock::new())),
            last_update: Arc::new(Mutex::new(Instant::now())),
            value: Arc::new(Mutex::new(value)),
        }
    }
}



///![Causet]
/// We shall use the following naming convention for causet:
/// causet_<lightlike or timelike>_<primary key>_<secondary key>
/// The primary key is the primary key of the underlying database table. The secondary key is the secondary key of the underlying database table.
/// We use Lamport virtual clocks to bring about a metric which tells us the throughput of the causet.
/// WE name two different timelines as being lightlike and timelike. This is because we want to be able to compare the throughput of two causets.
/// In a relativistically linearizable bolt-on consistency model, the causet is lightlike.
///
/// In a non-relativistically linearizable bolt-on consistency model, the causet is timelike.




#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Causetq<T> {
    pub primary_key: T,
    pub secondary_key: T,
    pub lamport_clock: Arc<Mutex<LamportClock>>,
    pub last_update: Arc<Mutex<Instant>>,
    pub value: Arc<Mutex<T>>,
}


impl<T> Causetq<T> {

    pub fn new(primary_key: T, secondary_key: T, value: T) -> Causetq<T> {
        Causetq {
            primary_key,
            secondary_key,
            lamport_clock: Arc::new(Mutex::new(LamportClock::new())),
            last_update: Arc::new(Mutex::new(Instant::now())),
            value: Arc::new(Mutex::new(value)),
        }
    }
}


impl<T> Causetq<T> {
    pub fn new_with_lamport_clock(primary_key: T, secondary_key: T, lamport_clock: Arc<Mutex<LamportClock>>, value: T) -> Causetq<T> {
        Causetq {
            primary_key,
            secondary_key,
            lamport_clock,
            last_update: Arc::new(Mutex::new(Instant::now())),
            value: Arc::new(Mutex::new(value)),
        }
    }
}


impl<T> Causetq<T> {
    pub fn new_with_lamport_clock_and_last_update(primary_key: T, secondary_key: T, lamport_clock: Arc<Mutex<LamportClock>>, last_update: Arc<Mutex<Instant>>, value: T) -> Causetq<T> {
        Causetq {
            primary_key,
            secondary_key,
            lamport_clock,
            last_update,
            value: Arc::new(Mutex::new(value)),
        }
    }
}


impl<T> Causetq<T> {
    pub fn new_with_lamport_clock_and_last_update_and_value(primary_key: T, secondary_key: T, lamport_clock: Arc<Mutex<LamportClock>>, last_update: Arc<Mutex<Instant>>, value: Arc<Mutex<T>>) -> Causetq<T> {
        Causetq {
            primary_key,
            secondary_key,
            lamport_clock,
            last_update,
            value,
        }
    }
}


impl<T> Causetq<T> {
    pub fn new_with_lamport_clock_and_value(primary_key: T, secondary_key: T, lamport_clock: Arc<Mutex<LamportClock>>, value: Arc<Mutex<T>>) -> Causetq<T> {
        Causetq {
            primary_key,
            secondary_key,
            lamport_clock,
            last_update: Arc::new(Mutex::new(Instant::now())),
            value,
        }
    }
}





#[allow(dead_code)]

pub struct Causetq_<T> {
    pub primary_key: T,
    pub secondary_key: T,
    pub lamport_clock: Arc<Mutex<LamportClock>>,
    pub last_update: Arc<Mutex<Instant>>,
    pub value: Arc<Mutex<T>>,
}

pub fn causetq_<T>(primary_key: T, secondary_key: T, value: T) -> Causetq<T> {
    Causetq {
        primary_key,
        secondary_key: (),
        lamport_clock: Arc::new(Mutex::new(LamportClock::new())),
        last_update: Arc::new(Mutex::new(Instant::now())),
        value: Arc::new(Mutex::new(value)),

    }
}

fn switch_to_db_thread(primary_key: StringRef, secondary_key: StringRef, value: StringRef) -> Causetq_<StringRef> {
    Causetq_ {
        primary_key,
        secondary_key,
        lamport_clock: Arc::new(Mutex::new(LamportClock::new())),
        last_update: Arc::new(Mutex::new(Instant::now())),
        value: Arc::new(Mutex::new(value)),
    }

}


impl<T> Causetq<T> {
    pub fn get_value(&self) -> T {
        self.value.lock().unwrap().clone()
    }
}


impl<T> Causetq<T> {
    pub fn get_primary_key(&self) -> T {
        self.primary_key.clone() as Then<T>()
    }
}



impl<T> Causetq<T> {
    pub fn get_secondary_key(&self) -> T {
        let mutex = self.lamport_clock.lock().unwrap();
        if mutex.is_lightlike() {
            self.primary_key.clone()
        } else {
            self.secondary_key.clone()
        }
    }
}


impl<T> Causetq<T> {
    pub fn get_lamport_clock(&self) -> LamportClock {
        self.lamport_clock.lock().unwrap().clone()
    }

    pub fn get_last_update(&self) -> Instant {
        self.last_update.lock().unwrap().clone()
    }

    pub fn get_value(&self) -> T {
        self.value.lock().unwrap().clone()
    }

    pub fn set_value(&self, value: T) {
        self.value.lock().unwrap().clone_from(&value);
    }

    pub fn set_lamport_clock(&self, lamport_clock: LamportClock) {
        self.lamport_clock.lock().unwrap().clone_from(&lamport_clock);
    }

    pub fn set_last_update(&self, last_update: Instant) {
        self.last_update.lock().unwrap().clone_from(&last_update);
    }

    pub fn set_primary_key(&self, primary_key: T) {
        self.primary_key.clone_from(&primary_key);
    }

    pub fn set_secondary_key(&self, secondary_key: T) {
        self.secondary_key.clone_from(&secondary_key);
    }

    pub fn set_value_and_lamport_clock(&self, value: T, lamport_clock: LamportClock) {
        self.value.lock().unwrap().clone_from(&value);
        self.lamport_clock.lock().unwrap().clone_from(&lamport_clock);
    }

    pub fn set_value_and_last_update(&self, value: T, last_update: Instant) {
        self.value.lock().unwrap().clone_from(&value);
        self.last_update.lock().unwrap().clone_from(&last_update);
    }

    pub fn set_lamport_clock_and_last_update(&self, lamport_clock: LamportClock, last_update: Instant) {
        self.lamport_clock.lock().unwrap().clone_from(&lamport_clock);
        self.last_update.lock().unwrap().clone_from(&last_update);
    }


    pub fn set_value_and_lamport_clock_and_last_update(&self, value: T, lamport_clock: LamportClock, last_update: Instant) {
        self.value.lock().unwrap().clone_from(&value);
        self.lamport_clock.lock().unwrap().clone_from(&lamport_clock);
        self.last_update.lock().unwrap().clone_from(&last_update);
    }


    pub fn set_value_and_lamport_clock_and_last_update_and_primary_key(&self, value: T, lamport_clock: LamportClock, last_update: Instant, primary_key: T) {
        self.value.lock().unwrap().clone_from(&value);
        self.lamport_clock.lock().unwrap().clone_from(&lamport_clock);
        self.last_update.lock().unwrap().clone_from(&last_update);
        self.primary_key.clone_from(&primary_key);
    }

    pub fn set_value_and_lamport_clock_and_last_update_and_primary_key_and_secondary_key(&self, value: T, lamport_clock: LamportClock, last_update: Instant, primary_key: T, secondary_key: T) {
        self.value.lock().unwrap().clone_from(&value);
        self.lamport_clock.lock().unwrap().clone_from(&lamport_clock);
        self.last_update.lock().unwrap().clone_from(&last_update);
        self.primary_key.clone_from(&primary_key);
        self.secondary_key.clone_from(&secondary_key);

        for event in self.lamport_clock.lock().unwrap().get_events() {
            println!("{:?}", event);
        }

        println!("{:?}", self.lamport_clock.lock().unwrap().get_events());


        #[allow(dead_code)]
        fn spacelike_mux(primary_key: StringRef, secondary_key: StringRef, value: StringRef) -> Causetq_<StringRef, String> {
            Causetq_ {
                primary_key,
                secondary_key,
                lamport_clock: Arc::new(Mutex::new(LamportClock::new())),
                last_update: Arc::new(Mutex::new(Instant::now())),
                value: Arc::new(Mutex::new(value)),
            }
        }

        #[allow(dead_code)]
        fn spacelike_demux(primary_key: StringRef, secondary_key: StringRef, value: StringRef) -> Causetq_<StringRef, String> {
            Causetq_ {
                primary_key,
                secondary_key,
                lamport_clock: Arc::new(Mutex::new(LamportClock::new())),
                last_update: Arc::new(Mutex::new(Instant::now())),
                value: Arc::new(Mutex::new(value)),
            }
        }

        #[allow(dead_code)]
        fn spacelike_mux_<T>(primary_key: T, secondary_key: T, value: T) -> Causetq_<T, T> {
            Causetq_ {
                primary_key,
                secondary_key,
                lamport_clock: Arc::new(Mutex::new(LamportClock::new())),
                last_update: Arc::new(Mutex::new(Instant::now())),
                value: Arc::new(Mutex::new(value)),
            }
        }

        #[allow(dead_code)]
        fn spacelike_demux_<T>(primary_key: T, secondary_key: T, value: T) -> Causetq_<T, T> {
            Causetq_ {
                primary_key,
                secondary_key,
                lamport_clock: Arc::new(Mutex::new(LamportClock::new())),
                last_update: Arc::new(Mutex::new(Instant::now())),
                value: Arc::new(Mutex::new(value)),
            }
        }
    }

    pub fn get_primary_key(&self) -> T {
        self.primary_key.clone()
    }

    pub fn get_secondary_key(&self) -> T {
        self.secondary_key.clone()
    }
}
/// We shall use the following naming convention for causet:
/// causet_<lightlike or timelike>_<primary key>_<secondary key>
/// The primary key is the primary key of the underlying database table. The secondary key is the secondary key of the underlying database table.
/// We use Lamport virtual clocks to bring about a metric which tells us the throughput of the causet.
/// WE name two different timelines as being lightlike and timelike. This is because we want to be able to compare the throughput of two causets.
///


#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LightlikeCauset<T> {
    pub primary_key: T,
    pub secondary_key: T,
    pub lamport_clock: Arc<Mutex<LamportClock>>,
    pub last_update: Arc<Mutex<Instant>>,
    pub value: Arc<Mutex<T>>,
}


impl<T> LightlikeCauset<T> {
    pub fn new(primary_key: T, secondary_key: T, value: T) -> LightlikeCauset<T> {
        LightlikeCauset {
            primary_key,
            secondary_key,
            lamport_clock: Arc::new(Mutex::new(LamportClock::Lightlike)),
            last_update: Arc::new(Mutex::new(Instant::now())),
            value: Arc::new(Mutex::new(value)),
        }


    }
}
    pub fn new(primary_key: T, value: T) -> LightlikeCauset<T> {

    let causet = Causet::new(StringRef::new("primary key"), StringRef::new("secondary key"), StringRef::new("value"));
for i in 0..10 {
    let causet = Causet::new(StringRef::new("primary key"), StringRef::new("secondary key"), StringRef::new("value"));
async fn get_value(causet: &Causet) -> StringRef {
    causet.get_value()
}
}
}
