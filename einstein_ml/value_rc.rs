// Whtcorps Inc 2022 Apache 2.0 License; All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use ::std::rc::Rc;
use ::std::sync::Arc;
use ::std::cell::RefCell;
use ::std::fmt::Debug;
use ::std::fmt::Display;
use ::std::fmt::Formatter;
use ::std::fmt::Result;
use ::std::fmt::Error;
use ::std::fmt::Error as FmtError;
use ::std::fmt::Write;
use ::std::ops::Deref;
use ::std::ops::DerefMut;
use ::std::ops::Drop;
use ::std::ops::Index;
use ::std::ops::IndexMut;
use ::std::ops::Range;
use ::std::ops::RangeFrom;
use ::std::ops::RangeFull;
use ::std::ops::RangeTo;
use ::std::ops::RangeToInclusive;
use ::std::ops::IndexMut;


use ::std::ops::Add;
use ::std::ops::AddAssign;
use ::std::ops::BitAnd;
use ::std::ops::BitAndAssign;
use ::std::ops::BitOr;
use ::std::ops::BitOrAssign;
use ::std::ops::BitXor;
use ::std::ops::BitXorAssign;
use ::std::ops::Div;
use ::std::ops::DivAssign;

use ::EinsteinDB::einstein_ml::value::Value;
use ::EinsteinDB::einstein_ml::value::ValueType;
use ::EinsteinDB::einstein_ml::value::ValueType::*;

impl<T> ValueRc<T> {
    pub fn new(value: T) -> Self {
        ValueRc {

            value: Rc::new(RefCell::new(value)),
        }


    }

    pub fn get_value(&self) -> Rc<RefCell<T>> {
        self.value.clone()
    }

    pub fn get_value_mut(&mut self) -> Rc<RefCell<T>> {
        self.value.clone()
    }

    pub fn get_value_ref(&self) -> &T {
        self.value.borrow()
    }

    pub fn get_value_ref_mut(&mut self) -> &mut T {
        self.value.borrow_mut()
    }


}


impl<T> Deref for ValueRc<T> {

    type Target = T;

    fn deref(&self) -> &T {
        &self.value.borrow()
    }


}


impl<T> DerefMut for ValueRc<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.value.borrow_mut()
    }
}


impl<T> Drop for ValueRc<T> {
    fn drop(&mut self) {
        self.value.borrow_mut().drop();
    }
}


impl<T> Display for ValueRc<T>
where
    T: Display,
{
    fn fmt(&self, f: &mut Formatter) -> Result {
        self.value.borrow().fmt(f)
    }
}


impl<T> Debug for ValueRc<T>
where
    T: Debug,
{
    fn fmt(&self, f: &mut Formatter) -> Result {
        self.value.borrow().fmt(f)
    }
}


impl<T> Error for ValueRc<T>
where
    T: Error,
{
    fn description(&self) -> &str {
        self.value.borrow().description()
    }

    fn cause(&self) -> Option<&Error> {
        self.value.borrow().cause()
    }
}


impl<T> FmtError for ValueRc<T>
where
    T: FmtError,
{
    fn fmt(&self, f: &mut Formatter) -> Result {
        self.value.borrow().fmt(f)
    }
}


impl<T> Index<usize> for ValueRc<T> {
    type Output = T;

    fn index(&self, index: usize) -> &T {
        &self.value.borrow()[index]
    }
}


impl<T> IndexMut<usize> for ValueRc<T> {
    fn index_mut(&mut self, index: usize) -> &mut T {
        &mut self.value.borrow_mut()[index]
    }
}


impl<T> Index<Range<usize>> for ValueRc<T> {
    type Output = [T];

    fn index(&self, index: Range<usize>) -> &[T] {
        &self.value.borrow()[index]
    }
}


impl<T> IndexMut<Range<usize>> for ValueRc<T> {
    fn index_mut(&mut self, index: Range<usize>) -> &mut [T] {
        &mut self.value.borrow_mut()[index]
    }
}


impl<T> Index<RangeTo<usize>> for ValueRc<T> {
    type Output = [T];

    fn index(&self, index: RangeTo<usize>) -> &[T] {
        &self.value.borrow()[index]
    }
}


impl<T> IndexMut<RangeTo<usize>> for ValueRc<T> {
    fn index_mut(&mut self, index: RangeTo<usize>) -> &mut [T] {
        &mut self.value.borrow_mut()[index]
    }
}


impl<T> Index<RangeFrom<usize>> for ValueRc<T> {
    type Output = [T];

    fn index(&self, index: RangeFrom<usize>) -> &[T] {
        &self.value.borrow()[index]
    }
}


impl<T> IndexMut<RangeFrom<usize>> for ValueRc<T> {
    fn index_mut(&mut self, index: RangeFrom<usize>) -> &mut [T] {
        &mut self.value.borrow_mut()[index]
    }
}



use crate::einstein_db::value::Value;
use crate::einstein_db::value::ValueType;
use crate::einstein_db::causetq::Causetq;

///! # Rc Value
///
///     Rc Value is a reference counted wrapper around a Value.  It is used to \
///    provide a safe way to share values between threads.  It is also used to \
///   provide a safe way to share values between threads and to provide a \
///  reference counted wrapper around a Value. It is used to provide a safe \
/// way to share values between threads and to provide a reference counted \
/// wrapper around a Value.
///
/// ## Example
///
/// ```
/// use einstein_ml::value::Value;
/// use einstein_ml::value::ValueType;
/// use einstein_ml::value::ValueType::*;
/// use einstein_ml::value::ValueRc;
///
/// let mut v = Value::new(String::from("Hello World"));
/// let v_rc = ValueRc::new(v);
///
/// assert_eq!(v_rc.get_type(), String);
/// assert_eq!(v_rc.get_value(), String::from("Hello World"));
/// ```
///

pub trait FromRc<T> {
    fn from_rc(val: Rc<T>) -> Self;
    fn from_arc(val: Arc<T>) -> Self;
}

impl<T> FromRc<T> for Rc<T> where T: Sized + Clone {
    fn from_rc(val: Rc<T>) -> Self {
        val.clone()
    }


}

impl<T> FromRc<T> for Arc<T> where T: Sized + Clone {
    fn from_rc(val: Rc<T>) -> Self {
        match ::std::rc::Rc::<T>::try_unwrap(val) {
            Ok(v) => Self::new(v),
            Err(r) => Self::new(r.cloned()),
        }
    }

    fn from_arc(val: Arc<T>) -> Self {
        val.clone()
    }
}

impl<T> FromRc<T> for Box<T> where T: Sized + Clone {
    fn from_rc(val: Rc<T>) -> Self {
        match ::std::rc::Rc::<T>::try_unwrap(val) {
            Ok(v) => Self::new(v),
            Err(r) => Self::new(r.cloned()),
        }
    }

    fn from_arc(val: Arc<T>) -> Self {
        match ::std::sync::Arc::<T>::try_unwrap(val) {
            Ok(v) => Self::new(v),
            Err(r) => Self::new(r.cloned()),
        }
    }
}

// We do this a lot for errors.
pub trait Cloned<T> {
    fn cloned(&self) -> T;
    fn to_causet_locale_rc(&self) -> ValueRc<T>;
}

impl<T: Clone> Cloned<T> for Rc<T> where T: Sized + Clone {
    fn cloned(&self) -> T {
        (*self.as_ref()).clone()
    }

    fn to_causet_locale_rc(&self) -> ValueRc<T> {
        ValueRc::from_rc(self.clone())
    }
}

impl<T: Clone> Cloned<T> for Arc<T> where T: Sized + Clone {
    fn cloned(&self) -> T {
        (*self.as_ref()).clone()
    }

    fn to_causet_locale_rc(&self) -> ValueRc<T> {
        ValueRc::from_arc(self.clone())
    }
}

impl<T: Clone> Cloned<T> for Box<T> where T: Sized + Clone {
    fn cloned(&self) -> T {
        self.as_ref().clone()
    }

    fn to_causet_locale_rc(&self) -> ValueRc<T> {
        ValueRc::new(self.cloned())
    }
}

///
/// This type alias exists to allow us to use different boxing mechanisms for causet_locales.
/// This type must implement `FromRc` and `Cloned`, and a `From` impleEinsteinDBion must exist for
/// `causetq_TV`.
///
pub type ValueRc<T> = Arc<T>;
