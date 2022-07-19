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
use ::std::ops::{ Mul, MulAssign, Sub, SubAssign};


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
use std::error;
use std::marker::PhantomData;

use ::EinsteinDB::einstein_ml::value::Value;
use ::EinsteinDB::einstein_ml::value::ValueType;
use ::EinsteinDB::einstein_ml::value::ValueType::*;


use ::EinsteinDB::einstein_ml::value::ValueRef;
use ::EinsteinDB::einstein_ml::value::ValueRefMut;
use ::EinsteinDB::einstein_ml::value::ValueRefMut::*;
use crate::ast::Solitonid;
use crate::einstein_ml_stdout::Value;


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

    fn from_arc(val: Arc<T>) -> Self {
        Rc::new(val.deref().clone())
    }
}

impl<T> FromRc<T> for Arc<T> where T: Sized + Clone {
    fn from_rc(val: Rc<T>) -> Self {
        match Rc::<T>::try_unwrap(val) {
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
        match Rc::<T>::try_unwrap(val) {
            Ok(v) => Self::new(v),
            Err(r) => Self::new(r.cloned()),
        }
    }

    fn from_arc(val: Arc<T>) -> Self {
        match Arc::<T>::try_unwrap(val) {
            Ok(v) => Self::new(v),
            Err(r) => Self::new(r.cloned()),
        }
    }
}



#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct ErrorImpl {


    pub message: String,
    pub cause: Option<Box<dyn error::Error + Send + Sync>>,
}

#[macro_use]
pub mod mvrsi
{
    pub use EinsteinDB::einstein_ml::value::mvrsi::{
        MVRSI,
        MVRSI_SCHEMA_VERSION,
    };
}


pub mod db_
{
    pub use EinsteinDB::einstein_ml::value::db_::{
        DB,
        DB_SCHEMA_VERSION,
    };
}


pub mod cache
{
    pub use EinsteinDB::einstein_ml::value::cache::{
        Cache,
        Cache_SCHEMA_VERSION,
    };
}


pub mod bootstrap
{
    pub use EinsteinDB::einstein_ml::value::bootstrap::{
        CORE_SCHEMA_VERSION,
    };
}




pub mod causetids
{
    pub use EinsteinDB::einstein_ml::value::causetids::einsteindb_SCHEMA_CORE;
}

/// add sized type to the value type
/// this is used to add the size of the value to the value type



pub trait ValueSized {
    fn size(&self) -> usize;
}







// We do this a lot for errors.
pub trait Cloned<T> {


    fn cloned(&self) -> T;
    fn to_causet_locale_rc(&self) -> ValueRc<T>;
}


impl<T> Cloned<T> for T {

    fn cloned(&self) -> T {
        self.clone()
    }
    fn to_causet_locale_rc(&self) -> ValueRc<T> {
        ValueRc::new(self.clone())
    }


}


impl<T> Cloned<T> for Rc<T> {
    fn cloned(&self) -> T {
        self.clone().deref().clone()
    }
    fn to_causet_locale_rc(&self) -> ValueRc<T> {
        ValueRc::new(self.clone())
    }


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

pub trait TimeStamp {
    fn timestamp(&self) -> u64;
}







#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct ValueRc<T> {
    pub value: Rc<T>,
    pub timestamp: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct ValueArc<T> {
    pub value: Arc<T>,
    pub timestamp: u64,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct ValueBox<T> {
    pub value: Box<T>,
    pub timestamp: u64,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct ValueRef<T> {
    pub value: T,
    pub timestamp: u64,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct ValueRefMut<T> {
    pub value: T,
    pub timestamp: u64,
}


pub static DISCRETE_MORSE_MAIN: i64 = 0;
pub static DISCRETE_MORSE_MAIN_0: i64 = 0;
pub static DISCRETE_MORSE_MAIN_1: i64 = 1;
pub static DISCRETE_MORSE_MAIN_2: i64 = 2;
pub static DISCRETE_MORSE_MAIN_3: i64 = 3;
pub static DISCRETE_MORSE_MAIN_4: i64 = 4;
pub static DISCRETE_MORSE_MAIN_5: i64 = 5;
pub static DISCRETE_MORSE_MAIN_6: i64 = 6;
pub static DISCRETE_MORSE_MAIN_7: i64 = 7;


pub static DISCRETE_MORSE_MAIN_8: i64 = 8;



///! The Value class is the base class for all values in EinsteinDB.
/// ! It is a reference counted object.
/// ! It is a wrapper around a pointer to a ValueImpl object.
/// ! It is a wrapper around a pointer to a ValueImpl object.
/// ! IF


#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct ValueImpl {
    pub value_type: ValueType,
    pub value: Box<dyn ValueSized + Send + Sync>, // Values are Send + Sync.
    pub value_ref: Rc<RefCell<ValueImpl>>,
    pub value_ref_mut: Rc<RefCell<ValueImpl>>,
    pub value_ref_mut_ptr: Rc<RefCell<ValueImpl>>,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct ValueImplRef {
    pub value_type: ValueType,
    pub value: Value<T>,
    pub value_ref: Rc<RefCell<ValueImpl>>,
    pub value_ref_mut: Rc<RefCell<ValueImpl>>,
    pub value_ref_mut_ptr: Rc<RefCell<ValueImpl>>,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct ValueImplRefMut {
    pub value_type: ValueType,
    pub value: Value<T>,
    pub value_ref: Rc<RefCell<ValueImpl>>,
    pub value_ref_mut: Rc<RefCell<ValueImpl>>,
    pub value_ref_mut_ptr: Rc<RefCell<ValueImpl>>,
}


impl ValueImpl {
    fn clone(&self) -> Self {
        match self {
            WaLValueRc::ValueRc(v) => WaLValueRc::ValueRc(v.clone()),
            WaLValueRc::CausetidRc(v) => WaLValueRc::CausetidRc(v.clone()),
            WaLValueRc::SolitonidRc(v) => WaLValueRc::SolitonidRc(v.clone()),
        }
    }
}

struct ValueImplRefRef {
    value_type: ValueType,
    value: Value<T>,
    value_ref: Rc<RefCell<ValueImpl>>,
    value_ref_mut: Rc<RefCell<ValueImpl>>,
    value_ref_mut_ptr: Rc<RefCell<ValueImpl>>,
}

impl ValueImplRef {

    fn clone(&self) -> Self {
        ValueImplRef {
            value_type: self.value_type.clone(),
            value: self.value.clone(),
            value_ref: self.value_ref.clone(),
            value_ref_mut: self.value_ref_mut.clone(),
            value_ref_mut_ptr: self.value_ref_mut_ptr.clone(),
        }


    }
}


impl ValueImplRefMut {

    fn clone(&self) -> Self {
        ValueImplRefMut {
            value_type: self.value_type.clone(),
            value: self.value.clone(),
            value_ref: self.value_ref.clone(),
            value_ref_mut: self.value_ref_mut.clone(),
            value_ref_mut_ptr: self.value_ref_mut_ptr.clone()
        }
    }
}




impl ValueImplRefRef {
    fn clone(&self) -> Self {
        ValueImplRefRef {
            value_type: self.value_type.clone(),
            value: self.value.clone(),
            value_ref: self.value_ref.clone(),
            value_ref_mut: self.value_ref_mut.clone(),
            value_ref_mut_ptr: self.value_ref_mut_ptr.clone()

        }

    }

}




impl ValueImplRefMut {

    fn clone(&self) -> Self {
        ValueImplRefMut {
            value_type: self.value_type.clone(),
            value: self.value.clone(),
            value_ref: self.value_ref.clone(),
            value_ref_mut: self.value_ref_mut.clone(),
            value_ref_mut_ptr: self.value_ref_mut_ptr.clone(),
        }
    }
}


impl ValueImplRefRef {
    fn clone(&self) -> Self {
        ValueImplRefRef {
            value_type: self.value_type.clone(),
            value: self.value.clone(),
            value_ref: self.value_ref.clone(),
            value_ref_mut: self.value_ref_mut.clone(),
            value_ref_mut_ptr: self.value_ref_mut_ptr.clone(),
        }
    }
}



impl ValueImplRefRef {
    fn clone(&self) -> Self {
        ValueImplRefRef {
            value_type: self.value_type.clone(),
            value: self.value.clone(),
            value_ref: self.value_ref.clone(),
            value_ref_mut: self.value_ref_mut.clone(),
            value_ref_mut_ptr: self.value_ref_mut_ptr.clone()

        }

    }

}


impl ValueImplRef {
    fn clone(&self) -> Self {
        ValueImplRef {
            value_type: self.value_type.clone(),
            value: self.value.clone(),
            value_ref: self.value_ref.clone(),
            value_ref_mut: self.value_ref_mut.clone(),
            value_ref_mut_ptr: self.value_ref_mut_ptr.clone(),
        }

    }

}


impl ValueImplRefMut {
    fn clone(&self) -> Self {
        ValueImplRefMut {
            value_type: self.value_type.clone(),
            value: self.value.clone(),
            value_ref: self.value_ref.clone(),
            value_ref_mut: self.value_ref_mut.clone(),
            value_ref_mut_ptr: self.value_ref_mut_ptr.clone(),
        }
    }
}



impl ValueImplRefRef {
    fn clone(&self) -> Self {
        ValueImplRefRef {
            value_type: self.value_type.clone(),
            value: self.value.clone(),
            value_ref: self.value_ref.clone(),
            value_ref_mut: self.value_ref_mut.clone(),
            value_ref_mut_ptr: self.value_ref_mut_ptr.clone()

        }

    }

}


impl ValueImplRefMut {
    fn clone(&self) -> Self {
        ValueImplRefMut {
            value_type: self.value_type.clone(),
            value: self.value.clone(),
            value_ref: self.value_ref.clone(),
            value_ref_mut: self.value_ref_mut.clone(),
            value_ref_mut_ptr: self.value_ref_mut_ptr.clone(),
        }
    }
}




impl ValueImplRefRef {
    fn drop(&mut self) {
        match self {
            WaLValueRc::ValueRc(v) => v.drop(),
            WaLValueRc::CausetidRc(v) => v.drop(),
            WaLValueRc::SolitonidRc(v) => v.drop(),
        }
    }
}




impl ValueImplRefMut {
    fn drop(&mut self) {
        match self {
            WaLValueRc::ValueRc(v) => v.drop(),
            WaLValueRc::CausetidRc(v) => v.drop(),
            WaLValueRc::SolitonidRc(v) => v.drop(),
        }
    }
}

pub trait WaLValueRc {
    fn clone(&self) -> Self;
    fn drop(&mut self);
}


impl WaLValueRc for ValueImplRef {
    fn clone(&self) -> Self {
        ValueImplRef {
            value_type: self.value_type.clone(),
            value: self.value.clone(),
            value_ref: self.value_ref.clone(),
            value_ref_mut: self.value_ref_mut.clone(),
            value_ref_mut_ptr: self.value_ref_mut_ptr.clone(),
        }
    }

    fn drop(&mut self) {
        match self {
            WaLValueRc::ValueRc(v) => v.drop(),
            WaLValueRc::CausetidRc(v) => v.drop(),
            WaLValueRc::SolitonidRc(v) => v.drop(),
        }
    }
}


impl WaLValueRc for ValueImplRefMut {
    fn clone(&self) -> Self {
        ValueImplRefMut {
            value_type: self.value_type.clone(),
            value: self.value.clone(),
            value_ref: self.value_ref.clone(),
            value_ref_mut: self.value_ref_mut.clone(),
            value_ref_mut_ptr: self.value_ref_mut_ptr.clone(),
        }
    }

    fn drop(&mut self) {
        match self {
            WaLValueRc::ValueRc(v) => v.drop(),
            WaLValueRc::CausetidRc(v) => v.drop(),
            WaLValueRc::SolitonidRc(v) => v.drop(),
        }
    }
}


impl WaLValueRc for ValueImplRefRef {
    fn clone(&self) -> Self {
        ValueImplRefRef {
            value_type: self.value_type.clone(),
            value: self.value.clone(),
            value_ref: self.value_ref.clone(),
            value_ref_mut: self.value_ref_mut.clone(),
            value_ref_mut_ptr: self.value_ref_mut_ptr.clone(),
        }
    }

    fn drop(&mut self) {
        match self {
            WaLValueRc::ValueRc(v) => v.drop(),
            WaLValueRc::CausetidRc(v) => v.drop(),
            WaLValueRc::SolitonidRc(v) => v.drop(),
        }
    }
}


impl WaLValueRc for ValueImplRefRef {
    fn clone(&self) -> Self {
        ValueImplRefRef {
            value_type: self.value_type.clone(),
            value: self.value.clone(),
            value_ref: self.value_ref.clone(),
            value_ref_mut: self.value_ref_mut.clone(),
            value_ref_mut_ptr: self.value_ref_mut_ptr.clone(),
        }
    }

    fn drop(&mut self) {
        match self {
            WaLValueRc::ValueRc(v) => v.drop(),
            WaLValueRc::CausetidRc(v) => v.drop(),
            WaLValueRc::SolitonidRc(v) => v.drop(),
        }
    }
}


impl WaLValueRc for ValueImplRefMut {
    fn clone(&self) -> Self {
        ValueImplRefMut {
            value_type: self.value_type.clone(),
            value: self.value.clone(),
            value_ref: self.value_ref.clone(),
            value_ref_mut: self.value_ref_mut.clone(),
            value_ref_mut_ptr: self.value_ref_mut_ptr.clone(),
        }
    }

    fn drop(&mut self) {
        match self {
            WaLValueRc::ValueRc(v) => v.drop(),
            WaLValueRc::CausetidRc(v) => v.drop(),
            WaLValueRc::SolitonidRc(v) => v.drop(),
        }
    }

}


impl WaLValueRc for ValueImplRefRef {
    fn clone(&self) -> Self {
        ValueImplRefRef {
            value_type: self.value_type.clone(),
            value: self.value.clone(),
            value_ref: self.value_ref.clone(),
            value_ref_mut: self.value_ref_mut.clone(),
            value_ref_mut_ptr: self.value_ref_mut_ptr.clone(),
        }
    }

    fn drop(&mut self) {
        match self {
            WaLValueRc::ValueRc(v) => v.drop(),
            WaLValueRc::CausetidRc(v) => v.drop(),
            WaLValueRc::SolitonidRc(v) => v.drop(),
        }
    }

}

