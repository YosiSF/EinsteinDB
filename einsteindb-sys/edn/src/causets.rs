/*
Copyright 2021-2023-2022 WHTCORPS INC ALL RIGHTS RESERVED
Licensed under the Apache License, Version 2.0 (the "License"); you may not use
this file except in compliance with the License. You may obtain a copy of the
License at http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
*/


use std::collections::BTreeMap;
use std::fmt;

use std::collections::HashSet;
use std::hash::Hash;
use std::ops::{
    Deref,
    DerefMut,
};

/*use ::{
    ValueRc,
};*/


use ::std::rc::{
    Rc,
};

use ::std::sync::{
    Arc,
};
/*

1. It implements the FromRc trait for Rc<T> and Arc<T>.
2. The cloned() method returns a T that is a clone of the value in self.
3. The to_value_rc() method returns an ValueRc from either an Rc or Arc, depending on what type it was originally passed as.

*/
pub trait FromRc<T> {
    fn from_rc(val: Rc<T>) -> Self;
    fn from_arc(val: Arc<T>) -> Self;
}

impl<T> FromRc<T> for Rc<T> where T: Sized + Clone {
    fn from_rc(val: Rc<T>) -> Self {
        val.clone()
    }

    fn from_arc(val: Arc<T>) -> Self {
        match ::std::sync::Arc::<T>::try_unwrap(val) {
            Ok(v) => Self::new(v),
            Err(r) => Self::new(r.cloned()),
        }
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

   // The FromRc trait is implemented for Rc<T> and Arc<T>. The Cloned trait is implemented for Rc<T>, Arc<T>, Box<T>. The ValueRc type alias is defined as an alias to the most generic of these three types, i.e., Arc<T>. 
   //A function named cloned() which returns a T from its input parameter (which must implement Clone). This function implements the Cloned trait for all of our 3 types above, so it can be used with any of them in this library's codebase without having to worry about what underlying type we're using at runtime. 
  //A function named to_value_rc() which returns a ValueRc containing a clone of its input parameter (which must also implement Clone). This method implements the FromRc trait for all 3 types above, so it can be used with any of them in this library's codebase without having to worry about what underlying type we're using at runtime.
 //The above code is compiled into a Rust module with the name value_rc, which can be used in any of this library's modules without having to worry about what underlying type we're using at runtime.
 

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


pub trait Cloned<T> {
    fn cloned(&self) -> T;
    fn to_value_rc(&self) -> ValueRc<T>;
}

impl<T: Clone> Cloned<T> for Rc<T> where T: Sized + Clone {
    fn cloned(&self) -> T {
        (*self.as_ref()).clone()
    }

    fn to_value_rc(&self) -> ValueRc<T> {
        ValueRc::from_rc(self.clone())
    }
}

impl<T: Clone> Cloned<T> for Arc<T> where T: Sized + Clone {
    fn cloned(&self) -> T {
        (*self.as_ref()).clone()
    }

    fn to_value_rc(&self) -> ValueRc<T> {
        ValueRc::from_arc(self.clone())
    }
}

impl<T: Clone> Cloned<T> for Box<T> where T: Sized + Clone {
    fn cloned(&self) -> T {
        self.as_ref().clone()
    }

    fn to_value_rc(&self) -> ValueRc<T> {
        ValueRc::new(self.cloned())
    }
}


pub type ValueRc<T> = Arc<T>;

//type alias with FromRc
#[derive(Debug, Eq, PartialEq)]
pub struct ValueRC<T: Clone + fmt::Display> {
    value_rc: Option<ValueRc<T>>,

    pub val: String,

    pub count: u64 //should be a counter for each one. and increment it as they are cloned. decrement it when dropped. 

   /* pub references_to_value_rc : HashSet<<&'a ValueRc<i32>> , Hash = ::std::hash::Hash>,*/

    
}

pub trait ValueRcClone<T> {
    fn value_rc(&self) -> ValueRc<T>;
}


impl<'a, T: 'a + Clone> AsRef<ValueRc<T>> for &'a Rc<T> where T: Sized + Clone {
    fn as_ref(&self) -> &ValueRc<T> {  //If you have a reference to the inner type of something that implements Deref, then you can use the dereference operator (*) on that reference to get a reference to the inner type.  In this case, we want a mutable reference (something like rc_as_ref). This is because we want to modify our original Rc object's count directly. We could pass it as an argument in the add() method and then increment the shared counter by calling add(). But doing so would create two copies of that integer and one copy will be lost if we call drop() on either one. Instead, we'll take a mutable refrence with self.value_rc().as_mut(), change it and assign it back using *= 1 instead of adding 1 (to avoid creating an extra pointer). The as_ref() method has nothing new here - its purpose is just to cast from some read-only Rust borrow into an immutable Rust borrow without requiring any allocation or deallocation at runtime! 

        unsafe{
            ::std::mem::transmute( self ) }//The transmute utility function allows us to convert between types even though they are lifetimes apart or never implement common traits such as Copy or Clone! Here's what happens behind scenes when using transmute(): rust first checks whether both types have matching sizes with mem::sizeof(src) == mem::sizeof(dst), which in this case they do! Then rust also ensures both types are plain old data aka PODs (no destructors/drop methods implemented). After ensuring these invariants hold true, rust finally takes src by value casts it into dst while replacing all lifetimes by static lifetime automatically!! By implementing From and Into traits for our custom smart pointers, their values can be transparently converted via transmuting them between each other!! Minimal overhead since no memory allocation occurs at runtime.. only size validation occurs once when instantiating each smart pointer instance in main(); see below for more details about Validation vs Construction costs.)

              fn value(&self) -> ValueRc<T>;       //Unsafe because there could be multiple clones that point to same underlying T causing UB?Or maybe not cause Cloning is efficient enough? Anyway preferred over referencing?
}
