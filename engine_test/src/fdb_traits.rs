/// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.
/// 
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and limitations under the License.
///     
/// 


// #[macro_export]
// macro_rules! einsteindb_macro {
//     ($($tokens:tt)*) => {
//         $crate::einsteindb_macro_impl!($($tokens)*)
//     };
// }
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



/// here we can use the macro to define the trait
/// #[einsteindb_macro]
/// trait FdbTrait {
///    fn fdb_trait_method(&self) -> i32;
/// }
/// 
/// #[einsteindb_macro]
/// 
/// impl FdbTrait for i32 {
///    fn fdb_trait_method(&self) -> i32 {
///       *self
///   }
/// }
/// 
/// let x = 1;
/// let y = x.fdb_trait_method();
/// assert_eq!(y, 1);
/// 
/// #[einsteindb_macro]
/// trait FdbTrait2 {
///   fn fdb_trait_method(&self) -> i32;
/// }
/// 
/// #[einsteindb_macro]
/// impl FdbTrait2 for i32 {
///  fn fdb_trait_method(&self) -> i32 {
///   *self
/// }
/// 
/// let x = 1;
/// let y = x.fdb_trait_method();
/// assert_eq!(y, 1);



/// here we can use the macro to define the trait
/// 
#[einsteindb_macro]
trait FdbTrait {
   fn fdb_trait_method(&self) -> i32;
}


#[einsteindb_macro]
impl FdbTrait for i32 {
   fn fdb_trait_method(&self) -> i32 {
      *self
   }
}


let x = 1;
let y = x.fdb_trait_method();


assert_eq!(y, 1);


#[einsteindb_macro]
trait FdbTrait2 {
   fn fdb_trait_method(&self) -> i32;
}


#[einsteindb_macro]
impl FdbTrait2 for i32 {
   fn fdb_trait_method(&self) -> i32 {
      *self
   }
}


let x = 1;
let y = x.fdb_trait_method();


assert_eq!(y, 1);

