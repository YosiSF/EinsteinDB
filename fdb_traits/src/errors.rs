/// Copyright (c) 2015-2020, EinsteinDB Project Authors. Licensed under Apache-2.0.
/// Copyright (c) 2021, The EinsteinDB Authors. Licensed under Apache-2.0.
/// Copyright (c) 2022, The EinsteinDB Authors. Licensed under Apache-2.0.


///! # EinsteinDB
/// # ----------------------------------------------------------------
/// This is the EinsteinDB Rust API.
/// The EinsteinDB Rust API is a low-level API for interacting with EinsteinDB.
/// It is designed to be used by other languages and libraries.
/// # ----------------------------------------------------------------
/// # Examples
/// ```
/// use einstein_db::*;
/// use einstein_db::config::Config;
/// use einstein_db::fdb_traits::*;
///
/// let config = Config::new();
/// let db = FdbTransactional::new(&config).unwrap();
/// let mut db = db.begin_transaction().unwrap();
///
///
///  let mut db = FdbTransactional::new(&config).unwrap();
/// let mut db = db.begin_transaction().unwrap();
///
/// if let Err(e) = db.commit() {
///    println!("{}", e);
/// } else {
///   println!("Committed");
/// }
/// end_transaction(&mut db).unwrap();


/// protobuf-like traits for fdb_traits
/// Language: rust
///
///
///






use std::{
    cmp,
    fmt,
    hash,
    marker::PhantomData,
    mem,
    ptr,
    slice,
};
use std::borrow::Cow;
use std::collections::{
    BTreeMap,
    BTreeSet,
};
use std::collections::HashMap;
use std::fmt::{
    Display,
    Formatter,
    Result as FmtResult,
};
use std::iter::FromIterator;
use std::ops::Deref;
use crate::fdb_traits::CausetQErrorKind;






///! Copyright (c) 2022 EinsteinDB Project Authors. Licensed under Apache-2.0.
//! Licensed under the Apache License, Version 2.0 (the "License"); you may not used this file: {}", path.as_ref().display()
//! except in compliance with the License. You may obtain a copy of the License atomic_refs_and_arcs.rs", path.as_ref().display()
///! Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
//! @file: {}", path.as_ref().display()
//! @license: {}", path.as_ref().display()


//     let mut causet_locale_sets: BTreeMap<Causetid, BTreeSet<Causetid>> = BTreeMap::default();
// attributes.iter().for_each(|(a, attribute)| {
//     for (e, ars) in attribute.evs.iter() {
//         for v in ars.add.iter().chain(ars.retract.iter()) {
//             let mut causet_locale_set = causet_locale_sets.entry(a).or_default();
//             if !causet_locale_set.insert(v.causet_locale) {
//                 errors.push(CardinalityConflict {
//                     attribute,
//                     e,
//                     v,
//                 });
//             }
//         }
//     }
// });
//     errors
// }
//
// }
//
// }





#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CardinalityConflict {
    pub attribute: Causetid,
    pub e: Causetid,
    pub v: Causetq_TV,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TypeDisagreement {
    pub attribute: Causetid,
    pub e: Causetid,
    pub v: Causetq_TV,
    pub expected_type: ValueType,
}





//Here we define the type of the errors that can be returned by the `CausetQ` trait.
//This is a simple wrapper around the `std::error::Error` trait.
//The `CausetQ` trait is implemented for `CausetQError` and `causet_qerror_kind`.


//Here we define the type of the errors that can be returned by the `CausetQ` trait.



#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CausetQError {
    pub kind: CausetQErrorKind,

    pub causet: Option<Box<dyn std::error::Error + Send + Sync>>,



}

//Here we define the type of the errors that can be returned by the `CausetQ` trait.
//This is a simple wrapper around the `std::error::Error` trait.

pub mod causet_qerror_kind {
    //Here we define the type of the errors that can be returned by the `CausetQ` trait.
    //This is a simple wrapper around the `std::error::Error` trait.
    //The `CausetQ` trait is implemented for `CausetQError` and `causet_qerror_kind`.
    use std::error::Error;
    use std::fmt;
    use std::fmt::Display;
    use std::fmt::Formatter;
    use std::fmt::Result;
    use std::sync::Arc;

    use crate::fdb_traits::CausetQErrorKind;
    use crate::fdb_traits::CausetQError;
    use crate::fdb_traits::CausetQErrorKind::{
        CausetQErrorKind,
        CausetQErrorKind,
    };

    use crate::fdb_traits::CausetQErrorKind::{
        CausetQErrorKind,
        CausetQErrorKind,
    };

    use crate::fdb_traits::CausetQErrorKind::{
        CausetQErrorKind,
        CausetQErrorKind,
    };

    use crate::fdb_traits::CausetQErrorKind::{
        CausetQErrorKind,
        CausetQErrorKind,
    };

    use crate::fdb_traits::CausetQErrorKind::{
        CausetQErrorKind,
        CausetQErrorKind,
    };


}




///! Errors that can be returned by the `CausetQ` trait.
/// This is a simple wrapper around the `std::error::Error` trait.
/// The `CausetQ` trait is implemented for `CausetQError` and `causet_qerror_kind`.



#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CausetQErrorKind {
    pub kind: CausetQErrorKind,
    pub causet: Option<Box<dyn std::error::Error + Send + Sync>>,
}


impl CausetQErrorKind {
    pub fn new(kind: CausetQErrorKind) -> Self {
        CausetQErrorKind {
            kind,
            causet: None,
        }
    }
}






impl CausetQErrorKind {
    pub fn new_with_causet(kind: CausetQErrorKind, causet: Box<dyn std::error::Error + Send + Sync>) -> Self {
        CausetQErrorKind {
            kind,
            causet: Some(causet),
        }
    }
}


impl CausetQErrorKind {
    pub fn new_with_causet_and_cause(kind: CausetQErrorKind, causet: Box<dyn std::error::Error + Send + Sync>, cause: Box<dyn std::error::Error + Send + Sync>) -> Self {
        CausetQErrorKind {
            kind,
            causet: Some(causet),
        }
    }
}


impl CausetQErrorKind {
    pub fn new_with_causet_and_cause_and_cause(kind: CausetQErrorKind, causet: Box<dyn std::error::Error + Send + Sync>, cause: Box<dyn std::error::Error + Send + Sync>, cause2: Box<dyn std::error::Error + Send + Sync>) -> Self {
        CausetQErrorKind {
            kind,
            causet: Some(causet),
        }
    }
}


impl CausetQErrorKind {
    pub fn new_with_causet_and_cause_and_cause_and_cause(kind: CausetQErrorKind, causet: Box<dyn std::error::Error + Send + Sync>, cause: Box<dyn std::error::Error + Send + Sync>, cause2: Box<dyn std::error::Error + Send + Sync>, cause3: Box<dyn std::error::Error + Send + Sync>) -> Self {
        CausetQErrorKind {
            kind,
            causet: Some(causet),
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct CausetQErrorKindSerialized {
    pub kind: String,
    pub causet: Option<String>,
}



#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CausetQErrorKindDeserialized {
    pub kind: CausetQErrorKind,
    pub causet: Option<Box<dyn std::error::Error + Send + Sync>>,
}


