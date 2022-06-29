/// protobuf-like traits for fdb_traits
/// Language: rust
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