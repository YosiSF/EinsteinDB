// Copyright 2022 EinsteinDB Project Authors. Licensed under Apache-2.0.
// -----------------------------------------------------------------------------
//! # EinsteinDB
//! # Copyright (C) 2020 EinsteinDB Project Authors. All rights reserved.
//! # License: Apache-2.0 License Terms for the Project, see the LICENSE file.

use std::fmt;
use std::hash::Hash;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::rc::Rc;
use std::cell::RefCell;
use std::cell::Ref;
use std::ops::{
    Add,
    AddAssign,
    Sub,
    SubAssign,
    Mul,
    MulAssign,
    Div,
    DivAssign,
    Deref,
    DerefMut,
    Index,
    IndexMut,
};



// -----------------------------------------------------------------------------
//! # EinsteinDB
//! # ----------------------------------------------------------------
//!
//causetq
// -----------------------------------------------------------------------------

//#[macro_use]
//extern crate causetq;
//#[macro_use]
#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate serde_json;

#[macro_use]
extern crate log;

#[macro_use]
extern crate serde_json_utils;



mod einsteindb;


pub use einsteindb::*;


#[cfg(test)]
mod tests {
    use super::*;
    use crate::einsteindb::*;
    use crate::einsteindb::{Einsteindb, EinsteindbOptions};
    use crate::einsteindb::{EinsteindbIterator, EinsteindbIteratorOptions};
    use crate::einsteindb::{EinsteindbIteratorItem, EinsteindbIteratorItemOptions};
    use crate::einsteindb::{EinsteindbIteratorItemType, EinsteindbIteratorItemTypeOptions};
    use crate::einsteindb::{EinsteindbIteratorOptions, EinsteindbIteratorOptionsOptions};
    use crate::einsteindb::{EinsteindbIteratorType, EinsteindbIteratorTypeOptions};
    use crate::einsteindb::{EinsteindbIteratorTypeOptions, EinsteindbIteratorTypeOptionsOptions};
    use crate::einsteindb::{EinsteindbOptions, EinsteindbOptionsOptions};
    use crate::einsteindb::{EinsteindbOptionsType, EinsteindbOptionsTypeOptions};
    use crate::einsteindb::{EinsteindbOptionsTypeOptions, EinsteindbOptionsTypeOptionsOptions};
    use crate::einsteindb::{EinsteindbType, EinsteindbTypeOptions};
    use crate::einsteindb::{EinsteindbTypeOptions, EinsteindbTypeOptionsOptions};
    use crate::einsteindb::{EinsteindbTypeOptionsType, EinsteindbTypeOptionsTypeOptions};
    use crate::einsteindb::{EinsteindbTypeOptionsTypeOptions, EinsteindbTypeOptionsTypeOptionsOptions};
    use crate::einsteindb::{EinsteindbTypeType, EinsteindbTypeTypeOptions};
    use crate::einsteindb::{EinsteindbTypeTypeOptions, EinsteindbTypeTypeOptionsOptions};
}




/// Length is unspecified, applicable to `FieldType`'s `flen` and `decimal`.
pub const UNSPECIFIED_LENGTH: isize = -1;

/// MyBerolinaSQL type maximum length
pub const MAX_BLOB_WIDTH: i32 = 16_777_216; // FIXME: Should be isize
pub const MAX_DECIMAL_WIDTH: isize = 65;
pub const MAX_REAL_WIDTH: isize = 23;
pub const MAX_DOUBLE_WIDTH: isize = 52;
pub const MAX_INT_WIDTH: isize = 21;
pub const MAX_DATE_WIDTH: isize = 10;
pub const MAX_TIME_WIDTH: isize = 8;
pub const MAX_DATETIME_WIDTH: isize = 19;
pub const MAX_TIMESTAMP_WIDTH: isize = 19;
pub const MAX_YEAR_WIDTH: isize = 4;
pub const MAX_CHAR_WIDTH: isize = 255;
pub const MAX_VARCHAR_WIDTH: isize = 65535;
pub const MAX_TINYTEXT_WIDTH: isize = 255;


/// MyBerolinaSQL type minimum length
/// FIXME: Should be isize
///
/// # Example
/// ```
/// use einsteindb::*;
/// use einsteindb::{Einsteindb, EinsteindbOptions};
/// use einsteindb::{EinsteindbIterator, EinsteindbIteratorOptions};
/// use einsteindb::{EinsteindbIteratorItem, EinsteindbIteratorItemOptions};
///
/// let mut einsteindb = Einsteindb::new(EinsteindbOptions::new());
/// let mut einsteindb_iterator = EinsteindbIterator::new(EinsteindbIteratorOptions::new());
///
/// let mut einsteindb_iterator_item = EinsteindbIteratorItem::new(EinsteindbIteratorItemOptions::new());
/// let mut einsteindb_iterator_item_options = EinsteindbIteratorItemOptions::new();
///
/// einsteindb_iterator_item_options.set_type(EinsteindbIteratorItemType::INT);
/// einsteindb_iterator_item_options.set_length(Einsteindb::MIN_INT_WIDTH);
///
/// einsteindb_iterator_item.set_options(einsteindb_iterator_item_options);
/// let mut einsteindb_iterator_item_options = EinsteindbIteratorItemOptions::new();
///
///
/// einsteindb_iterator_item_options.set_type(EinsteindbIteratorItemType::INT);



pub const MIN_BLOB_WIDTH: i32 = 0;
pub const MIN_DECIMAL_WIDTH: isize = 1;
pub const MIN_REAL_WIDTH: isize = 7;
pub const MIN_DOUBLE_WIDTH: isize = 15;
pub const MIN_INT_WIDTH: isize = 10;
pub const MIN_DATE_WIDTH: isize = 10;
pub const MIN_TIME_WIDTH: isize = 8;
pub const MIN_DATETIME_WIDTH: isize = 19;
pub const MIN_TIMESTAMP_WIDTH: isize = 19;
pub const MIN_YEAR_WIDTH: isize = 4;
pub const MIN_CHAR_WIDTH: isize = 1;
pub const MIN_VARCHAR_WIDTH: isize = 1;
pub const MIN_TINYTEXT_WIDTH: isize = 1;








