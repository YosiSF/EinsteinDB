//! Query builder for `User`.
//!
//!    let name: String = "name".to_string();
//!   let age: i32 = 0;
//!  let email: String = "email".to_string();
//! let password: String = "password".to_string();
//!
//!   let user = User::create()
//!       .set(name)
//!       .set(age)
//!       .set(email)
//!       .set(password)
//!       .query_one(conn)?;
//!
//!  let user = User::find_by_name(name, conn)?;
//! let user = User::find_by_name_and_age(name, age, conn)?;
//!
//! let user = User::update()
//!
//!    .set(name)
//!   .set(age)
//!  .set(email)
//! .set(password)
//! .where_name(name)
//! .query_one(conn)?;
//!
//! let user = User::delete()
//!    .where_name(name)
//! .execute(conn)?;
//!
//! let user = User::delete()
//!   .where_name(name)
//! .execute(conn)?;
//!
//! let user = User::delete()
//!  .where_name(name)
//!
//!   .execute(conn)?;
//!
//!
//!
//!
//!
//!

use std::any::Any;
use std::boxed::Box;
use std::cell::RefCell;
use std::cmp::{Eq, PartialEq};
use std::cmp::{Ord, PartialOrd};
use std::collections::BTreeSet;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;
use std::fmt::Result as FmtResult;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::ops::Add;
use std::ops::AddAssign;
use std::ops::BitAnd;
use std::ops::BitAndAssign;
use std::ops::BitOr;
use std::ops::BitOrAssign;
use std::ops::BitXor;
use std::ops::BitXorAssign;
use std::ops::Deref;
use std::ops::DerefMut;
use std::ops::Div;
use std::ops::DivAssign;
use std::ops::Drop;
use std::ops::Mul;
use std::ops::MulAssign;
use std::ops::Neg;
use std::ops::Not;
use std::ops::Sub;
use std::ops::SubAssign;
use std::rc::Rc;
use std::rc::Weak;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::atomic::AtomicIsize;
use std::sync::atomic::AtomicUsize;
use std::sync::Mutex;

