/* write a design continuum that abide
 * by the following rules:
 */
#[macro_use]
extern crate log;
extern crate chrono;
extern crate env_logger;
extern crate rustc_serialize;
extern crate alga;
extern crate rand;
extern crate quickersort;
extern crate uuid;
extern crate libc;
extern crate bit_set;
extern crate generic_array;
extern crate smallbitvec;

use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;

use std::collections::HashMap;

use std::hash::Hash;

use std::cmp::Ordering;


use std::rc::Rc;
use std::cell::RefCell;
use std::cell::Cell;
use std::cell::Ref;


mod db;
mod design_continuum;
