extern crate named_type_metadata;
extern crate named_type_metadata_derive;
extern crate num;
extern crate ordered_float;
extern crate uuid;


use std::cmp::Ordering::{self, Greater, Less};
use std::collections::{HashMap, HashSet};
use std::ffi::CString;
use std::fs::{self, File};
use std::io::{self, Write};
use std::ops::{
    Deref,
    DerefMut,
};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::sync::Arc;
use std::time::{Duration, Instant};


use num::BigInt;
use ordered_float::OrderedFloat;
use uuid::ParseError as UuidParseError;
use uuid::Uuid;


use named_type_metadata::*;
use named_type_metadata_derive::*;


use std::collections::HashMap;
use std::collections::HashSet;  



pub fn main() {
    let mut map = HashMap::new();
    map.insert("a", 1);
    map.insert("b", 2);
    map.insert("c", 3);
    println!("{:?}", map);
}