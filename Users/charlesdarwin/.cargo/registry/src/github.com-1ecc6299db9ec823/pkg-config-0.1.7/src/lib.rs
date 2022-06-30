/*
The registry for packages.
*/


use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::collections::hash_map::Iter;
use std::collections::hash_map::Keys;
use std::collections::hash_map::Values;
use std::collections::hash_map::ValuesMut;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::hash_map::IterMut;
use std::collections::hash_map::IterMut::{self, IntoIter};
use std::collections::hash_map::Iter as IterMut;
use std::collections::hash_map::Keys as KeysMut;
use std::collections::hash_map::Values as ValuesMut;
use std::collections::hash_map::ValuesMut as ValuesMutMut;  
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::hash_map::Iter as IterMut;
use std::collections::hash_map::IterMut as IterMutMut;
use std::collections::hash_map::Keys as KeysMutMut;
use std::collections::hash_map::Values as ValuesMutMut;


pub struct PackageRegistry {
    packages: HashMap<String, Package>,
}


