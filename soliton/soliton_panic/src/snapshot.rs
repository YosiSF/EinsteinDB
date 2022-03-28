// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use std::ops::Deref;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub struct PanicLightlikePersistence;


#[derive(Clone, Debug)]
pub struct PanicMerkleTree;

impl PanicMerkleTree {
    pub fn new() -> Self {
        panic!()
    }
}


impl Deref for PanicMerkleTree {
    type Target = PanicLightlikePersistence;
    fn deref(&self) -> &Self::Target {
        panic!()
    }
}

