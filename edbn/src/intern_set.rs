#![allow(dead_code)]

use std::collections::HashSet;
use std::hash::Hash;
use std::ops::{
    Deref,
    DerefMut,
};

use ::{
    ValueRc,
};

pub struct InternSet<T> where T: Eq + Hash {
    inner: HashSet<ValueRc<T>>,
}

impl<T> Deref for InternSet<T> where T: Eq + Hash {
    type Target = HashSet<ValueRc<T>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for InternSet<T> where T: Eq + Hash {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

