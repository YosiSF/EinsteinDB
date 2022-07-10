// Copyright 2020 EinsteinDB Project Authors. 
// Licensed under Apache-2.0.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// ----------------------------------------------------------------------------



#[allow(dead_code)]

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::hash::Hash;
use std::iter::FromIterator;
use std::iter::Iterator;
use std::iter::once;
use std::iter::Peekable;
use std::iter::repeat;
use std::iter::Rev;
use std::iter::Zip;
use std::ops::Bound;
use std::ops::Deref;
use std::ops::DerefMut;
use std::ops::Index;
use std::ops::IndexMut;
use std::ops::Bounds;
use std::ops::From;
use std::ops::Full;
use std::ops::Inclusive;
use std::ops::To;
use std::ops::ToInclusive;


use std::ops::Range;


pub trait RangeProperties {
    fn range_properties(&self) -> dyn RangeProperties;

    fn range_properties_mut(&mut self) -> dyn RangePropertiesMut;
}


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
}



#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Properties<T> {
    pub min: Option<T>,
    pub max: Option<T>,
    pub sum: Option<T>,
    pub avg: Option<T>,
    pub count: Option<usize>,
}


pub struct PropertiesIterator<T> {
    iter: Peekable<Zip<Rev<VecDeque<T>>, Rev<VecDeque<T>>>>,

}

pub struct GreedoidsExt<T> {
    pub range_properties: Properties<T>,

    pub range_properties_iterator: PropertiesIterator<T>,

    pub range_greedoids: Vec<Greedoid<T>>,

    pub range_greedoids_iterator: PropertiesIterator<T>,

    iter: Peekable<Zip<Rev<VecDeque<T>>, Rev<VecDeque<T>>>>,

}

impl GreedoidsExt<i32> {
    pub fn new(range_properties: Properties<i32>, range_properties_iterator: PropertiesIterator<i32>, range_greedoids: Vec<Greedoid<i32>>, range_greedoids_iterator: PropertiesIterator<i32>) -> GreedoidsExt<i32> {
        GreedoidsExt {
            range_properties,
            range_properties_iterator,
            range_greedoids,
            range_greedoids_iterator,
            iter: Peekable::new(Zip::new(Rev::new(VecDeque::new()), Rev::new(VecDeque::new()))),
        }
    }
    pub fn new_from_iter(range: impl Iterator<Item=i32>) -> Self {
        panic!()
    }

    pub fn new_from_iter_with_properties(range: impl Iterator<Item=(i32, Properties<i32>)>) -> Self {
        panic!()
    }

    pub fn new_from_iter_with_properties_and_greedoids(range: impl Iterator<Item=(i32, Properties<i32>, Vec<Greedoid<i32>>)>) -> Self {
        panic!()
    }

    pub fn new_from_iter_with_properties_and_greedoids_and_iterator(range: impl Iterator<Item=(i32, Properties<i32>, Vec<Greedoid<i32>>, PropertiesIterator<i32>)>) -> Self {
        panic!()
    }

    pub fn new_from_iter_with_properties_and_greedoids_and_iter_and_iterator(range: impl Iterator<Item=(i32, Properties<i32>, Vec<Greedoid<i32>>, PropertiesIterator<i32>, PropertiesIterator<i32>)>) -> Self {
        panic!()
    }

    pub fn new_from_iter_with_properties_and_greedoids_and_iter_and_iter_and_iterator(range: impl Iterator<Item=(i32, Properties<i32>, Vec<Greedoid<i32>>, PropertiesIterator<i32>, PropertiesIterator<i32>, PropertiesIterator<i32>)>) -> Self {
        panic!()
    }
}




impl GreedoidsExt<i64> {
    pub fn new(range_properties: Properties<i64>, range_properties_iterator: PropertiesIterator<i64>, range_greedoids: Vec<Greedoid<i64>>, range_greedoids_iterator: PropertiesIterator<i64>) -> GreedoidsExt<i64> {
        GreedoidsExt {
            range_properties,
            range_properties_iterator,
            range_greedoids,
            range_greedoids_iterator,
            iter: Peekable::new(Zip::new(Rev::new(VecDeque::new()), Rev::new(VecDeque::new()))),
        }
    }

    pub fn new_from_iter(range: impl Iterator<Item=i64>) -> Self {
        panic!()
    }

    pub fn new_from_iter_with_properties(range: impl Iterator<Item=(i64, Properties<i64>)>) -> Self {
        panic!()
    }

    pub fn new_from_iter_with_properties_and_greedoids(range: impl Iterator<Item=(i64, Properties<i64>, Vec<Greedoid<i64>>)>) -> Self {
        panic!()
    }
}


impl GreedoidsExt<f32> {
    pub fn new(range_properties: Properties<f32>, range_properties_iterator: PropertiesIterator<f32>, range_greedoids: Vec<Greedoid<f32>>, range_greedoids_iterator: PropertiesIterator<f32>) -> GreedoidsExt<f32> {
        GreedoidsExt {
            range_properties,
            range_properties_iterator,
            range_greedoids,
            range_greedoids_iterator,
            iter: Peekable::new(Zip::new(Rev::new(VecDeque::new()), Rev::new(VecDeque::new()))),
        }
    }
    pub fn new_from_iter(range: impl Iterator<Item=f32>) -> Self {
        panic!()
    }

    pub fn new_from_iter_with_properties(range: impl Iterator<Item=(f32, Properties<f32>)>) -> Self {
        panic!()
    }

    switch_to_impl!(f32, i32, i64, f64);
}




impl GreedoidsExt<f64> {
    pub fn new(range_properties: Properties<f64>, range_properties_iterator: PropertiesIterator<f64>, range_greedoids: Vec<Greedoid<f64>>, range_greedoids_iterator: PropertiesIterator<f64>) -> GreedoidsExt<f64> {
        GreedoidsExt {
            range_properties,
            range_properties_iterator,
            range_greedoids,
            range_greedoids_iterator,
            iter: Peekable::new(Zip::new(Rev::new(VecDeque::new()), Rev::new(VecDeque::new()))),
        }
    }
    pub fn new_from_iter(range: impl Iterator<Item=f64>) -> Self {
        panic!()
    }
    pub fn new_from_iter_with_properties(range: impl Iterator<Item=(f64, Properties<f64>)>) -> Self {
        panic!()
    }
    switch_to_impl!(f64, i32, i64, f32);
}







    fn get_range_approximate_size_namespaced_with_redundant_causet_with_redundant_causet_with_redundant_causet(
        range: impl Iterator<Item=i32>,
        range_properties: Properties<i32>,
        range_properties_iterator: PropertiesIterator<i32>,
        range_greedoids: Vec<Greedoid<i32>>,
        range_greedoids_iterator: PropertiesIterator<i32>,
    ) -> (i32, Properties<i32>, Vec<Greedoid<i32>>) {
        panic!()
    }


    fn get_range_approximate_split_soliton_ids_namespaced_with_redundant_causet(
        range: impl Iterator<Item=i32>,
        range_properties: Properties<i32>,
        range_properties_iterator: PropertiesIterator<i32>,
        range_greedoids: Vec<Greedoid<i32>>,
        range_greedoids_iterator: PropertiesIterator<i32>,
    ) -> (i32, Properties<i32>, Vec<Greedoid<i32>>) {
        panic!()
    }



