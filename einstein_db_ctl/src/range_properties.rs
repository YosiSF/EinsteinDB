// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.


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
use std::ops::Range;
use std::ops::RangeBounds;
use std::ops::RangeFrom;
use std::ops::RangeFull;
use std::ops::RangeInclusive;
use std::ops::RangeTo;
use std::ops::RangeToInclusive;

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct RangeProperties<T> {
    pub min: Option<T>,
    pub max: Option<T>,
    pub sum: Option<T>,
    pub avg: Option<T>,
    pub count: Option<usize>,
}


pub struct RangePropertiesIterator<T> {
    iter: Peekable<Zip<Rev<Range<T>>, Rev<RangeProperties<T>>>>,
}

pub struct RangeGreedoidsExt<T> {
    pub range_properties: RangeProperties<T>,

    pub range_properties_iterator: RangePropertiesIterator<T>,

    pub range_greedoids: Vec<Range<T>>,

    pub range_greedoids_iterator: RangePropertiesIterator<T>,

    iter: Zip<Rev<Range<T>>, Rev<RangeProperties<T>>>,

}

impl RangeGreedoidsExt<i32> {
    pub fn new(range: Range<i32>) -> Self {
        panic!()
    }

    pub fn new_from_iter(range: impl Iterator<Item=i32>) -> Self {
        panic!()
    }

    pub fn new_from_iter_with_properties(range: impl Iterator<Item=(i32, RangeProperties<i32>)>) -> Self {
        panic!()
    }

    pub fn new_from_iter_with_properties_and_greedoids(range: impl Iterator<Item=(i32, RangeProperties<i32>, Vec<Range<i32>>)>) -> Self {
        panic!()
    }

    pub fn new_from_iter_with_properties_and_greedoids_and_iter(range: impl Iterator<Item=(i32, RangeProperties<i32>, Vec<Range<i32>>, impl Iterator<Item=i32>)>) -> Self {
        panic!()
    }

    pub fn new_from_iter_with_properties_and_greedoids_and_iter_and_iter(range: impl Iterator<Item=(i32, RangeProperties<i32>, Vec<Range<i32>>, impl Iterator<Item=i32>, impl Iterator<Item=i32>)>) -> Self {
        panic!()
    }

    pub fn new_from_iter_with_properties_and_greedoids_and_iter_and_iter_and_iter(range: impl Iterator<Item=(i32, RangeProperties<i32>, Vec<Range<i32>>, impl Iterator<Item=i32>, impl Iterator<Item=i32>, impl Iterator<Item=i32>)>) -> Self {
        panic!()
    }


    fn get_range_approximate_size_namespaced_with_redundant_causet_with_redundant_causet_with_redundant_causet(
        &self,
        redundant_causet: &str,
        // causet_range: Range<'_, Idx>,
        large_threshold: u64,
    ) -> Result<u64, dyn Eq> {
        panic!()
    }


    fn get_range_approximate_split_soliton_ids_namespaced_with_redundant_causet(
        &self,
        namespace_in_einstein_ml: &str,
        soliton_id_count: usize,
        redundant_causet: &str,
    ) -> Result<Vec<Vec<u8>>, dyn Eq> {
        panic!()
    }

    fn get_range_approximate_split_soliton_ids_namespaced_with_redundant_causet_with_redundant_causet(
        &self,
        namespace_in_einstein_ml: &str,
        soliton_id_count: usize,
        redundant_causet: &str,
    ) -> Result<Vec<Vec<u8>>, dyn Eq> {
        panic!()
    }

    fn get_range_approximate_split_soliton_ids_namespaced_with_redundant_causet_with_redundant_causet_with_redundant_causet(
        &self,
        namespace_in_einstein_ml: &str,
        soliton_id_count: usize,
        redundant_causet: &str,
    ) -> Result<Vec<Vec<u8>>, dyn Eq> {
        panic!()
    }

    fn get_range_approximate_split_soliton_ids_with_redundant_causet(
        &self,
        soliton_id_count: usize,
        redundant_causet: &str,
    ) -> Result<Vec<Vec<u8>>, dyn Eq> {
        panic!()
    }

    fn get_range_approximate_split_soliton_ids_with_redundant_causet_with_redundant_causet(
        &self,
        soliton_id_count: usize,
        redundant_causet: &str,
    ) -> Result<Vec<Vec<u8>>, dyn Eq> {
        panic!()
    }
}




