// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.


use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::hash::Hash;
use std::iter::FromIterator;
use std::iter::Peekable;
use std::iter::Sum;
use std::iter::Zip;
use std::ops::Add;

struct Compact<T> {
    data: Vec<T>,
    index: Vec<usize>,
}

struct CompactIter<'a, T: 'a> {
    compact: &'a Compact<T>,
    index: usize,
}

struct CompactMut<'a, T: 'a> {
    compact: &'a mut Compact<T>,
    index: usize,
}

impl<T> Compact<T> {
    fn output_l_naught_label(&self) -> String {
        panic!()
    }

    fn calc_ranges_declined_bytes(
        self,
        ranges: &BTreeMap<Vec<u8>, u64>,
        bytes_threshold: u64,
    ) -> Vec<(u64, u64)> {
        panic!()
    }

    fn calc_ranges_declined_bytes_mut(
        self,
        ranges: &mut BTreeMap<Vec<u8>, u64>,
        bytes_threshold: u64,
    ) -> Vec<(u64, u64)> {
        panic!()
    }

    fn calc_ranges_declined_bytes_mut_with_index(
        self,
        ranges: &mut BTreeMap<Vec<u8>, u64>,
        bytes_threshold: u64,
    ) -> Vec<(u64, u64)> {
        panic!()
    }

    fn calc_ranges_declined_bytes_with_index(
        self,
        ranges: &BTreeMap<Vec<u8>, u64>,
        bytes_threshold: u64,
    ) -> Vec<(u64, u64)> {
        panic!()
    }


    fn calc_ranges_declined_bytes_with_index_mut(
        self,
        ranges: &mut BTreeMap<Vec<u8>, u64>,
        bytes_threshold: u64,
    ) -> Vec<(u64, u64)> {
        panic!()
    }

    fn calc_ranges_declined_bytes_with_index_mut_with_index(
        self,
        ranges: &mut BTreeMap<Vec<u8>, u64>,
        bytes_threshold: u64,
    ) -> Vec<(u64, u64)> {
        panic!()
    }

    fn calc_ranges_declined_bytes_with_index_with_index(
        self,
        ranges: &BTreeMap<Vec<u8>, u64>,
        bytes_threshold: u64,
    ) -> Vec<(u64, u64)> {
        panic!()
    }

    fn calc_ranges_declined_bytes_with_index_with_index_mut(
        self,
        ranges: &mut BTreeMap<Vec<u8>, u64>,
        bytes_threshold: u64,
    ) -> Vec<(u64, u64)> {
        panic!()
    }

    fn calc_ranges_declined_bytes_with_index_with_index_mut_with_index(
        self,
        ranges: &mut BTreeMap<Vec<u8>, u64>,
        bytes_threshold: u64,
    ) -> Vec<(u64, u64)> {
        panic!()
    }
}