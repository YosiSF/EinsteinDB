use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use std::{cmp, u64};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;
use std::thread::JoinHandle;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::AtomicU8;



//suffix_tree    is a structure that holds a suffix tree.

#[derive(Debug, Clone, Default, Eq, PartialEq)]

struct SuffixTree<T> {
    pub root: Node<T>,
    pub alphabet: Vec<T>,
    pub alphabet_size: usize,
    pub alphabet_map: HashMap<T, usize>,
    pub alphabet_map_rev: HashMap<usize, T>,
    pub alphabet_map_rev_size: usize,
    pub alphabet_map_rev_size_max: usize,
    pub alphabet_map_rev_size_min: usize,
    pub alphabet_map_rev_size_avg: usize,
    pub alphabet_map_rev_size_median: usize,
    pub alphabet_map_rev_size_mode: usize,
    pub alphabet_map_rev_size_variance: usize,

    pub alphabet_map_rev_size_min_max_diff: usize,
    pub alphabet_map_rev_size_min_max_diff_abs: usize,
    pub alphabet_map_rev_size_min_max_diff_percent: usize,
    pub alphabet_map_rev_size_min_max_diff_percent_abs: usize,
    pub alphabet_map_rev_size_min_max_diff_percent_abs_percent: usize,
}




