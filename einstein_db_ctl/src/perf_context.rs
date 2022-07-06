// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

/// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.
/// 
/// 
/// #[allow(dead_code)]
///  `perf_context` is used to measure the execution time of a piece of code.
///  It is used to collect the execution time of a piece of code.
/// When a piece of code is executed, a `PerfContext` instance is created and passed as an argument to the code.
/// The code will use the `PerfContext` to record its execution time.
/// The `PerfContext` will be destroyed after the code is executed.


use std::time::{Instant, Duration};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::thread;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;
use std::time::{Duration, Instant};
use sqxl::time::{self, Time};
use allegro_poset::{self, Poset};
use allegro_poset::{Poset, PosetError};

pub struct PosetError(String);

pub use sqxl::time::{Time, TimeError};
pub use sqxl::time::{TimeContext, TimeContextError};




/// Here we define the `TimeContext` trait.
/// The `TimeContext` trait is used to measure the execution time of a piece of code.
/// 




pub struct PerfContextBuilder {
    name: String,
    parent: Option<Arc<PerfContext>>,
    children: Vec<Arc<PerfContext>>,
    child_count: Arc<AtomicUsize>,
    child_count_mutex: Arc<Mutex<()>>,
    child_count_map: Arc<Mutex<HashMap<String, usize>>>,
    child_count_map_mutex: Arc<Mutex<()>>,
}


/// `PerfContext` is used to measure the execution time of a piece of code.

pub fn get_time() -> Time {
    let now = Instant::now();
    let time = Time::new(now.elapsed().as_secs(), now.elapsed().subsec_nanos());
    time
}


pub fn get_time_str() -> String {
    let now = Instant::now();
    let time = Time::new(now.elapsed().as_secs(), now.elapsed().subsec_nanos());
    time.to_string()
}


pub fn get_time_str_milli() -> String {
    let now = Instant::now();
    let time = Time::new(now.elapsed().as_secs(), now.elapsed().subsec_nanos());
    time.to_string_milli()
}


pub fn get_time_str_micro() -> String {

    let now = Instant::now();
    let time = Time::new(now.elapsed().as_secs(), now.elapsed().subsec_nanos());
    time.to_string_micro()
}


pub fn get_time_str_nano() -> String {
    let now = Instant::now();
    let time = Time::new(now.elapsed().as_secs(), now.elapsed().subsec_nanos());
    time.to_string_nano()
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PerfContext {
    pub name: String,
    pub start_time: Instant,
    pub end_time: Instant,
    pub duration: Duration,
    pub poset: Arc<Poset>,
}





/// `PerfContextManager` is used to manage the `PerfContext` instances.
/// It is used to record the execution time of a piece of code.
/// When a piece of code is executed, a `PerfContext` instance is created and passed as an argument to the code.
/// The code will use the `PerfContext` to record its execution time.
/// 
/// 



#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PerfContextManager {
    pub root: Arc<PerfContext>,
    pub current: Arc<PerfContext>,
    pub current_mutex: Arc<Mutex<()>>,
    pub current_map: Arc<Mutex<HashMap<String, Arc<PerfContext>>>>,
    pub current_map_mutex: Arc<Mutex<()>>,
    pub perf_contexts: HashMap<String, PerfContext>,
}



#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PerfContextManagerBuilder {
    pub root: Arc<PerfContext>,
    pub current: Arc<PerfContext>,
    pub perf_contexts: HashMap<String, PerfContext>,
}







