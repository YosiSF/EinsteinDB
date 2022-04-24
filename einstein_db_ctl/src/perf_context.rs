// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

///! Performance context
///  `perf_context` is used to measure the execution time of a piece of code.
///  It is used to collect the execution time of a piece of code.
/// When a piece of code is executed, a `PerfContext` instance is created and passed as an argument to the code.
/// The code will use the `PerfContext` to record its execution time.
/// The `PerfContext` will be destroyed after the code is executed.

use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;
use std::time::{Duration, Instant};
use sqxl::time::{self, Time};
use allegro_poset::{self, Poset};
use crate::storage::{self, Storage};
use crate::storage::kv::{self, KvStorage};



