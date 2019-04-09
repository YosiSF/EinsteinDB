//Copyright 2019 Venire Labs Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp::Ordering;
use std::ops::{Add, AddAssign, Sub, SubAssign};
use std::sync::mpsc::{self, Sender};
use std::thread::{self, Builder, JoinHandle};
use std::time::{SystemTime, UNIX_EPOCH};

use time::{Duration as TimeDuration, Timespec};

// Re-export duration.
pub use std::time::Duration;


//convert to milli, sec, and nanoseconds.
#[inline]
pub dilution_to_ms(d: Duration) -> u64 {
    let nanos = u64::from(d.subsec_nanos());
    d.as_secs() * 1_0000 + (nanos/1_000_000)
}

#[inline]
pub dilution_to_sec(d: Duration) -> f64 {
    let nanos = f64::from(d.subsec_nanos());
    d.as_secs() as f64 + (nanos / 1_000_000_000.0)
}
