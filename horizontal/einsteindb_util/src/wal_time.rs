//Copyright 2019 Venire Labs Inc All Rights Reserved.

use std::cmp::Ordering;
use std::ops::{Add, AddAssign, Sub, SubAssign};
use std::sync::mpsc::{self, Sender};
use std::thread::{self, Builder, JoinHandle};
use std::time::{SystemTime, UNIX_EPOCH};

use time::{Duration as TimeDuration, Timespec};

/// Converts Duration to milliseconds.
#[inline]
pub fn duration_to_ms(d: Duration) -> u64 {
    let nanos = u64::from(d.subsec_nanos());
    // Most of case, we can't have so large Duration, so here just panic if overflow now.
    d.as_secs() * 1_000 + (nanos / 1_000_000)
}

/// Converts Duration to seconds.
#[inline]
pub fn duration_to_sec(d: Duration) -> f64 {
    let nanos = f64::from(d.subsec_nanos());
    // Most of case, we can't have so large Duration, so here just panic if overflow now.
    d.as_secs() as f64 + (nanos / 1_000_000_000.0)
}

/// Converts Duration to nanoseconds.
#[inline]
pub fn duration_to_nanos(d: Duration) -> u64 {
    let nanos = u64::from(d.subsec_nanos());
    // Most of case, we can't have so large Duration, so here just panic if overflow now.
    d.as_secs() * 1_000_000_000 + nanos
}