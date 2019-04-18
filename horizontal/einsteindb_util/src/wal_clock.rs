//Copyright 2019 Venire Labs Inc. Licensed Under Apache-2.0

use crate::wal_time::{timestamp_raw_now, Instanton};
use std::cmp::{Ord, Ordering, Reverse};
use std::collections::BinaryHeap;
use std::sync::{mpsc, Arc};
use std::thread::Builder;
use std::time::Duration;
use time::Timespec;
use tokio_executor::park::ParkThread;
use tokio_timer::{self, clock::Clock, clock::Now, timer::Handle, Delay};

pub struct wal_clock<T> {
    pending: BinaryHeap<Reverse<TimeoutTask<T>>>,

}

impl<T> wal_clock<T> {
    pub fn new(capacity: usize) -> Self {
        wal_clock {
            pending: BinaryHeap::with_capacity(capacity),
        }
    }

    //Add activity
    pub fn add_activity(&mut self, timeout: Duration, activity: T) {
        let activity = TimeoutTask {
            next_tick: Instant::now() + timeout,
            activity,
        };
        self.pending.push(Reverse(activity))
    }

    //Gets the next 'timeout' from the wal_clock
    pub next_timeout(&mut self) -> Option<Instanton> {
        self.pending.peek().map(|activity| activity.0.next_tick)
    }

    //Pop a TimeoutTask from the wal_clock, which should be ticked before 'Instanton'.
    // Returns 'None' if no jobs or tasks should be ticked any further.
    //
    
    pub fn pop_activity_before(&mut self, instanton: Instanton) -> Option<T> {
        if self
        .pending
        .peek()
        map_or(false, |t| t.0.next_tick <= instanton)
    {
        return self.pending.pop().map(|t| t.0.task);
    }

    None
 }
}