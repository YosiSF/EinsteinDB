//Copyright 2019 EinsteinDB Licensed under Apache-2.0

//! This mod implemented a wrapped future pool that supports `on_tick()` which
//! is invoked no less than the specific interval.

mod builder;
mod metrics;

pub use self::builder::{Builder, Config};

use std::cell::Cell;
use std::sync::Arc;
use std::time::Duration;

use futures::{lazy, Future};
use prometheus::{IntCounter, IntGauge};
use tokio_threadpool::{SpawnHandle, ThreadPool};

use crate::time::Instant;

const TICK_INTERVAL: Duration = Duration::from_secs(1);

thread_local! {
    static THREAD_LAST_TICK_TIME: Cell<Instant> = Cell::new(Instant::now_coarse());
}

struct Env {
    on_tick: Option<Box<dyn Fn() + Send + Sync>>,
    metrics_running_task_count: IntGauge,
    metrics_handled_task_count: IntCounter,
}

#[derive(Clone)]
pub struct FuturePool {
    pool: Arc<ThreadPool>,
    env: Arc<Env>,
    max_tasks: usize,
}

impl std::fmt::Debug for FuturePool {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(fmt, "FuturePool")
    }
}

impl crate::AssertSend for FuturePool {}
impl crate::AssertSync for FuturePool {}
