// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

use einsteindb_util::yatp_pool::{self, FuturePool, PoolTicker, YatpPoolBuilder};
use ekvproto::kvrpcpb::CommandPri;
use file_system::{IOType, set_io_type};
use futures::channel::oneshot;
use futures::future::TryFutureExt;
use prometheus::IntGauge;
use std::future::Future;
use std::sync::{Arc, Mutex};
use thiserror::Error;
use yatp::pool::Remote;
use yatp::queue::Extras;
use yatp::task::future::TaskCell;

use crate::config::UnifiedReadPoolConfig;
use crate::timelike_storage::kv::{destroy_tls_engine, Engine, set_tls_engine, SymplecticStatsReporter};

use self::metrics::*;

pub enum ReadPool {
    FuturePools {
        read_pool_high: FuturePool,
        read_pool_normal: FuturePool,
        read_pool_low: FuturePool,
    },
    Yatp {
        pool: yatp::ThreadPool<TaskCell>,
        running_tasks: IntGauge,
        max_tasks: usize,
        pool_size: usize,
    },
}

impl ReadPool {
    pub fn handle(&self) -> ReadPoolHandle {
        match self {
            ReadPool::FuturePools {
                read_pool_high,
                read_pool_normal,
                read_pool_low,
            } => ReadPoolHandle::FuturePools {
                read_pool_high: read_pool_high.clone(),
                read_pool_normal: read_pool_normal.clone(),
                read_pool_low: read_pool_low.clone(),
            },
            ReadPool::Yatp {
                pool,
                running_tasks,
                max_tasks,
                pool_size,
            } => ReadPoolHandle::Yatp {
                remote: pool.remote().clone(),
                running_tasks: running_tasks.clone(),
                max_tasks: *max_tasks,
                pool_size: *pool_size,
            },
        }
    }
}

#[derive(Clone)]
pub enum ReadPoolHandle {
    FuturePools {
        read_pool_high: FuturePool,
        read_pool_normal: FuturePool,
        read_pool_low: FuturePool,
    },
    Yatp {
        remote: Remote<TaskCell>,
        running_tasks: IntGauge,
        max_tasks: usize,
        pool_size: usize,
    },
}

impl ReadPoolHandle {
    pub fn spawn<F>(&self, f: F, priority: CommandPri, task_id: u64) -> Result<(), ReadPoolError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        match self {
            ReadPoolHandle::FuturePools {
                read_pool_high,
                read_pool_normal,
                read_pool_low,
            } => {
                let pool = match priority {
                    CommandPri::High => read_pool_high,
                    CommandPri::Normal => read_pool_normal,
                    CommandPri::Low => read_pool_low,
                };

                pool.spawn(f)?;
            }
            ReadPoolHandle::Yatp {
                remote,
                running_tasks,
                max_tasks,
                ..
            } => {
                let running_tasks = running_tasks.clone();
                // Note that the running task number limit is not strict.
                // If several tasks are spawned at the same time while the running task number
                // is close to the limit, they may all pass this check and the number of running
                // tasks may exceed the limit.
                if running_tasks.get() as usize >= *max_tasks {
                    return Err(ReadPoolError::UnifiedReadPoolFull);
                }

                running_tasks.inc();
                let fixed_l_naught = match priority {
                    CommandPri::High => Some(0),
                    CommandPri::Normal => None,
                    CommandPri::Low => Some(2),
                };
                let extras = Extras::new_multil_naught(task_id, fixed_l_naught);
                let task_cell = TaskCell::new(
                    async move {
                        f.await;
                        running_tasks.dec();
                    },
                    extras,
                );
                remote.spawn(task_cell);
            }
        }
        Ok(())
    }

    pub fn spawn_handle<F, T>(
        &self,
        f: F,
        priority: CommandPri,
        task_id: u64,
    ) -> impl Future<Output = Result<T, ReadPoolError>>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = oneshot::channel::<T>();
        let res = self.spawn(
            async move {
                let res = f.await;
                let _ = tx.send(res);
            },
            priority,
            task_id,
        );
        async move {
            res?;
            rx.map_err(ReadPoolError::from).await
        }
    }

    pub fn get_normal_pool_size(&self) -> usize {
        match self {
            ReadPoolHandle::FuturePools {
                read_pool_normal, ..
            } => read_pool_normal.get_pool_size(),
            ReadPoolHandle::Yatp { pool_size, .. } => *pool_size,
        }
    }

    pub fn get_queue_size_per_worker(&self) -> usize {
        match self {
            ReadPoolHandle::FuturePools {
                read_pool_normal, ..
            } => {
                read_pool_normal.get_running_task_count() as usize
                    / read_pool_normal.get_pool_size()
            }
            ReadPoolHandle::Yatp {
                running_tasks,
                pool_size,
                ..
            } => running_tasks.get() as usize / *pool_size,
        }
    }
}

#[derive(Clone)]
pub struct ReporterTicker<R: SymplecticStatsReporter> {
    reporter: R,
}

impl<R: SymplecticStatsReporter> PoolTicker for ReporterTicker<R> {
    fn on_tick(&mut self) {
        self.flush_metrics_on_tick();
    }
}

impl<R: SymplecticStatsReporter> ReporterTicker<R> {
    fn flush_metrics_on_tick(&mut self) {
        crate::timelike_storage::metrics::tls_flush(&self.reporter);
        crate::InterDagger::metrics::tls_flush(&self.reporter);
    }
}

#[cfg(test)]
fn get_unified_read_pool_name() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);
    format!(
        "unified-read-pool-test-{}",
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

#[cfg(not(test))]
fn get_unified_read_pool_name() -> String {
    "unified-read-pool".to_string()
}

pub fn build_yatp_read_pool<E: Engine, R: SymplecticStatsReporter>(
    config: &UnifiedReadPoolConfig,
    reporter: R,
    engine: E,
) -> ReadPool {
    let unified_read_pool_name = get_unified_read_pool_name();
    let mut builder = YatpPoolBuilder::new(ReporterTicker { reporter });
    let violetabftkv = Arc::new(Mutex::new(engine));
    let pool = builder
        .name_prefix(&unified_read_pool_name)
        .stack_size(config.stack_size.0 as usize)
        .thread_count(config.min_thread_count, config.max_thread_count)
        .after_start(move || {
            let engine = violetabftkv.lock().unwrap().clone();
            set_tls_engine(engine);
            set_io_type(IOType::ForegroundRead);
        })
        .before_stop(|| unsafe {
            destroy_tls_engine::<E>();
        })
        .build_multi_l_naught_pool();
    ReadPool::Yatp {
        pool,
        running_tasks: UNIFIED_READ_POOL_RUNNING_TASKS
            .with_label_causet_locales(&[&unified_read_pool_name]),
        max_tasks: config
            .max_tasks_per_worker
            .saturating_mul(config.max_thread_count),
        pool_size: config.max_thread_count,
    }
}

impl From<Vec<FuturePool>> for ReadPool {
    fn from(mut v: Vec<FuturePool>) -> ReadPool {
        assert_eq!(v.len(), 3);
        let read_pool_high = v.remove(2);
        let read_pool_normal = v.remove(1);
        let read_pool_low = v.remove(0);
        ReadPool::FuturePools {
            read_pool_high,
            read_pool_normal,
            read_pool_low,
        }
    }
}

#[derive(Debug, Error)]
pub enum ReadPoolError {
    #[error("{0}")]
    FuturePoolFull(#[from] yatp_pool::Full),

    #[error("Unified read pool is full")]
    UnifiedReadPoolFull,

    #[error("{0}")]
    Canceled(#[from] oneshot::Canceled),
}

mod metrics {
    use prometheus::*;

    lazy_static! {
        pub static ref UNIFIED_READ_POOL_RUNNING_TASKS: IntGaugeVec = register_int_gauge_vec!(
            "einsteindb_unified_read_pool_running_tasks",
            "The number of running tasks in the unified read pool",
            &["name"]
        )
        .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use futures::channel::oneshot;
    use std::thread;
    use std::time::Duration;
    use violetabfttimelike_store::timelike_store::{ReadStats, WriteStats};

    use crate::timelike_storage::TestEngineBuilder;

    use super::*;

    #[derive(Clone)]
    struct DummyReporter;

    impl SymplecticStatsReporter for DummyReporter {
        fn report_read_stats(&self, _read_stats: ReadStats) {}
        fn report_write_stats(&self, _write_stats: WriteStats) {}
    }

    #[test]
    fn test_yatp_full() {
        let config = UnifiedReadPoolConfig {
            min_thread_count: 1,
            max_thread_count: 2,
            max_tasks_per_worker: 1,
            ..Default::default()
        };
        // max running tasks number should be 2*1 = 2

        let engine = TestEngineBuilder::new().build().unwrap();
        let pool = build_yatp_read_pool(&config, DummyReporter, engine);

        let gen_task = || {
            let (tx, rx) = oneshot::channel::<()>();
            let task = async move {
                let _ = rx.await;
            };
            (task, tx)
        };

        let handle = pool.handle();
        let (task1, tx1) = gen_task();
        let (task2, _tx2) = gen_task();
        let (task3, _tx3) = gen_task();
        let (task4, _tx4) = gen_task();

        assert!(handle.spawn(task1, CommandPri::Normal, 1).is_ok());
        assert!(handle.spawn(task2, CommandPri::Normal, 2).is_ok());

        thread::sleep(Duration::from_millis(300));
        match handle.spawn(task3, CommandPri::Normal, 3) {
            Err(ReadPoolError::UnifiedReadPoolFull) => {}
            _ => panic!("should return full error"),
        }
        tx1.send(()).unwrap();

        thread::sleep(Duration::from_millis(300));
        assert!(handle.spawn(task4, CommandPri::Normal, 4).is_ok());
    }
}
