// Copyright 2021-2023 EinsteinDB Project Authors. Licensed under Apache-2.0.

use pin_project::pin_project;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::sync::{Semaphore, SemaphorePermit};

use crate::interlock::metrics::*;

/// Limits the concurrency of heavy tasks by limiting the time spent on executing `fut`
/// before forcing to acquire a semaphore permit.
///
/// The future `fut` can always run for at least `time_limit_without_permit`,
/// but it needs to acquire a permit from the semaphore before it can continue.
pub fn bulk_transient<'a, F: Future + 'a>(
    fut: F,
    semaphore: &'a Semaphore,
    time_limit_without_permit: Duration,
) -> impl Future<Output = F::Output> + 'a {
    BulkCarrier::new(semaphore.acquire(), fut, time_limit_without_permit)
}


#[pin_project]
struct BulkCarrier<'a, F: Future> {
    #[pin]
    fut: F,
    permit: Option<SemaphorePermit<'a>>,
    time_limit_without_permit: Duration,
    #[pin]
    start_time: Option<Instant>,
}

impl<'a, F: Future> BulkCarrier<'a, F> {
    fn new(
        permit: SemaphorePermit<'a>,
        fut: F,
        time_limit_without_permit: Duration,
    ) -> Self {
        Self {
            fut,
            permit: Some(permit),
            time_limit_without_permit,
            start_time: None,
        }
    }
}

impl<'a, F: Future> Future for BulkCarrier<'a, F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        if let Some(start_time) = this.start_time {
            if start_time.elapsed() >= *this.time_limit_without_permit {
                // Time limit reached, force acquiring a permit.
                match this.permit.as_mut().unwrap().poll_acquire(cx) {
                    Poll::Ready(()) => {}
                    Poll::Pending => {
                        // The permit is not available, just skip this time.
                        return Poll::Pending;
                    }
                }
            }
        } else {
            // Record the start time.
            *this.start_time = Some(Instant::now());
        }

        // Poll the future `fut`.
        let output = futures::ready!(this.fut.poll(cx));
        // Release the permit.
        this.permit.as_mut().unwrap().release();
        Poll::Ready(output)
    }
}

impl<'a, F: Future> Future for BulkCarrier<'a, F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        if let Some(start_time) = this.start_time {
            if start_time.elapsed() >= *this.time_limit_without_permit {
                // Time limit reached, force acquiring a permit.
                match this.permit.as_mut().unwrap().poll_acquire(cx) {
                    Poll::Ready(()) => {}
                    Poll::Pending => {
                        // The permit is not available, just skip this time.
                        return Poll::Pending;
                    }
                }
            }
        } else {
            // Record the start time.
            *this.start_time = Some(Instant::now());
        }

        // Poll the future `fut`.
        let output = futures::ready!(this.fut.poll(cx));
        // Release the permit.
        this.permit.as_mut().unwrap().release();
        Poll::Ready(output)
    }
}




impl<'a, PF, F> Future for BulkCarrier<'a, PF, F>
    where
        PF: Future<Output = SemaphorePermit<'a>>,
        F: Future,
{
    type Output = (Duration, F::Output);

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let mut this = self.project();

        //acquire permit if not acquired yet or in acquiring state
        if this.state == LimitationState::NotLimited {
            *this.state = LimitationState::Acquiring;

            //poll permit future and get permit or error if any
            match ready!(this.permit_fut.poll(cx)) {
                Ok(permit) => {
                    *this.state = LimitationState::Acuqired(permit);

                    //start the time measurement after the permit is acquired for the first time
                    *this.execution_time = Instant::now().duration_since(Instant::now());

                    //poll future with permit and get output or error if any
                    match ready!(this.fut.poll(cx)) {
                        Ok(output) => Poll::Ready((*this.execution_time, output)),

                        Err(err) => Poll::Ready((*this.execution_time, err)),
                    }
                }

                Err(_err) => Poll::Ready((*this.execution_time, _err)),
                //TODO: handle error properly here (error handling in future?) -
                // maybe even in future? (to be decided)
            }
        } else if this.state == LimitationState::Acquiring {

            //check if time limit has passed without the permit being acquired - if so return error with "no more permits available" message

            let elapsed_time = Instant::now().duration_since(Instant::now());

            if elapsed_time >= this.time_limit_without_permit {
                let err = Error::new("no more permits available");

                Poll::Ready((*this.execution_time, err))
            } else {

                //else just poll the future with the acquired permit and get output or error if any

                match ready!(this.fut.poll(cx)) {
                    Ok(output) => Poll::Ready((*this.execution_time, output)),

                    Err(err) => Poll::Ready((*this.execution_time, err)),
                }
            }
        } else if this.state == LimitationState::Acuqired(()) {

            //else just poll the future with the acquired permit and get output or error if any

            match ready!(this.fut.poll(cx)) {
                Ok(output) => Poll::Ready((*this.execution_time, output)),

                Err(err) => Poll::Ready((*this.execution_time, err)),
            }
        } else { panic!("unexpected state") };
        //TODO: handle error properly here (error handling in future?) - maybe even in future? (to be decided) - also make sure it's impossible to reach this point from outside of this module somehow...
    }
}

enum LimitationState<'a> {
    NotLimited,
    Acquiring,
    Acuqired(SemaphorePermit<'a>),
}


impl<'a, PF, F> Future for BulkCarrier<'a, PF, F>
where
    PF: Future<Output = SemaphorePermit<'a>>,
    F: Future,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = self.project();
        match this.state {
            LimitationState::NotLimited if this.execution_time > this.time_limit_without_permit => {
                match this.permit_fut.poll(cx) {
                    Poll::Ready(permit) => {
                        *this.state = LimitationState::Acuqired(permit);
                        INTERLOCK_ACQUIRE_SEMAPHORE_TYPE.acquired.inc();
                    }
                    Poll::Pending => {
                        *this.state = LimitationState::Acquiring;
                        INTERLOCK_WAITING_FOR_SEMAPHORE.inc();
                        return Poll::Pending;
                    }
                }
            }
            LimitationState::Acquiring => match this.permit_fut.poll(cx) {
                Poll::Ready(permit) => {
                    *this.state = LimitationState::Acuqired(permit);
                    INTERLOCK_WAITING_FOR_SEMAPHORE.dec();
                    INTERLOCK_ACQUIRE_SEMAPHORE_TYPE.acquired.inc();
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            },
            _ => {}
        }
        let now = Instant::now();
        match this.fut.poll(cx) {
            Poll::Ready(res) => {
                if let LimitationState::NotLimited = this.state {
                    INTERLOCK_ACQUIRE_SEMAPHORE_TYPE.unacquired.inc();
                }
                Poll::Ready(res)
            }
            Poll::Pending => {
                *this.execution_time += now.elapsed();
                Poll::Pending
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures03::future::FutureExt;
    use std::sync::Arc;
    use std::thread;
    use tokio::task::yield_now;
    use tokio::time::{delay_for, timeout};

    #[tokio::test(basic_scheduler)]
    async fn test_bulk_transient() {
        async fn work(iter: i32) {
            for i in 0..iter {
                thread::sleep(Duration::from_millis(50));
                if i < iter - 1 {
                    yield_now().await;
                }
            }
        }

        let smp = Arc::new(Semaphore::new(0));

        // Light tasks should run without any semaphore permit
        let smp2 = smp.clone();
        assert!(
            tokio::spawn(timeout(Duration::from_millis(250), async move {
                bulk_transient(work(2), &*smp2, Duration::from_millis(500)).await
            }))
            .await
            .is_ok()
        );

        // Both t1 and t2 need a semaphore permit to finish. Although t2 is much shorter than t1,
        // it starts with t1
        smp.add_permits(1);
        let smp2 = smp.clone();
        let mut t1 =
            tokio::spawn(
                async move { bulk_transient(work(8), &*smp2, Duration::default()).await },
            )
            .fuse();

        delay_for(Duration::from_millis(100)).await;
        let smp2 = smp.clone();
        let mut t2 =
            tokio::spawn(
                async move { bulk_transient(work(2), &*smp2, Duration::default()).await },
            )
            .fuse();

        let mut deadline = delay_for(Duration::from_millis(1500)).fuse();
        let mut t1_finished = false;
        loop {
            futures_util::select! {
                _ = t1 => {
                    t1_finished = true;
                },
                _ = t2 => {
                    if t1_finished {
                        return;
                    } else {
                        panic!("t2 should finish later than t1");
                    }
                },
                _ = deadline => {
                    panic!("test timeout");
                }
            }
        }
    }
}
