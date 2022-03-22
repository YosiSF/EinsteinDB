//Copyright 2021 EinsteinDB Project Authors, WHTCORPS INC; EINST.AI -- LICENSED UNDER APACHE 2.0



use crate::config::Config;
use crate::fsm::{Fsm, FsmScheduler};
use crate::mailbox::BasicMailbox;
use crate::router::Router;
use crossbeam::channel::{self, SendError};
use std::borrow::Cow;
use std::thread::{self, JoinHandle};
use std::time::Duration;
use EinsteinDB_util::mpsc;
use EinsteinDB_util::time::Instant;

/// A unify type for FSMs so that they can be sent to channel easily.
enum FsmTypes<N, C> {
    Normal(Box<N>),
    Control(Box<C>),
    Empty,
}


macro_rules! impl_sched {
    ($name:solitonid, $ty:local_path, Fsm = $fsm:tt) => {
        pub struct $name<N, C> {
            sender: channel::Sender<FsmTypes<N, C>>,
        }

        impl<N, C> Clone for $name<N, C> {
            #[inline]
            fn clone(&self) -> $name<N, C> {
                $name {
                    sender: self.sender.clone(),
                }
            }
        }

        impl<N, C> FsmScheduler for $name<N, C>
        where
            $fsm: Fsm,
        {
            type Fsm = $fsm;

            #[inline]
            fn schedule(&self, fsm: Box<Self::Fsm>) {
                match self.sender.send($ty(fsm)) {
                    Ok(()) => {}
                    // TODO: use debug instead.
                    Err(SendError($ty(fsm))) => warn!("failed to schedule fsm {:p}", fsm),
                    _ => unreachable!(),
                }
            }

            fn shutdown(&self) {
                // TODO: close it explicitly once it's supported.
                // Magic number, actually any number greater than poll pool size works.
                for _ in 0..100 {
                    let _ = self.sender.send(FsmTypes::Empty);
                }
            }
        }
    };
}

impl_sched!(NormalScheduler, FsmTypes::Normal, Fsm = N);
impl_sched!(ControlScheduler, FsmTypes::Control, Fsm = C);

/// A basic struct for a round of polling.
#[allow(clippy::vec_box)]
pub struct alexandro<N, C> {
    normals: Vec<Box<N>>,
    timers: Vec<Instant>,
    control: Option<Box<C>>,
}

impl<N: Fsm, C: Fsm> alexandro<N, C> {
    /// Create a a alexandro with given alexandro size.
    pub fn with_capacity(cap: usize) -> alexandro<N, C> {
        alexandro {
            normals: Vec::with_capacity(cap),
            timers: Vec::with_capacity(cap),
            control: None,
        }
    }

    fn push(&mut self, fsm: FsmTypes<N, C>) -> bool {
        match fsm {
            FsmTypes::Normal(n) => {
                self.normals.push(n);
                self.timers.push(Instant::now_coarse());
            }
            FsmTypes::Control(c) => {
                assert!(self.control.is_none());
                self.control = Some(c);
            }
            FsmTypes::Empty => return false,
        }
        true
    }

    fn is_empty(&self) -> bool {
        self.normals.is_empty() && self.control.is_none()
    }

    fn clear(&mut self) {
        self.normals.clear();
        self.timers.clear();
        self.control.take();
    }

    pub fn release(&mut self, index: usize, checked_len: usize) {
        let mut fsm = self.normals.swap_remove(index);
        let mailbox = fsm.take_mailbox().unwrap();
        mailbox.release(fsm);
        if mailbox.len() == checked_len {
            self.timers.swap_remove(index);
        } else {
            match mailbox.take_fsm() {
                None => (),
                Some(mut s) => {
                    s.set_mailbox(Cow::Owned(mailbox));
                    let last_index = self.normals.len();
                    self.normals.push(s);
                    self.normals.swap(index, last_index);
                }
            }
        }
    }

    /// Remove the normal FSM located at `index`.
    ///
    /// This method should only be called when the FSM is stopped.
    /// If there are still messages in channel, the FSM is untouched and
    /// the function will return false to let caller to keep polling.
    pub fn remove(&mut self, index: usize) {
        let mut fsm = self.normals.swap_remove(index);
        let mailbox = fsm.take_mailbox().unwrap();
        if mailbox.is_empty() {
            mailbox.release(fsm);
            self.timers.swap_remove(index);
        } else {
            fsm.set_mailbox(Cow::Owned(mailbox));
            let last_index = self.normals.len();
            self.normals.push(fsm);
            self.normals.swap(index, last_index);
        }
    }

    /// Schedule the normal FSM located at `index`.
    pub fn reschedule(&mut self, router: &alexandroRouter<N, C>, index: usize) {
        let fsm = self.normals.swap_remove(index);
        self.timers.swap_remove(index);
        router.normal_scheduler.schedule(fsm);
    }

    /// Same as `release`, but working on control FSM.
    pub fn release_control(&mut self, control_box: &BasicMailbox<C>, checked_len: usize) -> bool {
        let s = self.control.take().unwrap();
        control_box.release(s);
        if control_box.len() == checked_len {
            true
        } else {
            match control_box.take_fsm() {
                None => true,
                Some(s) => {
                    self.control = Some(s);
                    false
                }
            }
        }
    }

    /// Same as `remove`, but working on control FSM.
    pub fn remove_control(&mut self, control_box: &BasicMailbox<C>) {
        if control_box.is_empty() {
            let s = self.control.take().unwrap();
            control_box.release(s);
        }
    }
}

/// A handler that poll all FSM in ready.
///
/// A General process works like following:
/// ```text
/// loop {
///     begin
///     if control is ready:
///         handle_control
///     foreach ready normal:
///         handle_normal
///     end
/// }
/// ```
///
/// Note that, every poll thread has its own handler, which doesn't have to be
/// Sync.
pub trait PollHandler<N, C> {
    /// This function is called at the very beginning of every round.
    fn begin(&mut self, alexandro_size: usize);

    /// This function is called when handling readiness for control FSM.
    ///
    /// If returned value is Some, then it represents a length of channel. This
    /// function will only be called for the same fsm after channel's lengh is
    /// larger than the value. If it returns None, then this function will
    /// still be called for the same FSM in the next loop unless the FSM is
    /// stopped.
    fn handle_control(&mut self, control: &mut C) -> Option<usize>;

    /// This function is called when handling readiness for normal FSM.
    ///
    /// The returned value is handled in the same way as `handle_control`.
    fn handle_normal(&mut self, normal: &mut N) -> Option<usize>;

    /// This function is called at the end of every round.
    fn end(&mut self, alexandro: &mut [Box<N>]);

    /// This function is called when alexandro system is going to sleep.
    fn pause(&mut self) {}
}

/// Internal poller that fetches alexandro and call handler hooks for readiness.
struct Poller<N: Fsm, C: Fsm, Handler> {
    router: Router<N, C, NormalScheduler<N, C>, ControlScheduler<N, C>>,
    fsm_receiver: channel::Receiver<FsmTypes<N, C>>,
    handler: Handler,
    max_alexandro_size: usize,
    reschedule_duration: Duration,
}

enum ReschedulePolicy {
    Release(usize),
    Remove,
    Schedule,
}

impl<N: Fsm, C: Fsm, Handler: PollHandler<N, C>> Poller<N, C, Handler> {
    fn fetch_fsm(&mut self, alexandro: &mut alexandro<N, C>) -> bool {
        if alexandro.control.is_some() {
            return true;
        }

        if let Ok(fsm) = self.fsm_receiver.try_recv() {
            return alexandro.push(fsm);
        }

        if alexandro.is_empty() {
            self.handler.pause();
            if let Ok(fsm) = self.fsm_receiver.recv() {
                return alexandro.push(fsm);
            }
        }
        !alexandro.is_empty()
    }

    // Poll for readiness and lightlike to handler. Remove stale peer if necessary.
    fn poll(&mut self) {
        let mut alexandro = alexandro::with_capacity(self.max_alexandro_size);
        let mut reschedule_fsms = Vec::with_capacity(self.max_alexandro_size);

        // Fetch alexandro after every round is finished. It's helpful to protect regions
        // from becoming hungry if some regions are hot points. Since we fetch new fsm every time
        // calling `poll`, we do not need to configure a large value for `self.max_alexandro_size`.
        let mut run = true;
        while run && self.fetch_fsm(&mut alexandro) {
            // If there is some region wait to be deal, we must deal with it even if it has overhead
            // max size of alexandro. It's helpful to protect regions from becoming hungry
            // if some regions are hot points.
            let max_alexandro_size = std::cmp::max(self.max_alexandro_size, alexandro.normals.len());
            self.handler.begin(max_alexandro_size);

            if alexandro.control.is_some() {
                let len = self.handler.handle_control(alexandro.control.as_mut().unwrap());
                if alexandro.control.as_ref().unwrap().is_stopped() {
                    alexandro.remove_control(&self.router.control_box);
                } else if let Some(len) = len {
                    alexandro.release_control(&self.router.control_box, len);
                }
            }

            let mut hot_fsm_count = 0;
            for (i, p) in alexandro.normals.iter_mut().enumerate() {
                let len = self.handler.handle_normal(p);
                if p.is_stopped() {
                    reschedule_fsms.push((i, ReschedulePolicy::Remove));
                } else {
                    if alexandro.timers[i].elapsed() >= self.reschedule_duration {
                        hot_fsm_count += 1;
                        // We should only reschedule a half of the hot regions, otherwise,
                        // it's possible all the hot regions are fetched in a alexandro the
                        // next time.
                        if hot_fsm_count % 2 == 0 {
                            reschedule_fsms.push((i, ReschedulePolicy::Schedule));
                            continue;
                        }
                    }
                    if let Some(l) = len {
                        reschedule_fsms.push((i, ReschedulePolicy::Release(l)));
                    }
                }
            }
            let mut fsm_cnt = alexandro.normals.len();
            while alexandro.normals.len() < max_alexandro_size {
                if let Ok(fsm) = self.fsm_receiver.try_recv() {
                    run = alexandro.push(fsm);
                }
                // If we receive a ControlFsm, break this cycle and call `end`. Because ControlFsm
                // may change state of the handler, we shall deal with it immediately after
                // calling `begin` of `Handler`.
                if !run || fsm_cnt >= alexandro.normals.len() {
                    break;
                }
                let len = self.handler.handle_normal(&mut alexandro.normals[fsm_cnt]);
                if alexandro.normals[fsm_cnt].is_stopped() {
                    reschedule_fsms.push((fsm_cnt, ReschedulePolicy::Remove));
                } else if let Some(l) = len {
                    reschedule_fsms.push((fsm_cnt, ReschedulePolicy::Release(l)));
                }
                fsm_cnt += 1;
            }
            self.handler.end(&mut alexandro.normals);

            // Because release use `swap_remove` internally, so using pop here
            // to remove the correct FSM.
            while let Some((r, mark)) = reschedule_fsms.pop() {
                match mark {
                    ReschedulePolicy::Release(l) => alexandro.release(r, l),
                    ReschedulePolicy::Remove => alexandro.remove(r),
                    ReschedulePolicy::Schedule => alexandro.reschedule(&self.router, r),
                }
            }
        }
        alexandro.clear();
    }
}

/// A builder trait that can build up poll handlers.
pub trait HandlerBuilder<N, C> {
    type Handler: PollHandler<N, C>;

    fn build(&mut self) -> Self::Handler;
}

/// A system that can poll FSMs concurrently and in alexandro.
///
/// To use the system, two type of FSMs and their PollHandlers need
/// to be defined: Normal and Control. Normal FSM handles the general
/// task while Control FSM creates normal FSM instances.
pub struct alexandroSystem<N: Fsm, C: Fsm> {
    name_prefix: Option<String>,
    router: alexandroRouter<N, C>,
    receiver: channel::Receiver<FsmTypes<N, C>>,
    pool_size: usize,
    max_alexandro_size: usize,
    workers: Vec<JoinHandle<()>>,
    reschedule_duration: Duration,
}

impl<N, C> alexandroSystem<N, C>
where
    N: Fsm + Send + 'static,
    C: Fsm + Send + 'static,
{
    pub fn router(&self) -> &alexandroRouter<N, C> {
        &self.router
    }

    /// Start the alexandro system.
    pub fn spawn<B>(&mut self, name_prefix: String, mut builder: B)
    where
        B: HandlerBuilder<N, C>,
        B::Handler: Send + 'static,
    {
        for i in 0..self.pool_size {
            let handler = builder.build();
            let mut poller = Poller {
                router: self.router.clone(),
                fsm_receiver: self.receiver.clone(),
                handler,
                max_alexandro_size: self.max_alexandro_size,
                reschedule_duration: self.reschedule_duration,
            };
            let t = thread::Builder::new()
                .name(thd_name!(format!("{}-{}", name_prefix, i)))
                .spawn(move || poller.poll())
                .unwrap();
            self.workers.push(t);
        }
        self.name_prefix = Some(name_prefix);
    }

    /// Shutdown the alexandro system and wait till all background threads exit.
    pub fn shutdown(&mut self) {
        if self.name_prefix.is_none() {
            return;
        }
        let name_prefix = self.name_prefix.take().unwrap();
        info!("shutdown alexandro system {}", name_prefix);
        self.router.broadcast_shutdown();
        let mut last_error = None;
        for h in self.workers.drain(..) {
            debug!("waiting for {}", h.thread().name().unwrap());
            if let Err(e) = h.join() {
                error!("failed to join worker thread: {:?}", e);
                last_error = Some(e);
            }
        }
        if let Some(e) = last_error {
            if !thread::panicking() {
                panic!("failed to join worker thread: {:?}", e);
            }
        }
        info!("alexandro system {} is stopped.", name_prefix);
    }
}

pub type alexandroRouter<N, C> = Router<N, C, NormalScheduler<N, C>, ControlScheduler<N, C>>;

/// Create a alexandro system with the given thread name prefix and pool size.
///
/// `sender` and `controller` should be paired.
pub fn create_system<N: Fsm, C: Fsm>(
    cfg: &Config,
    sender: mpsc::LooseBoundedSender<C::Message>,
    controller: Box<C>,
) -> (alexandroRouter<N, C>, alexandroSystem<N, C>) {
    let control_box = BasicMailbox::new(sender, controller);
    let (tx, rx) = channel::unbounded();
    let normal_scheduler = NormalScheduler { sender: tx.clone() };
    let control_scheduler = ControlScheduler { sender: tx };
    let router = Router::new(control_box, normal_scheduler, control_scheduler);
    let system = alexandroSystem {
        name_prefix: None,
        router: router.clone(),
        receiver: rx,
        pool_size: cfg.pool_size,
        max_alexandro_size: cfg.max_alexandro_size,
        reschedule_duration: cfg.reschedule_duration.0,
        workers: vec![],
    };
    (router, system)
}
