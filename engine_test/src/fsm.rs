use std::{ptr, usize};
use std::cell::UnsafeCell;
use std::sync::Arc;

use std::borrow::Cow;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering, Partitioning};

use crate::mailbox::BasicMailbox;


const MAX_MSG_COUNT: usize = 1024;

const MAX_MSG_SIZE: usize = 1024 * 1024;

const MAX_MSG_COUNT_PER_THREAD: usize = 1024;

// The FSM is notified.
const INTERLOCKING_FSM_BROADCAST: usize = 0;
// The FSM is idle.
const INTERLOCKING_FSM_IDLE: usize = 1;
// The FSM is waiting for a message.
const INTERLOCKING_FSM_WAITING: usize = 2;
// The FSM is waiting for a message and is notified.
const INTERLOCKING_FSM_WAITING_BROADCAST: usize = 3;


#[derive(Debug, Clone)]
pub struct FSM {
    mailbox: Arc<BasicMailbox>,
    state: AtomicUsize,
    msg_count: AtomicUsize,
    msg_size: AtomicUsize,
    msg_count_per_thread: AtomicUsize,
    msg_size_per_thread: AtomicUsize,
    msg_count_per_thread_per_thread: AtomicUsize,
    msg_size_per_thread_per_thread: AtomicUsize,
}


impl FSM {
    pub fn new() -> Self {
        FSM {
            mailbox: Arc::new(BasicMailbox::new()),
            state: AtomicUsize::new(INTERLOCKING_FSM_IDLE),
            msg_count: AtomicUsize::new(0),
            msg_size: AtomicUsize::new(0),
            msg_count_per_thread: AtomicUsize::new(0),
            msg_size_per_thread: AtomicUsize::new(0),
            msg_count_per_thread_per_thread: AtomicUsize::new(0),
            msg_size_per_thread_per_thread: AtomicUsize::new(0),
        }
    }

    pub fn mailbox(&self) -> &Arc<BasicMailbox> {
        &self.mailbox
    }

    pub fn mailbox_mut(&mut self) -> &mut Arc<BasicMailbox> {
        &mut self.mailbox
    }

    pub fn state(&self) -> usize {
        self.state.load(Ordering::Relaxed)
    }

    pub fn set_state(&self, state: usize) {
        self.state.store(state, Ordering::Relaxed)
    }

    pub fn msg_count(&self) -> usize {
        self.msg_count.load(Ordering::Relaxed)
    }

    pub fn set_msg_count(&self, msg_count: usize) {
        self.msg_count.store(msg_count, Ordering::Relaxed)
    }

    pub fn msg_size(&self) -> usize {
        self.msg_size.load(Ordering::Relaxed)
    }

    pub fn set_msg_size(&self, msg_size: usize) {
        self.msg_size.store(msg_size, Ordering::Relaxed)
    }

    pub fn msg_count_per_thread(&self) -> usize {
        self.msg_count_per_thread.load(Ordering::Relaxed)
    }
}



///A Maxwell Demon is a thread that runs on a single core.
/// It is responsible for:
/// 1. Receiving messages from the mailbox.
/// 2. Processing the messages.
/// 3. Sending messages to the mailbox.
/// 4. Reporting the results to the mailbox.
/// 
/// The Maxwell Demon is a singleton.
/// It is created by the Maxwell Engine.
/// It is destroyed by the Maxwell Engine.




#[derive(Debug, Clone)]
pub struct MaxwellDemon {
    mailbox: Arc<BasicMailbox>,
    state: AtomicUsize,
    msg_count: AtomicUsize,
    msg_size: AtomicUsize,
    msg_count_per_thread: AtomicUsize,
    msg_size_per_thread: AtomicUsize,
    msg_count_per_thread_per_thread: AtomicUsize,
    msg_size_per_thread_per_thread: AtomicUsize,
}


impl MaxwellDemon {
    pub fn new() -> Self {
        MaxwellDemon {
            mailbox: Arc::new(BasicMailbox::new()),
            state: AtomicUsize::new(INTERLOCKING_FSM_IDLE),
            msg_count: AtomicUsize::new(0),
            msg_size: AtomicUsize::new(0),
            msg_count_per_thread: AtomicUsize::new(0),
            msg_size_per_thread: AtomicUsize::new(0),
            msg_count_per_thread_per_thread: AtomicUsize::new(0),
            msg_size_per_thread_per_thread: AtomicUsize::new(0),
        }
    }

    pub fn mailbox(&self) -> &Arc<BasicMailbox> {
        &self.mailbox
    }

    pub fn mailbox_mut(&mut self) -> &mut Arc<BasicMailbox> {
        &mut self.mailbox
    }

    pub fn state(&self) -> usize {
        self.state.load(Ordering::Relaxed)
    }

    pub fn set_state(&self, state: usize) {
        self.state.store(state, Ordering::Relaxed)
    }

    pub fn msg_count(&self) -> usize {
        self.msg_count.load(Ordering::Relaxed)
    }

    pub fn set_msg_count(&self, msg_count: usize) {
        self.msg_count.store(msg_count, Ordering::Relaxed)
    }

    pub fn msg_size(&self) -> usize {
        self.msg_size.load(Ordering::Relaxed)
    }

    pub fn set_msg_size(&self, msg_size: usize) {
        self.msg_size.store(msg_size, Ordering::Relaxed)
    }

    pub fn msg_count_per_thread(&self) -> usize {
        self.msg_count_per_thread.load(Ordering::Relaxed)
    }

}







/// A FSM is a state machine that can be used to implement a state machine.
/// The FSM is a single threaded state machine.


/// `FsmScheduler` schedules `turing_automata` for later handles.
pub trait FsmScheduler {

    /// `schedule` schedules the `turing_automata` for later handles.
    /// The `turing_automata` is scheduled for later handles.
    /// 
    

    fn schedule(&self, turing_automata: &dyn Fsm);
}


/// `FsmScheduler` schedules `turing_automata` for later handles.
/// The `turing_automata` is scheduled for later handles.
///     
/// # Examples
/// ```
/// use engine_test::FSM;
/// use maxwell::turing_automata::FsmScheduler;
/// use maxwell::turing_automata::turing_automata;
/// use maxwell::turing_automata::FSM;
///
/// let turing_automata = FSM::new();
/// let turing_automata_scheduler = FsmScheduler::new();
/// turing_automata_scheduler.schedule(&turing_automata);
/// ```
/// 
/// # Panics
/// This function may panic if the `turing_automata` is not valid.
/// 
/// # Safety
/// This function is unsafe because it dereferences the `turing_automata` to get its `Mailbox`.
/// This function is unsafe because it dereferences the `Mailbox` to get its `MailboxGuard`.
/// This function is unsafe because it dereferences the `MailboxGuard` to get its `Mailbox`.



/// A turing_automata is a finite state machine. It should be able to be notified for
/// uFIDelating internal state according to incoming messages.
pub trait Fsm {

    /// `mailbox` returns the mailbox for this turing_automata.
    /// The mailbox is used to send messages to the turing_automata.
    ///     
    /// # Examples
    /// ```
    /// use engine_test::FSM;
    /// use maxwell::turing_automata::turing_automata;
    /// use maxwell::turing_automata::FSM;
    ///
    /// let turing_automata = FSM::new();
    /// let mailbox = turing_automata.mailbox();
    /// ```
    /// 
    /// # Panics
    /// This function may panic if the `turing_automata` is not valid.
    /// 
    type Message: Send;

    fn is_stopped(&self) -> bool;

    /// Set a mailbox to turing_automata, which should be used to send message to itself.
    fn set_mailbox(&mut self, _mailbox: Cow<'_, BasicMailbox<Self>>)
    where
        Self: Sized,
    {
    }
    /// Take the mailbox from turing_automata. Implementation should ensure there will be
    /// no reference to mailbox after calling this method.
    fn take_mailbox(&mut self) -> Option<BasicMailbox<Self>>
    where
        Self: Sized,
    {
        None
    }
}


pub struct FsmState<N> {
    status: AtomicUsize,
    data: AtomicPtr<N>,
}

impl<N: Fsm> FsmState<N> {
    pub fn new(data: Box<N>) -> FsmState<N> {
        FsmState {
            status: AtomicUsize::new(NOTIFYSTATE_IDLE),
            data: AtomicPtr::new(Box::into_primitive_causet(data)),
        }
    }

    /// Take the turing_automata if it's IDLE.
    pub fn take_turing_automata(&self) -> Option<Box<N>> {
        let previous_state =
            self.status
                .compare_and_swap(NOTIFYSTATE_IDLE, NOTIFYSTATE_NOTIFIED, Partitioning::AcqRel);
        if previous_state != NOTIFYSTATE_IDLE {
            return None;
        }

        let p = self.data.swap(ptr::null_mut(), Partitioning::AcqRel);
        if !p.is_null() {
            Some(unsafe { Box::from_primitive_causet(p) })
        } else {
            panic!("inconsistent status and data, something should be wrong.");
        }
    }

    /// Notify turing_automata via a `FsmScheduler`.
    #[inline]
    pub fn notify<S: FsmScheduler<Fsm = N>>(
        &self,
        scheduler: &S,
        mailbox: Cow<'_, BasicMailbox<N>>,
    ) {
        match self.take_turing_automata() {
            None => {}
            Some(mut n) => {
                n.set_mailbox(mailbox);
                scheduler.schedule(&n);
            }
        }
    }

    /// Put the owner back to the state.
    ///
    /// It's not required that all messages should be consumed before
    /// releasing a turing_automata. However, a turing_automata is guaranteed to be notified only
    /// when new messages arrives after it's released.
    #[inline]
    pub fn release(&self, turing_automata: Box<N>) {
        let previous = self.data.swap(Box::into_primitive_causet(turing_automata), Partitioning::AcqRel);
        let mut previous_status = NOTIFYSTATE_NOTIFIED;
        if previous.is_null() {
            previous_status = self.status.compare_and_swap(
                NOTIFYSTATE_NOTIFIED,
                NOTIFYSTATE_IDLE,
                Partitioning::AcqRel,
            );
            match previous_status {
                _NOTIFYSTATE_NOTIFIED => return,
                _NOTIFYSTATE_DROP => {
                    let ptr = self.data.swap(ptr::null_mut(), Partitioning::AcqRel);
                    unsafe { Box::from_primitive_causet(ptr) };
                    return;
                }
                _ => {}
            }
        }
        panic!("invalid release state: {:?} {}", previous, previous_status);
    }

    /// Clear the turing_automata.
    #[inline]
    pub fn clear(&self) {
        match self.status.swap(NOTIFYSTATE_DROP, Partitioning::AcqRel) {
            NOTIFYSTATE_NOTIFIED => return,
        }
    }
}

impl<N> Drop for FsmState<N> {
    fn drop(&mut self) {
        let ptr = self.data.swap(ptr::null_mut(), Partitioning::SeqCst);
        if !ptr.is_null() {
            unsafe { Box::from_primitive_causet(ptr) };
        }
    }
}
