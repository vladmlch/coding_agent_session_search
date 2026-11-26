//! Per-request time budget: a cooperative deadline plus a process-killing
//! backstop.
//!
//! Every request carries a [`Deadline`] — its cooperative deadline — that
//! the long ops (`search_sessions`, the raw rank lane) check at their loop
//! boundaries. Expiry answers THAT request with the typed, retryable
//! `deadline_exceeded` refusal and the process keeps serving the next
//! request; one slow search no longer takes down catalog polling, session
//! reads and every other viewer lane.
//!
//! The [`Watchdog`] survives only as the last-resort backstop: it fires
//! `BACKSTOP_GRACE_MS` past the same deadline and kills the process with
//! exit(70) — for a request that can no longer reach ANY boundary check
//! (a stuck engine call that never returns). A request that IS making
//! progress always refuses cooperatively first, because the grace exceeds
//! the largest single uninterruptible step a healthy request can be inside
//! when its deadline passes.

use std::any::Any;
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::fmt::Display;
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use rusqlite::InterruptHandle;

use super::protocol::refuse_retryable;

/// Re-poke cadence while an interrupt guard remains armed past its deadline so
/// an interrupt firing in SQLite's `nVdbeActive == 0` window (between prepare
/// and step) still interrupts once statement execution begins.
const INTERRUPT_POKE_INTERVAL: Duration = Duration::from_millis(5);

/// Margin between a request's cooperative deadline and the process-killing
/// backstop. The cooperative deadline produces a typed, retryable
/// refusal; the backstop exists only for a request that stops making
/// progress and therefore would never refuse on its own. The margin must
/// exceed the largest single uninterruptible step a healthy request can be
/// inside when its deadline passes — measured on the 4.9 GB owner archive
/// (6k sessions, 831k universe region rows), the whole-universe region
/// scan runs ~5.6 s warm and ~10 s under concurrent load
/// (`.scratch/glm/2026-11-10-viewer-latency` §3), so 15 s lets a request
/// that is merely slow finish its in-flight step, hit the next boundary
/// check and refuse cooperatively, while a genuinely stuck request still
/// dies in bounded time. Documented on the wire through hello's
/// `limits.hard_backstop_grace_ms`.
pub const BACKSTOP_GRACE_MS: u64 = 15_000;

/// Test-only override for the backstop grace
/// (`CASS_TEST_VIEWER_BRIDGE_BACKSTOP_GRACE_MS`): lets the pinned
/// regression test prove the backstop still kills a stuck request without
/// waiting 15 s. Production callers never set this variable.
fn backstop_grace_ms() -> u64 {
    dotenvy::var("CASS_TEST_VIEWER_BRIDGE_BACKSTOP_GRACE_MS")
        .ok()
        .and_then(|raw| raw.trim().parse().ok())
        .filter(|ms| *ms > 0)
        .unwrap_or(BACKSTOP_GRACE_MS)
}

/// A request's cooperative deadline. Long ops check it at their loop
/// boundaries ([`Deadline::check`]); expiry is a typed, retryable refusal
/// naming the phase and the work completed so far — never a partial
/// answer labelled complete, never a process kill.
#[derive(Debug, Clone, Copy)]
pub struct Deadline {
    at: Instant,
}

impl Deadline {
    pub fn from_millis(timeout_ms: u64) -> Self {
        Self {
            at: Instant::now() + Duration::from_millis(timeout_ms),
        }
    }

    /// The underlying instant (used by the rank lane, whose cursor shares
    /// one deadline across `rank_begin`/`rank_next` requests).
    pub fn at(&self) -> Instant {
        self.at
    }

    pub fn exceeded(&self) -> bool {
        Instant::now() >= self.at
    }

    /// The typed, retryable refusal for an expired deadline: the phase it
    /// died in plus the counts of what was completed, so callers can
    /// adapt (retry under less load, narrower filters, smaller page).
    pub fn refusal(phase: &str, detail: impl Display) -> anyhow::Error {
        refuse_retryable(
            "deadline_exceeded",
            format!(
                "Request deadline exceeded in phase {phase} ({detail}); \
                 the same request may succeed under less load"
            ),
        )
    }

    /// Loop-boundary check: refuse when the deadline has passed.
    pub fn check(&self, phase: &str, detail: impl Display) -> anyhow::Result<()> {
        if self.exceeded() {
            Err(Self::refusal(phase, detail))
        } else {
            Ok(())
        }
    }

    /// Arm a SQLite `InterruptHandle` to fire when this cooperative deadline
    /// expires. Dropping the returned guard disarms the timer synchronously.
    #[allow(dead_code)]
    pub fn arm_interrupt(&self, handle: InterruptHandle) -> InterruptGuard {
        InterruptGuard::arm(handle, self.at)
    }
}

/// Type-erased [`InterruptGuard::arm`] adapter for `StrictArchive::snapshot_stamped`,
/// keeping `reader.rs` decoupled from `deadline.rs` in standalone test builds.
pub fn arm_snapshot_interrupt(handle: InterruptHandle, deadline: Instant) -> Box<dyn Any> {
    Box::new(InterruptGuard::arm(handle, deadline))
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct InterruptSlotState {
    disarmed: bool,
    handle: InterruptHandle,
}

struct InterruptSlot {
    state: Mutex<InterruptSlotState>,
}

struct InterruptEntry {
    deadline: Instant,
    id: u64,
    slot: Arc<InterruptSlot>,
}

impl PartialEq for InterruptEntry {
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline && self.id == other.id
    }
}

impl Eq for InterruptEntry {}

impl PartialOrd for InterruptEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for InterruptEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.deadline
            .cmp(&other.deadline)
            .then_with(|| self.id.cmp(&other.id))
    }
}

struct InterruptTimerState {
    heap: BinaryHeap<Reverse<InterruptEntry>>,
    next_id: u64,
}

struct InterruptTimer {
    state: Mutex<InterruptTimerState>,
    woken: Condvar,
}

static INTERRUPT_TIMER: OnceLock<Arc<InterruptTimer>> = OnceLock::new();

impl InterruptTimer {
    fn global() -> &'static Arc<InterruptTimer> {
        INTERRUPT_TIMER.get_or_init(|| {
            let timer = Arc::new(Self {
                state: Mutex::new(InterruptTimerState {
                    heap: BinaryHeap::new(),
                    next_id: 1,
                }),
                woken: Condvar::new(),
            });
            let worker = Arc::clone(&timer);
            let _ = thread::Builder::new()
                .name("viewer-bridge-interrupt".into())
                .spawn(move || interrupt_worker(&worker));
            timer
        })
    }

    fn arm(&self, handle: InterruptHandle, deadline: Instant) -> InterruptGuard {
        let slot = Arc::new(InterruptSlot {
            state: Mutex::new(InterruptSlotState {
                disarmed: false,
                handle,
            }),
        });
        {
            let mut state = lock_unpoisoned(&self.state);
            let seq = state.next_id;
            state.next_id = state.next_id.wrapping_add(1);
            state.heap.push(Reverse(InterruptEntry {
                deadline,
                id: seq,
                slot: Arc::clone(&slot),
            }));
        }
        self.woken.notify_one();
        InterruptGuard { slot }
    }
}

fn interrupt_worker(timer: &Arc<InterruptTimer>) {
    loop {
        // Lock order is strictly `slot.state` (worker firing) or `timer.state`
        // (heap pop/push), never both simultaneously -> deadlock-free.
        let expired = {
            let mut state = lock_unpoisoned(&timer.state);
            let now = Instant::now();
            let mut expired = Vec::new();
            while let Some(Reverse(top)) = state.heap.peek() {
                if top.deadline > now {
                    break;
                }
                if let Some(Reverse(entry)) = state.heap.pop() {
                    expired.push(entry);
                }
            }
            expired
        };

        for entry in expired {
            let repush = {
                let slot_state = lock_unpoisoned(&entry.slot.state);
                if slot_state.disarmed {
                    false
                } else {
                    slot_state.handle.interrupt();
                    true
                }
            };
            if repush {
                let mut state = lock_unpoisoned(&timer.state);
                let id = state.next_id;
                state.next_id = state.next_id.wrapping_add(1);
                state.heap.push(Reverse(InterruptEntry {
                    deadline: Instant::now() + INTERRUPT_POKE_INTERVAL,
                    id,
                    slot: entry.slot,
                }));
            }
        }

        let state = lock_unpoisoned(&timer.state);
        match state.heap.peek().map(|Reverse(entry)| entry.deadline) {
            Some(next_deadline) => {
                let delay = next_deadline.saturating_duration_since(Instant::now());
                if delay.is_zero() {
                    continue;
                }
                drop(
                    timer
                        .woken
                        .wait_timeout(state, delay)
                        .unwrap_or_else(|poisoned| poisoned.into_inner()),
                );
            }
            None => {
                drop(
                    timer
                        .woken
                        .wait(state)
                        .unwrap_or_else(|poisoned| poisoned.into_inner()),
                );
            }
        }
    }
}

/// RAII guard arming a `rusqlite::InterruptHandle` against a cooperative
/// deadline. Dropping or disarming synchronizes with any in-flight
/// `interrupt()` call so cleanup (`ROLLBACK`) and subsequent queries on the
/// same connection are never interrupted late.
pub struct InterruptGuard {
    slot: Arc<InterruptSlot>,
}

impl InterruptGuard {
    pub fn arm(handle: InterruptHandle, deadline: Instant) -> Self {
        InterruptTimer::global().arm(handle, deadline)
    }

    pub fn disarm(&self) {
        lock_unpoisoned(&self.slot.state).disarmed = true;
    }
}

impl Drop for InterruptGuard {
    fn drop(&mut self) {
        self.disarm();
    }
}

enum Message {
    Begin(Instant),
    Done,
    Stop,
}

pub struct Watchdog {
    sender: Sender<Message>,
    thread: Option<JoinHandle<()>>,
}

impl Watchdog {
    pub fn start() -> std::io::Result<Self> {
        let (sender, receiver) = mpsc::channel();
        let thread = thread::Builder::new()
            .name("viewer-bridge-deadline".into())
            .spawn(move || {
                let mut deadline: Option<Instant> = None;
                loop {
                    let message = match deadline {
                        Some(end) => match receiver
                            .recv_timeout(end.saturating_duration_since(Instant::now()))
                        {
                            Ok(value) => value,
                            Err(RecvTimeoutError::Disconnected) => return,
                            Err(RecvTimeoutError::Timeout) => {
                                // The last-resort backstop: the request
                                // made no progress past its cooperative
                                // deadline, so the shared stdio worker is
                                // killed rather than pinned forever. This
                                // is the ONLY exit(70) left in the bridge.
                                eprintln!(
                                    "cass-viewer-bridge: request hard backstop exceeded — \
                                     no cooperative deadline refusal within the {} ms grace \
                                     past the deadline; exiting 70",
                                    backstop_grace_ms()
                                );
                                std::process::exit(70);
                            }
                        },
                        None => match receiver.recv() {
                            Ok(value) => value,
                            Err(_) => return,
                        },
                    };
                    match message {
                        Message::Begin(end) => deadline = Some(end),
                        Message::Done => deadline = None,
                        Message::Stop => return,
                    }
                }
            })?;
        Ok(Self {
            sender,
            thread: Some(thread),
        })
    }

    pub fn begin(&self, timeout_ms: u64) {
        // The cooperative deadline at `timeout_ms` answers first (typed,
        // retryable refusal); the backstop fires only for a request that
        // cannot reach any boundary check — hence the generous grace.
        let _ = self.sender.send(Message::Begin(
            Instant::now() + Duration::from_millis(timeout_ms + backstop_grace_ms()),
        ));
    }

    pub fn done(&self) {
        let _ = self.sender.send(Message::Done);
    }
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        let _ = self.sender.send(Message::Stop);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
