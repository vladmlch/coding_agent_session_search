use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fmt;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};

use super::FrankenError;

// ---------------------------------------------------------------------------
// params! macro
// ---------------------------------------------------------------------------

/// Positional `ParamValue` slice matching the rusqlite `params![...]` shape:
/// produces `&[compat::ParamValue]` (a slice — the `as` coercion keeps
/// `if`/`else` arms with different arities unifiable, as in `fparams!`).
#[macro_export]
macro_rules! __cass_franken_params {
    () => {
        &[] as &[$crate::franken_sync::compat::ParamValue]
    };
    ($($value:expr),+ $(,)?) => {
        &[$(
            $crate::franken_sync::compat::ParamValue::from($value)
        ),+] as &[$crate::franken_sync::compat::ParamValue]
    };
}
pub use crate::__cass_franken_params as params;

// ---------------------------------------------------------------------------
// Cancellation (TLS scope + interrupt-handle fan-out)
// ---------------------------------------------------------------------------

thread_local! {
    static CANCELLATION: RefCell<Option<CancellationHandle>> = const { RefCell::new(None) };
}

pub(super) fn current_cancellation() -> Option<CancellationHandle> {
    CANCELLATION.with(|slot| slot.borrow().clone())
}

pub(super) fn current_is_cancelled() -> bool {
    current_cancellation().is_some_and(|handle| handle.is_cancelled())
}

/// Check the thread-local cancellation scope; `Abort` when cancelled.
/// Open and operation entry points call this so a cancelled scope fails fast
/// even when no SQLite statement is in flight.
pub(crate) fn check_connection_admission() -> Result<(), FrankenError> {
    if current_is_cancelled() {
        return Err(FrankenError::Abort);
    }
    Ok(())
}

/// Cooperative cancellation handle. Holds every `InterruptHandle` registered
/// by in-flight operations on the owning thread so `cancel()` interrupts
/// active SQLite statements immediately, then latches `Abort`.
#[derive(Clone)]
pub struct CancellationHandle {
    state: Arc<CancellationState>,
}

struct CancellationState {
    cancelled: AtomicBool,
    /// Interrupt handles of in-flight operations registered on this scope,
    /// keyed by a monotonically increasing registration id.
    active: Mutex<Vec<(u64, rusqlite::InterruptHandle)>>,
    next_registration: AtomicU64,
}

impl fmt::Debug for CancellationHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CancellationHandle")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

impl Default for CancellationHandle {
    fn default() -> Self {
        Self::new()
    }
}

impl CancellationHandle {
    pub fn new() -> Self {
        Self {
            state: Arc::new(CancellationState {
                cancelled: AtomicBool::new(false),
                active: Mutex::new(Vec::new()),
                next_registration: AtomicU64::new(1),
            }),
        }
    }

    /// Cancel the scope: latch the flag, then interrupt every registered
    /// connection so in-flight SQLite calls return promptly.
    /// `InterruptHandle` is not `Clone` in rusqlite 0.40.2, so the
    /// interrupts are issued while holding the registration lock —
    /// `sqlite3_interrupt` is non-blocking, so this is safe. When ops are
    /// still in flight, a repeating 5 ms poke on the shared deadline timer
    /// re-interrupts them so a `cancel()` landing inside `conn.prepare`
    /// (`nVdbeActive == 0`, where `sqlite3_interrupt` is a no-op) is not
    /// lost once `sqlite3_step` begins.
    pub fn cancel(&self) {
        if self.state.cancelled.swap(true, AtomicOrdering::AcqRel) {
            return;
        }
        let has_active = {
            let active = lock_unpoisoned(&self.state.active);
            for (_, handle) in active.iter() {
                handle.interrupt();
            }
            !active.is_empty()
        };
        if has_active {
            deadline_timer().arm_poke(Arc::clone(&self.state), DEADLINE_POKE_INTERVAL);
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.state.cancelled.load(AtomicOrdering::Acquire)
    }

    /// Install this scope on the calling thread until the returned scope
    /// token drops. Nested scopes restore their parent on drop.
    pub fn enter(&self) -> CancellationScope {
        let parent = CANCELLATION.with(|slot| slot.borrow_mut().replace(self.clone()));
        CancellationScope {
            parent,
            _thread_affine: PhantomData::<Rc<()>>,
        }
    }

    /// Attach a connection's interrupt handle to this scope. Returns
    /// `Err(FrankenError::Abort)` when the scope is already cancelled —
    /// checked under the registration lock so a `cancel()` racing between
    /// the caller's pre-check and the push can never leave an op running
    /// under a dead scope.
    pub(super) fn register(
        &self,
        handle: rusqlite::InterruptHandle,
    ) -> Result<CancellationRegistration, FrankenError> {
        let id = self
            .state
            .next_registration
            .fetch_add(1, AtomicOrdering::Relaxed);
        let mut active = lock_unpoisoned(&self.state.active);
        if self.is_cancelled() {
            return Err(FrankenError::Abort);
        }
        active.push((id, handle));
        Ok(CancellationRegistration {
            state: Arc::downgrade(&self.state),
            id,
        })
    }
}

/// RAII token removing a registered interrupt handle when an op completes.
pub(super) struct CancellationRegistration {
    state: std::sync::Weak<CancellationState>,
    id: u64,
}

impl Drop for CancellationRegistration {
    fn drop(&mut self) {
        if let Some(state) = self.state.upgrade() {
            let mut active = lock_unpoisoned(&state.active);
            active.retain(|(id, _)| *id != self.id);
        }
    }
}

/// RAII scope token restoring the previous cancellation scope on drop.
/// `!Send` on purpose: cancellation scopes are thread-local.
pub struct CancellationScope {
    parent: Option<CancellationHandle>,
    _thread_affine: PhantomData<Rc<()>>,
}

impl Drop for CancellationScope {
    fn drop(&mut self) {
        CANCELLATION.with(|slot| {
            *slot.borrow_mut() = self.parent.take();
        });
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

// ---------------------------------------------------------------------------
// Shared deadline timer (one worker thread, BinaryHeap + Condvar)
// ---------------------------------------------------------------------------

/// What an expired deadline pokes: a single statement's interrupt handle, or
/// every handle registered on a cancellation scope (re-poke while in-flight).
enum DeadlineFire {
    Interrupt(rusqlite::InterruptHandle),
    Cancellation(Arc<CancellationState>),
}

/// Mutable per-slot state guarded by its own mutex. Locking it on disarm
/// synchronizes `DeadlineGuard::drop` with an in-flight `interrupt()` so a
/// late fire can never land on the next query using the same connection.
struct DeadlineSlotState {
    disarmed: bool,
    fired: bool,
}

/// Shared state for one armed deadline.
struct DeadlineSlot {
    /// Per-slot lock serializing fire vs. disarm (see `deadline_worker`).
    state: Mutex<DeadlineSlotState>,
    /// Fire target: one connection's interrupt handle, or a whole
    /// cancellation scope's registered handles.
    fire: DeadlineFire,
}

/// Re-poke cadence for `nVdbeActive == 0` windows: `sqlite3_interrupt`
/// before `sqlite3_step` is a C SQLite no-op, so fired entries re-arm
/// themselves every 5 ms until their guard drops / scope drains.
const DEADLINE_POKE_INTERVAL: Duration = Duration::from_millis(5);

struct DeadlineEntry {
    deadline: Instant,
    id: u64,
    slot: Arc<DeadlineSlot>,
}

impl PartialEq for DeadlineEntry {
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline && self.id == other.id
    }
}
impl Eq for DeadlineEntry {}
impl PartialOrd for DeadlineEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for DeadlineEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.deadline
            .cmp(&other.deadline)
            .then_with(|| self.id.cmp(&other.id))
    }
}

struct TimerState {
    heap: BinaryHeap<std::cmp::Reverse<DeadlineEntry>>,
    next_id: u64,
}

/// The process-wide deadline timer: one reusable worker thread that pops the
/// earliest deadline off a min-heap, sleeps on a `Condvar`, and interrupts
/// armed slots as they expire.
struct DeadlineTimer {
    state: Mutex<TimerState>,
    woken: Condvar,
}

impl DeadlineTimer {
    fn spawn() -> Arc<Self> {
        let timer = Arc::new(Self {
            state: Mutex::new(TimerState {
                heap: BinaryHeap::new(),
                next_id: 1,
            }),
            woken: Condvar::new(),
        });
        let worker = Arc::clone(&timer);
        let _ = std::thread::Builder::new()
            .name("cass-franken-sync-deadline".to_string())
            .spawn(move || deadline_worker(&worker));
        timer
    }

    fn arm(&self, interrupt: rusqlite::InterruptHandle, deadline: Instant) -> DeadlineGuard {
        let slot = Arc::new(DeadlineSlot {
            state: Mutex::new(DeadlineSlotState {
                disarmed: false,
                fired: false,
            }),
            fire: DeadlineFire::Interrupt(interrupt),
        });
        {
            let mut state = lock_unpoisoned(&self.state);
            let id = state.next_id;
            state.next_id = state.next_id.wrapping_add(1);
            state.heap.push(std::cmp::Reverse(DeadlineEntry {
                deadline,
                id,
                slot: Arc::clone(&slot),
            }));
        }
        self.woken.notify_one();
        DeadlineGuard { slot }
    }

    /// Arm a repeating poke against every interrupt handle registered on a
    /// cancellation scope: fires `delay` from now, then re-arms every
    /// `DEADLINE_POKE_INTERVAL` while the scope still has in-flight
    /// operations (covers `nVdbeActive == 0` misses inside `prepare`).
    fn arm_poke(&self, cancel_state: Arc<CancellationState>, delay: Duration) {
        let slot = Arc::new(DeadlineSlot {
            state: Mutex::new(DeadlineSlotState {
                disarmed: false,
                fired: false,
            }),
            fire: DeadlineFire::Cancellation(cancel_state),
        });
        {
            let mut state = lock_unpoisoned(&self.state);
            let id = state.next_id;
            state.next_id = state.next_id.wrapping_add(1);
            state.heap.push(std::cmp::Reverse(DeadlineEntry {
                deadline: Instant::now() + delay,
                id,
                slot,
            }));
        }
        self.woken.notify_one();
    }
}

static DEADLINE_TIMER: OnceLock<Arc<DeadlineTimer>> = OnceLock::new();

fn deadline_timer() -> &'static Arc<DeadlineTimer> {
    DEADLINE_TIMER.get_or_init(DeadlineTimer::spawn)
}

fn deadline_worker(timer: &Arc<DeadlineTimer>) {
    loop {
        // Pop every expired entry, then fire each under ITS OWN slot lock.
        // Lock order is strictly slot.state -> timer.state (disarm only
        // takes slot.state; arm only takes timer.state), so no cycle.
        let expired: Vec<DeadlineEntry> = {
            let mut state = lock_unpoisoned(&timer.state);
            let now = Instant::now();
            let mut expired = Vec::new();
            while let Some(std::cmp::Reverse(top)) = state.heap.peek() {
                if top.deadline > now {
                    break;
                }
                expired.push(state.heap.pop().expect("peeked entry").0);
            }
            expired
        };

        for entry in expired {
            let repush = {
                let mut slot_state = lock_unpoisoned(&entry.slot.state);
                if slot_state.disarmed {
                    false
                } else {
                    slot_state.fired = true;
                    let repush = match &entry.slot.fire {
                        DeadlineFire::Interrupt(handle) => {
                            handle.interrupt();
                            // Keep poking until the guard drops so a fire
                            // landing in the nVdbeActive == 0 window (inside
                            // prepare / between statements) is retried.
                            true
                        }
                        DeadlineFire::Cancellation(cancel_state) => {
                            let active = lock_unpoisoned(&cancel_state.active);
                            for (_, handle) in active.iter() {
                                handle.interrupt();
                            }
                            !active.is_empty()
                        }
                    };
                    if !repush {
                        slot_state.disarmed = true;
                    }
                    repush
                }
            };
            if repush {
                let mut state = lock_unpoisoned(&timer.state);
                state.heap.push(std::cmp::Reverse(DeadlineEntry {
                    deadline: Instant::now() + DEADLINE_POKE_INTERVAL,
                    id: entry.id,
                    slot: entry.slot,
                }));
            }
        }

        {
            let state = lock_unpoisoned(&timer.state);
            let wait_for = state
                .heap
                .peek()
                .map(|std::cmp::Reverse(entry)| entry.deadline);
            match wait_for {
                Some(deadline) => {
                    let delay = deadline.saturating_duration_since(Instant::now());
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
}

/// RAII deadline token. While armed, expiry interrupts the owning connection
/// (re-poking every `DEADLINE_POKE_INTERVAL` so an expiry landing before
/// `sqlite3_step` is retried); `disarm()` (or `Drop`) synchronizes with any
/// in-flight interrupt through the per-slot lock, so a dropped guard can
/// never let a late fire hit the next query on that connection.
pub struct DeadlineGuard {
    slot: Arc<DeadlineSlot>,
}

impl DeadlineGuard {
    /// Arm a deadline against an interrupt handle directly.
    pub fn arm(interrupt: rusqlite::InterruptHandle, deadline: Instant) -> Self {
        deadline_timer().arm(interrupt, deadline)
    }

    /// `true` once the deadline actually expired and interrupted.
    pub fn fired(&self) -> bool {
        lock_unpoisoned(&self.slot.state).fired
    }

    /// `true` while the deadline entry is still armed.
    pub fn is_armed(&self) -> bool {
        !lock_unpoisoned(&self.slot.state).disarmed
    }

    /// Disarm the timer entry; expiry after this call cannot fire. The call
    /// blocks briefly if the worker is mid-`interrupt()` on this slot, so no
    /// late fire can ever land on a subsequent query.
    pub fn disarm(&self) {
        lock_unpoisoned(&self.slot.state).disarmed = true;
    }
}

impl Drop for DeadlineGuard {
    fn drop(&mut self) {
        self.disarm();
    }
}

