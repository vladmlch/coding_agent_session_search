//! Task 12: independent committed-change push channel (spec decision 7).
//!
//! ## Transport choice: dedicated loopback TCP listener
//!
//! The bridge's only stdio pair is the serialized request/response FIFO;
//! interleaving event frames onto it would share the blocking RPC queue the
//! spec forbids ("the channel MUST NOT block the serialized
//! request/response FIFO") and would break protocol-1 reply correlation.
//! A Unix socket is not portable to the Windows builds this crate ships.
//! A `TcpListener` on `127.0.0.1:0` is portable, needs no new dependency,
//! and the ephemeral port is published in
//! `<db>.viewer_events.addr` — a sidecar any process holding the archive
//! path can read, so a separate stock indexer can wake the bridge with no
//! extra configuration. WebSocket/SSE was rejected for this hop: a plain
//! notification socket does not by itself guarantee catch-up anyway — the
//! SQLite `viewer_change_log` journal is the truth; this channel only
//! delivers replay and wakeups. Viewer→browser SSE stays unchanged.
//!
//! ## Wire protocol (`cass-viewer-events/1`, one JSON object per line)
//!
//! Client → bridge first frame:
//! * `{"op":"subscribe","after_seq":N}` — attach a subscriber resuming from
//!   durable journal seq `N` (absent/`0` = replay the retained head).
//! * `{"op":"notify"}` — wakeup-only hint from a post-commit writer
//!   (`send_bridge_wakeup_best_effort`); the pump rechecks the journal
//!   immediately. Carries no payload semantics: a forged or stale wakeup
//!   costs one bounded journal read, never a wrong event.
//!
//! Bridge → client frames:
//! * `{"type":"subscribed","after_seq":N,"high_water":H,"truncated":B,
//!    "current_revisions":{...},"lexical_tracking":"bound"}` — the
//!   watermark ack. `"bound"` negotiates that this channel's `index_ready`
//!   frames certify real Quill publications against the durable
//!   `PublicationReceipt`, never canonical progress alone (Task 8).
//! * Event frame: `{type:<journal kind>, sequence, conversation_id?,
//!    revision, payload?, canonical_revision, annotation_revision,
//!    projection_revision, lexical_generation}`. Task 8 readiness split:
//!   `canonical_ready` is the journal's canonical-progress marker and
//!   carries `"lexical_ready": false`; `index_ready` exists only because
//!   `certify_lexical_publication` committed a real Quill publication, so
//!   it carries `"lexical_ready": true` and binds the receipt's own
//!   covered revisions + `lexical_generation` — never a readiness that
//!   was not published.
//! * `{"type":"heartbeat","high_water":H}` every `HEARTBEAT_TICKS` pump
//!   ticks — liveness only; idle connections receive this and nothing
//!   else. A fresh subscriber whose first pass replays no event backlog
//!   (it resumed at the journal tail) also gets a heartbeat on that pass,
//!   so post-reset/post-subscribe liveness is prompt rather than waiting
//!   out the heartbeat phase.
//! * `{"type":"reset_required",...}` on an expired/regressed cursor or a
//!   pending-queue overflow, then the connection closes; the client falls
//!   back to `changes_since` refetch + refetch of affected views.
//!
//! ## Atomic reconnect (read watermark → subscribe → recheck/replay)
//!
//! The client records its last consumed `sequence`. On reconnect it sends
//! `{"op":"subscribe","after_seq":W}`; the pump serves `changes_since(W)`
//! pages — the same monotone seq space the live pump reads — so a commit
//! racing the reconnect is either already inside the `subscribed` ack's
//! `high_water` or arrives on the very next pump pass. No window can be
//! missed; expired windows are answered `truncated`/`reset_required`,
//! never a silently partial replay.
//!
//! ## Concurrency and resource bounds
//!
//! Two threads, never the RPC thread: an acceptor (`listener.accept()`
//! blocking — zero CPU while idle) hands each stream to a single pump
//! thread through an mpsc channel. The pump owns its own strictly
//! read-only archive connection (a second connection to `StrictArchive`'s
//! — two connections, never shared state), polls the journal every
//! `poll_ms` only while subscribers exist, enqueues into per-subscriber
//! bounded byte/event queues, and flushes nonblocking. A subscriber that
//! misses more than `MAX_PENDING_EVENTS`/`MAX_PENDING_BYTES` gets
//! `reset_required` + close: large catch-up belongs on the `changes_since`
//! RPC page lane, not on a slow socket. The pump re-opens its journal
//! connection every `REOPEN_TICKS` so a replaced archive inode un-pins
//! (the seq-regression reset path then does the rest). With zero
//! subscribers the loop performs only channel ops — bounded idle cost.
//! Sudden writer or bridge death is safe: writers crash AFTER commit leave
//! a fully replayable journal, and bridge death just closes sockets.

use std::collections::VecDeque;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use coding_agent_search::franken_sync::Connection;
use coding_agent_search::franken_sync::compat::{OpenFlags, open_with_flags};
use coding_agent_search::viewer_extensions::changes as journal;
use serde_json::{Value, json};

/// Subscribers per bridge — a viewer needs one; eight bounds the fan-out
/// work any pump pass performs.
const MAX_SUBSCRIBERS: usize = 8;
/// Pending-queue bounds per subscriber: exceeded → `reset_required` +
/// close. These are also the replay bound — a window wider than this is
/// served by the `changes_since` RPC page lane by design.
const MAX_PENDING_EVENTS: usize = 256;
const MAX_PENDING_BYTES: usize = 256 * 1024;
/// First-frame handshake bound and deadline.
const HANDSHAKE_BYTES: usize = 4 * 1024;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(2);
/// Heartbeat interval multiplier: `poll * HEARTBEAT_TICKS` wall-clock
/// (40 × 250 ms default = 10 s). Wall-clock, not tick count — a slow
/// journal pass must not delay liveness.
const HEARTBEAT_TICKS: u64 = 40;
/// Journal connection re-open every N ticks so a replaced archive inode
/// un-pins (20 × 250 ms = 5 s; scales with the configured poll).
const REOPEN_TICKS: u64 = 20;
/// A subscriber that never drains is dropped after this many stalled
/// ticks (400 × 250 ms = 100 s worst case at default poll).
const STALL_LIMIT_TICKS: u32 = 400;
/// Bytes attempted per subscriber per flush pass — fairness bound.
const FLUSH_SLICE: usize = 16 * 1024;

const OP_SUBSCRIBE: &str = "subscribe";
const OP_NOTIFY: &str = journal::EVENT_OP_NOTIFY;
const PROTOCOL: &str = "cass-viewer-events/1";

/// Owning handle for the event listener + pump. Dropping it stops both
/// threads and removes the endpoint sidecar.
pub struct EventStreamHandle {
    endpoint: String,
    sidecar: PathBuf,
    shutdown: Arc<AtomicBool>,
    acceptor: Option<JoinHandle<()>>,
    pump: Option<JoinHandle<()>>,
}

impl EventStreamHandle {
    /// Bind `127.0.0.1:0`, publish `<db>.viewer_events.addr` and spawn the
    /// acceptor + pump threads. `poll` is the safety-net journal recheck
    /// cadence; the IPC wakeup stays the primary low-latency path.
    pub fn start(db: &Path, poll: Duration) -> Result<Self> {
        let poll = poll.max(Duration::from_millis(5));
        let listener = TcpListener::bind("127.0.0.1:0").context("binding viewer event listener")?;
        let endpoint = listener
            .local_addr()
            .context("event listener local address")?
            .to_string();
        let sidecar = journal::event_endpoint_sidecar_path(db);
        let epoch = format!("{:032x}", rand::random::<u128>());
        let advertisement = json!({
            "addr": endpoint,
            "pid": std::process::id(),
            "epoch": epoch,
            "protocol": PROTOCOL,
        });
        std::fs::write(&sidecar, advertisement.to_string()).with_context(|| {
            format!("publishing event endpoint sidecar at {}", sidecar.display())
        })?;

        let shutdown = Arc::new(AtomicBool::new(false));
        let (accepted, inbox) = mpsc::channel::<TcpStream>();
        // A spawn failure must not leave a published endpoint advertising a
        // listener that does not exist — or a leaked acceptor still bound
        // to it.
        let spawn = || -> Result<(JoinHandle<()>, JoinHandle<()>)> {
            let acceptor = {
                let shutdown = Arc::clone(&shutdown);
                thread::Builder::new()
                    .name("viewer-events-acceptor".into())
                    .spawn(move || acceptor_loop(listener, accepted, shutdown))
            }
            .context("spawning viewer event acceptor")?;
            let pump = {
                let shutdown = Arc::clone(&shutdown);
                let db = db.to_owned();
                thread::Builder::new()
                    .name("viewer-events-pump".into())
                    .spawn(move || pump_loop(&db, inbox, shutdown, poll))
            }
            .context("spawning viewer event pump")?;
            Ok((acceptor, pump))
        };
        let (acceptor, pump) = match spawn() {
            Ok(threads) => threads,
            Err(error) => {
                shutdown.store(true, Ordering::Release);
                // Wake any acceptor already running so it exits on the flag.
                let _ = TcpStream::connect(&endpoint);
                let _ = std::fs::remove_file(&sidecar);
                return Err(error);
            }
        };
        Ok(Self {
            endpoint,
            sidecar,
            shutdown,
            acceptor: Some(acceptor),
            pump: Some(pump),
        })
    }

    /// `127.0.0.1:PORT` subscribers and wakeup senders connect to.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

impl Drop for EventStreamHandle {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        // Wake the blocking accept() so the acceptor sees the flag.
        let _ = TcpStream::connect(&self.endpoint);
        if let Some(thread) = self.acceptor.take() {
            let _ = thread.join();
        }
        if let Some(thread) = self.pump.take() {
            let _ = thread.join();
        }
        // A crashed bridge leaves a stale sidecar; the pid/epoch fields
        // plus connect failure bound the damage — wakeup is best-effort
        // by contract and the poll fallback always remains.
        let _ = std::fs::remove_file(&self.sidecar);
    }
}

/// Blocking accept — zero CPU while idle. Every accepted stream is handed
/// to the pump; the pump owns all further I/O.
fn acceptor_loop(listener: TcpListener, sender: Sender<TcpStream>, shutdown: Arc<AtomicBool>) {
    for stream in listener.incoming() {
        if shutdown.load(Ordering::Relaxed) {
            return;
        }
        match stream {
            Ok(stream) => {
                let _ = stream.set_nonblocking(true);
                let _ = stream.set_nodelay(true);
                if sender.send(stream).is_err() {
                    return;
                }
            }
            Err(_) if shutdown.load(Ordering::Relaxed) => return,
            Err(_) => {}
        }
    }
}

/// A just-accepted connection completing its single-line handshake
/// nonblocking; a deadline drops half-open clients.
struct Handshake {
    stream: TcpStream,
    buffer: Vec<u8>,
    deadline: Instant,
}

enum HandshakeOutcome {
    Pending,
    Ready(String),
    Closed,
}

impl Handshake {
    fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            buffer: Vec::with_capacity(512),
            deadline: Instant::now() + HANDSHAKE_TIMEOUT,
        }
    }

    fn progress(&mut self) -> HandshakeOutcome {
        let mut chunk = [0_u8; 2048];
        match self.stream.read(&mut chunk) {
            Ok(0) => HandshakeOutcome::Closed,
            Ok(read) => {
                self.buffer.extend_from_slice(&chunk[..read]);
                if let Some(newline) = self.buffer.iter().position(|byte| *byte == b'\n') {
                    let line = String::from_utf8_lossy(&self.buffer[..newline]).into_owned();
                    return HandshakeOutcome::Ready(line);
                }
                if self.buffer.len() > HANDSHAKE_BYTES || Instant::now() > self.deadline {
                    HandshakeOutcome::Closed
                } else {
                    HandshakeOutcome::Pending
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                if Instant::now() > self.deadline {
                    HandshakeOutcome::Closed
                } else {
                    HandshakeOutcome::Pending
                }
            }
            Err(_) => HandshakeOutcome::Closed,
        }
    }
}

enum ClientOp {
    Subscribe { after_seq: i64 },
    Notify,
}

/// Parse the first client frame. Unknown/malformed frames are refused;
/// `after_seq` must be a nonnegative integer.
fn classify(line: &str) -> Result<ClientOp> {
    let value: Value = serde_json::from_str(line).map_err(|_| {
        super::protocol::refuse("invalid_request", "Handshake must be one JSON object")
    })?;
    let op = value
        .get("op")
        .and_then(Value::as_str)
        .ok_or_else(|| super::protocol::refuse("invalid_request", "Handshake requires an op"))?;
    match op {
        OP_SUBSCRIBE => {
            let after_seq = match value.get("after_seq") {
                None | Some(Value::Null) => 0,
                Some(raw) => raw.as_i64().ok_or_else(|| {
                    super::protocol::refuse("invalid_request", "after_seq must be an integer")
                })?,
            };
            if after_seq < 0 {
                return Err(super::protocol::refuse(
                    "invalid_request",
                    "after_seq must be nonnegative",
                ));
            }
            Ok(ClientOp::Subscribe { after_seq })
        }
        OP_NOTIFY => Ok(ClientOp::Notify),
        _ => Err(super::protocol::refuse(
            "invalid_request",
            format!("Unknown event op '{op}'"),
        )),
    }
}

/// One fully encoded wire frame with per-frame flush progress. `pinned`
/// marks frames an overflow reset must preserve — the `subscribed` ack is
/// the handshake contract and is always delivered even when the pending
/// queue behind it overflows to `reset_required`.
struct OutFrame {
    bytes: Vec<u8>,
    sent: usize,
    pinned: bool,
}

/// A live subscriber: resume watermark plus the bounded pending queue the
/// backpressure contract enforces.
struct Subscriber {
    stream: TcpStream,
    watermark: i64,
    pending: VecDeque<OutFrame>,
    pending_bytes: usize,
    close_after_flush: bool,
    dead: bool,
    stalled_ticks: u32,
    /// First pump pass not yet completed: a subscriber resumed at the
    /// journal tail (no event backlog) gets its first heartbeat on that
    /// pass instead of waiting out the global `HEARTBEAT_TICKS` phase, so
    /// post-subscribe liveness is prompt and deterministic rather than
    /// phase-dependent.
    fresh: bool,
}

impl Subscriber {
    fn new(stream: TcpStream, watermark: i64) -> Self {
        Self {
            stream,
            watermark,
            pending: VecDeque::new(),
            pending_bytes: 0,
            close_after_flush: false,
            dead: false,
            stalled_ticks: 0,
            fresh: true,
        }
    }

    fn enqueue(&mut self, bytes: Vec<u8>) {
        self.enqueue_inner(bytes, false);
    }

    fn enqueue_pinned(&mut self, bytes: Vec<u8>) {
        self.enqueue_inner(bytes, true);
    }

    fn enqueue_inner(&mut self, bytes: Vec<u8>, pinned: bool) {
        self.pending_bytes += bytes.len();
        self.pending.push_back(OutFrame {
            bytes,
            sent: 0,
            pinned,
        });
    }

    /// The backpressure contract: a pending queue past the bound is
    /// answered with `reset_required` + close — coarse refetch through
    /// `changes_since`, never an unbounded buffer or a silently truncated
    /// stream. Pinned frames (the subscribed ack) survive the reset.
    fn overflow(&mut self, high_water: Option<i64>, revisions: Option<&journal::JournalRevisions>) {
        self.pending.retain(|frame| frame.pinned);
        self.pending_bytes = self
            .pending
            .iter()
            .map(|frame| frame.bytes.len() - frame.sent)
            .sum();
        let mut frame = json!({
            "type": "reset_required",
            "reason": "queue_overflow",
            "sequence": high_water,
        });
        if let Some(revisions) = revisions {
            frame["current_revisions"] = serde_json::to_value(revisions).unwrap_or(Value::Null);
        }
        let mut bytes = frame.to_string().into_bytes();
        bytes.push(b'\n');
        self.enqueue(bytes);
        self.close_after_flush = true;
    }

    /// Nonblocking drain of the pending front; returns bytes written.
    fn flush(&mut self) -> usize {
        let mut written = 0_usize;
        while written < FLUSH_SLICE {
            let Some(front) = self.pending.front_mut() else {
                break;
            };
            let remaining = &front.bytes[front.sent..];
            let take = remaining.len().min(FLUSH_SLICE - written);
            match self.stream.write(&remaining[..take]) {
                Ok(0) => break,
                Ok(did) => {
                    front.sent += did;
                    self.pending_bytes = self.pending_bytes.saturating_sub(did);
                    written += did;
                    if front.sent == front.bytes.len() {
                        self.pending.pop_front();
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(_) => {
                    self.dead = true;
                    break;
                }
            }
        }
        written
    }

    fn over_bound(&self) -> bool {
        self.pending.len() > MAX_PENDING_EVENTS || self.pending_bytes > MAX_PENDING_BYTES
    }
}

fn encode(value: &Value) -> Vec<u8> {
    let mut bytes = value.to_string().into_bytes();
    bytes.push(b'\n');
    bytes
}

/// Serialize one journal event into the spec-12 wire shape:
/// `{type, sequence, conversation_id?, revision, payload?,
///   canonical_revision, annotation_revision, projection_revision,
///   lexical_generation}`. Task 8 readiness split: `canonical_ready`
/// stamps `lexical_ready:false` (canonical progress only); `index_ready`
/// stamps `lexical_ready:true` — it exists ONLY because
/// `certify_lexical_publication` persisted the receipt — and overlays the
/// receipt's own covered revisions/`lexical_generation` so the frame binds
/// the generation it certifies even when a newer certification already
/// moved the live counters.
fn event_frame(
    event: &journal::ChangeEvent,
    revisions: Option<&journal::JournalRevisions>,
) -> Vec<u8> {
    let mut frame = json!({
        "type": event.kind,
        "sequence": event.seq,
        "revision": event.revision,
    });
    if let Some(conversation_id) = event.conversation_id {
        frame["conversation_id"] = conversation_id.into();
    }
    let mut receipt: Option<Value> = None;
    if let Some(payload) = &event.payload_json {
        let parsed =
            serde_json::from_str(payload).unwrap_or_else(|_| Value::String(payload.clone()));
        if parsed.is_object() {
            receipt = Some(parsed.clone());
        }
        frame["payload"] = parsed;
    }
    if let Some(revisions) = revisions {
        frame["canonical_revision"] = revisions.canonical_revision.into();
        frame["annotation_revision"] = revisions.annotation_revision.into();
        frame["projection_revision"] = revisions.projection_revision.into();
        frame["lexical_generation"] = revisions.lexical_generation.into();
    }
    if event.kind == journal::kind::INDEX_READY {
        // Only a version-1 PublicationReceipt — the exact payload schema
        // `certify_lexical_publication` commits — may claim readiness.
        // Retained pre-Task-8 `index_ready` rows and malformed receipts
        // never do (`"v": true`/`"v": "1"` are not the integer 1).
        let certified = receipt
            .as_ref()
            .is_some_and(|receipt| {
                receipt.get("v").and_then(Value::as_i64)
                    == Some(journal::PUBLICATION_RECEIPT_VERSION)
            });
        if certified
            && let Some(receipt) = &receipt
        {
            // The durable receipt the certification committed with the
            // event is the binding — a later certification must not
            // retroactively widen what this frame announces.
            for field in [
                "canonical_revision",
                "annotation_revision",
                "projection_revision",
                "lexical_generation",
            ] {
                if let Some(value) = receipt.get(field) {
                    frame[field] = value.clone();
                }
            }
        }
        frame["lexical_ready"] = certified.into();
    }
    if event.kind == journal::kind::CANONICAL_READY {
        frame["lexical_ready"] = false.into();
    }
    encode(&frame)
}

fn subscribed_frame(after_seq: i64, connection: Option<&Connection>) -> Vec<u8> {
    let (high_water, truncated, revisions) = match connection {
        Some(connection) => {
            let high_water = journal::journal_high_water(connection).ok();
            let probe = journal::changes_since(connection, after_seq, 1).ok();
            (
                high_water,
                probe.as_ref().map(|page| page.truncated),
                probe.and_then(|page| page.current_revisions),
            )
        }
        None => (None, None, None),
    };
    encode(&json!({
        "type": "subscribed",
        "after_seq": after_seq,
        "high_water": high_water,
        "truncated": truncated,
        "current_revisions": revisions,
        // Task 8: this channel's index_ready frames are bound to durable
        // PublicationReceipt certifications — `lexical_ready:true` means
        // the named Quill generation is committed, covered and replayable.
        "lexical_tracking": "bound",
        "protocol": PROTOCOL,
    }))
}

fn heartbeat_frame(
    high_water: Option<i64>,
    revisions: Option<&journal::JournalRevisions>,
) -> Vec<u8> {
    encode(&json!({
        "type": "heartbeat",
        "high_water": high_water,
        "current_revisions": revisions,
    }))
}

fn error_frame(kind: &str, message: &str) -> Vec<u8> {
    encode(&json!({
        "type": "error",
        "kind": kind,
        "message": message,
    }))
}

/// Best-effort single write for refusal frames on closing connections.
fn send_and_close(stream: &mut TcpStream, bytes: &[u8]) {
    let _ = stream.write_all(bytes);
    let _ = stream.flush();
}

/// Open the pump's own strictly read-only journal connection — a second
/// connection to `StrictArchive`'s, never shared state.
fn open_journal_reader(db: &Path) -> Option<Connection> {
    let text = db.to_str()?;
    let connection = open_with_flags(text, OpenFlags::SQLITE_OPEN_READ_ONLY).ok()?;
    connection.execute("PRAGMA query_only = ON").ok()?;
    connection.execute("PRAGMA busy_timeout = 250").ok()?;
    Some(connection)
}

/// One pump pass over every subscriber: bounded journal replay per
/// subscriber plus the periodic heartbeat (`heartbeat_due` is wall-clock —
/// a slow journal pass must not delay liveness). Journal read failures
/// skip that subscriber — the connection is refreshed on the
/// `REOPEN_TICKS` cadence and subscribers keep their watermarks, so a
/// transient busy window delays rather than drops events.
fn pump_pass(connection: Option<&Connection>, subscribers: &mut [Subscriber], heartbeat_due: bool) {
    let high_water = connection.and_then(|conn| journal::journal_high_water(conn).ok());
    let mut revisions = None;
    for subscriber in subscribers.iter_mut() {
        let mut delivered_events = false;
        if let Some(conn) = connection {
            match journal::changes_since(conn, subscriber.watermark, journal::CHANGES_PAGE_LIMIT) {
                Ok(page) => {
                    revisions = page.current_revisions.or(revisions);
                    delivered_events = !page.events.is_empty();
                    for event in &page.events {
                        subscriber.enqueue(event_frame(event, page.current_revisions.as_ref()));
                    }
                    subscriber.watermark = page.next_seq;
                    if page.truncated {
                        // The replay page already carried the reset_required
                        // event — deliver it, then close.
                        subscriber.close_after_flush = true;
                    }
                }
                Err(_) => continue,
            }
        }
        // A fresh subscriber whose first pass enqueued no event frames is
        // already caught up at the journal tail — its liveness cannot wait
        // for the next global heartbeat tick (post-reset resume must be
        // answered promptly, never by a silent phase wait).
        let first_liveness = subscriber.fresh && !delivered_events;
        if (heartbeat_due || first_liveness) && !subscriber.close_after_flush {
            subscriber.enqueue(heartbeat_frame(high_water, revisions.as_ref()));
        }
        subscriber.fresh = false;
        if subscriber.over_bound() {
            subscriber.overflow(high_water, revisions.as_ref());
        }
    }
}

fn pump_loop(db: &Path, inbox: Receiver<TcpStream>, shutdown: Arc<AtomicBool>, poll: Duration) {
    let heartbeat_interval = poll.saturating_mul(HEARTBEAT_TICKS as u32);
    let mut connection = None;
    let mut connection_age: u64 = 0;
    let mut handshakes: Vec<Handshake> = Vec::new();
    let mut subscribers: Vec<Subscriber> = Vec::new();
    let mut dirty = true;
    let mut last_heartbeat = Instant::now();
    let mut next_tick = Instant::now();

    while !shutdown.load(Ordering::Relaxed) {
        // 1. Drain freshly accepted streams into handshake progress.
        while let Ok(stream) = inbox.try_recv() {
            handshakes.push(Handshake::new(stream));
        }
        // 2. Progress every pending handshake nonblocking; completed lines
        //    keep their stream so classification can subscribe or refuse it.
        let mut ready: Vec<(TcpStream, String)> = Vec::new();
        let mut index = 0;
        while index < handshakes.len() {
            match handshakes[index].progress() {
                HandshakeOutcome::Pending => index += 1,
                HandshakeOutcome::Closed => {
                    handshakes.swap_remove(index);
                }
                HandshakeOutcome::Ready(line) => {
                    let handshake = handshakes.swap_remove(index);
                    ready.push((handshake.stream, line));
                }
            }
        }
        // 3. Classify completed handshakes.
        for (mut stream, line) in ready {
            match classify(&line) {
                Ok(ClientOp::Notify) => {
                    // Wakeup-only: the journal is re-read below; the frame
                    // carries no payload semantics.
                    dirty = true;
                }
                Ok(ClientOp::Subscribe { after_seq }) => {
                    if subscribers.len() >= MAX_SUBSCRIBERS {
                        send_and_close(
                            &mut stream,
                            &error_frame("subscriber_limit", "Too many event subscribers"),
                        );
                    } else {
                        // Atomic reconnect: the ack binds the watermark at
                        // subscribe time; the next pump pass replays
                        // changes_since(after_seq) — the same monotone seq
                        // space — so a commit racing the reconnect lands in
                        // the ack's high_water or the very next pass.
                        if connection.is_none() {
                            connection = open_journal_reader(db);
                        }
                        let mut subscriber = Subscriber::new(stream, after_seq);
                        subscriber.enqueue_pinned(subscribed_frame(after_seq, connection.as_ref()));
                        subscribers.push(subscriber);
                        dirty = true;
                    }
                }
                Err(error) => {
                    let kind = error
                        .downcast_ref::<super::protocol::Refusal>()
                        .map_or("invalid_request", |refusal| refusal.kind);
                    send_and_close(&mut stream, &error_frame(kind, &error.to_string()));
                }
            }
        }

        // 4. Pump pass when the journal may have moved.
        let now = Instant::now();
        if !subscribers.is_empty() && (dirty || now >= next_tick) {
            if connection.is_none() {
                connection = open_journal_reader(db);
            }
            let heartbeat_due = last_heartbeat.elapsed() >= heartbeat_interval;
            pump_pass(connection.as_ref(), &mut subscribers, heartbeat_due);
            if heartbeat_due {
                last_heartbeat = Instant::now();
            }
            dirty = false;
            next_tick = now + poll;
            connection_age += 1;
            if connection_age >= REOPEN_TICKS {
                connection = None; // un-pin a possibly replaced archive inode
                connection_age = 0;
            }
        }

        // 5. Flush subscriber queues; retire dead/closed streams.
        for subscriber in subscribers.iter_mut() {
            let wrote = subscriber.flush();
            subscriber.stalled_ticks = if wrote == 0 && !subscriber.pending.is_empty() {
                subscriber.stalled_ticks.saturating_add(1)
            } else {
                0
            };
            if subscriber.stalled_ticks > STALL_LIMIT_TICKS
                || (subscriber.close_after_flush && subscriber.pending.is_empty())
            {
                subscriber.dead = true;
            }
        }
        subscribers.retain(|subscriber| !subscriber.dead);

        // 6. Wait for the next accept or the poll deadline — the channel
        //    recv IS the wakeup: a notify or subscribe connection wakes the
        //    pump instantly instead of waiting out the tick, so this wait
        //    can be the full poll interval (bounded idle cost, no spin).
        match inbox.recv_timeout(poll) {
            Ok(stream) => handshakes.push(Handshake::new(stream)),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
    // Best-effort thread-local engine runtime teardown; the connection was
    // created on this thread so its driver lives here too.
    drop(connection);
    let _ = coding_agent_search::shutdown_thread_local_bridge_runtimes();
}
