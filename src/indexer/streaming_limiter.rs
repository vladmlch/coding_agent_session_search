#[cfg(not(test))]
use crate::dependency_pin_correlation::build_metadata::current as current_build_metadata;
#[cfg(test)]
use crate::dependency_pin_correlation::build_metadata::current_for_unit_test as current_build_metadata;
use super::*;
#[cfg(test)]
use std::iter::Peekable;

// =============================================================================
// Streaming Indexing (Opt 8.2)
// =============================================================================

/// Message type for streaming indexing channel.
///
/// Producers (connector scan threads) send batches of conversations through
/// the channel. The consumer (main indexing thread) receives and ingests them.
pub enum IndexMessage {
    /// The final bounded batch for an observed source. Earlier batches from
    /// this producer must succeed before its completion can become durable.
    SourceComplete {
        connector_name: &'static str,
        conversations: Vec<NormalizedConversation>,
        completion: crate::storage::sqlite::SourceIngestLedgerEntry,
        byte_reservation: usize,
    },
    /// A batch of conversations from a connector scan.
    Batch {
        /// Connector name (e.g., "claude", "codex")
        connector_name: &'static str,
        /// Scanned conversations
        conversations: Vec<NormalizedConversation>,
        /// Whether this connector was newly discovered
        is_discovered: bool,
        /// Message count in this batch (for stats)
        message_count: usize,
        /// Reserved text-byte budget for this batch that must be released after ingestion.
        byte_reservation: usize,
    },
    /// A scan error occurred (non-fatal, logged but continues)
    ScanError {
        connector_name: &'static str,
        error: String,
    },
    /// Producer has finished scanning
    Done {
        connector_name: &'static str,
        /// Time spent scanning this connector (ms)
        scan_ms: u64,
        /// Whether this connector was discovered even if it produced no batches
        is_discovered: bool,
        /// Whether every scan scope for this connector completed without error
        scan_succeeded: bool,
        /// Whether this connector skipped at least one source that was still
        /// being written. Its own watermark must remain behind for that source,
        /// but unrelated connectors may still advance safely.
        active_source_skipped: bool,
    },
}

/// Default channel buffer size for streaming indexing.
/// Balances memory usage with throughput - too small causes producer stalls,
/// too large defeats the purpose of backpressure.
pub(in crate::indexer) const STREAMING_CHANNEL_SIZE: usize = 32;

#[derive(Debug, Clone, Copy)]
pub(in crate::indexer) struct StreamingBatchLimits {
    pub(in crate::indexer) max_conversations: usize,
    pub(in crate::indexer) max_messages: usize,
    pub(in crate::indexer) max_chars: usize,
}

pub(in crate::indexer) const DEFAULT_STREAMING_BATCH_LIMITS: StreamingBatchLimits =
    StreamingBatchLimits {
        max_conversations: 64,
        max_messages: 2_000,
        max_chars: 4 * 1024 * 1024,
    };

/// Maximum total text bytes allowed across queued/in-flight streaming batches.
///
/// This preserves the intended memory envelope for normal batches while also
/// preventing oversized single conversations from multiplying across the queue.
pub(in crate::indexer) const STREAMING_MAX_BYTES_IN_FLIGHT: usize =
    STREAMING_CHANNEL_SIZE * DEFAULT_STREAMING_BATCH_LIMITS.max_chars;

#[derive(Debug)]
pub(in crate::indexer) struct StreamingByteLimiterState {
    pub(in crate::indexer) bytes_in_flight: usize,
    pub(in crate::indexer) closed: bool,
}

#[derive(Debug)]
pub(in crate::indexer) struct StreamingByteLimiter {
    pub(in crate::indexer) max_bytes_in_flight: AtomicUsize,
    pub(in crate::indexer) active_waiter_count: AtomicUsize,
    pub(in crate::indexer) state: Mutex<StreamingByteLimiterState>,
    pub(in crate::indexer) cv: Condvar,
}

#[derive(Debug)]
pub(in crate::indexer) struct ActiveStreamingByteWaiter<'a> {
    pub(in crate::indexer) active_waiter_count: &'a AtomicUsize,
}

impl<'a> ActiveStreamingByteWaiter<'a> {
    pub(in crate::indexer) fn new(active_waiter_count: &'a AtomicUsize) -> Self {
        active_waiter_count.fetch_add(1, Ordering::Relaxed);
        Self {
            active_waiter_count,
        }
    }
}

impl Drop for ActiveStreamingByteWaiter<'_> {
    fn drop(&mut self) {
        self.active_waiter_count.fetch_sub(1, Ordering::Relaxed);
    }
}

impl StreamingByteLimiter {
    pub(in crate::indexer) fn new(max_bytes_in_flight: usize) -> Self {
        debug_assert!(max_bytes_in_flight > 0);
        Self {
            max_bytes_in_flight: AtomicUsize::new(max_bytes_in_flight.max(1)),
            active_waiter_count: AtomicUsize::new(0),
            state: Mutex::new(StreamingByteLimiterState {
                bytes_in_flight: 0,
                closed: false,
            }),
            cv: Condvar::new(),
        }
    }

    pub(in crate::indexer) fn acquire(&self, requested_bytes: usize) -> Result<usize> {
        self.acquire_with_wait(requested_bytes)
            .map(|(reservation, _, _)| reservation)
    }

    pub(in crate::indexer) fn acquire_with_wait(
        &self,
        requested_bytes: usize,
    ) -> Result<(usize, Duration, bool)> {
        if requested_bytes == 0 {
            return Ok((0, Duration::ZERO, false));
        }

        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut waited = false;
        let mut active_waiter = None;
        let wait_started = Instant::now();
        loop {
            if state.closed {
                return Err(anyhow::anyhow!(
                    "streaming byte limiter closed while waiting for capacity"
                ));
            }

            let max_bytes_in_flight = self.max_bytes_in_flight.load(Ordering::Acquire).max(1);
            let reservation = requested_bytes.min(max_bytes_in_flight);
            if state.bytes_in_flight.saturating_add(reservation) <= max_bytes_in_flight {
                state.bytes_in_flight += reservation;
                let wait_duration = if waited {
                    wait_started.elapsed()
                } else {
                    Duration::default()
                };
                return Ok((reservation, wait_duration, waited));
            }

            waited = true;
            active_waiter
                .get_or_insert_with(|| ActiveStreamingByteWaiter::new(&self.active_waiter_count));
            // #288: bounded wait so a parked pipeline periodically logs the
            // blocked predicate (bytes in flight vs cap) instead of vanishing
            // into an unbounded condvar park. Pure observability — the loop
            // always re-arms and continues waiting.
            let (next_state, timeout_result) = self
                .cv
                .wait_timeout(state, LEXICAL_PIPELINE_BLOCKED_WAIT_LOG_INTERVAL)
                .unwrap_or_else(|e| e.into_inner());
            state = next_state;
            if timeout_result.timed_out() {
                let waited_ms = wait_started.elapsed().as_millis() as u64;
                let bytes_in_flight = state.bytes_in_flight;
                let max_bytes_in_flight = self.max_bytes_in_flight.load(Ordering::Acquire);
                let active_waiters = self.active_waiter_count.load(Ordering::Relaxed);
                if lexical_pipeline_trace_enabled() {
                    tracing::info!(
                        requested_bytes,
                        bytes_in_flight,
                        max_bytes_in_flight,
                        active_waiters,
                        waited_ms,
                        "CASS_PIPELINE_TRACE streaming byte budget still parked waiting for capacity"
                    );
                } else {
                    tracing::debug!(
                        requested_bytes,
                        bytes_in_flight,
                        max_bytes_in_flight,
                        active_waiters,
                        waited_ms,
                        "streaming byte budget still parked waiting for capacity"
                    );
                }
            }
        }
    }

    pub(in crate::indexer) fn release(&self, reserved_bytes: usize) {
        if reserved_bytes == 0 {
            return;
        }

        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.bytes_in_flight = state.bytes_in_flight.saturating_sub(reserved_bytes);
        self.cv.notify_all();
    }

    pub(in crate::indexer) fn bytes_in_flight(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .bytes_in_flight
    }

    /// Whether any thread is currently parked inside [`Self::acquire_with_wait`].
    ///
    /// Take the predicate mutex before observing the waiter count. A waiter
    /// registers while holding this same mutex and `Condvar::wait_timeout`
    /// releases it, so the lock handoff makes the cross-thread observation
    /// authoritative. The starvation flush is a liveness mechanism and must
    /// not depend on eventual visibility of an otherwise relaxed atomic poll.
    pub(in crate::indexer) fn has_active_waiters(&self) -> bool {
        let _state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        self.active_waiter_count.load(Ordering::Relaxed) > 0
    }

    pub(in crate::indexer) fn update_max_bytes_in_flight(&self, max_bytes_in_flight: usize) {
        // [coding_agent_session_search-wxsy8] Acquire the same state
        // lock that `acquire_with_wait` uses for its predicate
        // evaluation BEFORE updating the budget and notifying. Without
        // this, the standard condvar lost-wakeup race fires:
        //
        //   T1 (waiter) holds state-lock, reads max via load(Acquire),
        //                evaluates predicate (no fit), drops lock,
        //                ↘  notify_all here lands on zero subscribers
        //   T2 (updater) stores new max atomically + notify_all
        //                   (waiter not yet parked)
        //   T1 enters cv.wait — parks indefinitely; the higher budget
        //   would have admitted the request, but no one will notify
        //   again until release().
        //
        // Standard correct condvar protocol requires the predicate
        // update to happen INSIDE the lock that the waiter holds
        // during predicate evaluation. The lock is released before
        // the parked waiter is woken, so cost is bounded by the
        // critical section (a single atomic store).
        let _state_guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        self.max_bytes_in_flight
            .store(max_bytes_in_flight.max(1), Ordering::Release);
        self.cv.notify_all();
    }

    pub(in crate::indexer) fn max_bytes_in_flight(&self) -> usize {
        self.max_bytes_in_flight.load(Ordering::Acquire)
    }

    pub(in crate::indexer) fn active_waiter_count(&self) -> usize {
        self.active_waiter_count.load(Ordering::Relaxed)
    }

    pub(in crate::indexer) fn close(&self) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.closed = true;
        self.cv.notify_all();
    }

    pub(in crate::indexer) fn is_closed(&self) -> bool {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).closed
    }
}

#[derive(Debug)]
pub(in crate::indexer) struct LexicalRebuildReservationOrderState {
    pub(in crate::indexer) next_sequence: u64,
    pub(in crate::indexer) closed: bool,
}

#[derive(Debug)]
pub(in crate::indexer) struct LexicalRebuildReservationOrder {
    pub(in crate::indexer) state: Mutex<LexicalRebuildReservationOrderState>,
    pub(in crate::indexer) cv: Condvar,
}

impl LexicalRebuildReservationOrder {
    pub(in crate::indexer) fn new() -> Self {
        Self {
            state: Mutex::new(LexicalRebuildReservationOrderState {
                next_sequence: 0,
                closed: false,
            }),
            cv: Condvar::new(),
        }
    }

    pub(in crate::indexer) fn wait_for_turn(&self, sequence: u64) -> Result<()> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let wait_started = Instant::now();
        loop {
            if state.closed {
                return Err(anyhow::anyhow!(
                    "lexical rebuild reservation order closed while waiting for sequence {}",
                    sequence
                ));
            }
            if sequence < state.next_sequence {
                return Err(anyhow::anyhow!(
                    "lexical rebuild page sequence {} tried to reserve after sequence {}",
                    sequence,
                    state.next_sequence
                ));
            }
            if sequence == state.next_sequence {
                return Ok(());
            }
            // #288: bounded wait so a parked pipeline periodically logs the
            // blocked predicate instead of vanishing into an unbounded
            // condvar park. Pure observability — the loop always re-arms.
            let (next_state, timeout_result) = self
                .cv
                .wait_timeout(state, LEXICAL_PIPELINE_BLOCKED_WAIT_LOG_INTERVAL)
                .unwrap_or_else(|e| e.into_inner());
            state = next_state;
            if timeout_result.timed_out() {
                let waited_ms = wait_started.elapsed().as_millis() as u64;
                if lexical_pipeline_trace_enabled() {
                    tracing::info!(
                        sequence,
                        holds_turn = state.next_sequence,
                        waited_ms,
                        "CASS_PIPELINE_TRACE lexical rebuild page still parked waiting for ordered reservation turn"
                    );
                } else {
                    tracing::debug!(
                        sequence,
                        holds_turn = state.next_sequence,
                        waited_ms,
                        "lexical rebuild page still parked waiting for ordered reservation turn"
                    );
                }
            }
        }
    }

    pub(in crate::indexer) fn next_sequence(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .next_sequence
    }

    pub(in crate::indexer) fn finish_turn(&self, sequence: u64) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.next_sequence == sequence {
            state.next_sequence = state.next_sequence.saturating_add(1);
        }
        self.cv.notify_all();
    }

    pub(in crate::indexer) fn close(&self) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.closed = true;
        self.cv.notify_all();
    }

    pub(in crate::indexer) fn is_closed(&self) -> bool {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).closed
    }
}

pub(in crate::indexer) fn acquire_ordered_lexical_rebuild_page_budget(
    reservation_order: &LexicalRebuildReservationOrder,
    flow_limiter: &StreamingByteLimiter,
    producer_telemetry: &LexicalRebuildProducerTelemetry,
    sequence: u64,
    page_working_set_bytes: usize,
) -> Result<(usize, Duration, bool)> {
    producer_telemetry.set_producer_state(LexicalRebuildProducerParkSite::WaitingTurn);
    producer_telemetry.record_reservation_next_sequence(reservation_order.next_sequence());
    let turn = reservation_order.wait_for_turn(sequence);
    producer_telemetry.record_reservation_next_sequence(reservation_order.next_sequence());
    if let Err(err) = turn {
        producer_telemetry.set_producer_state(LexicalRebuildProducerParkSite::Idle);
        return Err(err);
    }
    producer_telemetry.set_producer_state(LexicalRebuildProducerParkSite::WaitingBudget);
    let acquired = match flow_limiter.acquire_with_wait(page_working_set_bytes) {
        Ok(acquired) => {
            reservation_order.finish_turn(sequence);
            producer_telemetry.record_reservation_next_sequence(reservation_order.next_sequence());
            Ok(acquired)
        }
        Err(err) => {
            reservation_order.close();
            Err(err)
        }
    };
    producer_telemetry.set_producer_state(LexicalRebuildProducerParkSite::Idle);
    acquired
}

/// Conservative resident-byte reservation for one lexical page while it is
/// fetched and normalized into Quill packets.
///
/// Message text moves through the owned replay path without cloning, but the
/// input, contract, and grouped-message vector allocations overlap while each
/// page is transformed. The retained packet also has substantial fixed
/// structure per message and per conversation, including its SmallVec inline
/// message slab. Charging content alone let pages with many short messages pass
/// a nominal byte cap while their real object graphs multiplied RSS (#320).
pub(in crate::indexer) fn lexical_rebuild_page_working_set_reservation_bytes(
    content_bytes: usize,
    message_count: usize,
    conversation_count: usize,
) -> usize {
    let content_peak = content_bytes;
    let per_message = std::mem::size_of::<crate::model::types::Message>()
        .saturating_add(std::mem::size_of::<
            crate::model::conversation_packet::ConversationPacketMessage,
        >())
        .saturating_add(std::mem::size_of::<
            crate::storage::sqlite::LexicalRebuildGroupedMessageRow,
        >())
        .saturating_add(2usize.saturating_mul(std::mem::size_of::<usize>()));
    let per_conversation =
        std::mem::size_of::<crate::storage::sqlite::LexicalRebuildConversationRow>()
            .saturating_add(std::mem::size_of::<LexicalRebuildConversationPacket>())
            .saturating_add(std::mem::size_of::<ConversationPacket>());

    content_peak
        .saturating_add(message_count.saturating_mul(per_message))
        .saturating_add(conversation_count.saturating_mul(per_conversation))
        .max(1)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::indexer) struct StreamingConversationFootprint {
    pub(in crate::indexer) message_count: usize,
    pub(in crate::indexer) content_bytes: usize,
    pub(in crate::indexer) retained_bytes: usize,
}

/// Heap bytes retained by a `serde_json::Value`, excluding the inline root
/// enum already counted by its owner. Allocator bookkeeping is not observable
/// without unsafe allocator APIs, so object entries include the owned key/value
/// pair that must exist for each node and all recursively owned allocations.
pub(in crate::indexer) fn json_heap_bytes(value: &serde_json::Value) -> usize {
    match value {
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => 0,
        serde_json::Value::String(text) => text.capacity(),
        serde_json::Value::Array(values) => values
            .capacity()
            .saturating_mul(std::mem::size_of::<serde_json::Value>())
            .saturating_add(
                values
                    .iter()
                    .map(json_heap_bytes)
                    .fold(0usize, usize::saturating_add),
            ),
        serde_json::Value::Object(values) => values
            .len()
            .saturating_mul(std::mem::size_of::<(String, serde_json::Value)>())
            .saturating_add(values.iter().fold(0usize, |bytes, (key, value)| {
                bytes
                    .saturating_add(key.capacity())
                    .saturating_add(json_heap_bytes(value))
            })),
    }
}

pub(in crate::indexer) fn optional_string_heap_bytes(value: Option<&String>) -> usize {
    value.map_or(0, String::capacity)
}

pub(in crate::indexer) fn path_heap_bytes(path: &Path) -> usize {
    path.as_os_str().as_encoded_bytes().len()
}

pub(in crate::indexer) fn normalized_snippet_heap_bytes(snippet: &NormalizedSnippet) -> usize {
    snippet
        .file_path
        .as_deref()
        .map_or(0, path_heap_bytes)
        .saturating_add(optional_string_heap_bytes(snippet.language.as_ref()))
        .saturating_add(optional_string_heap_bytes(snippet.snippet_text.as_ref()))
}

/// Estimate the complete normalized object graph retained in the producer
/// buffer/channel, not just message content. This is deliberately based on
/// actual vector/string capacities where the public APIs expose them. It also
/// charges connector metadata, snippets, and tool invocation arguments, which
/// can dominate tiny or empty message bodies (#320).
pub(in crate::indexer) fn conversation_batch_footprint(
    conv: &NormalizedConversation,
) -> StreamingConversationFootprint {
    let message_count = conv.messages.len();
    let content_bytes = conv
        .messages
        .iter()
        .map(|message| message.content.len())
        .fold(0usize, usize::saturating_add);
    let mut retained_bytes = std::mem::size_of_val(conv)
        .saturating_add(conv.agent_slug.capacity())
        .saturating_add(optional_string_heap_bytes(conv.external_id.as_ref()))
        .saturating_add(optional_string_heap_bytes(conv.title.as_ref()))
        .saturating_add(conv.workspace.as_deref().map_or(0, path_heap_bytes))
        .saturating_add(path_heap_bytes(&conv.source_path))
        .saturating_add(json_heap_bytes(&conv.metadata))
        .saturating_add(
            conv.messages
                .capacity()
                .saturating_mul(std::mem::size_of::<NormalizedMessage>()),
        );

    for message in &conv.messages {
        retained_bytes =
            retained_bytes
                .saturating_add(message.role.capacity())
                .saturating_add(optional_string_heap_bytes(message.author.as_ref()))
                .saturating_add(message.content.capacity())
                .saturating_add(json_heap_bytes(&message.extra))
                .saturating_add(
                    message
                        .snippets
                        .capacity()
                        .saturating_mul(std::mem::size_of::<NormalizedSnippet>()),
                )
                .saturating_add(
                    message
                        .snippets
                        .iter()
                        .map(normalized_snippet_heap_bytes)
                        .fold(0usize, usize::saturating_add),
                )
                .saturating_add(message.invocations.capacity().saturating_mul(
                    std::mem::size_of::<franken_agent_detection::NormalizedInvocation>(),
                ));
        for invocation in &message.invocations {
            retained_bytes = retained_bytes
                .saturating_add(invocation.kind.capacity())
                .saturating_add(invocation.name.capacity())
                .saturating_add(optional_string_heap_bytes(invocation.raw_name.as_ref()))
                .saturating_add(optional_string_heap_bytes(invocation.call_id.as_ref()))
                .saturating_add(invocation.arguments.as_ref().map_or(0, json_heap_bytes));
        }
    }

    StreamingConversationFootprint {
        message_count,
        content_bytes,
        retained_bytes: retained_bytes.max(content_bytes),
    }
}

#[cfg(test)]
pub(in crate::indexer) fn next_streaming_batch(
    conversations: &mut Peekable<std::vec::IntoIter<NormalizedConversation>>,
    limits: StreamingBatchLimits,
) -> Option<(Vec<NormalizedConversation>, usize)> {
    let first = conversations.next()?;
    let first_footprint = conversation_batch_footprint(&first);
    let mut batch = vec![first];
    let mut total_messages = first_footprint.message_count;
    let mut total_content_bytes = first_footprint.content_bytes;

    while let Some(next) = conversations.peek() {
        let next_footprint = conversation_batch_footprint(next);
        let would_exceed_limits = batch.len() >= limits.max_conversations
            || total_messages.saturating_add(next_footprint.message_count) > limits.max_messages
            || total_content_bytes.saturating_add(next_footprint.content_bytes) > limits.max_chars;
        if would_exceed_limits {
            break;
        }

        let conv = conversations
            .next()
            .expect("peek indicated another conversation existed");
        total_messages = total_messages.saturating_add(next_footprint.message_count);
        total_content_bytes = total_content_bytes.saturating_add(next_footprint.content_bytes);
        batch.push(conv);
    }

    Some((batch, total_messages))
}

pub(in crate::indexer) struct StreamingBatchSender<'a> {
    pub(in crate::indexer) retain_final: bool,
    pub(in crate::indexer) tx: &'a Sender<IndexMessage>,
    pub(in crate::indexer) flow_limiter: Arc<StreamingByteLimiter>,
    pub(in crate::indexer) connector_name: &'static str,
    pub(in crate::indexer) next_batch_is_discovered: bool,
    pub(in crate::indexer) conversations: Vec<NormalizedConversation>,
    pub(in crate::indexer) message_count: usize,
    pub(in crate::indexer) content_bytes: usize,
    pub(in crate::indexer) retained_bytes: usize,
    pub(in crate::indexer) byte_reservation: usize,
    /// Sources skipped because the source ledger shows them unchanged, for
    /// the scope's ingest report.
    pub(in crate::indexer) reused_sources: Vec<PathBuf>,
}

pub(in crate::indexer) fn remember_discovered_connector(
    discovered_names: &mut Vec<String>,
    connector_name: &'static str,
) {
    if !discovered_names.iter().any(|name| name == connector_name) {
        discovered_names.push(connector_name.to_string());
    }
}

pub(in crate::indexer) fn source_ledger_key(
    source: &DiscoveredSourceFile,
    ctx: &ScanContext,
) -> Option<String> {
    // dunce: on Windows the plain spelling (C:\...) a scan records, not the
    // verbatim \\?\C:\... std::fs::canonicalize returns (9v9xi).
    let canonical =
        dunce::canonicalize(&source.source_path).unwrap_or_else(|_| source.source_path.clone());
    let mappings = ctx
        .scan_roots
        .iter()
        .map(|root| (&root.path, &root.workspace_rewrites))
        .collect::<Vec<_>>();
    // Path's JSON serializer rejects non-Unicode names. Missing identity
    // evidence must disable reuse, not panic before a source can be parsed or
    // collapse distinct native paths through lossy replacement characters.
    // Keep the Value intermediate so existing workspace-mapping object order
    // (and therefore every already-persisted key) stays byte-for-byte stable.
    let identity = serde_json::to_value((
        &source.provider_slug,
        &source.scan_root,
        &canonical,
        &source.origin.source_id,
        &source.origin.kind,
        &source.origin.host,
        mappings,
    ))
    .ok()?;
    let encoded = serde_json::to_vec(&identity).ok()?;
    Some(format!(
        "source_ingest_v1:{}",
        blake3::hash(&encoded).to_hex()
    ))
}

pub(in crate::indexer) fn source_file_observation(path: &Path) -> Option<serde_json::Value> {
    crate::connectors::source_dependencies::observation::file_observation(path)
}

pub(in crate::indexer) fn source_ledger_matches_with_contract(
    observation: &str,
    source: &DiscoveredSourceFile,
    producer_contract: &str,
) -> bool {
    crate::connectors::source_dependencies::observation::ledger_matches(
        observation,
        &source.source_path,
        source_folder_is_dependency(source),
        producer_contract,
    )
}

#[cfg(test)]
pub(in crate::indexer) fn source_ledger_matches(
    observation: &str,
    source: &DiscoveredSourceFile,
) -> bool {
    source_ledger_matches_with_contract(
        observation,
        source,
        current_build_metadata().ingest_contract,
    )
}

/// A connector capability, not an inference from this scan's sidecar list.
pub(in crate::indexer) fn source_folder_is_dependency(source: &DiscoveredSourceFile) -> bool {
    crate::connectors::source_dependencies::source_dependency_policy(&source.provider_slug)
        .observes_parent_directory()
}

pub(in crate::indexer) fn source_parent_observation(
    source: &DiscoveredSourceFile,
) -> Option<serde_json::Value> {
    if source_folder_is_dependency(source) {
        source
            .source_path
            .parent()
            .and_then(source_file_observation)
    } else {
        None
    }
}

/// Capture the reconstruction dependencies after a source finishes parsing.
/// Folder growth must not withhold a self-contained source's durable marker,
/// but changes to the primary or any declared sidecar must still withhold it.
pub(in crate::indexer) fn source_observations_after_scan(
    source: &DiscoveredSourceFile,
    sidecars: &[DiscoveredSourceFile],
    primary_before: Option<&serde_json::Value>,
    parent_before: Option<&serde_json::Value>,
    dependencies_before: &HashMap<PathBuf, serde_json::Value>,
) -> Option<(serde_json::Value, Vec<serde_json::Value>)> {
    let primary = source_file_observation(&source.source_path)?;
    if primary["absent"].as_bool() == Some(true)
        || Some(&primary) != primary_before
        || source.fs_metadata_changed()
    {
        return None;
    }
    let mut dependencies = Vec::new();
    if source_folder_is_dependency(source) {
        let parent = source_parent_observation(source)?;
        if Some(&parent) != parent_before {
            return None;
        }
        dependencies.push(parent);
    }
    for sidecar in sidecars {
        if sidecar.fs_metadata_changed() {
            return None;
        }
        let observation = source_file_observation(&sidecar.source_path)?;
        if dependencies_before.get(&sidecar.source_path) != Some(&observation) {
            return None;
        }
        dependencies.push(observation);
    }
    Some((primary, dependencies))
}

/// Keep only the final bounded batch until the connector certifies a source.
/// Filtering or a changing reconstruction dependency withholds the marker.
pub(in crate::indexer) fn scan_with_durable_source_boundaries(
    connector: &dyn Connector,
    ctx: &ScanContext,
    config: &StreamingProducerConfig,
    sender: &mut StreamingBatchSender<'_>,
    mut prepare: impl FnMut(NormalizedConversation) -> Result<Option<NormalizedConversation>>,
) -> Result<()> {
    let build_meta = current_build_metadata();
    let producer_contract =
        WorkspaceExclusions::from_env()?.bind_contract(build_meta.ingest_contract);
    if !connector.supports_source_boundaries() {
        return connector.scan_with_callback(ctx, &mut |conversation| {
            if let Some(conversation) = prepare(conversation)? {
                sender.push(conversation)?;
            }
            Ok(())
        });
    }
    let mut ctx = ctx.clone();
    ctx.since_ts = None;
    sender.retain_final = true;
    let sender = std::cell::RefCell::new(sender);
    let filtered = std::cell::Cell::new(false);
    let before = std::cell::RefCell::new(None);
    let primary_before = std::cell::RefCell::new(None);
    // Sidecar-aware connectors keep the discovery-time guard: a second full
    // inventory proves the parent and every optional sidecar were observed
    // before the connector's own discovery. Self-contained connectors skip that
    // inventory; their explicit dependencies are captured per offer below, and
    // a completion sidecar that was never offered still withholds the marker.
    let observes_parent_directory =
        crate::connectors::source_dependencies::source_dependency_policy(
            sender.borrow().connector_name,
        )
        .observes_parent_directory();
    let dependencies_before = if observes_parent_directory {
        crate::connectors::source_dependencies::observation::DependencyObservations::capture(
            connector
                .discover_source_files(&ctx)?
                .into_iter()
                .map(|source| source.source_path),
        )
    } else {
        Default::default()
    };
    // A self-contained connector may still declare explicit reconstruction
    // dependencies: a merged Codex thread's continuation segments are offered
    // (and observed here) before the connector reads any of them. Capture each
    // offered segment's first observation so the durable marker binds exactly
    // the thread's own files. The implicit parent stays unobserved, so an
    // unrelated new sibling never invalidates a self-contained source's reuse.
    let sidecars_before = std::cell::RefCell::new(HashMap::<PathBuf, serde_json::Value>::new());
    let flush_error = std::cell::RefCell::new(None);
    let mut should_scan = |source: &DiscoveredSourceFile| {
        if config
            .progress
            .as_ref()
            .is_some_and(|progress| progress.stop_requested())
        {
            return false;
        }
        if let Err(error) = sender.borrow_mut().flush() {
            *flush_error.borrow_mut() = Some(error);
            return false;
        }
        // Some connectors exclude complete source files before this hook. An
        // unrelated exclusion does not make an admitted transcript partial.
        // Keep conservative behavior for unknown/partial-source filters; the
        // prepare callback below still withholds any actually filtered source.
        filtered.set(
            scan_path_exclusions_active()
                && !crate::connectors::source_dependencies::can_reuse_with_path_exclusions(
                    sender.borrow().connector_name,
                    &source.provider_slug,
                ),
        );
        // The connector may already have selected its sidecars by this hook.
        // Use the pre-discovery parent, not a later snapshot that could bless
        // an untracked sidecar appearing between discovery and admission.
        *before.borrow_mut() = dependencies_before.parent_for(&source.source_path).cloned();
        let primary_observation = source_file_observation(&source.source_path);
        if !observes_parent_directory && let Some(observation) = &primary_observation {
            sidecars_before
                .borrow_mut()
                .entry(source.source_path.clone())
                .or_insert_with(|| observation.clone());
        }
        *primary_before.borrow_mut() = primary_observation;
        let skip = build_meta.ingest_reuse
            && !filtered.get()
            && source_ledger_key(source, &ctx)
                .and_then(|key| config.source_ledger.get(&key))
                .is_some_and(|saved| {
                    source_ledger_matches_with_contract(saved, source, &producer_contract)
                });
        tracing::debug!(connector=%source.provider_slug, skipped=skip,"source_ingest_observation");
        if skip {
            sender
                .borrow_mut()
                .reused_sources
                .push(source.source_path.clone());
        }
        !skip
    };
    let mut complete = |completion: &franken_agent_detection::connectors::SourceCompletion| {
        if filtered.get()
            || completion.conversations_emitted == 0
            || completion.source.fs_metadata_changed()
            || completion
                .required_sidecars
                .iter()
                .any(DiscoveredSourceFile::fs_metadata_changed)
        {
            return sender.borrow_mut().flush();
        }
        let self_contained_sidecars = sidecars_before.borrow();
        let dependency_observations = if observes_parent_directory {
            &dependencies_before.files
        } else {
            &self_contained_sidecars
        };
        let Some((primary, dependencies)) = source_observations_after_scan(
            &completion.source,
            &completion.required_sidecars,
            primary_before.borrow().as_ref(),
            before.borrow().as_ref(),
            dependency_observations,
        ) else {
            return sender.borrow_mut().flush();
        };
        let Some(key) = source_ledger_key(&completion.source, &ctx) else {
            return sender.borrow_mut().flush();
        };
        let entry = crate::storage::sqlite::SourceIngestLedgerEntry {
            key,
            observation: serde_json::json!({"primary":primary,"dependencies":dependencies,
                "producer_contract":producer_contract})
            .to_string(),
        };
        sender.borrow_mut().complete_source(entry)
    };
    let mut hooks = franken_agent_detection::connectors::SourceScanHooks {
        should_scan_source: Some(&mut should_scan),
        on_source_complete: Some(&mut complete),
    };
    connector.scan_with_source_boundaries(&ctx, &mut hooks, &mut |conversation| {
        if config
            .progress
            .as_ref()
            .is_some_and(|progress| progress.stop_requested())
        {
            anyhow::bail!("indexing interrupted at source boundary");
        }
        match prepare(conversation)? {
            Some(conversation) => sender.borrow_mut().push(conversation),
            None => {
                filtered.set(true);
                Ok(())
            }
        }
    })?;
    if let Some(error) = flush_error.into_inner() {
        return Err(error);
    }
    Ok(())
}

impl<'a> StreamingBatchSender<'a> {
    pub(in crate::indexer) fn new(
        tx: &'a Sender<IndexMessage>,
        flow_limiter: Arc<StreamingByteLimiter>,
        connector_name: &'static str,
        is_discovered: bool,
    ) -> Self {
        Self {
            retain_final: false,
            tx,
            flow_limiter,
            connector_name,
            next_batch_is_discovered: is_discovered,
            conversations: Vec::new(),
            message_count: 0,
            content_bytes: 0,
            retained_bytes: 0,
            byte_reservation: 0,
            reused_sources: Vec::new(),
        }
    }

    pub(in crate::indexer) fn push(&mut self, conversation: NormalizedConversation) -> Result<()> {
        let footprint = conversation_batch_footprint(&conversation);
        let would_exceed_limits = !self.conversations.is_empty()
            && (self.conversations.len() >= DEFAULT_STREAMING_BATCH_LIMITS.max_conversations
                || self.message_count.saturating_add(footprint.message_count)
                    > DEFAULT_STREAMING_BATCH_LIMITS.max_messages
                || self.content_bytes.saturating_add(footprint.content_bytes)
                    > DEFAULT_STREAMING_BATCH_LIMITS.max_chars
                || self.retained_bytes.saturating_add(footprint.retained_bytes)
                    > DEFAULT_STREAMING_BATCH_LIMITS.max_chars);
        if would_exceed_limits {
            self.flush()?;
        }

        let byte_reservation = self
            .flow_limiter
            .acquire(footprint.retained_bytes)
            .map_err(|_| {
                anyhow::Error::new(StreamingConsumerDisconnected {
                    connector_name: self.connector_name,
                })
            })?;
        self.message_count = self.message_count.saturating_add(footprint.message_count);
        self.content_bytes = self.content_bytes.saturating_add(footprint.content_bytes);
        self.retained_bytes = self.retained_bytes.saturating_add(footprint.retained_bytes);
        self.byte_reservation = self.byte_reservation.saturating_add(byte_reservation);
        self.conversations.push(conversation);

        // Keep the final conversation until the next push or source-complete
        // event. Even an oversized singleton must share its final transaction
        // with the source observation; the limiter already admits one giant.
        if !self.retain_final
            && self.conversations.len() == 1
            && (self.message_count > DEFAULT_STREAMING_BATCH_LIMITS.max_messages
                || self.content_bytes > DEFAULT_STREAMING_BATCH_LIMITS.max_chars
                || self.retained_bytes > DEFAULT_STREAMING_BATCH_LIMITS.max_chars)
        {
            self.flush()?;
        }

        Ok(())
    }

    pub(in crate::indexer) fn flush(&mut self) -> Result<()> {
        if self.conversations.is_empty() {
            if self.byte_reservation > 0 {
                self.flow_limiter.release(self.byte_reservation);
                self.byte_reservation = 0;
            }
            self.message_count = 0;
            self.content_bytes = 0;
            self.retained_bytes = 0;
            return Ok(());
        }

        let message_count = self.message_count;
        let byte_reservation = self.byte_reservation;
        let conversations = std::mem::take(&mut self.conversations);
        if let Err(send_error) = self.tx.send(IndexMessage::Batch {
            connector_name: self.connector_name,
            conversations,
            is_discovered: self.next_batch_is_discovered,
            message_count,
            byte_reservation,
        }) {
            // `send_error` owns the rejected batch. Free it before returning
            // that batch's capacity to other connector producers.
            drop(send_error);
            self.flow_limiter.release(byte_reservation);
            self.message_count = 0;
            self.content_bytes = 0;
            self.retained_bytes = 0;
            self.byte_reservation = 0;
            return Err(anyhow::Error::new(StreamingConsumerDisconnected {
                connector_name: self.connector_name,
            }));
        }
        self.message_count = 0;
        self.content_bytes = 0;
        self.retained_bytes = 0;
        self.byte_reservation = 0;
        self.next_batch_is_discovered = false;
        Ok(())
    }

    pub(in crate::indexer) fn complete_source(
        &mut self,
        completion: crate::storage::sqlite::SourceIngestLedgerEntry,
    ) -> Result<()> {
        if self.conversations.is_empty() {
            return Ok(());
        }
        let byte_reservation = std::mem::take(&mut self.byte_reservation);
        let conversations = std::mem::take(&mut self.conversations);
        self.message_count = 0;
        self.content_bytes = 0;
        self.retained_bytes = 0;
        if let Err(error) = self.tx.send(IndexMessage::SourceComplete {
            connector_name: self.connector_name,
            conversations,
            completion,
            byte_reservation,
        }) {
            drop(error);
            self.flow_limiter.release(byte_reservation);
            return Err(anyhow::Error::new(StreamingConsumerDisconnected {
                connector_name: self.connector_name,
            }));
        }
        self.next_batch_is_discovered = false;
        Ok(())
    }
}

impl Drop for StreamingBatchSender<'_> {
    fn drop(&mut self) {
        if self.byte_reservation > 0 {
            // Unflushed conversations still own the bytes represented by this
            // reservation. Drop them before publishing capacity to peers.
            self.conversations.clear();
            self.flow_limiter.release(self.byte_reservation);
            self.byte_reservation = 0;
        }
    }
}
