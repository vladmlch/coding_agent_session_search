//! Stable steps/turns/tools projection anchored to canonical messages
//! (spec decisions 4/5, plan Task 8).
//!
//! `project_session_structure` folds the normalized packet's per-message
//! `extra_json` evidence (verbatim provider records the connector preserved)
//! plus the verified [`crate::viewer_extensions::facts::ToolCallFact`] links from the facts pass into a
//! deterministic [`StructuralPublication`]: surviving projection steps with
//! per-message provenance, emitted turns, and call→result links. NO full
//! `SessionTrajectory` blob and no message-body copies are persisted — every
//! row is a structural pointer (`step_index`, `turn_index`, scope, canonical
//! `messages.idx`/`id`) plus derived-only part keys. `get_tool_output`
//! reconstructs output text from the canonical message at the anchor.
//!
//! Oracle alignment (`viewer/domain/passes/normalize/*`,
//! `viewer/backends/cass/canonical/{mappings,projection}.py`):
//!
//! * Physical uuid dedup keeps the first identical record and unions
//!   provenance; a uuid whose records DISAGREE is `conflicting_event` —
//!   dropped entirely (CASS facts semantics: conflict is unknown, never
//!   first-wins merged). Codex `response_item` payload-`id` replays dedup
//!   the same way.
//! * Claude assistant records sharing `(requestId|message.id, message.id)`
//!   merge into ONE logical step regardless of interleaved foreign records;
//!   merge/summary boundary records or a foreign `sessionId` between
//!   members split the group. Identical `text`/`thinking` blocks and
//!   same-`id` `tool_use` blocks dedup inside the merged step.
//! * A pure tool-result record produces NO step of its own; its verified
//!   pairing lands in `viewer_tool_calls.result_*` and the step that
//!   recorded the call gains a `scope='result'` member row — evidence-bound
//!   (the result record literally names the call id), richer than the
//!   oracle's lookahead bookkeeping and still never positional.
//! * Turns follow `pass_project_chat_turns` structure: a user step, a
//!   system step and each boundary/approval step are their own turn;
//!   consecutive assistant/error steps accumulate into one `ai` turn.
//!   `turn_index` is the 0-based emitted ordinal (native `turnIndex`).
//!   Steps that emit no turn (empty user/system prompts) keep `turn_index`
//!   NULL rather than guessing.
//! * Records the canonical mappings leave unmapped (unknown roles,
//!   attachment/control envelopes, `session_meta`/`turn_context`-style
//!   noise) produce no step — never coerced.

mod classify;
mod generation;
mod parts;
mod publish;

pub use generation::{
    GenerationPublishOutcome, IngestGeneration, generation_for_canonical,
    generation_markers_visible, publish_ingest_generation, publish_ingest_generation_best_effort,
    publish_ingest_generation_in_tx, publish_ingest_generations_batch_best_effort,
};
#[cfg(test)]
pub(crate) use generation::{
    test_publication_commit_calls, test_reset_publication_commit_calls,
    test_set_generation_crash_probe, test_set_generation_fail_probe,
};
pub use parts::project_session_structure;
pub use publish::{
    publish_session_structure, publish_structure_for_canonical_batch_best_effort,
    publish_structure_for_canonical_best_effort, structure_for_canonical,
};

/// Bump when projection semantics change so persisted rows re-derive on
/// the next ingest pass (targeted recomputation via the
/// `viewer_session_projection` marker) instead of silently going stale.
///
/// v2: the `{"cass":{…}}` single-key skeleton no longer degrades cursor
/// records to flattened text — the ingest extra-compaction that wrote it
/// was codex-only (`should_compact_connector_extra`, removed 2026-11-08;
/// codex rows indexed before that still carry the skeleton), so for every
/// other provider that shape is the ingest merge envelope carrying lifted
/// `invocations`, and the block inventory now derives one tool_call part
/// per recorded call.
pub const PROJECTION_VERSION: i64 = 2;

// ---------------------------------------------------------------------------
// Publication model
// ---------------------------------------------------------------------------

/// One `viewer_step_links` row: provenance (`scope = member|result`) or a
/// projected content part (`scope = text|thinking|tool_call|tool_result`).
#[derive(Debug, Clone, PartialEq)]
pub struct StepLink {
    /// 1-based surviving step ordinal (pre-filter steps are renumbered the
    /// same way the oracle renumbers survivors).
    pub step_index: i64,
    /// 0-based emitted turn ordinal; `None` when the step produced no turn.
    pub turn_index: Option<i64>,
    /// `user_message|system_message|turn|compact_boundary|error_message`.
    pub step_kind: String,
    /// Emission order of this row inside the step (member rows first, then
    /// absorbed-result members, then content parts).
    pub link_index: i64,
    /// `member` (record provenance), `result` (absorbed verified result
    /// record), or a part kind.
    pub scope: String,
    /// For `tool_call`/`tool_result` part rows: ordinal among that step's
    /// tool parts — the `(step_index, tool_index)` anchor component.
    pub block_index: Option<i64>,
    /// Scoped/verified call id for verified links, else the recorded raw id.
    pub call_id: Option<String>,
    /// Canonical `messages.idx` of the contributing message (stable anchor
    /// even when `message_id` is unresolved).
    pub source_idx: i64,
    /// `m<id>` when a canonical message id is known, else `i<idx>` — the
    /// facts `Event.key` convention.
    pub source_key: String,
    /// Canonical `messages.id` when resolved (packet replay id or the
    /// publish-time idx→id join).
    pub message_id: Option<i64>,
    /// Deterministic digest of the part's recorded identity for
    /// reindex-stability evidence (member/result rows carry none).
    pub part_key: Option<String>,
}

/// One `viewer_turn_links` row.
#[derive(Debug, Clone, PartialEq)]
pub struct TurnLink {
    pub turn_index: i64,
    /// `user|ai|system|compact_boundary|fork_boundary|approval`.
    pub turn_kind: String,
    pub first_step_index: i64,
    pub step_count: i64,
    /// Anchor message of the turn-opening step for singleton turns
    /// (user/system/boundary); `None` for accumulated `ai` turns.
    pub source_idx: Option<i64>,
    pub message_id: Option<i64>,
    pub source_key: Option<String>,
}

/// One `viewer_tool_calls` row — a verified call→result link.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolCallLink {
    /// Verified scoped call id (`codex-segment:*`, `devin-call:*`, raw).
    pub call_id: String,
    /// Provider-recorded id, byte-for-byte.
    pub raw_call_id: String,
    pub name: String,
    /// Step that recorded the call; `None` when its message produced none.
    pub step_index: Option<i64>,
    pub turn_index: Option<i64>,
    pub has_result: bool,
    pub is_error: Option<bool>,
    pub exit_code: Option<i64>,
    pub file_path: Option<String>,
    /// Call anchor: canonical `messages.idx`/`id` of the recording message.
    pub call_idx: i64,
    pub call_message_id: Option<i64>,
    /// Paired result anchor; `None` when `has_result` is false or no
    /// recorded result record claimed the link.
    pub result_idx: Option<i64>,
    pub result_message_id: Option<i64>,
    /// Ordinal among result-recording shapes inside the result message —
    /// the `block_index` `get_tool_output` needs for multi-output records.
    pub result_block_index: Option<i64>,
    pub ordinal: i64,
}

/// Deterministic structural projection of one session.
#[derive(Debug, Clone)]
pub struct StructuralPublication {
    pub projection_version: i64,
    /// Facts extractor version whose verified tools this projection used.
    pub extractor_version: i64,
    /// Content-addressed canonical marker (`semantic_hash` first 64 bits).
    pub source_revision: i64,
    pub provider: String,
    pub steps: Vec<StepLink>,
    pub turns: Vec<TurnLink>,
    pub tool_calls: Vec<ToolCallLink>,
    /// Exact reasons for dropped/unverifiable structure — never silent.
    pub warnings: Vec<String>,
}

/// Outcome of one structural publication attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionPublishOutcome {
    /// Link rows written and the marker updated.
    Published,
    /// `(projection_version, source_revision)` unchanged — write skipped.
    SkippedUnchanged,
    /// The `viewer_*` profile (schema v2) is not installed in this archive.
    ExtensionNotInstalled,
}
