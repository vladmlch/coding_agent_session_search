//! Task 5: persisted search regions — the ingest-time region inventory the
//! `search_sessions` DSL evaluates through indexed Quill/SQL algebra instead
//! of query-time corpus reconstruction.
//!
//! One [`publish_search_regions`] call inside the coherent ingest
//! generation (`projection::publish_ingest_generation_in_tx`) derives every
//! region the DSL can address — chat, thinking, tools, artifacts, subagents
//! and metadata (title/notes/tags/child descriptors) — from the SAME
//! `ConversationPacket` and `StructuralPublication` the facts/projection
//! lanes publish against, bound to the same `source_revision`.
//!
//! Storage-byte justification for `indexed_text`: the persisted text
//! fragment exists ONLY for regions whose text cannot be read back from
//! the canonical `messages.content` column — derived extra projections
//! (thinking/tools/artifacts) and metadata overlays (title/notes/tags/
//! child descriptors). Chat regions store NULL and hydrate straight from
//! `messages.id`. Without the fragment the Quill region documents and the
//! query-time verification/snippet lane would have to re-decode every
//! referenced message extra — the exact corpus scan this task removes.
//! Fragments are bounded per region (`MAX_REGION_TEXT`) and no complete
//! trajectory is ever persisted.
//!
//! Region documents in Quill: every non-chat region (and every chat region
//! the noise filter excludes from the message projection) owns one
//! `CassDocument` whose `msg_idx` slot is `REGION_DOC_BASE + region_ordinal`
//! — conversation-scoped identities via `cass_document_identity`, never
//! parsed opaquely. `region_docs` rebuilds that doc set from the persisted
//! rows; [`stage_region_docs`] reconciles it on an uncommitted index
//! writer (delete stale ids, upsert the current set) and
//! [`stamp_region_docs_published`] flips `docs_state='published'` ONLY
//! after the index commit seals those documents — stamping before the
//! commit would certify documents a failed commit or crash never
//! shipped, and `converge_pending` would then never revisit the session.
//! A session whose marks row is missing or not `published` is honest
//! *unseen* evidence at query time — never a silently wrong `False`.

mod derive_helpers;
mod derive_message;
mod derive_meta;
mod docs;
mod publish;
mod queries;
#[cfg(test)]
mod tests;

pub use derive_message::derive_message_regions;
pub use derive_meta::derive_meta_regions;
pub(crate) use docs::pack_region_boundaries;
pub use docs::{
    RegionDocStamp, SessionScopeDoc, hydrate_region_text, region_doc_stage, region_docs,
    region_docs_published_current, session_scope_cass_documents, session_scope_docs,
    stage_region_docs, stamp_region_docs_published,
};
pub use publish::{publish_search_regions, replace_meta_regions};
pub(crate) use queries::has_region_marks_table;
pub use queries::{
    live_region_doc_bound, marked_region_doc_sessions, pending_region_doc_sessions, region_mark,
    region_marks_for, region_reproject_stale_session_ids, regions_for_conversation,
    regions_for_conversation_structural, regions_for_conversations, regions_for_quill_slots,
    regions_for_universe_subquery, stale_region_version_session_count,
};

/// Region schema revision; bumps when the derivation shape changes so the
/// `(region_version, source_revision)` idempotence key republishes rows.
/// v2: the antigravity F10 `extra.error` text (distinct from the record
/// content) indexes as an `m{idx}:error` region — tools scope on tool-role
/// steps, chat scope on every other role.
/// v3: compacted claude/codex extras and every devin record gain their
/// `cass.invocations` call arguments as tools regions (for compacted
/// records the invocations merge is the ONLY surviving copy of the
/// arguments; the devin connector normalizes `tool_calls` away), and the
/// claude Desktop sidecar's user-typed `raw.initialMessage` indexes as an
/// `m{idx}:initial_prompt` chat region.
/// v4: v3 plus gemini raw-record derivations — `thoughts[]` index as
/// `m{idx}:thinking` regions and raw `toolCalls[]` (call args + inline
/// result text, recorded status verdicts) index as tool regions, with
/// `cass.invocations` as the post-compaction fallback; the pinned
/// connector keeps tool arguments out of the delivered content — and
/// pi_agent derivations: `toolCall` content blocks index as tool regions
/// paired with their `toolResult` records by toolCallId (the generic
/// role fallback stays suppressed for those messages).
/// v5: v4 plus copilot F14 derivations — the connector archives the raw
/// `response` part array verbatim in `extra.copilot_response_parts`, so
/// `thinking` part text (values + `generatedTitle`) indexes as
/// `m{idx}:thinking` regions, `mcpServersStarting` server ids index as
/// `m{idx}:mcp` tools regions, and `toolInvocationSerialized` groups
/// (arguments + rendered call messages + `resultDetails` strings, paired
/// by `toolCallId`) index as tool regions; the `cass.invocations`
/// fallback stays suppressed when the parts array is present (the facts
/// lane classifies the same shapes — providers/copilot.rs,
/// `scan_copilot_parts`).
/// v6: v5 plus session×scope documents — every marked conversation also
/// publishes one aggregated lexical document per (scope, chunk) packed from
/// its whole region inventory (`session_scope_docs`), minted at
/// `SESSION_DOC_BASE + (scope_ordinal << SESSION_DOC_SCOPE_SHIFT) +
/// chunk_seq`, so single-token strict plans answer membership from posting
/// sets instead of the capped message-lane enumeration (Stage 1 of the
/// 2026-11-10 strict-common-terms report, §6.1/§6.2). The bump forces one
/// republish of every existing marks row — the fast path refuses to trust
/// session docs stamped at any older version. Transition note (review
/// M-2): that first republish queues session-doc stale slots bounded by
/// each conversation's OLD per-scope region-row count — slots that never
/// held a live document under v5, when session docs did not exist.
/// Harmless (the retire DELETE ignores absent ids) but the one-time
/// `pending_doc_deletes` JSON and live-bound contribution across a full
/// reindex is proportional to region counts, not to real stale chunks.
/// v7: v6 plus the follow-up-1 codex rendered-exit trailer — a paired
/// result of a census-proven renderer name (`exec_command`, `write_stdin`)
/// whose output text carries `Process exited with code N` now records that
/// code as the region's `exit_code`/`has_error` (the facts lane extracts
/// the same verdict), so strict `exit:`/`has:error` answer True/False
/// exactly for sessions whose status exists only as rendered text.
pub const REGION_VERSION: i64 = 7;

/// Synthetic `msg_idx` space for region documents: `REGION_DOC_BASE +
/// region_ordinal`. Below `i64::MAX` so `decode_hit` accepts them; far above
/// any real message index so a consumer that treats the slot as a message
/// coordinate misses honestly instead of hydrating the wrong message.
pub const REGION_DOC_BASE: u64 = 1 << 40;

/// Synthetic `msg_idx` space for session×scope documents:
/// `SESSION_DOC_BASE + (scope_ordinal << SESSION_DOC_SCOPE_SHIFT) +
/// chunk_seq`. Above every region slot (region ordinals stay far below
/// `1 << 41`), below `i64::MAX`, and never colliding with
/// [`REGION_DOC_BASE`] slots. Far above any real message index for the
/// same honest-miss reason.
pub const SESSION_DOC_BASE: u64 = 1 << 41;

/// Per-scope chunk width of the session-doc slot space (the report's
/// `(scope_ordinal << 20) + chunk_seq`). A scope packing this many chunks
/// is refused loudly at derivation — a single conversation×scope that big
/// is an archive anomaly, not something to silently mis-slot.
pub const SESSION_DOC_SCOPE_SHIFT: u64 = 20;

/// Hard per-scope chunk ceiling: `chunk_seq < 1 << SESSION_DOC_SCOPE_SHIFT`.
pub const SESSION_DOC_MAX_CHUNKS_PER_SCOPE: u64 = 1 << SESSION_DOC_SCOPE_SHIFT;

/// Session×scope chunks pack WHOLE regions (never mid-region) until the
/// next region would push the chunk past this many CHARACTERS — an
/// oversized region takes a chunk of its own. Chars, not bytes, because
/// the count derivation reads SQL `LENGTH()` (characters for TEXT), so the
/// staging packer and the SQL-side count agree exactly; 65,536 chars is
/// ≤ 256 KiB of UTF-8 for every text (max 4 bytes per scalar).
pub const SESSION_DOC_CHUNK_CHAR_CAP: usize = 64 * 1024;

/// Fixed scope→ordinal mapping for session-doc slots. The six canonical
/// scopes get stable ordinals; any unknown scope string shares the 6
/// bucket so the mapping is total and deterministic from the rows alone.
pub fn session_scope_ordinal(scope: &str) -> u64 {
    match scope {
        "title" => 0,
        "chat" => 1,
        "tools" => 2,
        "thinking" => 3,
        "artifacts" => 4,
        "subagents" => 5,
        _ => 6,
    }
}

/// One region's session-doc text contribution in characters INCLUDING the
/// trailing `'\n'` separator that keeps the last token of one region from
/// merging with the first token of the next (`text.chars().count() + 1`).
/// The SQL-side count derivation must add the same `+ 1` per region.
///
/// The SQL twin of the Rust text selection — `indexed_text` when present
/// (fragment wins: noise-chat and every derived/meta row), else the
/// anchored message content for `chat` rows, else the empty string:
pub const SESSION_REGION_TEXT_SQL: &str = "COALESCE(r.indexed_text, CASE WHEN r.text_source = 'chat' \
     THEN COALESCE(m.content, '') ELSE '' END)";

/// Same bound the query-time derivation applied to one region's text.
pub const MAX_REGION_TEXT: usize = 64 * 1024;

/// `text_source` lanes: `chat` hydrates `messages.content` via
/// `message_id`; `derived`/`meta` carry their fragment in `indexed_text`.
pub const TEXT_SOURCE_CHAT: &str = "chat";
pub const TEXT_SOURCE_DERIVED: &str = "derived";
pub const TEXT_SOURCE_META: &str = "meta";

/// `docs_state` values on `viewer_search_region_marks`.
pub const DOCS_PENDING: &str = "pending";
pub const DOCS_PUBLISHED: &str = "published";
pub const DOCS_META_PENDING: &str = "meta_pending";

// ---------------------------------------------------------------------------
// Row model
// ---------------------------------------------------------------------------

/// One `viewer_search_regions` row — the persisted FactBlock: stable key,
/// scope/knownness, canonical coordinates, structural anchors, predicate
/// fields and the hydration pointer.
#[derive(Debug, Clone, Default)]
pub struct RegionRow {
    pub region_ordinal: i64,
    pub region_key: String,
    pub scope: String,
    pub scope_known: bool,
    pub quill_idx: i64,
    pub message_idx: Option<i64>,
    pub message_id: Option<i64>,
    pub step_index: Option<i64>,
    pub turn_index: Option<i64>,
    pub tool_name: Option<String>,
    pub file_path: Option<String>,
    pub exit_code: Option<i64>,
    pub has_error: Option<bool>,
    pub error_type: Option<String>,
    pub text_source: String,
    /// Whether a persisted fragment exists (`indexed_text IS NOT NULL`) —
    /// carried by the structural universe load, which never materializes
    /// the text itself (Stage 1 lazy fragments).
    pub has_fragment: bool,
    /// Persisted bounded fragment for `derived`/`meta` lanes.
    pub indexed_text: Option<String>,
    pub source_revision: i64,
}

impl RegionRow {
    pub fn is_meta(&self) -> bool {
        self.region_key.starts_with("meta:")
    }
}
