//! Version-one bounded JSONL. No partial result is ever written to stdout.
use std::io::{self, BufRead, Write};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub const REQUEST_BYTES: usize = 65_536;
pub const RESPONSE_BYTES: usize = 1_048_576;
pub const PAGE_LIMIT: usize = 256;
pub const MAX_CURSORS: usize = 8;
pub const TIMEOUT_MS: u64 = 10_000;
pub const MAX_RAW_HITS: usize = 100_000;

/// Versioned capability negotiated through `hello`/`status`: the CASS-owned
/// query lane that binds named logical revisions (spec decision 12) and a
/// narrow, explicitly privileged write surface. Protocol-1 clients that
/// never opt in keep the original strictly read-only surface unchanged.
pub const CAPABILITY_CASS_OWNED_QUERIES_V1: &str = "cass_owned_queries_v1";

/// Logical revisions bound into capability responses and write receipts.
/// Physical `catalog_revision` tokens still gate raw snapshots for
/// protocol-1 compatibility; these counters are the committed logical
/// generations — a WAL write is not a canonical content change.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Generation {
    pub canonical_revision: i64,
    pub annotation_revision: i64,
    pub projection_revision: i64,
    pub lexical_generation: i64,
}

/// Bounded patch for `patch_annotations`. Every field is a double option so
/// an absent key differs from an explicit `null` clear. No canonical column
/// can be named here; this is the entire writable surface.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AnnotationPatch {
    #[serde(default)]
    pub is_pinned: Option<bool>,
    #[serde(default, deserialize_with = "double_option")]
    pub custom_title: Option<Option<String>>,
    #[serde(default, deserialize_with = "double_option")]
    pub notes: Option<Option<String>>,
    /// Full tag-set replacement (viewer `MetadataPatch` semantics: a
    /// present list replaces the set; absent key leaves tags untouched;
    /// explicit `null` decodes to absent). Raw strings are sanitized
    /// (`annotations::sanitize_tags`) at write time.
    #[serde(default)]
    pub tags: Option<Vec<String>>,
}

fn double_option<'de, D>(deserializer: D) -> Result<Option<Option<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer).map(Some)
}

/// Verified coordinate for `get_tool_output`. `kind = "message"` selects a
/// canonical message by `message_id` (canonical `messages.id`) or `idx`
/// (exactly one); `block_index` picks among the recorded tool-output blocks
/// inside that message's preserved `extra` (default 0). Structural
/// `step`/`tool` anchors land with the Task-8 projection and are refused
/// `anchor_unsupported` until verified links exist.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StepAnchor {
    pub kind: String,
    #[serde(default)]
    pub message_id: Option<i64>,
    #[serde(default)]
    pub idx: Option<i64>,
    #[serde(default)]
    pub block_index: Option<i64>,
}

impl AnnotationPatch {
    /// Field byte bounds; notes stay well under the 64 KiB request frame so
    /// one patch can never starve the wire.
    pub const MAX_TITLE_BYTES: usize = 4_096;
    pub const MAX_NOTES_BYTES: usize = 65_536 / 2;
    /// Bounded tag surface: at most 32 raw strings of ≤ 4 KiB each per
    /// patch (sanitization shrinks them further).
    pub const MAX_TAGS: usize = 32;
    pub const MAX_TAG_RAW_BYTES: usize = 4_096;

    pub fn validate(&self) -> anyhow::Result<()> {
        let mut present = self.is_pinned.is_some() | self.tags.is_some();
        for (value, cap) in [
            (&self.custom_title, Self::MAX_TITLE_BYTES),
            (&self.notes, Self::MAX_NOTES_BYTES),
        ] {
            if let Some(text) = value {
                present = true;
                if let Some(text) = text
                    && (text.is_empty() || text.len() > cap || text.trim() != text.as_str())
                {
                    return Err(refuse(
                        "invalid_request",
                        "Annotation text must be non-empty, trimmed and within its byte bound",
                    ));
                }
            }
        }
        if let Some(tags) = &self.tags
            && (tags.len() > Self::MAX_TAGS
                || tags.iter().any(|tag| tag.len() > Self::MAX_TAG_RAW_BYTES))
        {
            return Err(refuse(
                "invalid_request",
                "Annotation tags exceed the bounded patch surface",
            ));
        }
        if !present {
            return Err(refuse(
                "invalid_request",
                "Annotation patch must set at least one field",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct Refusal {
    pub kind: &'static str,
    pub message: String,
}

pub fn refuse(kind: &'static str, message: impl Into<String>) -> anyhow::Error {
    Refusal {
        kind,
        message: message.into(),
    }
    .into()
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BridgeFilters {
    #[serde(default)]
    pub agents: Vec<String>,
    #[serde(default)]
    pub workspaces: Vec<String>,
    pub source_id: Option<String>,
    pub created_from: Option<i64>,
    pub created_to: Option<i64>,
}

impl BridgeFilters {
    pub fn validate(&self) -> anyhow::Result<()> {
        let mut values = self
            .agents
            .iter()
            .chain(&self.workspaces)
            .chain(self.source_id.iter());
        if self.agents.len() > 32
            || self.workspaces.len() > 32
            || values.any(|s| s.is_empty() || s.len() > 4096 || s.trim() != s.as_str())
            || matches!((self.created_from, self.created_to), (Some(a), Some(b)) if a > b)
        {
            return Err(refuse(
                "invalid_request",
                "Invalid or excessive engine filters",
            ));
        }
        Ok(())
    }
}

/// `list_sessions` filter set (`cass_owned_queries_v1` read op). Every
/// field is optional and ANDed; unknown keys are refused at parse time.
/// Unknown-vs-absent semantics live in `query.rs`: absent facts rows mean
/// "unknown", never "no match".
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ListFilters {
    /// Normalized provider namespace (`claude` covers `claude_code` rows);
    /// `all`/`all_providers`/empty disables the filter.
    pub provider: Option<String>,
    /// Canonical `conversations.source_id` ("local", "ssh:host", …).
    pub source_id: Option<String>,
    /// Workspace path or unique display name.
    pub project: Option<String>,
    /// `viewer_session_facts.task_category`.
    pub category: Option<String>,
    /// Explicit subagent membership; `None` uses `include_subagents`.
    pub is_subagent: Option<bool>,
    /// Browse rule: `true` keeps proven subagents in an unfiltered browse.
    pub include_subagents: Option<bool>,
    /// Pin-state membership (`is_bookmarked` in viewer terms).
    pub pinned: Option<bool>,
    /// Single `viewer_tags` membership (sanitized like the writer).
    pub tag: Option<String>,
    /// Lower bound on the activity timestamp (ms); alias of
    /// `activity_from_ms`.
    pub updated_after_ms: Option<i64>,
    pub activity_from_ms: Option<i64>,
    /// Exclusive upper bound on the activity timestamp (ms).
    pub activity_to_ms: Option<i64>,
    /// `conversations.started_at` window (ms).
    pub created_from_ms: Option<i64>,
    pub created_to_ms: Option<i64>,
}

/// Fixed analytics scope for `projects`/`dashboard`/`health` — resolved
/// once per request into provider/project/activity/subagent predicates and
/// applied identically to every aggregate statement of that request
/// (oracle: `_build_filter_clause` + `resolve_analytics_scope`).
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AnalyticsScope {
    /// Normalized provider namespace (`claude` covers `claude_code`);
    /// `all`/`all_providers`/absent disables the filter.
    pub provider: Option<String>,
    /// Workspace path or unique display name; `all`/`all_projects`/absent
    /// disables the filter.
    pub project: Option<String>,
    /// Native rolling period alias: `all` (default), `today`, `7d`,
    /// `7days`, `week`, `30d`, `30days`, `month`. Resolved to a lower
    /// activity bound at request time and merged with `since_ms`.
    pub period: Option<String>,
    /// Lower activity bound (ms, inclusive); NULL-activity rows never
    /// match a window (unknown ≠ included, `_matches` semantics).
    pub since_ms: Option<i64>,
    /// Upper activity bound (ms, exclusive).
    pub until_ms: Option<i64>,
    /// `true` keeps proven subagents in the scope; default excludes them
    /// like the shared `AnalyticsScope.include_subagents` flag. Rows with
    /// unknown classification are kept either way (COALESCE(f,0)=0).
    pub include_subagents: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScanBudget {
    #[serde(default = "default_raw_hits")]
    pub max_raw_hits: usize,
    #[serde(default = "default_timeout")]
    pub deadline_ms: u64,
}

fn default_raw_hits() -> usize {
    MAX_RAW_HITS
}
fn default_timeout() -> u64 {
    TIMEOUT_MS
}
fn default_page() -> usize {
    PAGE_LIMIT
}

impl Default for ScanBudget {
    fn default() -> Self {
        Self {
            max_raw_hits: MAX_RAW_HITS,
            deadline_ms: TIMEOUT_MS,
        }
    }
}

impl ScanBudget {
    pub fn validate(&self) -> anyhow::Result<()> {
        if !(1..=MAX_RAW_HITS).contains(&self.max_raw_hits)
            || !(1..=TIMEOUT_MS).contains(&self.deadline_ms)
        {
            return Err(refuse(
                "invalid_request",
                "Scan budgets may only lower the profile limits",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    Hello {
        id: u64,
    },
    Status {
        id: u64,
    },
    CatalogPage {
        id: u64,
        after_id: Option<i64>,
        #[serde(default = "default_page")]
        limit: usize,
    },
    Session {
        id: u64,
        conversation_id: i64,
    },
    MessagesPage {
        id: u64,
        conversation_id: i64,
        after_idx: Option<i64>,
        #[serde(default = "default_page")]
        limit: usize,
    },
    RankBegin {
        id: u64,
        query: String,
        #[serde(default)]
        filters: BridgeFilters,
        #[serde(default)]
        budget: ScanBudget,
    },
    RankNext {
        id: u64,
        cursor: String,
        max_hits: usize,
    },
    RankCancel {
        id: u64,
        cursor: String,
    },
    Reload {
        id: u64,
    },
    /// `cass_owned_queries_v1` read op: annotation state for one
    /// conversation through the strict read lane. Legal without the write
    /// opt-in; reports `installed:false` on unextended archives. Address
    /// exactly one of `conversation_id` or `public_id`.
    GetAnnotations {
        id: u64,
        #[serde(default)]
        conversation_id: Option<i64>,
        #[serde(default)]
        public_id: Option<String>,
    },
    /// `cass_owned_queries_v1` write op: compare-and-swap one annotation
    /// row. Requires the explicit local `--enable-viewer-writes`
    /// authorization; refused with `capability_not_granted` otherwise.
    /// Address exactly one of `conversation_id` or `public_id` (verified
    /// through `identity::resolve_conversation_id`).
    PatchAnnotations {
        id: u64,
        #[serde(default)]
        conversation_id: Option<i64>,
        #[serde(default)]
        public_id: Option<String>,
        expected_revision: i64,
        patch: AnnotationPatch,
    },
    /// `cass_owned_queries_v1` read op: indexed session browse — one
    /// bounded `WHERE + ORDER BY + LIMIT limit+1` statement with a keyset
    /// cursor, never a catalog materialization.
    ListSessions {
        id: u64,
        #[serde(default)]
        filters: ListFilters,
        sort: Option<String>,
        pin_first: Option<bool>,
        cursor: Option<String>,
        #[serde(default = "default_page")]
        limit: usize,
    },
    /// `cass_owned_queries_v1` read op: bounded rich detail for ONE
    /// addressed conversation — canonical summary/messages/raw sections
    /// plus `viewer_*` overlays, with typed `unavailable` for named Native
    /// surfaces the canonical lane cannot yet express. Address exactly one
    /// of `conversation_id` or `public_id`; `cursor` continues the paged
    /// messages/raw/attachments section.
    GetSessionDetails {
        id: u64,
        #[serde(default)]
        conversation_id: Option<i64>,
        #[serde(default)]
        public_id: Option<String>,
        #[serde(default)]
        sections: Vec<String>,
        #[serde(default)]
        cursor: Option<String>,
        #[serde(default = "default_page")]
        limit: usize,
    },
    /// `cass_owned_queries_v1` read op: one recorded tool output bound to
    /// a verified canonical-message anchor and the logical
    /// `canonical_revision` the anchor was observed at; a stale revision is
    /// a typed `detail_changed` refusal.
    GetToolOutput {
        id: u64,
        #[serde(default)]
        conversation_id: Option<i64>,
        #[serde(default)]
        public_id: Option<String>,
        step_anchor: StepAnchor,
        expected_revision: i64,
        /// UTF-8 byte offset for chunked reads: each call returns at most
        /// `TOOL_OUTPUT_BYTES` of content plus `next_offset`/`content_bytes`
        /// so output beyond the single-response budget reassembles exactly
        /// instead of silently dropping its tail.
        #[serde(default)]
        byte_offset: Option<i64>,
    },
    /// `cass_owned_queries_v1` read op: session-level lexical search.
    /// `ast` carries the canonical viewer `SearchAst` wire object
    /// (`{"root": <node>}` or a bare node: `term`/`filter`/`not`/`and`/`or`);
    /// `query` is the raw DSL string parsed by the CASS grammar port —
    /// `ast` wins when both are present. `filters` narrows the SQL
    /// candidate universe; `scopes` injects the Search-In scope union when
    /// the AST does not scope itself; `sort` defaults to `relevance`
    /// (witness-bound Quill scores) with `updated_desc`/`updated_asc`/
    /// `created_desc`/`created_asc` also served; `mode` is `lexical` —
    /// `semantic`/`hybrid` are separate ungranted capabilities.
    /// `allow_partial` is the explicit opt-in that permits `complete:false`
    /// receipts when the candidate/evaluation budgets exhaust.
    SearchSessions {
        id: u64,
        #[serde(default)]
        ast: Option<Value>,
        #[serde(default)]
        query: Option<String>,
        #[serde(default)]
        filters: ListFilters,
        #[serde(default)]
        scopes: Option<Vec<String>>,
        sort: Option<String>,
        mode: Option<String>,
        #[serde(default)]
        allow_partial: Option<bool>,
        cursor: Option<String>,
        #[serde(default = "default_page")]
        limit: usize,
        #[serde(default)]
        max_matches_per_session: Option<usize>,
    },
    /// `cass_owned_queries_v1` read op: per-workspace aggregate over
    /// canonical + extension facts — GROUP BY workspace with per-metric
    /// known/unknown coverage, never a catalog materialization.
    Projects {
        id: u64,
        #[serde(default)]
        scope: AnalyticsScope,
    },
    /// `cass_owned_queries_v1` read op: project dashboard aggregates —
    /// financials, quality KPIs (incl. median TTFE), punchcard, file churn,
    /// category/tool distributions, provider/model histograms, health
    /// audit and the recent-session slice, all over stored facts only.
    Dashboard {
        id: u64,
        #[serde(default)]
        scope: AnalyticsScope,
    },
    /// `cass_owned_queries_v1` read op: aggregate health — persisted
    /// per-session health metrics folded with `audit_aggregates` findings,
    /// PLUS a clearly separated `dynamic_resources` section (project
    /// rules/instruction files read on demand) that never feeds the score.
    Health {
        id: u64,
        #[serde(default)]
        scope: AnalyticsScope,
    },
    /// `cass_owned_queries_v1` read op: bounded replay of the committed
    /// `viewer_change_log` journal (Task 11 journal, Task 12 wire). Absent
    /// `after_seq`/`0` replays the retained head; the response is
    /// `{events, next_seq, truncated, current_revisions}` under the same
    /// snapshot discipline as canonical pages — `next_seq` is the resume
    /// token, and `truncated` + a `reset_required` event demands a coarse
    /// refetch. This is the refetch fallback the `--enable-viewer-events`
    /// push channel overflows to; it needs no opt-in flag.
    ChangesSince {
        id: u64,
        #[serde(default)]
        after_seq: Option<i64>,
        #[serde(default)]
        limit: Option<usize>,
    },
    Shutdown {
        id: u64,
    },
}

impl Request {
    pub fn id(&self) -> u64 {
        match self {
            Self::Hello { id }
            | Self::Status { id }
            | Self::CatalogPage { id, .. }
            | Self::Session { id, .. }
            | Self::MessagesPage { id, .. }
            | Self::RankBegin { id, .. }
            | Self::RankNext { id, .. }
            | Self::RankCancel { id, .. }
            | Self::Reload { id }
            | Self::GetAnnotations { id, .. }
            | Self::PatchAnnotations { id, .. }
            | Self::ListSessions { id, .. }
            | Self::SearchSessions { id, .. }
            | Self::GetSessionDetails { id, .. }
            | Self::GetToolOutput { id, .. }
            | Self::Projects { id, .. }
            | Self::Dashboard { id, .. }
            | Self::Health { id, .. }
            | Self::ChangesSince { id, .. }
            | Self::Shutdown { id } => *id,
        }
    }
}

pub fn page_limit(limit: usize) -> anyhow::Result<()> {
    if !(1..=PAGE_LIMIT).contains(&limit) {
        return Err(refuse("invalid_request", "Page limit must be 1..256"));
    }
    Ok(())
}

pub fn success(id: u64, result: Value) -> Value {
    json!({"schema_version":1, "id":id, "ok":true, "result":result})
}

pub fn failure(id: u64, error: &anyhow::Error) -> Value {
    let kind = error
        .downcast_ref::<Refusal>()
        .map_or("backend_read_failed", |e| e.kind);
    let incomplete = matches!(
        kind,
        "ranking_budget_exceeded" | "stale_snapshot" | "unmappable_hit"
    );
    json!({"schema_version":1,"id":id,"ok":false,"error":{
        "kind":kind,"message":error.to_string(),"enumeration_complete":if incomplete { Some(false) } else { None },
    }})
}

pub enum Frame {
    Line(Vec<u8>),
    End,
    TooLarge,
    Unterminated,
}

pub fn read_frame(input: &mut impl BufRead) -> io::Result<Frame> {
    let mut line = Vec::new();
    loop {
        let available = input.fill_buf()?;
        if available.is_empty() {
            return Ok(if line.is_empty() {
                Frame::End
            } else {
                Frame::Unterminated
            });
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let count = newline.unwrap_or(available.len());
        if count > REQUEST_BYTES - line.len() {
            return Ok(Frame::TooLarge);
        }
        line.extend_from_slice(&available[..count]);
        input.consume(count + usize::from(newline.is_some()));
        if newline.is_some() {
            return Ok(Frame::Line(line));
        }
    }
}

struct BoundedBuffer(Vec<u8>);
impl Write for BoundedBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > RESPONSE_BYTES - 1 - self.0.len() {
            return Err(io::Error::other("Bridge response exceeds 1 MiB"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub fn encode_line(value: &impl Serialize) -> io::Result<Vec<u8>> {
    let mut buffer = BoundedBuffer(Vec::new());
    serde_json::to_writer(&mut buffer, value).map_err(io::Error::other)?;
    buffer.0.push(b'\n');
    Ok(buffer.0)
}

pub fn write_frame(output: &mut impl Write, bytes: &[u8]) -> io::Result<()> {
    output.write_all(bytes)?;
    output.flush()
}
