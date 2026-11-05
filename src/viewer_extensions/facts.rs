//! Provider-specific session facts persisted at ingest (spec decisions 4/5).
//!
//! `extract_session_facts` projects a [`ConversationPacket`] into a typed,
//! nullable [`FactPublication`]; `publish_session_facts` upserts it into
//! `viewer_session_facts` idempotently on `(extractor_version,
//! source_revision)`: identical inputs skip the write, changed content is a
//! deterministic `INSERT OR REPLACE`. Unrecorded evidence stays NULL and the
//! per-scope coverage manifest records the exact reason — never a fabricated
//! zero.
//!
//! The behavioral oracle is the Viewer's `viewer/backends/cass/facts/*.py`;
//! this is a ported implementation over the normalized CASS packet, not a
//! re-parse of source journals (no JSONL rescan, no second parser).

use anyhow::{Context, Result};
use serde_json::{Value, json};

use crate::franken_sync::compat::{ConnectionExt, ParamValue, RowExt, Transaction, TransactionExt};
use crate::franken_sync::{Connection, FrankenError, Row};
use crate::model::conversation_packet::{ConversationPacket, ConversationPacketProvenance};
use crate::model::types::Conversation;

use super::providers;

// The quality bundle is a public field of [`FactPublication`], so its type
// must be nameable outside `providers`.
pub use providers::classification::SessionQualityMetrics;

/// Bump when any provider extraction semantics change so persisted rows
/// re-derive on the next ingest pass instead of silently going stale.
pub const FACTS_EXTRACTOR_VERSION: i64 = 1;

// ---------------------------------------------------------------------------
// Ingest publication receipts (durable generation state)
// ---------------------------------------------------------------------------

/// `viewer_meta` key prefix for per-session ingest publication receipts.
/// The full key is `{INGEST_RECEIPT_KEY_PREFIX}{conversation_id}`; the
/// suffix-only shape keeps receipts out of the `viewer_change_log_*`
/// journal bookkeeping namespace.
pub const INGEST_RECEIPT_KEY_PREFIX: &str = "ingest_receipt:";

/// Receipt `state` values — a generation is only ever `complete` when the
/// SAME transaction committed both `viewer_session_facts` and the
/// `viewer_session_projection` marker + link rows for this
/// `source_revision`. Anything else is honest unfinished work.
pub const RECEIPT_PENDING: &str = "pending";
pub const RECEIPT_COMPLETE: &str = "complete";
pub const RECEIPT_FAILED: &str = "failed";

/// Durable per-session publication receipt persisted in `viewer_meta`.
///
/// Lifecycle: the ingest orchestrator commits `pending` BEFORE the
/// generation transaction so a crash mid-publication is visible rather
/// than silent; `complete` is written INSIDE the generation transaction
/// (the receipt can never claim coverage the rows do not have); `failed`
/// is committed afterwards with the exact error. `attempts` counts
/// pending marks so repeated failures are visible to operators.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct IngestReceipt {
    pub conversation_id: i64,
    /// `pending|complete|failed` — see the `RECEIPT_*` constants.
    pub state: String,
    /// Content-addressed canonical marker (`semantic_hash` first 64 bits)
    /// this receipt binds — the generation's revision key.
    pub source_revision: i64,
    pub extractor_version: i64,
    pub projection_version: i64,
    /// Exact failure reason for `failed`; `null` otherwise.
    pub reason: Option<String>,
    /// Number of `pending` marks written for this conversation.
    pub attempts: i64,
    pub updated_at_ms: i64,
}

/// The durable receipt for one session, or `None` when none was ever
/// written (a pre-profile session or a generation that was never
/// attempted — both discoverable as unattempted work).
pub fn ingest_receipt_for(
    db: &impl FactsDb,
    conversation_id: i64,
) -> Result<Option<IngestReceipt>> {
    if db
        .facts_query_map(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='viewer_meta' LIMIT 1",
            &[],
            |_| Ok(()),
        )?
        .is_empty()
    {
        return Ok(None);
    }
    let rows = db.facts_query_map(
        "SELECT value FROM viewer_meta WHERE key = ?1 LIMIT 1",
        &[ParamValue::from(format!(
            "{INGEST_RECEIPT_KEY_PREFIX}{conversation_id}"
        ))],
        |row| row.get_typed::<String>(0),
    )?;
    let Some(text) = rows.into_iter().next() else {
        return Ok(None);
    };
    let receipt: IngestReceipt = serde_json::from_str(&text)
        .with_context(|| format!("viewer_meta ingest receipt for {conversation_id} is not JSON"))?;
    Ok(Some(receipt))
}

/// Write one receipt inside the caller's transaction boundary (a bare
/// `&Connection` commits immediately; inside the generation transaction
/// the write commits with the rows it certifies).
pub fn write_ingest_receipt(db: &impl FactsDb, receipt: &IngestReceipt) -> Result<()> {
    let value = serde_json::to_string(receipt).context("serializing ingest receipt")?;
    db.facts_execute(
        "INSERT OR REPLACE INTO viewer_meta (key, value) VALUES (?1, ?2)",
        &[
            ParamValue::from(format!(
                "{INGEST_RECEIPT_KEY_PREFIX}{}",
                receipt.conversation_id
            )),
            ParamValue::from(value),
        ],
    )?;
    Ok(())
}

/// Every receipt that is NOT `complete` — the durable set the explicit
/// resumable backfill reconciles. Sessions with no receipt at all are
/// covered by the backfill's conversation scan, not this list.
pub fn list_incomplete_ingest_receipts(db: &impl FactsDb) -> Result<Vec<IngestReceipt>> {
    if db
        .facts_query_map(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='viewer_meta' LIMIT 1",
            &[],
            |_| Ok(()),
        )?
        .is_empty()
    {
        return Ok(Vec::new());
    }
    let mut receipts = Vec::new();
    for text in db.facts_query_map(
        "SELECT value FROM viewer_meta WHERE key LIKE ?1 ORDER BY key",
        &[ParamValue::from(format!("{INGEST_RECEIPT_KEY_PREFIX}%"))],
        |row| row.get_typed::<String>(0),
    )? {
        match serde_json::from_str::<IngestReceipt>(&text) {
            Ok(receipt) if receipt.state != RECEIPT_COMPLETE => receipts.push(receipt),
            Ok(_) => {}
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "ignoring undecodable ingest receipt in viewer_meta"
                );
            }
        }
    }
    Ok(receipts)
}

/// Per-scope coverage record: `state` is one of
/// `complete|partial|unavailable|pending`, `reasons` are exact evidence
/// reasons — the manifest consumers print instead of trusting a blanket
/// `complete`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoverageEntry {
    pub field: String,
    pub state: String,
    pub reasons: Vec<String>,
}

/// One verified call→result link. Persisted columns carry only the counts
/// today (`viewer_step_links`/`viewer_tool_calls` land with Task 8); the full
/// link stays in-memory so tests pin exact call IDs and scoping.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolCallFact {
    /// Call ID after provider scoping (e.g. Codex compaction segments).
    pub call_id: String,
    /// Provider-recorded ID, byte-for-byte, before scoping.
    pub raw_call_id: String,
    pub name: String,
    pub has_result: bool,
    pub exit_code: Option<i64>,
    pub is_error: Option<bool>,
    pub file_path: Option<String>,
    /// Source anchor: canonical `idx` of the message that recorded the call.
    pub source_idx: i64,
    /// Canonical `messages.id` when the packet came from a canonical replay.
    pub source_message_id: Option<i64>,
}

/// One session's derived facts, every column nullable.
#[derive(Debug, Clone)]
pub struct FactPublication {
    /// Extractor revision label, e.g. `claude-facts-v1`, `extractor_unavailable`.
    pub extractor: String,
    pub extractor_version: i64,
    /// Content-addressed marker of the canonical input (`semantic_hash`).
    pub source_revision: i64,
    pub provider: String,
    pub source_id: String,
    pub coverage: Vec<CoverageEntry>,
    /// Worst-severity rollup of `coverage`; `unknown` for unavailable extractors.
    pub coverage_state: String,
    pub is_subagent: Option<bool>,
    /// Provider-level parent evidence (metadata `parentSessionId`/`parent_id`
    /// or Codex `session_meta` ids), resolved to `parent_conversation_id` at
    /// publish time against canonical rows.
    pub parent_external_id: Option<String>,
    pub task_category: Option<String>,
    pub turn_count: Option<i64>,
    pub step_count: Option<i64>,
    pub thinking_block_count: Option<i64>,
    pub artifact_count: Option<i64>,
    pub subagent_count: Option<i64>,
    pub tool_error_count: Option<i64>,
    pub tool_calls_with_exit_status: Option<i64>,
    pub file_fact_count: Option<i64>,
    pub compaction_count: Option<i64>,
    pub model_set: Vec<String>,
    /// First user prompt text, persisted in `viewer_session_facts.first_prompt`.
    pub first_prompt: Option<String>,
    /// Verified call→result links (in-memory; counts persisted).
    pub tools: Vec<ToolCallFact>,
    /// `SessionMetricsCalculator` bundle persisted to
    /// `viewer_session_metrics` (Task 10). `None` only for
    /// `extractor_unavailable`.
    pub quality: Option<providers::classification::SessionQualityMetrics>,
    /// Per-session health inputs computed from tool evidence + canonical
    /// totals in `facts_for_canonical`; `None` = unmeasured (packet-only
    /// extraction has no canonical totals). Persisted to
    /// `viewer_session_metrics`.
    pub health: Option<SessionHealth>,
}

/// Per-session health summary persisted to `viewer_session_metrics`: the
/// trajectory-evidence half of `summarize_session_health` (junk-folder
/// reads + elevated tool failure rate). Rules-file/memory findings are a
/// dynamic resource boundary and are NEVER folded in here — the
/// `health(scope)` RPC reports them separately.
#[derive(Debug, Clone)]
pub struct SessionHealth {
    pub score: i64,
    pub grade: String,
    pub issues_count: i64,
    pub potential_waste_tokens: i64,
    pub potential_savings_usd: f64,
}

/// Outcome of one publication attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FactPublishOutcome {
    /// Row inserted or deterministically replaced.
    Published,
    /// `(extractor_version, source_revision)` unchanged — write skipped.
    SkippedUnchanged,
    /// The `viewer_*` profile is not installed in this archive.
    ExtensionNotInstalled,
}

/// Minimal SQL surface shared by a bare writer [`Connection`] and an open
/// [`Transaction`] so facts can publish either inside the canonical ingest
/// transaction or in an explicitly atomic adjacent one.
pub trait FactsDb {
    fn facts_query_map<T, F>(
        &self,
        sql: &str,
        params: &[ParamValue],
        f: F,
    ) -> Result<Vec<T>, FrankenError>
    where
        F: FnMut(&Row) -> Result<T, FrankenError>;
    fn facts_execute(&self, sql: &str, params: &[ParamValue]) -> Result<usize, FrankenError>;
}

impl FactsDb for Connection {
    fn facts_query_map<T, F>(
        &self,
        sql: &str,
        params: &[ParamValue],
        f: F,
    ) -> Result<Vec<T>, FrankenError>
    where
        F: FnMut(&Row) -> Result<T, FrankenError>,
    {
        self.query_map_collect(sql, params, f)
    }

    fn facts_execute(&self, sql: &str, params: &[ParamValue]) -> Result<usize, FrankenError> {
        self.execute_compat(sql, params)
    }
}

impl FactsDb for Transaction<'_> {
    fn facts_query_map<T, F>(
        &self,
        sql: &str,
        params: &[ParamValue],
        f: F,
    ) -> Result<Vec<T>, FrankenError>
    where
        F: FnMut(&Row) -> Result<T, FrankenError>,
    {
        self.query_map_collect(sql, params, f)
    }

    fn facts_execute(&self, sql: &str, params: &[ParamValue]) -> Result<usize, FrankenError> {
        self.execute_compat(sql, params)
    }
}

/// Content-addressed source revision: the packet `semantic_hash` already
/// digests identity, metadata, timestamps, roles, content, extras and
/// snippets, so its first 64 bits are a deterministic committed marker.
fn source_revision(packet: &ConversationPacket) -> i64 {
    let hash = &packet.hashes.semantic_hash;
    let take = hash.len().min(16);
    u64::from_str_radix(&hash[..take], 16).unwrap_or(0) as i64
}

/// Extract provider-specific facts from a normalized/canonical packet.
///
/// `provider` is the canonical `agent_slug` (`claude_code`, `codex`, …);
/// unknown slugs produce an `extractor_unavailable` publication.
pub fn extract_session_facts(packet: &ConversationPacket, provider: &str) -> FactPublication {
    let mut publication = providers::extract(packet, provider).into_publication(packet, provider);
    publication.source_revision = source_revision(packet);
    publication
}

/// Idempotent upsert of `publication` into `viewer_session_facts`, keyed on
/// `(extractor_version, source_revision)`. Callable inside the caller's open
/// transaction (`&Transaction`) or on a bare writer `&Connection` (the
/// statements then run in autocommit — prefer wrapping in an explicit
/// transaction at the hook site).
pub fn publish_session_facts(
    db: &impl FactsDb,
    conversation_id: i64,
    publication: &FactPublication,
) -> Result<FactPublishOutcome> {
    if db
        .facts_query_map(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='viewer_session_facts' LIMIT 1",
            &[],
            |_| Ok(()),
        )?
        .is_empty()
    {
        return Ok(FactPublishOutcome::ExtensionNotInstalled);
    }

    let existing = db.facts_query_map(
        "SELECT extractor_version, source_revision FROM viewer_session_facts WHERE conversation_id = ?1",
        &[ParamValue::from(conversation_id)],
        |row| {
            Ok((
                row.get_typed::<i64>(0)?,
                row.get_typed::<i64>(1)?,
            ))
        },
    )?;
    if existing.first().is_some_and(|&(version, revision)| {
        version == publication.extractor_version && revision == publication.source_revision
    }) {
        return Ok(FactPublishOutcome::SkippedUnchanged);
    }
    // The Task-10 metric table is facts-owned (kept out of schema.rs so the
    // projection/schema wave stays additive); created lazily inside the
    // same transaction as the facts upsert so both rows commit atomically.
    ensure_metrics_schema(db)?;

    let (parent_id, parent_resolution) =
        resolve_parent_conversation(db, publication, conversation_id)?;
    let detail = coverage_detail_json(publication, parent_resolution);

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0);
    let model_set_json = (!publication.model_set.is_empty())
        .then(|| serde_json::to_string(&publication.model_set).unwrap_or_default());
    db.facts_execute(
        "INSERT OR REPLACE INTO viewer_session_facts (\
            conversation_id, extractor_version, source_revision, coverage_state, \
            coverage_detail_json, is_subagent, parent_conversation_id, \
            parent_external_id, \
            task_category, turn_count, step_count, thinking_block_count, \
            artifact_count, subagent_count, tool_error_count, \
            tool_calls_with_exit_status, file_fact_count, compaction_count, \
            model_set_json, first_prompt, extracted_at_ms\
         ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21)",
        &[
            ParamValue::from(conversation_id),
            ParamValue::from(publication.extractor_version),
            ParamValue::from(publication.source_revision),
            ParamValue::from(publication.coverage_state.as_str()),
            ParamValue::from(detail),
            ParamValue::from(publication.is_subagent.map(i64::from)),
            ParamValue::from(parent_id),
            ParamValue::from(publication.parent_external_id.as_deref()),
            ParamValue::from(publication.task_category.as_deref()),
            ParamValue::from(publication.turn_count),
            ParamValue::from(publication.step_count),
            ParamValue::from(publication.thinking_block_count),
            ParamValue::from(publication.artifact_count),
            ParamValue::from(publication.subagent_count),
            ParamValue::from(publication.tool_error_count),
            ParamValue::from(publication.tool_calls_with_exit_status),
            ParamValue::from(publication.file_fact_count),
            ParamValue::from(publication.compaction_count),
            ParamValue::from(model_set_json),
            ParamValue::from(publication.first_prompt.as_deref()),
            ParamValue::from(now_ms),
        ],
    )?;
    publish_session_metrics(db, conversation_id, publication, now_ms)?;
    Ok(FactPublishOutcome::Published)
}

/// Facts-owned additive metric table (Task 10). Lives next to
/// `viewer_session_facts` — never inside schema.rs, so the projection/
/// schema wave stays additive. Every metric column is nullable: an
/// unmeasured axis stays NULL, never a fabricated zero.
const METRICS_SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS viewer_session_metrics (\
    conversation_id INTEGER PRIMARY KEY \
        REFERENCES conversations(id) ON DELETE CASCADE, \
    extractor_version INTEGER NOT NULL, \
    source_revision INTEGER NOT NULL DEFAULT 0, \
    ttfe_seconds REAL, \
    one_shot_rate REAL, \
    retries_count INTEGER, \
    self_corrections_count INTEGER, \
    user_corrections_count INTEGER, \
    task_breakdown_json TEXT, \
    tools_used_json TEXT, \
    health_score INTEGER, \
    health_grade TEXT, \
    health_issues_count INTEGER, \
    potential_waste_tokens INTEGER, \
    potential_savings_usd REAL, \
    computed_at_ms INTEGER\
)";

/// Idempotent `viewer_session_metrics` install on any writer lane — a bare
/// `Connection`, an open transaction, or an authorized `--init-extensions`
/// pass. `true` when the table now exists.
pub fn ensure_metrics_schema(db: &impl FactsDb) -> Result<bool> {
    let present = !db
        .facts_query_map(
            "SELECT 1 FROM sqlite_master WHERE type='table' \
             AND name='viewer_session_metrics' LIMIT 1",
            &[],
            |_| Ok(()),
        )?
        .is_empty();
    if present {
        return Ok(true);
    }
    db.facts_execute(METRICS_SCHEMA, &[])?;
    Ok(true)
}

/// Upsert the quality/health half of the publication into
/// `viewer_session_metrics`, keyed on the same
/// `(extractor_version, source_revision)` idempotence the facts row uses
/// (the caller runs this only when the facts row actually changed).
fn publish_session_metrics(
    db: &impl FactsDb,
    conversation_id: i64,
    publication: &FactPublication,
    now_ms: i64,
) -> Result<()> {
    let quality = publication.quality.as_ref();
    let (breakdown_json, tools_used_json) = match quality {
        Some(quality) => {
            // Native `task_breakdown` = category_distribution + a nested
            // `reworked_files` object (metrics.py:111-114).
            let mut breakdown = serde_json::Map::new();
            for (category, turns) in &quality.category_distribution {
                breakdown.insert(category.clone(), json!(turns));
            }
            breakdown.insert(
                "reworked_files".to_string(),
                serde_json::to_value(&quality.reworked_files).unwrap_or_default(),
            );
            let tools: Vec<&str> = quality
                .tools_frequency
                .iter()
                .map(|(name, _)| name.as_str())
                .collect();
            (
                Some(serde_json::to_string(&breakdown).unwrap_or_default()),
                Some(serde_json::to_string(&tools).unwrap_or_default()),
            )
        }
        None => (None, None),
    };
    let health = publication.health.as_ref();
    db.facts_execute(
        "INSERT OR REPLACE INTO viewer_session_metrics (\
            conversation_id, extractor_version, source_revision, \
            ttfe_seconds, one_shot_rate, retries_count, \
            self_corrections_count, user_corrections_count, \
            task_breakdown_json, tools_used_json, \
            health_score, health_grade, health_issues_count, \
            potential_waste_tokens, potential_savings_usd, computed_at_ms\
         ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)",
        &[
            ParamValue::from(conversation_id),
            ParamValue::from(publication.extractor_version),
            ParamValue::from(publication.source_revision),
            ParamValue::from(quality.and_then(|q| q.ttfe_seconds)),
            ParamValue::from(quality.and_then(|q| q.one_shot_rate)),
            ParamValue::from(quality.map(|q| q.retries_count)),
            ParamValue::from(quality.map(|q| q.self_corrections_count)),
            ParamValue::from(quality.map(|q| q.user_corrections_count)),
            ParamValue::from(breakdown_json),
            ParamValue::from(tools_used_json),
            ParamValue::from(health.map(|h| h.score)),
            ParamValue::from(health.map(|h| h.grade.as_str())),
            ParamValue::from(health.map(|h| h.issues_count)),
            ParamValue::from(health.map(|h| h.potential_waste_tokens)),
            ParamValue::from(health.map(|h| h.potential_savings_usd)),
            ParamValue::from(now_ms),
        ],
    )?;
    Ok(())
}

/// `viewer/health_rules.py::JUNK_FOLDERS` — build/cache dirs a read hit is
/// wasted context on.
const JUNK_FOLDERS: &[&str] = &[
    "node_modules",
    "dist",
    "build",
    ".git",
    ".next",
    "__pycache__",
    ".venv",
    "venv",
    ".turbo",
    "target",
    ".nuxt",
    ".cache",
    ".output",
    "out",
];

/// Oracle `is_junk_path`: any lowercase path component in `JUNK_FOLDERS`.
fn is_junk_path(path: &str) -> bool {
    path.replace('\\', "/")
        .split('/')
        .filter(|component| !component.is_empty())
        .any(|component| JUNK_FOLDERS.contains(&component.to_lowercase().as_str()))
}

/// Oracle `score_to_grade` (per-session banding — includes `A+`).
fn score_to_grade(score: i64) -> &'static str {
    match score {
        95.. => "A+",
        90..=94 => "A",
        80..=89 => "B",
        70..=79 => "C",
        60..=69 => "D",
        _ => "F",
    }
}

/// Per-session health from tool evidence and canonical totals — the
/// `audit_session` rules that are NOT dynamic: junk-folder reads (tool file
/// paths) and elevated tool failure rate (recorded results + canonical
/// token/cost totals). The rules-file/memory-layer finding is a dynamic
/// resource boundary reported by the `health` RPC, never persisted here.
///
/// Returns `None` when no tool evidence exists at all AND the extractor
/// did not establish tool coverage as `complete` — a session with
/// unverifiable tools keeps its health unknown rather than reading as a
/// fabricated 100/A+.
fn session_health_inputs(
    publication: &FactPublication,
    total_tokens: Option<i64>,
    cost_usd: Option<f64>,
) -> Option<SessionHealth> {
    let tools_known = publication
        .coverage
        .iter()
        .any(|entry| entry.field == "tools" && entry.state == "complete");
    if publication.tools.is_empty() && !tools_known {
        return None;
    }
    let mut penalty = 0_i64;
    let mut issues = 0_i64;
    let mut waste = 0_i64;
    let mut savings = 0.0_f64;

    // _audit_junk_reads: unique junk paths, count * 3000 tokens, priced at
    // claude-3-7-sonnet input ($3.00/1M) via calculate_call_cost.
    let junk: std::collections::BTreeSet<&str> = publication
        .tools
        .iter()
        .filter_map(|tool| tool.file_path.as_deref())
        .filter(|path| is_junk_path(path))
        .collect();
    if !junk.is_empty() {
        issues += 1;
        penalty += 15; // severity high
        let tokens = (junk.len() as i64) * 3_000;
        waste += tokens;
        savings += (tokens as f64 * 3.0 / 1_000_000.0 * 10_000.0).round() / 10_000.0;
    }

    // _audit_tool_failure_rate: >5 recorded calls AND >30% errors.
    let tool_call_count = publication.tools.len() as i64;
    let error_count = publication
        .tools
        .iter()
        .filter(|tool| tool.is_error == Some(true))
        .count() as i64;
    if tool_call_count > 5 && (error_count as f64 / tool_call_count as f64) > 0.3 {
        issues += 1;
        penalty += 15; // severity high
        waste += total_tokens.unwrap_or(0).max(0) / 4;
        savings += (cost_usd.unwrap_or(0.0).max(0.0) * 0.25 * 10_000.0).round() / 10_000.0;
    }

    let score = (100 - penalty).clamp(0, 100);
    Some(SessionHealth {
        score,
        grade: score_to_grade(score).to_string(),
        issues_count: issues,
        potential_waste_tokens: waste,
        potential_savings_usd: (savings * 10_000.0).round() / 10_000.0,
    })
}

/// Resolve `parent_external_id` to a canonical `conversations.id` using the
/// same exactly-one-match rule the oracle applies: provider + source scoped,
/// `external_id` equal or a path/`*.jsonl` suffix match (Claude
/// `parentSessionId` names the parent's session directory, not its
/// full relative-path external id). Zero or ambiguous stays NULL.
fn resolve_parent_conversation(
    db: &impl FactsDb,
    publication: &FactPublication,
    conversation_id: i64,
) -> Result<(Option<i64>, &'static str)> {
    let Some(external) = publication.parent_external_id.as_deref() else {
        return Ok((None, "no_evidence"));
    };
    let mut ids = db.facts_query_map(
        "SELECT c.id FROM conversations c JOIN agents a ON a.id = c.agent_id \
         WHERE a.slug = ?1 AND c.source_id = ?2 AND c.id != ?3 AND (\
             c.external_id = ?4 OR c.external_id LIKE ?5 OR c.external_id LIKE ?6\
         ) ORDER BY c.id",
        &[
            ParamValue::from(publication.provider.as_str()),
            ParamValue::from(publication.source_id.as_str()),
            ParamValue::from(conversation_id),
            ParamValue::from(external),
            ParamValue::from(format!("%/{external}")),
            ParamValue::from(format!("%{external}.jsonl")),
        ],
        |row| row.get_typed::<i64>(0),
    )?;
    ids.dedup();
    match ids.as_slice() {
        [id] => Ok((Some(*id), "resolved")),
        [] => Ok((None, "unresolved")),
        _ => Ok((None, "ambiguous")),
    }
}

/// Targeted child-before-parent fixup (schema v3): sessions whose durable
/// `parent_external_id` evidence matches the JUST-PUBLISHED parent gain a
/// resolved `parent_conversation_id` inside the parent's own publication
/// transaction — the child never needs a republish and the work stays
/// scoped to rows that recorded this exact evidence (the evidence index),
/// never a catalog rescan.
///
/// The match rule is `resolve_parent_conversation` inverted: same
/// provider + same `source_id` scope, and the child's evidence equal to
/// the parent's `external_id` or matching its `/{evidence}` /
/// `{evidence}.jsonl` suffix shapes. The forward rule's self-exclusion is
/// kept too (`f.conversation_id != ?4`) so a corrupt row can never
/// self-parent.
///
/// One recorded resolution is NOT claimable here: children the forward
/// pass deliberately left `ambiguous` (their evidence matched several
/// parents at publish time) must stay NULL rather than be linked to
/// whichever matching parent arrives later. `no_evidence` rows are
/// already excluded by the `parent_external_id IS NOT NULL` guard.
///
/// Each resolved child's `coverage_detail_json` manifest is refreshed
/// honestly (`parent.resolution` → `resolved`), so the persisted record
/// stops claiming `unresolved` once the link is real.
pub fn fixup_pending_parent_links(
    db: &impl FactsDb,
    parent_conversation_id: i64,
    parent_provider: &str,
    parent_source_id: &str,
    parent_external_id: &str,
) -> Result<usize> {
    let candidates = db.facts_query_map(
        "SELECT f.conversation_id, f.coverage_detail_json \
         FROM viewer_session_facts f \
         JOIN conversations c ON c.id = f.conversation_id \
         JOIN agents a ON a.id = c.agent_id \
         WHERE f.parent_conversation_id IS NULL \
           AND f.parent_external_id IS NOT NULL \
           AND f.parent_external_id != '' \
           AND f.conversation_id != ?4 \
           AND a.slug = ?1 AND c.source_id = ?2 \
           AND (?3 = f.parent_external_id \
                OR ?3 LIKE '%/' || f.parent_external_id \
                OR ?3 LIKE '%' || f.parent_external_id || '.jsonl') \
         ORDER BY f.conversation_id",
        &[
            ParamValue::from(parent_provider),
            ParamValue::from(parent_source_id),
            ParamValue::from(parent_external_id),
            ParamValue::from(parent_conversation_id),
        ],
        |row| {
            Ok((
                row.get_typed::<i64>(0)?,
                row.get_typed::<Option<String>>(1)?,
            ))
        },
    )?;
    let mut resolved = 0_usize;
    for (child_id, detail) in candidates {
        let parsed = detail
            .as_deref()
            .and_then(|text| serde_json::from_str::<Value>(text).ok());
        if parsed
            .as_ref()
            .and_then(|value| value.pointer("/parent/resolution"))
            .and_then(Value::as_str)
            == Some("ambiguous")
        {
            // The forward pass refused to guess among multiple matching
            // parents; a later arrival must not claim the child either.
            continue;
        }
        resolved += 1;
        let patched = match (detail, parsed) {
            (Some(text), Some(mut value)) => {
                if let Some(slot) = value.pointer_mut("/parent/resolution") {
                    *slot = json!("resolved");
                }
                Some(serde_json::to_string(&value).unwrap_or(text))
            }
            _ => None,
        };
        match patched {
            Some(detail) => db.facts_execute(
                "UPDATE viewer_session_facts \
                 SET parent_conversation_id = ?1, coverage_detail_json = ?2 \
                 WHERE conversation_id = ?3",
                &[
                    ParamValue::from(parent_conversation_id),
                    ParamValue::from(detail),
                    ParamValue::from(child_id),
                ],
            )?,
            None => db.facts_execute(
                "UPDATE viewer_session_facts \
                 SET parent_conversation_id = ?1 \
                 WHERE conversation_id = ?2",
                &[
                    ParamValue::from(parent_conversation_id),
                    ParamValue::from(child_id),
                ],
            )?,
        };
    }
    Ok(resolved)
}

/// Per-scope coverage manifest persisted in `coverage_detail_json`: every
/// field carries its state plus exact reasons, so review can enumerate
/// unknown/partial fields per provider instead of a blanket `complete`.
fn coverage_detail_json(publication: &FactPublication, parent_resolution: &str) -> String {
    let mut fields = serde_json::Map::new();
    for entry in &publication.coverage {
        fields.insert(
            entry.field.clone(),
            json!({"state": entry.state, "reasons": entry.reasons}),
        );
    }
    serde_json::to_string(&json!({
        "extractor": publication.extractor,
        "extractor_version": publication.extractor_version,
        "provider": publication.provider,
        "fields": fields,
        "counts": {
            "tool_calls": publication.tools.len(),
            "paired_calls": publication.tools.iter().filter(|t| t.has_result).count(),
        },
        "parent": {
            "evidence": publication.parent_external_id,
            "resolution": parent_resolution,
        },
    }))
    .unwrap_or_default()
}

fn provenance_of(conv: &Conversation) -> ConversationPacketProvenance {
    let origin_kind = conv
        .metadata_json
        .pointer("/cass/origin/kind")
        .and_then(Value::as_str)
        .unwrap_or("local")
        .to_string();
    ConversationPacketProvenance {
        source_id: conv.source_id.clone(),
        origin_kind,
        origin_host: conv.origin_host.clone(),
    }
}

/// Extract facts for an internal (storage-mapped, redacted) canonical
/// `Conversation` — the same payload bytes that were committed.
pub fn facts_for_canonical(conv: &Conversation) -> FactPublication {
    let packet = ConversationPacket::from_canonical_replay(conv, provenance_of(conv));
    // `health` stays None here: canonical token/cost totals live on the
    // conversations row, never the packet — `publish_facts_for_canonical`
    // reads them and fills the inputs before the row is written.
    extract_session_facts(&packet, &conv.agent_slug)
}

/// Canonical health inputs: `grand_total_tokens`/`estimated_cost_usd` live
/// on the `conversations` row, never the packet. Both the ingest hook and
/// the coherent-generation publisher fill `publication.health` through
/// this so the tool-failure finding's waste/savings stay measured
/// (unknown totals stay NULL, never zeroed).
pub fn fill_canonical_health(
    db: &impl FactsDb,
    conversation_id: i64,
    publication: &mut FactPublication,
) -> Result<()> {
    let totals = db.facts_query_map(
        "SELECT grand_total_tokens, estimated_cost_usd \
         FROM conversations WHERE id = ?1",
        &[ParamValue::from(conversation_id)],
        |row| {
            Ok((
                row.get_typed::<Option<i64>>(0)?,
                row.get_typed::<Option<f64>>(1)?,
            ))
        },
    )?;
    let (total_tokens, cost_usd) = totals.first().copied().unwrap_or((None, None));
    publication.health = session_health_inputs(publication, total_tokens, cost_usd);
    Ok(())
}

/// `publish_session_facts` convenience for ingest call sites holding the
/// canonical `Conversation` instead of a packet.
pub fn publish_facts_for_canonical(
    db: &impl FactsDb,
    conv: &Conversation,
    conversation_id: i64,
) -> Result<FactPublishOutcome> {
    let mut publication = facts_for_canonical(conv);
    fill_canonical_health(db, conversation_id, &mut publication)?;
    publish_session_facts(db, conversation_id, &publication)
}

/// Adjacent-transaction publication used by the ingest hook: one explicit
/// transaction on the writer connection, idempotent on
/// `(extractor_version, source_revision)`. Errors are logged, never fatal —
/// a missing facts row reads as `unknown` coverage rather than aborting the
/// canonical ingest commit that already succeeded.
pub fn publish_facts_for_canonical_best_effort(
    connection: &Connection,
    conv: &Conversation,
    conversation_id: i64,
) {
    let result = (|| -> Result<FactPublishOutcome> {
        let mut tx = connection
            .transaction()
            .context("opening viewer session facts transaction")?;
        let outcome = publish_facts_for_canonical(&tx, conv, conversation_id)?;
        tx.commit()
            .context("committing viewer session facts transaction")?;
        Ok(outcome)
    })();
    if let Err(error) = result {
        tracing::warn!(
            conversation_id,
            error = %error,
            "viewer session facts publication failed; session facts remain unknown"
        );
    }
}

/// Batched sibling of [`publish_facts_for_canonical_best_effort`]: one
/// adjacent transaction covers every `(conversation, conversation_id)` pair
/// so the serial ingest path pays a single commit.
pub fn publish_facts_for_canonical_batch_best_effort<'a>(
    connection: &Connection,
    pairs: impl IntoIterator<Item = (&'a Conversation, i64)>,
) {
    let result = (|| -> Result<()> {
        let mut tx = connection
            .transaction()
            .context("opening viewer session facts batch transaction")?;
        for (conv, conversation_id) in pairs {
            publish_facts_for_canonical(&tx, conv, conversation_id)?;
        }
        tx.commit()
            .context("committing viewer session facts batch transaction")?;
        Ok(())
    })();
    if let Err(error) = result {
        tracing::warn!(
            error = %error,
            "viewer session facts batch publication failed; session facts remain unknown"
        );
    }
}
