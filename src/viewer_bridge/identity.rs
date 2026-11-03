//! Source-evidence-bound public session identity for the CASS-owned Viewer
//! backend.
//!
//! A conversation's public CID is `{provider}:{source_cid}` where `provider`
//! is the normalized agent slug and `source_cid` is the verified provider-
//! native session ID for local rows, or `cassremote:<b64(source)>:<b64(native)>`
//! for remote-owned rows. The rules here mirror the Viewer's identity oracle
//! (`viewer/backends/cass/identity.py`, `provenance.py`) over canonical
//! columns (`conversations.external_id`/`source_path`/`source_id`/
//! `origin_host`/`metadata_*`, `agents.slug`, `sources.kind`/`host_label`):
//!
//! * a basename alone is never evidence — each provider rule requires the
//!   canonical path/external-id/metadata evidence to *agree*;
//! * `conversation_id` is never a public ID;
//! * ambiguous or unverifiable evidence is a typed `identity_conflict`
//!   refusal, never an arbitrary first match;
//! * source provenance contradictions (kind vs. source_id vs. origin host)
//!   are a typed `provenance_conflict`;
//! * canonical columns establish the mapping completely, so no
//!   `viewer_identity` persistence table is needed: the forward direction
//!   reads one row by primary key and the reverse direction scans one
//!   `source_id` range of the canonical provenance index, verifying each
//!   candidate by recomputation rather than trusting a stored binding.
//!
//! The catalog-layer oracle never supplies an embedded session ID, so this
//! port omits that input too. Legacy aliases are not synthesized here — only
//! an explicit ingest-time alias registration (a later task) may add one.

use std::collections::BTreeSet;
use std::sync::LazyLock;

use anyhow::Result;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use coding_agent_search::franken_sync::compat::{RowExt, param_slice_to_values};
use coding_agent_search::franken_sync::{Connection, Row, SqliteValue, params};
use regex::Regex;
use serde::Serialize;
use serde_json::Value;

use super::extra;
use super::protocol::refuse;

/// Reserved namespace for remote-owned identities (viewer provenance.py).
pub const REMOTE_PREFIX: &str = "cassremote:";

const LOCAL_SOURCE_ID: &str = "local";

/// `IN (…)` fanout bound for the batched canonical evidence probe —
/// comfortably inside the engine's bound-parameter limit while keeping
/// each statement's parse trivial.
const IDENTITY_CHUNK: usize = 256;

/// Built-in provider namespaces that own a qualified `provider:` key
/// (mirrors `_KNOWN_KEY_PREFIXES`); runtime agent slugs extend this set.
const KNOWN_PROVIDERS: &[&str] = &[
    "antigravity",
    "claude",
    "codex",
    "cursor",
    "devin",
    "junie",
    "qoder",
    "composite",
    "generic",
];

static SLUG_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-z][a-z0-9_-]*$").expect("slug regex"));
static UUID_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
        .expect("uuid regex")
});

/// Verified public identity of one canonical conversation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VerifiedIdentity {
    /// Normalized provider namespace (`claude_code` → `claude`, etc.).
    pub provider: String,
    /// Canonical `conversations.source_id` owning this row.
    pub source_id: String,
    /// Qualified public CID: `{provider}:{source_cid}`.
    pub public_id: String,
}

/// Internal identity resolution result: the public identity plus the
/// provider-native CID and the logical source key used to collapse
/// multi-row identities (e.g. codex chunk files of one thread).
struct ResolvedIdentity {
    provider: String,
    source_id: String,
    native_id: String,
    public_id: String,
    logical_key: String,
    conversation_id: i64,
}

/// One canonical conversation row with the provenance needed for identity.
/// `agent_id`/`source_id` are resolved through the tiny lookup maps below:
/// joining `agents`/`sources` inside the candidate query makes the engine
/// planner pick a table scan, so both directions stay single-table probes.
struct IdentityRow {
    conversation_id: i64,
    agent_id: i64,
    source_id: String,
    external_id: Option<String>,
    source_path: String,
    origin_host: Option<String>,
    metadata_bin: Option<Vec<u8>>,
    metadata_json: Option<String>,
}

/// `agents`/`sources` are bounded dimension tables; loading them once keeps
/// every conversation probe a pure primary-key/index range.
#[derive(Default)]
struct IdentityLookups {
    agents: std::collections::HashMap<i64, String>,
    /// source_id -> (kind, host_label)
    sources: std::collections::HashMap<String, (Option<String>, Option<String>)>,
}

fn load_lookups(connection: &Connection) -> Result<IdentityLookups> {
    let mut lookups = IdentityLookups::default();
    for row in connection.query("SELECT id, slug FROM agents")? {
        let id: i64 = row.get_typed(0)?;
        let slug: Option<String> = row.get_typed(1)?;
        let Some(slug) = slug else { continue };
        if lookups.agents.insert(id, slug).is_some() {
            return Err(refuse(
                "canonical_coordinates_invalid",
                "Ambiguous canonical row",
            ));
        }
    }
    for row in connection.query("SELECT id, kind, host_label FROM sources")? {
        let id: String = row.get_typed(0)?;
        if lookups
            .sources
            .insert(id, (row.get_typed(1)?, row.get_typed(2)?))
            .is_some()
        {
            return Err(refuse(
                "canonical_coordinates_invalid",
                "Ambiguous canonical row",
            ));
        }
    }
    Ok(lookups)
}

/// Columns shared by both lookup directions; `LIMIT 2` on the forward form
/// detects an impossible duplicate primary-key read instead of picking one.
pub const FORWARD_ROW_SQL: &str = concat!(
    "SELECT c.id, c.agent_id, c.source_id, c.external_id, c.source_path, ",
    "c.origin_host, c.metadata_bin, c.metadata_json ",
    "FROM conversations c WHERE c.id = ?1 LIMIT 2"
);

/// Candidate enumeration for the reverse direction: one canonical
/// `source_id` range of `idx_conversations_source_id`, never a catalog-wide
/// scan. Provider filtering happens in Rust so aliased slugs share one
/// statement shape.
pub const REVERSE_CANDIDATE_SQL: &str = concat!(
    "SELECT c.id, c.agent_id, c.source_id, c.external_id, c.source_path, ",
    "c.origin_host, c.metadata_bin, c.metadata_json ",
    "FROM conversations c INDEXED BY idx_conversations_source_id ",
    "WHERE c.source_id = ?1"
);

fn identity_row(row: &Row) -> Result<IdentityRow> {
    Ok(IdentityRow {
        conversation_id: row.get_typed(0)?,
        agent_id: row.get_typed(1)?,
        source_id: row.get_typed(2)?,
        external_id: row.get_typed(3)?,
        source_path: row.get_typed(4)?,
        origin_host: row.get_typed(5)?,
        metadata_bin: row.get_typed(6)?,
        metadata_json: row.get_typed(7)?,
    })
}

// ---------------------------------------------------------------------------
// Provider namespace and native-ID validation (viewer identity.py).
// ---------------------------------------------------------------------------

/// `normalize_agent_slug`: canonicalize a stored agent slug into its public
/// provider namespace. CASS keeps `gemini` distinct (the legacy gemini →
/// antigravity alias belongs only to the native lane). `pub(crate)` for the
/// list lane's provider-filter normalization.
pub(crate) fn normalize_agent_slug(slug: &str) -> Result<String> {
    let normalized = slug.trim().to_lowercase();
    if !SLUG_RE.is_match(&normalized) {
        return Err(refuse(
            "identity_conflict",
            "CASS agent slug is not a valid provider namespace",
        ));
    }
    Ok(match normalized.as_str() {
        "claude_code" => "claude".to_owned(),
        "qoder_cli" => "qoder".to_owned(),
        _ => normalized,
    })
}

/// `_native_id`: a provider-native CID must be nonempty, trimmed, free of
/// control characters and outside the reserved remote namespace.
fn require_native_id(value: &str) -> Result<String> {
    if value.is_empty() || value != value.trim() || value.chars().any(|c| (c as u32) < 32) {
        return Err(refuse(
            "identity_conflict",
            "Session identity is empty or contains invalid whitespace",
        ));
    }
    if value.starts_with(REMOTE_PREFIX) {
        return Err(refuse(
            "identity_conflict",
            "Local native identity collides with the reserved remote namespace",
        ));
    }
    Ok(value.to_owned())
}

/// `_agree`: every candidate plus embedded evidence must collapse to exactly
/// one distinct verified ID.
fn agree(candidates: impl IntoIterator<Item = String>) -> Result<String> {
    let mut values = BTreeSet::new();
    for value in candidates {
        if value.is_empty() {
            continue;
        }
        values.insert(require_native_id(&value)?);
    }
    if values.len() != 1 {
        return Err(refuse(
            "identity_conflict",
            "Session path, external ID and embedded identity do not agree",
        ));
    }
    Ok(values.into_iter().next().expect("one value"))
}

// ---------------------------------------------------------------------------
// Path helpers mirroring PurePosixPath semantics used by the oracle.
// ---------------------------------------------------------------------------

/// `PurePosixPath(value.replace("\\", "/")).parts`: slashes collapse, `.`
/// elements drop, `..` stays.
fn path_parts(value: &str) -> Vec<String> {
    value
        .replace('\\', "/")
        .split('/')
        .filter(|part| !part.is_empty() && *part != ".")
        .map(str::to_owned)
        .collect()
}

/// `PurePosixPath(...).name` — the final component, empty for root-like
/// data. `pub(crate)` for the project display-name filter/display.
pub(crate) fn path_name(value: &str) -> String {
    path_parts(value).last().cloned().unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Provider-specific native-CID rules.
// ---------------------------------------------------------------------------

/// `session_file_stem`: Claude session containers are `<session>.jsonl` or
/// `<session>.json`; auxiliary files yield no stem.
fn session_file_stem(file_name: &str) -> Option<String> {
    if file_name.starts_with('.')
        || file_name.starts_with("deleted_")
        || file_name.starts_with("local_")
        || file_name.starts_with("agent-acompact")
        || file_name.starts_with("acompact")
        || file_name.ends_with(".meta.json")
        || file_name == "history.jsonl"
        || file_name == "stats-cache.json"
    {
        return None;
    }
    for ext in [".jsonl", ".json"] {
        if let Some(stem) = file_name.strip_suffix(ext) {
            return Some(stem.to_owned());
        }
    }
    None
}

/// `parse_rollout_file_name`: `rollout-…` prefix + embedded UUID + a known
/// rollout extension; returns the extension-stripped stem.
fn parse_rollout_file_name(file_name: &str) -> Option<String> {
    if !file_name.starts_with("rollout-") || file_name.starts_with('.') {
        return None;
    }
    for suffix in [".jsonl.zst", ".jsonl", ".json"] {
        if file_name.to_lowercase().ends_with(suffix) {
            let stem = &file_name[..file_name.len() - suffix.len()];
            return UUID_RE.is_match(stem).then(|| stem.to_owned());
        }
    }
    None
}

/// `extract_uuid_from_string`: first embedded UUID, lowercased.
fn extract_uuid(text: &str) -> Option<String> {
    UUID_RE.find(text).map(|m| m.as_str().to_lowercase())
}

/// `_claude_id`: path and external ID must each yield the same session stem.
fn claude_native_id(row: &IdentityRow) -> Result<String> {
    let external = row.external_id.as_deref().unwrap_or("");
    let mut candidates = Vec::new();
    for value in [row.source_path.as_str(), external] {
        if value.is_empty() {
            continue;
        }
        let parts = path_parts(value);
        let filename = path_name(value);
        if parts
            .iter()
            .any(|part| part == "subagents" || part == "workflows")
            || filename.starts_with("agent-")
            || filename.starts_with("wf_")
            || filename.starts_with("local_")
        {
            return Err(refuse(
                "identity_conflict",
                "Claude child/Desktop/workflow identity needs verified source evidence",
            ));
        }
        if let Some(stem) = session_file_stem(&filename) {
            candidates.push(stem);
        } else if value == external && !value.contains('/') && !value.contains('\\') {
            candidates.push(value.to_owned());
        } else {
            return Err(refuse(
                "identity_conflict",
                "Claude source is not a recognized session container",
            ));
        }
    }
    agree(candidates)
}

/// `_codex_id`: a rollout filename (or its extension-less external-id form)
/// yields the thread UUID; a bare normalized UUID external ID is admissible.
fn codex_native_id(row: &IdentityRow) -> Result<String> {
    let external = row.external_id.as_deref().unwrap_or("");
    let mut candidates = Vec::new();
    for value in [row.source_path.as_str(), external] {
        if value.is_empty() {
            continue;
        }
        let filename = path_name(value);
        let mut parsed = parse_rollout_file_name(&filename);
        if parsed.is_none() && value == external && !filename.contains('.') {
            // CASS stores a sessions-relative path WITHOUT the extension.
            parsed = parse_rollout_file_name(&format!("{filename}.jsonl"));
        }
        let mut candidate = parsed.as_deref().and_then(extract_uuid);
        if candidate.is_none() && value == external {
            if let Some(uuid) = extract_uuid(value)
                && uuid == value.to_lowercase()
            {
                candidate = Some(uuid);
            }
        }
        let Some(candidate) = candidate else {
            return Err(refuse(
                "identity_conflict",
                "Codex identity requires a native rollout filename or verified binding",
            ));
        };
        candidates.push(candidate);
    }
    agree(candidates)
}

/// `_container_id`: cursor/devin container sources encode the session in the
/// `#selector` suffix; external_id evidence must agree with the derivation.
fn container_native_id(provider: &str, row: &IdentityRow) -> Result<String> {
    let mut candidates: Vec<String> = row
        .external_id
        .as_deref()
        .filter(|value| !value.is_empty())
        .map(|value| vec![value.to_owned()])
        .unwrap_or_default();
    let path = row.source_path.as_str();
    if let Some((container, selector)) = path.rsplit_once('#') {
        if selector.is_empty() || selector.contains('/') || selector.contains('\\') {
            return Err(refuse(
                "identity_conflict",
                "Container selector is not a session identity",
            ));
        }
        let filename = path_name(container);
        match provider {
            "cursor" => {
                let lower = filename.to_lowercase();
                if !(lower.ends_with(".vscdb") || lower.ends_with(".db")) {
                    return Err(refuse(
                        "identity_conflict",
                        "Cursor source is not a recognized database container",
                    ));
                }
                candidates.push(selector.to_owned());
            }
            "devin" => {
                if !filename.to_lowercase().ends_with(".db") {
                    return Err(refuse(
                        "identity_conflict",
                        "Devin source is not a recognized database container",
                    ));
                }
                let parent = &filename[..filename.len() - 3];
                let candidate = if filename == "sessions.db" {
                    selector.to_owned()
                } else if selector.strip_prefix("devin:") == Some(parent) {
                    parent.to_owned()
                } else {
                    format!("devin:{parent}:{selector}")
                };
                candidates.push(candidate);
            }
            _ => {}
        }
    } else if provider == "devin" && !path.is_empty() {
        let filename = path_name(path);
        if filename.ends_with(".db") && filename != "sessions.db" {
            let parent = &filename[..filename.len() - 3];
            let external = row.external_id.as_deref().unwrap_or("");
            if !external.starts_with(&format!("devin:{parent}:")) {
                candidates.push(parent.to_owned());
            }
        }
    }
    agree(candidates)
}

/// `resolve_local_source_cid`: verified metadata evidence wins over physical
/// filenames; otherwise each provider's canonical rule must produce one ID.
fn native_source_cid(
    provider: &str,
    row: &IdentityRow,
    metadata: &extra::DecodedExtra,
) -> Result<String> {
    if provider == "claude"
        && let Some(verified) = verified_claude_native_id(row, metadata)
    {
        return require_native_id(&verified);
    }
    match provider {
        "claude" => claude_native_id(row),
        "codex" => codex_native_id(row),
        "cursor" | "devin" => container_native_id(provider, row),
        _ => {
            let external = row.external_id.as_deref().filter(|v| !v.is_empty());
            let Some(external) = external else {
                return Err(refuse(
                    "identity_conflict",
                    "Connector has no proven session identity",
                ));
            };
            agree([external.to_owned()])
        }
    }
}

/// `_verified_catalog_native_id`: Claude child/Desktop/job containers carry
/// connector-verified metadata evidence that overrides filename inference.
fn verified_claude_native_id(row: &IdentityRow, metadata: &extra::DecodedExtra) -> Option<String> {
    let parts = path_parts(&row.source_path);
    let filename = path_name(&row.source_path);
    let ext_parts = path_parts(row.external_id.as_deref().unwrap_or(""));
    let len = parts.len();
    if metadata.complete
        && let Some(meta) = metadata.value.as_object()
    {
        let source = meta.get("source").and_then(Value::as_str);
        let sidechain = meta.get("sidechain") == Some(&Value::Bool(true));
        let parent_id = meta
            .get("parentSessionId")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty());
        if source == Some("claude_code")
            && sidechain
            && let Some(parent) = parent_id
            && filename.starts_with("agent-")
            && let Some(stem) = session_file_stem(&filename)
        {
            if len >= 3
                && parts[len - 3] == parent
                && parts[len - 2] == "subagents"
                && parts[len - 1] == filename
                && (ext_parts.is_empty()
                    || (ext_parts.len() >= 3
                        && ext_parts[ext_parts.len() - 3..] == parts[len - 3..]))
            {
                return Some(format!("{parent}--{stem}"));
            }
            if len >= 5
                && parts[len - 5] == parent
                && parts[len - 4] == "subagents"
                && parts[len - 3] == "workflows"
                && !parts[len - 2].is_empty()
                && parts[len - 1] == filename
                && (ext_parts.is_empty()
                    || (ext_parts.len() >= 5
                        && ext_parts[ext_parts.len() - 5..] == parts[len - 5..]))
            {
                return Some(format!("{parent}--{}--{stem}", parts[len - 2]));
            }
        }
        let session_id = meta.get("sessionId").and_then(Value::as_str);
        if source == Some("claude_code_desktop_sidecar")
            && let Some(session_id) = session_id
            && session_id.starts_with("local_")
            && filename == format!("{session_id}.json")
        {
            return Some(session_id.to_owned());
        }
    }
    if filename == "audit.jsonl"
        && parts.iter().any(|part| part == "local-agent-mode-sessions")
        && len >= 2
        && !parts[len - 2].is_empty()
        && (ext_parts.is_empty()
            || (ext_parts.len() >= 2 && ext_parts[ext_parts.len() - 2..] == parts[len - 2..]))
    {
        return Some(format!("{}--audit", parts[len - 2]));
    }
    if filename == "parent-transcript.jsonl"
        && len >= 3
        && parts[len - 3] == "jobs"
        && !parts[len - 2].is_empty()
        && (ext_parts.is_empty()
            || (ext_parts.len() >= 3
                && ext_parts[ext_parts.len() - 3..]
                    == [format!("jobs"), parts[len - 2].clone(), filename.clone()]))
    {
        return Some(format!("job-{}--parent-transcript", parts[len - 2]));
    }
    None
}

/// `catalog_logical_source_key`: identities that verify to one public ID may
/// still bind different rows; they collapse only when their logical source
/// agrees (codex chunk rollouts share the dated sessions root).
fn logical_source_key(provider: &str, source_path: &str, native_id: &str) -> String {
    let mut path = source_path.to_owned();
    if provider == "codex" {
        let normalized = source_path.replace('\\', "/");
        let parts = path_parts(source_path);
        let name = parts.last().map(String::as_str).unwrap_or("");
        let len = parts.len();
        let digit = |value: &str| value.chars().all(|c| c.is_ascii_digit());
        if name.starts_with("rollout-")
            && len >= 5
            && parts[len - 4].len() == 4
            && digit(&parts[len - 4])
            && parts[len - 3].len() == 2
            && digit(&parts[len - 3])
            && parts[len - 2].len() == 2
            && digit(&parts[len - 2])
        {
            let root = parts[..len - 4].join("/");
            path = if normalized.starts_with('/') {
                format!("/{root}")
            } else {
                root
            };
        }
    }
    format!("{provider}\x00{path}\x00{native_id}")
}

// ---------------------------------------------------------------------------
// Remote identity encoding (viewer provenance.py).
// ---------------------------------------------------------------------------

fn encode_remote_source_cid(source_id: &str, native_id: &str) -> Result<String> {
    let encode = |value: &str| -> Result<String> {
        if value.is_empty() {
            return Err(refuse(
                "identity_conflict",
                "Remote identity component is empty",
            ));
        }
        Ok(URL_SAFE_NO_PAD.encode(value.as_bytes()))
    };
    Ok(format!(
        "{REMOTE_PREFIX}{}:{}",
        encode(source_id)?,
        encode(native_id)?
    ))
}

/// `decode_remote_source_cid`: strict `cassremote:<b64>:<b64>` with the
/// canonical re-encode check. Malformed *input* is `invalid_request`.
fn decode_remote_source_cid(value: &str) -> Result<(String, String)> {
    let invalid = || {
        refuse(
            "invalid_request",
            "Remote identity is not canonically encoded",
        )
    };
    let parts: Vec<&str> = value.split(':').collect();
    if parts.len() != 3 || parts[0] != "cassremote" {
        return Err(invalid());
    }
    let mut decoded = Vec::with_capacity(2);
    for part in &parts[1..] {
        if part.is_empty()
            || !part
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            return Err(invalid());
        }
        let raw = URL_SAFE_NO_PAD.decode(part).map_err(|_| invalid())?;
        let text = String::from_utf8(raw).map_err(|_| invalid())?;
        decoded.push(text);
    }
    let (source_id, native_id) = (decoded[0].clone(), decoded[1].clone());
    if encode_remote_source_cid(&source_id, &native_id)
        .ok()
        .as_deref()
        != Some(value)
    {
        return Err(invalid());
    }
    Ok((source_id, native_id))
}

// ---------------------------------------------------------------------------
// Public API.
// ---------------------------------------------------------------------------

/// Recompute one row's verified identity from canonical provenance.
fn resolve_row(row: &Row, lookups: &IdentityLookups) -> Result<ResolvedIdentity> {
    let row = identity_row(row)?;

    // `resolve_catalog_identity`: provenance disagreement is checked first.
    let agent_slug = lookups.agents.get(&row.agent_id).ok_or_else(|| {
        refuse(
            "canonical_coordinates_invalid",
            "Conversation references a missing agent row",
        )
    })?;
    let (source_kind, host_label) = lookups
        .sources
        .get(&row.source_id)
        .cloned()
        .unwrap_or((None, None));
    let origin_host = row.origin_host.clone().or(host_label);
    let kind = source_kind.as_deref();
    if (kind == Some("local") && row.source_id != LOCAL_SOURCE_ID)
        || (kind == Some("ssh") && row.source_id == LOCAL_SOURCE_ID)
        || (kind == Some("local") && origin_host.is_some())
    {
        return Err(refuse(
            "provenance_conflict",
            "Canonical source provenance disagrees",
        ));
    }

    let metadata = extra::decode(row.metadata_bin.clone(), row.metadata_json.clone());
    let provider = normalize_agent_slug(agent_slug)?;
    let native_id = native_source_cid(&provider, &row, &metadata)?;
    let logical_key = logical_source_key(&provider, &row.source_path, &native_id);

    let public_id = if row.source_id == LOCAL_SOURCE_ID {
        format!("{provider}:{native_id}")
    } else {
        // `bind_remote_identity`: a remote source must be a distinct,
        // well-formed canonical source ID.
        if row.source_id.is_empty() || row.source_id != row.source_id.trim() {
            return Err(refuse(
                "identity_conflict",
                "Remote origin requires a distinct canonical source ID",
            ));
        }
        if row.source_id.chars().any(|c| (c as u32) < 32) {
            return Err(refuse(
                "identity_conflict",
                "Remote source ID contains control characters",
            ));
        }
        let encoded = encode_remote_source_cid(&row.source_id, &native_id)?;
        format!("{provider}:{encoded}")
    };

    Ok(ResolvedIdentity {
        provider,
        source_id: row.source_id,
        native_id,
        public_id,
        logical_key,
        conversation_id: row.conversation_id,
    })
}

/// One primary-key probe + verified resolution against preloaded dimension
/// lookups. Shared by the single and batch public-id resolvers.
fn resolve_one(
    connection: &Connection,
    lookups: &IdentityLookups,
    conversation_id: i64,
) -> Result<ResolvedIdentity> {
    let rows = connection.query_with_params(
        FORWARD_ROW_SQL,
        &param_slice_to_values(params![conversation_id]),
    )?;
    if rows.len() > 1 {
        return Err(refuse(
            "canonical_coordinates_invalid",
            "Ambiguous canonical row",
        ));
    }
    let Some(row) = rows.first() else {
        return Err(refuse(
            "session_not_found",
            "No canonical conversation carries this ID",
        ));
    };
    resolve_row(row, lookups)
}

/// Resolve a canonical `conversation_id` to its verified public identity.
///
/// Reads exactly one conversation row (primary key probe), recomputes the
/// provider-native CID from canonical evidence and returns the qualified
/// public CID. Never manufactures an ID from a basename and never exposes
/// `conversation_id` as identity.
pub fn resolve_public_id(
    connection: &Connection,
    conversation_id: i64,
) -> Result<VerifiedIdentity> {
    if conversation_id <= 0 {
        return Err(refuse(
            "invalid_request",
            "conversation_id must be positive",
        ));
    }
    let resolved = resolve_one(connection, &load_lookups(connection)?, conversation_id)?;
    Ok(VerifiedIdentity {
        provider: resolved.provider,
        source_id: resolved.source_id,
        public_id: resolved.public_id,
    })
}

/// Batch variant for the `list_sessions` overlay and the maintenance
/// lanes: dimension lookups load once, then the canonical evidence comes
/// from `IN (ids)` probes on the primary key — repeated `SeekRowid`
/// lookups under FrankenSQLite, one bounded statement per chunk instead
/// of one interpreted query per row. Each row's identity is still
/// recomputed from canonical evidence — an unverifiable row surfaces its
/// typed refusal for the whole page, matching the oracle's catalog
/// binding.
pub fn resolve_public_ids(
    connection: &Connection,
    conversation_ids: &[i64],
) -> Result<std::collections::BTreeMap<i64, VerifiedIdentity>> {
    Ok(
        resolve_public_ids_with_evidence(connection, conversation_ids)?
            .into_iter()
            .map(|(conversation_id, (verified, _row))| (conversation_id, verified))
            .collect(),
    )
}

/// [`resolve_public_ids`] plus the raw canonical evidence [`Row`] each
/// identity was verified against — `FORWARD_ROW_SQL`'s exact column order
/// (`c.id` at index 0, the seven identity-bearing columns at 1..=7). The
/// sort maintenance lane (`viewer_extensions::sort::repair_sort_keys`)
/// binds its in-transaction write to this evidence through
/// `publish_sort_tie_guarded`: a cross-connection canonical identity
/// change committed between this probe and the chunk's write transaction
/// makes the guard refuse, so a stale resolved key can never overwrite
/// the invalidation the conversations trigger installed.
///
/// Dead on the bridge-binary compilation (only the library's sort lane
/// and the test includes call this shape).
#[allow(dead_code)]
pub fn resolve_public_ids_with_evidence(
    connection: &Connection,
    conversation_ids: &[i64],
) -> Result<std::collections::BTreeMap<i64, (VerifiedIdentity, Row)>> {
    let lookups = load_lookups(connection)?;
    for &conversation_id in conversation_ids {
        if conversation_id <= 0 {
            return Err(refuse(
                "canonical_coordinates_invalid",
                "Nonpositive conversation identity",
            ));
        }
    }
    let mut evidence: std::collections::BTreeMap<i64, Row> = std::collections::BTreeMap::new();
    for chunk in conversation_ids.chunks(IDENTITY_CHUNK) {
        let params: Vec<SqliteValue> = chunk.iter().map(|id| SqliteValue::Integer(*id)).collect();
        let placeholders = (1..=chunk.len())
            .map(|i| format!("?{i}"))
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT c.id, c.agent_id, c.source_id, c.external_id, c.source_path, \
             c.origin_host, c.metadata_bin, c.metadata_json \
             FROM conversations c WHERE c.id IN ({placeholders})"
        );
        for row in connection.query_with_params(&sql, &params)? {
            evidence.insert(row.get_typed(0)?, row);
        }
    }
    let mut resolved_map = std::collections::BTreeMap::new();
    for &conversation_id in conversation_ids {
        let Some(row) = evidence.get(&conversation_id) else {
            return Err(refuse(
                "session_not_found",
                "No canonical conversation carries this ID",
            ));
        };
        let resolved = resolve_row(row, &lookups)?;
        resolved_map.insert(
            conversation_id,
            (
                VerifiedIdentity {
                    provider: resolved.provider,
                    source_id: resolved.source_id,
                    public_id: resolved.public_id,
                },
                // The probe row IS the resolution basis — cloning it into
                // the result lets the guarded write re-verify exactly the
                // evidence this identity was recomputed from.
                row.clone(),
            ),
        );
    }
    Ok(resolved_map)
}

enum ParsedPublicId {
    Qualified {
        provider: String,
        source_id: String,
        native_id: String,
    },
    Bare {
        native_id: String,
    },
}

/// Provider namespaces that can own a qualified key: the built-in set plus
/// the normalized slugs actually present in `agents` (the registry's
/// runtime-registered equivalent).
fn provider_namespaces(connection: &Connection) -> Result<BTreeSet<String>> {
    let mut providers: BTreeSet<String> = KNOWN_PROVIDERS.iter().map(|s| s.to_string()).collect();
    for row in connection.query("SELECT slug FROM agents")? {
        let slug: String = row.get_typed(0)?;
        if let Ok(provider) = normalize_agent_slug(&slug) {
            providers.insert(provider);
        }
    }
    Ok(providers)
}

/// Split a public session ID into its verified parts. Qualified
/// `provider:cid` interpretation wins when the prefix is a registered
/// provider namespace; otherwise the whole input is a bare local CID.
fn parse_public_id(connection: &Connection, input: &str) -> Result<ParsedPublicId> {
    if input.is_empty() || input.chars().any(|c| (c as u32) < 32) {
        return Err(refuse(
            "invalid_request",
            "Public session id is empty or contains control characters",
        ));
    }
    if let Some((prefix, cid)) = input.split_once(':') {
        if prefix.is_empty() {
            return Err(refuse("invalid_request", "Public session id is malformed"));
        }
        if let Ok(provider) = normalize_agent_slug(prefix)
            && provider_namespaces(connection)?.contains(&provider)
        {
            if cid.is_empty() {
                return Err(refuse("invalid_request", "Public session id is malformed"));
            }
            if cid.starts_with(REMOTE_PREFIX) {
                let (source_id, native_id) = decode_remote_source_cid(cid)?;
                return Ok(ParsedPublicId::Qualified {
                    provider,
                    source_id,
                    native_id,
                });
            }
            return Ok(ParsedPublicId::Qualified {
                provider,
                source_id: LOCAL_SOURCE_ID.to_owned(),
                native_id: cid.to_owned(),
            });
        }
    }
    if input.starts_with(REMOTE_PREFIX) {
        // The reserved remote namespace can never be a bare local CID.
        return Err(refuse(
            "invalid_request",
            "Reserved remote identity namespace cannot be a bare CID",
        ));
    }
    Ok(ParsedPublicId::Bare {
        native_id: input.to_owned(),
    })
}

/// A candidate row that *claims* the queried native ID (external_id match or
/// the ID embedded in its source path) but failed verification: its error is
/// the honest answer, not a silent miss.
fn plausibly_claims(row: &Row, native_id: &str) -> Result<bool> {
    let external_id: Option<String> = row.get_typed(3)?;
    if external_id.as_deref() == Some(native_id) {
        return Ok(true);
    }
    let source_path: String = row.get_typed(4)?;
    Ok(source_path.contains(native_id))
}

/// Resolve a public session ID to its canonical `conversation_id`.
///
/// Scopes the scan to the queried canonical `source_id` (the provenance
/// index), recomputes each candidate's identity and matches on the verified
/// result — never on a guessed basename or a raw column equality alone.
/// Returns `Ok(None)` when no row verifies to the ID; rows whose evidence
/// plausibly claims the ID but cannot verify surface their typed conflict.
/// When several rows verify to the same public ID, they must agree on the
/// logical source and bind to the lowest `conversation_id` — the same
/// collapse the catalog registry performs for chunk-continued sessions.
pub fn resolve_conversation_id(connection: &Connection, public_id: &str) -> Result<Option<i64>> {
    let parsed = parse_public_id(connection, public_id)?;
    let (scope_source_id, native_id) = match &parsed {
        ParsedPublicId::Qualified {
            source_id,
            native_id,
            ..
        } => (source_id.as_str(), native_id.as_str()),
        ParsedPublicId::Bare { native_id } => (LOCAL_SOURCE_ID, native_id.as_str()),
    };
    let rows = connection.query_with_params(
        REVERSE_CANDIDATE_SQL,
        &param_slice_to_values(params![scope_source_id]),
    )?;
    let lookups = load_lookups(connection)?;
    let mut matched: Vec<(i64, String, String)> = Vec::new();
    for row in &rows {
        match resolve_row(row, &lookups) {
            Ok(resolved) => {
                let hit = match &parsed {
                    ParsedPublicId::Qualified { provider, .. } => {
                        resolved.provider == *provider && resolved.public_id == public_id
                    }
                    ParsedPublicId::Bare { .. } => resolved.native_id == *native_id,
                };
                if hit {
                    matched.push((
                        resolved.conversation_id,
                        resolved.public_id,
                        resolved.logical_key,
                    ));
                }
            }
            Err(error) => {
                if plausibly_claims(row, native_id)? {
                    return Err(error);
                }
            }
        }
    }
    let Some(first) = matched.first() else {
        return Ok(None);
    };
    if matched.iter().any(|item| item.1 != first.1) {
        return Err(refuse(
            "identity_conflict",
            "Bare session ID belongs to more than one provider",
        ));
    }
    if matched.iter().any(|item| item.2 != first.2) {
        return Err(refuse(
            "identity_conflict",
            "Conflicting logical sources for session identity",
        ));
    }
    Ok(Some(
        matched.iter().map(|item| item.0).min().expect("nonempty"),
    ))
}
