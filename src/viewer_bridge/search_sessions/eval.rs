use std::collections::{BTreeSet, HashMap, HashSet};

use anyhow::Result;
use coding_agent_search::franken_sync::Connection;
use serde_json::Value;

use super::super::annotations;
use super::super::identity;
use super::super::protocol::ListFilters;
use super::dsl::{BLOCK_FIELDS, Domain, LiteralMatcher, PlanNode};
use super::fast_lane::unseen_inventory_result;
use super::loader::{BlockSeekEvidence, UNKNOWN_PREDICATE_SENTINEL};
use super::regions::{
    HydrateStats, MAX_UNGATED_CHAT_HYDRATIONS, Region, SessionCtx, region_text, region_text_full,
};

// ---------------------------------------------------------------------------
// Three-valued evaluation (dsl/regions.py + dsl/predicates.py port)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Truth {
    True,
    False,
    Unknown,
}

fn truth_and(a: Truth, b: Truth) -> Truth {
    if a == Truth::False || b == Truth::False {
        Truth::False
    } else if a == Truth::Unknown || b == Truth::Unknown {
        Truth::Unknown
    } else {
        Truth::True
    }
}

fn truth_not(a: Truth) -> Truth {
    match a {
        Truth::Unknown => Truth::Unknown,
        Truth::True => Truth::False,
        Truth::False => Truth::True,
    }
}

/// One node's evaluation: `known` = proven region keys, `possible` =
/// possibly-matching keys (superset incl. known), `unseen` = inventory
/// gaps that could still carry evidence, plus positive literal witnesses.
#[derive(Debug, Default)]
pub(super) struct NodeResult {
    pub(super) all: bool,
    pub(super) known: BTreeSet<String>,
    pub(super) possible_all: bool,
    pub(super) possible: BTreeSet<String>,
    pub(super) unseen: bool,
    pub(super) witnesses: Vec<(String, String)>, // (region key, branch id)
    pub(super) reasons: BTreeSet<String>,        // "field:state:reason"
}

impl NodeResult {
    pub(super) fn truth(&self) -> Truth {
        if self.all || !self.known.is_empty() {
            Truth::True
        } else if self.possible_all || !self.possible.is_empty() || self.unseen {
            Truth::Unknown
        } else {
            Truth::False
        }
    }
    pub(super) fn constant(truth: Truth, reasons: BTreeSet<String>) -> Self {
        match truth {
            Truth::False => Self::default(),
            _ => Self {
                all: truth == Truth::True,
                possible_all: true,
                reasons,
                ..Self::default()
            },
        }
    }
    pub(super) fn negate(self) -> Self {
        Self::constant(truth_not(self.truth()), self.reasons)
    }
}

pub(super) fn combine(left: &NodeResult, right: &NodeResult, conjunction: bool) -> NodeResult {
    let mut out = NodeResult::default();
    if conjunction {
        out.all = left.all && right.all;
        // Intersection with the "all" side acting as universal set.
        out.known = if left.all {
            right.known.clone()
        } else if right.all {
            left.known.clone()
        } else {
            left.known.intersection(&right.known).cloned().collect()
        };
        out.possible_all = left.possible_all && right.possible_all;
        out.possible = if left.possible_all {
            right.possible.clone()
        } else if right.possible_all {
            left.possible.clone()
        } else {
            left.possible
                .intersection(&right.possible)
                .cloned()
                .collect()
        };
        out.unseen = (left.possible_all || left.unseen) && (right.possible_all || right.unseen);
    } else {
        out.all = left.all || right.all;
        out.known = left.known.union(&right.known).cloned().collect();
        out.possible_all = left.possible_all || right.possible_all;
        out.possible = left.possible.union(&right.possible).cloned().collect();
        out.unseen = left.unseen || right.unseen;
    }
    // Witnesses survive only when their region key stays inside the
    // resulting known set (or the node proved all-regions).
    out.witnesses = left
        .witnesses
        .iter()
        .chain(&right.witnesses)
        .filter(|(key, _)| out.all || out.known.contains(key))
        .cloned()
        .collect();
    out.reasons = left.reasons.union(&right.reasons).cloned().collect();
    out
}

/// `_like`: ASCII-folded glob/`LIKE` matcher — `*` and (with `sql_wildcards`)
/// `%`/`_`, else literal substring when no `*` present for field patterns.
pub(super) fn ascii_fold(text: &str) -> String {
    text.chars().map(|ch| ch.to_ascii_lowercase()).collect()
}

fn glob_match(value: &str, pattern: &str) -> bool {
    // Case-folded `*`/`?` glob (fnmatch semantics; `?` matches one char).
    let value: Vec<char> = ascii_fold(value).chars().collect();
    let pattern: Vec<char> = ascii_fold(pattern).chars().collect();
    let (mut v, mut p) = (0_usize, 0_usize);
    let (mut star, mut mark) = (usize::MAX, 0_usize);
    while v < value.len() {
        if p < pattern.len() && (pattern[p] == '?' || pattern[p] == value[v]) {
            v += 1;
            p += 1;
        } else if p < pattern.len() && pattern[p] == '*' {
            star = p;
            mark = v;
            p += 1;
        } else if star != usize::MAX {
            p = star + 1;
            mark += 1;
            v = mark;
        } else {
            return false;
        }
    }
    while p < pattern.len() && pattern[p] == '*' {
        p += 1;
    }
    p == pattern.len()
}

pub(super) fn like_match(value: &str, pattern: &str, sql_wildcards: bool, substring: bool) -> bool {
    // Translate to a literal-aware pattern: `*` is always a wildcard,
    // `%`/`_` are wildcards only under SQL LIKE semantics; every other
    // character stays literal (the oracle escapes them the same way).
    enum Tok {
        Star,
        QMark,
        Lit(char),
    }
    let mut tokens: Vec<Tok> = Vec::new();
    if substring {
        tokens.push(Tok::Star);
    }
    for ch in ascii_fold(pattern).chars() {
        tokens.push(match ch {
            '*' => Tok::Star,
            '%' if sql_wildcards => Tok::Star,
            '_' if sql_wildcards => Tok::QMark,
            other => Tok::Lit(other),
        });
    }
    if substring {
        tokens.push(Tok::Star);
    }
    let value: Vec<char> = ascii_fold(value).chars().collect();
    let (mut v, mut p) = (0_usize, 0_usize);
    let (mut star, mut mark) = (usize::MAX, 0_usize);
    while v < value.len() {
        if p < tokens.len()
            && (matches!(tokens[p], Tok::QMark)
                || matches!(tokens[p], Tok::Lit(ch) if ch == value[v]))
        {
            v += 1;
            p += 1;
        } else if p < tokens.len() && matches!(tokens[p], Tok::Star) {
            star = p;
            mark = v;
            p += 1;
        } else if star != usize::MAX {
            p = star + 1;
            mark += 1;
            v = mark;
        } else {
            return false;
        }
    }
    while p < tokens.len() && matches!(tokens[p], Tok::Star) {
        p += 1;
    }
    p == tokens.len()
}

const BASH_TOOL_SYNONYMS: &[&str] = &[
    "run_command",
    "bash",
    "Bash",
    "exec_command",
    "exec",
    "execute_command",
    "run_terminal_cmd",
    "run_terminal_cmd_v2",
    "terminal",
    "Terminal",
];
const EDIT_TOOL_SYNONYMS: &[&str] = &[
    "replace_file_content",
    "write_to_file",
    "edit",
    "Edit",
    "write",
    "Write",
    "strreplace",
    "StrReplace",
    "apply_patch",
    "patch",
];

pub(super) fn tool_match(value: &str, pattern: &str, op: &str) -> bool {
    let lowered = pattern.to_lowercase();
    if matches!(lowered.as_str(), "bash" | "terminal" | "cmd" | "sh") {
        return BASH_TOOL_SYNONYMS.contains(&value);
    }
    if matches!(lowered.as_str(), "edit" | "write" | "patch") {
        return EDIT_TOOL_SYNONYMS.contains(&value);
    }
    if pattern.contains('*') {
        return glob_match(value, pattern);
    }
    if pattern.contains('%') || op == "LIKE" {
        return like_match(value, pattern, true, false);
    }
    ascii_fold(value) == ascii_fold(pattern)
}

fn compare_numbers(value: f64, expected: f64, op: &str) -> Truth {
    if !(value.is_finite() && expected.is_finite()) {
        return Truth::Unknown;
    }
    let hit = match op {
        "EQ" => value == expected,
        "NEQ" => value != expected,
        "GT" => value > expected,
        "GTE" => value >= expected,
        "LT" => value < expected,
        "LTE" => value <= expected,
        _ => return Truth::Unknown,
    };
    if hit { Truth::True } else { Truth::False }
}

// ---------------------------------------------------------------------------
// Predicate evaluation
// ---------------------------------------------------------------------------

const COVERAGE_FIELD: &[(&str, &str)] = &[
    ("tool", "tools"),
    ("mcp", "tools"),
    ("file", "files"),
    ("exit", "tool_exit"),
    ("error_type", "tool_error"),
    ("has_error", "tool_error"),
    ("has_mcp", "tools"),
];

pub(super) fn coverage_field(field: &str) -> &str {
    COVERAGE_FIELD
        .iter()
        .find(|(name, _)| *name == field)
        .map(|(_, scope)| *scope)
        .unwrap_or(field)
}

fn missing_reason(session: &SessionCtx, field: &str, fallback: &str) -> String {
    let state = session.coverage_of(coverage_field(field));
    format!("{field}:{state}:{fallback}")
}

/// `_block_value`: predicate truth over ONE region.
pub(super) fn region_value(
    field: &str,
    op: &str,
    expected: &Value,
    session: &SessionCtx,
    region: &Region,
) -> Truth {
    match field {
        "scope" => {
            if !region.scope_known {
                return Truth::Unknown;
            }
            let wanted = expected.as_str().unwrap_or_default();
            // Oracle `_block_value`: `in:chat` admits title blocks too.
            let hit = if wanted == "chat" {
                matches!(region.scope.as_str(), "title" | "chat")
            } else {
                region.scope == wanted
            };
            if hit { Truth::True } else { Truth::False }
        }
        "tool" => match &region.tool_name {
            None => unknown_field(session, "tools", region),
            Some(name) => {
                if tool_match(name, expected.as_str().unwrap_or_default(), op) {
                    Truth::True
                } else {
                    Truth::False
                }
            }
        },
        "file" => match &region.file_path {
            None => unknown_field(session, "files", region),
            Some(path) => {
                let pattern = expected.as_str().unwrap_or_default();
                if like_match(path, pattern, false, !pattern.contains('*')) {
                    Truth::True
                } else {
                    Truth::False
                }
            }
        },
        "mcp" => {
            let pattern = expected.as_str().unwrap_or_default();
            // mcp membership binds tool servers: the persisted fragment
            // (tool-call text for derived lanes, meta/noise-chat rows
            // too) or the tool name. Chat regions with `indexed_text`
            // NULL carry no text surface here — their canonical content
            // is a `chat` literal's concern, not tool membership. This
            // is the deliberately narrowed reading of the Python
            // oracle's `fts.tool_name OR fts.content`: the oracle had
            // every block's text in memory for free, while a region
            // predicate in the indexed lane evaluates persisted row
            // fields only — hydrating `messages.content` per region
            // would be the corpus scan this architecture removes.
            // Pinned by `search_sessions_mcp_binds_tool_and_meta_surfaces`.
            let matched = region
                .indexed_text
                .as_deref()
                .is_some_and(|text| like_match(text, pattern, false, true))
                || region
                    .tool_name
                    .as_deref()
                    .is_some_and(|name| like_match(name, pattern, false, true));
            if matched {
                Truth::True
            } else if region.tool_name.is_some() {
                Truth::False
            } else {
                unknown_field(session, "tools", region)
            }
        }
        "exit" => match region.exit_code {
            None => unknown_field(session, "tool_exit", region),
            Some(code) => compare_numbers(code as f64, expected.as_f64().unwrap_or(f64::NAN), op),
        },
        "error_type" => match region.error_type.as_deref() {
            None => unknown_field(session, "tool_error", region),
            Some(error_type) => {
                if error_type == expected.as_str().unwrap_or_default() {
                    Truth::True
                } else {
                    Truth::False
                }
            }
        },
        "has_error" => match region.has_error {
            None => unknown_field(session, "tool_error", region),
            Some(has_error) => {
                if has_error == expected.as_bool().unwrap_or(false) {
                    Truth::True
                } else {
                    Truth::False
                }
            }
        },
        _ => Truth::False,
    }
}

/// `_unknown_field`: coverage decides whether a missing value is
/// `FALSE` (complete inventory — or text provably absent at the source,
/// which cannot record a value it never had) or `UNKNOWN`.
fn unknown_field(session: &SessionCtx, field: &str, region: &Region) -> Truth {
    if !session.field_blocks_strict(field) {
        return Truth::False;
    }
    if matches!(field, "tools" | "tool_exit")
        && region.scope_known
        && region.scope.as_str() != "tools"
    {
        return Truth::False;
    }
    Truth::Unknown
}

/// `_collection`: predicate over a session's region set.
fn collection(field: &str, op: &str, expected: &Value, session: &SessionCtx) -> NodeResult {
    let mut out = NodeResult::default();
    let coverage = coverage_field(field);
    for region in &session.regions {
        match region_value(field, op, expected, session, region) {
            Truth::True => {
                out.known.insert(region.key.clone());
                out.possible.insert(region.key.clone());
            }
            Truth::Unknown => {
                out.possible.insert(region.key.clone());
                out.reasons
                    .insert(missing_reason(session, coverage, "value_unknown"));
            }
            Truth::False => {}
        }
    }
    // Unseen: block inventory or the field's own coverage incomplete can
    // still hide a matching region. Source-absent gaps (owner decision
    // 2026-11-07) cannot: unrecorded blocks would have to exist (not-read
    // `block_inventory`) AND carry this field's text (the field's own
    // not-read coverage) for a match to hide. The title leg stays on the
    // raw state: an unread title (`overlay_not_supplied`) is a not-read
    // gap that can hide a chat-scope match independent of the block
    // inventory's source-absence.
    let unseen = (session.field_blocks_strict("block_inventory")
        && session.field_blocks_strict(coverage))
        || session.inventory_partial
        || (field == "scope"
            && expected.as_str() == Some("chat")
            && session.coverage_of("title") != "complete"
            && session.coverage_of("block_inventory") != "complete");
    if unseen {
        out.unseen = true;
        out.reasons.insert(missing_reason(
            session,
            coverage,
            "block_inventory_incomplete",
        ));
    }
    out
}

/// The seek lane's `collection` — byte-identical [`NodeResult`] without the
/// eager region load (see [`super::loader::BlockSeekEvidence`] for the
/// exactness argument). True keys come from the request-wide value seek
/// (the resident scan re-derives any resident True for belt-and-suspenders
/// parity, since the seek is table-wide exact anyway); resident Unknown
/// candidates come from `region_value` over the literal-gated rows —
/// exactly the Unknown keys that can intersect literal member keys, since
/// literal members are always gated residents; non-resident Unknown
/// existence comes from the NULL-candidate probe, discharged through the
/// sentinel key so the verdict and reasons stay identical to the eager
/// lane's per-region Unknowns.
fn collection_narrowed(
    field: &str,
    op: &str,
    expected: &Value,
    session: &SessionCtx,
    evidence: &BlockSeekEvidence,
    keys: &HashMap<i64, BTreeSet<String>>,
) -> NodeResult {
    let mut out = NodeResult::default();
    let coverage = coverage_field(field);
    if let Some(known) = keys.get(&session.conversation_id) {
        for key in known {
            out.known.insert(key.clone());
            out.possible.insert(key.clone());
        }
    }
    let mut any_unknown = false;
    for region in &session.regions {
        match region_value(field, op, expected, session, region) {
            Truth::True => {
                out.known.insert(region.key.clone());
                out.possible.insert(region.key.clone());
            }
            Truth::Unknown => {
                out.possible.insert(region.key.clone());
                any_unknown = true;
            }
            Truth::False => {}
        }
    }
    // Non-resident Unknown existence: the probe row proves some region of
    // this conversation could discharge Unknown; only a session whose
    // coverage actually blocks the field can ever discharge it.
    if session.field_blocks_strict(coverage) && evidence.probe_hit(field, session.conversation_id) {
        any_unknown = true;
    }
    if any_unknown {
        out.reasons
            .insert(missing_reason(session, coverage, "value_unknown"));
        if out.known.is_empty() {
            out.possible.insert(UNKNOWN_PREDICATE_SENTINEL.to_owned());
        }
    }
    // Unseen tail: the identical formula to `collection`.
    let unseen = (session.field_blocks_strict("block_inventory")
        && session.field_blocks_strict(coverage))
        || session.inventory_partial
        || (field == "scope"
            && expected.as_str() == Some("chat")
            && session.coverage_of("title") != "complete"
            && session.coverage_of("block_inventory") != "complete");
    if unseen {
        out.unseen = true;
        out.reasons.insert(missing_reason(
            session,
            coverage,
            "block_inventory_incomplete",
        ));
    }
    out
}

/// `_session_value`: session-domain predicate truth.
fn session_value(field: &str, op: &str, expected: &Value, session: &SessionCtx) -> Truth {
    let expected_str = expected.as_str().unwrap_or_default();
    match field {
        "tokens" | "cost" | "duration" => {
            let metric = match field {
                "tokens" => session.tokens.map(|v| v as f64),
                "cost" => session.cost,
                _ => session.duration,
            };
            match metric {
                None => Truth::Unknown,
                Some(value) => compare_numbers(value, expected.as_f64().unwrap_or(f64::NAN), op),
            }
        }
        "is_subagent" => match session.is_subagent {
            None => Truth::Unknown,
            Some(value) => {
                if value == expected.as_bool().unwrap_or(false) {
                    Truth::True
                } else {
                    Truth::False
                }
            }
        },
        "provider" => {
            let hit = if op == "LIKE" {
                like_match(&session.provider, expected_str, true, false)
            } else {
                session.provider == expected_str
            };
            if hit { Truth::True } else { Truth::False }
        }
        "project" => {
            let project = session.project.as_deref().unwrap_or_default();
            let hit = if op == "LIKE" {
                like_match(project, expected_str, true, false)
            } else {
                ascii_fold(project) == ascii_fold(expected_str)
            };
            if hit { Truth::True } else { Truth::False }
        }
        "tag" => {
            let folded = ascii_fold(expected_str);
            if session.tags.iter().any(|tag| ascii_fold(tag) == folded) {
                Truth::True
            } else {
                Truth::False
            }
        }
        "title" | "model" => {
            let value = if field == "title" {
                session.effective_title()
            } else {
                session.model.as_deref()
            };
            match value {
                None => Truth::Unknown,
                Some(text) => {
                    if like_match(text, expected_str, false, !expected_str.contains('*')) {
                        Truth::True
                    } else {
                        Truth::False
                    }
                }
            }
        }
        _ => Truth::False,
    }
}

pub(super) fn eval_predicate(
    field: &str,
    op: &str,
    expected: &Value,
    domain: Domain,
    session: &SessionCtx,
    seek: Option<&BlockSeekEvidence>,
) -> NodeResult {
    if field == "has_mcp" {
        let tagged = session
            .tags
            .iter()
            .any(|tag| ascii_fold(tag).contains("mcp"));
        // The seek lane proves `named` table-wide (the tool_name vocabulary
        // seek covers every region, resident or not); the eager lane
        // iterates the resident rows — the same fact either way.
        let named = match seek {
            Some(evidence) => evidence.mcp_named.contains(&session.conversation_id),
            None => session.regions.iter().any(|r| {
                r.tool_name
                    .as_deref()
                    .is_some_and(|n| ascii_fold(n).contains("mcp") || n.contains('/'))
            }),
        };
        let mut value = if tagged || named {
            Truth::True
        } else if !session.field_blocks_strict("tools") {
            // Source-absent tools gaps (allowlisted reasons) prove mcp
            // absence the same way every other field predicate does —
            // only a not-read gap may keep this Unknown.
            Truth::False
        } else {
            Truth::Unknown
        };
        // Oracle: `if bool(node.value) != (node.op != "NEQ"): truth_not`.
        if (expected.as_bool() == Some(true)) != (op != "NEQ") {
            value = truth_not(value);
        }
        let mut reasons = BTreeSet::new();
        if value == Truth::Unknown {
            reasons.insert(missing_reason(session, "tools", "value_unknown"));
        }
        return NodeResult::constant(value, reasons);
    }
    if !BLOCK_FIELDS.contains(&field) {
        let mut value = session_value(field, op, expected, session);
        if op == "NEQ" && !matches!(field, "tokens" | "duration" | "cost") {
            value = truth_not(value);
        }
        let mut reasons = BTreeSet::new();
        if value == Truth::Unknown {
            reasons.insert(missing_reason(session, field, "value_unknown"));
        }
        return NodeResult::constant(value, reasons);
    }
    if domain == Domain::Session
        && field == "scope"
        && matches!(expected.as_str(), Some("chat" | "title"))
    {
        let hit = expected.as_str() == Some("chat") || session.title.is_some();
        return NodeResult::constant(
            if hit { Truth::True } else { Truth::False },
            BTreeSet::new(),
        );
    }
    let mut out = match seek.and_then(|evidence| {
        evidence
            .keys_for(field, op, expected)
            .map(|keys| (evidence, keys))
    }) {
        Some((evidence, keys)) => collection_narrowed(field, op, expected, session, evidence, keys),
        None => collection(field, op, expected, session),
    };
    if domain == Domain::Session {
        // Non-term heuristics: explicit project path + metadata markers.
        if field == "file"
            && session.project_path.as_deref().is_some_and(|path| {
                let pattern = expected.as_str().unwrap_or_default();
                like_match(path, pattern, false, !pattern.contains('*'))
            })
        {
            return NodeResult::constant(Truth::True, BTreeSet::new());
        }
        if field == "mcp"
            && session
                .tags
                .iter()
                .any(|tag| like_match(tag, expected.as_str().unwrap_or_default(), false, true))
        {
            return NodeResult::constant(Truth::True, BTreeSet::new());
        }
        return NodeResult::constant(out.truth(), out.reasons);
    }
    std::mem::take(&mut out)
}

// ---------------------------------------------------------------------------
// Literal + plan evaluation
// ---------------------------------------------------------------------------

/// One region's literal-verification verdict — always decided against
/// the region's COMPLETE text evidence:
///
/// - A canonical-row chat region anchors the full `messages.content`
///   (the same body the canonical Quill document indexed). A gated
///   (index-proven) candidate first tries the bounded window — the
///   common in-window hit reproduces there at zero escalation cost —
///   and escalates to the whole body only when the window cannot
///   reproduce the hit, which is where a past-window hit lives. An
///   ungated (non-ASCII) literal skips the window entirely: the
///   analyzer cannot prove the negative, so the whole body is the only
///   honest evidence.
/// - Persisted-fragment regions (derived/meta/noise-chat) verify against
///   their stored fragment — the region document publishes exactly that
///   fragment, so its evidence is already complete.
///
/// Whole-body scans are metered against the request's byte ceiling
/// ([`HydrateStats::verification_budget`]): once the running total
/// reaches the ceiling no further scan starts, and the verdict is the
/// typed [`RegionMatch::BudgetExceeded`] — an explicit, reasoned
/// refusal, never a silent cap. The one scan that crosses the ceiling
/// overdrafts by at most a single region (the largest measured message
/// is ~2.2 MB).
pub(super) enum RegionMatch {
    /// The region's complete text evidence reproduces the literal.
    Member,
    /// The complete evidence is in and does NOT match.
    NonMember,
    /// No local text exists for the region (absent anchor or absent row).
    NoText,
    /// The request's whole-body byte ceiling is exhausted — the text
    /// exists but cannot be scanned.
    BudgetExceeded,
}

pub(super) fn verify_region_text(
    connection: &Connection,
    region: &Region,
    matcher: &LiteralMatcher,
    quill_possible: Option<&HashSet<i64>>,
    stats: &mut HydrateStats,
) -> Result<RegionMatch> {
    let canonical = region.hydrates_canonical_row();
    if canonical
        && quill_possible.is_some()
        && let Some(bounded) = region_text(connection, region, stats)?
        && matcher.matches(&bounded)
    {
        // Gated candidate: the bounded window already reproduces the hit.
        return Ok(RegionMatch::Member);
    }
    let Some(text) = (if canonical {
        region_text_full(connection, region, stats)?
    } else {
        region_text(connection, region, stats)?
    }) else {
        return Ok(RegionMatch::NoText);
    };
    if canonical {
        if stats.verification_bytes >= stats.verification_budget {
            return Ok(RegionMatch::BudgetExceeded);
        }
        stats.verification_bytes += text.len();
    }
    if matcher.matches(&text) {
        Ok(RegionMatch::Member)
    } else {
        Ok(RegionMatch::NonMember)
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn eval_literal(
    value: &str,
    exact: bool,
    branch: &str,
    positive: bool,
    session: &SessionCtx,
    connection: &Connection,
    stats: &mut HydrateStats,
    matchers: &HashMap<(String, bool), LiteralMatcher>,
    quill_possible: Option<&HashSet<i64>>,
) -> Result<NodeResult> {
    let matcher = &matchers[&(value.to_owned(), exact)];
    let mut out = NodeResult::default();
    if matcher.never {
        return Ok(out);
    }
    // A literal the index cannot express (non-ASCII text — quill's CASS
    // analyzer skips every non-ASCII non-CJK scalar, producing no tokens
    // for it) must not gate membership on hits: the gate would read the
    // index's blind spot as a proven negative and silently false-negative
    // every session.
    // `quill_possible = None` evaluates every persisted region through the
    // matcher — still bounded by the region inventory, never a corpus read.
    let mut gated_unreproduced = false;
    for region in &session.regions {
        // Indexed membership gate: only regions whose Quill document slot
        // produced a hit for this literal can be members. Everything else
        // is provably absent — the index proves the negative, the corpus
        // is never scanned.
        if let Some(gate) = quill_possible
            && !gate.contains(&region.quill_idx)
        {
            continue;
        }
        // Ungated evaluation (non-ASCII literal — the index cannot prove
        // the negative) must not degrade into one `messages.content`
        // point read per region across the whole universe: chat rows
        // carry no persisted text, so each check is a real read. Past
        // the request-level budget the session is `unseen` — honest
        // uncertainty that refuses without `allow_partial` — while
        // derived/meta regions keep verifying from persisted fragments.
        if quill_possible.is_none() && region.hydrates_canonical_row() {
            if stats.ungated_chat_hydrations >= MAX_UNGATED_CHAT_HYDRATIONS {
                out.unseen = true;
                out.reasons
                    .insert("literal:chat_verification_budget_exceeded".to_owned());
                continue;
            }
            stats.ungated_chat_hydrations += 1;
        }
        match verify_region_text(connection, region, matcher, quill_possible, stats)? {
            RegionMatch::Member => {
                out.known.insert(region.key.clone());
                out.possible.insert(region.key.clone());
                if positive {
                    out.witnesses.push((region.key.clone(), branch.to_owned()));
                }
            }
            RegionMatch::BudgetExceeded => {
                if quill_possible.is_some() {
                    // A gated candidate the budget could not verify: the
                    // index-proven hit stays possible membership with a
                    // typed reason — honest uncertainty, never silently
                    // dropped or admitted.
                    out.possible.insert(region.key.clone());
                    out.reasons.insert(missing_reason(
                        session,
                        "text_inventory",
                        "verification_bytes_budget_exceeded",
                    ));
                } else {
                    out.unseen = true;
                    out.reasons
                        .insert("literal:verification_bytes_budget_exceeded".to_owned());
                }
            }
            // A Quill-verified hit whose COMPLETE evidence cannot
            // reproduce the match (analyzer drift, a canonical rewrite
            // after publication — a hit past the returned-text window
            // now escalates to the whole body, and the literal matcher
            // reads the analyzer's own token boundaries, so neither a
            // plain past-the-bound hit nor an index-proven hit on
            // `_`/`-`/`$`-glued text lands here anymore) is POSSIBLE
            // membership — honest
            // uncertainty, never silently dropped or admitted, and
            // always REASONED: an
            // Unknown without a reason would make the strict refusal
            // below report `facts_pending` over an EMPTY reason list (a
            // vacuous all()), claiming reindexable facts where the
            // evidence says the archived text could not verify the hit.
            // Only a gated candidate earns `possible`: under the ungated
            // non-ASCII evaluation a non-matching region is a plain
            // non-member, not uncertainty. The reason itself is
            // session-scoped — one String per session, built once after
            // the loop: the reason set is identical either way (the set
            // deduplicates), so per-region formatting would only add
            // allocations to this hot loop.
            _ => {
                if quill_possible.is_some() {
                    out.possible.insert(region.key.clone());
                    gated_unreproduced = true;
                }
            }
        }
    }
    if gated_unreproduced {
        out.reasons.insert(missing_reason(
            session,
            "text_inventory",
            "quill_hit_not_reproduced",
        ));
    }
    // Unseen tail: the shared inventory-gap verdict (see
    // [`super::fast_lane::unseen_inventory_result`]) — identical bytes
    // for the legacy and fast lanes so an Unknown classifies the same
    // refusal either way.
    let tail = unseen_inventory_result(session);
    out.unseen = out.unseen || tail.unseen;
    out.reasons.extend(tail.reasons);
    Ok(out)
}

pub(super) fn eval_node(
    node: &PlanNode,
    session: &SessionCtx,
    connection: &Connection,
    stats: &mut HydrateStats,
    matchers: &HashMap<(String, bool), LiteralMatcher>,
    quill_possible: &HashMap<String, HashMap<i64, HashSet<i64>>>,
    seek: Option<&BlockSeekEvidence>,
) -> Result<NodeResult> {
    match node {
        PlanNode::All => Ok(NodeResult::constant(Truth::True, BTreeSet::new())),
        PlanNode::Literal {
            value,
            exact,
            branch,
            positive,
        } => {
            let key = format!("{value}|{exact}");
            let empty = HashSet::new();
            // Non-ASCII literals are invisible to the Quill analyzer
            // (verified: a Cyrillic term parses but can never hit), so the
            // hit gate cannot prove a negative for them — `None` means
            // "evaluate every persisted region through the matcher" and
            // keeps Unicode membership truthful instead of silently
            // reporting the index's blind spot as absence.
            let hits = if value.is_ascii() {
                Some(
                    quill_possible
                        .get(&key)
                        .and_then(|by_session| by_session.get(&session.conversation_id))
                        .unwrap_or(&empty),
                )
            } else {
                None
            };
            eval_literal(
                value, *exact, branch, *positive, session, connection, stats, matchers, hits,
            )
        }
        PlanNode::Predicate {
            field,
            op,
            value,
            domain,
        } => Ok(eval_predicate(field, op, value, *domain, session, seek)),
        PlanNode::Not(child) => Ok(eval_node(
            child,
            session,
            connection,
            stats,
            matchers,
            quill_possible,
            seek,
        )?
        .negate()),
        PlanNode::And(children) => {
            let mut result = NodeResult::constant(Truth::True, BTreeSet::new());
            for child in children {
                let next = eval_node(
                    child,
                    session,
                    connection,
                    stats,
                    matchers,
                    quill_possible,
                    seek,
                )?;
                result = combine(&result, &next, true);
                if result.truth() == Truth::False {
                    break;
                }
            }
            Ok(result)
        }
        PlanNode::Or(children) => {
            let mut result = NodeResult::constant(Truth::False, BTreeSet::new());
            for child in children {
                let next = eval_node(
                    child,
                    session,
                    connection,
                    stats,
                    matchers,
                    quill_possible,
                    seek,
                )?;
                result = combine(&result, &next, false);
            }
            Ok(result)
        }
    }
}

// ---------------------------------------------------------------------------
// Controls (QueryControls port over the ListFilters surface)
// ---------------------------------------------------------------------------

pub(super) fn control_result(filters: &ListFilters, session: &SessionCtx) -> NodeResult {
    let mut checks: Vec<(Truth, &'static str)> = Vec::new();
    if let Some(provider) = filters.provider.as_deref()
        && !matches!(
            provider.trim().to_lowercase().as_str(),
            "" | "all" | "all_providers"
        )
    {
        let expected = identity::normalize_agent_slug(provider).unwrap_or_default();
        checks.push((
            if session.provider == expected {
                Truth::True
            } else {
                Truth::False
            },
            "provider",
        ));
    }
    if let Some(project) = filters.project.as_deref()
        && !matches!(
            project.trim().to_lowercase().as_str(),
            "" | "all" | "all_projects"
        )
    {
        let hit = ascii_fold(session.project.as_deref().unwrap_or_default()) == ascii_fold(project);
        checks.push((if hit { Truth::True } else { Truth::False }, "project"));
    }
    if let Some(tag) = filters.tag.as_deref() {
        let folded = ascii_fold(&annotations::sanitize_tag(tag));
        let hit = session.tags.iter().any(|t| ascii_fold(t) == folded);
        checks.push((if hit { Truth::True } else { Truth::False }, "tag"));
    }
    if let Some(pinned) = filters.pinned {
        checks.push((
            if session.is_pinned == pinned {
                Truth::True
            } else {
                Truth::False
            },
            "is_bookmarked",
        ));
    }
    if let Some(expected) = filters.is_subagent {
        checks.push((
            match session.is_subagent {
                None => Truth::Unknown,
                Some(value) => {
                    if value == expected {
                        Truth::True
                    } else {
                        Truth::False
                    }
                }
            },
            "is_subagent",
        ));
    } else if !filters.include_subagents.unwrap_or(false) {
        checks.push((
            if session.is_subagent != Some(true) {
                Truth::True
            } else {
                Truth::False
            },
            "is_subagent",
        ));
    }
    if let Some(category) = filters.category.as_deref() {
        checks.push((
            match &session.category {
                None => Truth::Unknown,
                Some(value) => {
                    if value.trim() == category.trim() {
                        Truth::True
                    } else {
                        Truth::False
                    }
                }
            },
            "category",
        ));
    }
    let activity = session.activity_ms.or(session.created_ms);
    if let Some(lower) = filters.activity_from_ms.or(filters.updated_after_ms) {
        checks.push((
            match activity {
                None => Truth::Unknown,
                Some(value) => {
                    if value >= lower {
                        Truth::True
                    } else {
                        Truth::False
                    }
                }
            },
            "updated_at",
        ));
    }
    if let Some(upper) = filters.activity_to_ms {
        checks.push((
            match activity {
                None => Truth::Unknown,
                Some(value) => {
                    if value < upper {
                        Truth::True
                    } else {
                        Truth::False
                    }
                }
            },
            "updated_at",
        ));
    }
    if let Some(lower) = filters.created_from_ms {
        checks.push((
            match session.created_ms {
                None => Truth::Unknown,
                Some(value) => {
                    if value >= lower {
                        Truth::True
                    } else {
                        Truth::False
                    }
                }
            },
            "created_at",
        ));
    }
    if let Some(upper) = filters.created_to_ms {
        checks.push((
            match session.created_ms {
                None => Truth::Unknown,
                Some(value) => {
                    if value < upper {
                        Truth::True
                    } else {
                        Truth::False
                    }
                }
            },
            "created_at",
        ));
    }
    let mut truth = Truth::True;
    let mut reasons = BTreeSet::new();
    for (check, field) in checks {
        truth = truth_and(truth, check);
        if check == Truth::Unknown {
            reasons.insert(missing_reason(session, field, "value_unknown"));
        }
    }
    NodeResult::constant(truth, reasons)
}
