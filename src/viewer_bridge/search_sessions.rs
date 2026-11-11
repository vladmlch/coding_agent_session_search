//! `search_sessions` — session-level lexical search for the
//! `cass_owned_queries_v1` read lane (plan Task 9, spec decisions 6/11/12).
//!
//! Pipeline:
//! 1. **Parse/validate the DSL in CASS.** `ast` accepts the canonical
//!    viewer `SearchAst` wire object; `query` is parsed by a bounded
//!    recursive-descent port of `viewer/search/grammar.py` (same filter
//!    families, same operator tables, malformed input falls back to one
//!    bare term — the oracle's final fallback).
//! 2. **Compile** to an explicit plan tree mirroring
//!    `dsl/compile.py`: literals, block-vs-session predicate domains,
//!    `NEQ`-as-absence for block collections, `has_error` normalization.
//! 3. **Narrow the session universe in SQL** (provider/source/project/
//!    pinned/tag/classification/time windows) — the same honest-refusal
//!    probes as `list_sessions`: explicit classification or timestamp
//!    filters on unknown facts are `facts_pending`/`data_unavailable`.
//! 4. **Evaluate every session in the bounded universe** with the
//!    three-valued region semantics ported from `dsl/regions.py` +
//!    `dsl/predicates.py` (`boundaries_known` is always true here: our
//!    regions are canonical-message/extra/metadata bound, so the
//!    `flat_proofs` machinery is unnecessary). Unknown membership is a
//!    typed refusal unless `allow_partial` opted in.
//! 5. **Quill supplies witness score evidence**: each positive literal
//!    branch enumerates its message hits inside the same pinned reader
//!    set; scores are admitted only through proven witness regions
//!    (`dsl/hit_mapping.py` semantics — never a bare engine hit).
//! 6. **Sort + generation-bound keyset page.** `relevance` (default) =
//!    best admitted witness score; explicit activity sorts reuse the
//!    canonical tail-precedence formula. `complete=false` only under the
//!    explicit `allow_partial` opt-in; `semantic`/`hybrid` are
//!    `capability_not_granted`.
//! 7. **Stage 1 session×scope fast lane (owner decision 2026-11-10).**
//!    Plans whose every literal is a single ASCII token and whose shape
//!    composes over per-session truth (see [`classify_fast_shape`]) answer
//!    from the published session×scope documents — one Quill doc per
//!    (conversation, scope) chunk staged through the same marks-
//!    disciplined region-doc lane. Membership is the per-literal posting
//!    set; NOT/AND/OR keep the three-valued set algebra; witnesses are
//!    discovered textually for the EMITTED page only. The lane engages
//!    only when EVERY universe session's marks are `published` at the
//!    current REGION_VERSION — one unconverged session keeps the whole
//!    query on the per-region lane with today's exact semantics, and
//!    `membership_source` (`session_scope_docs` | `region_literals`)
//!    receipts which lane answered. Pending marks and not-read coverage
//!    gaps refuse exactly as before. The ONE deliberate relaxation
//!    (approved): a doc-claimed member whose regions cannot textually
//!    reproduce the literal (analyzer n-gram drift) is EMITTED with
//!    `witness_count=0` plus the `witnesses_not_reproduced` warning —
//!    never silently dropped, never a whole-page refusal. Region
//!    fragments (`indexed_text`) hydrate LAZILY: the universe load is
//!    structural-only, one read per conversation on first touch
//!    (receipted as `fragments_loaded`), and the one predicate arm that
//!    reads fragments (`mcp`) keeps eager semantics through a chunked
//!    prefetch. Stage 1b plans the region READ the request spends
//!    through [`plan_region_load_mode`] — `emitted_page_sessions`
//!    (fast lane: the emitted page's conversations only, loaded at page
//!    time), `gated_quill_seeks` (legacy: per-literal Quill gates),
//!    `block_predicate_seeks` (value-provable block predicates answer
//!    through request-wide SQL value seeks + NULL-candidate probes, see
//!    [`BlockSeekEvidence`]) or the `universe_structural` fallback —
//!    receipted as
//!    `region_load_mode`/`region_load_reason`/`region_rows_loaded`.
//!    Receipt semantics (review I-2): `witness_count` is the DELIVERED
//!    witness count — textual witnesses the page could prove and
//!    deep-link, 0 under the §6.4 annotation — not a claim that the
//!    doc's membership was fully reproduced; `score_domain`
//!    (`session_docs` | `region_documents`) receipts which document set
//!    scored the page. Budget exhaustion is NEVER drift: when §6.4
//!    discovery exhausts the request's whole-body verification budget
//!    (`--text-verification-budget-bytes`), the page refuses with the
//!    typed `verification_budget_exceeded` kind naming the legacy lane's
//!    `verification_bytes_budget_exceeded` reason — never a member with
//!    `complete:true`; `witnesses_not_reproduced` stays reserved for
//!    regions that were consulted and disagreed. Bare (unquoted)
//!    literals claim membership through the engine's prefix-n-gram field,
//!    which frankensearch-quill builds from only the first
//!    `CONTENT_PREFIX_MAX_BYTES` = 4 KiB of each 64-KiB chunk's packed
//!    text (`cass_prefix_source`): a bare term whose only occurrences
//!    sit past that window is not claimed — §8.3, owner decision option
//!    1: accepted, pending an engine-side fix; exact-quoted literals
//!    match the full chunk text and are unaffected. Because the window
//!    can only UNDER-claim bare-term members, a NOT over the same
//!    literal correspondingly over-includes its complement: a session
//!    the window cannot see as a member may still carry the term
//!    past-window, and the complement keeps it. The session docs are
//!    a MEMBERSHIP source, not a text substitute: every emitted witness
//!    is still proven from the canonical region rows, and a doc claim
//!    the regions cannot reproduce is the §6.4 annotation, never a
//!    silent substitution.
//!
//! 8. **Cooperative deadline.** The request's deadline (hello's
//!    `limits.default_timeout_ms`, lowerable per request through
//!    `budget.deadline_ms`) is checked at every stage boundary — plan
//!    compile, literal enumeration, universe load, the hydration reads,
//!    the per-session eval loop, the page loop and page-time witness
//!    discovery. Expiry answers with the typed, retryable
//!    `deadline_exceeded` refusal naming the phase and the completed
//!    work; the answer is never a partial page stamped complete, and
//!    the process keeps serving (the watchdog's exit-70 backstop fires
//!    only for a request that stops making progress).
//!
//! Forbidden here (plan): no `cass_facts` FTS5, no unbounded corpus
//! regex, no silent `max_hits` truncation, no first-20-hits ⇒ first-20-
//! sessions assumption.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use anyhow::Result;
use coding_agent_search::franken_sync::compat::RowExt;
use coding_agent_search::franken_sync::{Connection, SqliteValue};
use serde::Deserialize;
use serde_json::{Map, Value, json};

use super::annotations;
use super::deadline::Deadline;
use super::identity;
use super::protocol::{ListFilters, PAGE_LIMIT, RESPONSE_BYTES, refuse};
use super::query;
use super::revisions;
use super::search::{ReaderSet, decode_hit, engine_filters, raw_page, raw_page_session_docs};
use coding_agent_search::viewer_extensions::facts::is_source_absent_gap;

// ---------------------------------------------------------------------------
// Budgets (mirroring dsl/normalize.py + evaluate.py EvaluationBudget)
// ---------------------------------------------------------------------------

const MAX_QUERY_DEPTH: usize = 32;
const MAX_QUERY_NODES: usize = 1024;
const MAX_QUERY_BYTES: usize = 65_536;
const MAX_PAREN_DEPTH: usize = 32;
/// The indexed path evaluates the full candidate universe against
/// persisted region rows — there is no universe truncation. This ceiling
/// is the explicit refusal bound for workloads whose universe itself is
/// too large to be honest evidence (refusal, not a clipped answer).
const HARD_UNIVERSE_CEILING: usize = 200_000;
/// Bound on the region text RETURNED to the client (snippets/payload) and
/// on the bounded first-chance verification window — never on what is
/// VERIFIED: literal verification sees the whole region text (see
/// [`region_text_full`]), because an index hit (or, ungated, any
/// occurrence) may live past any prefix bound.
const MAX_REGION_TEXT: usize = 64 * 1024;
/// Request-total ceiling on whole-body verification scanning — the
/// `messages.content` bytes one `search_sessions` request may match
/// against (gated escalations past the bounded window plus every
/// ungated chat-region check). The running total is checked before each
/// whole-body scan; once it reaches the ceiling no further scan runs and
/// the affected session degrades to honest uncertainty with a TYPED
/// reason — `text_inventory:<state>:verification_bytes_budget_exceeded`
/// for a gated candidate (the index-proven hit stays possible
/// membership), `literal:verification_bytes_budget_exceeded` for
/// ungated evaluation — never a silent cap. Persisted-fragment regions
/// (derived/meta/noise) verify against their stored fragment and are not
/// metered: that cost is bounded per region at publication and predates
/// this budget.
///
/// The default covers the owner's real archive with headroom (measured
/// 2026-11-09, read-only): 552,782 messages, 1.37 GB of content, largest
/// message 2,298,357 B. The ungated count budget
/// ([`MAX_UNGATED_CHAT_HYDRATIONS`]) admits at most 4,096 chat-region
/// checks whose worst-case whole-body total is the top-4,096 byte sum,
/// 196 MB; the gated escalation set is the 233 messages over 64 KiB,
/// 26 MB. 512 MiB leaves ≥2.6x margin on the adversarial ungated shape
/// and ~20x on real totals. Overridable for tests and operators through
/// the bridge's `--text-verification-budget-bytes`.
pub(crate) const MAX_TEXT_VERIFICATION_BYTES: usize = 512 * 1024 * 1024;
const MAX_LITERAL_HITS: usize = 100_000;
/// Session-doc membership chunk: how many hits one
/// [`raw_page_session_docs`] call may return before
/// [`enumerate_session_doc_hits`] re-checks the deadline.
///
/// The lane's old 256-doc `PAGE_LIMIT` multiplied the engine's per-call
/// lowering once per page — the owner-archive B2 profile measured a common
/// term paying 156 full lowerings of the same query (`test` refused at
/// "reader 0: 39936 session-doc hits paged"), so every page re-expanded
/// the two-arm glob from scratch. A 25,000-doc chunk keeps the loop shape
/// the cooperative deadline was designed around — the deadline still
/// fires at every chunk boundary, and the worst single uninterruptible
/// step (one lowering + one union count + one bounded collect) stays far
/// below the 15 s backstop grace its calibration requires — while
/// collapsing the multiplicity to at most `MAX_LITERAL_HITS /
/// SESSION_DOC_ENUM_PAGE + 1` lowerings per reader. The first chunk of
/// each reader asks for the exact total (the enumeration cap and the
/// loop's termination both hang on it); later chunks page without
/// recounting and end on the first short page.
const SESSION_DOC_ENUM_PAGE: usize = 25_000;
const CURSOR_MAX_BYTES: usize = 4_096;

// ---------------------------------------------------------------------------
// AST decode (viewer/search/ast.py wire contract)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
enum AstNode {
    #[serde(rename = "term")]
    Term {
        value: String,
        #[serde(default)]
        exact: bool,
    },
    #[serde(rename = "filter")]
    Filter {
        field: String,
        #[serde(default = "default_op")]
        op: String,
        value: Value,
    },
    #[serde(rename = "not")]
    Not { expr: Box<AstNode> },
    #[serde(rename = "and")]
    And {
        #[serde(default)]
        children: Vec<AstNode>,
    },
    #[serde(rename = "or")]
    Or {
        #[serde(default)]
        children: Vec<AstNode>,
    },
}

fn default_op() -> String {
    "EQ".to_owned()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AstEnvelope {
    #[serde(default)]
    root: Option<Box<AstNode>>,
}

/// Decode the canonical wire AST: `{"root": <node>}` or a bare node.
fn decode_ast(raw: &Value) -> Result<Option<AstNode>> {
    if raw.is_null() {
        return Ok(None);
    }
    if let Ok(envelope) = serde_json::from_value::<AstEnvelope>(raw.clone())
        && envelope.root.is_some()
    {
        return Ok(envelope.root.map(|node| *node));
    }
    let node: AstNode = serde_json::from_value(raw.clone()).map_err(|_| {
        refuse(
            "invalid_request",
            "AST is not a canonical term/filter/not/and/or node",
        )
    })?;
    Ok(Some(node))
}

/// `validate_ast`: depth/node/value budgets before compile.
fn validate_ast(root: Option<&AstNode>) -> Result<()> {
    let mut stack: Vec<(&AstNode, usize)> = root.into_iter().map(|n| (n, 1)).collect();
    let (mut count, mut size) = (0_usize, 0_usize);
    let unsupported = |msg: &str| refuse("unsupported_filter", format!("DSL tree invalid: {msg}"));
    while let Some((node, depth)) = stack.pop() {
        count += 1;
        if depth > MAX_QUERY_DEPTH || count > MAX_QUERY_NODES {
            return Err(unsupported("exceeds depth or node budget"));
        }
        match node {
            AstNode::Term { value, .. } => size += value.len(),
            AstNode::Filter { field, value, .. } => {
                size += field.len();
                if let Some(text) = value.as_str() {
                    size += text.len();
                }
                if value.as_str().is_some_and(|text| text.contains('\0')) {
                    return Err(unsupported("values must not contain NUL"));
                }
            }
            AstNode::Not { expr } => stack.push((expr, depth + 1)),
            AstNode::And { children } | AstNode::Or { children } => {
                stack.extend(children.iter().map(|child| (child, depth + 1)));
            }
        }
        if size > MAX_QUERY_BYTES {
            return Err(unsupported("values exceed the input byte budget"));
        }
    }
    Ok(())
}

fn ast_has_terms(root: Option<&AstNode>) -> bool {
    match root {
        None => false,
        Some(AstNode::Term { value, .. }) => !value.trim().is_empty(),
        Some(AstNode::Filter { .. }) => false,
        Some(AstNode::Not { expr }) => ast_has_terms(Some(expr)),
        Some(AstNode::And { children }) | Some(AstNode::Or { children }) => {
            children.iter().any(|child| ast_has_terms(Some(child)))
        }
    }
}

fn ast_contains_field(root: Option<&AstNode>, name: &str) -> bool {
    match root {
        None => false,
        Some(AstNode::Term { .. }) => false,
        Some(AstNode::Filter { field, .. }) => field.eq_ignore_ascii_case(name),
        Some(AstNode::Not { expr }) => ast_contains_field(Some(expr), name),
        Some(AstNode::And { children }) | Some(AstNode::Or { children }) => children
            .iter()
            .any(|child| ast_contains_field(Some(child), name)),
    }
}

// ---------------------------------------------------------------------------
// DSL string parser (bounded port of viewer/search/grammar.py)
// ---------------------------------------------------------------------------

const EQ_NEQ_OPS: &[(&str, &str)] = &[
    (":!=", "NEQ"),
    (":=", "EQ"),
    ("!=", "NEQ"),
    ("=", "EQ"),
    (":", "EQ"),
];
const ALL_OPS: &[(&str, &str)] = &[
    (":!=", "NEQ"),
    (":>=", "GTE"),
    (":<=", "LTE"),
    (":=", "EQ"),
    (":>", "GT"),
    (":<", "LT"),
    ("!=", "NEQ"),
    (">=", "GTE"),
    ("<=", "LTE"),
    (">", "GT"),
    ("<", "LT"),
    ("=", "EQ"),
    (":", "EQ"),
];

const SCOPE_ALIASES: &[(&str, &str)] = &[
    ("title", "title"),
    ("chat", "chat"),
    ("message", "chat"),
    ("messages", "chat"),
    ("tools", "tools"),
    ("tool", "tools"),
    ("thinking", "thinking"),
    ("thought", "thinking"),
    ("artifacts", "artifacts"),
    ("artifact", "artifacts"),
    ("file", "artifacts"),
    ("files", "artifacts"),
    ("subagents", "subagents"),
    ("subagent", "subagents"),
    ("agent", "subagents"),
];
const ALL_SCOPES: &[&str] = &[
    "title",
    "chat",
    "tools",
    "thinking",
    "artifacts",
    "subagents",
];

fn scope_alias(raw: &str) -> Option<&'static str> {
    SCOPE_ALIASES
        .iter()
        .find(|(name, _)| *name == raw)
        .map(|(_, scope)| *scope)
}

fn unescape_quoted(text: &str) -> String {
    let bytes = text.as_bytes();
    if bytes.len() >= 2
        && (bytes[0] == b'"' || bytes[0] == b'\'')
        && bytes[bytes.len() - 1] == bytes[0]
    {
        let inner = &text[1..text.len() - 1];
        let mut out = String::new();
        let mut chars = inner.chars().peekable();
        while let Some(ch) = chars.next() {
            if ch == '\\'
                && let Some(next) = chars.next()
            {
                out.push(next);
                continue;
            }
            out.push(ch);
        }
        out
    } else {
        text.to_owned()
    }
}

fn op_split(token: &str, table: &[(&str, &str)]) -> Option<(String, String, String)> {
    for (sep, op) in table {
        if let Some((field, rest)) = token.split_once(sep)
            && !field.is_empty()
        {
            return Some((field.to_owned(), (*op).to_owned(), rest.to_owned()));
        }
    }
    None
}

fn normalize_number(raw: &str) -> Option<f64> {
    let text = raw.trim().to_lowercase();
    let (digits, scale) = if let Some(head) = text.strip_suffix('k') {
        (head, 1_000.0)
    } else if let Some(head) = text.strip_suffix('m') {
        (head, 1_000_000.0)
    } else {
        (text.as_str(), 1.0)
    };
    digits.parse::<f64>().ok().map(|value| value * scale)
}

fn normalize_duration(raw: &str) -> Option<f64> {
    let text = raw.trim().to_lowercase();
    let (digits, scale) = if let Some(head) = text.strip_suffix("ms") {
        (head, 0.001)
    } else if let Some(head) = text.strip_suffix("min") {
        (head, 60.0)
    } else if let Some(head) = text.strip_suffix('m') {
        (head, 60.0)
    } else if let Some(head) = text.strip_suffix('h') {
        (head, 3600.0)
    } else if let Some(head) = text.strip_suffix('s') {
        (head, 1.0)
    } else {
        (text.as_str(), 1.0)
    };
    digits.parse::<f64>().ok().map(|value| value * scale)
}

/// `parse_filter_node`: a filter token → FilterNode, or a TermNode fallback.
fn parse_filter_token(token: &str) -> AstNode {
    let token = token.trim();
    // Comparator fields take the full operator table.
    if let Some((field, op, rest)) = op_split(token, ALL_OPS)
        && matches!(
            field.to_lowercase().as_str(),
            "exit" | "tokens" | "duration" | "cost"
        )
    {
        let field = field.to_lowercase();
        let value = if rest.is_empty() {
            None
        } else if field == "exit" {
            rest.trim().parse::<i64>().ok().map(|v| json!(v))
        } else if field == "duration" {
            normalize_duration(&rest).map(|v| json!(v))
        } else {
            let cleaned = rest.trim().trim_start_matches('$').to_owned();
            normalize_number(&cleaned).map(|v| json!(v))
        };
        if let Some(value) = value {
            return AstNode::Filter {
                field,
                op: op.to_owned(),
                value,
            };
        }
        return AstNode::Term {
            value: token.to_owned(),
            exact: false,
        };
    }
    if let Some((field_raw, op, rest)) = op_split(token, EQ_NEQ_OPS) {
        let field = field_raw.to_lowercase();
        match field.as_str() {
            "in" | "scope" => {
                if let Some(scope) = scope_alias(rest.trim().to_lowercase().as_str()) {
                    return AstNode::Filter {
                        field: "scope".to_owned(),
                        op: op.to_owned(),
                        value: json!(scope),
                    };
                }
            }
            "error" => {
                let value = rest.trim().to_lowercase();
                if matches!(value.as_str(), "agent" | "tool") {
                    return AstNode::Filter {
                        field: "error_type".to_owned(),
                        op: op.to_owned(),
                        value: json!(value),
                    };
                }
            }
            "has" => {
                let value = rest.trim().to_lowercase();
                if matches!(value.as_str(), "error" | "mcp") {
                    return AstNode::Filter {
                        field: format!("has_{value}"),
                        op: op.to_owned(),
                        value: json!(true),
                    };
                }
            }
            "is" => {
                let value = rest.trim().to_lowercase();
                let mapped = match value.as_str() {
                    "subagents" | "subagent" | "child" => Some(true),
                    "parent" => Some(false),
                    _ => None,
                };
                if let Some(mapped) = mapped {
                    return AstNode::Filter {
                        field: "is_subagent".to_owned(),
                        op: op.to_owned(),
                        value: json!(mapped),
                    };
                }
            }
            "title" | "tool" | "mcp" | "file" | "provider" | "model" | "project" | "tag"
            | "has_error" => {
                return AstNode::Filter {
                    field,
                    op: op.to_owned(),
                    value: json!(unescape_quoted(rest.trim())),
                };
            }
            _ => {}
        }
    }
    AstNode::Term {
        value: token.to_owned(),
        exact: false,
    }
}

enum LexToken {
    LParen,
    RParen,
    And,
    Or,
    Not,
    Phrase(String),
    Word(String),
}

/// Whitespace/paren lexer honoring quotes, mirroring the grammar's token
/// classes: `(` always opens (grammar keeps it a group opener only at word
/// start — the CASS port admits the stricter reading), quoted spans are
/// single phrases, `-` prefixes fold into `Not`.
fn lex_dsl(query: &str) -> Option<Vec<LexToken>> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = query.chars().collect();
    let mut pos = 0_usize;
    let mut paren_depth = 0_usize;
    while pos < chars.len() {
        let ch = chars[pos];
        if ch.is_whitespace() {
            pos += 1;
            continue;
        }
        match ch {
            '(' => {
                paren_depth += 1;
                if paren_depth > MAX_PAREN_DEPTH {
                    return None;
                }
                tokens.push(LexToken::LParen);
                pos += 1;
            }
            ')' => {
                paren_depth = paren_depth.saturating_sub(1);
                tokens.push(LexToken::RParen);
                pos += 1;
            }
            '"' | '\'' => {
                let quote = ch;
                pos += 1;
                let mut text = String::new();
                while pos < chars.len() && chars[pos] != quote {
                    if chars[pos] == '\\' && pos + 1 < chars.len() {
                        pos += 1;
                    }
                    text.push(chars[pos]);
                    pos += 1;
                }
                if pos >= chars.len() {
                    return None; // unclosed quote → term fallback
                }
                pos += 1;
                tokens.push(LexToken::Phrase(text));
            }
            _ => {
                let start = pos;
                while pos < chars.len()
                    && !chars[pos].is_whitespace()
                    && !matches!(chars[pos], '(' | ')' | '"' | '\'')
                {
                    pos += 1;
                }
                let word: String = chars[start..pos].iter().collect();
                let upper = word.to_uppercase();
                match upper.as_str() {
                    "AND" => tokens.push(LexToken::And),
                    "OR" => tokens.push(LexToken::Or),
                    "NOT" => tokens.push(LexToken::Not),
                    _ => {
                        if word.starts_with('-') && word.len() > 1 && !word[1..].starts_with('-') {
                            tokens.push(LexToken::Not);
                            tokens.push(LexToken::Word(word[1..].to_owned()));
                        } else {
                            tokens.push(LexToken::Word(word));
                        }
                    }
                }
            }
        }
    }
    Some(tokens)
}

/// Recursive-descent parse of the lexed stream: `or > and > not > primary`,
/// implicit AND between adjacent primaries, empty groups and dangling
/// operators recovered by skipping (the oracle repairs similarly).
fn parse_dsl_string(query: &str) -> Option<AstNode> {
    let tokens = lex_dsl(query)?;
    struct Parser<'a> {
        tokens: &'a [LexToken],
        pos: usize,
    }
    impl Parser<'_> {
        fn peek(&self) -> Option<&LexToken> {
            self.tokens.get(self.pos)
        }
        fn parse_or(&mut self) -> Option<AstNode> {
            let mut children = vec![self.parse_and()?];
            while matches!(self.peek(), Some(LexToken::Or)) {
                self.pos += 1;
                if let Some(next) = self.parse_and() {
                    children.push(next);
                }
            }
            if children.len() == 1 {
                children.pop()
            } else {
                Some(AstNode::Or { children })
            }
        }
        fn parse_and(&mut self) -> Option<AstNode> {
            let mut children = vec![self.parse_unary()?];
            loop {
                if matches!(self.peek(), Some(LexToken::And)) {
                    self.pos += 1;
                }
                match self.peek() {
                    Some(LexToken::RParen | LexToken::Or) | None => break,
                    _ => {
                        if let Some(next) = self.parse_unary() {
                            children.push(next);
                        }
                    }
                }
            }
            if children.len() == 1 {
                children.pop()
            } else {
                Some(AstNode::And { children })
            }
        }
        fn parse_unary(&mut self) -> Option<AstNode> {
            let mut negations = 0_usize;
            while matches!(self.peek(), Some(LexToken::Not)) {
                self.pos += 1;
                negations += 1;
            }
            let mut node = self.parse_primary()?;
            for _ in 0..negations {
                node = AstNode::Not {
                    expr: Box::new(node),
                };
            }
            Some(node)
        }
        fn parse_primary(&mut self) -> Option<AstNode> {
            match self.peek()? {
                LexToken::LParen => {
                    self.pos += 1;
                    let inner = self.parse_or();
                    if matches!(self.peek(), Some(LexToken::RParen)) {
                        self.pos += 1;
                    }
                    inner
                }
                LexToken::Phrase(text) => {
                    let text = text.clone();
                    self.pos += 1;
                    if text.is_empty() {
                        None
                    } else {
                        Some(AstNode::Term {
                            value: text,
                            exact: true,
                        })
                    }
                }
                LexToken::Word(word) => {
                    let word = word.clone();
                    self.pos += 1;
                    Some(parse_filter_token(&word))
                }
                _ => None,
            }
        }
    }
    let mut parser = Parser {
        tokens: &tokens,
        pos: 0,
    };
    let node = parser.parse_or();
    // Trailing garbage (e.g. unmatched ')') → whole query degrades to term.
    if parser.pos != tokens.len() {
        return None;
    }
    node
}

/// `SearchDslParser.parse`: strict grammar, then the oracle's final
/// fallback — one bare `TermNode` over the whole trimmed input.
fn parse_query_string(query: &str) -> Option<AstNode> {
    let clean = query.trim();
    if clean.is_empty() || matches!(clean, "()" | "(  )" | "\"\"" | "''") {
        return None;
    }
    if let Some(node) = parse_dsl_string(clean) {
        return Some(node);
    }
    Some(AstNode::Term {
        value: clean.to_owned(),
        exact: false,
    })
}

// ---------------------------------------------------------------------------
// Plan compile (dsl/compile.py)
// ---------------------------------------------------------------------------

const BLOCK_FIELDS: &[&str] = &[
    "scope",
    "tool",
    "mcp",
    "file",
    "exit",
    "error_type",
    "has_error",
];
const TEXT_FIELDS: &[&str] = &[
    "title", "tool", "mcp", "file", "provider", "model", "project", "tag",
];
const NUMERIC_FIELDS: &[&str] = &["exit", "tokens", "duration", "cost"];
const BOOL_FIELDS: &[&str] = &["has_error", "has_mcp", "is_subagent"];

#[derive(Debug, Clone)]
enum PlanNode {
    Literal {
        value: String,
        exact: bool,
        branch: String,
        positive: bool,
    },
    Predicate {
        field: String,
        op: String,
        value: Value,
        domain: Domain,
    },
    And(Vec<PlanNode>),
    Or(Vec<PlanNode>),
    Not(Box<PlanNode>),
    All,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Domain {
    Session,
    Block,
}

struct Plan {
    root: PlanNode,
    literals: Vec<(String, bool)>, // dedup'd (value, exact)
    positive_branches: HashMap<String, (String, bool)>, // branch_id -> literal
}

fn unsupported(node: &AstNode, reason: &str) -> anyhow::Error {
    let (field, op) = match node {
        AstNode::Filter { field, op, .. } => (field.as_str(), op.as_str()),
        _ => ("", ""),
    };
    refuse(
        "unsupported_filter",
        format!("{reason} (field={field};op={op})"),
    )
}

fn boolean_value(node: &AstNode, field: &str, value: &Value) -> Result<bool> {
    if let Some(flag) = value.as_bool() {
        return Ok(flag);
    }
    if let Some(text) = value.as_str() {
        let lowered = text.to_lowercase();
        let (true_set, false_set): (&[&str], &[&str]) = if field == "is_subagent" {
            (
                &["true", "1", "subagent", "child"],
                &["false", "0", "parent"],
            )
        } else {
            (&["true", "1"], &["false", "0"])
        };
        if true_set.contains(&lowered.as_str()) {
            return Ok(true);
        }
        if false_set.contains(&lowered.as_str()) {
            return Ok(false);
        }
    }
    Err(unsupported(
        node,
        "A Boolean filter requires an explicit Boolean value",
    ))
}

/// `_value`: validate/translate a FilterNode's value by field kind.
fn filter_value(node: &AstNode, field: &str, op: &str, value: &Value) -> Result<Value> {
    let name = field.to_lowercase();
    if NUMERIC_FIELDS.contains(&name.as_str()) {
        if op == "LIKE" {
            return Err(unsupported(
                node,
                "Numeric predicates require finite numeric values",
            ));
        }
        let number = match value {
            Value::Number(n) => n.as_f64(),
            _ => None,
        };
        let Some(number) = number.filter(|n| n.is_finite()) else {
            return Err(unsupported(
                node,
                "Numeric predicates require finite numeric values",
            ));
        };
        if matches!(name.as_str(), "exit" | "tokens") && number.fract() != 0.0 {
            return Err(unsupported(node, "This predicate requires an integer"));
        }
        return Ok(value.clone());
    }
    if BOOL_FIELDS.contains(&name.as_str()) {
        if !matches!(op, "EQ" | "NEQ") {
            return Err(unsupported(
                node,
                "Boolean predicates support equality and inequality",
            ));
        }
        return Ok(json!(boolean_value(node, &name, value)?));
    }
    if !(TEXT_FIELDS.contains(&name.as_str()) || matches!(name.as_str(), "scope" | "error_type")) {
        return Err(unsupported(node, "Unknown programmatic filter"));
    }
    if !(value.is_string() && matches!(op, "EQ" | "NEQ" | "LIKE")) {
        return Err(unsupported(
            node,
            "Text predicates require a string and equality/inequality/LIKE",
        ));
    }
    if name == "scope" {
        let raw = value.as_str().unwrap_or_default().to_lowercase();
        if op == "LIKE" {
            return Err(unsupported(node, "Unknown search scope"));
        }
        let Some(scope) = scope_alias(&raw) else {
            return Err(unsupported(node, "Unknown search scope"));
        };
        return Ok(json!(scope));
    }
    if name == "error_type" {
        let raw = value.as_str().unwrap_or_default().to_lowercase();
        if op == "LIKE" || !matches!(raw.as_str(), "agent" | "tool") {
            return Err(unsupported(node, "Unknown error type"));
        }
        return Ok(json!(raw));
    }
    if name == "provider" {
        return Ok(json!(identity::normalize_agent_slug(
            value.as_str().unwrap_or_default()
        )?));
    }
    Ok(value.clone())
}

/// `_Compiler.node` — recursive plan compilation with positive-branch
/// witness tracking (negation never authorizes a ranking witness).
fn compile_node(
    node: &AstNode,
    path: &str,
    positive: bool,
    has_terms: bool,
    literals: &mut Vec<(String, bool)>,
    branches: &mut HashMap<String, (String, bool)>,
) -> Result<PlanNode> {
    match node {
        AstNode::Term { value, exact } => {
            if positive && !value.trim().is_empty() {
                branches
                    .entry(path.to_owned())
                    .or_insert_with(|| (value.clone(), *exact));
            }
            let key = (value.clone(), *exact);
            if !literals.contains(&key) {
                literals.push(key);
            }
            Ok(PlanNode::Literal {
                value: value.clone(),
                exact: *exact,
                branch: path.to_owned(),
                positive,
            })
        }
        AstNode::Filter { field, op, value } => {
            let name = field.to_lowercase();
            let compiled_value = filter_value(node, &name, op, value)?;
            let domain = if BLOCK_FIELDS.contains(&name.as_str()) && has_terms {
                Domain::Block
            } else {
                Domain::Session
            };
            if name == "has_error" {
                let target = if op == "NEQ" {
                    !compiled_value.as_bool().unwrap_or(false)
                } else {
                    compiled_value.as_bool().unwrap_or(false)
                };
                let positive_plan = PlanNode::Predicate {
                    field: name,
                    op: "EQ".to_owned(),
                    value: json!(true),
                    domain,
                };
                return Ok(if target {
                    positive_plan
                } else {
                    PlanNode::Not(Box::new(positive_plan))
                });
            }
            // Inequality of a block collection means ABSENCE of the positive
            // predicate anywhere, not existence of a different block.
            if BLOCK_FIELDS.contains(&name.as_str()) && op == "NEQ" {
                return Ok(PlanNode::Not(Box::new(PlanNode::Predicate {
                    field: name,
                    op: "EQ".to_owned(),
                    value: compiled_value,
                    domain,
                })));
            }
            Ok(PlanNode::Predicate {
                field: name,
                op: op.to_owned(),
                value: compiled_value,
                domain,
            })
        }
        AstNode::Not { expr } => Ok(PlanNode::Not(Box::new(compile_node(
            expr,
            &format!("{path}.n"),
            false,
            has_terms,
            literals,
            branches,
        )?))),
        AstNode::And { children } => Ok(PlanNode::And(
            children
                .iter()
                .enumerate()
                .map(|(index, child)| {
                    compile_node(
                        child,
                        &format!("{path}.{index}"),
                        positive,
                        has_terms,
                        literals,
                        branches,
                    )
                })
                .collect::<Result<_>>()?,
        )),
        AstNode::Or { children } => Ok(PlanNode::Or(
            children
                .iter()
                .enumerate()
                .map(|(index, child)| {
                    compile_node(
                        child,
                        &format!("{path}.{index}"),
                        positive,
                        has_terms,
                        literals,
                        branches,
                    )
                })
                .collect::<Result<_>>()?,
        )),
    }
}

fn compile_plan(root: Option<AstNode>, controls_scopes: Option<&[String]>) -> Result<Plan> {
    validate_ast(root.as_ref())?;
    let mut root = root;
    let has_terms = ast_has_terms(root.as_ref());
    // `scopes` control injection (normalize.py): ANDed scope-union only
    // when the query carries terms and does not scope itself.
    if let Some(scopes) = controls_scopes
        && has_terms
        && !ast_contains_field(root.as_ref(), "scope")
    {
        let mut normalized: Vec<&str> = Vec::new();
        for raw in scopes {
            if let Some(scope) = scope_alias(raw.trim().to_lowercase().as_str())
                && !normalized.contains(&scope)
            {
                normalized.push(scope);
            }
        }
        let selected: BTreeSet<&str> = normalized.iter().copied().collect();
        let all: BTreeSet<&str> = ALL_SCOPES.iter().copied().collect();
        if !normalized.is_empty() && selected != all {
            let scope_children: Vec<AstNode> = normalized
                .iter()
                .map(|scope| AstNode::Filter {
                    field: "scope".to_owned(),
                    op: "EQ".to_owned(),
                    value: json!(scope),
                })
                .collect();
            let scope_node = if scope_children.len() == 1 {
                scope_children.into_iter().next().expect("one")
            } else {
                AstNode::Or {
                    children: scope_children,
                }
            };
            let base = root.take();
            root = Some(match base {
                Some(base) => AstNode::And {
                    children: vec![base, scope_node],
                },
                None => scope_node,
            });
        }
    }
    let (mut literals, mut branches) = (Vec::new(), HashMap::new());
    let root_plan = match root.as_ref() {
        None => PlanNode::All,
        Some(node) => compile_node(node, "q", true, has_terms, &mut literals, &mut branches)?,
    };
    Ok(Plan {
        root: root_plan,
        literals,
        positive_branches: branches,
    })
}

// ---------------------------------------------------------------------------
// Literal matcher — a checked local verification of the INDEX's own
// tokenization (bounded, never a corpus scan).
//
// The gated lane's candidate set is defined by quill's CASS analyzer
// (`frankensearch-quill` scribe: the cass schema analyzes `title`/
// `content` with the hyphen pipeline — ASCII-alphanumeric runs, `_`/`$`
// and every other non-alphanumeric scalar as separators, hyphen-joined
// runs additionally emitting their parts — and the edge-prefix fields
// split on `!is_alphanumeric` the same way). The verifier must read
// region text with THOSE boundaries or an index-proven hit cannot
// reproduce and strict search degrades the session to
// `quill_hit_not_reproduced`: the old `[^\w\-_$]+` classes (the Python
// oracle's `_compile_fts_pattern`) joined on `_`/`-`/`$`, reading
// `G5_MODE_SEARCH_README.md` as one `g5_mode_search_readme` token where
// the index reads [g5, mode, search, readme, md] — 27 sessions of
// exactly that shape kept strict `README` search refusing on the owner
// archive even after whole-body hydration. Splitting on every
// non-alphanumeric scalar mirrors the analyzer's token boundaries for
// everything it can see while staying broader where harmless (quill
// only gates candidates; this matcher is the truth oracle), and
// non-ASCII scripts the analyzer cannot see at all (Cyrillic — the
// ungated lane's reason to exist) keep their Unicode-alphanumeric
// reading. The one known remaining narrow class is CJK: the analyzer
// bigrams unspaced CJK runs while this scan sees whole runs —
// un-evidenced on the owner archive, left aligned-pending a real
// reproducer rather than guessed at.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct LiteralMatcher {
    /// Token sequence for exact phrases, or independent prefix terms.
    tokens: Vec<String>,
    exact: bool,
    /// `true` when the literal produced no matchable tokens (matches nothing).
    never: bool,
}

/// Keep only the scalars the analyzer can see — used for the
/// no-alphanumeric-run fallback in [`LiteralMatcher::new`].
fn sanitize_fts_token(token: &str) -> String {
    token.chars().filter(|ch| ch.is_alphanumeric()).collect()
}

/// Tokenize a literal the way the index analyzer tokenizes the same
/// query: split on every non-alphanumeric scalar (see the section comment).
fn literal_tokens(text: &str) -> Vec<String> {
    text.split(|ch: char| !ch.is_alphanumeric())
        .filter(|token| !token.is_empty())
        .map(str::to_owned)
        .collect()
}

/// Stream one region's tokens — the same non-alphanumeric boundaries
/// [`literal_tokens`] splits on — one borrowed slice at a time. The
/// matchers below lowercase each token the way the analyzer does.
/// Whole-body verification streams a multi-megabyte region in one pass
/// with O(phrase-length) scratch: the split runs on `char` boundaries,
/// so a token is never cut the way a byte-windowed chunk would cut it,
/// and no token vector is built.
fn scan_region_tokens(text: &str) -> impl Iterator<Item = &str> {
    text.split(|ch: char| !ch.is_alphanumeric())
        .filter(|token| !token.is_empty())
}

impl LiteralMatcher {
    fn new(value: &str, exact: bool) -> Self {
        let clean = value.trim();
        if clean.is_empty() {
            return Self {
                tokens: Vec::new(),
                exact,
                never: true,
            };
        }
        if exact {
            let tokens = literal_tokens(clean)
                .into_iter()
                .map(|token| token.to_lowercase())
                .collect::<Vec<_>>();
            let never = tokens.is_empty();
            return Self {
                tokens,
                exact,
                never,
            };
        }
        let words = literal_tokens(clean)
            .into_iter()
            .map(|token| token.to_lowercase())
            .collect::<Vec<_>>();
        if words.is_empty() {
            let fallback = sanitize_fts_token(clean).to_lowercase();
            if fallback.is_empty() {
                return Self {
                    tokens: Vec::new(),
                    exact,
                    never: true,
                };
            }
            return Self {
                tokens: vec![fallback],
                exact,
                never: false,
            };
        }
        Self {
            tokens: words,
            exact,
            never: false,
        }
    }

    /// FTS-equivalent local verification over one region's text — the
    /// whole body for whole-body callers, the bounded window for the
    /// first chance. Tokens stream one at a time, so a huge region
    /// verifies in bounded scratch; both arms return on the first proof.
    fn matches(&self, text: &str) -> bool {
        if self.never {
            return false;
        }
        if self.exact {
            // Phrase: the literal token sequence must appear contiguously
            // — a rolling window over the lowercased token stream.
            let want = self.tokens.len();
            let mut window: Vec<String> = Vec::with_capacity(want);
            for token in scan_region_tokens(text) {
                if window.len() == want {
                    if window.as_slice() == self.tokens.as_slice() {
                        return true;
                    }
                    window.remove(0);
                }
                window.push(token.to_lowercase());
            }
            window.as_slice() == self.tokens.as_slice()
        } else {
            // Prefix terms: every word must prefix-match some token
            // (independent) — each word drops out when its proof appears.
            let mut pending: Vec<&str> = self.tokens.iter().map(String::as_str).collect();
            for token in scan_region_tokens(text) {
                let lowered = token.to_lowercase();
                pending.retain(|word| !lowered.starts_with(word));
                if pending.is_empty() {
                    return true;
                }
            }
            false
        }
    }
}

// ---------------------------------------------------------------------------
// Session model + regions (FactBlock equivalents)
// ---------------------------------------------------------------------------

/// One searchable region — a persisted `viewer_search_regions` row: the
/// stable key, scope/knownness, canonical + structural anchors, predicate
/// fields and the hydration pointer. `quill_idx` is the region's Quill
/// document slot: the canonical `messages.idx` for chat regions (the
/// message document itself), or `2^40 + ordinal` — the synthetic slot the
/// region document published for derived/meta scopes. `has_fragment` is
/// the structural bit `indexed_text IS NOT NULL`; the fragment TEXT
/// itself hydrates lazily (Stage 1 §6.3): [`region_text`] memoizes one
/// per-session read on first touch, `indexed_text` is populated in place
/// only by the `mcp`-predicate prefetch (the one predicate arm that
/// reads fragments for the whole universe). `chat` regions without a
/// fragment hydrate the canonical row on demand — the ONLY message read
/// the indexed path performs.
#[derive(Debug)]
struct Region {
    key: String,
    scope: String,
    scope_known: bool,
    quill_idx: i64,
    /// The owning conversation — the lazy fragment memo is keyed
    /// `(conversation_id, region_ordinal)`.
    conversation_id: i64,
    region_ordinal: i64,
    /// Structural `indexed_text IS NOT NULL` bit from the universe load.
    has_fragment: bool,
    /// Canonical `messages.idx` anchoring this region (deep-link).
    message_idx: Option<i64>,
    message_id: Option<i64>,
    /// Structural anchors persisted for deep-link consumers; the
    /// `block_key` already pins the exact block today.
    #[allow(dead_code)]
    step_index: Option<i64>,
    #[allow(dead_code)]
    turn_index: Option<i64>,
    tool_name: Option<String>,
    file_path: Option<String>,
    exit_code: Option<i64>,
    has_error: Option<bool>,
    error_type: Option<String>,
    text_source: String,
    /// The persisted fragment — `None` at load (lazy); `Some` only after
    /// the `mcp`-predicate prefetch filled it in place.
    indexed_text: Option<String>,
}

impl Region {
    /// A canonical-row chat region: no persisted fragment (noise-chat
    /// rows carry one) and the row anchors a canonical message, so the
    /// region's evidence is the FULL `messages.content` — the same body
    /// the canonical Quill document indexed. Only these regions hydrate
    /// the canonical row, and only they can hold a hit past the 64 KiB
    /// returned-text window.
    fn hydrates_canonical_row(&self) -> bool {
        !self.has_fragment
            && self.message_id.is_some()
            && self.text_source
                == coding_agent_search::viewer_extensions::search_regions::TEXT_SOURCE_CHAT
    }
}

/// Request-level bound on ungated (non-ASCII) literal verification:
/// every chat region check costs one `messages.content` point read, so
/// an ungated literal over a large universe would otherwise hydrate the
/// whole archive one region at a time. Sessions past the budget degrade
/// to `unseen` — the honest-uncertainty path that refuses without
/// `allow_partial` — rather than issuing an unbounded read storm.
/// Derived/meta regions carry persisted fragments and stay free.
const MAX_UNGATED_CHAT_HYDRATIONS: usize = 4_096;

/// Hydration accounting for the indexed path — the diagnostics the brief
/// requires: every region/message materialization is counted so an
/// unrelated selective search provably never re-reads the corpus.
#[derive(Debug, Default)]
struct HydrateStats {
    regions_loaded: usize,
    regions_hydrated: usize,
    messages_hydrated: usize,
    /// Reads spent on ungated (non-ASCII) chat verification — bounded by
    /// [`MAX_UNGATED_CHAT_HYDRATIONS`].
    ungated_chat_hydrations: usize,
    /// Request-scoped `messages.content` memo keyed by canonical
    /// `messages.id`. The strict snapshot already proves the archive is
    /// quiescent for the whole read, so a row fetched once can answer every
    /// later region anchored to it — including page snippets — without a
    /// second point read. [`prefetch_gated_message_texts`] pre-populates it
    /// with one `IN()`-chunked read per request instead of one point read
    /// per gated region. Diagnostics keep counting hydration *calls*
    /// (`messages_hydrated`), the wire contract tests bound — the memo only
    /// removes redundant engine round-trips, never a region's accounted
    /// materialization.
    text_memo: HashMap<i64, Option<String>>,
    /// Request-scoped memo of FULL `messages.content` rows — the
    /// verification lane's whole-body text, fetched lazily only for the
    /// regions that need it (a gated hit the bounded window cannot
    /// reproduce, or an ungated chat check) so a request's resident text
    /// stays proportional to what verification actually escalated, not to
    /// the gated candidate set. `None` memoizes a proven-miss anchor.
    full_text_memo: HashMap<i64, Option<String>>,
    /// Stage 1 §6.3 lazy fragments: the persisted `indexed_text`
    /// fragments, keyed `(conversation_id, region_ordinal)`, fetched by
    /// [`hydrate_session_fragments`] on the FIRST fragment touch of a
    /// conversation — one read per session, never one per region — so a
    /// request's resident fragment text is proportional to the sessions
    /// whose evidence was actually consulted (the emitted page, the
    /// gated candidates), never to the universe.
    fragment_memo: HashMap<(i64, i64), String>,
    /// Conversations whose fragment read already ran (memo provenance).
    fragment_sessions: HashSet<i64>,
    /// Fragment reads spent this request (diagnostics:
    /// `fragments_loaded`) — one per first-touch session plus one per
    /// chunk of the `mcp`-predicate prefetch.
    fragments_loaded: usize,
    /// Whole-body text bytes scanned by literal verification this
    /// request — the meter [`MAX_TEXT_VERIFICATION_BYTES`] ceilings.
    verification_bytes: usize,
    /// The request's whole-body scan ceiling (from the bridge's
    /// `--text-verification-budget-bytes`, default
    /// [`MAX_TEXT_VERIFICATION_BYTES`]). Set once at request start.
    verification_budget: usize,
    /// Calls spent fetching whole-body text (diagnostics; the memo makes
    /// repeated checks on the same message free).
    full_messages_hydrated: usize,
}

/// UTF-8-safe bound on hydrated region text — a raw byte slice panics when
/// the budget lands inside a multibyte scalar.
fn bound_region_text(text: &str) -> String {
    let mut end = text.len().min(MAX_REGION_TEXT);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_owned()
}

/// Chunk bound for the prefetched `IN (...)` hydration read — the same
/// 256-slot probing shape the identity lane already exercises.
const HYDRATE_CHUNK: usize = 256;

/// Prefetch the exact set of `messages.content` rows the gated literals
/// can hydrate this request — one chunked set read instead of one point
/// read per gated region. The set mirrors `eval_literal`'s gate byte for
/// byte: ASCII literals only (non-ASCII evaluates ungated on demand), a
/// `matcher.never` literal's gate is never consulted, an absent
/// `(literal, session)` gate admits no regions, and only chat regions
/// without a persisted fragment hydrate. Plan short-circuiting can leave a
/// prefetched row unconsumed; that is an accounted speculative read, never
/// a semantic one — the row set is still bounded by the index-proven
/// gates, and hydration counters still measure calls.
fn prefetch_gated_message_texts(
    connection: &Connection,
    sessions: &[SessionCtx],
    literals: &[(String, bool)],
    matchers: &HashMap<(String, bool), LiteralMatcher>,
    quill_possible: &HashMap<String, HashMap<i64, HashSet<i64>>>,
    stats: &mut HydrateStats,
    deadline: &Deadline,
) -> Result<()> {
    // Literal-scoped work hoisted out of the session×literal loop: the
    // ASCII check, the `matcher.never` verdict and the
    // `"{value}|{exact}"` gate-map key all depend only on the literal, so
    // each is resolved once per literal per request instead of once per
    // pair.
    let gated_literals: Vec<Option<&HashMap<i64, HashSet<i64>>>> = literals
        .iter()
        .map(|(value, exact)| {
            if !value.is_ascii()
                || matchers
                    .get(&(value.clone(), *exact))
                    .is_some_and(|matcher| matcher.never)
            {
                return None;
            }
            quill_possible.get(&format!("{value}|{exact}"))
        })
        .collect();
    let mut pending: BTreeSet<i64> = BTreeSet::new();
    for session in sessions {
        for by_session in &gated_literals {
            let Some(gate) = by_session.and_then(|gates| gates.get(&session.conversation_id))
            else {
                continue;
            };
            for region in &session.regions {
                if gate.contains(&region.quill_idx)
                    && !region.has_fragment
                    && region.text_source
                        == coding_agent_search::viewer_extensions::search_regions::TEXT_SOURCE_CHAT
                    && let Some(message_id) = region.message_id
                {
                    pending.insert(message_id);
                }
            }
        }
    }
    if pending.is_empty() {
        return Ok(());
    }
    let ids: Vec<i64> = pending.into_iter().collect();
    let mut seen: HashSet<i64> = HashSet::with_capacity(ids.len());
    for chunk in ids.chunks(HYDRATE_CHUNK) {
        deadline.check(
            "hydrate",
            format!("message-text chunk of {} id(s)", chunk.len()),
        )?;
        let placeholders = (1..=chunk.len())
            .map(|i| format!("?{i}"))
            .collect::<Vec<_>>()
            .join(",");
        let params: Vec<SqliteValue> = chunk.iter().map(|id| SqliteValue::Integer(*id)).collect();
        let rows = connection.query_with_params(
            &format!("SELECT id, content FROM messages WHERE id IN ({placeholders})"),
            &params,
        )?;
        for row in &rows {
            let id: i64 = row.get_typed(0)?;
            let content: Option<String> = row.get_typed(1)?;
            seen.insert(id);
            stats
                .text_memo
                .insert(id, content.map(|t| bound_region_text(&t)));
        }
    }
    // Absent anchors memo as a proven miss — identical outcome to the
    // point read returning zero rows, without re-probing per region.
    for id in ids {
        if !seen.contains(&id) {
            stats.text_memo.insert(id, None);
        }
    }
    Ok(())
}

/// Whether the plan evaluates the one predicate arm that reads persisted
/// fragments across the WHOLE universe (`mcp` binds tool servers to the
/// fragment surface — see the pinned `mcp` arm in [`region_value`]). Such
/// plans keep today's eager fragment availability: the structural
/// universe load (Stage 1 §6.3) is followed by one chunked set read that
/// fills `indexed_text` in place, so the arm's semantics are byte-for-byte
/// what the eager loader fed it. Every other plan — the entire fast lane
/// included — never pays for fragments it did not consult.
fn plan_reads_fragments(node: &PlanNode) -> bool {
    match node {
        PlanNode::Predicate { field, .. } => field == "mcp",
        PlanNode::Not(child) => plan_reads_fragments(child),
        PlanNode::And(children) | PlanNode::Or(children) => {
            children.iter().any(plan_reads_fragments)
        }
        PlanNode::Literal { .. } | PlanNode::All => false,
    }
}

/// Stage 1b (owner decision 2026-11-10): which region rows the plan's
/// evaluation can actually consult — decided from the plan alone, before
/// the universe read, so the whole-universe region load only happens for
/// the plans that need it.
///
/// `universe_structural` — today's eager load, for that plan only:
/// - any Predicate over a block field the seek lane cannot prove
///   (`scope`, whose Unknown candidates are unconditional on coverage;
///   `mcp`, whose True arm reads persisted fragments; `exit` with a
///   non-comparison op or a non-i64 expected value), or `has_mcp` mixed
///   into a plan that needs the eager load for another reason: `collection`
///   iterates the WHOLE region set and its Unknown verdicts hinge on
///   per-session coverage × per-region scope (`unknown_field`) — state no
///   index seek can reproduce. The brief's explicit fallback path.
/// - two positive block predicates under one AND (see
///   `block_predicate_same_block_and`).
/// - any non-ASCII literal that is not a `never` matcher: the analyzer
///   cannot prove the negative, so `eval_literal`'s ungated branch
///   consults EVERY region under MAX_UNGATED_CHAT_HYDRATIONS.
///
/// `seek_block_domain` — plans whose block predicates are all
/// value-provable (tool/file/exit/error_type/has_error/has_mcp, at most
/// one positive predicate atom per AND): the exact True key sets come
/// from request-wide SQL value seeks over the region columns plus
/// NULL-candidate probes for the Unknown discharge (see
/// [`BlockSeekEvidence`]); region rows attach only where literal gates
/// pointed (the predicates never need resident rows). Receipts as
/// `block_predicate_seeks` (or `emitted_page_sessions` under the fast
/// lane, whose page strategy is unchanged).
///
/// `on_demand` — every other plan: literals are all gated (ASCII,
/// index-provable) and predicates are all session-domain, so region rows
/// are consulted only where the Quill gates pointed (legacy lane:
/// `gated_quill_seeks`; fast lane: the emitted page's sessions,
/// `emitted_page_sessions`). False regions are provable no-ops in both
/// consumers (`collection` counts only True/Unknown rows; gated
/// `eval_literal` skips every non-gated row), which is what makes loading
/// exactly the consulted rows byte-identical to loading all of them.
enum RegionLoadMode {
    /// (reason) — today's eager structural universe load; the reason is
    /// receipted in diagnostics.
    UniverseStructural(&'static str),
    /// Block-domain predicates answer through request-wide SQL value seeks
    /// over the region columns (see [`BlockSeekEvidence`]); region rows are
    /// read only where the predicates' values point (plus any literal
    /// gates), never as an eager universe load.
    SeekBlockDomain,
    /// Region rows load on demand, scaled to hit/page size.
    OnDemand,
}

/// Block-predicate fields whose per-region truth (`region_value`) is a pure
/// function of one persisted column, so the exact True key set is provable
/// by SQL value reads (tool/file through their dedicated indexes,
/// exit/error_type/has_error through a comparison-filtered narrow
/// projection — no index exists for them, but the engine-side filter still
/// returns only matching rows). `scope` is NOT here: its Unknown candidates
/// (scope_known = 0 rows) are unconditional on coverage and a value seek
/// cannot prove their absence cheaply. `mcp` is NOT here: its True arm
/// reads persisted fragments. `has_mcp` is: its only region consult is the
/// `named` existence check, which the tool_name vocabulary seek proves
/// exactly (see [`build_block_seek_evidence`]).
fn seekable_block_field(field: &str) -> bool {
    matches!(
        field,
        "tool" | "file" | "exit" | "error_type" | "has_error" | "has_mcp"
    )
}

/// Does this subtree bear a POSITIVE seekable block predicate — one whose
/// evaluation produces a region-set [`NodeResult`] that participates in AND
/// set algebra? `has_mcp` never bears (its result is a collapsed constant,
/// and constants compose under AND without key surfaces); a predicate
/// under a `Not` does not bear either (the `Not` collapses the child's sets
/// before any sibling intersection can consult them). An `Or` of atoms
/// counts as ONE atom at its parent AND (it needs one matching region, not
/// the same region for two conjuncts) — the same composition rule
/// [`classify_fast_shape`] applies to fast-lane region atoms.
fn bears_positive_block_predicate(node: &PlanNode) -> bool {
    match node {
        PlanNode::Predicate { field, .. } => {
            matches!(
                field.as_str(),
                "tool" | "file" | "exit" | "error_type" | "has_error"
            )
        }
        PlanNode::Not(_) => false,
        PlanNode::And(children) | PlanNode::Or(children) => {
            children.iter().any(bears_positive_block_predicate)
        }
        PlanNode::Literal { .. } | PlanNode::All => false,
    }
}

/// An exit predicate is seek-expressible only when the SQL comparison
/// `exit_code <op> ?N` is exactly the Rust `compare_numbers(exit as f64,
/// expected, op)` over the stored integers: a comparison op the Rust arm
/// knows (anything else makes every region Unknown) and an i64-shaped
/// expected value (a float-shaped AST value like `2.0` keeps the eager
/// lane — conservative and correct, never a silently narrowed comparison).
fn exit_predicate_seekable(op: &str, expected: &Value) -> bool {
    matches!(op, "EQ" | "NEQ" | "GT" | "GTE" | "LT" | "LTE") && expected.as_i64().is_some()
}

fn plan_region_load_mode(
    node: &PlanNode,
    matchers: &HashMap<(String, bool), LiteralMatcher>,
) -> RegionLoadMode {
    match node {
        PlanNode::Literal { value, exact, .. } => {
            let never = matchers
                .get(&(value.clone(), *exact))
                .is_some_and(|matcher| matcher.never);
            if !value.is_ascii() && !never {
                RegionLoadMode::UniverseStructural("ungated_literal")
            } else {
                RegionLoadMode::OnDemand
            }
        }
        PlanNode::Predicate {
            field, op, value, ..
        } => {
            // Eager for every region-consulting predicate the seek lane
            // cannot express (scope, mcp) and for exit comparisons the
            // seek cannot mirror (`compare_numbers` returns Unknown for
            // any other op or a non-integer expected value).
            if !seekable_block_field(field)
                || (field == "exit" && !exit_predicate_seekable(op, value))
            {
                RegionLoadMode::UniverseStructural("region_domain_predicate")
            } else {
                RegionLoadMode::SeekBlockDomain
            }
        }
        PlanNode::Not(child) => plan_region_load_mode(child, matchers),
        PlanNode::And(children) => {
            let modes: Vec<RegionLoadMode> = children
                .iter()
                .map(|child| plan_region_load_mode(child, matchers))
                .collect();
            if let Some(RegionLoadMode::UniverseStructural(reason)) =
                modes.iter().find_map(|mode| match mode {
                    RegionLoadMode::UniverseStructural(reason) => {
                        Some(RegionLoadMode::UniverseStructural(reason))
                    }
                    _ => None,
                })
            {
                return reason_mode(reason);
            }
            if modes
                .iter()
                .any(|mode| matches!(mode, RegionLoadMode::SeekBlockDomain))
            {
                // The narrowed predicate sets are exact under AND only while
                // at most ONE positive block predicate atom participates:
                // a second predicate's unknown-candidate keys (UP ∩ K cross
                // terms) are not provable by value seeks, so the shape falls
                // back to the eager lane and receipts why.
                let atoms = children
                    .iter()
                    .filter(|child| bears_positive_block_predicate(child))
                    .count();
                if atoms > 1 {
                    return RegionLoadMode::UniverseStructural("block_predicate_same_block_and");
                }
                RegionLoadMode::SeekBlockDomain
            } else {
                RegionLoadMode::OnDemand
            }
        }
        PlanNode::Or(children) => {
            // An Or evaluates EVERY arm, so any child that needs the eager
            // load (scope/mcp predicates, ungated literals) forces the eager
            // lane for the whole plan — the seek mode must never shadow a
            // later sibling's eager requirement.
            let modes: Vec<RegionLoadMode> = children
                .iter()
                .map(|child| plan_region_load_mode(child, matchers))
                .collect();
            if let Some(RegionLoadMode::UniverseStructural(reason)) =
                modes.iter().find_map(|mode| match mode {
                    RegionLoadMode::UniverseStructural(reason) => {
                        Some(RegionLoadMode::UniverseStructural(reason))
                    }
                    _ => None,
                })
            {
                return reason_mode(reason);
            }
            if modes
                .iter()
                .any(|mode| matches!(mode, RegionLoadMode::SeekBlockDomain))
            {
                RegionLoadMode::SeekBlockDomain
            } else {
                RegionLoadMode::OnDemand
            }
        }
        PlanNode::All => RegionLoadMode::OnDemand,
    }
}

/// Return the eager mode for `reason`, keeping the existing
/// `UniverseStructural` spelling in one place.
fn reason_mode(reason: &'static str) -> RegionLoadMode {
    RegionLoadMode::UniverseStructural(reason)
}

/// Fill every universe region's persisted fragment in place (the
/// `mcp`-predicate plan's eager lane): chunked `IN()` reads over the
/// conversation set — the same shape as [`prefetch_gated_message_texts`]
/// — so a fragment-reading predicate keeps its exact eager-loaded
/// semantics on the structural universe load. Counted in
/// `fragments_loaded` (diagnostics), never in `messages_hydrated`.
fn prefetch_region_fragments(
    connection: &Connection,
    sessions: &mut [SessionCtx],
    stats: &mut HydrateStats,
    deadline: &Deadline,
) -> Result<()> {
    let ids: Vec<i64> = sessions
        .iter()
        .filter(|session| session.regions.iter().any(|region| region.has_fragment))
        .map(|session| session.conversation_id)
        .collect();
    let mut by_session: HashMap<i64, HashMap<i64, String>> = HashMap::new();
    for chunk in ids.chunks(HYDRATE_CHUNK) {
        if chunk.is_empty() {
            continue;
        }
        deadline.check(
            "hydrate",
            format!("fragment chunk of {} conversation(s)", chunk.len()),
        )?;
        stats.fragments_loaded += 1;
        let placeholders = (1..=chunk.len())
            .map(|i| format!("?{i}"))
            .collect::<Vec<_>>()
            .join(",");
        let params: Vec<SqliteValue> = chunk.iter().map(|id| SqliteValue::Integer(*id)).collect();
        let rows = connection.query_with_params(
            &format!(
                "SELECT conversation_id, region_ordinal, indexed_text \
                 FROM viewer_search_regions WHERE conversation_id IN ({placeholders}) \
                 AND indexed_text IS NOT NULL"
            ),
            &params,
        )?;
        for row in &rows {
            let cid: i64 = row.get_typed(0)?;
            let ordinal: i64 = row.get_typed(1)?;
            let text: String = row.get_typed(2)?;
            by_session.entry(cid).or_default().insert(ordinal, text);
        }
    }
    for session in sessions {
        let Some(fragments) = by_session.remove(&session.conversation_id) else {
            continue;
        };
        for region in &mut session.regions {
            if region.has_fragment
                && let Some(text) = fragments.get(&region.region_ordinal)
            {
                region.indexed_text = Some(text.clone());
            }
        }
    }
    Ok(())
}

/// Stage 1b gated lane: attach ONLY the region rows the plan's Quill hit
/// gates can consult — the union, per conversation, of every gated
/// literal's `(conversation_id, quill_idx)` set (the same gate maps
/// `prefetch_gated_message_texts` and `eval_literal` filter by), read
/// through one indexed seek per conversation chunk
/// ([`regions_for_quill_slots`]). Every consumer of `session.regions` on
/// this lane is gate-filtered (`eval_literal` skips non-gated rows;
/// `prefetch_gated_message_texts` re-checks each literal's own gate;
/// witnesses and snippets only reference gated members), so a session with
/// no gated hits for any plan literal reads NOTHING — and the rows that do
/// load evaluate byte-identically to the eager universe load, in the same
/// `region_ordinal` order.
///
/// Non-ASCII and `never` literals contribute no slots: the former keep
/// the eager structural lane (see [`plan_region_load_mode`]), the latter
/// return from `eval_literal` before ever touching a region.
fn attach_gated_regions(
    connection: &Connection,
    sessions: &mut [SessionCtx],
    plan: &Plan,
    matchers: &HashMap<(String, bool), LiteralMatcher>,
    quill_possible: &HashMap<String, HashMap<i64, HashSet<i64>>>,
    stats: &mut HydrateStats,
    deadline: &Deadline,
) -> Result<()> {
    let mut slots: HashMap<i64, Vec<i64>> = HashMap::new();
    for (value, exact) in &plan.literals {
        if !value.is_ascii()
            || matchers
                .get(&(value.clone(), *exact))
                .is_some_and(|matcher| matcher.never)
        {
            continue;
        }
        let Some(gates) = quill_possible.get(&format!("{value}|{exact}")) else {
            continue;
        };
        for (cid, quill_idxs) in gates {
            slots
                .entry(*cid)
                .or_default()
                .extend(quill_idxs.iter().copied());
        }
    }
    if slots.is_empty() {
        return Ok(());
    }
    deadline.check(
        "hydrate",
        format!("gated region seeks for {} session(s)", slots.len()),
    )?;
    let rows = coding_agent_search::viewer_extensions::search_regions::regions_for_quill_slots(
        connection, &slots,
    )?;
    let mut by_cid: HashMap<i64, usize> = HashMap::with_capacity(sessions.len());
    for (index, session) in sessions.iter().enumerate() {
        by_cid.insert(session.conversation_id, index);
    }
    for (cid, rows) in rows {
        let Some(&index) = by_cid.get(&cid) else {
            continue;
        };
        let session = &mut sessions[index];
        session.regions = rows.into_iter().map(|row| region_from(cid, row)).collect();
        stats.regions_loaded += session.regions.len();
    }
    Ok(())
}

/// Sentinel `possible` key for a narrowed predicate's non-resident Unknown
/// candidates (see [`BlockSeekEvidence`]). A region key it can never be
/// (region keys never contain NUL), so it never survives an intersection
/// with a literal's member keys — it exists to keep the predicate node's
/// own verdict `Unknown` exactly where the eager lane's per-region Unknown
/// keys would have, when those regions are precisely the rows the seek lane
/// did not attach. Consumers that only read set membership through
/// `combine`/`negate`/`truth` see byte-identical verdicts and reasons.
const UNKNOWN_PREDICATE_SENTINEL: &str = "\u{0}predicate_unknown";

/// `IN()` chunk width for the value seeks (SQL placeholder budget).
const SEEK_VALUE_CHUNK: usize = 500;

/// Per-request SQL value-seek evidence for one [`RegionLoadMode::SeekBlockDomain`]
/// plan. Built once after the region attach, consulted by every predicate
/// evaluation. Three exactness pillars (the report's equivalence argument):
///
/// 1. **True keys** — per distinct predicate, the seek reads the WHOLE
///    `viewer_search_regions` table by value (the DISTINCT vocabulary is
///    filtered through the SAME Rust matcher the eager per-region arm
///    uses, then sought through `IN()`; exit/error_type/has_error compare
///    the stored column directly), so `known` is the exact resident-True
///    key set per conversation — identical to the eager `collection`'s,
///    without loading any of those rows.
/// 2. **Resident Unknown keys** — the narrowed collection still evaluates
///    `region_value` over the attached (literal-gated) residents, so every
///    Unknown key the eager lane would intersect with literal member keys
///    is present (literal members are always residents — the Quill gate
///    attached them).
/// 3. **Non-resident Unknown existence** — one NULL-candidate probe per
///    predicate field, run only when some universe session's coverage
///    actually blocks that field, answers "could a non-attached region
///    discharge Unknown here"; the sentinel key turns that into the same
///    `Unknown` verdict with the same `value_unknown` reason the eager
///    lane produced per region.
///
/// The probe conditions mirror `unknown_field` exactly: `tool`/`exit` add
/// the scope discharge (`scope_known = 0 OR scope = 'tools'`), `file`/
/// `error_type`/`has_error` probe the bare NULL column. Vocabulary reads
/// (DISTINCT column values) are index-only and NOT counted in
/// `regions_loaded`; seek and probe result rows ARE counted.
struct BlockSeekEvidence {
    /// (field, op, json(expected)) → conversation → the exact True region
    /// keys. Inserted for EVERY seekable predicate of the plan (possibly
    /// empty), so a lookup can never silently miss.
    predicates: HashMap<(String, String, String), HashMap<i64, BTreeSet<String>>>,
    /// field → conversations holding a region whose NULL value can
    /// discharge Unknown (the probe rows). `has_mcp` never appears: its
    /// Unknown is coverage-conditional only.
    probes: HashMap<String, HashSet<i64>>,
    /// Conversations with any tool name that names an MCP server
    /// (`has_mcp`'s `named` arm: ascii-fold contains "mcp", or contains
    /// '/') — proven table-wide by the same tool_name vocabulary seek.
    mcp_named: HashSet<i64>,
}

fn block_predicate_key(field: &str, op: &str, expected: &Value) -> (String, String, String) {
    (
        field.to_owned(),
        op.to_owned(),
        serde_json::to_string(expected).unwrap_or_default(),
    )
}

impl BlockSeekEvidence {
    fn keys_for(
        &self,
        field: &str,
        op: &str,
        expected: &Value,
    ) -> Option<&HashMap<i64, BTreeSet<String>>> {
        self.predicates
            .get(&block_predicate_key(field, op, expected))
    }

    /// Does this conversation hold a region whose NULL value can discharge
    /// Unknown for `field`? `None` when no probe ran (no blocking session
    /// existed — nothing can discharge Unknown).
    fn probe_hit(&self, field: &str, conversation_id: i64) -> bool {
        self.probes
            .get(field)
            .is_some_and(|candidates| candidates.contains(&conversation_id))
    }
}

/// Collect every seekable block predicate of the plan (any nesting, any
/// polarity — a `Not` child still evaluates the predicate node itself).
fn collect_seek_predicates(node: &PlanNode, out: &mut Vec<(String, String, Value)>) {
    match node {
        PlanNode::Predicate {
            field, op, value, ..
        } => {
            if seekable_block_field(field) {
                out.push((field.clone(), op.clone(), value.clone()));
            }
        }
        PlanNode::Not(child) => collect_seek_predicates(child, out),
        PlanNode::And(children) | PlanNode::Or(children) => {
            for child in children {
                collect_seek_predicates(child, out);
            }
        }
        PlanNode::Literal { .. } | PlanNode::All => {}
    }
}

/// DISTINCT non-NULL vocabulary of one region column — the value set the
/// per-field Rust matcher folds over, read once per request per column
/// (memoized by the caller). Index-only: never counted in `regions_loaded`.
fn read_distinct_column(
    connection: &Connection,
    column: &str,
    deadline: &Deadline,
) -> Result<Vec<String>> {
    deadline.check("seek", format!("DISTINCT {column} vocabulary read"))?;
    let rows = connection.query_with_params(
        &format!(
            "SELECT DISTINCT {column} FROM viewer_search_regions \
             WHERE {column} IS NOT NULL"
        ),
        &[],
    )?;
    let mut values = Vec::with_capacity(rows.len());
    for row in &rows {
        values.push(row.get_typed(0)?);
    }
    Ok(values)
}

/// Seek `(conversation_id, region_key)` pairs by an `IN()` chunk of
/// matched column values; group the keys per conversation, filtered to the
/// request's universe. Counts every returned row in `regions_loaded` —
/// the receipt means "region rows read by this request".
fn seek_region_keys_by_values(
    connection: &Connection,
    column: &str,
    values: &[&str],
    universe: &HashSet<i64>,
    stats: &mut HydrateStats,
    deadline: &Deadline,
) -> Result<HashMap<i64, BTreeSet<String>>> {
    let mut per_conversation: HashMap<i64, BTreeSet<String>> = HashMap::new();
    for chunk in values.chunks(SEEK_VALUE_CHUNK) {
        if chunk.is_empty() {
            continue;
        }
        deadline.check(
            "seek",
            format!("{column} value seek over {} value(s)", chunk.len()),
        )?;
        let placeholders = (1..=chunk.len())
            .map(|i| format!("?{i}"))
            .collect::<Vec<_>>()
            .join(",");
        let params: Vec<SqliteValue> = chunk
            .iter()
            .map(|value| SqliteValue::Text((*value).into()))
            .collect();
        let rows = connection.query_with_params(
            &format!(
                "SELECT conversation_id, region_key FROM viewer_search_regions \
                 WHERE {column} IN ({placeholders})"
            ),
            &params,
        )?;
        stats.regions_loaded += rows.len();
        for row in &rows {
            let cid: i64 = row.get_typed(0)?;
            let key: String = row.get_typed(1)?;
            if universe.contains(&cid) {
                per_conversation.entry(cid).or_default().insert(key);
            }
        }
    }
    Ok(per_conversation)
}

/// The NULL-candidate probe SQL for one field — exactly `unknown_field`'s
/// discharge condition in SQL: the region's value column is NULL and, for
/// the tools-scope fields, the scope cannot prove the region is not a
/// tools block (`scope_known = 0 OR scope = 'tools'`). One table-wide
/// bounded pass per field, only run when some universe session's coverage
/// actually blocks that field.
fn block_probe_sql(field: &str) -> &'static str {
    match field {
        "tool" => {
            "SELECT DISTINCT conversation_id FROM viewer_search_regions \
                  WHERE tool_name IS NULL AND (scope_known = 0 OR scope = 'tools')"
        }
        "exit" => {
            "SELECT DISTINCT conversation_id FROM viewer_search_regions \
                   WHERE exit_code IS NULL AND (scope_known = 0 OR scope = 'tools')"
        }
        "file" => {
            "SELECT DISTINCT conversation_id FROM viewer_search_regions \
                   WHERE file_path IS NULL"
        }
        "error_type" => {
            "SELECT DISTINCT conversation_id FROM viewer_search_regions \
                        WHERE error_type IS NULL"
        }
        "has_error" => {
            "SELECT DISTINCT conversation_id FROM viewer_search_regions \
                        WHERE has_error IS NULL"
        }
        // has_mcp needs no probe: its Unknown is coverage-conditional only.
        _ => "",
    }
}

/// Build the [`BlockSeekEvidence`] for a [`RegionLoadMode::SeekBlockDomain`]
/// plan. `None` for every other mode (no cost).
fn build_block_seek_evidence(
    connection: &Connection,
    sessions: &[SessionCtx],
    plan: &Plan,
    region_load: &RegionLoadMode,
    stats: &mut HydrateStats,
    deadline: &Deadline,
) -> Result<Option<BlockSeekEvidence>> {
    if !matches!(region_load, RegionLoadMode::SeekBlockDomain) {
        return Ok(None);
    }
    let universe: HashSet<i64> = sessions
        .iter()
        .map(|session| session.conversation_id)
        .collect();
    let mut predicates: Vec<(String, String, Value)> = Vec::new();
    collect_seek_predicates(&plan.root, &mut predicates);
    let mut evidence = BlockSeekEvidence {
        predicates: HashMap::new(),
        probes: HashMap::new(),
        mcp_named: HashSet::new(),
    };
    let mut tool_vocabulary: Option<Vec<String>> = None;
    let mut file_vocabulary: Option<Vec<String>> = None;
    let mut mcp_named_done = false;
    let mut seen: HashSet<(String, String, String)> = HashSet::new();
    for (field, op, expected) in &predicates {
        let key = block_predicate_key(field, op, expected);
        if !seen.insert(key.clone()) {
            continue;
        }
        match field.as_str() {
            "tool" => {
                if tool_vocabulary.is_none() {
                    tool_vocabulary =
                        Some(read_distinct_column(connection, "tool_name", deadline)?);
                }
                let vocabulary = tool_vocabulary.as_deref().unwrap_or(&[]);
                let pattern = expected.as_str().unwrap_or_default();
                let matched: Vec<&str> = vocabulary
                    .iter()
                    .filter(|value| tool_match(value, pattern, op))
                    .map(String::as_str)
                    .collect();
                let per_conversation = seek_region_keys_by_values(
                    connection,
                    "tool_name",
                    &matched,
                    &universe,
                    stats,
                    deadline,
                )?;
                evidence.predicates.insert(key, per_conversation);
            }
            "file" => {
                if file_vocabulary.is_none() {
                    file_vocabulary =
                        Some(read_distinct_column(connection, "file_path", deadline)?);
                }
                let vocabulary = file_vocabulary.as_deref().unwrap_or(&[]);
                let pattern = expected.as_str().unwrap_or_default();
                let matched: Vec<&str> = vocabulary
                    .iter()
                    .filter(|value| like_match(value, pattern, false, !pattern.contains('*')))
                    .map(String::as_str)
                    .collect();
                let per_conversation = seek_region_keys_by_values(
                    connection,
                    "file_path",
                    &matched,
                    &universe,
                    stats,
                    deadline,
                )?;
                evidence.predicates.insert(key, per_conversation);
            }
            "exit" => {
                // Planner guarantee: comparison op + i64 expected, so the
                // SQL comparison is exactly the Rust compare_numbers over
                // the stored integers (NULL never matches a SQL comparison,
                // matching the Rust unknown_field discharge).
                let sql_op = match op.as_str() {
                    "EQ" => "=",
                    "NEQ" => "!=",
                    "GT" => ">",
                    "GTE" => ">=",
                    "LT" => "<",
                    _ => "<=",
                };
                let bound = SqliteValue::Integer(expected.as_i64().unwrap_or_default());
                let rows = connection.query_with_params(
                    &format!(
                        "SELECT conversation_id, region_key FROM viewer_search_regions \
                         WHERE exit_code {sql_op} ?1"
                    ),
                    &[bound],
                )?;
                stats.regions_loaded += rows.len();
                let mut per_conversation: HashMap<i64, BTreeSet<String>> = HashMap::new();
                for row in &rows {
                    let cid: i64 = row.get_typed(0)?;
                    let key: String = row.get_typed(1)?;
                    if universe.contains(&cid) {
                        per_conversation.entry(cid).or_default().insert(key);
                    }
                }
                evidence.predicates.insert(key, per_conversation);
            }
            "error_type" => {
                let bound = SqliteValue::Text(expected.as_str().unwrap_or_default().into());
                let rows = connection.query_with_params(
                    "SELECT conversation_id, region_key FROM viewer_search_regions \
                     WHERE error_type = ?1",
                    &[bound],
                )?;
                stats.regions_loaded += rows.len();
                let mut per_conversation: HashMap<i64, BTreeSet<String>> = HashMap::new();
                for row in &rows {
                    let cid: i64 = row.get_typed(0)?;
                    let key: String = row.get_typed(1)?;
                    if universe.contains(&cid) {
                        per_conversation.entry(cid).or_default().insert(key);
                    }
                }
                evidence.predicates.insert(key, per_conversation);
            }
            "has_error" => {
                // The eager arm ignores the op (plain bool equality), so
                // the seek mirrors it with `=`.
                let wanted = expected.as_bool().unwrap_or(false);
                let bound = SqliteValue::Integer(i64::from(wanted));
                let rows = connection.query_with_params(
                    "SELECT conversation_id, region_key FROM viewer_search_regions \
                     WHERE has_error = ?1",
                    &[bound],
                )?;
                stats.regions_loaded += rows.len();
                let mut per_conversation: HashMap<i64, BTreeSet<String>> = HashMap::new();
                for row in &rows {
                    let cid: i64 = row.get_typed(0)?;
                    let key: String = row.get_typed(1)?;
                    if universe.contains(&cid) {
                        per_conversation.entry(cid).or_default().insert(key);
                    }
                }
                evidence.predicates.insert(key, per_conversation);
            }
            "has_mcp" => {
                // The `named` arm: any region tool_name that ascii-folds
                // to contain "mcp" or contains '/'. Table-wide vocabulary
                // seek, computed once for the whole request.
                if !mcp_named_done {
                    mcp_named_done = true;
                    if tool_vocabulary.is_none() {
                        tool_vocabulary =
                            Some(read_distinct_column(connection, "tool_name", deadline)?);
                    }
                    let vocabulary = tool_vocabulary.as_deref().unwrap_or(&[]);
                    let matched: Vec<&str> = vocabulary
                        .iter()
                        .filter(|value| {
                            let folded = ascii_fold(value);
                            folded.contains("mcp") || value.contains('/')
                        })
                        .map(String::as_str)
                        .collect();
                    if !matched.is_empty() {
                        evidence.mcp_named = seek_distinct_conversations(
                            connection,
                            "tool_name",
                            &matched,
                            &universe,
                            stats,
                            deadline,
                        )?;
                    }
                }
                // has_mcp evaluates to a constant; no per-key entry is
                // consulted, but insert one so `keys_for` can never miss.
                evidence.predicates.insert(key, HashMap::new());
            }
            _ => {
                // Unreachable: collect_seek_predicates only collects
                // seekable fields.
            }
        }
    }
    // NULL-candidate probes, one bounded pass per DISTINCT field (the
    // probe question is field-level, not per-op) that some universe
    // session's coverage actually blocks.
    let mut fields: BTreeSet<&str> = BTreeSet::new();
    for (field, _, _) in &predicates {
        if !matches!(field.as_str(), "has_mcp") {
            fields.insert(field.as_str());
        }
    }
    for field in fields {
        let coverage = coverage_field(field);
        if !sessions
            .iter()
            .any(|session| session.field_blocks_strict(coverage))
        {
            continue;
        }
        let sql = block_probe_sql(field);
        if sql.is_empty() {
            continue;
        }
        deadline.check("seek", format!("{field} NULL-candidate probe"))?;
        let rows = connection.query_with_params(sql, &[])?;
        stats.regions_loaded += rows.len();
        let mut candidates: HashSet<i64> = HashSet::new();
        for row in &rows {
            candidates.insert(row.get_typed(0)?);
        }
        evidence.probes.insert(field.to_owned(), candidates);
    }
    Ok(Some(evidence))
}

/// Conversations owning a region whose column is IN the matched values —
/// the has_mcp `named` existence set. Same seeking shape as
/// [`seek_region_keys_by_values`] (the `(conversation_id, region_key)`
/// projection SEEKs `idx_viewer_search_regions_*`; a `SELECT DISTINCT
/// conversation_id` variant plans as a full-table SCAN in the pinned fork
/// rev, pinned as a planner gap in tests/frankensqlite_compat_gates.rs),
/// deduped here in Rust. Counts every returned row in `regions_loaded` —
/// the receipt means "region rows read by this request", not "distinct
/// conversations found".
fn seek_distinct_conversations(
    connection: &Connection,
    column: &str,
    values: &[&str],
    universe: &HashSet<i64>,
    stats: &mut HydrateStats,
    deadline: &Deadline,
) -> Result<HashSet<i64>> {
    let mut found: HashSet<i64> = HashSet::new();
    for chunk in values.chunks(SEEK_VALUE_CHUNK) {
        if chunk.is_empty() {
            continue;
        }
        deadline.check(
            "seek",
            format!("{column} conversation seek over {} value(s)", chunk.len()),
        )?;
        let placeholders = (1..=chunk.len())
            .map(|i| format!("?{i}"))
            .collect::<Vec<_>>()
            .join(",");
        let params: Vec<SqliteValue> = chunk
            .iter()
            .map(|value| SqliteValue::Text((*value).into()))
            .collect();
        let rows = connection.query_with_params(
            &format!(
                "SELECT conversation_id, region_key FROM viewer_search_regions \
                 WHERE {column} IN ({placeholders})"
            ),
            &params,
        )?;
        stats.regions_loaded += rows.len();
        for row in &rows {
            let cid: i64 = row.get_typed(0)?;
            if universe.contains(&cid) {
                found.insert(cid);
            }
        }
    }
    Ok(found)
}

/// Bounded text for one persisted region — `chat` reads the canonical
/// `messages.content` row it anchors (memoized per request; the gated
/// prefetch already materialized every gated candidate in one chunked set
/// read); `derived`/`meta`/noise-chat return the persisted fragment,
/// which hydrates LAZILY (Stage 1 §6.3): one read per conversation on
/// first fragment touch, memoized by `(conversation_id, region_ordinal)`
/// in [`HydrateStats::fragment_memo`]. `has_fragment` is the structural
/// bit the universe load persisted, so a region that never carried a
/// fragment answers `None` without a read.
fn region_text(
    connection: &Connection,
    region: &Region,
    stats: &mut HydrateStats,
) -> Result<Option<String>> {
    stats.regions_hydrated += 1;
    if region.text_source
        == coding_agent_search::viewer_extensions::search_regions::TEXT_SOURCE_CHAT
        && !region.has_fragment
    {
        stats.messages_hydrated += 1;
        let Some(message_id) = region.message_id else {
            return Ok(None);
        };
        if let Some(memo) = stats.text_memo.get(&message_id) {
            return Ok(memo.clone());
        }
        // A whole-body row the verification lane already fetched answers
        // the bounded window without a second point read; memoize the
        // bound so later callers skip the re-slice.
        if let Some(full) = stats.full_text_memo.get(&message_id) {
            let bounded = full.as_ref().map(|text| bound_region_text(text));
            stats.text_memo.insert(message_id, bounded.clone());
            return Ok(bounded);
        }
        let rows = connection.query_with_params(
            "SELECT content FROM messages WHERE id = ?1 LIMIT 1",
            &[SqliteValue::Integer(message_id)],
        )?;
        let content: Option<String> = rows
            .first()
            .map(|row| row.get_typed(0))
            .transpose()?
            .flatten();
        let bounded = content.map(|text| bound_region_text(&text));
        stats.text_memo.insert(message_id, bounded.clone());
        return Ok(bounded);
    }
    if !region.has_fragment {
        return Ok(None);
    }
    // The mcp-predicate prefetch fills fragments in place (that arm has
    // no memo access); prefer it so the bytes are never fetched twice.
    if let Some(text) = region.indexed_text.clone() {
        return Ok(Some(text));
    }
    hydrate_session_fragments(connection, region.conversation_id, stats)?;
    Ok(stats
        .fragment_memo
        .get(&(region.conversation_id, region.region_ordinal))
        .cloned())
}

/// Fetch every persisted fragment of one conversation into the request
/// memo — the ONE read the lazy-fragment lane spends per conversation on
/// first touch (`fragments_loaded` receipts it). `has_fragment` is
/// exactly `indexed_text IS NOT NULL`, so the read answers every fragment
/// region of the conversation and no proven-miss bookkeeping is needed.
fn hydrate_session_fragments(
    connection: &Connection,
    conversation_id: i64,
    stats: &mut HydrateStats,
) -> Result<()> {
    if stats.fragment_sessions.contains(&conversation_id) {
        return Ok(());
    }
    stats.fragment_sessions.insert(conversation_id);
    stats.fragments_loaded += 1;
    let rows = connection.query_with_params(
        "SELECT region_ordinal, indexed_text FROM viewer_search_regions \
         WHERE conversation_id = ?1 AND indexed_text IS NOT NULL",
        &[SqliteValue::Integer(conversation_id)],
    )?;
    for row in &rows {
        let ordinal: i64 = row.get_typed(0)?;
        let text: String = row.get_typed(1)?;
        stats.fragment_memo.insert((conversation_id, ordinal), text);
    }
    Ok(())
}

/// The FULL canonical row text for one canonical-row chat region — the
/// whole `messages.content` value, memoized per request. This is the
/// verification lane: matching must see the entire body, because a hit
/// (an index-proven one past the window, or, ungated, any occurrence)
/// may live beyond any prefix bound. The 64 KiB [`MAX_REGION_TEXT`]
/// cap applies only to text RETURNED to clients (snippets/payload via
/// [`region_text`]), never to what is verified. Callers gate on
/// [`Region::hydrates_canonical_row`]; rows with a persisted fragment
/// never reach here — their evidence is the fragment itself.
fn region_text_full(
    connection: &Connection,
    region: &Region,
    stats: &mut HydrateStats,
) -> Result<Option<String>> {
    let Some(message_id) = region.message_id else {
        return Ok(None);
    };
    stats.full_messages_hydrated += 1;
    if let Some(memo) = stats.full_text_memo.get(&message_id) {
        return Ok(memo.clone());
    }
    let rows = connection.query_with_params(
        "SELECT content FROM messages WHERE id = ?1 LIMIT 1",
        &[SqliteValue::Integer(message_id)],
    )?;
    let content: Option<String> = rows
        .first()
        .map(|row| row.get_typed(0))
        .transpose()?
        .flatten();
    stats.full_text_memo.insert(message_id, content.clone());
    Ok(content)
}

struct SessionCtx {
    conversation_id: i64,
    public_id: Option<String>,
    provider: String,
    project: Option<String>,
    project_path: Option<String>,
    title: Option<String>,
    custom_title: Option<String>,
    notes: Option<String>,
    is_pinned: bool,
    tags: Vec<String>,
    is_subagent: Option<bool>,
    category: Option<String>,
    tokens: Option<i64>,
    cost: Option<f64>,
    duration: Option<f64>,
    activity_ms: Option<i64>,
    created_ms: Option<i64>,
    model: Option<String>,
    regions: Vec<Region>,
    /// Per-field coverage states (`coverage_detail_json /fields/<f>/state`
    /// of the persisted facts row, else the query-time extraction).
    coverage: HashMap<String, String>,
    /// Per-field coverage reason codes (`coverage_detail_json
    /// /fields/<f>/reasons`) — the evidence that decides whether an
    /// incomplete state is text ABSENT AT THE SOURCE (owner decision
    /// 2026-11-07: exact, never blocks strict search) or text that was
    /// NOT READ (still blocks).
    coverage_reasons: HashMap<String, Vec<String>>,
    /// Every reason code this session's facts prove as text absent at the
    /// source (sorted, deduped) — the per-item `source_text_absent`
    /// annotation. Empty for sessions without such gaps.
    source_text_absent: Vec<String>,
    /// `true` when the message inventory was truncated by budget.
    inventory_partial: bool,
    /// `true` when the persisted publication mark is fully converged —
    /// `docs_state == published` AND the mark's `region_version` equals
    /// the current REGION_VERSION. The session×scope fast lane engages
    /// only when EVERY universe member sets this (one stale session keeps
    /// the whole query on the legacy region-literal lane).
    session_docs_current: bool,
}

impl SessionCtx {
    fn effective_title(&self) -> Option<&str> {
        self.custom_title.as_deref().or(self.title.as_deref())
    }
    fn coverage_of(&self, field: &str) -> &str {
        self.coverage
            .get(field)
            .map(String::as_str)
            .unwrap_or("unavailable")
    }
    fn coverage_reasons_of(&self, field: &str) -> &[String] {
        self.coverage_reasons
            .get(field)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }
    /// `true` when the field's coverage gap is a NOT-READ gap: incomplete
    /// coverage that is not provably text absent at the source. Such a
    /// field can still hide a matching region, so it keeps strict
    /// membership `Unknown`; a source-absent gap cannot (no local record
    /// ever held the text), so it stops contributing `unseen`.
    fn field_blocks_strict(&self, field: &str) -> bool {
        let state = self.coverage_of(field);
        state != "complete" && !is_source_absent_gap(state, self.coverage_reasons_of(field))
    }
}

// ---------------------------------------------------------------------------
// Session loading — the SQL-narrowed universe
// ---------------------------------------------------------------------------

/// Load the filtered universe: every `ListFilters` dimension pushed to SQL
/// (provider/source/project/pinned/tag/classification/windows), the same
/// unknown-facts probes as `list_sessions`. Returns session skeletons in
/// `conversation_id` order.
fn load_universe(
    connection: &Connection,
    filters: &ListFilters,
) -> Result<(Vec<i64>, String, Vec<SqliteValue>)> {
    let resolved = query::resolve_filters_public(connection, filters)?;
    query::run_probes_public(connection, &resolved)?;
    let (sql, params) = resolved.universe_sql();
    let rows = connection.query_with_params(&sql, &params)?;
    let mut ids = Vec::with_capacity(rows.len());
    for row in &rows {
        ids.push(row.get_typed::<i64>(0)?);
    }
    // The unordered `IN (…)` form feeds the batched auxiliary reads — the
    // identical filter clauses, never a re-derived id list.
    let (subquery, subquery_params) = resolved.universe_subquery();
    Ok((ids, subquery, subquery_params))
}

/// `conversations` columns the session context needs (one set-based row
/// per universe member, no joins).
#[derive(Default)]
struct ConvParts {
    title: Option<String>,
    started_at: Option<i64>,
    ended_at: Option<i64>,
    last_message_created_at: Option<i64>,
    approx_tokens: Option<i64>,
    grand_total_tokens: Option<i64>,
    estimated_cost_usd: Option<f64>,
    primary_model: Option<String>,
    workspace_id: Option<i64>,
    agent_id: Option<i64>,
}

/// `viewer_annotations` fields the session context needs.
#[derive(Default)]
struct AnnParts {
    is_pinned: bool,
    custom_title: Option<String>,
    notes: Option<String>,
}

/// `viewer_session_facts` fields the session context needs.
#[derive(Default)]
struct FactParts {
    is_subagent: Option<bool>,
    category: Option<String>,
    coverage_state: Option<String>,
    coverage_detail_json: Option<String>,
}

/// One session-context projection assembled from the split set reads —
/// same fields as the old per-session SELECT and the 6-way LEFT JOIN it
/// replaced, minus tags/regions which the loader fills from its own
/// batched reads.
#[allow(clippy::too_many_arguments)]
fn session_ctx_from_parts(
    conversation_id: i64,
    conv: ConvParts,
    ann: AnnParts,
    fact: FactParts,
    activity_ms: Option<i64>,
    docs_state: Option<&str>,
    docs_version: Option<i64>,
    workspace_path: Option<String>,
    agent_slug: Option<String>,
) -> Result<SessionCtx> {
    let provider =
        identity::normalize_agent_slug(&agent_slug.unwrap_or_default()).unwrap_or_default();
    let mut coverage: HashMap<String, String> = HashMap::new();
    let mut coverage_reasons: HashMap<String, Vec<String>> = HashMap::new();
    if let Some(detail) = fact
        .coverage_detail_json
        .as_deref()
        .and_then(|t| serde_json::from_str::<Value>(t).ok())
        && let Some(fields) = detail.get("fields").and_then(Value::as_object)
    {
        for (field, entry) in fields {
            if let Some(state) = entry.get("state").and_then(Value::as_str) {
                coverage.insert(field.clone(), state.to_owned());
            }
            if let Some(reasons) = entry.get("reasons").and_then(Value::as_array) {
                let parsed: Vec<String> = reasons
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect();
                if !parsed.is_empty() {
                    coverage_reasons.insert(field.clone(), parsed);
                }
            }
        }
    }
    let facts_present = fact.coverage_state.is_some();
    if !facts_present {
        coverage.insert("is_subagent".to_owned(), "pending".to_owned());
        coverage.insert("category".to_owned(), "pending".to_owned());
    }
    let tokens = conv.grand_total_tokens.or(conv.approx_tokens);

    // The persisted publication mark decides inventory completeness: no
    // marks row (publication never committed) or a non-`published` docs
    // state (republished rows whose Quill region documents have not
    // converged, or a meta edit awaiting doc replacement) means this
    // session's region set cannot prove `False` — it degrades to honest
    // pending coverage instead of silently missing hits.
    let docs_pending =
        docs_state != Some(coding_agent_search::viewer_extensions::search_regions::DOCS_PUBLISHED);
    // Fast-lane convergence: `published` marks under the CURRENT
    // REGION_VERSION. A version-skew mark means the persisted region rows
    // predate the current chunking/packing contract, so their session×scope
    // documents are not trustworthy membership evidence — the session keeps
    // the whole query on the legacy lane (see [`search_sessions`]).
    let session_docs_current = !docs_pending
        && docs_version
            == Some(coding_agent_search::viewer_extensions::search_regions::REGION_VERSION);
    let inventory_partial = docs_pending;
    if inventory_partial {
        coverage.insert("block_inventory".to_owned(), "pending".to_owned());
        coverage.insert("text_inventory".to_owned(), "pending".to_owned());
    } else {
        // Published marks prove the region inventory whole; provider facts
        // that independently report a partial inventory keep precedence.
        coverage
            .entry("block_inventory".to_owned())
            .or_insert_with(|| "complete".to_owned());
        coverage
            .entry("text_inventory".to_owned())
            .or_insert_with(|| "complete".to_owned());
    }

    // Source-absent classification (owner decision 2026-11-07): every
    // field whose incomplete coverage is PROVEN text absent at the
    // source — all of its reason codes are on the closed allowlist —
    // contributes its reasons to the per-item annotation. The docs-pending
    // overwrite above is `pending` state, which never classifies, so
    // unconverged region documents keep blocking strict search.
    let source_text_absent: Vec<String> = coverage
        .iter()
        .filter(|(field, state)| {
            is_source_absent_gap(
                state.as_str(),
                coverage_reasons
                    .get(field.as_str())
                    .map(Vec::as_slice)
                    .unwrap_or(&[]),
            )
        })
        .flat_map(|(field, _)| {
            coverage_reasons
                .get(field.as_str())
                .map(Vec::as_slice)
                .unwrap_or(&[])
                .iter()
                .cloned()
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    Ok(SessionCtx {
        conversation_id,
        public_id: None,
        provider,
        project: workspace_path
            .as_deref()
            .map(identity::path_name)
            .filter(|name| !name.is_empty()),
        project_path: workspace_path,
        title: conv.title,
        custom_title: ann.custom_title,
        notes: ann.notes,
        is_pinned: ann.is_pinned,
        tags: Vec::new(),
        is_subagent: fact.is_subagent,
        category: fact.category,
        tokens,
        cost: conv.estimated_cost_usd,
        duration: match (conv.started_at, conv.ended_at) {
            (Some(start), Some(end)) => Some((end - start) as f64 / 1000.0),
            _ => None,
        },
        activity_ms,
        created_ms: conv.started_at,
        model: conv.primary_model,
        regions: Vec::new(),
        coverage,
        coverage_reasons,
        source_text_absent,
        inventory_partial,
        session_docs_current,
    })
}

fn region_from(
    conversation_id: i64,
    row: coding_agent_search::viewer_extensions::search_regions::RegionRow,
) -> Region {
    Region {
        key: row.region_key,
        scope: row.scope,
        scope_known: row.scope_known,
        quill_idx: row.quill_idx,
        conversation_id,
        region_ordinal: row.region_ordinal,
        has_fragment: row.has_fragment,
        message_idx: row.message_idx,
        message_id: row.message_id,
        step_index: row.step_index,
        turn_index: row.turn_index,
        tool_name: row.tool_name,
        file_path: row.file_path,
        exit_code: row.exit_code,
        has_error: row.has_error,
        error_type: row.error_type,
        text_source: row.text_source,
        indexed_text: row.indexed_text,
    }
}

/// The whole candidate universe in a handful of per-table set reads —
/// `conversations`, `viewer_annotations`, `viewer_session_facts`,
/// `conversation_tail_state`, `viewer_search_region_marks`, `viewer_tags`
/// and the persisted region inventory — each bound by the SAME
/// `IN (universe subquery)`, with `workspaces`/`agents` read once as
/// small lookup maps. A single 6-way LEFT JOIN costs one engine probe
/// per joined table per universe row, which a >50k universe cannot
/// afford inside the request deadline; splitting turns that into one
/// linear scan per table plus Rust-side key assembly. NO `messages` read
/// happens here: the persisted `viewer_search_regions` rows are the whole
/// searchable surface, and the marks row decides whether that surface is
/// complete evidence or honest `pending`/`unavailable` coverage.
fn load_sessions_indexed(
    connection: &Connection,
    universe_ids: &[i64],
    id_subquery: &str,
    id_params: &[SqliteValue],
    stats: &mut HydrateStats,
    load_universe_regions: bool,
    deadline: &Deadline,
) -> Result<Vec<SessionCtx>> {
    deadline.check("hydrate", "conversations read")?;
    let conv_rows = connection.query_with_params(
        &format!(
            "SELECT c.id, c.title, c.started_at, c.ended_at, \
             c.last_message_created_at, c.approx_tokens, c.grand_total_tokens, \
             c.estimated_cost_usd, c.primary_model, c.workspace_id, c.agent_id \
             FROM conversations c WHERE c.id IN ({id_subquery}) ORDER BY c.id"
        ),
        id_params,
    )?;
    let mut conv: HashMap<i64, ConvParts> = HashMap::with_capacity(universe_ids.len());
    for row in &conv_rows {
        let cid: i64 = row.get_typed(0)?;
        if conv
            .insert(
                cid,
                ConvParts {
                    title: row.get_typed(1)?,
                    started_at: row.get_typed(2)?,
                    ended_at: row.get_typed(3)?,
                    last_message_created_at: row.get_typed(4)?,
                    approx_tokens: row.get_typed(5)?,
                    grand_total_tokens: row.get_typed(6)?,
                    estimated_cost_usd: row.get_typed(7)?,
                    primary_model: row.get_typed(8)?,
                    workspace_id: row.get_typed(9)?,
                    agent_id: row.get_typed(10)?,
                },
            )
            .is_some()
        {
            return Err(refuse(
                "canonical_coordinates_invalid",
                "Ambiguous canonical row",
            ));
        }
    }
    deadline.check("hydrate", "viewer_annotations read")?;
    let ann_rows = connection.query_with_params(
        &format!(
            "SELECT conversation_id, is_pinned, custom_title, notes \
             FROM viewer_annotations WHERE conversation_id IN ({id_subquery})"
        ),
        id_params,
    )?;
    let mut ann: HashMap<i64, AnnParts> = HashMap::new();
    for row in &ann_rows {
        ann.insert(
            row.get_typed(0)?,
            AnnParts {
                is_pinned: row.get_typed::<Option<i64>>(1)?.unwrap_or(0) != 0,
                custom_title: row.get_typed(2)?,
                notes: row.get_typed(3)?,
            },
        );
    }
    deadline.check("hydrate", "viewer_session_facts read")?;
    let fact_rows = connection.query_with_params(
        &format!(
            "SELECT conversation_id, is_subagent, task_category, coverage_state, \
             coverage_detail_json FROM viewer_session_facts \
             WHERE conversation_id IN ({id_subquery})"
        ),
        id_params,
    )?;
    let mut facts: HashMap<i64, FactParts> = HashMap::new();
    for row in &fact_rows {
        facts.insert(
            row.get_typed(0)?,
            FactParts {
                is_subagent: row.get_typed::<Option<i64>>(1)?.map(|v| v != 0),
                category: row.get_typed(2)?,
                coverage_state: row.get_typed(3)?,
                coverage_detail_json: row.get_typed(4)?,
            },
        );
    }
    deadline.check("hydrate", "conversation_tail_state read")?;
    let tail_rows = connection.query_with_params(
        &format!(
            "SELECT conversation_id, last_message_created_at \
             FROM conversation_tail_state WHERE conversation_id IN ({id_subquery})"
        ),
        id_params,
    )?;
    let mut tail: HashMap<i64, Option<i64>> = HashMap::new();
    for row in &tail_rows {
        tail.insert(row.get_typed(0)?, row.get_typed(1)?);
    }
    deadline.check("hydrate", "region marks read")?;
    let mark_rows = connection.query_with_params(
        &format!(
            "SELECT conversation_id, docs_state, region_version \
             FROM viewer_search_region_marks \
             WHERE conversation_id IN ({id_subquery})"
        ),
        id_params,
    )?;
    let mut marks: HashMap<i64, (String, i64)> = HashMap::new();
    for row in &mark_rows {
        marks.insert(row.get_typed(0)?, (row.get_typed(1)?, row.get_typed(2)?));
    }
    // Agent/workspace slugs: small lookup tables, read once — the LEFT
    // JOIN equivalent without a probe per universe row.
    deadline.check("hydrate", "workspaces read")?;
    let workspace_rows = connection.query("SELECT id, path FROM workspaces")?;
    let mut workspaces: HashMap<i64, Option<String>> = HashMap::new();
    for row in &workspace_rows {
        workspaces.insert(row.get_typed(0)?, row.get_typed(1)?);
    }
    deadline.check("hydrate", "agents read")?;
    let agent_rows = connection.query("SELECT id, slug FROM agents")?;
    let mut agents: HashMap<i64, Option<String>> = HashMap::new();
    for row in &agent_rows {
        agents.insert(row.get_typed(0)?, row.get_typed(1)?);
    }
    deadline.check("hydrate", "viewer_tags read")?;
    let tag_rows = connection.query_with_params(
        &format!(
            "SELECT conversation_id, tag FROM viewer_tags \
             WHERE conversation_id IN ({id_subquery}) \
             ORDER BY conversation_id, tag"
        ),
        id_params,
    )?;
    let mut tags: BTreeMap<i64, Vec<String>> = BTreeMap::new();
    for row in &tag_rows {
        tags.entry(row.get_typed::<i64>(0)?)
            .or_default()
            .push(row.get_typed::<String>(1)?);
    }
    // Stage 1b: the whole-universe region read only happens for plans that
    // can consult ANY region (see [`plan_region_load_mode`]). On-demand
    // plans attach regions later — the gated (conversation, quill_idx)
    // seeks on the legacy lane, the emitted page's sessions on the fast
    // lane — so a zero-hit or rare-term request never pays the archive-
    // sized read (762k rows on the 6k-session archive).
    // The single biggest read on the eager lane — the whole-universe
    // region scan (~831k rows on the 4.9 GB owner archive). Uninterruptible
    // (lib-side, one statement), so it is bounded by the backstop grace
    // when it starts before expiry; the checks around it refuse
    // cooperatively before and after.
    deadline.check("hydrate", "universe region rows read")?;
    let region_rows = if load_universe_regions {
        Some(
            coding_agent_search::viewer_extensions::search_regions::regions_for_universe_subquery(
                connection,
                id_subquery,
                id_params,
            )?,
        )
    } else {
        None
    };
    let mut ordered = Vec::with_capacity(universe_ids.len());
    for (index, cid) in universe_ids.iter().enumerate() {
        deadline.check(
            "hydrate",
            format!("{index} of {} sessions assembled", universe_ids.len()),
        )?;
        let Some(conv_p) = conv.remove(cid) else {
            return Err(refuse(
                "canonical_coordinates_invalid",
                "Universe row vanished between read passes",
            ));
        };
        let activity_ms = tail
            .get(cid)
            .copied()
            .flatten()
            .or(conv_p.last_message_created_at)
            .or(conv_p.ended_at)
            .or(conv_p.started_at);
        let workspace_path = conv_p
            .workspace_id
            .and_then(|id| workspaces.get(&id).cloned().flatten());
        let agent_slug = conv_p
            .agent_id
            .and_then(|id| agents.get(&id).cloned().flatten());
        let (docs_state, docs_version) = marks
            .remove(cid)
            .map(|(state, version)| (Some(state), Some(version)))
            .unwrap_or((None, None));
        let mut session = session_ctx_from_parts(
            *cid,
            conv_p,
            ann.remove(cid).unwrap_or_default(),
            facts.remove(cid).unwrap_or_default(),
            activity_ms,
            docs_state.as_deref(),
            docs_version,
            workspace_path,
            agent_slug,
        )?;
        session.tags = tags.remove(cid).unwrap_or_default();
        session.regions = region_rows
            .as_ref()
            .and_then(|rows| rows.get(cid))
            .map(|rows| {
                rows.iter()
                    .cloned()
                    .map(|row| region_from(*cid, row))
                    .collect()
            })
            .unwrap_or_default();
        if load_universe_regions {
            stats.regions_loaded += session.regions.len();
        }
        ordered.push(session);
    }
    Ok(ordered)
}

// ---------------------------------------------------------------------------
// Three-valued evaluation (dsl/regions.py + dsl/predicates.py port)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Truth {
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
struct NodeResult {
    all: bool,
    known: BTreeSet<String>,
    possible_all: bool,
    possible: BTreeSet<String>,
    unseen: bool,
    witnesses: Vec<(String, String)>, // (region key, branch id)
    reasons: BTreeSet<String>,        // "field:state:reason"
}

impl NodeResult {
    fn truth(&self) -> Truth {
        if self.all || !self.known.is_empty() {
            Truth::True
        } else if self.possible_all || !self.possible.is_empty() || self.unseen {
            Truth::Unknown
        } else {
            Truth::False
        }
    }
    fn constant(truth: Truth, reasons: BTreeSet<String>) -> Self {
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
    fn negate(self) -> Self {
        Self::constant(truth_not(self.truth()), self.reasons)
    }
}

fn combine(left: &NodeResult, right: &NodeResult, conjunction: bool) -> NodeResult {
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
fn ascii_fold(text: &str) -> String {
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

fn like_match(value: &str, pattern: &str, sql_wildcards: bool, substring: bool) -> bool {
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

fn tool_match(value: &str, pattern: &str, op: &str) -> bool {
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

fn coverage_field(field: &str) -> &str {
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
fn region_value(
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
/// eager region load (see [`BlockSeekEvidence`] for the exactness
/// argument). True keys come from the request-wide value seek (the
/// resident scan re-derives any resident True for belt-and-suspenders
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

fn eval_predicate(
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
enum RegionMatch {
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

fn verify_region_text(
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
fn eval_literal(
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
    // [`unseen_inventory_result`]) — identical bytes for the legacy and
    // fast lanes so an Unknown classifies the same refusal either way.
    let tail = unseen_inventory_result(session);
    out.unseen = out.unseen || tail.unseen;
    out.reasons.extend(tail.reasons);
    Ok(out)
}

fn eval_node(
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

fn control_result(filters: &ListFilters, session: &SessionCtx) -> NodeResult {
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

// ---------------------------------------------------------------------------
// Cursor (generation-bound keyset)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct SearchCursor {
    value: Option<f64>,
    null_flag: bool,
    id: i64,
}

fn encode_cursor(
    sort: &str,
    cursor: &SearchCursor,
    generation: &super::protocol::Generation,
    digest: &str,
) -> String {
    json!({
        // ss2: value travels as its f64 bit pattern (see the comment below);
        // ss1 cursors (decimal float "v") are refused as cursor_expired.
        "k": "ss2",
        "s": sort,
        "nf": cursor.null_flag,
        // The sort value travels as its exact f64 bit pattern, never as a
        // decimal float: serde_json's float PARSE is not correctly rounded
        // for every f64 (the significand x pow10 path can land one ULP off),
        // and a keyset bound even one ULP below the true key makes the walk
        // strand every entry that ties the bound score — 302 members of
        // `cargo` on the owner archive were never emitted by a full
        // cursor walk (a one-ULP-HIGH bound instead re-emits ties forever,
        // the cycling the incumbent binary showed). Integer parse of the
        // bit pattern is exact, so encode -> decode is bit-identical.
        "v": cursor.value.map(f64::to_bits),
        "id": cursor.id,
        "g": generation,
        "q": digest,
    })
    .to_string()
}

fn decode_cursor(
    raw: &str,
    sort: &str,
    generation: &super::protocol::Generation,
    digest: &str,
) -> Result<SearchCursor> {
    let expired = || refuse("cursor_expired", "Invalid or stale search cursor");
    if raw.len() > CURSOR_MAX_BYTES || !raw.is_ascii() {
        return Err(expired());
    }
    let value: Value = serde_json::from_str(raw).map_err(|_| expired())?;
    let object = value.as_object().ok_or_else(expired)?;
    if object.get("k").and_then(Value::as_str) != Some("ss2")
        || object.get("s").and_then(Value::as_str) != Some(sort)
        || object.get("q").and_then(Value::as_str) != Some(digest)
    {
        return Err(expired());
    }
    let bound = object.get("g").ok_or_else(expired)?;
    let bound_generation: super::protocol::Generation =
        serde_json::from_value(bound.clone()).map_err(|_| expired())?;
    if bound_generation != *generation {
        return Err(expired());
    }
    let null_flag = object
        .get("nf")
        .and_then(Value::as_bool)
        .ok_or_else(expired)?;
    let id = object
        .get("id")
        .and_then(Value::as_i64)
        .ok_or_else(expired)?;
    if id <= 0 {
        return Err(expired());
    }
    let value = if null_flag {
        None
    } else {
        // Mirror of encode_cursor: "v" is the f64 bit pattern (u64), so the
        // decode never passes through serde_json's inexact float parse.
        Some(f64::from_bits(
            object
                .get("v")
                .and_then(Value::as_u64)
                .ok_or_else(expired)?,
        ))
    };
    Ok(SearchCursor {
        value,
        null_flag,
        id,
    })
}

// ---------------------------------------------------------------------------
// Quill witness enumeration
// ---------------------------------------------------------------------------

/// Per-literal message evidence: `(conversation_id -> idx -> best score)`.
type LiteralHits = HashMap<i64, HashMap<i64, f32>>;

/// The per-literal `possible` gate map `eval_literal` consults:
/// `value|exact` -> (conversation -> candidate `quill_idx` set).
type LiteralGate = HashMap<String, HashMap<i64, HashSet<i64>>>;

/// One fast-lane expected witness: `(branch_id, (literal value, exact))`
/// — a positive branch whose session-doc posting set contains the session.
type FastClaim = (String, (String, bool));

/// Enumerate ALL Quill hits for one literal query across the pinned reader
/// set (bounded by MAX_LITERAL_HITS — an honest budget, never a silent
/// page-one truncation). Returns `(hits per session per msg idx, issues)`.
fn enumerate_literal(
    readers: &ReaderSet,
    literal_text: &str,
    filters: &frankensearch::quill::query::CassQueryFilters,
    deadline: &Deadline,
) -> Result<(LiteralHits, Vec<String>)> {
    let mut hits: LiteralHits = HashMap::new();
    for (reader_index, reader) in readers.readers.iter().enumerate() {
        let mut offset = 0_usize;
        loop {
            deadline.check(
                "literal_enumeration",
                format!("reader {reader_index}: {offset} raw hits paged"),
            )?;
            let page = raw_page(reader, literal_text, filters, offset, PAGE_LIMIT)?;
            let total = page.total_count.ok_or_else(|| {
                refuse(
                    "ranking_budget_exceeded",
                    "Engine supplied no exact raw count",
                )
            })?;
            if total > MAX_LITERAL_HITS {
                return Err(refuse(
                    "ranking_budget_exceeded",
                    "Literal raw match count exceeds the enumeration cap",
                ));
            }
            for hit in &page.hits {
                let decoded = decode_hit(reader, hit, &readers.keys[reader_index])?;
                let entry = hits
                    .entry(decoded.conversation_id)
                    .or_default()
                    .entry(decoded.message_index_zero_based as i64)
                    .or_insert(decoded.score);
                if decoded.score > *entry {
                    *entry = decoded.score;
                }
            }
            offset += page.hits.len();
            if page.hits.is_empty() || offset >= total {
                break;
            }
        }
    }
    Ok((hits, Vec::new()))
}

/// Compile a literal to its Quill query string: exact → quoted phrase;
/// multi-token → space-joined terms (AND); single token → bare term.
fn literal_query_text(value: &str, exact: bool) -> String {
    if exact {
        let sanitized = value.replace('"', "");
        format!("\"{sanitized}\"")
    } else {
        value.to_owned()
    }
}

// ---------------------------------------------------------------------------
// Strict search Stage 1: the session×scope-docs fast lane
// (owner decision 2026-11-10)
// ---------------------------------------------------------------------------

/// The witness key the fast lane places in a literal's `known` set. It is
/// deliberately not a region key: membership on this lane is proven by the
/// conversation's aggregated session×scope documents, and the textual
/// witnesses are discovered later (page time, emitted sessions only). The
/// set algebra only ever compares keys for equality/intersection, so one
/// stable sentinel per literal verdict carries the truth through
/// [`combine`] exactly like a real region key does.
const SESSION_DOC_WITNESS_KEY: &str = "\u{0}session_scope_doc";

/// A literal the session×scope documents can decide: one ASCII token the
/// analyzer can see (never `never` — a token-less literal matches nothing
/// and keeps its matcher-only semantics on the legacy lane).
fn literal_session_doc_eligible(
    value: &str,
    exact: bool,
    matchers: &HashMap<(String, bool), LiteralMatcher>,
) -> bool {
    value.is_ascii()
        && matchers
            .get(&(value.to_owned(), exact))
            .is_some_and(|matcher| !matcher.never && matcher.tokens.len() == 1)
}

/// Composition shape for the fast-lane eligibility guard (Stage 1).
///
/// `safe` — the node's three-valued truth is expressible over the
/// session-doc posting sets: single-token literals are per-session
/// membership; NOT is per-arm complement; OR is per-arm union; session-
/// domain predicates read the same `SessionCtx`. AND additionally admits
/// at most ONE region-atom — two atoms under AND demand same-block
/// co-occurrence the aggregated documents cannot prove.
///
/// `region_atom` — the node's truth hinges on WHICH regions exist or
/// match: a single-token literal, or a block-domain predicate. OR/NOT
/// keep per-arm truth, so an atom under them stays decidable; an OR of
/// atoms nested under an AND counts as one atom (it needs one block, not
/// the same block for two conjuncts) but two such atoms under one AND do
/// not compose.
#[derive(Debug, Clone, Copy)]
struct FastShape {
    safe: bool,
    region_atom: bool,
}

fn classify_fast_shape(
    node: &PlanNode,
    matchers: &HashMap<(String, bool), LiteralMatcher>,
) -> FastShape {
    match node {
        PlanNode::Literal { value, exact, .. } => {
            let eligible = literal_session_doc_eligible(value, *exact, matchers);
            FastShape {
                safe: eligible,
                region_atom: eligible,
            }
        }
        PlanNode::Predicate { field, domain, .. } => FastShape {
            // `mcp` binds tool servers to the persisted fragment surface —
            // it stays on the legacy lane so its eager fragment semantics
            // (see [`plan_reads_fragments`]) are never lazily starved.
            // Every other predicate reads structural fields or the
            // `SessionCtx`, which both lanes see identically.
            safe: field != "mcp",
            region_atom: *domain == Domain::Block,
        },
        PlanNode::Not(child) => {
            let inner = classify_fast_shape(child, matchers);
            FastShape {
                safe: inner.safe,
                region_atom: false,
            }
        }
        PlanNode::Or(children) => {
            let shapes: Vec<FastShape> = children
                .iter()
                .map(|child| classify_fast_shape(child, matchers))
                .collect();
            FastShape {
                safe: shapes.iter().all(|shape| shape.safe),
                region_atom: shapes.iter().any(|shape| shape.region_atom),
            }
        }
        PlanNode::And(children) => {
            let shapes: Vec<FastShape> = children
                .iter()
                .map(|child| classify_fast_shape(child, matchers))
                .collect();
            FastShape {
                safe: shapes.iter().all(|shape| shape.safe)
                    && shapes.iter().filter(|shape| shape.region_atom).count() <= 1,
                region_atom: shapes.iter().any(|shape| shape.region_atom),
            }
        }
        PlanNode::All => FastShape {
            safe: true,
            region_atom: false,
        },
    }
}

/// Membership posting set for one literal over the session×scope
/// documents: `(conversation_id -> best chunk score)` — the fast lane's
/// whole evidence for that literal. Enumerated per reader in
/// [`SESSION_DOC_ENUM_PAGE`]-sized chunks (deadline checked at every
/// chunk boundary, honest `MAX_LITERAL_HITS` refusal on the first
/// chunk's exact total) over [`raw_page_session_docs`], so the posting
/// set contains conversations, never per-region coordinates.
fn enumerate_session_doc_hits(
    readers: &ReaderSet,
    literal: (&str, bool),
    filters: &frankensearch::quill::query::CassQueryFilters,
    deadline: &Deadline,
) -> Result<HashMap<i64, f32>> {
    let text = literal_query_text(literal.0, literal.1);
    let mut hits: HashMap<i64, f32> = HashMap::new();
    for (reader_index, reader) in readers.readers.iter().enumerate() {
        // One count walk per reader: the first chunk carries the exact
        // total (the cap check and the loop's termination both hang on
        // it); later chunks page without recounting and end on the first
        // short page, so the engine lowers the query at most
        // `MAX_LITERAL_HITS / SESSION_DOC_ENUM_PAGE + 1` times per reader
        // instead of once per 256-doc page.
        let mut offset = 0_usize;
        let mut total: Option<usize> = None;
        loop {
            deadline.check(
                "literal_enumeration",
                format!("reader {reader_index}: {offset} session-doc hits paged"),
            )?;
            let first_chunk = total.is_none();
            let page = raw_page_session_docs(
                reader,
                &text,
                filters,
                offset,
                SESSION_DOC_ENUM_PAGE,
                first_chunk,
            )?;
            if first_chunk {
                let exact = page.total_count.ok_or_else(|| {
                    refuse(
                        "ranking_budget_exceeded",
                        "Engine supplied no exact raw count",
                    )
                })?;
                if exact > MAX_LITERAL_HITS {
                    return Err(refuse(
                        "ranking_budget_exceeded",
                        "Session-doc match count exceeds the enumeration cap",
                    ));
                }
                total = Some(exact);
            }
            for hit in &page.hits {
                let decoded = decode_hit(reader, hit, &readers.keys[reader_index])?;
                let entry = hits.entry(decoded.conversation_id).or_insert(decoded.score);
                if decoded.score > *entry {
                    *entry = decoded.score;
                }
            }
            offset += page.hits.len();
            if page.hits.is_empty() || total.is_some_and(|exact| offset >= exact) {
                break;
            }
        }
    }
    Ok(hits)
}

/// (B) Page-time witness gates, memoized once per request:
/// `(value, exact)` -> `Some(conversation -> quill_idx set)` when the
/// literal's raw Quill hits enumerated completely under
/// [`MAX_LITERAL_HITS`]; `None` when the literal is too common to
/// enumerate or the engine refused — that claim's witness discovery
/// degrades to the ungated structural scan.
type WitnessGates = HashMap<(String, bool), Option<HashMap<i64, HashSet<i64>>>>;

/// Enumerate one claim literal's raw Quill hits — message docs + region
/// docs through the same `literal_query_text` -> [`raw_page`] lowering the
/// legacy lane's [`enumerate_literal`] uses, so the quill fork's
/// content-glob prefix arm is respected — into the conversation ->
/// `quill_idx` witness gate the page-time discovery consults.
///
/// The gate is an optimization with a hard floor, never a contract
/// surface: a literal too common to enumerate honestly, an engine refusal
/// of any kind (fuel, glob expansion), a page without an exact count or a
/// hit without unambiguous coordinates all return `Ok(None)` — the claim's
/// discovery then runs ungated, today's semantics verbatim. Only the
/// cooperative deadline propagates: it is honest unavailability, not an
/// enumeration outcome.
fn enumerate_witness_gate(
    readers: &ReaderSet,
    literal: (&str, bool),
    filters: &frankensearch::quill::query::CassQueryFilters,
    deadline: &Deadline,
) -> Result<Option<HashMap<i64, HashSet<i64>>>> {
    let text = literal_query_text(literal.0, literal.1);
    let mut gate: HashMap<i64, HashSet<i64>> = HashMap::new();
    for (reader_index, reader) in readers.readers.iter().enumerate() {
        let mut offset = 0_usize;
        loop {
            deadline.check(
                "witness_gate_enumeration",
                format!("reader {reader_index}: {offset} raw hits paged"),
            )?;
            // Any engine refusal degrades the gate (see the doc comment):
            // `?` is reserved for the deadline alone. Paged in
            // SESSION_DOC_ENUM_PAGE chunks — the 256-doc PAGE_LIMIT
            // multiplies the engine's per-call lowering cost (see the
            // SESSION_DOC_ENUM_PAGE doc comment).
            let Some(page) = raw_page(reader, &text, filters, offset, SESSION_DOC_ENUM_PAGE).ok()
            else {
                return Ok(None);
            };
            let Some(total) = page.total_count else {
                return Ok(None);
            };
            if total > MAX_LITERAL_HITS {
                return Ok(None);
            }
            for hit in &page.hits {
                let decoded = match decode_hit(reader, hit, &readers.keys[reader_index]) {
                    Ok(decoded) => decoded,
                    Err(_) => return Ok(None),
                };
                gate.entry(decoded.conversation_id)
                    .or_default()
                    .insert(decoded.message_index_zero_based as i64);
            }
            offset += page.hits.len();
            if page.hits.is_empty() || offset >= total {
                break;
            }
        }
    }
    Ok(Some(gate))
}

/// The legacy lane's per-literal Quill evidence bundle.
struct LegacyLiteralEvidence {
    /// `(value, exact)` hit maps for witness scoring.
    scores: HashMap<(String, bool), LiteralHits>,
    /// The `possible` gate map `eval_literal` consults.
    possible: LiteralGate,
}

/// Per-literal Quill evidence over the per-REGION documents — the exact
/// enumeration the legacy lane runs (the loop `search_sessions` carried
/// inline before Stage 1, byte-for-byte).
fn legacy_literal_evidence(
    readers: &ReaderSet,
    plan: &Plan,
    engine_filters: &frankensearch::quill::query::CassQueryFilters,
    deadline: &Deadline,
) -> Result<LegacyLiteralEvidence> {
    let mut scores: HashMap<(String, bool), LiteralHits> = HashMap::new();
    let mut possible: LiteralGate = HashMap::new();
    for (index, (value, exact)) in plan.literals.iter().enumerate() {
        deadline.check(
            "literal_enumeration",
            format!(
                "{} of {} plan literals enumerated",
                index,
                plan.literals.len()
            ),
        )?;
        let hits = if value.is_ascii() {
            let text = literal_query_text(value, *exact);
            enumerate_literal(readers, &text, engine_filters, deadline)?.0
        } else {
            // The Quill analyzer emits no tokens for non-ASCII input —
            // skip the engine call; `eval_node` gates such literals on
            // `None` (evaluate every region) instead of trusting a
            // guaranteed-empty hit set.
            LiteralHits::new()
        };
        possible.insert(
            format!("{value}|{exact}"),
            hits.iter()
                .map(|(cid, m)| (*cid, m.keys().copied().collect()))
                .collect(),
        );
        scores.insert((value.clone(), *exact), hits);
    }
    Ok(LegacyLiteralEvidence { scores, possible })
}

/// The inventory-tail verdict shared by both literal evaluators: a
/// session whose region inventory is not fully read cannot prove a
/// miss — `unseen` with today's reason strings, exactly what
/// [`eval_literal`]'s tail produces, so a fast-lane Unknown classifies
/// `facts_pending` identically to a legacy one.
fn unseen_inventory_result(session: &SessionCtx) -> NodeResult {
    let mut out = NodeResult::default();
    // Unseen: an inventory gap that could still hide a matching region.
    // Source-absent gaps (text provably non-existent in any local record,
    // owner decision 2026-11-07) cannot hide anything, so they stop
    // contributing `unseen` — the session's existing text answers exactly.
    // `inventory_partial` (docs not converged) is never source-absent.
    let unseen = session.inventory_partial
        || session.field_blocks_strict("block_inventory")
        || session.field_blocks_strict("text_inventory");
    if unseen {
        out.unseen = true;
        for field in ["block_inventory", "text_inventory"] {
            if session.inventory_partial || session.field_blocks_strict(field) {
                out.reasons.insert(format!(
                    "{field}:{}:inventory_incomplete",
                    session.coverage_of(field)
                ));
            }
        }
    }
    out
}

/// [`eval_node`] over the session×scope posting sets (the fast lane).
/// Literals are per-session membership: a doc hit is proven membership
/// (the aggregated document's tokenization decided it); a miss keeps
/// today's inventory-tail Unknown semantics through
/// [`unseen_inventory_result`]; predicates read the same `SessionCtx` and
/// region rows as the legacy lane. Witnesses are NOT discovered here —
/// they are a page-time concern on this lane (emitted sessions only).
fn eval_node_fast(
    node: &PlanNode,
    session: &SessionCtx,
    session_doc_hits: &HashMap<(String, bool), HashMap<i64, f32>>,
    seek: Option<&BlockSeekEvidence>,
) -> NodeResult {
    match node {
        PlanNode::All => NodeResult::constant(Truth::True, BTreeSet::new()),
        PlanNode::Literal { value, exact, .. } => {
            if session_doc_hits
                .get(&(value.clone(), *exact))
                .is_some_and(|hits| hits.contains_key(&session.conversation_id))
            {
                let mut out = NodeResult::default();
                out.known.insert(SESSION_DOC_WITNESS_KEY.to_owned());
                out.possible.insert(SESSION_DOC_WITNESS_KEY.to_owned());
                out
            } else {
                unseen_inventory_result(session)
            }
        }
        PlanNode::Predicate {
            field,
            op,
            value,
            domain,
        } => eval_predicate(field, op, value, *domain, session, seek),
        PlanNode::Not(child) => eval_node_fast(child, session, session_doc_hits, seek).negate(),
        PlanNode::And(children) => {
            let mut result = NodeResult::constant(Truth::True, BTreeSet::new());
            for child in children {
                let next = eval_node_fast(child, session, session_doc_hits, seek);
                result = combine(&result, &next, true);
                if result.truth() == Truth::False {
                    break;
                }
            }
            result
        }
        PlanNode::Or(children) => {
            let mut result = NodeResult::constant(Truth::False, BTreeSet::new());
            for child in children {
                let next = eval_node_fast(child, session, session_doc_hits, seek);
                result = combine(&result, &next, false);
            }
            result
        }
    }
}

/// The fast lane's best score and expected-witness claims for one accepted
/// session: every positive literal branch whose posting set contains the
/// session contributes its best chunk score, and its literal rides the
/// page-time witness discovery. Sessions admitted without any literal
/// claim (a NOT complement, a predicate-only OR arm) carry no score and
/// expect no witnesses — exactly the legacy lane's treatment of
/// witness-less members.
fn fast_score_and_claims(
    plan: &Plan,
    conversation_id: i64,
    session_doc_hits: &HashMap<(String, bool), HashMap<i64, f32>>,
) -> (Option<f64>, Vec<FastClaim>) {
    let mut best_score: Option<f64> = None;
    let mut claims: Vec<FastClaim> = Vec::new();
    for (branch, (value, exact)) in &plan.positive_branches {
        let Some(score) = session_doc_hits
            .get(&(value.clone(), *exact))
            .and_then(|hits| hits.get(&conversation_id))
        else {
            continue;
        };
        claims.push((branch.clone(), (value.clone(), *exact)));
        let score = *score as f64;
        best_score = Some(best_score.map_or(score, |best: f64| best.max(score)));
    }
    (best_score, claims)
}

/// Page-time textual witness discovery for one emitted fast-lane session
/// (Stage 1 §6.4): the doc-claimed member's regions are verified
/// textually, bounded by `max_matches`, through the same
/// [`verify_region_text`] the legacy lane used at eval time — so an
/// emitted witness binds canonical coordinates and a snippet exactly as
/// before. The region slice is the Stage 1b page-time load: the EMITTED
/// session's full structural row set (one indexed read, same
/// `region_ordinal` order the eager universe load produced), so discovery
/// consults exactly the region set it always saw. A claim no region
/// reproduces (analyzer n-gram drift) returns an empty list; the caller
/// annotates `witness_count=0` with the `witnesses_not_reproduced`
/// warning instead of refusing the page — that warning therefore names
/// ONLY regions that were consulted and disagreed. A verification budget
/// exhaustion is NOT drift: discovery reports [`FastWitnesses::BudgetExhausted`]
/// and the caller refuses the whole page with the typed
/// `verification_budget_exceeded` kind (design §7), never a member
/// emitted with `complete:true`.
#[allow(clippy::too_many_arguments)]
fn discover_fast_witnesses(
    connection: &Connection,
    regions: &[Region],
    claims: &[FastClaim],
    claim_gates: &[Option<&HashSet<i64>>],
    matchers: &HashMap<(String, bool), LiteralMatcher>,
    max_matches: usize,
    stats: &mut HydrateStats,
    deadline: &Deadline,
) -> Result<FastWitnesses> {
    let mut discovered: Vec<(String, String)> = Vec::new();
    for ((branch, (value, exact)), claim_gate) in claims.iter().zip(claim_gates) {
        if discovered.len() >= max_matches {
            break;
        }
        let Some(matcher) = matchers.get(&(value.clone(), *exact)) else {
            continue;
        };
        if matcher.never {
            continue;
        }
        for (region_index, region) in regions.iter().enumerate() {
            if discovered.len() >= max_matches {
                break;
            }
            deadline.check(
                "page",
                format!(
                    "witness discovery: region {region_index} of {}",
                    regions.len()
                ),
            )?;
            // (B) The claim's witness gate: only regions whose Quill
            // document slot produced a hit for this literal can hold a
            // witness — the same pruning `eval_literal` applies on the
            // legacy lane, over the same gate source. `claim_gate = None`
            // (the ungated fallback) evaluates every region — today's
            // semantics verbatim.
            if let Some(gate) = claim_gate
                && !gate.contains(&region.quill_idx)
            {
                continue;
            }
            // `Some(gate)` only unlocks the bounded-window shortcut in
            // `verify_region_text` — the Member/NonMember/NoText/Budget
            // verdicts are identical to the ungated call (a bounded-window
            // match implies a full-text match), so the gated and fallback
            // passes prove the same witnesses.
            match verify_region_text(connection, region, matcher, *claim_gate, stats)? {
                RegionMatch::Member => {
                    discovered.push((region.key.clone(), branch.clone()));
                }
                RegionMatch::BudgetExceeded => {
                    return Ok(FastWitnesses::BudgetExhausted);
                }
                RegionMatch::NoText | RegionMatch::NonMember => {}
            }
        }
    }
    Ok(FastWitnesses::Found(discovered))
}

/// The §6.4 discovery outcome for one emitted fast-lane session.
enum FastWitnesses {
    /// Discovery ran to its bounded end (or found `max_matches`): the
    /// (region key, branch) witnesses proven from the region rows. An
    /// empty list here is genuine consulted-and-disagreed drift.
    Found(Vec<(String, String)>),
    /// The request's whole-body verification budget ran out mid-discovery.
    /// Honest unavailability per design §7 — the page must refuse with
    /// the typed `verification_budget_exceeded` kind, mirroring the legacy
    /// lane's `verification_bytes_budget_exceeded` reason, and must never
    /// degrade into the drift annotation or a `complete:true` member.
    BudgetExhausted,
}

/// (B) The pre-(B) page-time path, verbatim: this conversation's full
/// structural region load + ungated discovery over every region. Serves
/// both fallbacks — a gated pass that found zero witnesses (§6.4 drift or
/// an out-of-gate witness) and a session one of whose claims cannot be
/// gated for it (literal over the enumeration cap, engine refusal, or no
/// raw hit in this conversation — a shared region load must not shrink an
/// ungated claim's candidate set). Returns the loaded regions alongside
/// the discovery outcome so the caller keeps ONE budget-refusal site.
fn ungated_fast_witnesses(
    connection: &Connection,
    conversation_id: i64,
    claims: &[FastClaim],
    matchers: &HashMap<(String, bool), LiteralMatcher>,
    max_matches: usize,
    stats: &mut HydrateStats,
    deadline: &Deadline,
) -> Result<(Vec<Region>, FastWitnesses)> {
    let rows = coding_agent_search::viewer_extensions::search_regions::regions_for_conversation_structural(
        connection,
        conversation_id,
    )?;
    let regions: Vec<Region> = rows
        .into_iter()
        .map(|row| region_from(conversation_id, row))
        .collect();
    stats.regions_loaded += regions.len();
    let ungated: Vec<Option<&HashSet<i64>>> = vec![None; claims.len()];
    let outcome = discover_fast_witnesses(
        connection,
        &regions,
        claims,
        &ungated,
        matchers,
        max_matches,
        stats,
        deadline,
    )?;
    Ok((regions, outcome))
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

struct EvaluatedSession {
    session: SessionCtx,
    /// Eval-time proven witnesses (legacy lane). The fast lane leaves
    /// this empty and discovers textual witnesses at PAGE time, for the
    /// emitted sessions only (Stage 1 §6.4).
    witnesses: Vec<(String, String)>,
    score: Option<f64>,
    /// `true` when membership came from the session×scope documents
    /// (Stage 1 §6.1). Receipts as `membership_source` and steers the
    /// page loop between delivered witnesses and page-time discovery.
    fast_lane: bool,
    /// The fast lane's expected witnesses: every positive literal branch
    /// whose session-doc posting set contains the session. Drives
    /// [`discover_fast_witnesses`] and the `witnesses_not_reproduced`
    /// annotation when a claim no region reproduces.
    fast_claims: Vec<FastClaim>,
}

/// `search_sessions(ast, query, filters, scopes, sort, mode, allow_partial,
/// cursor, limit, max_matches, verification_budget_bytes, deadline)` — one
/// bounded, generation-bound page. `verification_budget_bytes` is the
/// request's whole-body scan ceiling (see
/// [`MAX_TEXT_VERIFICATION_BYTES`]); `deadline` is the request's
/// cooperative deadline, checked at every stage boundary and answered with
/// the typed, retryable `deadline_exceeded` refusal — never a partial
/// answer labelled complete, never a process kill.
#[allow(clippy::too_many_arguments)]
pub fn search_sessions(
    connection: &Connection,
    readers: &ReaderSet,
    ast: Option<&Value>,
    query_text: Option<&str>,
    filters: &ListFilters,
    scopes: Option<&[String]>,
    sort: Option<&str>,
    mode: Option<&str>,
    allow_partial: bool,
    cursor: Option<&str>,
    limit: usize,
    max_matches: usize,
    verification_budget_bytes: usize,
    deadline: &Deadline,
) -> Result<Value> {
    if !revisions::extensions_installed(connection)? {
        return Err(refuse(
            "viewer_extensions_missing",
            "The viewer extension profile is not installed; run --init-extensions first",
        ));
    }
    match mode.unwrap_or("lexical") {
        "lexical" => {}
        "semantic" | "hybrid" => {
            return Err(refuse(
                "capability_not_granted",
                "Semantic/hybrid ranking is a separate ungranted capability",
            ));
        }
        other => {
            return Err(refuse(
                "unsupported_filter",
                format!("Unknown search mode: {other}"),
            ));
        }
    }
    let sort_name = sort.unwrap_or("relevance");
    if !matches!(
        sort_name,
        "relevance" | "updated_desc" | "updated_asc" | "created_desc" | "created_asc"
    ) {
        return Err(refuse(
            "unsupported_filter",
            format!("Unsupported session search sort: {sort_name}"),
        ));
    }

    // 1) DSL: AST wins over the raw query string; both feed one plan.
    let ast_root = match ast {
        Some(raw) => decode_ast(raw)?,
        None => match query_text {
            Some(text) => {
                if text.len() > MAX_QUERY_BYTES {
                    return Err(refuse(
                        "invalid_request",
                        "Query string exceeds the byte budget",
                    ));
                }
                parse_query_string(text)
            }
            None => None,
        },
    };
    let plan = compile_plan(ast_root, scopes)?;
    // The cooperative deadline: every long stage below checks it at its
    // boundaries; expiry is the typed, retryable `deadline_exceeded`
    // refusal naming the phase and the completed work — never a partial
    // answer labelled complete, never a process kill (the watchdog stays
    // armed only as the backstop). `budget.deadline_ms: 0` refuses here,
    // at the very first boundary.
    deadline.check(
        "plan_compile",
        format!("{} literal(s) planned", plan.literals.len()),
    )?;

    // Literal matchers + Quill evidence.
    let mut matchers: HashMap<(String, bool), LiteralMatcher> = HashMap::new();
    for (value, exact) in &plan.literals {
        matchers
            .entry((value.clone(), *exact))
            .or_insert_with(|| LiteralMatcher::new(value, *exact));
    }
    let engine_filters = engine_filters(&super::protocol::BridgeFilters {
        source_id: filters.source_id.clone(),
        ..Default::default()
    });
    // Stage 1 lane shape (owner decision 2026-11-10): a plan whose every
    // node is fast-safe and which carries at least one literal can answer
    // from the published session×scope documents — per-session membership
    // posting sets instead of the per-region enumeration. The lane only
    // ENGAGES when every universe session's marks are converged (the gate
    // below, after the universe load); one unconverged session keeps the
    // whole query on the legacy lane with today's exact semantics.
    let fast_eligible =
        classify_fast_shape(&plan.root, &matchers).safe && !plan.literals.is_empty();

    // Per-literal Quill evidence, per lane. The legacy lane enumerates the
    // per-region documents right here (today's order — before the universe
    // read); the fast lane defers to `enumerate_session_doc_hits` until
    // the marks gate has proven the session documents converged.
    let LegacyLiteralEvidence {
        scores: mut quill_scores,
        possible: mut quill_possible,
    } = if fast_eligible {
        LegacyLiteralEvidence {
            scores: HashMap::new(),
            possible: HashMap::new(),
        }
    } else {
        legacy_literal_evidence(readers, &plan, &engine_filters, deadline)?
    };

    // 2) Universe: SQL-narrowed candidate sessions. The indexed path
    // evaluates the whole universe against persisted rows — never silently
    // truncated. Universes beyond the hard ceiling are an explicit
    // refusal, not a clipped answer.
    deadline.check("universe_load", "universe SQL narrow")?;
    let (universe, id_subquery, id_params) = load_universe(connection, filters)?;
    let universe_size = universe.len();
    if universe_size > HARD_UNIVERSE_CEILING {
        return Err(refuse(
            "search_budget_exceeded",
            format!(
                "Session universe {universe_size} exceeds the evaluation ceiling {HARD_UNIVERSE_CEILING}"
            ),
        ));
    }

    // 3) Evaluate every session against its persisted region rows.
    let generation = revisions::generation(connection)?.unwrap_or_default();
    let mut stats = HydrateStats {
        verification_budget: verification_budget_bytes,
        ..HydrateStats::default()
    };
    let mut accepted: Vec<EvaluatedSession> = Vec::new();
    let mut unknown_reasons: BTreeMap<String, usize> = BTreeMap::new();
    let mut unknown_sessions = 0_usize;
    // Per-table set reads for the whole universe — the indexed path must
    // never degrade to a per-session query storm (a >50k universe still
    // evaluates inside the request deadline). `mut`: the legacy lane's
    // fragment prefetch (mcp plans) fills `indexed_text` in place.
    //
    // Stage 1b: the whole-universe REGION read rides along only for plans
    // that can consult any region (block-domain predicates, ungated
    // literals — see [`plan_region_load_mode`]); every other plan loads
    // its region rows on demand, scaled to hit/page size instead of
    // archive size.
    let region_load = plan_region_load_mode(&plan.root, &matchers);
    let load_universe_regions = matches!(region_load, RegionLoadMode::UniverseStructural(_));
    let mut sessions = load_sessions_indexed(
        connection,
        &universe,
        &id_subquery,
        &id_params,
        &mut stats,
        load_universe_regions,
        deadline,
    )?;
    // How many of the evaluated sessions carry source-absent (b) gaps —
    // counted over the WHOLE candidate universe, not just result items:
    // a (b) session excluded by proven absence (a NOT-match or a
    // non-member) still had its verdict decided by source-absence, and
    // the response must not hide that.
    let source_absent_sessions = sessions
        .iter()
        .filter(|session| !session.source_text_absent.is_empty())
        .count();
    // Stage 1 marks gate: the session×scope documents are trusted as
    // membership evidence only when EVERY universe session's marks row is
    // `published` at the current REGION_VERSION. One pending or
    // version-stale mark falls the whole query back to the legacy
    // per-region lane — answers and refusals stay byte-identical to
    // today, and `membership_source` receipts the lane that ran.
    let fast_lane = fast_eligible && sessions.iter().all(|session| session.session_docs_current);
    let mut session_doc_hits: HashMap<(String, bool), HashMap<i64, f32>> = HashMap::new();
    if fast_lane {
        // §6.1: one posting set per literal — `(conversation_id -> best
        // session-doc score)`. No gated-message prefetch, no fragment
        // prefetch: the fast lane never touches region text at eval time
        // (witnesses are page-time work for the emitted sessions only).
        for (value, exact) in &plan.literals {
            let hits = enumerate_session_doc_hits(
                readers,
                (value.as_str(), *exact),
                &engine_filters,
                deadline,
            )?;
            session_doc_hits.insert((value.clone(), *exact), hits);
        }
    } else {
        if fast_eligible {
            // The marks-gate fallback: the legacy per-region enumeration
            // runs here instead of before the universe read. For every
            // answerable query the two reads have no coupling, so the
            // reordering is unobservable — with one honest corner
            // (review M-1): a plan BOTH over the literal enumeration cap
            // and over the universe ceiling refuses
            // `search_budget_exceeded` here (the ceiling check ran
            // first), where a non-fast plan with the same literal and
            // universe refuses `ranking_budget_exceeded` (it enumerated
            // first). Both are honest typed refusals of the same query;
            // only the reported kind differs by lane. Pinning the corner
            // needs a >HARD_UNIVERSE_CEILING-session fixture carrying a
            // >MAX_LITERAL_HITS literal (both hard constants, no test
            // knobs) — deferred as disproportionate to the divergence.
            let LegacyLiteralEvidence { scores, possible } =
                legacy_literal_evidence(readers, &plan, &engine_filters, deadline)?;
            quill_scores = scores;
            quill_possible = possible;
        }
        // Stage 1b gated lane: attach ONLY the region rows the Quill hit
        // gates can consult (one indexed seek per conversation chunk) —
        // a session with no gated hits for any plan literal reads
        // nothing. Byte-identical evaluation: every consumer on this
        // lane is gate-filtered (see [`attach_gated_regions`]).
        if !load_universe_regions {
            attach_gated_regions(
                connection,
                &mut sessions,
                &plan,
                &matchers,
                &quill_possible,
                &mut stats,
                deadline,
            )?;
        }
        // One chunked set read for every `messages.content` row the gated
        // literals can touch — replacing a point read per gated region
        // with an O(1)-queries-per-request hydration memo.
        prefetch_gated_message_texts(
            connection,
            &sessions,
            &plan.literals,
            &matchers,
            &quill_possible,
            &mut stats,
            deadline,
        )?;
        // The one predicate arm that reads persisted fragments (`mcp`)
        // keeps its eager fragment availability under the lazy load
        // (Stage 1 §6.3 caller audit): one chunked set read fills every
        // fragment in place before evaluation.
        if plan_reads_fragments(&plan.root) {
            prefetch_region_fragments(connection, &mut sessions, &mut stats, deadline)?;
        }
    }
    // The seek lane's request-wide predicate evidence — one build over the
    // attached (literal-gated) residents, consulted by every predicate
    // evaluation on both lanes (a `has:mcp AND literal` plan rides the
    // fast lane with has_mcp proven from the mcp-named seek). `None` for
    // every other mode: zero cost.
    let block_seek = build_block_seek_evidence(
        connection,
        &sessions,
        &plan,
        &region_load,
        &mut stats,
        deadline,
    )?;
    for (index, session) in sessions.into_iter().enumerate() {
        deadline.check(
            "evaluate",
            format!("{index} of {universe_size} sessions evaluated"),
        )?;
        let conversation_id = session.conversation_id;
        let evidence = if fast_lane {
            // §6.1: membership from the session×scope posting sets. The
            // sentinel witness in the `known` set carries the verdict
            // through the same set algebra; region text is never touched.
            eval_node_fast(&plan.root, &session, &session_doc_hits, block_seek.as_ref())
        } else {
            eval_node(
                &plan.root,
                &session,
                connection,
                &mut stats,
                &matchers,
                &quill_possible,
                block_seek.as_ref(),
            )?
        };
        let result = combine(&control_result(filters, &session), &evidence, true);
        match result.truth() {
            Truth::True => {
                if fast_lane {
                    // Scores and witness claims derive from the same
                    // posting sets; the textual witnesses themselves are
                    // discovered at page time, for the emitted sessions
                    // only (Stage 1 §6.4).
                    let (score, claims) =
                        fast_score_and_claims(&plan, conversation_id, &session_doc_hits);
                    accepted.push(EvaluatedSession {
                        session,
                        witnesses: Vec::new(),
                        score,
                        fast_lane: true,
                        fast_claims: claims,
                    });
                } else {
                    // Admitted witness scores: positive branches only, best
                    // Quill hit bound to the witness region's message.
                    let mut best_score: Option<f64> = None;
                    for (region_key, branch) in &result.witnesses {
                        let Some((literal, exact)) = plan
                            .positive_branches
                            .get(branch)
                            .map(|(v, e)| (v.clone(), *e))
                        else {
                            continue;
                        };
                        let region = session
                            .regions
                            .iter()
                            .find(|region| &region.key == region_key);
                        let Some(idx) = region.map(|r| r.quill_idx) else {
                            continue;
                        };
                        if let Some(hit_map) = quill_scores.get(&(literal, exact))
                            && let Some(scores) = hit_map.get(&conversation_id)
                            && let Some(score) = scores.get(&idx)
                        {
                            let score = *score as f64;
                            best_score =
                                Some(best_score.map_or(score, |best: f64| best.max(score)));
                        }
                    }
                    accepted.push(EvaluatedSession {
                        session,
                        witnesses: result.witnesses,
                        score: best_score,
                        fast_lane: false,
                        fast_claims: Vec::new(),
                    });
                }
            }
            Truth::Unknown => {
                unknown_sessions += 1;
                for reason in &result.reasons {
                    *unknown_reasons.entry(reason.clone()).or_default() += 1;
                }
                // A session must never be Unknown with zero reasons: the
                // refusal below reads `all()` over the reason list, and
                // an empty list is vacuously true — it would report
                // `facts_pending` with an empty evidence list, claiming
                // reindexable facts the evidence never says. Internal-
                // inconsistency backstop: every Unknown arm carries its
                // reason, and this one names the violation.
                if result.reasons.is_empty() {
                    *unknown_reasons
                        .entry("internal:inconsistency:unknown_without_reason".to_owned())
                        .or_default() += 1;
                }
            }
            Truth::False => {}
        }
    }
    if unknown_sessions > 0 && !allow_partial {
        // `facts_pending` (a reindex converges the evidence) needs a
        // non-empty reason list whose every entry is pending. An empty
        // list is data_unavailable — never a vacuous facts_pending.
        let pending = !unknown_reasons.is_empty()
            && unknown_reasons
                .keys()
                .all(|reason| reason.contains(":pending:"));
        return Err(refuse(
            if pending {
                "facts_pending"
            } else {
                "data_unavailable"
            },
            format!(
                "The final DSL membership depends on unavailable fact evidence: {}",
                unknown_reasons
                    .keys()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join("; ")
            ),
        ));
    }

    // 4) Sort + keyset page.
    let mut warnings: Vec<String> = Vec::new();
    if unknown_sessions > 0 {
        warnings.push("membership_incomplete".to_owned());
    }
    let mut ranking_incomplete = false;
    for entry in &accepted {
        // Every persisted region owns a Quill document slot, so index
        // witness evidence exists iff the score loop bound a hit. An
        // accepted session whose witnesses produced no hit (predicate-
        // only proof, e.g. `has:tag` controls) contributes no score —
        // the honest "ranking incomplete" signal survives unchanged. On
        // the fast lane the expected-witness claims play the witness
        // role: a claim always carries its posting-set score, so the
        // incomplete signal stays honest there too.
        let has_positive_witness = if entry.fast_lane {
            !entry.fast_claims.is_empty()
        } else {
            !entry.witnesses.is_empty()
        };
        if has_positive_witness && entry.score.is_none() {
            ranking_incomplete = true;
        }
    }
    accepted.sort_by(|a, b| {
        let (va, vb) = match sort_name {
            "relevance" | "updated_desc" => {
                (a.score.unwrap_or(f64::MIN), b.score.unwrap_or(f64::MIN))
            }
            "updated_asc" | "created_asc" | "created_desc" => (0.0, 0.0),
            _ => (0.0, 0.0),
        };
        let ordering = match sort_name {
            "relevance" => vb.partial_cmp(&va),
            "updated_desc" => b
                .session
                .activity_ms
                .map(|v| v as f64)
                .unwrap_or(f64::MIN)
                .partial_cmp(&a.session.activity_ms.map(|v| v as f64).unwrap_or(f64::MIN)),
            "updated_asc" => a
                .session
                .activity_ms
                .map(|v| v as f64)
                .unwrap_or(f64::MAX)
                .partial_cmp(&b.session.activity_ms.map(|v| v as f64).unwrap_or(f64::MAX)),
            "created_desc" => b
                .session
                .created_ms
                .map(|v| v as f64)
                .unwrap_or(f64::MIN)
                .partial_cmp(&a.session.created_ms.map(|v| v as f64).unwrap_or(f64::MIN)),
            "created_asc" => a
                .session
                .created_ms
                .map(|v| v as f64)
                .unwrap_or(f64::MAX)
                .partial_cmp(&b.session.created_ms.map(|v| v as f64).unwrap_or(f64::MAX)),
            _ => None,
        };
        ordering
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.session.conversation_id.cmp(&b.session.conversation_id))
    });

    let query_digest = blake3::hash(
        serde_json::to_vec(&json!({
            "ast": ast, "query": query_text, "filters": filters, "scopes": scopes,
            "sort": sort_name, "mode": mode.unwrap_or("lexical"),
            "partial": allow_partial, "epoch": readers.index_epoch,
        }))?
        .as_slice(),
    )
    .to_hex()
    .to_string();
    let start = match cursor {
        Some(raw) => Some(decode_cursor(raw, sort_name, &generation, &query_digest)?),
        None => None,
    };

    let key_of = |entry: &EvaluatedSession| -> SearchCursor {
        let value = match sort_name {
            "relevance" => entry.score,
            "created_asc" | "created_desc" => entry.session.created_ms.map(|v| v as f64),
            _ => entry.session.activity_ms.map(|v| v as f64),
        };
        SearchCursor {
            value,
            null_flag: value.is_none(),
            id: entry.session.conversation_id,
        }
    };
    let mut items = Vec::new();
    let mut matches_out = Map::new();
    let mut bytes = 0_usize;
    let mut last: Option<SearchCursor> = None;
    let mut emitted = 0_usize;
    // (B) Page-time witness gates, memoized once per request and shared by
    // every emitted fast-lane session: one raw enumeration per distinct
    // claim literal, however many sessions the page hydrates.
    let mut witness_gates: WitnessGates = HashMap::new();
    let mut iter = accepted.iter().peekable();
    let mut has_more = false;
    for entry in iter.by_ref() {
        deadline.check(
            "page",
            format!("emitted {emitted} of limit {limit} page items"),
        )?;
        let key = key_of(entry);
        if let Some(bound) = &start {
            let after = match (bound.null_flag, key.null_flag) {
                (false, true) => true,
                (true, false) => false,
                (false, false) => {
                    let desc = matches!(sort_name, "relevance" | "updated_desc" | "created_desc");
                    let (a, b) = (
                        key.value.unwrap_or_default(),
                        bound.value.unwrap_or_default(),
                    );
                    if desc {
                        b > a || (b == a && key.id > bound.id)
                    } else {
                        b < a || (b == a && key.id > bound.id)
                    }
                }
                (true, true) => key.id > bound.id,
            };
            if !after {
                continue;
            }
        }
        if emitted >= limit {
            has_more = true;
            break;
        }
        emitted += 1;
        let session = &entry.session;
        let mut item = json!({
            "conversation_id": session.conversation_id,
            "public_id": session.public_id,
            "provider": session.provider,
            "source_id": Value::Null,
            "workspace_path": session.project_path,
            "project": session.project,
            "title": session.title,
            "custom_title": session.custom_title,
            "notes": session.notes,
            "is_pinned": session.is_pinned,
            "tags": session.tags,
            "is_subagent": session.is_subagent,
            "category": session.category,
            "started_at_ms": session.created_ms,
            "activity_ms": session.activity_ms,
            "score": entry.score,
            // Owner decision 2026-11-07: reason codes this session's facts
            // prove as text ABSENT AT THE SOURCE — the strict answer is
            // exact over the existing text even though these fields'
            // coverage is incomplete. Always present; empty when every
            // gap is a read-recoverable one.
            "source_text_absent": session.source_text_absent,
            // Stage 1 §6.4: the delivered textual-witness count for this
            // session (both lanes). Set to its final value below, once
            // the page's witness work has run; 0 with the page-level
            // `witnesses_not_reproduced` warning names the ONE
            // deliberate relaxation — a doc-claimed member whose regions
            // cannot reproduce the literal textually.
            "witness_count": 0,
        });
        // Public identity is resolved lazily and stays nullable on failure
        // (a search page must not die on one unverifiable row).
        let item_bytes = serde_json::to_vec(&item)?.len() + 2;
        if bytes + item_bytes > RESPONSE_BYTES - 8192 {
            break;
        }
        bytes += item_bytes;
        // Witnesses → bounded deep-link matches. The fast lane discovers
        // its textual witnesses HERE (§6.4) — for the emitted session only,
        // bounded by max_matches, through the same verify_region_text the
        // legacy lane ran at eval time.
        //
        // (B) Gated page-time witness discovery. The page's region read is
        // gated by the claims' raw Quill slots: only regions whose literal
        // produced a hit in this conversation's region/message documents
        // can hold a witness, so the page seeks exactly those rows
        // (`regions_for_quill_slots`, `region_ordinal` order — an
        // order-preserving subsequence of the structural load, so witness
        // order and choice are today's for every gated witness). A session
        // one of whose claims cannot be gated for it, and a gated pass
        // that finds ZERO witnesses, fall back to
        // [`ungated_fast_witnesses`] — the pre-(B) structural load +
        // ungated discovery, §6.4's contract verbatim; claims-empty
        // entries (`NOT quux`) load nothing at all. Sessions past the
        // byte budget (the `break` above) never load.
        let page_outcome: (Vec<Region>, FastWitnesses) = if !entry.fast_lane
            || entry.fast_claims.is_empty()
        {
            (Vec::new(), FastWitnesses::Found(Vec::new()))
        } else {
            // Fill the request-memoized witness gates for this entry's
            // claims first — one raw enumeration per distinct literal per
            // request, shared by every emitted session.
            for (_, (value, exact)) in &entry.fast_claims {
                let key = (value.clone(), *exact);
                if let std::collections::hash_map::Entry::Vacant(slot) = witness_gates.entry(key) {
                    // `?` propagates BEFORE the insert, so a deadline hit
                    // leaves the memo unfilled — the next request retries.
                    let gate = enumerate_witness_gate(
                        readers,
                        (value.as_str(), *exact),
                        &engine_filters,
                        deadline,
                    )?;
                    slot.insert(gate);
                }
            }
            let mut claim_gates: Vec<Option<&HashSet<i64>>> =
                Vec::with_capacity(entry.fast_claims.len());
            for (_, (value, exact)) in &entry.fast_claims {
                claim_gates.push(
                    witness_gates
                        .get(&(value.clone(), *exact))
                        .and_then(|gate| gate.as_ref())
                        .and_then(|per_conversation| {
                            per_conversation.get(&session.conversation_id)
                        }),
                );
            }
            if claim_gates.iter().all(|gate| gate.is_some()) {
                // Every claim enumerated AND hit in this conversation: the
                // union of its slots is the exact candidate set for the
                // page's witnesses.
                let slots: Vec<i64> = claim_gates
                    .iter()
                    .filter_map(|gate| *gate)
                    .flat_map(|gate| gate.iter().copied())
                    .collect();
                let mut slot_map: HashMap<i64, Vec<i64>> = HashMap::new();
                slot_map.insert(session.conversation_id, slots);
                let mut rows = coding_agent_search::viewer_extensions::search_regions::regions_for_quill_slots(
                    connection,
                    &slot_map,
                )?;
                let gated_regions: Vec<Region> = rows
                    .remove(&session.conversation_id)
                    .map(|found| {
                        found
                            .into_iter()
                            .map(|row| region_from(session.conversation_id, row))
                            .collect()
                    })
                    .unwrap_or_default();
                stats.regions_loaded += gated_regions.len();
                match discover_fast_witnesses(
                    connection,
                    &gated_regions,
                    &entry.fast_claims,
                    &claim_gates,
                    &matchers,
                    max_matches,
                    &mut stats,
                    deadline,
                )? {
                    FastWitnesses::Found(discovered) if !discovered.is_empty() => {
                        (gated_regions, FastWitnesses::Found(discovered))
                    }
                    FastWitnesses::Found(_) => {
                        // Zero gated witnesses — §6.4 drift or a witness
                        // outside every gate: the fallback reproduces the
                        // pre-(B) witness list (and the
                        // `witnesses_not_reproduced` annotation) verbatim.
                        ungated_fast_witnesses(
                            connection,
                            session.conversation_id,
                            &entry.fast_claims,
                            &matchers,
                            max_matches,
                            &mut stats,
                            deadline,
                        )?
                    }
                    FastWitnesses::BudgetExhausted => {
                        (gated_regions, FastWitnesses::BudgetExhausted)
                    }
                }
            } else {
                // Some claim cannot be gated for this session (literal
                // over the cap, engine refusal, or no raw hit here): a
                // shared region load must not shrink that claim's
                // candidate set, so the session keeps today's path.
                ungated_fast_witnesses(
                    connection,
                    session.conversation_id,
                    &entry.fast_claims,
                    &matchers,
                    max_matches,
                    &mut stats,
                    deadline,
                )?
            }
        };
        let (page_regions, fast_discovered): (Vec<Region>, Vec<(String, String)>) =
            match page_outcome {
                (regions, FastWitnesses::Found(discovered)) => (regions, discovered),
                // Design §7: budget exhaustion is honest unavailability —
                // not §6.4 drift and never a silently-truncated member
                // with `complete:true`. Refuse the whole page with the
                // typed kind, naming the same
                // verification_bytes_budget_exceeded reason the legacy
                // lane's refusal carries.
                (_, FastWitnesses::BudgetExhausted) => {
                    return Err(refuse(
                        "verification_budget_exceeded",
                        format!(
                            "Witness verification for conversation {} exhausted the \
                             request's {}-byte text-verification budget mid-discovery \
                             (verification_bytes_budget_exceeded) — honest unavailability, \
                             never a complete answer. Raise --text-verification-budget-bytes \
                             and retry.",
                            session.conversation_id, stats.verification_budget
                        ),
                    ));
                }
            };
        let witnesses: &[(String, String)] = if entry.fast_lane {
            &fast_discovered
        } else {
            &entry.witnesses
        };
        let mut session_matches = Vec::new();
        // The fast lane resolves witness regions against its page-time
        // load; the legacy lane against the session's (eager or gated)
        // resident rows — either way exactly the set the witnesses were
        // proven from.
        let regions_lookup: &[Region] = if entry.fast_lane {
            &page_regions
        } else {
            &entry.session.regions
        };
        for (region_key, branch) in witnesses {
            if session_matches.len() >= max_matches {
                break;
            }
            let Some(region) = regions_lookup
                .iter()
                .find(|region| &region.key == region_key)
            else {
                continue;
            };
            // Snippets hydrate exactly the regions surfaced as matches —
            // bounded by the page's witness set, never the corpus.
            let snippet = region_text(connection, region, &mut stats)?.unwrap_or_default();
            session_matches.push(json!({
                "location": region.scope,
                "location_type": region.scope,
                "location_name": region.tool_name.as_deref()
                    .or(region.file_path.as_deref())
                    .unwrap_or(region.scope.as_str()),
                "block_key": region.key,
                "message_idx": region.message_idx,
                "message_id": region.message_id,
                "branch_id": branch,
                "snippet": snippet.chars().take(160).collect::<String>(),
                "anchor_status": if region.message_idx.is_some() { "verified" } else { "unavailable" },
                "score": Value::Null,
            }));
        }
        if entry.fast_lane && !entry.fast_claims.is_empty() && fast_discovered.is_empty() {
            // The §6.4 annotation: the session documents claimed the
            // member but no consulted region reproduced the literal
            // textually (analyzer n-gram drift). Emitted with
            // witness_count=0 and named — never silently dropped, never
            // a whole-page refusal. Budget exhaustion never reaches
            // here: it refuses the page above, so this warning names
            // ONLY consulted-and-disagreed drift.
            warnings.push("witnesses_not_reproduced".to_owned());
        }
        if !session_matches.is_empty() {
            matches_out.insert(session.conversation_id.to_string(), json!(session_matches));
        }
        item["witness_count"] = json!(session_matches.len());
        last = Some(key);
        items.push(item);
    }
    if iter.peek().is_some() {
        has_more = true;
    }
    // Resolve public ids for the emitted page only (bounded).
    if !items.is_empty() {
        let emitted_ids: Vec<i64> = items
            .iter()
            .map(|item| item["conversation_id"].as_i64().expect("id"))
            .collect();
        let public_ids = identity::resolve_public_ids(connection, &emitted_ids)?;
        for item in &mut items {
            let id = item["conversation_id"].as_i64().expect("id");
            if let Some(verified) = public_ids.get(&id) {
                item["public_id"] = json!(verified.public_id);
                item["source_id"] = json!(verified.source_id);
            }
        }
    }
    let complete = unknown_sessions == 0;
    // Stage 1b receipts, hoisted for the json! macro below: which
    // region-read strategy ran, and — for the structural fallback — why.
    let region_load_mode = match (&region_load, fast_lane) {
        (RegionLoadMode::UniverseStructural(_), _) => "universe_structural",
        (RegionLoadMode::SeekBlockDomain, true) => "emitted_page_sessions",
        (RegionLoadMode::SeekBlockDomain, false) => "block_predicate_seeks",
        (RegionLoadMode::OnDemand, true) => "emitted_page_sessions",
        (RegionLoadMode::OnDemand, false) => "gated_quill_seeks",
    };
    let region_load_reason = match region_load {
        RegionLoadMode::UniverseStructural(reason) => json!(reason),
        RegionLoadMode::SeekBlockDomain | RegionLoadMode::OnDemand => Value::Null,
    };
    let next_cursor = if has_more {
        last.map(|cursor| encode_cursor(sort_name, &cursor, &generation, &query_digest))
            .map(Value::String)
            .unwrap_or(Value::Null)
    } else {
        Value::Null
    };

    Ok(json!({
        "items": items,
        "matches": matches_out,
        "next_cursor": next_cursor,
        "complete": complete,
        "ranking_complete": !ranking_incomplete,
        "total_sessions": if complete { Value::from(accepted.len()) } else { Value::Null },
        "evaluated_sessions": accepted.len() + unknown_sessions,
        "unknown_sessions": unknown_sessions,
        "unknown_reasons": unknown_reasons,
        // Sessions in the evaluated universe whose only inventory gaps
        // are provably text absent at the source (see the per-item
        // `source_text_absent` annotation).
        "source_absent_sessions": source_absent_sessions,
        // Stage 1 §6.1 receipt: which membership evidence answered the
        // query — the aggregated session×scope documents or the per-region
        // lane (including the marks-gate fallback). Always present.
        "membership_source": if fast_lane {
            "session_scope_docs"
        } else {
            "region_literals"
        },
        "generation": generation,
        "index_epoch": readers.index_epoch,
        "warnings": warnings,
        "issues": Value::Null,
        // Explicit diagnostics required by Task 5: candidate + hydration
        // counts prove the indexed path never re-reads the corpus. The
        // verification meters prove whole-body scanning stays inside the
        // request's byte ceiling. `fragments_loaded` receipts the lazy
        // fragment reads (§6.3: first touch per session plus mcp-plan
        // prefetch chunks); `score_domain` names the document space the
        // scores came from.
        "diagnostics": {
            "candidate_sessions": universe_size,
            "region_rows_loaded": stats.regions_loaded,
            // Stage 1b receipts: which region-read strategy ran and — for
            // the structural fallback — why. `region_rows_loaded` above now
            // counts ONLY what the request actually read (gated seeks,
            // emitted-page loads, or the full structural load for
            // fallback plans), never a fixed archive-sized floor.
            "region_load_mode": region_load_mode,
            "region_load_reason": region_load_reason,
            "regions_hydrated": stats.regions_hydrated,
            "messages_hydrated": stats.messages_hydrated,
            "full_text_messages_hydrated": stats.full_messages_hydrated,
            "fragments_loaded": stats.fragments_loaded,
            "verification_bytes_scanned": stats.verification_bytes,
            "verification_budget_bytes": stats.verification_budget,
            "score_domain": if fast_lane { "session_docs" } else { "region_documents" },
        },
    }))
}

#[cfg(test)]
mod cursor_bit_roundtrip_tests {
    use super::*;
    use crate::viewer_bridge::protocol::Generation;

    fn generation() -> Generation {
        Generation {
            canonical_revision: 7,
            annotation_revision: 3,
            projection_revision: 5,
            lexical_generation: 9,
        }
    }

    /// encode -> decode must be BIT-EXACT for every sort value. The keyset
    /// after-predicate resumes inside tie groups via exact f64 equality, so
    /// a bound even one ULP off the encoded key either strands the whole tie
    /// suffix (bound low: `cargo` on the owner archive lost 302 of 1907
    /// members — the decoded bound 1.590656280517578 never equals the scores
    /// 1.5906562805175781) or re-emits ties forever (bound high: the cycling
    /// the incumbent binary's walk showed). Decimal floats cannot be trusted
    /// here because serde_json's float parse is not correctly rounded for
    /// every f64, so `v` carries the raw bit pattern instead.
    #[test]
    fn cursor_value_roundtrips_bit_exactly() {
        let cur_gen = generation();
        let digest = "d473e0e4cf3a6b1c";
        // The f32-widened relevance scores the fast lane emits: exactly the
        // 17-significant-digit f64 class a decimal roundtrip can miss.
        let f32_widened: Vec<f32> = vec![
            1.5906563, 3.9335427, 27.335167, 29.436178, 1.5365977, 1.4536486, 0.0, 1043.9766,
        ];
        let mut values: Vec<f64> = f32_widened.iter().map(|v| f64::from(*v)).collect();
        // Millisecond epoch keys (created/activity sorts): exact in f64.
        values.push(1_762_000_000_123_f64);
        // Hostile f64s: if the encoding ever degrades to a decimal string,
        // these are the first shapes to lose a ULP or worse.
        values.extend([
            0.1 + 0.2,
            -2.75,
            f64::EPSILON,
            f64::MIN_POSITIVE,
            f64::MAX,
            f64::INFINITY,
            f64::NAN,
        ]);
        for v in values {
            for sort in ["relevance", "created_asc"] {
                let cursor = SearchCursor {
                    value: Some(v),
                    null_flag: false,
                    id: 42,
                };
                let raw = encode_cursor(sort, &cursor, &cur_gen, digest);
                let decoded = decode_cursor(&raw, sort, &cur_gen, digest)
                    .unwrap_or_else(|e| panic!("sort={sort} value={v:?}: {e:?}"));
                assert!(!decoded.null_flag, "sort={sort} value={v:?}");
                assert_eq!(decoded.id, 42, "sort={sort} value={v:?}");
                let got = decoded.value.expect("value must decode present");
                assert_eq!(
                    got.to_bits(),
                    v.to_bits(),
                    "sort={sort}: decoded bits differ from encoded bits"
                );
            }
        }
    }

    /// Null sort keys (sessions without created/activity metadata) must
    /// roundtrip as null_flag, never as a numeric value.
    #[test]
    fn cursor_null_value_roundtrips() {
        let cur_gen = generation();
        let cursor = SearchCursor {
            value: None,
            null_flag: true,
            id: 7,
        };
        let raw = encode_cursor("created_asc", &cursor, &cur_gen, "digest");
        let decoded = decode_cursor(&raw, "created_asc", &cur_gen, "digest").unwrap();
        assert!(decoded.null_flag);
        assert_eq!(decoded.value, None);
        assert_eq!(decoded.id, 7);
    }

    /// ss1 cursors (decimal-float `v`) are the lossy format this fix retires:
    /// they must refuse honestly as cursor_expired, never silently decode a
    /// 1-ULP-off bound. A float-typed `v` inside an ss2 cursor is refused the
    /// same way — the bit-pattern lane only accepts integers.
    #[test]
    fn legacy_and_forged_cursors_refuse() {
        use crate::viewer_bridge::protocol::Refusal;
        let refusal_kind = |err: anyhow::Error| -> &'static str {
            err.downcast_ref::<Refusal>()
                .map(|refusal| refusal.kind)
                .unwrap_or("non-refusal error")
        };
        let cur_gen = generation();
        let digest = "d473e0e4cf3a6b1c";
        let legacy = json!({
            "k": "ss1",
            "s": "relevance",
            "nf": false,
            "v": 1.5906562805175781_f64,
            "id": 42,
            "g": cur_gen,
            "q": digest,
        })
        .to_string();
        let err = decode_cursor(&legacy, "relevance", &cur_gen, digest).unwrap_err();
        assert_eq!(
            refusal_kind(err),
            "cursor_expired",
            "ss1 (decimal-float v) must refuse as cursor_expired"
        );

        let forged = json!({
            "k": "ss2",
            "s": "relevance",
            "nf": false,
            "v": 1.5906562805175781_f64,
            "id": 42,
            "g": cur_gen,
            "q": digest,
        })
        .to_string();
        let err = decode_cursor(&forged, "relevance", &cur_gen, digest).unwrap_err();
        assert_eq!(
            refusal_kind(err),
            "cursor_expired",
            "float-typed v inside an ss2 cursor must refuse as cursor_expired"
        );

        // Bound integrity: a different query digest, a different sort, or a
        // different generation must never resume another query's walk.
        let cursor = SearchCursor {
            value: Some(1.5_f64),
            null_flag: false,
            id: 42,
        };
        let raw = encode_cursor("relevance", &cursor, &cur_gen, digest);
        assert!(decode_cursor(&raw, "relevance", &cur_gen, "other-digest").is_err());
        assert!(decode_cursor(&raw, "created_asc", &cur_gen, digest).is_err());
        let mut other = cur_gen;
        other.lexical_generation += 1;
        assert!(decode_cursor(&raw, "relevance", &other, digest).is_err());
    }
}
