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
use super::identity;
use super::protocol::{ListFilters, PAGE_LIMIT, RESPONSE_BYTES, refuse};
use super::query;
use super::revisions;
use super::search::{ReaderSet, decode_hit, engine_filters, raw_page};

// ---------------------------------------------------------------------------
// Budgets (mirroring dsl/normalize.py + evaluate.py EvaluationBudget)
// ---------------------------------------------------------------------------

const MAX_QUERY_DEPTH: usize = 32;
const MAX_QUERY_NODES: usize = 1024;
const MAX_QUERY_BYTES: usize = 65_536;
const MAX_PAREN_DEPTH: usize = 32;
const MAX_UNIVERSE: usize = 2_000;
const MAX_MESSAGES_PER_SESSION: usize = 1_024;
const MAX_REGION_TEXT: usize = 64 * 1024;
const MAX_LITERAL_HITS: usize = 100_000;
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
// Literal matcher — `_compile_fts_pattern` semantics as a checked local
// verification (bounded, never a corpus scan).
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct LiteralMatcher {
    /// Token sequence for exact phrases, or independent prefix terms.
    tokens: Vec<String>,
    exact: bool,
    /// `true` when the literal produced no matchable tokens (matches nothing).
    never: bool,
}

fn sanitize_fts_token(token: &str) -> String {
    token
        .chars()
        .filter(|ch| ch.is_alphanumeric() || matches!(ch, '_' | '-' | '$'))
        .collect()
}

fn literal_tokens(text: &str) -> Vec<String> {
    // `_TOKEN_RE = r"[^\w\-_$]+"` split + `sanitize_fts_token`.
    text.split(|ch: char| !(ch.is_alphanumeric() || ch == '_' || ch == '-' || ch == '$'))
        .map(sanitize_fts_token)
        .filter(|token| !token.is_empty())
        .collect()
}

fn region_tokens(text: &str) -> Vec<String> {
    text.split(|ch: char| !(ch.is_alphanumeric() || ch == '_' || ch == '-' || ch == '$'))
        .filter(|token| !token.is_empty())
        .map(|token| token.to_lowercase())
        .collect()
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

    /// FTS-equivalent local verification over one region's text.
    fn matches(&self, text: &str) -> bool {
        if self.never {
            return false;
        }
        let tokens = region_tokens(text);
        if self.exact {
            // Phrase: the literal token sequence must appear contiguously.
            if self.tokens.len() > tokens.len() {
                return false;
            }
            return tokens
                .windows(self.tokens.len())
                .any(|window| window == self.tokens.as_slice());
        }
        // Prefix terms: every word must prefix-match some token (independent).
        self.tokens
            .iter()
            .all(|word| tokens.iter().any(|token| token.starts_with(word.as_str())))
    }
}

// ---------------------------------------------------------------------------
// Session model + regions (FactBlock equivalents)
// ---------------------------------------------------------------------------

/// One searchable region — the CASS FactBlock equivalent: a typed,
/// boundary-known text region bound to canonical or extension evidence.
#[derive(Debug)]
struct Region {
    key: String,
    scope: &'static str,
    scope_known: bool,
    /// Text the literal verifier runs against (bounded).
    text: String,
    /// Canonical `messages.idx` anchoring this region (deep-link).
    message_idx: Option<i64>,
    message_id: Option<i64>,
    tool_name: Option<String>,
    file_path: Option<String>,
    exit_code: Option<i64>,
    has_error: Option<bool>,
    error_type: Option<&'static str>,
    /// `true` when this region's text is NOT part of the Quill document
    /// projection (metadata/extra evidence): literal matches here are exact
    /// membership but yield no Quill witness score.
    outside_index: bool,
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
    /// `true` when the message inventory was truncated by budget.
    inventory_partial: bool,
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
}

// ---------------------------------------------------------------------------
// Region derivation — bounded per-session decode, provider-aware where the
// canonical extra shape is recorded. Mirrors facts overlays + the detail
// lane's attachment evidence; unknown shapes stay unscoped (scope_known
// false) rather than misattributed.
// ---------------------------------------------------------------------------

fn extra_string<'a>(map: &'a Map<String, Value>, keys: &[&str]) -> Option<&'a str> {
    keys.iter()
        .find_map(|key| map.get(*key).and_then(Value::as_str))
        .filter(|text| !text.is_empty())
}

/// Collect `(name, input-text, call_id)` and `(call_id, result-text, exit,
/// is_error)` records from one decoded extra. Bounded extraction — never a
/// second full parser; shapes the recorded providers actually emit.
fn tool_records(extra: &Value) -> (Vec<Value>, Vec<Value>) {
    let mut calls = Vec::new();
    let mut results = Vec::new();
    let kind = extra.get("type").and_then(Value::as_str).unwrap_or("");
    if let Some(content) = extra.pointer("/message/content").and_then(Value::as_array) {
        for part in content {
            let part_kind = part.get("type").and_then(Value::as_str).unwrap_or("");
            if matches!(part_kind, "tool_use" | "tool_call") {
                calls.push(part.clone());
            } else if matches!(part_kind, "tool_result") {
                results.push(part.clone());
            }
        }
    }
    match kind {
        "tool_use" | "tool_call" | "function_call" => calls.push(extra.clone()),
        "tool_result" | "tool_call_output" | "function_call_output" => results.push(extra.clone()),
        _ => {}
    }
    if let Some(item) = extra.get("payload") {
        match item.get("type").and_then(Value::as_str).unwrap_or("") {
            "function_call" | "tool_use" | "tool_call" => calls.push(item.clone()),
            "function_call_output" | "tool_result" | "tool_call_output" => {
                results.push(item.clone())
            }
            _ => {}
        }
    }
    (calls, results)
}

fn call_text(call: &Value) -> String {
    let mut parts = Vec::new();
    if let Some(input) = call.get("input") {
        match input {
            Value::Object(map) => {
                for key in ["command", "file_path", "path", "query", "pattern", "cmd"] {
                    if let Some(text) = map.get(key).and_then(Value::as_str) {
                        parts.push(text.to_owned());
                    }
                }
                if parts.is_empty() {
                    let text = input.to_string();
                    if text.len() <= 8_192 {
                        parts.push(text);
                    }
                }
            }
            Value::String(text) => parts.push(text.clone()),
            _ => {}
        }
    }
    if let Some(args) = call.get("arguments").and_then(Value::as_str) {
        parts.push(args.to_owned());
    }
    parts.join(" ")
}

fn result_text(result: &Value) -> String {
    for key in ["content", "output", "text", "stdout"] {
        if let Some(value) = result.get(key) {
            match value {
                Value::String(text) => return text.clone(),
                Value::Array(parts) => {
                    return parts
                        .iter()
                        .filter_map(|part| {
                            part.as_str().map(str::to_owned).or_else(|| {
                                part.get("text").and_then(Value::as_str).map(str::to_owned)
                            })
                        })
                        .collect::<Vec<_>>()
                        .join("\n");
                }
                _ => {}
            }
        }
    }
    String::new()
}

fn numeric_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| {
        value
            .as_f64()
            .filter(|v| v.fract() == 0.0)
            .map(|v| v as i64)
    })
}

fn thinking_text(extra: &Value) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(content) = extra.pointer("/message/content").and_then(Value::as_array) {
        for part in content {
            if matches!(
                part.get("type").and_then(Value::as_str),
                Some("thinking" | "reasoning")
            ) && let Some(text) = part
                .get("thinking")
                .or_else(|| part.get("text"))
                .and_then(Value::as_str)
            {
                parts.push(text.to_owned());
            }
        }
    }
    for key in ["thinking", "reasoning", "reasoning_content"] {
        if let Some(text) = extra.get(key).and_then(Value::as_str) {
            parts.push(text.to_owned());
        }
    }
    if matches!(
        extra.get("type").and_then(Value::as_str),
        Some("thinking" | "reasoning" | "agent_reasoning")
    ) {
        for key in ["text", "content", "summary"] {
            if let Some(text) = extra.get(key).and_then(Value::as_str) {
                parts.push(text.to_owned());
            }
        }
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("\n"))
    }
}

fn attachment_paths(extra: &Value) -> Vec<String> {
    let mut paths = Vec::new();
    let kind = extra.get("type").and_then(Value::as_str).unwrap_or("");
    if matches!(kind, "attachment" | "artifact" | "file") {
        if let Some(path) = extra_string(
            extra.as_object().expect("checked"),
            &["file_path", "path", "name"],
        ) {
            paths.push(path.to_owned());
        }
    }
    if let Some(list) = extra.get("attachments").and_then(Value::as_array) {
        for element in list {
            if let Some(path) = element.as_str().filter(|p| !p.is_empty()) {
                paths.push(path.to_owned());
            } else if let Some(object) = element.as_object()
                && let Some(path) = extra_string(object, &["file_path", "path", "name"])
            {
                paths.push(path.to_owned());
            }
        }
    }
    paths
}

// ---------------------------------------------------------------------------
// Session loading — the SQL-narrowed universe
// ---------------------------------------------------------------------------

/// Load the filtered universe: every `ListFilters` dimension pushed to SQL
/// (provider/source/project/pinned/tag/classification/windows), the same
/// unknown-facts probes as `list_sessions`. Returns session skeletons in
/// `conversation_id` order.
fn load_universe(connection: &Connection, filters: &ListFilters) -> Result<Vec<i64>> {
    let resolved = query::resolve_filters_public(connection, filters)?;
    query::run_probes_public(connection, &resolved)?;
    let (sql, params) = resolved.universe_sql();
    let rows = connection.query_with_params(&sql, &params)?;
    let mut ids = Vec::with_capacity(rows.len());
    for row in &rows {
        ids.push(row.get_typed::<i64>(0)?);
    }
    Ok(ids)
}

/// One bounded session load: summary columns, annotations, tags, facts
/// coverage, canonical messages and derived regions. Anything beyond the
/// per-session budgets marks the inventory partial rather than scanning on.
fn load_session(
    connection: &Connection,
    conversation_id: i64,
    max_messages: usize,
) -> Result<SessionCtx> {
    let rows = connection.query_with_params(
        &format!(
            "SELECT c.id,c.source_id,c.title,c.started_at,c.ended_at,\
             c.last_message_created_at,c.approx_tokens,c.grand_total_tokens,\
             c.estimated_cost_usd,c.primary_model,c.external_id,\
             va.is_pinned,va.custom_title,va.notes,\
             f.is_subagent,f.task_category,f.coverage_state,f.coverage_detail_json,\
             f.subagent_count,f.parent_conversation_id,f.first_prompt,\
             w.path,a.slug,\
             COALESCE((SELECT t2.last_message_created_at FROM conversation_tail_state t2 \
              WHERE t2.conversation_id = c.id), c.last_message_created_at, c.ended_at, c.started_at) \
             FROM conversations c \
             LEFT JOIN viewer_annotations va ON va.conversation_id = c.id \
             LEFT JOIN viewer_session_facts f ON f.conversation_id = c.id \
             LEFT JOIN workspaces w ON w.id = c.workspace_id \
             LEFT JOIN agents a ON a.id = c.agent_id WHERE c.id = ?1 LIMIT 2"
        ),
        &[SqliteValue::Integer(conversation_id)],
    )?;
    if rows.len() != 1 {
        return Err(refuse(
            "canonical_coordinates_invalid",
            "Ambiguous canonical row",
        ));
    }
    let row = &rows[0];
    let provider =
        identity::normalize_agent_slug(&row.get_typed::<Option<String>>(22)?.unwrap_or_default())
            .unwrap_or_default();
    let workspace_path: Option<String> = row.get_typed(21)?;
    let coverage_json: Option<String> = row.get_typed(17)?;
    let mut coverage: HashMap<String, String> = HashMap::new();
    if let Some(detail) = coverage_json
        .as_deref()
        .and_then(|t| serde_json::from_str::<Value>(t).ok())
        && let Some(fields) = detail.get("fields").and_then(Value::as_object)
    {
        for (field, entry) in fields {
            if let Some(state) = entry.get("state").and_then(Value::as_str) {
                coverage.insert(field.clone(), state.to_owned());
            }
        }
    }
    let coverage_state: Option<String> = row.get_typed(16)?;
    let facts_present = coverage_state.is_some();
    if !facts_present {
        coverage.insert("is_subagent".to_owned(), "pending".to_owned());
        coverage.insert("category".to_owned(), "pending".to_owned());
    }
    let started_at: Option<i64> = row.get_typed(3)?;
    let ended_at: Option<i64> = row.get_typed(4)?;
    let tokens: Option<i64> = row.get_typed::<Option<i64>>(7)?.or(row.get_typed(6)?);
    let tags = annotations::tags_for(connection, &[conversation_id])?
        .remove(&conversation_id)
        .unwrap_or_default();
    let subagent_count: Option<i64> = row.get_typed(18)?;
    let parent_id: Option<i64> = row.get_typed(19)?;

    // Canonical messages (bounded).
    let messages = connection.query_with_params(
        "SELECT id,idx,role,content,extra_bin,extra_json FROM messages \
         WHERE conversation_id = ?1 ORDER BY idx LIMIT ?2",
        &[
            SqliteValue::Integer(conversation_id),
            SqliteValue::Integer((max_messages + 1) as i64),
        ],
    )?;
    let inventory_partial = messages.len() > max_messages;
    let messages = &messages[..messages.len().min(max_messages)];

    let mut session = SessionCtx {
        conversation_id,
        public_id: None,
        provider,
        project: workspace_path
            .as_deref()
            .map(identity::path_name)
            .filter(|name| !name.is_empty()),
        project_path: workspace_path,
        title: row.get_typed(2)?,
        custom_title: row.get_typed(12)?,
        notes: row.get_typed(13)?,
        is_pinned: row.get_typed::<Option<i64>>(11)?.unwrap_or(0) != 0,
        tags,
        is_subagent: row.get_typed::<Option<i64>>(14)?.map(|v| v != 0),
        category: row.get_typed(15)?,
        tokens,
        cost: row.get_typed(8)?,
        duration: match (started_at, ended_at) {
            (Some(start), Some(end)) => Some((end - start) as f64 / 1000.0),
            _ => None,
        },
        activity_ms: row.get_typed(23)?,
        created_ms: started_at,
        model: row.get_typed(9)?,
        regions: Vec::new(),
        coverage,
        inventory_partial,
    };
    let _ = (facts_present, parent_id);

    // Regions: effective title (metadata overlay `title` scope).
    if let Some(title) = session.effective_title().filter(|t| !t.is_empty()) {
        session.regions.push(Region {
            key: "meta:title".to_owned(),
            scope: "title",
            scope_known: true,
            text: title[..title.len().min(MAX_REGION_TEXT)].to_owned(),
            message_idx: None,
            message_id: None,
            tool_name: None,
            file_path: None,
            exit_code: None,
            has_error: None,
            error_type: None,
            outside_index: true,
        });
    }
    // Metadata chat blocks (oracle `_metadata_blocks`: tags + notes → chat).
    if let Some(notes) = session.notes.as_deref().filter(|t| !t.is_empty()) {
        session.regions.push(Region {
            key: "meta:notes".to_owned(),
            scope: "chat",
            scope_known: true,
            text: notes[..notes.len().min(MAX_REGION_TEXT)].to_owned(),
            message_idx: None,
            message_id: None,
            tool_name: None,
            file_path: None,
            exit_code: None,
            has_error: None,
            error_type: None,
            outside_index: true,
        });
    }
    if !session.tags.is_empty() {
        session.regions.push(Region {
            key: "meta:tags".to_owned(),
            scope: "chat",
            scope_known: true,
            text: session.tags.join(" "),
            message_idx: None,
            message_id: None,
            tool_name: None,
            file_path: None,
            exit_code: None,
            has_error: None,
            error_type: None,
            outside_index: true,
        });
    }

    // Subagents scope: child descriptors (label = child title, text =
    // child first prompt / external id) bound to the parent session.
    if subagent_count.unwrap_or(0) > 0 {
        let children = connection.query_with_params(
            "SELECT c2.id,c2.title,c2.external_id FROM viewer_session_facts f2 \
             JOIN conversations c2 ON c2.id = f2.conversation_id \
             WHERE f2.parent_conversation_id = ?1 ORDER BY c2.id LIMIT 64",
            &[SqliteValue::Integer(conversation_id)],
        )?;
        for (ordinal, child) in children.iter().enumerate() {
            let child_id: i64 = child.get_typed(0)?;
            let title: Option<String> = child.get_typed(1)?;
            let external: Option<String> = child.get_typed(2)?;
            let first_prompt = connection
                .query_with_params(
                    "SELECT content FROM messages WHERE conversation_id = ?1 \
                     AND role = 'user' ORDER BY idx LIMIT 1",
                    &[SqliteValue::Integer(child_id)],
                )?
                .first()
                .and_then(|r| r.get_typed::<Option<String>>(0).ok().flatten());
            let text = [
                title.as_deref(),
                external.as_deref(),
                first_prompt.as_deref(),
            ]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join("\n");
            if !text.is_empty() {
                session.regions.push(Region {
                    key: format!("child:{ordinal}:{child_id}"),
                    scope: "subagents",
                    scope_known: true,
                    text: text[..text.len().min(MAX_REGION_TEXT)].to_owned(),
                    message_idx: None,
                    message_id: None,
                    tool_name: None,
                    file_path: None,
                    exit_code: None,
                    has_error: None,
                    error_type: None,
                    outside_index: true,
                });
            }
        }
    }

    // Message-derived regions + call/result correlation.
    struct PendingCall {
        name: String,
        call_id: String,
        text: String,
        message_idx: i64,
        message_id: i64,
        file_path: Option<String>,
    }
    struct PendingResult {
        call_id: Option<String>,
        text: String,
        exit_code: Option<i64>,
        is_error: Option<bool>,
        message_idx: i64,
        message_id: i64,
    }
    let mut calls: Vec<PendingCall> = Vec::new();
    let mut results: Vec<PendingResult> = Vec::new();

    for row in messages {
        let message_id: i64 = row.get_typed(0)?;
        let idx: i64 = row.get_typed(1)?;
        let role: String = row.get_typed(2)?;
        let content: Option<String> = row.get_typed(3)?;
        let extra_bin: Option<Vec<u8>> = row.get_typed(4)?;
        let extra_json: Option<String> = row.get_typed(5)?;
        let content = content.unwrap_or_default();
        let decoded = super::extra::decode(extra_bin, extra_json);
        let extra = if decoded.value.is_null() {
            None
        } else {
            Some(decoded.value)
        };

        // Chat region: every message's canonical text.
        if !content.is_empty() {
            session.regions.push(Region {
                key: format!("m{idx}:chat"),
                scope: "chat",
                scope_known: true,
                text: content[..content.len().min(MAX_REGION_TEXT)].to_owned(),
                message_idx: Some(idx),
                message_id: Some(message_id),
                tool_name: None,
                file_path: None,
                exit_code: None,
                has_error: None,
                error_type: None,
                outside_index: false,
            });
        }
        let Some(extra) = extra else { continue };

        // Thinking region.
        if let Some(text) = thinking_text(&extra) {
            session.regions.push(Region {
                key: format!("m{idx}:thinking"),
                scope: "thinking",
                scope_known: true,
                text: text[..text.len().min(MAX_REGION_TEXT)].to_owned(),
                message_idx: Some(idx),
                message_id: Some(message_id),
                tool_name: None,
                file_path: None,
                exit_code: None,
                has_error: None,
                error_type: None,
                outside_index: true,
            });
        }
        // Attachment/artifact regions.
        for (ordinal, path) in attachment_paths(&extra).into_iter().enumerate() {
            session.regions.push(Region {
                key: format!("m{idx}:artifact:{ordinal}"),
                scope: "artifacts",
                scope_known: true,
                text: path[..path.len().min(MAX_REGION_TEXT)].to_owned(),
                message_idx: Some(idx),
                message_id: Some(message_id),
                tool_name: None,
                file_path: Some(path),
                exit_code: None,
                has_error: None,
                error_type: None,
                outside_index: true,
            });
        }
        // Tool call/result records.
        let (call_records, result_records) = tool_records(&extra);
        for call in &call_records {
            let name = extra_string(
                call.as_object().unwrap_or(&Map::new()),
                &["name", "tool_name"],
            )
            .map(str::to_owned)
            .unwrap_or_else(|| "unknown".to_owned());
            let call_id = extra_string(
                call.as_object().unwrap_or(&Map::new()),
                &["id", "call_id", "tool_use_id"],
            )
            .map(str::to_owned)
            .unwrap_or_else(|| format!("anon:{idx}:{}", calls.len()));
            let mut text = call_text(call);
            if text.is_empty() {
                text = content.clone();
            }
            let file_path = call
                .get("input")
                .and_then(Value::as_object)
                .and_then(|input| {
                    extra_string(
                        input,
                        &["file_path", "path", "TargetFile", "file", "filename"],
                    )
                })
                .map(str::to_owned);
            calls.push(PendingCall {
                name,
                call_id,
                text,
                message_idx: idx,
                message_id,
                file_path,
            });
        }
        for result in &result_records {
            let call_id = extra_string(
                result.as_object().unwrap_or(&Map::new()),
                &["tool_use_id", "call_id", "id"],
            )
            .map(str::to_owned);
            let exit_code = result
                .get("exit_code")
                .or_else(|| result.get("exitCode"))
                .and_then(numeric_i64);
            let mut is_error = result
                .get("is_error")
                .or_else(|| result.get("isError"))
                .and_then(Value::as_bool);
            if let Some(code) = exit_code {
                is_error = Some(code != 0 || is_error == Some(true));
            }
            let mut text = result_text(result);
            if text.is_empty() {
                text = content.clone();
            }
            results.push(PendingResult {
                call_id,
                text,
                exit_code,
                is_error,
                message_idx: idx,
                message_id,
            });
        }
        // Bare tool-role messages without decoded records still yield a
        // tools region (the canonical evidence says "this is a tool step").
        if role == "tool" && call_records.is_empty() && result_records.is_empty() {
            calls.push(PendingCall {
                name: "unknown".to_owned(),
                call_id: format!("role:{idx}"),
                text: content.clone(),
                message_idx: idx,
                message_id,
                file_path: None,
            });
        }
    }

    // Correlate calls + results on call_id into one tools region each
    // (the oracle's tool-call block carries name+exit+is_error together).
    let mut results_by_call: HashMap<String, Vec<PendingResult>> = HashMap::new();
    let mut orphan_results: Vec<PendingResult> = Vec::new();
    for result in results {
        match &result.call_id {
            Some(call_id) => results_by_call
                .entry(call_id.clone())
                .or_default()
                .push(result),
            None => orphan_results.push(result),
        }
    }
    for (ordinal, call) in calls.into_iter().enumerate() {
        let linked = results_by_call.remove(&call.call_id).unwrap_or_default();
        let exit_code = linked.iter().find_map(|r| r.exit_code);
        let is_error = linked.iter().find_map(|r| r.is_error);
        let result_text_joined = linked
            .iter()
            .map(|r| r.text.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        let text = [call.text.as_str(), result_text_joined.as_str()]
            .into_iter()
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
            .join("\n");
        // Result message indexes the region's deep link at the RESULT
        // message when present (witness lands where output was recorded).
        let anchor_idx = linked
            .first()
            .map(|r| r.message_idx)
            .unwrap_or(call.message_idx);
        let anchor_id = linked
            .first()
            .map(|r| r.message_id)
            .unwrap_or(call.message_id);
        session.regions.push(Region {
            key: format!("tool:{ordinal}:{}", call.call_id),
            scope: "tools",
            scope_known: true,
            text: text[..text.len().min(MAX_REGION_TEXT)].to_owned(),
            message_idx: Some(anchor_idx),
            message_id: Some(anchor_id),
            tool_name: Some(call.name),
            file_path: call.file_path,
            exit_code,
            has_error: is_error,
            error_type: is_error.map(|error| if error { "tool" } else { "tool" }),
            outside_index: true,
        });
    }
    for (ordinal, result) in orphan_results.into_iter().enumerate() {
        session.regions.push(Region {
            key: format!("toolresult:{ordinal}:{}", result.message_idx),
            scope: "tools",
            scope_known: true,
            text: result.text[..result.text.len().min(MAX_REGION_TEXT)].to_owned(),
            message_idx: Some(result.message_idx),
            message_id: Some(result.message_id),
            tool_name: None,
            file_path: None,
            exit_code: result.exit_code,
            has_error: result.is_error,
            error_type: result
                .is_error
                .map(|error| if error { "tool" } else { "tool" }),
            outside_index: true,
        });
    }
    Ok(session)
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
                matches!(region.scope, "title" | "chat")
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
            let matched = like_match(&region.text, pattern, false, true)
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
        "error_type" => match region.error_type {
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
/// `FALSE` (complete inventory) or `UNKNOWN`.
fn unknown_field(session: &SessionCtx, field: &str, region: &Region) -> Truth {
    if session.coverage_of(field) == "complete" {
        return Truth::False;
    }
    if matches!(field, "tools" | "tool_exit") && region.scope_known && region.scope != "tools" {
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
    // still hide a matching region.
    let unseen = (session.coverage_of("block_inventory") != "complete"
        && session.coverage_of(coverage) != "complete")
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
) -> NodeResult {
    if field == "has_mcp" {
        let tagged = session
            .tags
            .iter()
            .any(|tag| ascii_fold(tag).contains("mcp"));
        let named = session.regions.iter().any(|r| {
            r.tool_name
                .as_deref()
                .is_some_and(|n| ascii_fold(n).contains("mcp") || n.contains('/'))
        });
        let mut value = if tagged || named {
            Truth::True
        } else if session.coverage_of("tools") == "complete" {
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
    let mut out = collection(field, op, expected, session);
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

fn eval_literal(
    value: &str,
    exact: bool,
    branch: &str,
    positive: bool,
    session: &SessionCtx,
    matchers: &HashMap<(String, bool), LiteralMatcher>,
    quill_possible: &HashSet<i64>,
) -> NodeResult {
    let matcher = &matchers[&(value.to_owned(), exact)];
    let mut out = NodeResult::default();
    if matcher.never {
        return out;
    }
    for region in &session.regions {
        if matcher.matches(&region.text) {
            out.known.insert(region.key.clone());
            out.possible.insert(region.key.clone());
            if positive {
                out.witnesses.push((region.key.clone(), branch.to_owned()));
            }
            continue;
        }
        // Quill-verified hits that local verification cannot attribute are
        // POSSIBLE membership (analyzer drift is honest uncertainty, never
        // silently dropped or silently admitted).
        if let Some(idx) = region.message_idx
            && !region.outside_index
            && quill_possible.contains(&idx)
        {
            out.possible.insert(region.key.clone());
        }
    }
    let unseen = session.inventory_partial
        || session.coverage_of("block_inventory") != "complete"
        || session.coverage_of("text_inventory") != "complete";
    if unseen {
        out.unseen = true;
        for field in ["block_inventory", "text_inventory"] {
            if session.coverage_of(field) != "complete" || session.inventory_partial {
                out.reasons.insert(format!(
                    "{field}:{}:inventory_incomplete",
                    session.coverage_of(field)
                ));
            }
        }
    }
    out
}

fn eval_node(
    node: &PlanNode,
    session: &SessionCtx,
    matchers: &HashMap<(String, bool), LiteralMatcher>,
    quill_possible: &HashMap<String, HashMap<i64, HashSet<i64>>>,
) -> NodeResult {
    match node {
        PlanNode::All => NodeResult::constant(Truth::True, BTreeSet::new()),
        PlanNode::Literal {
            value,
            exact,
            branch,
            positive,
        } => {
            let key = format!("{value}|{exact}");
            let empty = HashSet::new();
            let hits = quill_possible
                .get(&key)
                .and_then(|by_session| by_session.get(&session.conversation_id))
                .unwrap_or(&empty);
            eval_literal(value, *exact, branch, *positive, session, matchers, hits)
        }
        PlanNode::Predicate {
            field,
            op,
            value,
            domain,
        } => eval_predicate(field, op, value, *domain, session),
        PlanNode::Not(child) => eval_node(child, session, matchers, quill_possible).negate(),
        PlanNode::And(children) => {
            let mut result = NodeResult::constant(Truth::True, BTreeSet::new());
            for child in children {
                let next = eval_node(child, session, matchers, quill_possible);
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
                let next = eval_node(child, session, matchers, quill_possible);
                result = combine(&result, &next, false);
            }
            result
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
        "k": "ss1",
        "s": sort,
        "nf": cursor.null_flag,
        "v": cursor.value,
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
    if object.get("k").and_then(Value::as_str) != Some("ss1")
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
        Some(
            object
                .get("v")
                .and_then(Value::as_f64)
                .ok_or_else(expired)?,
        )
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

/// Enumerate ALL Quill hits for one literal query across the pinned reader
/// set (bounded by MAX_LITERAL_HITS — an honest budget, never a silent
/// page-one truncation). Returns `(hits per session per msg idx, issues)`.
fn enumerate_literal(
    readers: &ReaderSet,
    literal_text: &str,
    filters: &frankensearch::quill::query::CassQueryFilters,
) -> Result<(LiteralHits, Vec<String>)> {
    let mut hits: LiteralHits = HashMap::new();
    for (reader_index, reader) in readers.readers.iter().enumerate() {
        let mut offset = 0_usize;
        loop {
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
// Entry point
// ---------------------------------------------------------------------------

struct EvaluatedSession {
    session: SessionCtx,
    witnesses: Vec<(String, String)>,
    score: Option<f64>,
}

/// `search_sessions(ast, query, filters, scopes, sort, mode, allow_partial,
/// cursor, limit, max_matches)` — one bounded, generation-bound page.
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
    let mut quill_scores: HashMap<(String, bool), LiteralHits> = HashMap::new();
    let mut quill_possible: HashMap<String, HashMap<i64, HashSet<i64>>> = HashMap::new();
    for (value, exact) in &plan.literals {
        let text = literal_query_text(value, *exact);
        let (hits, _issues) = enumerate_literal(readers, &text, &engine_filters)?;
        quill_possible.insert(
            format!("{value}|{exact}"),
            hits.iter()
                .map(|(cid, m)| (*cid, m.keys().copied().collect()))
                .collect(),
        );
        quill_scores.insert((value.clone(), *exact), hits);
    }

    // 2) Universe: SQL-narrowed candidate sessions.
    let universe = load_universe(connection, filters)?;
    if universe.len() > MAX_UNIVERSE && !allow_partial {
        return Err(refuse(
            "search_budget_exceeded",
            "Session universe exceeds the evaluation budget",
        ));
    }
    let universe_bounded = universe.len() > MAX_UNIVERSE;
    let mut truncated_universe = false;
    let universe: Vec<i64> = if universe_bounded {
        truncated_universe = true;
        universe.into_iter().take(MAX_UNIVERSE).collect()
    } else {
        universe
    };

    // 3) Evaluate every session in the bounded universe.
    let generation = revisions::generation(connection)?.unwrap_or_default();
    let mut accepted: Vec<EvaluatedSession> = Vec::new();
    let mut unknown_reasons: BTreeMap<String, usize> = BTreeMap::new();
    let mut unknown_sessions = 0_usize;
    for conversation_id in &universe {
        let session = load_session(connection, *conversation_id, MAX_MESSAGES_PER_SESSION)?;
        let mut result = combine(
            &control_result(filters, &session),
            &eval_node(&plan.root, &session, &matchers, &quill_possible),
            true,
        );
        match result.truth() {
            Truth::True => {
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
                    let Some(idx) = region.and_then(|r| r.message_idx) else {
                        continue;
                    };
                    if let Some(hit_map) = quill_scores.get(&(literal, exact))
                        && let Some(scores) = hit_map.get(conversation_id)
                        && let Some(score) = scores.get(&idx)
                    {
                        let score = *score as f64;
                        best_score = Some(best_score.map_or(score, |best: f64| best.max(score)));
                    }
                }
                let _ = &mut result;
                accepted.push(EvaluatedSession {
                    session,
                    witnesses: result.witnesses,
                    score: best_score,
                });
            }
            Truth::Unknown => {
                unknown_sessions += 1;
                for reason in &result.reasons {
                    *unknown_reasons.entry(reason.clone()).or_default() += 1;
                }
            }
            Truth::False => {}
        }
    }
    if unknown_sessions > 0 && !allow_partial {
        let pending = unknown_reasons
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
    if truncated_universe {
        warnings.push("candidate_universe_truncated".to_owned());
    }
    if unknown_sessions > 0 {
        warnings.push("membership_incomplete".to_owned());
    }
    let mut ranking_incomplete = false;
    for entry in &accepted {
        // An accepted session whose positive-branch proof rests only on
        // non-indexed regions contributes no score evidence.
        let has_index_witness = entry.witnesses.iter().any(|(key, _)| {
            entry
                .session
                .regions
                .iter()
                .any(|r| &r.key == key && !r.outside_index)
        });
        let has_positive_witness = !entry.witnesses.is_empty();
        if has_positive_witness && !has_index_witness && entry.score.is_none() {
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
    let mut iter = accepted.iter().peekable();
    let mut has_more = false;
    while let Some(entry) = iter.next() {
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
        let item = json!({
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
        });
        // Public identity is resolved lazily and stays nullable on failure
        // (a search page must not die on one unverifiable row).
        let item_bytes = serde_json::to_vec(&item)?.len() + 2;
        if bytes + item_bytes > RESPONSE_BYTES - 8192 {
            break;
        }
        bytes += item_bytes;
        // Witnesses → bounded deep-link matches.
        let mut session_matches = Vec::new();
        for (region_key, branch) in &entry.witnesses {
            if session_matches.len() >= max_matches {
                break;
            }
            let Some(region) = session
                .regions
                .iter()
                .find(|region| &region.key == region_key)
            else {
                continue;
            };
            session_matches.push(json!({
                "location": region.scope,
                "location_type": region.scope,
                "location_name": region.tool_name.as_deref()
                    .or(region.file_path.as_deref())
                    .unwrap_or(region.scope),
                "block_key": region.key,
                "message_idx": region.message_idx,
                "message_id": region.message_id,
                "branch_id": branch,
                "snippet": region.text.chars().take(160).collect::<String>(),
                "anchor_status": if region.message_idx.is_some() { "verified" } else { "unavailable" },
                "score": Value::Null,
            }));
        }
        if !session_matches.is_empty() {
            matches_out.insert(session.conversation_id.to_string(), json!(session_matches));
        }
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
    let complete = !(truncated_universe || unknown_sessions > 0);
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
        "generation": generation,
        "index_epoch": readers.index_epoch,
        "warnings": warnings,
        "issues": Value::Null,
    }))
}
