use std::collections::{BTreeSet, HashMap};

use anyhow::Result;
use serde::Deserialize;
use serde_json::{Value, json};

use super::super::identity;
use super::super::protocol::refuse;
use super::{MAX_PAREN_DEPTH, MAX_QUERY_BYTES, MAX_QUERY_DEPTH, MAX_QUERY_NODES};

// ---------------------------------------------------------------------------
// AST decode (viewer/search/ast.py wire contract)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub(super) enum AstNode {
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
pub(super) fn decode_ast(raw: &Value) -> Result<Option<AstNode>> {
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
pub(super) fn parse_query_string(query: &str) -> Option<AstNode> {
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

pub(super) const BLOCK_FIELDS: &[&str] = &[
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
pub(super) enum PlanNode {
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
pub(super) enum Domain {
    Session,
    Block,
}

pub(super) struct Plan {
    pub(super) root: PlanNode,
    pub(super) literals: Vec<(String, bool)>, // dedup'd (value, exact)
    pub(super) positive_branches: HashMap<String, (String, bool)>, // branch_id -> literal
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

pub(super) fn compile_plan(
    root: Option<AstNode>,
    controls_scopes: Option<&[String]>,
) -> Result<Plan> {
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
pub(super) struct LiteralMatcher {
    /// Token sequence for exact phrases, or independent prefix terms.
    pub(super) tokens: Vec<String>,
    exact: bool,
    /// `true` when the literal produced no matchable tokens (matches nothing).
    pub(super) never: bool,
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
    pub(super) fn new(value: &str, exact: bool) -> Self {
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
    pub(super) fn matches(&self, text: &str) -> bool {
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
