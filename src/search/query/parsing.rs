use frankensearch::lexical_tantivy::{
    CassQueryToken as FsCassQueryToken, CassWildcardPattern as FsCassWildcardPattern,
    cass_parse_boolean_query as fs_cass_parse_boolean_query,
};

use super::sqlite_hydration::nfc_sanitize_query;
use super::types::{MatchType, SearchFilters};

/// A CASS query as the fallback lanes (SQLite FTS5 and the source scan)
/// evaluate it, parsed with the lexical engine's grammar (2l1b0.52): NOT
/// binds tightest, then AND (explicit or implied), then OR; parentheses
/// group; negation is parity-based; an operator with no operand is dropped.
#[derive(Clone, Debug, PartialEq)]
pub(in crate::search::query) enum CassBoolExpr<T> {
    Operand(T),
    Not(Box<CassBoolExpr<T>>),
    And(Vec<CassBoolExpr<T>>),
    Or(Vec<CassBoolExpr<T>>),
}

/// The shipping lexer's operand and operator tokens, plus the grouping
/// parentheses that [`cass_bool_tokens`] splits off before lexing. `Token`
/// never holds `LParen` or `RParen`.
#[derive(Clone, Debug, PartialEq)]
pub(in crate::search::query) enum CassBoolToken {
    Token(FsCassQueryToken),
    Open,
    Close,
}

/// Lex `raw` for the fallback lanes. Grouping parentheses follow the lexical
/// engine's rules: a `(` opens a group only at the start of a word (after
/// whitespace, `&&`, `||`, a phrase, another parenthesis or a leading `-`),
/// a `)` closes one only while a group is open, and phrases are opaque. The
/// text between them goes through the shipping lexer unchanged.
pub(in crate::search::query) fn cass_bool_tokens(raw: &str) -> Vec<CassBoolToken> {
    fn flush(segment: &mut String, tokens: &mut Vec<CassBoolToken>) {
        if !segment.is_empty() {
            // The loop below splits every grouping parenthesis off, so the
            // engine lexer (same grouping rules since frankensearch 0.7)
            // reports none here. Map any it does report onto the group
            // tokens anyway: a raw paren token would stall the parser.
            tokens.extend(fs_cass_parse_boolean_query(segment).into_iter().map(
                |token| match token {
                    FsCassQueryToken::LParen => CassBoolToken::Open,
                    FsCassQueryToken::RParen => CassBoolToken::Close,
                    token => CassBoolToken::Token(token),
                },
            ));
            segment.clear();
        }
    }
    let mut tokens = Vec::new();
    let mut segment = String::new();
    let mut chars = raw.chars().peekable();
    let mut in_phrase = false;
    let mut at_word_start = true;
    let mut open_groups = 0_usize;
    while let Some(ch) = chars.next() {
        match ch {
            '"' => {
                in_phrase = !in_phrase;
                at_word_start = true;
                segment.push(ch);
            }
            _ if in_phrase => segment.push(ch),
            '(' if at_word_start => {
                flush(&mut segment, &mut tokens);
                tokens.push(CassBoolToken::Open);
                open_groups += 1;
            }
            ')' if open_groups > 0 => {
                flush(&mut segment, &mut tokens);
                tokens.push(CassBoolToken::Close);
                open_groups -= 1;
                at_word_start = true;
            }
            ' ' | '\t' | '\n' => {
                at_word_start = true;
                segment.push(ch);
            }
            '&' | '|' if chars.peek() == Some(&ch) => {
                chars.next();
                segment.push(ch);
                segment.push(ch);
                at_word_start = true;
            }
            '-' if at_word_start => segment.push(ch),
            _ => {
                at_word_start = false;
                segment.push(ch);
            }
        }
    }
    flush(&mut segment, &mut tokens);
    tokens
}

/// The deepest group nesting a query may carry, as `cass_bool_tokens`
/// reads it (a `(` at the start of a word opens one group).
///
/// The bound exists for stack, not semantics: the recursive-descent
/// boolean parser pays one four-frame cycle (`parse_or` → `parse_and` →
/// `parse_unary` → `parse_primary`) per nesting level, and the consumers
/// of its AST (`render_query_structure`, `render_fts5_expr`, the
/// source-scan `evaluate`, the AST's own recursive `Drop`) each pay one
/// frame per level after that, so an unbounded query is an unbounded
/// stack — the fuzz crash input (see the
/// `fuzz_crash_input_never_overflows_a_small_stack` regression) drove
/// ~3,900 cycles into the guard page. 128 keeps every admitted query in
/// a few tens of KB of stack (≈4 small frames per level) while sitting
/// 2× above the lexical engine's own historical `MAX_QUERY_DEPTH = 64`
/// line and ~40× above the deepest query cass's metamorphic corpus
/// generator produces (depth 3, tests/search_metamorphic.rs) — far past
/// any human-written query.
pub const MAX_QUERY_NESTING_DEPTH: usize = 128;

/// Group nesting depth of a token stream: the deepest run of [`Open`](CassBoolToken::Open)
/// tokens minus their matching [`Close`](CassBoolToken::Close) tokens.
/// Iterative on purpose — this runs before the recursive descent it guards.
pub(in crate::search::query) fn cass_bool_token_nesting_depth(tokens: &[CassBoolToken]) -> usize {
    let mut depth = 0_usize;
    let mut max = 0_usize;
    for token in tokens {
        match token {
            CassBoolToken::Open => {
                depth += 1;
                max = max.max(depth);
            }
            CassBoolToken::Close => depth = depth.saturating_sub(1),
            CassBoolToken::Token(_) => {}
        }
    }
    max
}

/// A query whose group nesting exceeds [`MAX_QUERY_NESTING_DEPTH`]. The
/// message names both numbers so every refusal surface (CLI envelope,
/// bridge refusal, library error) is actionable without extra lookups.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryDepthExceeded {
    /// The nesting depth the lexer read.
    pub depth: usize,
    /// The limit the query exceeded ([`MAX_QUERY_NESTING_DEPTH`]).
    pub limit: usize,
}

impl std::fmt::Display for QueryDepthExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "query nesting depth {} exceeds the limit of {} groups",
            self.depth, self.limit
        )
    }
}

impl std::error::Error for QueryDepthExceeded {}

/// Refuse a query nested deeper than [`MAX_QUERY_NESTING_DEPTH`] before
/// any search lane runs. `CassBoolParser::parse` refuses the same
/// input at its own entry; this is the user-facing gate the
/// [`SearchClient`](super::SearchClient) entries, the CLI handlers and the viewer bridge
/// share, so a too-deep query surfaces as one typed error instead of a
/// lane-specific degradation.
pub fn ensure_query_nesting_depth(query: &str) -> Result<(), QueryDepthExceeded> {
    let depth = cass_bool_token_nesting_depth(&cass_bool_tokens(query));
    if depth > MAX_QUERY_NESTING_DEPTH {
        return Err(QueryDepthExceeded {
            depth,
            limit: MAX_QUERY_NESTING_DEPTH,
        });
    }
    Ok(())
}

/// An operand the lane cannot express: the whole query is declined.
pub(in crate::search::query) struct CassBoolUnsupported;

/// Why a query string cannot go through the recursive boolean stages.
pub(in crate::search::query) enum CassBoolParseError {
    /// An operand the lane cannot express (the closure's
    /// [`CassBoolUnsupported`]): the whole query is declined.
    Unsupported,
    /// Group nesting deeper than [`MAX_QUERY_NESTING_DEPTH`]. Refused
    /// before any recursive descent runs, so the parser and every later
    /// consumer of its AST stay bounded by the same constant. The
    /// measured depth and limit live in the user-facing
    /// [`QueryDepthExceeded`] the boundary gates return; the lanes
    /// degrade this refusal to `None` like any other declined query.
    TooDeep,
}

/// Recursive-descent parser for [`CassBoolExpr`]:
///
/// ```text
/// or      := and (OR and)*
/// and     := unary ([AND] unary)*
/// unary   := NOT* primary
/// primary := TERM | PHRASE | '(' or ')'
/// ```
pub(in crate::search::query) struct CassBoolParser<'a, T, F> {
    pub(in crate::search::query) tokens: &'a [CassBoolToken],
    pub(in crate::search::query) position: usize,
    pub(in crate::search::query) lower: F,
    pub(in crate::search::query) _operand: std::marker::PhantomData<T>,
}

impl<'a, T, F> CassBoolParser<'a, T, F>
where
    F: FnMut(&FsCassQueryToken) -> Result<Option<T>, CassBoolUnsupported>,
{
    /// Parse `tokens`, lowering each term or phrase with `lower`: `Ok(None)`
    /// skips an operand that normalizes to nothing, `Err` declines the query.
    ///
    /// A token stream nested deeper than [`MAX_QUERY_NESTING_DEPTH`] is
    /// refused here, before any recursive descent runs: the parser pays
    /// one `parse_or`/`parse_and`/`parse_unary`/`parse_primary` cycle per
    /// nesting level, and refusing up front bounds that cycle count — and
    /// so the depth of every AST the parser can build, and with it the
    /// recursion of every later consumer (renderers, scan `evaluate`,
    /// `Drop`) — by the same constant.
    pub(in crate::search::query) fn parse(
        tokens: &'a [CassBoolToken],
        lower: F,
    ) -> Result<Option<CassBoolExpr<T>>, CassBoolParseError> {
        let depth = cass_bool_token_nesting_depth(tokens);
        if depth > MAX_QUERY_NESTING_DEPTH {
            return Err(CassBoolParseError::TooDeep);
        }
        let mut parser = Self {
            tokens,
            position: 0,
            lower,
            _operand: std::marker::PhantomData,
        };
        let mut expr = parser.parse_or()?;
        // A `)` outside any group cannot come from cass_bool_tokens, but a
        // truncated parse must not drop the rest of the query.
        while parser.position < parser.tokens.len() {
            parser.position += 1;
            if let Some(rest) = parser.parse_or()? {
                expr = Some(match expr {
                    Some(CassBoolExpr::Or(mut operands)) => {
                        operands.push(rest);
                        CassBoolExpr::Or(operands)
                    }
                    Some(first) => CassBoolExpr::Or(vec![first, rest]),
                    None => rest,
                });
            }
        }
        Ok(expr)
    }

    pub(in crate::search::query) fn peek(&self) -> Option<&'a CassBoolToken> {
        self.tokens.get(self.position)
    }

    pub(in crate::search::query) fn parse_or(
        &mut self,
    ) -> Result<Option<CassBoolExpr<T>>, CassBoolParseError> {
        let mut operands = Vec::new();
        loop {
            match self.peek() {
                None | Some(CassBoolToken::Close) => break,
                Some(CassBoolToken::Token(FsCassQueryToken::Or)) => {
                    self.position += 1;
                    continue;
                }
                Some(_) => {}
            }
            if let Some(operand) = self.parse_and()? {
                operands.push(operand);
            }
        }
        Ok(match operands.len() {
            0 => None,
            1 => operands.pop(),
            _ => Some(CassBoolExpr::Or(operands)),
        })
    }

    pub(in crate::search::query) fn parse_and(
        &mut self,
    ) -> Result<Option<CassBoolExpr<T>>, CassBoolParseError> {
        let mut operands = Vec::new();
        loop {
            match self.peek() {
                None
                | Some(CassBoolToken::Close)
                | Some(CassBoolToken::Token(FsCassQueryToken::Or)) => break,
                Some(CassBoolToken::Token(FsCassQueryToken::And)) => {
                    self.position += 1;
                    continue;
                }
                Some(_) => {}
            }
            if let Some(operand) = self.parse_unary()? {
                operands.push(operand);
            }
        }
        Ok(match operands.len() {
            0 => None,
            1 => operands.pop(),
            _ => Some(CassBoolExpr::And(operands)),
        })
    }

    pub(in crate::search::query) fn parse_unary(
        &mut self,
    ) -> Result<Option<CassBoolExpr<T>>, CassBoolParseError> {
        let mut negated = false;
        while let Some(CassBoolToken::Token(FsCassQueryToken::Not)) = self.peek() {
            negated = !negated;
            self.position += 1;
        }
        // A NOT with no operand (before AND, OR, `)` or the end) is dropped.
        if !matches!(
            self.peek(),
            Some(
                CassBoolToken::Open
                    | CassBoolToken::Token(FsCassQueryToken::Term(_) | FsCassQueryToken::Phrase(_))
            )
        ) {
            return Ok(None);
        }
        let operand = self.parse_primary()?;
        Ok(match operand {
            Some(operand) if negated => Some(CassBoolExpr::Not(Box::new(operand))),
            other => other,
        })
    }

    pub(in crate::search::query) fn parse_primary(
        &mut self,
    ) -> Result<Option<CassBoolExpr<T>>, CassBoolParseError> {
        let Some(token) = self.peek() else {
            return Ok(None);
        };
        self.position += 1;
        match token {
            CassBoolToken::Open => {
                let inner = self.parse_or()?;
                // An unclosed group closes at the end of the query.
                if matches!(self.peek(), Some(CassBoolToken::Close)) {
                    self.position += 1;
                }
                Ok(inner)
            }
            CassBoolToken::Token(token) => (self.lower)(token)
                .map_err(|_| CassBoolParseError::Unsupported)
                .map(|operand| operand.map(CassBoolExpr::Operand)),
            CassBoolToken::Close => Ok(None),
        }
    }
}

// ============================================================================
// Query Explanation types (--explain flag support)
// ============================================================================

/// Classification of query type for explanation purposes
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryType {
    /// Single term without operators
    Simple,
    /// Quoted phrase ("exact match")
    Phrase,
    /// Contains AND/OR/NOT operators
    Boolean,
    /// Contains wildcards (* prefix/suffix)
    Wildcard,
    /// Has time/agent/workspace filters
    Filtered,
    /// Empty query
    Empty,
}

/// How the index will execute this query
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexStrategy {
    /// Fast path: edge n-gram prefix matching
    EdgeNgram,
    /// Regex scan for leading wildcards (*foo)
    RegexScan,
    /// Combined boolean query execution
    BooleanCombination,
    /// Range scan for time filters
    RangeScan,
    /// All documents (empty query)
    FullScan,
}

/// Rough complexity indicator for query execution
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryCost {
    /// Very fast (under 10ms typical)
    Low,
    /// Moderate (10-100ms typical)
    Medium,
    /// Expensive (100ms+ typical, may scan many documents)
    High,
}

/// Sub-component of a parsed term
#[derive(Debug, Clone, serde::Serialize)]
pub struct ParsedSubTerm {
    pub text: String,
    pub pattern: String,
}

/// Parsed term from the query
#[derive(Debug, Clone, serde::Serialize)]
pub struct ParsedTerm {
    /// Original term text
    pub text: String,
    /// Whether this is negated (NOT/-)
    pub negated: bool,
    /// Sub-terms if split (implicit AND)
    pub subterms: Vec<ParsedSubTerm>,
}

/// Parsed structure of the query
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct ParsedQuery {
    /// Individual terms extracted
    pub terms: Vec<ParsedTerm>,
    /// Phrases (quoted strings)
    pub phrases: Vec<String>,
    /// Boolean operators used
    pub operators: Vec<String>,
    /// Whether implicit AND is used between terms
    pub implicit_and: bool,
    /// How the operands group, as the lexical engine reads the query: every
    /// compound group parenthesized, e.g. `(a AND b) OR c` (2l1b0.52).
    pub structure: Option<String>,
}

/// Grouping of a parsed CASS query for `--explain`: compound operands are
/// parenthesized, so precedence never has to be inferred from the text.
pub(in crate::search::query) fn render_query_structure(expr: &CassBoolExpr<String>) -> String {
    fn grouped(expr: &CassBoolExpr<String>) -> String {
        match expr {
            CassBoolExpr::And(_) | CassBoolExpr::Or(_) => {
                format!("({})", render_query_structure(expr))
            }
            other => render_query_structure(other),
        }
    }
    match expr {
        CassBoolExpr::Operand(text) => text.clone(),
        CassBoolExpr::Not(inner) => format!("NOT {}", grouped(inner)),
        CassBoolExpr::And(operands) => operands
            .iter()
            .map(grouped)
            .collect::<Vec<_>>()
            .join(" AND "),
        CassBoolExpr::Or(operands) => operands
            .iter()
            .map(grouped)
            .collect::<Vec<_>>()
            .join(" OR "),
    }
}

/// How the lexical engine reads a query. `--explain` shows it as
/// `parsed.structure` and warnings; robot search echoes it in
/// `_meta.effective` (2l1b0.52, 2l1b0.68).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueryReading {
    /// Operand grouping with every compound group parenthesized, e.g.
    /// `a OR (b AND c)`; `None` for a query without operands.
    pub structure: Option<String>,
    /// Parentheses recovered instead of rejected, e.g. `1 unclosed '('
    /// closed at the end of the query`.
    pub recoveries: Vec<String>,
}

/// Read `query` with the grammar the lexical engine applies.
pub fn read_query(query: &str) -> QueryReading {
    let tokens = cass_bool_tokens(query);
    let structure = CassBoolParser::parse(&tokens, |token: &FsCassQueryToken| {
        Ok(match token {
            FsCassQueryToken::Term(text) => Some(text.clone()),
            FsCassQueryToken::Phrase(text) => Some(format!("\"{text}\"")),
            FsCassQueryToken::And
            | FsCassQueryToken::Or
            | FsCassQueryToken::Not
            | FsCassQueryToken::LParen
            | FsCassQueryToken::RParen => None,
        })
    })
    .ok()
    .flatten()
    .map(|expr| render_query_structure(&expr));
    QueryReading {
        structure,
        recoveries: group_recovery_warnings(&tokens),
    }
}

/// `--explain` warnings for parentheses the grammar recovers instead of
/// rejecting, read the way the lexical engine reads them: an unclosed `(`
/// closes at the end of the query and an empty `()` is skipped.
pub(in crate::search::query) fn group_recovery_warnings(tokens: &[CassBoolToken]) -> Vec<String> {
    let mut open_groups = 0_usize;
    let mut empty_groups = 0_usize;
    for (index, token) in tokens.iter().enumerate() {
        match token {
            CassBoolToken::Open => {
                open_groups += 1;
                if matches!(tokens.get(index + 1), Some(CassBoolToken::Close)) {
                    empty_groups += 1;
                }
            }
            CassBoolToken::Close => open_groups = open_groups.saturating_sub(1),
            CassBoolToken::Token(_) => {}
        }
    }
    let mut warnings = Vec::new();
    if open_groups > 0 {
        warnings.push(format!(
            "{open_groups} unclosed '(' closed at the end of the query"
        ));
    }
    if empty_groups > 0 {
        warnings.push(format!("{empty_groups} empty '()' skipped"));
    }
    warnings
}

/// Comprehensive query explanation for debugging and understanding search behavior
#[derive(Debug, Clone, serde::Serialize)]
pub struct QueryExplanation {
    /// Exact input string
    pub original_query: String,
    /// Sanitized query after normalization
    pub sanitized_query: String,
    /// Structured breakdown of query components
    pub parsed: ParsedQuery,
    /// High-level classification
    pub query_type: QueryType,
    /// How the index will execute this query
    pub index_strategy: IndexStrategy,
    /// Whether wildcard fallback was/will be applied
    pub wildcard_applied: bool,
    /// Rough complexity indicator
    pub estimated_cost: QueryCost,
    /// Active filters summary
    pub filters_summary: FiltersSummary,
    /// Any issues or suggestions
    pub warnings: Vec<String>,
}

/// Summary of active filters for explanation
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct FiltersSummary {
    /// Number of agent filters
    pub agent_count: usize,
    /// Number of workspace filters
    pub workspace_count: usize,
    /// Whether time range is applied
    pub has_time_filter: bool,
    /// Human-readable filter description
    pub description: Option<String>,
}

impl QueryExplanation {
    /// Build explanation from query string and filters
    pub fn analyze(query: &str, filters: &SearchFilters) -> Self {
        let sanitized = nfc_sanitize_query(query);
        // Parse original query to preserve quotes for phrases
        let tokens = fs_cass_parse_boolean_query(query);

        // Extract terms, phrases, and operators
        let mut parsed = ParsedQuery::default();
        let mut next_negated = false;
        let mut saw_operand = false;
        let mut explicit_binary_since_operand = false;
        let mut uses_implicit_and = false;

        for token in &tokens {
            match token {
                FsCassQueryToken::Term(t) => {
                    let parts: Vec<String> = nfc_sanitize_query(t)
                        .split_whitespace()
                        .map(|s| s.to_string())
                        .collect();
                    if parts.is_empty() {
                        next_negated = false;
                        continue;
                    }
                    let mut subterms = Vec::new();
                    for part in parts {
                        let pattern = FsCassWildcardPattern::parse(&part);
                        let pattern_str = match &pattern {
                            FsCassWildcardPattern::Exact(_) => "exact",
                            FsCassWildcardPattern::Prefix(_) => "prefix (*)",
                            FsCassWildcardPattern::Suffix(_) => "suffix (*)",
                            FsCassWildcardPattern::Substring(_) => "substring (*)",
                            FsCassWildcardPattern::Complex(_) => "complex (*)",
                        };
                        subterms.push(ParsedSubTerm {
                            text: part,
                            pattern: pattern_str.to_string(),
                        });
                    }
                    parsed.terms.push(ParsedTerm {
                        text: t.clone(),
                        negated: next_negated,
                        subterms,
                    });
                    if saw_operand && !explicit_binary_since_operand {
                        uses_implicit_and = true;
                    }
                    saw_operand = true;
                    explicit_binary_since_operand = false;
                    next_negated = false;
                }
                FsCassQueryToken::Phrase(p) => {
                    let parts: Vec<String> = nfc_sanitize_query(p)
                        .split_whitespace()
                        .map(|s| s.trim_matches('*').to_lowercase())
                        .filter(|s| !s.is_empty())
                        .collect();
                    if !parts.is_empty() {
                        parsed.phrases.push(parts.join(" "));
                        if saw_operand && !explicit_binary_since_operand {
                            uses_implicit_and = true;
                        }
                        saw_operand = true;
                        explicit_binary_since_operand = false;
                    }
                    next_negated = false;
                }
                FsCassQueryToken::And => {
                    parsed.operators.push("AND".to_string());
                    explicit_binary_since_operand = saw_operand;
                }
                FsCassQueryToken::Or => {
                    parsed.operators.push("OR".to_string());
                    explicit_binary_since_operand = saw_operand;
                }
                FsCassQueryToken::Not => {
                    parsed.operators.push("NOT".to_string());
                    next_negated = true;
                }
                // Grouping is reported through `structure` (read_query below).
                // A parenthesis is not an operand and does not separate two
                // operands, so implicit-AND adjacency carries across it.
                FsCassQueryToken::LParen | FsCassQueryToken::RParen => {}
            }
        }

        // Every pair of adjacent operands is implicitly conjoined, including
        // quoted phrases and a unary-NOT operand. Track adjacency directly:
        // a query can contain both an explicit connector and a later implicit
        // one (`foo AND bar baz`).
        parsed.implicit_and = uses_implicit_and;
        let reading = read_query(query);
        parsed.structure = reading.structure;

        // Determine query type
        let query_type = Self::classify_query(&parsed, filters, &sanitized);

        // Determine index strategy
        let index_strategy = Self::determine_strategy(&parsed, &sanitized);

        // Estimate cost
        let estimated_cost = Self::estimate_cost(&parsed, &index_strategy, filters);

        // Build filters summary
        let filters_summary = Self::summarize_filters(filters);

        // Generate warnings
        let mut warnings = Self::generate_warnings(&parsed, &sanitized, filters);
        warnings.extend(reading.recoveries);

        Self {
            original_query: query.to_string(),
            sanitized_query: sanitized,
            parsed,
            query_type,
            index_strategy,
            wildcard_applied: false, // Set later by search_with_fallback
            estimated_cost,
            filters_summary,
            warnings,
        }
    }

    pub(in crate::search::query) fn classify_query(
        parsed: &ParsedQuery,
        filters: &SearchFilters,
        sanitized: &str,
    ) -> QueryType {
        if sanitized.trim().is_empty() {
            return QueryType::Empty;
        }

        // Check for filters first (they modify everything)
        let has_filters = !filters.agents.is_empty()
            || !filters.workspaces.is_empty()
            || filters.created_from.is_some()
            || filters.created_to.is_some()
            || !filters.source_filter.is_all();

        if has_filters {
            return QueryType::Filtered;
        }

        // Check for boolean operators
        if !parsed.operators.is_empty() {
            return QueryType::Boolean;
        }

        // Check for phrases
        if !parsed.phrases.is_empty() {
            return QueryType::Phrase;
        }

        // Check for wildcards
        let has_wildcards = parsed
            .terms
            .iter()
            .flat_map(|t| &t.subterms)
            .any(|t| t.pattern != "exact");
        if has_wildcards {
            return QueryType::Wildcard;
        }

        QueryType::Simple
    }

    pub(in crate::search::query) fn determine_strategy(
        parsed: &ParsedQuery,
        sanitized: &str,
    ) -> IndexStrategy {
        if sanitized.trim().is_empty() {
            return IndexStrategy::FullScan;
        }

        // Check for leading wildcards (requires regex)
        let has_leading_wildcard = parsed
            .terms
            .iter()
            .flat_map(|t| &t.subterms)
            .any(|t| t.pattern == "suffix (*)" || t.pattern == "substring (*)");

        if has_leading_wildcard {
            return IndexStrategy::RegexScan;
        }

        // Boolean queries use combination strategy
        // Also if any single term is split into multiple subterms (e.g. "foo.bar" -> "foo", "bar")
        let has_compound_terms = parsed.terms.iter().any(|t| t.subterms.len() > 1);

        if !parsed.operators.is_empty()
            || parsed.terms.len() > 1
            || !parsed.phrases.is_empty()
            || has_compound_terms
        {
            return IndexStrategy::BooleanCombination;
        }

        // Single term uses edge n-gram
        IndexStrategy::EdgeNgram
    }

    pub(in crate::search::query) fn estimate_cost(
        parsed: &ParsedQuery,
        strategy: &IndexStrategy,
        filters: &SearchFilters,
    ) -> QueryCost {
        // Regex scans are always expensive
        if matches!(strategy, IndexStrategy::RegexScan) {
            return QueryCost::High;
        }

        // Full scans are expensive
        if matches!(strategy, IndexStrategy::FullScan) {
            return QueryCost::High;
        }

        // Time range filters add cost
        let has_time_filter = filters.created_from.is_some() || filters.created_to.is_some();

        // Count complexity factors
        let term_count: usize = parsed.terms.iter().map(|t| t.subterms.len()).sum();
        let operator_count = parsed.operators.len();
        let phrase_count = parsed.phrases.len();

        let complexity = term_count + operator_count * 2 + phrase_count * 2;

        if complexity > 6 || has_time_filter {
            QueryCost::High
        } else if complexity > 2 {
            QueryCost::Medium
        } else {
            QueryCost::Low
        }
    }

    pub(in crate::search::query) fn summarize_filters(filters: &SearchFilters) -> FiltersSummary {
        let agent_count = filters.agents.len();
        let workspace_count = filters.workspaces.len();
        let has_time_filter = filters.created_from.is_some() || filters.created_to.is_some();

        let mut parts = Vec::new();
        if agent_count > 0 {
            parts.push(format!(
                "{} agent{}",
                agent_count,
                if agent_count > 1 { "s" } else { "" }
            ));
        }
        if workspace_count > 0 {
            parts.push(format!(
                "{} workspace{}",
                workspace_count,
                if workspace_count > 1 { "s" } else { "" }
            ));
        }
        if has_time_filter {
            parts.push("time range".to_string());
        }

        let description = if parts.is_empty() {
            None
        } else {
            Some(format!("Filtering by: {}", parts.join(", ")))
        };

        FiltersSummary {
            agent_count,
            workspace_count,
            has_time_filter,
            description,
        }
    }

    pub(in crate::search::query) fn generate_warnings(
        parsed: &ParsedQuery,
        sanitized: &str,
        filters: &SearchFilters,
    ) -> Vec<String> {
        let mut warnings = Vec::new();

        // Warn about leading wildcards
        let has_leading_wildcard = parsed
            .terms
            .iter()
            .flat_map(|t| &t.subterms)
            .any(|t| t.pattern == "suffix (*)" || t.pattern == "substring (*)");
        if has_leading_wildcard {
            warnings.push(
                "Leading wildcards (*foo) require regex scan and may be slow on large indexes"
                    .to_string(),
            );
        }

        // Warn about very short terms
        for term in &parsed.terms {
            for sub in &term.subterms {
                if sub.text.trim_matches('*').len() < 2 {
                    warnings.push(format!(
                        "Very short term '{}' may match many documents",
                        sub.text
                    ));
                }
            }
        }

        // Warn about empty query
        if sanitized.trim().is_empty() {
            warnings.push("Empty query will return all documents (expensive)".to_string());
        }

        // Warn about complex boolean queries
        if parsed.operators.len() > 3 {
            warnings.push(
                "Complex boolean query: parsed.structure shows how its operands group".to_string(),
            );
        }

        // Warn about narrow filters that might miss results
        if let Some(agent) = filters.agents.iter().next()
            && filters.agents.len() == 1
            && filters.workspaces.is_empty()
        {
            warnings.push(format!(
                "Searching only in agent '{}' - results from other agents will be excluded",
                agent
            ));
        }

        warnings
    }

    /// Update `wildcard_applied` flag (called after `search_with_fallback`)
    pub fn with_wildcard_fallback(mut self, applied: bool) -> Self {
        self.wildcard_applied = applied;
        if applied
            && !self
                .warnings
                .iter()
                .any(|w| w.contains("wildcard fallback"))
        {
            self.warnings.push(
                "Wildcard fallback was applied automatically due to sparse exact matches"
                    .to_string(),
            );
        }
        self
    }
}

/// Type of suggestion for did-you-mean
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SuggestionKind {
    /// Typo correction (Levenshtein distance)
    SpellingFix,
    /// Try with wildcard prefix/suffix
    WildcardQuery,
    /// Remove restrictive filter
    RemoveFilter,
    /// Try different agent
    AlternateAgent,
    /// Broaden date range
    BroaderDateRange,
}

/// A "did-you-mean" suggestion when search returns zero hits.
#[derive(Debug, Clone, serde::Serialize)]
pub struct QuerySuggestion {
    /// What kind of suggestion this is
    pub kind: SuggestionKind,
    /// Human-readable description (e.g., "Did you mean: 'codex'?")
    pub message: String,
    /// The suggested query string (if query change)
    pub suggested_query: Option<String>,
    /// Suggested filters to apply (replaces current filters if Some)
    pub suggested_filters: Option<SearchFilters>,
    /// Shortcut key (1, 2, or 3) for quick apply in TUI
    pub shortcut: Option<u8>,
}

impl QuerySuggestion {
    pub(in crate::search::query) fn spelling(_query: &str, corrected: &str) -> Self {
        Self {
            kind: SuggestionKind::SpellingFix,
            message: format!("Did you mean: \"{corrected}\"?"),
            suggested_query: Some(corrected.to_string()),
            suggested_filters: None,
            shortcut: None,
        }
    }

    pub(in crate::search::query) fn wildcard(query: &str) -> Self {
        let wildcard_query = format!("*{}*", query.trim_matches('*'));
        Self {
            kind: SuggestionKind::WildcardQuery,
            message: format!("Try broader search: \"{wildcard_query}\""),
            suggested_query: Some(wildcard_query),
            suggested_filters: None,
            shortcut: None,
        }
    }

    pub(in crate::search::query) fn remove_agent_filter(
        current_agent: &str,
        current_filters: &SearchFilters,
    ) -> Self {
        // Clone current filters and only clear the agent filter, preserving
        // workspace and date range filters
        let mut filters = current_filters.clone();
        filters.agents.clear();
        Self {
            kind: SuggestionKind::RemoveFilter,
            message: format!("Remove agent filter (currently: {current_agent})"),
            suggested_query: None,
            suggested_filters: Some(filters),
            shortcut: None,
        }
    }

    pub(in crate::search::query) fn try_agent(agent_slug: &str) -> Self {
        let mut filters = SearchFilters::default();
        filters.agents.insert(agent_slug.to_string());
        Self {
            kind: SuggestionKind::AlternateAgent,
            message: format!("Try searching in: {agent_slug}"),
            suggested_query: None,
            suggested_filters: Some(filters),
            shortcut: None,
        }
    }

    pub(in crate::search::query) fn with_shortcut(mut self, key: u8) -> Self {
        self.shortcut = Some(key);
        self
    }
}

/// Normalize a term into FTS5-porter-aligned parts.
/// Splits punctuation into separate fragments while preserving a trailing `*`
/// on the final fragment so fallback queries match how SQLite tokenizes indexed
/// text in `fts_messages`.
pub(in crate::search::query) fn normalize_term_parts(raw: &str) -> Vec<String> {
    let mut parts = Vec::new();
    for token in nfc_sanitize_query(raw).split_whitespace() {
        let mut current = String::new();
        let mut chars = token.chars().peekable();
        while let Some(ch) = chars.next() {
            let trailing_wildcard = ch == '*' && chars.peek().is_none() && !current.is_empty();
            if ch.is_alphanumeric() || ch == '_' || trailing_wildcard {
                current.push(ch);
                continue;
            }

            if !current.is_empty() {
                parts.push(std::mem::take(&mut current));
            }
        }

        if !current.is_empty() {
            parts.push(current);
        }
    }
    parts
}

/// Normalize phrase text into tokenizer-aligned terms (lowercased, no wildcards).
pub(in crate::search::query) fn normalize_phrase_terms(raw: &str) -> Vec<String> {
    normalize_term_parts(raw)
        .into_iter()
        .map(|s| s.trim_matches('*').to_lowercase())
        .filter(|s| !s.is_empty())
        .collect()
}

pub(in crate::search::query) fn render_fts5_term_part(part: &str) -> Option<String> {
    let pattern = FsCassWildcardPattern::parse(part);
    if matches!(
        pattern,
        FsCassWildcardPattern::Suffix(_)
            | FsCassWildcardPattern::Substring(_)
            | FsCassWildcardPattern::Complex(_)
    ) {
        return None;
    }

    Some(part.to_string())
}

/// Determine the dominant match type from a query string.
/// Returns the "loosest" pattern used (Substring > Suffix > Prefix > Exact).
pub(in crate::search::query) fn dominant_match_type(query: &str) -> MatchType {
    let mut worst = MatchType::Exact;
    for term in query.split_whitespace() {
        let pattern = FsCassWildcardPattern::parse(term);
        let mt = match pattern {
            FsCassWildcardPattern::Exact(_) => MatchType::Exact,
            FsCassWildcardPattern::Prefix(_) => MatchType::Prefix,
            FsCassWildcardPattern::Suffix(_) => MatchType::Suffix,
            FsCassWildcardPattern::Substring(_) => MatchType::Substring,
            FsCassWildcardPattern::Complex(_) => MatchType::Wildcard,
        };
        // Lower quality factor = "looser" match = dominant
        if mt.quality_factor() < worst.quality_factor() {
            worst = mt;
        }
    }
    worst
}

/// Fuzz-only re-export of `transpile_to_fts5` so
/// `fuzz_targets/fuzz_query_transpiler.rs` can exercise the
/// user-reachable query-rewriting path (bead
/// `coding_agent_session_search-ugp09`). `#[doc(hidden)]` keeps it
/// off the public API surface — callers outside the fuzz harness
/// should go through `QueryExplanation::analyze` or `SearchClient`.
#[doc(hidden)]
pub fn fuzz_transpile_to_fts5(raw_query: &str) -> Option<String> {
    transpile_to_fts5(raw_query)
}

/// Transpile a raw query into an FTS5 query with the lexical parser's meaning
/// (see [`CassBoolExpr`]). Every compound operand is parenthesized, so the
/// result does not lean on the FTS5 engine's own precedence. Returns None
/// when FTS5 cannot express the query (a leading or inner wildcard, or a
/// complement: an OR operand or a conjunction made only of NOTs, since FTS5
/// NOT is binary), so the caller falls back to a source scan instead of
/// answering a different question.
pub(in crate::search::query) fn transpile_to_fts5(raw_query: &str) -> Option<String> {
    let tokens = cass_bool_tokens(raw_query);
    let expr = CassBoolParser::parse(&tokens, |token: &FsCassQueryToken| match token {
        FsCassQueryToken::Term(t) => {
            if matches!(
                FsCassWildcardPattern::parse(t),
                FsCassWildcardPattern::Suffix(_)
                    | FsCassWildcardPattern::Substring(_)
                    | FsCassWildcardPattern::Complex(_)
            ) {
                return Err(CassBoolUnsupported);
            }
            // Split punctuation into porter-aligned fragments first so
            // fallback queries match SQLite tokenization: a punctuated term
            // like `foo-bar` becomes `(foo AND bar)`.
            let term_parts = normalize_term_parts(t);
            if term_parts.is_empty() {
                return Ok(None);
            }
            let rendered_parts = term_parts
                .iter()
                .map(|part| render_fts5_term_part(part).ok_or(CassBoolUnsupported))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Some(if rendered_parts.len() > 1 {
                format!("({})", rendered_parts.join(" AND "))
            } else {
                rendered_parts[0].clone()
            }))
        }
        FsCassQueryToken::Phrase(p) => {
            let phrase_parts = normalize_phrase_terms(p);
            Ok((!phrase_parts.is_empty()).then(|| format!("\"{}\"", phrase_parts.join(" "))))
        }
        FsCassQueryToken::And
        | FsCassQueryToken::Or
        | FsCassQueryToken::Not
        | FsCassQueryToken::LParen
        | FsCassQueryToken::RParen => Ok(None),
    })
    .ok()?;
    match expr {
        None => Some(String::new()),
        Some(expr) => render_fts5_expr(&expr),
    }
}

/// FTS5 text for `expr`, or None for a complement FTS5 cannot express.
pub(in crate::search::query) fn render_fts5_expr(expr: &CassBoolExpr<String>) -> Option<String> {
    fn grouped(expr: &CassBoolExpr<String>) -> Option<String> {
        match expr {
            CassBoolExpr::Operand(text) => Some(text.clone()),
            compound => render_fts5_expr(compound).map(|text| format!("({text})")),
        }
    }
    match expr {
        CassBoolExpr::Operand(text) => Some(text.clone()),
        CassBoolExpr::Not(_) => None,
        CassBoolExpr::Or(operands) => Some(
            operands
                .iter()
                .map(grouped)
                .collect::<Option<Vec<_>>>()?
                .join(" OR "),
        ),
        CassBoolExpr::And(operands) => {
            let mut included = Vec::new();
            let mut excluded = Vec::new();
            for operand in operands {
                match operand {
                    CassBoolExpr::Not(inner) => excluded.push(grouped(inner)?),
                    other => included.push(grouped(other)?),
                }
            }
            if included.is_empty() {
                return None;
            }
            let mut text = included.join(" AND ");
            for exclusion in excluded {
                text.push_str(" NOT ");
                text.push_str(&exclusion);
            }
            Some(text)
        }
    }
}
