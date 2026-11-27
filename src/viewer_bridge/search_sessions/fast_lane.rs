use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap, HashSet};

use anyhow::Result;
use coding_agent_search::franken_sync::Connection;

use super::super::deadline::Deadline;
use super::super::protocol::{PAGE_LIMIT, refuse};
use super::super::search::{ReaderSet, decode_hit, raw_page, raw_page_session_docs};
use super::dsl::{Domain, LiteralMatcher, Plan, PlanNode};
use super::eval::{NodeResult, RegionMatch, Truth, combine, eval_predicate, verify_region_text};
use super::loader::{BlockSeekEvidence, region_from};
use super::regions::{HydrateStats, Region, SessionCtx};
use super::{MAX_LITERAL_HITS, SESSION_DOC_ENUM_PAGE};

// ---------------------------------------------------------------------------
// Quill witness enumeration
// ---------------------------------------------------------------------------

/// Per-literal message evidence: `(conversation_id -> idx -> best score)`.
pub(super) type LiteralHits = HashMap<i64, HashMap<i64, f32>>;

/// The per-literal `possible` gate map `eval_literal` consults:
/// `value|exact` -> (conversation -> candidate `quill_idx` set).
pub(super) type LiteralGate = HashMap<String, HashMap<i64, HashSet<i64>>>;

/// One fast-lane expected witness: `(branch_id, (literal value, exact))`
/// — a positive branch whose session-doc posting set contains the session.
pub(super) type FastClaim = (String, (String, bool));

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
/// [`super::eval::combine`] exactly like a real region key does.
const SESSION_DOC_WITNESS_KEY: &str = "\u{0}session_scope_doc";

/// A literal the session×scope documents can decide: one ASCII token the
/// analyzer can see (never `never` — a token-less literal matches nothing
/// and keeps its matcher-only semantics on the legacy lane).
fn literal_session_doc_eligible(
    value: &str,
    exact: bool,
    matchers: &[HashMap<String, LiteralMatcher>; 2],
) -> bool {
    value.is_ascii()
        && matchers[usize::from(exact)]
            .get(value)
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
pub(super) struct FastShape {
    pub(super) safe: bool,
    region_atom: bool,
}

pub(super) fn classify_fast_shape(
    node: &PlanNode,
    matchers: &[HashMap<String, LiteralMatcher>; 2],
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
            // (see [`super::regions::plan_reads_fragments`]) are never
            // lazily starved. Every other predicate reads structural fields
            // or the `SessionCtx`, which both lanes see identically.
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
/// [`super::SESSION_DOC_ENUM_PAGE`]-sized chunks (deadline checked at every
/// chunk boundary, honest `super::MAX_LITERAL_HITS` refusal on the first
/// chunk's exact total) over [`super::super::search::raw_page_session_docs`],
/// so the posting set contains conversations, never per-region coordinates.
pub(super) fn enumerate_session_doc_hits(
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
/// [`super::MAX_LITERAL_HITS`]; `None` when the literal is too common to
/// enumerate or the engine refused — that claim's witness discovery
/// degrades to the ungated structural scan.
pub(super) type WitnessGates = [HashMap<String, Option<HashMap<i64, HashSet<i64>>>>; 2];

/// Enumerate one claim literal's raw Quill hits — message docs + region
/// docs through the same `literal_query_text` ->
/// [`super::super::search::raw_page`] lowering the legacy lane's
/// [`enumerate_literal`] uses, so the quill fork's content-glob prefix arm
/// is respected — into the conversation -> `quill_idx` witness gate the
/// page-time discovery consults.
///
/// The gate is an optimization with a hard floor, never a contract
/// surface: a literal too common to enumerate honestly, an engine refusal
/// of any kind (fuel, glob expansion), a page without an exact count or a
/// hit without unambiguous coordinates all return `Ok(None)` — the claim's
/// discovery then runs ungated, today's semantics verbatim. Only the
/// cooperative deadline propagates: it is honest unavailability, not an
/// enumeration outcome.
pub(super) fn enumerate_witness_gate(
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
pub(super) struct LegacyLiteralEvidence {
    /// `(value, exact)` hit maps for witness scoring.
    pub(super) scores: HashMap<(String, bool), LiteralHits>,
    /// The `possible` gate map `eval_literal` consults.
    pub(super) possible: LiteralGate,
}

/// Per-literal Quill evidence over the per-REGION documents — the exact
/// enumeration the legacy lane runs (the loop `search_sessions` carried
/// inline before Stage 1, byte-for-byte).
pub(super) fn legacy_literal_evidence(
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
/// [`super::eval::eval_literal`]'s tail produces, so a fast-lane Unknown
/// classifies `facts_pending` identically to a legacy one.
pub(super) fn unseen_inventory_result(session: &SessionCtx) -> NodeResult {
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
                out.reasons.insert(Cow::Owned(format!(
                    "{field}:{}:inventory_incomplete",
                    session.coverage_of(field)
                )));
            }
        }
    }
    out
}

/// [`super::eval::eval_node`] over the session×scope posting sets (the fast
/// lane). Literals are per-session membership: a doc hit is proven
/// membership (the aggregated document's tokenization decided it); a miss
/// keeps today's inventory-tail Unknown semantics through
/// [`unseen_inventory_result`]; predicates read the same `SessionCtx` and
/// region rows as the legacy lane. Witnesses are NOT discovered here —
/// they are a page-time concern on this lane (emitted sessions only).
pub(super) fn eval_node_fast(
    node: &PlanNode,
    session: &SessionCtx,
    session_doc_hits: &[HashMap<String, HashMap<i64, f32>>; 2],
    seek: Option<&BlockSeekEvidence>,
) -> NodeResult {
    match node {
        PlanNode::All => NodeResult::constant(Truth::True, BTreeSet::new()),
        PlanNode::Literal { value, exact, .. } => {
            if session_doc_hits[usize::from(*exact)]
                .get(value.as_str())
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
pub(super) fn fast_score_and_claims(
    plan: &Plan,
    conversation_id: i64,
    session_doc_hits: &[HashMap<String, HashMap<i64, f32>>; 2],
) -> (Option<f64>, Vec<FastClaim>) {
    let mut best_score: Option<f64> = None;
    let mut claims: Vec<FastClaim> = Vec::new();
    for (branch, (value, exact)) in &plan.positive_branches {
        let Some(score) = session_doc_hits[usize::from(*exact)]
            .get(value.as_str())
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
/// [`super::eval::verify_region_text`] the legacy lane used at eval time —
/// so an emitted witness binds canonical coordinates and a snippet exactly
/// as before. The region slice is the Stage 1b page-time load: the
/// EMITTED session's full structural row set (one indexed read, same
/// `region_ordinal` order the eager universe load produced), so discovery
/// consults exactly the region set it always saw. A claim no region
/// reproduces (analyzer n-gram drift) returns an empty list; the caller
/// annotates `witness_count=0` with the `witnesses_not_reproduced`
/// warning instead of refusing the page — that warning therefore names
/// ONLY regions that were consulted and disagreed. A verification budget
/// exhaustion is NOT drift: discovery reports
/// [`FastWitnesses::BudgetExhausted`] and the caller refuses the whole page
/// with the typed `verification_budget_exceeded` kind (design §7), never a
/// member emitted with `complete:true`.
#[allow(clippy::too_many_arguments)]
pub(super) fn discover_fast_witnesses(
    connection: &Connection,
    regions: &[Region],
    claims: &[FastClaim],
    claim_gates: &[Option<&HashSet<i64>>],
    matchers: &[HashMap<String, LiteralMatcher>; 2],
    max_matches: usize,
    stats: &mut HydrateStats,
    deadline: &Deadline,
) -> Result<FastWitnesses> {
    let mut discovered: Vec<(String, String)> = Vec::new();
    for ((branch, (value, exact)), claim_gate) in claims.iter().zip(claim_gates) {
        if discovered.len() >= max_matches {
            break;
        }
        let Some(matcher) = matchers[usize::from(*exact)].get(value.as_str()) else {
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
pub(super) enum FastWitnesses {
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
pub(super) fn ungated_fast_witnesses(
    connection: &Connection,
    conversation_id: i64,
    claims: &[FastClaim],
    matchers: &[HashMap<String, LiteralMatcher>; 2],
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
