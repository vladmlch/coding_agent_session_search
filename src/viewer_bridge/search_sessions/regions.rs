use std::collections::{HashMap, HashSet};

use anyhow::Result;
use coding_agent_search::franken_sync::compat::RowExt;
use coding_agent_search::franken_sync::{Connection, SqliteValue};
use coding_agent_search::viewer_extensions::facts::is_source_absent_gap;
use serde_json::Value;

use super::super::deadline::Deadline;
use super::MAX_REGION_TEXT;
use super::dsl::{LiteralMatcher, Plan, PlanNode};
use super::loader::region_from;

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
pub(super) struct Region {
    pub(super) key: String,
    pub(super) scope: String,
    pub(super) scope_known: bool,
    pub(super) quill_idx: i64,
    /// The owning conversation — the lazy fragment memo is keyed
    /// `(conversation_id, region_ordinal)`.
    pub(super) conversation_id: i64,
    pub(super) region_ordinal: i64,
    /// Structural `indexed_text IS NOT NULL` bit from the universe load.
    pub(super) has_fragment: bool,
    /// Canonical `messages.idx` anchoring this region (deep-link).
    pub(super) message_idx: Option<i64>,
    pub(super) message_id: Option<i64>,
    /// Structural anchors persisted for deep-link consumers; the
    /// `block_key` already pins the exact block today.
    #[allow(dead_code)]
    pub(super) step_index: Option<i64>,
    #[allow(dead_code)]
    pub(super) turn_index: Option<i64>,
    pub(super) tool_name: Option<String>,
    pub(super) file_path: Option<String>,
    pub(super) exit_code: Option<i64>,
    pub(super) has_error: Option<bool>,
    pub(super) error_type: Option<String>,
    pub(super) text_source: String,
    /// The persisted fragment — `None` at load (lazy); `Some` only after
    /// the `mcp`-predicate prefetch filled it in place.
    pub(super) indexed_text: Option<String>,
}

impl Region {
    /// A canonical-row chat region: no persisted fragment (noise-chat
    /// rows carry one) and the row anchors a canonical message, so the
    /// region's evidence is the FULL `messages.content` — the same body
    /// the canonical Quill document indexed. Only these regions hydrate
    /// the canonical row, and only they can hold a hit past the 64 KiB
    /// returned-text window.
    pub(super) fn hydrates_canonical_row(&self) -> bool {
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
pub(super) const MAX_UNGATED_CHAT_HYDRATIONS: usize = 4_096;

/// Hydration accounting for the indexed path — the diagnostics the brief
/// requires: every region/message materialization is counted so an
/// unrelated selective search provably never re-reads the corpus.
#[derive(Debug, Default)]
pub(super) struct HydrateStats {
    pub(super) regions_loaded: usize,
    pub(super) regions_hydrated: usize,
    pub(super) messages_hydrated: usize,
    /// Reads spent on ungated (non-ASCII) chat verification — bounded by
    /// [`MAX_UNGATED_CHAT_HYDRATIONS`].
    pub(super) ungated_chat_hydrations: usize,
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
    pub(super) text_memo: HashMap<i64, Option<String>>,
    /// Request-scoped memo of FULL `messages.content` rows — the
    /// verification lane's whole-body text, fetched lazily only for the
    /// regions that need it (a gated hit the bounded window cannot
    /// reproduce, or an ungated chat check) so a request's resident text
    /// stays proportional to what verification actually escalated, not to
    /// the gated candidate set. `None` memoizes a proven-miss anchor.
    pub(super) full_text_memo: HashMap<i64, Option<String>>,
    /// Stage 1 §6.3 lazy fragments: the persisted `indexed_text`
    /// fragments, keyed `(conversation_id, region_ordinal)`, fetched by
    /// [`hydrate_session_fragments`] on the FIRST fragment touch of a
    /// conversation — one read per session, never one per region — so a
    /// request's resident fragment text is proportional to the sessions
    /// whose evidence was actually consulted (the emitted page, the
    /// gated candidates), never to the universe.
    pub(super) fragment_memo: HashMap<(i64, i64), String>,
    /// Conversations whose fragment read already ran (memo provenance).
    pub(super) fragment_sessions: HashSet<i64>,
    /// Fragment reads spent this request (diagnostics:
    /// `fragments_loaded`) — one per first-touch session plus one per
    /// chunk of the `mcp`-predicate prefetch.
    pub(super) fragments_loaded: usize,
    /// Whole-body text bytes scanned by literal verification this
    /// request — the meter [`super::MAX_TEXT_VERIFICATION_BYTES`] ceilings.
    pub(super) verification_bytes: usize,
    /// The request's whole-body scan ceiling (from the bridge's
    /// `--text-verification-budget-bytes`, default
    /// [`super::MAX_TEXT_VERIFICATION_BYTES`]). Set once at request start.
    pub(super) verification_budget: usize,
    /// Calls spent fetching whole-body text (diagnostics; the memo makes
    /// repeated checks on the same message free).
    pub(super) full_messages_hydrated: usize,
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
pub(super) fn prefetch_gated_message_texts(
    connection: &Connection,
    sessions: &[SessionCtx],
    literals: &[(String, bool)],
    matchers: &[HashMap<String, LiteralMatcher>; 2],
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
                || matchers[usize::from(*exact)]
                    .get(value.as_str())
                    .is_some_and(|matcher| matcher.never)
            {
                return None;
            }
            quill_possible.get(&format!("{value}|{exact}"))
        })
        .collect();
    let mut pending: std::collections::BTreeSet<i64> = std::collections::BTreeSet::new();
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
/// fragment surface — see the pinned `mcp` arm in
/// [`super::eval::region_value`]). Such plans keep today's eager fragment
/// availability: the structural universe load (Stage 1 §6.3) is followed
/// by one chunked set read that fills `indexed_text` in place, so the
/// arm's semantics are byte-for-byte what the eager loader fed it. Every
/// other plan — the entire fast lane included — never pays for fragments
/// it did not consult.
pub(super) fn plan_reads_fragments(node: &PlanNode) -> bool {
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
/// [`super::loader::BlockSeekEvidence`]); region rows attach only where
/// literal gates pointed (the predicates never need resident rows).
/// Receipts as `block_predicate_seeks` (or `emitted_page_sessions` under
/// the fast lane, whose page strategy is unchanged).
///
/// `on_demand` — every other plan: literals are all gated (ASCII,
/// index-provable) and predicates are all session-domain, so region rows
/// are consulted only where the Quill gates pointed (legacy lane:
/// `gated_quill_seeks`; fast lane: the emitted page's sessions,
/// `emitted_page_sessions`). False regions are provable no-ops in both
/// consumers (`collection` counts only True/Unknown rows; gated
/// `eval_literal` skips every non-gated row), which is what makes loading
/// exactly the consulted rows byte-identical to loading all of them.
pub(super) enum RegionLoadMode {
    /// (reason) — today's eager structural universe load; the reason is
    /// receipted in diagnostics.
    UniverseStructural(&'static str),
    /// Block-domain predicates answer through request-wide SQL value seeks
    /// over the region columns (see [`super::loader::BlockSeekEvidence`]);
    /// region rows are read only where the predicates' values point (plus
    /// any literal gates), never as an eager universe load.
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
/// exactly (see [`super::loader::build_block_seek_evidence`]).
pub(super) fn seekable_block_field(field: &str) -> bool {
    matches!(
        field,
        "tool" | "file" | "exit" | "error_type" | "has_error" | "has_mcp"
    )
}

/// Does this subtree bear a POSITIVE seekable block predicate — one whose
/// evaluation produces a region-set [`super::eval::NodeResult`] that
/// participates in AND set algebra? `has_mcp` never bears (its result is a
/// collapsed constant, and constants compose under AND without key
/// surfaces); a predicate under a `Not` does not bear either (the `Not`
/// collapses the child's sets before any sibling intersection can consult
/// them). An `Or` of atoms counts as ONE atom at its parent AND (it needs
/// one matching region, not the same region for two conjuncts) — the same
/// composition rule [`super::fast_lane::classify_fast_shape`] applies to
/// fast-lane region atoms.
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

pub(super) fn plan_region_load_mode(
    node: &PlanNode,
    matchers: &[HashMap<String, LiteralMatcher>; 2],
) -> RegionLoadMode {
    match node {
        PlanNode::Literal { value, exact, .. } => {
            let never = matchers[usize::from(*exact)]
                .get(value.as_str())
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
pub(super) fn prefetch_region_fragments(
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
/// ([`coding_agent_search::viewer_extensions::search_regions::regions_for_quill_slots`]).
/// Every consumer of `session.regions` on this lane is gate-filtered
/// (`eval_literal` skips non-gated rows; `prefetch_gated_message_texts`
/// re-checks each literal's own gate; witnesses and snippets only
/// reference gated members), so a session with no gated hits for any
/// plan literal reads NOTHING — and the rows that do load evaluate
/// byte-identically to the eager universe load, in the same
/// `region_ordinal` order.
///
/// Non-ASCII and `never` literals contribute no slots: the former keep
/// the eager structural lane (see [`plan_region_load_mode`]), the latter
/// return from `eval_literal` before ever touching a region.
pub(super) fn attach_gated_regions(
    connection: &Connection,
    sessions: &mut [SessionCtx],
    plan: &Plan,
    matchers: &[HashMap<String, LiteralMatcher>; 2],
    quill_possible: &HashMap<String, HashMap<i64, HashSet<i64>>>,
    stats: &mut HydrateStats,
    deadline: &Deadline,
) -> Result<()> {
    let mut slots: HashMap<i64, Vec<i64>> = HashMap::new();
    for (value, exact) in &plan.literals {
        if !value.is_ascii()
            || matchers[usize::from(*exact)]
                .get(value.as_str())
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

/// Bounded text for one persisted region — `chat` reads the canonical
/// `messages.content` row it anchors (memoized per request; the gated
/// prefetch already materialized every gated candidate in one chunked set
/// read); `derived`/`meta`/noise-chat return the persisted fragment,
/// which hydrates LAZILY (Stage 1 §6.3): one read per conversation on
/// first fragment touch, memoized by `(conversation_id, region_ordinal)`
/// in [`HydrateStats::fragment_memo`]. `has_fragment` is the structural
/// bit the universe load persisted, so a region that never carried a
/// fragment answers `None` without a read.
pub(super) fn region_text(
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
pub(super) fn region_text_full(
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

pub(super) struct SessionCtx {
    pub(super) conversation_id: i64,
    pub(super) public_id: Option<String>,
    pub(super) provider: String,
    pub(super) project: Option<String>,
    pub(super) project_path: Option<String>,
    pub(super) title: Option<String>,
    pub(super) custom_title: Option<String>,
    pub(super) notes: Option<String>,
    pub(super) is_pinned: bool,
    pub(super) tags: Vec<String>,
    pub(super) is_subagent: Option<bool>,
    pub(super) category: Option<String>,
    pub(super) tokens: Option<i64>,
    pub(super) cost: Option<f64>,
    pub(super) duration: Option<f64>,
    pub(super) activity_ms: Option<i64>,
    pub(super) created_ms: Option<i64>,
    pub(super) model: Option<String>,
    pub(super) regions: Vec<Region>,
    /// Per-field coverage states (`coverage_detail_json /fields/<f>/state`
    /// of the persisted facts row, else the query-time extraction).
    pub(super) coverage: HashMap<String, String>,
    /// Per-field coverage reason codes (`coverage_detail_json
    /// /fields/<f>/reasons`) — the evidence that decides whether an
    /// incomplete state is text ABSENT AT THE SOURCE (owner decision
    /// 2026-11-07: exact, never blocks strict search) or text that was
    /// NOT READ (still blocks).
    pub(super) coverage_reasons: HashMap<String, Vec<String>>,
    /// Every reason code this session's facts prove as text absent at the
    /// source (sorted, deduped) — the per-item `source_text_absent`
    /// annotation. Empty for sessions without such gaps.
    pub(super) source_text_absent: Vec<String>,
    /// `true` when the message inventory was truncated by budget.
    pub(super) inventory_partial: bool,
    /// `true` when the persisted publication mark is fully converged —
    /// `docs_state == published` AND the mark's `region_version` equals
    /// the current REGION_VERSION. The session×scope fast lane engages
    /// only when EVERY universe member sets this (one stale session keeps
    /// the whole query on the legacy region-literal lane).
    pub(super) session_docs_current: bool,
}

impl SessionCtx {
    pub(super) fn effective_title(&self) -> Option<&str> {
        self.custom_title.as_deref().or(self.title.as_deref())
    }
    pub(super) fn coverage_of(&self, field: &str) -> &str {
        self.coverage
            .get(field)
            .map(String::as_str)
            .unwrap_or("unavailable")
    }
    pub(super) fn coverage_reasons_of(&self, field: &str) -> &[String] {
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
    pub(super) fn field_blocks_strict(&self, field: &str) -> bool {
        let state = self.coverage_of(field);
        state != "complete" && !is_source_absent_gap(state, self.coverage_reasons_of(field))
    }
}
