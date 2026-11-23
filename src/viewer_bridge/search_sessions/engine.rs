use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::Result;
use coding_agent_search::franken_sync::Connection;
use serde_json::{Map, Value, json};

use super::super::deadline::Deadline;
use super::super::facts_bag;
use super::super::identity;
use super::super::protocol::{ListFilters, RESPONSE_BYTES, refuse};
use super::super::revisions;
use super::super::search::{ReaderSet, engine_filters};
use super::cursor::{SearchCursor, decode_cursor, encode_cursor};
use super::dsl::{LiteralMatcher, compile_plan, decode_ast, parse_query_string};
use super::eval::{Truth, combine, control_result, eval_node};
use super::fast_lane::{
    FastClaim, FastWitnesses, LegacyLiteralEvidence, WitnessGates, classify_fast_shape,
    discover_fast_witnesses, enumerate_session_doc_hits, enumerate_witness_gate, eval_node_fast,
    fast_score_and_claims, legacy_literal_evidence, ungated_fast_witnesses,
};
use super::loader::{build_block_seek_evidence, load_sessions_indexed, load_universe, region_from};
use super::regions::{
    HydrateStats, Region, RegionLoadMode, SessionCtx, attach_gated_regions, plan_reads_fragments,
    plan_region_load_mode, prefetch_gated_message_texts, prefetch_region_fragments, region_text,
};
use super::{HARD_UNIVERSE_CEILING, MAX_QUERY_BYTES, ROW_BYTES};

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
/// [`super::MAX_TEXT_VERIFICATION_BYTES`]); `deadline` is the request's
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
    let engine_filters = engine_filters(&super::super::protocol::BridgeFilters {
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
    // literals — see [`super::regions::plan_region_load_mode`]); every
    // other plan loads its region rows on demand, scaled to hit/page size
    // instead of archive size.
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
        // lane is gate-filtered (see [`super::regions::attach_gated_regions`]).
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
    // Codex continuation rollouts (`rollout-<ts>-<A>_<B>.jsonl`) verify to
    // the same thread identity `codex:<A>` as the original. Fold every
    // matched row sharing a verified public id onto the lowest matched
    // canonical id, so one logical session is sorted, paged and emitted
    // once. The representative keeps its own witnesses and score (they are
    // bound to its conversation_id); an unverifiable row keeps its own
    // identity rather than being dropped or failing the page.
    {
        let matched_ids: Vec<i64> = accepted
            .iter()
            .map(|entry| entry.session.conversation_id)
            .collect();
        let verified = identity::resolve_public_ids_tolerant(connection, &matched_ids)?;
        let mut representative: BTreeMap<String, i64> = BTreeMap::new();
        for entry in &accepted {
            let cid = entry.session.conversation_id;
            if let Some(Some(identity)) = verified.get(&cid) {
                representative
                    .entry(identity.public_id.clone())
                    .and_modify(|min| *min = (*min).min(cid))
                    .or_insert(cid);
            }
        }
        accepted.retain(|entry| {
            let cid = entry.session.conversation_id;
            match verified.get(&cid) {
                Some(Some(identity)) => representative.get(&identity.public_id) == Some(&cid),
                _ => true,
            }
        });
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
    // One cursor per emitted item, parallel to `items`, so a post-enrichment
    // byte trim can move the resume position back to the last kept session.
    let mut item_cursors: Vec<SearchCursor> = Vec::new();
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
        let (title_preview, title_truncated, title_bytes) =
            facts_bag::bound_text_preview(session.title.clone());
        let mut item = json!({
            "conversation_id": session.conversation_id,
            "public_id": session.public_id,
            "provider": session.provider,
            "source_id": Value::Null,
            "workspace_path": session.project_path,
            "project": session.project,
            "title": title_preview,
            "title_truncated": title_truncated,
            "title_bytes": title_bytes,
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
        // Annotations are never truncated: a row whose serialized item
        // alone exceeds the per-row profile refuses typed (the list lane's
        // contract) rather than silently stalling the cursor on a row no
        // page can ever emit.
        if item_bytes > ROW_BYTES {
            return Err(refuse(
                "canonical_payload_too_large",
                "Canonical row exceeds the strict bridge byte profile",
            ));
        }
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
        // [`super::fast_lane::ungated_fast_witnesses`] — the pre-(B)
        // structural load + ungated discovery, §6.4's contract verbatim;
        // claims-empty entries (`NOT quux`) load nothing at all. Sessions
        // past the byte budget (the `break` above) never load.
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
        item_cursors.push(key.clone());
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
    // The page cut above budgeted pre-enrichment item bytes; identity
    // resolution and the per-session `matches` map add bytes it never saw.
    // Re-measure the payload the response actually carries and drop tail
    // sessions (with their matches) until it fits the response budget,
    // moving the cursor back to the last kept session so the walk resumes
    // exactly where the page stopped — every row still visited once.
    let mut trimmed = false;
    loop {
        let payload_bytes =
            serde_json::to_vec(&items)?.len() + serde_json::to_vec(&matches_out)?.len();
        if payload_bytes <= RESPONSE_BYTES - 8192 {
            break;
        }
        if items.len() <= 1 {
            return Err(refuse(
                "canonical_payload_too_large",
                "Page exceeds bridge response budget; request fewer rows",
            ));
        }
        let dropped = items.pop().expect("checked non-empty");
        let conversation_id = dropped["conversation_id"].as_i64().expect("id");
        // shift_remove keeps the remaining matches in emission order (the
        // map preserves insertion order; remove() would swap the last key in).
        matches_out.shift_remove(&conversation_id.to_string());
        item_cursors.pop();
        trimmed = true;
    }
    if trimmed {
        has_more = true;
        last = item_cursors.last().cloned();
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
