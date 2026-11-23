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
//!    composes over per-session truth (see [`fast_lane::classify_fast_shape`])
//!    answer from the published session×scope documents — one Quill doc per
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
//!    through [`regions::plan_region_load_mode`] — `emitted_page_sessions`
//!    (fast lane: the emitted page's conversations only, loaded at page
//!    time), `gated_quill_seeks` (legacy: per-literal Quill gates),
//!    `block_predicate_seeks` (value-provable block predicates answer
//!    through request-wide SQL value seeks + NULL-candidate probes, see
//!    [`loader::BlockSeekEvidence`]) or the `universe_structural` fallback —
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
/// [`regions::region_text_full`]), because an index hit (or, ungated, any
/// occurrence) may live past any prefix bound.
const MAX_REGION_TEXT: usize = 64 * 1024;
/// Per-row byte profile (the same 512 KiB bound as the list lane): a
/// search item whose never-truncated annotations alone exceed it refuses
/// typed instead of stalling the page cursor. Bounded previews keep
/// summary text far below this bound.
const ROW_BYTES: usize = 512 * 1024;
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
/// ([`regions::MAX_UNGATED_CHAT_HYDRATIONS`]) admits at most 4,096 chat-region
/// checks whose worst-case whole-body total is the top-4,096 byte sum,
/// 196 MB; the gated escalation set is the 233 messages over 64 KiB,
/// 26 MB. 512 MiB leaves ≥2.6x margin on the adversarial ungated shape
/// and ~20x on real totals. Overridable for tests and operators through
/// the bridge's `--text-verification-budget-bytes`.
pub(crate) const MAX_TEXT_VERIFICATION_BYTES: usize = 512 * 1024 * 1024;
const MAX_LITERAL_HITS: usize = 100_000;
/// Session-doc membership chunk: how many hits one
/// [`super::search::raw_page_session_docs`] call may return before
/// [`fast_lane::enumerate_session_doc_hits`] re-checks the deadline.
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

mod cursor;
mod dsl;
mod engine;
mod eval;
mod fast_lane;
mod loader;
mod regions;

#[cfg(test)]
#[path = "search_sessions/cursor_tests.rs"]
mod cursor_bit_roundtrip_tests;

pub use engine::search_sessions;
