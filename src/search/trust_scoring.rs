//! Metadata-only trust/provenance scoring for search hits, sessions, and answer
//! pack evidence.
//!
//! Bead: coding_agent_session_search-guided-ops-repro-trust-5u82n.3
//! ("Trust-score search results and answer packs with provenance signals").
//!
//! ## Why
//!
//! cass surfaces *relevant* old conversations, but relevance is not correctness.
//! A result can be a landed fix — or a failed agent attempt, a superseded
//! workaround, or advice from a different workspace. This module reduces a
//! small set of **metadata-only** signals into a compact verdict
//! ([`TrustAssessment`]) so robot consumers can tell "reuse this" from
//! "do not copy this" without re-reading the conversation.
//!
//! ## Metadata-only, no raw text (by construction)
//!
//! [`TrustSignals`] carries only derived signals — ages, booleans, an outcome
//! marker, and a structured commit identifier. There is no field that holds
//! raw session/prompt/tool text, so this layer cannot leak it. Provenance refs
//! are compact, sanitized `commit:<sha>` identifiers; commit shas are
//! truncated. The verdict never changes result ordering — it is advisory
//! metadata only.
//!
//! ## Pure and deterministic
//!
//! [`assess_trust`] does no I/O and is a pure function of its input, so the same
//! signals always yield the same verdict, refs, reason, and confidence — safe to
//! pin in golden tests and fixtures.

use serde::{Deserialize, Serialize};

/// Stable schema version for the trust wire format.
pub const TRUST_SCHEMA_VERSION: u32 = 1;

/// A coarse, ordered verdict on how much to trust a result. Declaration order is
/// most-trusted → least-trusted, so derived `Ord` ranks `Trusted < … < Failed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrustTier {
    /// Landed, proof-backed work with strong provenance (a linked commit with
    /// proof). Safe to reuse.
    Trusted,
    /// Probably current — has provenance (a linked commit) but is not fully
    /// proof-backed. Confirm before relying on it.
    Likely,
    /// Relevant but unverified: no provenance links, an open outcome, or the
    /// result could only be corroborated lexically.
    Unverified,
    /// Known to be out of date — superseded by newer work or aged past its
    /// useful window.
    Stale,
    /// A failed or reverted attempt. Do not reuse.
    Failed,
}

impl TrustTier {
    /// Stable snake_case wire label.
    pub fn as_str(self) -> &'static str {
        match self {
            TrustTier::Trusted => "trusted",
            TrustTier::Likely => "likely",
            TrustTier::Unverified => "unverified",
            TrustTier::Stale => "stale",
            TrustTier::Failed => "failed",
        }
    }
}

/// Confidence in the trust verdict itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrustConfidence {
    Low,
    Medium,
    High,
}

impl TrustConfidence {
    /// Stable snake_case wire label.
    pub fn as_str(self) -> &'static str {
        match self {
            TrustConfidence::Low => "low",
            TrustConfidence::Medium => "medium",
            TrustConfidence::High => "high",
        }
    }

    /// The lower (more cautious) of two confidences.
    fn floor(self, other: TrustConfidence) -> TrustConfidence {
        if self <= other { self } else { other }
    }
}

/// The recorded outcome of the work a result represents (a metadata marker
/// derived from linked commits and known stale/failed signals).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeMarker {
    /// Linked to a landed commit.
    Landed,
    /// A failed command / reverted attempt.
    Failed,
    /// A newer result supersedes this one.
    Superseded,
    /// Linked to open / in-progress work.
    Open,
    /// No outcome signal available.
    Unknown,
}

/// Proof status of the linked work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProofStatus {
    /// Linked work carries proof artifacts (tests/gates/E2E).
    Proven,
    /// Claimed done but with outstanding proof debt.
    ProofDebt,
    /// No proof signal.
    Unknown,
}

/// Which search tier actually realized the result (lexical-only means semantic
/// could not corroborate the match).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RealizedMode {
    Hybrid,
    Semantic,
    Lexical,
}

/// Metadata-only inputs to trust scoring. Carries no raw session text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrustSignals {
    /// Age of the result in days (None when unknown).
    #[serde(default)]
    pub age_days: Option<u64>,
    /// Whether the result's workspace matches the active query workspace.
    pub workspace_match: bool,
    /// Linked commit sha, if any (provenance; truncated into refs).
    #[serde(default)]
    pub linked_commit: Option<String>,
    /// Recorded outcome marker.
    pub outcome: OutcomeMarker,
    /// Proof status of the linked work.
    pub proof: ProofStatus,
    /// Whether the backing source is healthy/reachable.
    pub source_healthy: bool,
    /// Realized search tier.
    pub realized_mode: RealizedMode,
}

impl Default for TrustSignals {
    fn default() -> Self {
        TrustSignals {
            age_days: None,
            workspace_match: true,
            linked_commit: None,
            outcome: OutcomeMarker::Unknown,
            proof: ProofStatus::Unknown,
            source_healthy: true,
            realized_mode: RealizedMode::Hybrid,
        }
    }
}

/// The metadata-only trust verdict exposed in robot output. Advisory; it never
/// changes result ordering.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrustAssessment {
    /// Mirrors [`TRUST_SCHEMA_VERSION`].
    pub schema_version: u32,
    /// Coarse trust tier.
    pub trust_tier: TrustTier,
    /// Confidence in the verdict.
    pub confidence: TrustConfidence,
    /// Sanitized provenance identifiers (e.g. `commit:ab0d12ef90ab`).
    /// Deterministic order; never raw text or paths.
    pub provenance_refs: Vec<String>,
    /// Stable snake_case code for the dominant reason trust is reduced, when the
    /// result is not fully trusted (e.g. `failed_attempt`, `superseded_by_newer`,
    /// `linked_open`, `aged_out`, `workspace_mismatch`, `source_unhealthy`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stale_reason: Option<String>,
    /// Advisory next step (text only — never a destructive command).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recommended_followup: Option<String>,
}

/// Age past which a result is treated as stale.
const STALE_AGE_DAYS: u64 = 365;
/// Age past which an already-unverified result is flagged as aging.
const AGING_AGE_DAYS: u64 = 180;
/// Max characters kept from a commit sha in a provenance ref.
const COMMIT_REF_LEN: usize = 12;

/// Build the deterministic, sanitized provenance ref list.
fn build_provenance_refs(signals: &TrustSignals) -> Vec<String> {
    let mut refs = Vec::new();
    if let Some(commit) = &signals.linked_commit {
        let short: String = commit
            .chars()
            .filter(|c| c.is_ascii_hexdigit())
            .take(COMMIT_REF_LEN)
            .collect();
        if !short.is_empty() {
            refs.push(format!("commit:{short}"));
        }
    }
    refs
}

/// Score a result's trust from metadata-only [`TrustSignals`]. Pure and
/// deterministic.
pub fn assess_trust(signals: &TrustSignals) -> TrustAssessment {
    let provenance_refs = build_provenance_refs(signals);
    let has_commit = signals.linked_commit.is_some();
    let proven = matches!(signals.proof, ProofStatus::Proven);
    let has_provenance = has_commit;

    // 1) Negative outcomes dominate — never reuse a failed or superseded result.
    if matches!(signals.outcome, OutcomeMarker::Failed) {
        return TrustAssessment {
            schema_version: TRUST_SCHEMA_VERSION,
            trust_tier: TrustTier::Failed,
            confidence: TrustConfidence::Low,
            provenance_refs,
            stale_reason: Some("failed_attempt".to_string()),
            recommended_followup: Some(
                "This was a failed or reverted attempt — prefer a newer landed result.".to_string(),
            ),
        };
    }
    if matches!(signals.outcome, OutcomeMarker::Superseded) {
        return TrustAssessment {
            schema_version: TRUST_SCHEMA_VERSION,
            trust_tier: TrustTier::Stale,
            confidence: TrustConfidence::Low,
            provenance_refs,
            stale_reason: Some("superseded_by_newer".to_string()),
            recommended_followup: Some(
                "A newer change supersedes this — look for the superseding fix.".to_string(),
            ),
        };
    }

    // 2) Positive provenance strength sets the base tier.
    let landed = matches!(signals.outcome, OutcomeMarker::Landed);
    let mut tier = if landed && proven && has_commit {
        TrustTier::Trusted
    } else if has_provenance {
        TrustTier::Likely
    } else {
        TrustTier::Unverified
    };

    // 3) Demotions + dominant reason (first/highest-priority reason wins) +
    //    confidence floor. The check order below is the reason priority order.
    let mut confidence = TrustConfidence::High;
    let mut reason: Option<&'static str> = None;

    // Source health gates how sure we are the result is reachable/actionable,
    // not whether the underlying work landed — it lowers confidence only.
    if !signals.source_healthy {
        confidence = confidence.floor(TrustConfidence::Low);
        reason.get_or_insert("source_unhealthy");
    }
    if matches!(signals.outcome, OutcomeMarker::Open) {
        if tier < TrustTier::Unverified {
            tier = TrustTier::Unverified;
        }
        confidence = confidence.floor(TrustConfidence::Medium);
        reason.get_or_insert("linked_open");
    }
    if let Some(age) = signals.age_days {
        if age >= STALE_AGE_DAYS {
            if tier < TrustTier::Stale {
                tier = TrustTier::Stale;
            }
            confidence = confidence.floor(TrustConfidence::Medium);
            reason.get_or_insert("aged_out");
        } else if age >= AGING_AGE_DAYS && matches!(tier, TrustTier::Unverified) {
            confidence = confidence.floor(TrustConfidence::Medium);
            reason.get_or_insert("aged_out");
        }
    }
    if !signals.workspace_match {
        confidence = confidence.floor(TrustConfidence::Medium);
        reason.get_or_insert("workspace_mismatch");
    }
    // Lexical-only realization means semantic could not corroborate the match.
    if matches!(signals.realized_mode, RealizedMode::Lexical) {
        confidence = confidence.floor(TrustConfidence::Medium);
    }
    // No provenance at all caps confidence regardless of relevance.
    if !has_provenance {
        confidence = confidence.floor(TrustConfidence::Medium);
    }
    // An unverified tier is never high-confidence.
    if matches!(tier, TrustTier::Unverified) {
        confidence = confidence.floor(TrustConfidence::Medium);
    }

    let recommended_followup = match tier {
        TrustTier::Trusted => None,
        TrustTier::Likely => {
            Some("Likely current — confirm via the cited commit before relying on it.".to_string())
        }
        TrustTier::Unverified => {
            Some("Unverified — corroborate before reuse (no linked commit).".to_string())
        }
        TrustTier::Stale => {
            Some("Aged out of its useful window — prefer a newer landed result.".to_string())
        }
        // Failed/Superseded already returned above.
        TrustTier::Failed => None,
    };

    TrustAssessment {
        schema_version: TRUST_SCHEMA_VERSION,
        trust_tier: tier,
        confidence,
        provenance_refs,
        stale_reason: reason.map(str::to_string),
        recommended_followup,
    }
}

/// How the backing source relates to an indexed result (drives `source_healthy`).
/// Maps from `crate::search::source_provenance::ProvenanceKind` at projection time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceTrustKind {
    /// A live local file currently backs the result.
    LocalPresent,
    /// Backed by a reachable remote mirror.
    RemoteMirror,
    /// Only the archive DB row survives (source pruned, deleted, or unreachable).
    ArchiveOnly,
}

/// Metadata available about a single result at projection time, used to derive
/// [`TrustSignals`]. All metadata-only — no raw session text. The richer
/// `linked_commit`/`outcome`/`proof` fields are populated by the local git
/// commit-reference correlation layer and default to "no signal" until it
/// lands, so the recency/workspace/source/mode portion of the verdict works on
/// its own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HitTrustContext {
    /// Result creation/index time (epoch ms), if known.
    pub created_at_ms: Option<i64>,
    /// Reference "now" (epoch ms) for age computation (injected for determinism).
    pub now_ms: i64,
    /// The result's workspace, if any.
    pub workspace: Option<String>,
    /// The active query workspace filter, if any (None = no workspace filter).
    pub query_workspace: Option<String>,
    /// How the backing source relates to the result.
    pub source_kind: SourceTrustKind,
    /// The realized search tier.
    pub realized_mode: RealizedMode,
    /// Linked commit sha (correlation layer; None until available).
    pub linked_commit: Option<String>,
    /// Outcome marker (correlation layer; Unknown until available).
    pub outcome: OutcomeMarker,
    /// Proof status (correlation layer; Unknown until available).
    pub proof: ProofStatus,
}

impl Default for HitTrustContext {
    fn default() -> Self {
        HitTrustContext {
            created_at_ms: None,
            now_ms: 0,
            workspace: None,
            query_workspace: None,
            source_kind: SourceTrustKind::LocalPresent,
            realized_mode: RealizedMode::Hybrid,
            linked_commit: None,
            outcome: OutcomeMarker::Unknown,
            proof: ProofStatus::Unknown,
        }
    }
}

/// Normalize a workspace path for comparison (trim + drop a trailing slash).
fn normalize_workspace(ws: &str) -> &str {
    ws.trim().trim_end_matches('/')
}

/// Derive [`TrustSignals`] from the metadata available about one result. Pure
/// and deterministic given an injected `now_ms`.
pub fn derive_trust_signals(ctx: &HitTrustContext) -> TrustSignals {
    let age_days = ctx.created_at_ms.map(|created| {
        // Clamp future timestamps to 0 rather than underflowing.
        let delta_ms = ctx.now_ms.saturating_sub(created).max(0);
        (delta_ms / 86_400_000) as u64
    });
    let workspace_match = match (ctx.query_workspace.as_deref(), ctx.workspace.as_deref()) {
        // No active workspace filter — do not penalize on workspace grounds.
        (None, _) => true,
        (Some(q), Some(w)) => normalize_workspace(q).eq_ignore_ascii_case(normalize_workspace(w)),
        // A workspace filter is active but the result has none — treat as a miss.
        (Some(_), None) => false,
    };
    let source_healthy = matches!(
        ctx.source_kind,
        SourceTrustKind::LocalPresent | SourceTrustKind::RemoteMirror
    );
    TrustSignals {
        age_days,
        workspace_match,
        linked_commit: ctx.linked_commit.clone(),
        outcome: ctx.outcome,
        proof: ctx.proof,
        source_healthy,
        realized_mode: ctx.realized_mode,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn landed_commit_proven() -> TrustSignals {
        TrustSignals {
            age_days: Some(10),
            workspace_match: true,
            linked_commit: Some("ab0d12ef90abcdef1234567890abcdef12345678".to_string()),
            outcome: OutcomeMarker::Landed,
            proof: ProofStatus::Proven,
            source_healthy: true,
            realized_mode: RealizedMode::Hybrid,
        }
    }

    // ---- the seven required fixture cases ----------------------------------

    #[test]
    fn landed_commit_is_trusted_high() {
        let a = assess_trust(&landed_commit_proven());
        assert_eq!(a.trust_tier, TrustTier::Trusted);
        assert_eq!(a.confidence, TrustConfidence::High);
        assert_eq!(a.stale_reason, None);
        assert_eq!(a.provenance_refs, vec!["commit:ab0d12ef90ab".to_string()]);
    }

    #[test]
    fn open_outcome_is_unverified_with_reason() {
        let s = TrustSignals {
            outcome: OutcomeMarker::Open,
            proof: ProofStatus::ProofDebt,
            age_days: Some(5),
            ..TrustSignals::default()
        };
        let a = assess_trust(&s);
        assert_eq!(a.trust_tier, TrustTier::Unverified);
        assert_eq!(a.stale_reason.as_deref(), Some("linked_open"));
        assert!(a.confidence <= TrustConfidence::Medium);
    }

    #[test]
    fn failed_command_is_failed_low() {
        let s = TrustSignals {
            outcome: OutcomeMarker::Failed,
            ..TrustSignals::default()
        };
        let a = assess_trust(&s);
        assert_eq!(a.trust_tier, TrustTier::Failed);
        assert_eq!(a.confidence, TrustConfidence::Low);
        assert_eq!(a.stale_reason.as_deref(), Some("failed_attempt"));
        assert!(a.recommended_followup.is_some());
    }

    #[test]
    fn superseded_fix_is_stale() {
        let s = TrustSignals {
            outcome: OutcomeMarker::Superseded,
            linked_commit: Some("deadbeefdeadbeefdeadbeef".to_string()),
            ..TrustSignals::default()
        };
        let a = assess_trust(&s);
        assert_eq!(a.trust_tier, TrustTier::Stale);
        assert_eq!(a.stale_reason.as_deref(), Some("superseded_by_newer"));
        // Provenance still surfaced even when stale.
        assert!(a.provenance_refs.iter().any(|r| r.starts_with("commit:")));
    }

    #[test]
    fn lexical_only_fallback_caps_confidence() {
        // Otherwise-trusted result, but only lexical corroboration.
        let mut s = landed_commit_proven();
        s.realized_mode = RealizedMode::Lexical;
        let a = assess_trust(&s);
        // Tier unchanged (ordering not affected), but confidence is capped.
        assert_eq!(a.trust_tier, TrustTier::Trusted);
        assert!(a.confidence <= TrustConfidence::Medium);
    }

    #[test]
    fn workspace_mismatch_lowers_confidence_and_sets_reason() {
        let mut s = landed_commit_proven();
        s.workspace_match = false;
        let a = assess_trust(&s);
        assert_eq!(a.trust_tier, TrustTier::Trusted, "ordering/tier unaffected");
        assert!(a.confidence <= TrustConfidence::Medium);
        assert_eq!(a.stale_reason.as_deref(), Some("workspace_mismatch"));
    }

    // ---- additional invariants ---------------------------------------------

    #[test]
    fn aged_result_becomes_stale() {
        let s = TrustSignals {
            age_days: Some(400),
            linked_commit: Some("abcabcabcabc".to_string()),
            outcome: OutcomeMarker::Landed,
            proof: ProofStatus::ProofDebt,
            ..TrustSignals::default()
        };
        let a = assess_trust(&s);
        assert_eq!(a.trust_tier, TrustTier::Stale);
        assert_eq!(a.stale_reason.as_deref(), Some("aged_out"));
    }

    #[test]
    fn no_provenance_is_unverified_capped_medium() {
        let a = assess_trust(&TrustSignals::default());
        assert_eq!(a.trust_tier, TrustTier::Unverified);
        assert!(a.provenance_refs.is_empty());
        assert!(a.confidence <= TrustConfidence::Medium);
    }

    #[test]
    fn source_unhealthy_dominates_reason_and_drops_confidence() {
        let mut s = landed_commit_proven();
        s.source_healthy = false;
        let a = assess_trust(&s);
        assert_eq!(a.confidence, TrustConfidence::Low);
        assert_eq!(a.stale_reason.as_deref(), Some("source_unhealthy"));
    }

    #[test]
    fn refs_are_sanitized_to_safe_tokens() {
        // Pathological ids: spaces, path separators, quotes, an injection phrase.
        let s = TrustSignals {
            linked_commit: Some("ab0d12ef90ab /home/alice/x 'or' 1=1".to_string()),
            outcome: OutcomeMarker::Landed,
            proof: ProofStatus::Proven,
            ..TrustSignals::default()
        };
        let a = assess_trust(&s);
        for r in &a.provenance_refs {
            // Each ref is `prefix:value`; the value carries only id chars.
            let value = r.split_once(':').map(|(_, v)| v).unwrap_or(r);
            assert!(
                value
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_')),
                "ref value not token-safe: {r}"
            );
        }
        let joined = a.provenance_refs.join(" ");
        assert!(
            !joined.contains('/'),
            "no path separators in refs: {joined}"
        );
        assert!(
            !joined.contains("table users"),
            "injection phrase broken: {joined}"
        );
    }

    #[test]
    fn assessment_json_contract_is_stable_and_round_trips() {
        let a = assess_trust(&landed_commit_proven());
        let value = serde_json::to_value(&a).unwrap();
        assert_eq!(value["schema_version"], TRUST_SCHEMA_VERSION);
        assert_eq!(value["trust_tier"], "trusted");
        assert_eq!(value["confidence"], "high");
        // stale_reason omitted (None) when fully trusted.
        assert!(value.get("stale_reason").is_none());
        let back: TrustAssessment = serde_json::from_value(value).unwrap();
        assert_eq!(back, a);
    }

    #[test]
    fn scoring_is_deterministic() {
        let s = landed_commit_proven();
        assert_eq!(assess_trust(&s), assess_trust(&s));
    }

    #[test]
    fn tier_and_confidence_wire_labels_are_stable() {
        assert_eq!(TrustTier::Trusted.as_str(), "trusted");
        assert_eq!(TrustTier::Failed.as_str(), "failed");
        assert_eq!(TrustConfidence::High.as_str(), "high");
        // Ordering: more trusted sorts lower; higher confidence sorts higher.
        assert!(TrustTier::Trusted < TrustTier::Failed);
        assert!(TrustConfidence::Low < TrustConfidence::High);
    }

    // ---- derivation bridge (HitTrustContext -> TrustSignals) ----------------

    const DAY_MS: i64 = 86_400_000;

    #[test]
    fn derive_age_days_handles_none_recent_and_future() {
        let none = derive_trust_signals(&HitTrustContext {
            created_at_ms: None,
            now_ms: 100 * DAY_MS,
            ..HitTrustContext::default()
        });
        assert_eq!(none.age_days, None);

        let recent = derive_trust_signals(&HitTrustContext {
            created_at_ms: Some(98 * DAY_MS),
            now_ms: 100 * DAY_MS,
            ..HitTrustContext::default()
        });
        assert_eq!(recent.age_days, Some(2));

        // A future creation time clamps to 0 rather than underflowing.
        let future = derive_trust_signals(&HitTrustContext {
            created_at_ms: Some(200 * DAY_MS),
            now_ms: 100 * DAY_MS,
            ..HitTrustContext::default()
        });
        assert_eq!(future.age_days, Some(0));
    }

    #[test]
    fn derive_workspace_match_normalizes_and_handles_no_filter() {
        // No active filter -> always a match (no penalty).
        let no_filter = derive_trust_signals(&HitTrustContext {
            query_workspace: None,
            workspace: Some("/proj/a".to_string()),
            ..HitTrustContext::default()
        });
        assert!(no_filter.workspace_match);

        // Trailing slash + case differences still match.
        let normalized = derive_trust_signals(&HitTrustContext {
            query_workspace: Some("/Proj/A/".to_string()),
            workspace: Some("/proj/a".to_string()),
            ..HitTrustContext::default()
        });
        assert!(normalized.workspace_match);

        let mismatch = derive_trust_signals(&HitTrustContext {
            query_workspace: Some("/proj/a".to_string()),
            workspace: Some("/proj/b".to_string()),
            ..HitTrustContext::default()
        });
        assert!(!mismatch.workspace_match);

        // Filter active but result has no workspace -> miss.
        let missing = derive_trust_signals(&HitTrustContext {
            query_workspace: Some("/proj/a".to_string()),
            workspace: None,
            ..HitTrustContext::default()
        });
        assert!(!missing.workspace_match);
    }

    #[test]
    fn derive_source_health_maps_from_kind() {
        for (kind, healthy) in [
            (SourceTrustKind::LocalPresent, true),
            (SourceTrustKind::RemoteMirror, true),
            (SourceTrustKind::ArchiveOnly, false),
        ] {
            let s = derive_trust_signals(&HitTrustContext {
                source_kind: kind,
                ..HitTrustContext::default()
            });
            assert_eq!(s.source_healthy, healthy, "kind {kind:?}");
        }
    }

    #[test]
    fn derive_then_assess_recent_unlinked_local_is_unverified() {
        // The common case until correlation lands: a recent, healthy, local,
        // hybrid result with no provenance links scores Unverified at <= medium.
        let ctx = HitTrustContext {
            created_at_ms: Some(99 * DAY_MS),
            now_ms: 100 * DAY_MS,
            source_kind: SourceTrustKind::LocalPresent,
            realized_mode: RealizedMode::Hybrid,
            ..HitTrustContext::default()
        };
        let a = assess_trust(&derive_trust_signals(&ctx));
        assert_eq!(a.trust_tier, TrustTier::Unverified);
        assert!(a.confidence <= TrustConfidence::Medium);
        assert!(a.provenance_refs.is_empty());
    }

    #[test]
    fn derive_then_assess_old_archive_only_is_stale() {
        let ctx = HitTrustContext {
            created_at_ms: Some(0),
            now_ms: 500 * DAY_MS,
            source_kind: SourceTrustKind::ArchiveOnly,
            ..HitTrustContext::default()
        };
        let a = assess_trust(&derive_trust_signals(&ctx));
        assert_eq!(a.trust_tier, TrustTier::Stale);
        // source_unhealthy is the highest-priority reason.
        assert_eq!(a.stale_reason.as_deref(), Some("source_unhealthy"));
    }
}
