// Dead-code tolerated module-wide: the readiness vocabulary lands here
// ahead of wiring into status / health / capabilities / search metadata
// callers. Downstream slices will plug these types into the JSON payload
// builders in src/lib.rs and the TUI status surface.
#![allow(dead_code)]

//! Truthful readiness-state vocabulary for lexical vs. semantic search
//! (bead ibuuh.9).
//!
//! Today cass reports a single "healthy / unhealthy" bit that conflates
//! "lexical index missing" (actually broken — search returns nothing),
//! "lexical index stale but searchable" (slightly old but fully correct),
//! "lexical index rebuilding in background" (search works, new content
//! will land shortly), and "semantic tier still backfilling" (lexical
//! results are complete, hybrid refinement catches up later). Agents and
//! humans keep triggering unnecessary repair rituals because the single
//! health bit cannot distinguish these cases.
//!
//! This module lands the vocabulary that future status/capabilities/
//! search-metadata payloads will project into their JSON. The fields are
//! intentionally orthogonal — lexical readiness and semantic readiness
//! are independent dimensions, and the user-facing `recommended_action`
//! is derived from their combination rather than dropping them behind a
//! single scalar.
//!
//! Invariants the types enforce:
//! - `LexicalReadinessState` covers the five states any agent must be
//!   able to distinguish: `Missing`, `Repairing`, `StaleButSearchable`,
//!   `Ready`, `CorruptQuarantined`. Ordinary search is correct in
//!   `StaleButSearchable` and `Ready` (and degrading-but-serving in
//!   `Repairing`); it is only unavailable in `Missing` and
//!   `CorruptQuarantined`.
//! - `SemanticReadinessState` covers `Absent`, `Backfilling`,
//!   `FastTierReady`, `HybridReady`, `PolicyDisabled`. Absence and
//!   policy-disabled both mean "no semantic refinement" but have
//!   different operator implications.
//! - `SearchRefinementLevel` describes what a PARTICULAR completed
//!   search actually returned (`LexicalOnly`, `FastTierRefined`,
//!   `FullyHybridRefined`). This is independent of the tier
//!   *readiness* above — a search may be `LexicalOnly` either because
//!   the semantic tier was absent or because the planner chose not to
//!   refine.
//! - `ReadinessSnapshot` groups all three plus a
//!   `RecommendedAction` so every downstream consumer (CLI, TUI,
//!   robot) derives its summary from the same canonical source.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LexicalReadinessState {
    /// No usable lexical index exists on disk. Search is unavailable
    /// until a rebuild runs.
    Missing,
    /// A lexical rebuild is actively running; ordinary queries may
    /// return partial results until the rebuild settles.
    Repairing,
    /// The lexical index exists and is byte-consistent but is known to
    /// lag recent DB mutations. Search is fully correct for everything
    /// already indexed; recent ingests may not be visible yet.
    StaleButSearchable,
    /// The lexical index is up to date against the canonical DB.
    Ready,
    /// The lexical index failed validation and has been quarantined
    /// for inspection. Search is unavailable; operator inspection is
    /// required before any auto-recover path is safe.
    CorruptQuarantined,
}

impl LexicalReadinessState {
    /// Whether ordinary search can run against this state. True for
    /// Ready, StaleButSearchable, and Repairing (degraded); false for
    /// Missing and CorruptQuarantined.
    pub(crate) fn is_searchable(self) -> bool {
        matches!(
            self,
            Self::Ready | Self::StaleButSearchable | Self::Repairing
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SemanticReadinessState {
    /// No semantic assets exist. Hybrid refinement is impossible until
    /// an acquisition run lands the required models and embeddings.
    Absent,
    /// Semantic assets are being acquired or backfilled; fast-tier
    /// refinement may become available mid-flight.
    Backfilling,
    /// Fast-tier semantic assets are ready; the quality tier is not
    /// yet available.
    FastTierReady,
    /// Both tiers ready; fully hybrid refinement is possible.
    HybridReady,
    /// The operator explicitly disabled semantic search via policy;
    /// absence is intentional, not a failure condition.
    PolicyDisabled,
}

impl SemanticReadinessState {
    /// Whether the semantic tier can contribute to query refinement at
    /// this state. True only for `FastTierReady` and `HybridReady`.
    pub(crate) fn can_refine(self) -> bool {
        matches!(self, Self::FastTierReady | Self::HybridReady)
    }
}

/// What a completed search actually produced. Independent of tier
/// *readiness* — a search can be `LexicalOnly` either because the
/// semantic tier was absent or because the planner chose not to refine
/// (e.g., a pinned-lexical flag or a fail-open demotion).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SearchRefinementLevel {
    LexicalOnly,
    FastTierRefined,
    FullyHybridRefined,
}

/// Operator / agent-facing remediation recommendation. Derived from a
/// `ReadinessSnapshot` rather than stored; kept as an enum so
/// downstream consumers can pattern-match consistently across CLI,
/// TUI, and robot payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RecommendedAction {
    /// Everything is converged or acceptably degraded; no user action
    /// needed.
    NothingRequired,
    /// The lexical index is missing or quarantined and must be
    /// rebuilt before search can resume.
    RepairLexicalNow,
    /// A lexical repair is already running. Foreground callers should
    /// attach or wait boundedly instead of starting another rebuild or
    /// reporting the semantic tier as the active wait reason.
    WaitForLexicalRepair,
    /// Lexical search is working; semantic assets are still
    /// converging. Waiting is sufficient.
    WaitForSemanticCatchUp,
    /// Lexical index is stale; a rebuild is recommended to pick up
    /// recent ingests but search continues to work in the meantime.
    RefreshLexicalSoon,
    /// Policy explicitly disabled semantic refinement; nothing to do
    /// beyond acknowledging the degraded search quality.
    SemanticDisabledByPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ReadinessSnapshot {
    pub lexical: LexicalReadinessState,
    pub semantic: SemanticReadinessState,
    /// Optional: the refinement level the most recent completed
    /// search actually achieved. `None` when no search has run since
    /// startup.
    #[serde(default)]
    pub last_search_refinement: Option<SearchRefinementLevel>,
}

impl ReadinessSnapshot {
    pub(crate) fn new(lexical: LexicalReadinessState, semantic: SemanticReadinessState) -> Self {
        Self {
            lexical,
            semantic,
            last_search_refinement: None,
        }
    }

    pub(crate) fn with_last_search_refinement(mut self, level: SearchRefinementLevel) -> Self {
        self.last_search_refinement = Some(level);
        self
    }

    /// Derive the recommended operator action from the current
    /// readiness state. Deliberately simple and conservative: the
    /// lexical axis dominates (a broken lexical index is a real
    /// outage; semantic issues are degraded-service at worst).
    pub(crate) fn recommended_action(&self) -> RecommendedAction {
        match self.lexical {
            LexicalReadinessState::Missing | LexicalReadinessState::CorruptQuarantined => {
                RecommendedAction::RepairLexicalNow
            }
            LexicalReadinessState::Repairing => {
                // Lexical repair dominates every semantic state: the
                // foreground contract is attach/wait/fail-open for the
                // active repair, not a second rebuild or a semantic wait.
                RecommendedAction::WaitForLexicalRepair
            }
            LexicalReadinessState::StaleButSearchable => RecommendedAction::RefreshLexicalSoon,
            LexicalReadinessState::Ready => match self.semantic {
                SemanticReadinessState::Absent | SemanticReadinessState::Backfilling => {
                    RecommendedAction::WaitForSemanticCatchUp
                }
                SemanticReadinessState::PolicyDisabled => {
                    RecommendedAction::SemanticDisabledByPolicy
                }
                SemanticReadinessState::FastTierReady | SemanticReadinessState::HybridReady => {
                    RecommendedAction::NothingRequired
                }
            },
        }
    }

    /// Whether ordinary search queries can run at all. Collapses the
    /// two lexical-axis failure modes into a single predicate for
    /// callers that only care about availability.
    pub(crate) fn is_searchable(&self) -> bool {
        self.lexical.is_searchable()
    }
}

// ---------------------------------------------------------------------------
// Canonical derived-asset truth table (bead
// cass-fleet-resilience-20260608-uojcg.1.1)
// ---------------------------------------------------------------------------
//
// The `ReadinessSnapshot` above is the lexical-vs-semantic *search* axis.
// The fleet/session analysis from 2026-06-08 showed agents reconciling a
// much wider field set by hand across `health` / `status` / `doctor` /
// `search --robot-meta`: canonical DB open result, configured-source
// coverage, scan watermark, last completed projection, lexical asset
// metadata (presence/hash/fingerprint/freshness), semantic tier state,
// quarantine exclusions, source-path availability, active rebuild/repair
// state, archive risk, and the single safe next command.
//
// `DerivedAssetTruthTable` is the one shared shape every readiness surface
// projects into its JSON. It *composes* `ReadinessSnapshot` for the
// search axis rather than re-deriving lexical/semantic states, and derives
// a single `SafeNextCommand` so CLI, TUI, fleet doctor, and robot payloads
// agree on the recommended next move. The derivation is conservative and
// archive-first: non-destructive inspection is always safe to surface, and
// `ArchiveRiskLevel::High` forces a backup-first recommendation ahead of
// any mutating repair (wires into bead .1.3).
//
// All enum values serialize as snake_case, matching the existing readiness
// vocabulary and the `fallback_mode` / `recommended_action` robot fields.

/// Result of opening the canonical SQLite database — the source of truth
/// from which every derived asset is projected. Distinguishes "no DB yet"
/// (fresh install, safe to build) from "DB present but unusable" (needs
/// inspection, never blind rebuild) from "host unreachable" (nothing local
/// to do), all of which collapse into a generic "unhealthy" today.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CanonicalDbAvailability {
    /// The canonical DB opened and passed a basic integrity/schema probe.
    Available,
    /// No canonical DB file exists yet — a fresh install. Building the
    /// initial index is the safe first step (no data to lose).
    Missing,
    /// The file exists but could not be opened (lock contention, EACCES,
    /// truncated header). Distinct from `Corrupt`: the bytes may be fine,
    /// the process just could not acquire/read them.
    OpenFailed,
    /// The DB opened but failed integrity/schema validation. Inspection is
    /// required before any repair; never blind-rebuild over it.
    Corrupt,
    /// The host or source backing the DB is unreachable (a fleet probe
    /// timed out / refused). No remaining fields are trustworthy.
    Unreachable,
}

impl CanonicalDbAvailability {
    /// Whether derived assets can be projected from the DB at all.
    pub(crate) fn is_projectable(self) -> bool {
        matches!(self, Self::Available)
    }
}

/// Coverage of the configured source paths relative to what the canonical
/// DB has ingested. Separates "no sources configured" (operator setup gap)
/// from "configured but unreachable" (transient/host problem) from
/// "partial" (index reflects only the reachable subset).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SourceCoverageState {
    /// Every configured source path is present and reachable.
    Complete,
    /// Some configured paths are missing/unreachable; the index reflects
    /// only the reachable subset.
    Partial,
    /// Configured paths exist but none are currently reachable.
    Unavailable,
    /// No source paths are configured at all.
    Unconfigured,
    /// Coverage could not be evaluated (e.g. unreachable host).
    Unknown,
}

/// What maintenance, if any, is actively mutating derived assets. Callers
/// use this to attach/wait instead of starting a duplicate job (the
/// single-flight contract from `asset_state`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MaintenanceActivity {
    /// No maintenance running.
    Idle,
    /// A lexical index rebuild is in flight.
    LexicalRebuild,
    /// Semantic model acquisition / embedding backfill is in flight.
    SemanticBackfill,
    /// A storage-integrity repair is in flight.
    StorageRepair,
    /// Activity could not be evaluated.
    Unknown,
}

/// Risk that a repair/rebuild touching the canonical archive could lose
/// data. `High` means the archive is effectively the only copy (or a
/// destructive path would touch source data); it forces backup-first
/// behavior before any mutating repair (bead .1.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ArchiveRiskLevel {
    /// Derived assets are fully reproducible from a safely-backed source.
    None,
    /// Reproducible, but with some manual effort / cost.
    Low,
    /// Loss would be expensive to recover; prefer backup-first.
    Elevated,
    /// The archive is the only copy or repair would touch source data;
    /// backup-first is mandatory before any mutating repair.
    High,
    /// Risk could not be evaluated.
    Unknown,
}

/// Compatibility of the running binary with the on-disk derived assets and
/// the fleet baseline. Version skew across the fleet manifests here: an
/// `Outdated` binary should be upgraded before its derived assets are
/// trusted or rebuilt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum BinaryCompatibility {
    /// Running binary matches the expected asset schema / fleet baseline.
    Current,
    /// Running binary is older than the fleet baseline; upgrade before
    /// relying on or rebuilding derived assets.
    Outdated,
    /// Running binary is newer than the on-disk assets, which therefore
    /// need a rebuild to match the current schema.
    Ahead,
    /// Compatibility could not be evaluated.
    Unknown,
}

/// Byte-level lexical asset metadata, independent of the freshness *state*
/// (which lives in `ReadinessSnapshot::lexical`). Carries the presence,
/// schema hash, storage fingerprint, and build timestamp that downstream
/// surfaces compare to detect drift.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub(crate) struct LexicalMetadata {
    /// Whether a lexical index directory + metadata sidecar exist on disk.
    pub present: bool,
    /// Schema hash baked into the index (matches `tantivy::SCHEMA_HASH`
    /// when current). `None` when absent or unreadable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_hash: Option<String>,
    /// Storage fingerprint tying the index to the canonical DB generation
    /// it was built from. `None` when absent or unreadable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_fingerprint: Option<String>,
    /// Wall-clock ms when the index was last published. `None` when
    /// unknown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub built_at_ms: Option<i64>,
}

/// Summary of assets excluded from search because they were quarantined.
/// Advisory: quarantine never makes search incorrect, it only excludes
/// the failing artifact. Causes are stable snake_case codes grouped for
/// status surfaces.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub(crate) struct QuarantineSummary {
    /// Number of quarantined artifacts currently excluded.
    pub quarantined_count: usize,
    /// Total on-disk bytes held by quarantined artifacts.
    pub total_quarantined_bytes: u64,
    /// Distinct quarantine causes (snake_case codes), e.g.
    /// `schema_drift`, `validation_failed`, `ingest_oom`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub causes: Vec<String>,
}

impl QuarantineSummary {
    /// Whether any artifact is currently quarantined.
    pub(crate) fn has_exclusions(&self) -> bool {
        self.quarantined_count > 0
    }
}

/// Stable, kebab-free identifier for the single safe next action. Kept as
/// an enum (snake_case wire form) so every consumer pattern-matches the
/// same vocabulary instead of string-sniffing prose.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SafeNextAction {
    /// All derived assets converged; nothing to do.
    None,
    /// Fresh install with no canonical DB; build the initial index.
    IndexFull,
    /// Archive risk is high; back up before any mutating repair.
    BackupThenRepair,
    /// Canonical DB is unusable; inspect (read-only) before repair.
    InspectCanonicalDb,
    /// Lexical index is missing/quarantined; rebuild from the DB.
    RepairLexical,
    /// Lexical index is stale; refresh to pick up recent ingests.
    RefreshLexical,
    /// A rebuild/repair is already running; attach or wait.
    WaitForMaintenance,
    /// Semantic backfill is running; hybrid refinement will catch up.
    WaitForSemantic,
    /// No sources configured; run setup.
    ConfigureSources,
    /// Configured sources unreachable; reconnect before sync.
    ReconnectSource,
    /// Quarantined artifacts present; inspect (advisory).
    InspectQuarantine,
    /// Semantic tier absent; install the model for hybrid refinement.
    InstallSemanticModel,
    /// Binary is older than the fleet baseline; upgrade first.
    UpgradeBinary,
    /// Binary is newer than derived assets; rebuild assets for this schema.
    RebuildForCurrentBinary,
    /// Host unreachable; nothing local to do.
    HostUnreachable,
}

impl SafeNextAction {
    /// Whether following this action would *mutate* derived assets or the
    /// archive. Inspection / wait / setup actions are non-mutating and are
    /// always safe to surface even under high archive risk.
    pub(crate) fn is_mutating(self) -> bool {
        matches!(
            self,
            Self::IndexFull
                | Self::RepairLexical
                | Self::RefreshLexical
                | Self::RebuildForCurrentBinary
        )
    }
}

/// The single safe next command derived from a `DerivedAssetTruthTable`.
/// Pairs the machine-matchable `action` with a copy-pasteable `command`
/// (absent for pure wait/none) and a one-line human rationale.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SafeNextCommand {
    pub action: SafeNextAction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    pub reason: String,
}

impl SafeNextCommand {
    fn new(action: SafeNextAction, command: Option<&str>, reason: &str) -> Self {
        Self {
            action,
            command: command.map(str::to_string),
            reason: reason.to_string(),
        }
    }
}

/// The archive-risk safety envelope every readiness surface (status,
/// triage, doctor, index preflight) projects so that high archive risk
/// forces backup-first guidance and marks coverage-reducing commands
/// *unsafe until a backup or fingerprinted plan exists*, rather than
/// emitting casual rebuild/repair advice (bead
/// cass-fleet-resilience-20260608-uojcg.1.3).
///
/// This is the single shared derivation; the four surfaces serialize it
/// into their JSON verbatim instead of each re-deciding when a rebuild is
/// safe. It is intentionally conservative: only a *mutating* action (one
/// that could reduce archive coverage) is ever gated, so a stale
/// low-risk index still gets normal `RefreshLexical` guidance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ArchiveSafetyEnvelope {
    /// The archive-loss risk level driving the envelope.
    pub archive_risk: ArchiveRiskLevel,
    /// Whether a backup (or explicit fingerprinted plan) is recommended
    /// before any mutating repair. True for `Elevated` and `High`.
    pub backup_recommended: bool,
    /// Actions that are unsafe to run until a backup / fingerprinted plan
    /// exists. Non-empty only under `High` risk; the hard gate that keeps
    /// archive-only nodes from being blindly rebuilt. Sorted + de-duped
    /// for deterministic JSON.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unsafe_until_backup: Vec<SafeNextAction>,
    /// The canonical data dir whose archive is at risk, when known. `None`
    /// for unreachable hosts (nothing local is trustworthy).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_dir: Option<String>,
    /// One-line human-facing rationale.
    pub reason: String,
}

impl ArchiveSafetyEnvelope {
    /// Whether `action` is currently gated as unsafe-until-backup.
    pub(crate) fn is_gated(&self, action: SafeNextAction) -> bool {
        self.unsafe_until_backup.contains(&action)
    }
}

/// The canonical per-node derived-asset truth table. Every readiness
/// surface (health, status, doctor, fleet doctor, search `--robot-meta`)
/// projects this one shape so the stale-but-searchable, missing,
/// unreachable, and high-archive-risk states never collapse into a single
/// "unhealthy" bit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DerivedAssetTruthTable {
    /// Canonical DB open result — the root of every derived asset.
    pub db: CanonicalDbAvailability,
    /// Coverage of configured source paths.
    pub source_coverage: SourceCoverageState,
    /// High-water timestamp (ms) up to which sources have been scanned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scan_watermark_ms: Option<i64>,
    /// Wall-clock ms of the last completed projection of derived assets
    /// from the canonical DB.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_projection_ms: Option<i64>,
    /// Byte-level lexical asset metadata.
    pub lexical_metadata: LexicalMetadata,
    /// Lexical + semantic readiness states and last search refinement.
    pub readiness: ReadinessSnapshot,
    /// Quarantine exclusions (advisory).
    pub quarantine: QuarantineSummary,
    /// Active rebuild/repair state.
    pub maintenance: MaintenanceActivity,
    /// Archive-loss risk for any mutating repair.
    pub archive_risk: ArchiveRiskLevel,
    /// Running-binary compatibility with assets / fleet baseline.
    pub binary: BinaryCompatibility,
}

impl DerivedAssetTruthTable {
    /// Whether ordinary search queries can run against this node, honoring
    /// both the DB availability and the lexical readiness axis.
    pub(crate) fn is_searchable(&self) -> bool {
        self.db.is_projectable() && self.readiness.is_searchable()
    }

    /// Derive the single safe next command. Priority is archive-first and
    /// conservative: unreachable hosts and unusable DBs are surfaced with
    /// non-destructive guidance; `ArchiveRiskLevel::High` forces a
    /// backup-first recommendation ahead of any mutating repair; active
    /// maintenance is waited on rather than duplicated; the lexical axis
    /// dominates the semantic axis; quarantine is advisory-only.
    pub(crate) fn safe_next_command(&self) -> SafeNextCommand {
        use SafeNextAction as A;

        // 0. Unreachable host — nothing local is trustworthy.
        if self.db == CanonicalDbAvailability::Unreachable {
            return SafeNextCommand::new(
                A::HostUnreachable,
                None,
                "host unreachable; retry from a reachable fleet node before acting",
            );
        }

        // 1. Unusable-but-present DB — inspect (read-only) before any
        //    repair, even under high archive risk (inspection is safe).
        match self.db {
            CanonicalDbAvailability::Corrupt => {
                return SafeNextCommand::new(
                    A::InspectCanonicalDb,
                    Some("cass diag --json --quarantine"),
                    "canonical database failed integrity validation; inspect before any repair",
                );
            }
            CanonicalDbAvailability::OpenFailed => {
                return SafeNextCommand::new(
                    A::InspectCanonicalDb,
                    Some("cass health --json"),
                    "canonical database could not be opened (lock/permission); inspect before repair",
                );
            }
            _ => {}
        }

        // 2. High archive risk dominates every *mutating* repair below.
        if self.archive_risk == ArchiveRiskLevel::High {
            return SafeNextCommand::new(
                A::BackupThenRepair,
                Some("cass doctor --json   # review the backup-first plan before --fix"),
                "high archive risk: back up the canonical archive before any repair",
            );
        }

        // 3. Fresh install — no DB yet, safe to build.
        if self.db == CanonicalDbAvailability::Missing {
            return SafeNextCommand::new(
                A::IndexFull,
                Some("cass index --full"),
                "no canonical database yet; build the initial index",
            );
        }

        // 4. Active maintenance — attach/wait, never start a duplicate.
        match self.maintenance {
            MaintenanceActivity::LexicalRebuild | MaintenanceActivity::StorageRepair => {
                return SafeNextCommand::new(
                    A::WaitForMaintenance,
                    None,
                    "a rebuild/repair is already running; wait or attach instead of starting another",
                );
            }
            // SemanticBackfill does not block lexical guidance; handled at
            // the semantic axis below.
            _ => {}
        }

        // 5. Source coverage gaps.
        match self.source_coverage {
            SourceCoverageState::Unconfigured => {
                return SafeNextCommand::new(
                    A::ConfigureSources,
                    Some("cass sources add <name> --path <dir>"),
                    "no sources configured; add a local source before indexing",
                );
            }
            SourceCoverageState::Unavailable => {
                return SafeNextCommand::new(
                    A::ReconnectSource,
                    Some("cass sources doctor --json"),
                    "configured source paths are unreachable; restore the paths or remove the entries",
                );
            }
            _ => {}
        }

        // 6. Binary skew — upgrade before trusting/rebuilding assets. No
        // cass command performs an upgrade (self-update was removed), so the
        // action carries only prose guidance.
        if self.binary == BinaryCompatibility::Outdated {
            return SafeNextCommand::new(
                A::UpgradeBinary,
                None,
                "binary is older than the baseline; reinstall from a source checkout (`cargo install --path . --locked --bin cass`) before relying on derived assets",
            );
        }
        if self.binary == BinaryCompatibility::Ahead {
            return SafeNextCommand::new(
                A::RebuildForCurrentBinary,
                Some("cass index --full"),
                "binary is newer than the on-disk assets; rebuild derived assets for the current schema",
            );
        }

        // 7. Lexical axis (mutating repairs gated by archive risk above).
        match self.readiness.lexical {
            LexicalReadinessState::Missing | LexicalReadinessState::CorruptQuarantined => {
                return SafeNextCommand::new(
                    A::RepairLexical,
                    Some("cass index --full"),
                    "lexical index unavailable; rebuild it from the canonical database",
                );
            }
            LexicalReadinessState::Repairing => {
                return SafeNextCommand::new(
                    A::WaitForMaintenance,
                    None,
                    "lexical repair already in progress; wait or attach",
                );
            }
            LexicalReadinessState::StaleButSearchable => {
                return SafeNextCommand::new(
                    A::RefreshLexical,
                    Some("cass index"),
                    "lexical index is stale; refresh to pick up recent ingests (search still works)",
                );
            }
            LexicalReadinessState::Ready => {}
        }

        // 8. Semantic axis — opportunistic, never blocks lexical search.
        match self.readiness.semantic {
            SemanticReadinessState::Absent => {
                return SafeNextCommand::new(
                    A::InstallSemanticModel,
                    Some("cass models install --from-file <DIR>"),
                    "semantic tier absent; install the model for hybrid refinement (lexical works now)",
                );
            }
            SemanticReadinessState::Backfilling => {
                return SafeNextCommand::new(
                    A::WaitForSemantic,
                    None,
                    "semantic backfill in progress; hybrid refinement will catch up on its own",
                );
            }
            SemanticReadinessState::FastTierReady
            | SemanticReadinessState::HybridReady
            | SemanticReadinessState::PolicyDisabled => {}
        }

        // 9. Quarantine is advisory — surfaced only when otherwise healthy.
        if self.quarantine.has_exclusions() {
            return SafeNextCommand::new(
                A::InspectQuarantine,
                Some("cass diag --json --quarantine"),
                "quarantined artifacts present (advisory; search results are unaffected)",
            );
        }

        // 10. Fully converged.
        SafeNextCommand::new(
            A::None,
            None,
            "all derived assets converged; no action required",
        )
    }

    /// Derive the archive-risk safety envelope for this node. `data_dir` is
    /// the canonical data directory whose archive is at risk (surfaced so
    /// operators know exactly what to back up); pass `None` when it is not
    /// known (e.g. an unreachable host).
    ///
    /// Gating rules (archive-first, conservative):
    /// - `High` → backup is mandatory; every *mutating* next-action that
    ///   could reduce archive coverage is marked unsafe-until-backup, so the
    ///   surface cannot emit a casual rebuild/repair command.
    /// - `Elevated` → backup recommended, but actions are not hard-gated
    ///   (advisory): recovery is expensive, not irreplaceable.
    /// - `None` / `Low` / `Unknown` → no backup gate; normal guidance
    ///   (e.g. a stale low-risk index still gets `RefreshLexical`).
    pub(crate) fn archive_safety_envelope(&self, data_dir: Option<&str>) -> ArchiveSafetyEnvelope {
        // The mutating actions that could reduce archive coverage; gated as
        // a unit so no single rebuild/repair path slips through under High
        // risk. Derived from `SafeNextAction::is_mutating` to stay in sync
        // with the safe-next-command vocabulary.
        const GATED: &[SafeNextAction] = &[
            SafeNextAction::IndexFull,
            SafeNextAction::RepairLexical,
            SafeNextAction::RefreshLexical,
            SafeNextAction::RebuildForCurrentBinary,
        ];
        debug_assert!(
            GATED.iter().all(|a| a.is_mutating()),
            "every gated action must be mutating"
        );

        // Unreachable hosts: nothing local is trustworthy, so surface the
        // risk verbatim with no data_dir and no actionable gate.
        if self.db == CanonicalDbAvailability::Unreachable {
            return ArchiveSafetyEnvelope {
                archive_risk: self.archive_risk,
                backup_recommended: false,
                unsafe_until_backup: Vec::new(),
                data_dir: None,
                reason: "host unreachable; archive risk cannot be assessed locally".to_string(),
            };
        }

        let data_dir = data_dir.map(str::to_string);
        match self.archive_risk {
            ArchiveRiskLevel::High => ArchiveSafetyEnvelope {
                archive_risk: ArchiveRiskLevel::High,
                backup_recommended: true,
                unsafe_until_backup: GATED.to_vec(),
                data_dir,
                reason:
                    "high archive risk: back up the canonical archive (or produce a fingerprinted \
                     recovery plan) before any rebuild or repair"
                        .to_string(),
            },
            ArchiveRiskLevel::Elevated => ArchiveSafetyEnvelope {
                archive_risk: ArchiveRiskLevel::Elevated,
                backup_recommended: true,
                unsafe_until_backup: Vec::new(),
                data_dir,
                reason: "elevated archive risk: a backup is recommended before any repair"
                    .to_string(),
            },
            ArchiveRiskLevel::None | ArchiveRiskLevel::Low => ArchiveSafetyEnvelope {
                archive_risk: self.archive_risk,
                backup_recommended: false,
                unsafe_until_backup: Vec::new(),
                data_dir,
                reason: "archive risk is low; derived assets are reproducible from a backed source"
                    .to_string(),
            },
            ArchiveRiskLevel::Unknown => ArchiveSafetyEnvelope {
                archive_risk: ArchiveRiskLevel::Unknown,
                backup_recommended: false,
                unsafe_until_backup: Vec::new(),
                data_dir,
                reason: "archive risk could not be evaluated".to_string(),
            },
        }
    }
}

/// Bridge the live `state_meta_json` snapshot into the canonical typed
/// [`DerivedAssetTruthTable`] (bead
/// `cass-fleet-resilience-20260608-uojcg.envu1`).
///
/// Today the typed truth table is only built in fixtures; every live
/// readiness surface (`run_status` / `run_doctor` / `run_health`) instead
/// works off a parallel `serde_json::Value` snapshot produced by
/// `state_meta_json_inner`. This function maps that same snapshot — the one
/// the surfaces already compute from the canonical data dir — into the typed
/// table, so the typed projections (`readiness_projection::project`,
/// [`DerivedAssetTruthTable::next_command_envelope`],
/// [`DerivedAssetTruthTable::safe_next_command`]) can be reused across
/// surfaces (recovery support bundle, human readiness summary) instead of
/// re-deriving the JSON ad hoc.
///
/// The lexical/semantic [`ReadinessSnapshot`] is computed by the caller
/// (`readiness_snapshot_from_state`, which already owns that derivation) and
/// passed in, so this bridge does not duplicate the search-axis logic.
///
/// Fields the local single-node snapshot does not carry are reported
/// truthfully as `Unknown` / `None` rather than guessed:
/// - `source_coverage` is `Unknown` (source-path coverage is the
///   sources-doctor surface's concern, not part of this readiness snapshot).
/// - `archive_risk` is `Unknown` (archive-loss evaluation is a fleet/source
///   concern, not derivable from the local readiness probe; reporting Unknown
///   makes the archive-safety envelope say "could not be evaluated" rather
///   than gating or green-lighting a mutating repair on a guess).
/// - `binary` is `Current` unless the on-disk lexical checkpoint reports a
///   schema mismatch, in which case the running binary is `Ahead` of the
///   assets (rebuild required to match the current schema). Fleet version
///   skew (`Outdated`) requires a fleet baseline and is not derived here.
pub(crate) fn truth_table_from_state(
    state: &serde_json::Value,
    readiness: ReadinessSnapshot,
) -> DerivedAssetTruthTable {
    use serde_json::Value;

    let as_bool = |ptr: &str| state.pointer(ptr).and_then(Value::as_bool);
    let as_str = |ptr: &str| {
        state
            .pointer(ptr)
            .and_then(Value::as_str)
            .map(str::to_string)
    };

    // --- Canonical DB availability ---------------------------------------
    let db_exists = as_bool("/database/exists").unwrap_or(false);
    let db_opened = as_bool("/database/opened").unwrap_or(false);
    let db_open_skipped = as_bool("/database/open_skipped").unwrap_or(false);
    let db = if !db_exists {
        // No canonical DB file yet — a fresh install, safe to build.
        CanonicalDbAvailability::Missing
    } else if db_opened || db_open_skipped {
        // Opened for real, or the open was deliberately elided (health's fast
        // path) on a present file: treat the present DB as available.
        CanonicalDbAvailability::Available
    } else {
        // File present but the (attempted) open failed. The snapshot cannot
        // distinguish lock/permission from header corruption (open_retryable
        // conflates them), so report the conservative "could not open" —
        // inspect before any repair, never blind-rebuild.
        CanonicalDbAvailability::OpenFailed
    };

    // --- Lexical asset metadata ------------------------------------------
    let lexical_metadata = LexicalMetadata {
        present: as_bool("/index/exists").unwrap_or(false),
        // The state snapshot exposes fingerprints, not the bare schema hash.
        schema_hash: None,
        storage_fingerprint: as_str("/index/fingerprint/checkpoint_fingerprint"),
        built_at_ms: as_str("/index/last_indexed_at").and_then(|ts| parse_rfc3339_millis(&ts)),
    };
    let last_projection_ms = lexical_metadata.built_at_ms;

    // --- Quarantine (advisory) -------------------------------------------
    let quarantined_count = state
        .pointer("/ingest_quarantine/quarantined_conversations")
        .and_then(Value::as_u64)
        .map(|count| count as usize)
        .unwrap_or(0);
    let quarantine = QuarantineSummary {
        quarantined_count,
        // Byte accounting lives in `cass diag --quarantine`, not the
        // readiness snapshot; report 0 rather than guessing.
        total_quarantined_bytes: 0,
        causes: Vec::new(),
    };

    // --- Active maintenance ----------------------------------------------
    let maintenance = if as_bool("/rebuild/active").unwrap_or(false) {
        MaintenanceActivity::LexicalRebuild
    } else if as_bool("/semantic/checkpoint/active").unwrap_or(false) {
        MaintenanceActivity::SemanticBackfill
    } else {
        MaintenanceActivity::Idle
    };

    // --- Binary compatibility --------------------------------------------
    // A present lexical checkpoint whose schema does not match the running
    // binary means the binary is *ahead* of the on-disk assets (rebuild to
    // match the current schema). Otherwise the running binary is the
    // reference for this local node (fleet `Outdated` skew needs a baseline
    // we do not have here).
    let binary = if matches!(as_bool("/index/checkpoint/schema_matches"), Some(false)) {
        BinaryCompatibility::Ahead
    } else {
        BinaryCompatibility::Current
    };

    DerivedAssetTruthTable {
        db,
        source_coverage: SourceCoverageState::Unknown,
        // The snapshot does not expose a separate scan watermark; the last
        // completed projection is the index's last_indexed_at.
        scan_watermark_ms: None,
        last_projection_ms,
        lexical_metadata,
        readiness,
        quarantine,
        maintenance,
        archive_risk: ArchiveRiskLevel::Unknown,
        binary,
    }
}

/// Parse an RFC-3339 timestamp (as serialized by the state snapshot) into
/// wall-clock milliseconds. Returns `None` for absent/malformed values.
fn parse_rfc3339_millis(ts: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(ts)
        .ok()
        .map(|dt| dt.timestamp_millis())
}

/// The seven canonical readiness states from the 2026-06-08 fleet report,
/// exposed as fixtures so downstream beads (.1.5 readiness fixture matrix,
/// .12.1 test matrix) consume one source of truth instead of hand-rolling
/// node states. The fleet-doctor surface that prompted the names is removed;
/// the states themselves are reused for local archive readiness. Returned as
/// `(name, table)` pairs in a stable order. Timestamps are fixed literals
/// for determinism.
pub(crate) fn fleet_fixtures() -> Vec<(&'static str, DerivedAssetTruthTable)> {
    // A fixed reference clock (ms) so fixtures are byte-deterministic.
    const NOW_MS: i64 = 1_749_350_000_000; // ~2026-06-08T00:00:00Z
    const HOUR_MS: i64 = 3_600_000;
    const DAY_MS: i64 = 86_400_000;

    vec![
        // local: index is stale but searchable, with quarantined assets.
        (
            "local_stale_quarantine",
            DerivedAssetTruthTable {
                db: CanonicalDbAvailability::Available,
                source_coverage: SourceCoverageState::Complete,
                scan_watermark_ms: Some(NOW_MS - HOUR_MS),
                last_projection_ms: Some(NOW_MS - 6 * HOUR_MS),
                lexical_metadata: LexicalMetadata {
                    present: true,
                    schema_hash: Some("schema-current".to_string()),
                    storage_fingerprint: Some("fp-old-gen".to_string()),
                    built_at_ms: Some(NOW_MS - 6 * HOUR_MS),
                },
                readiness: ReadinessSnapshot::new(
                    LexicalReadinessState::StaleButSearchable,
                    // Semantic assets exist but the planner fell back to
                    // lexical-only refinement for the last query (the report's
                    // "lexical-only semantic fallback").
                    SemanticReadinessState::HybridReady,
                )
                .with_last_search_refinement(SearchRefinementLevel::LexicalOnly),
                quarantine: QuarantineSummary {
                    // The report's local 0.6.13 node held 133 ingest-OOM
                    // quarantines (issue #258 class).
                    quarantined_count: 133,
                    total_quarantined_bytes: 139_460_608,
                    causes: vec!["ingest_oom".to_string()],
                },
                maintenance: MaintenanceActivity::Idle,
                archive_risk: ArchiveRiskLevel::Low,
                binary: BinaryCompatibility::Current,
            },
        ),
        // ts1: high archive risk — the canonical archive is the only copy.
        (
            "ts1_high_archive_risk",
            DerivedAssetTruthTable {
                db: CanonicalDbAvailability::Available,
                source_coverage: SourceCoverageState::Complete,
                scan_watermark_ms: Some(NOW_MS - 2 * HOUR_MS),
                last_projection_ms: Some(NOW_MS - 2 * HOUR_MS),
                lexical_metadata: LexicalMetadata {
                    present: true,
                    schema_hash: Some("schema-current".to_string()),
                    storage_fingerprint: Some("fp-current".to_string()),
                    built_at_ms: Some(NOW_MS - 2 * HOUR_MS),
                },
                readiness: ReadinessSnapshot::new(
                    LexicalReadinessState::Ready,
                    SemanticReadinessState::HybridReady,
                ),
                quarantine: QuarantineSummary::default(),
                maintenance: MaintenanceActivity::Idle,
                archive_risk: ArchiveRiskLevel::High,
                binary: BinaryCompatibility::Current,
            },
        ),
        // ts2: fast health, slow status — semantic backfill in flight so the
        // heavier status probe is slow while health stays fast/cached.
        (
            "ts2_fast_health_slow_status",
            DerivedAssetTruthTable {
                db: CanonicalDbAvailability::Available,
                source_coverage: SourceCoverageState::Complete,
                scan_watermark_ms: Some(NOW_MS - 30 * 60_000),
                last_projection_ms: Some(NOW_MS - HOUR_MS),
                lexical_metadata: LexicalMetadata {
                    present: true,
                    schema_hash: Some("schema-current".to_string()),
                    storage_fingerprint: Some("fp-current".to_string()),
                    built_at_ms: Some(NOW_MS - HOUR_MS),
                },
                readiness: ReadinessSnapshot::new(
                    LexicalReadinessState::Ready,
                    SemanticReadinessState::Backfilling,
                ),
                quarantine: QuarantineSummary::default(),
                maintenance: MaintenanceActivity::SemanticBackfill,
                archive_risk: ArchiveRiskLevel::Low,
                binary: BinaryCompatibility::Current,
            },
        ),
        // csd: lexical metadata absent entirely — index never built / lost.
        (
            "csd_missing_lexical_metadata",
            DerivedAssetTruthTable {
                db: CanonicalDbAvailability::Available,
                source_coverage: SourceCoverageState::Complete,
                scan_watermark_ms: Some(NOW_MS - DAY_MS),
                last_projection_ms: None,
                lexical_metadata: LexicalMetadata {
                    present: false,
                    schema_hash: None,
                    storage_fingerprint: None,
                    built_at_ms: None,
                },
                readiness: ReadinessSnapshot::new(
                    LexicalReadinessState::Missing,
                    SemanticReadinessState::Absent,
                ),
                quarantine: QuarantineSummary::default(),
                maintenance: MaintenanceActivity::Idle,
                archive_risk: ArchiveRiskLevel::None,
                binary: BinaryCompatibility::Current,
            },
        ),
        // css: a stale existing index against reachable sources.
        (
            "css_stale_existing_index",
            DerivedAssetTruthTable {
                db: CanonicalDbAvailability::Available,
                source_coverage: SourceCoverageState::Complete,
                scan_watermark_ms: Some(NOW_MS - 12 * HOUR_MS),
                last_projection_ms: Some(NOW_MS - 5 * DAY_MS),
                lexical_metadata: LexicalMetadata {
                    present: true,
                    schema_hash: Some("schema-current".to_string()),
                    storage_fingerprint: Some("fp-stale-gen".to_string()),
                    built_at_ms: Some(NOW_MS - 5 * DAY_MS),
                },
                readiness: ReadinessSnapshot::new(
                    LexicalReadinessState::StaleButSearchable,
                    SemanticReadinessState::HybridReady,
                ),
                quarantine: QuarantineSummary::default(),
                maintenance: MaintenanceActivity::Idle,
                archive_risk: ArchiveRiskLevel::Low,
                binary: BinaryCompatibility::Current,
            },
        ),
        // mac-mini-max: a stale old binary whose assets predate the current
        // schema; upgrade the binary before trusting/rebuilding assets.
        (
            "mac_mini_max_stale_old_binary",
            DerivedAssetTruthTable {
                db: CanonicalDbAvailability::Available,
                source_coverage: SourceCoverageState::Complete,
                scan_watermark_ms: Some(NOW_MS - 3 * DAY_MS),
                last_projection_ms: Some(NOW_MS - 3 * DAY_MS),
                lexical_metadata: LexicalMetadata {
                    present: true,
                    schema_hash: Some("schema-legacy".to_string()),
                    storage_fingerprint: Some("fp-legacy-gen".to_string()),
                    built_at_ms: Some(NOW_MS - 3 * DAY_MS),
                },
                readiness: ReadinessSnapshot::new(
                    LexicalReadinessState::StaleButSearchable,
                    SemanticReadinessState::FastTierReady,
                ),
                quarantine: QuarantineSummary::default(),
                maintenance: MaintenanceActivity::Idle,
                archive_risk: ArchiveRiskLevel::Low,
                binary: BinaryCompatibility::Outdated,
            },
        ),
        // mac-mini-old: unreachable via the fleet probe entirely.
        (
            "mac_mini_old_unreachable",
            DerivedAssetTruthTable {
                db: CanonicalDbAvailability::Unreachable,
                source_coverage: SourceCoverageState::Unknown,
                scan_watermark_ms: None,
                last_projection_ms: None,
                lexical_metadata: LexicalMetadata::default(),
                readiness: ReadinessSnapshot::new(
                    LexicalReadinessState::Missing,
                    SemanticReadinessState::Absent,
                ),
                quarantine: QuarantineSummary::default(),
                maintenance: MaintenanceActivity::Unknown,
                archive_risk: ArchiveRiskLevel::Unknown,
                binary: BinaryCompatibility::Unknown,
            },
        ),
    ]
}

#[cfg(test)]
mod tests;
