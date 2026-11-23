//! Phase-exact stale-refresh evidence ledger (bead ibuuh.25).
//!
//! Defines the canonical stale-refresh phase model and captures machine-readable
//! timings, counters, and correctness artifacts for each phase.  Downstream
//! performance beads use this ledger as their proof framework: "what changed,
//! how much, and was correctness preserved?"
//!
//! # Phase model
//!
//! ```text
//! ┌─────────┐   ┌─────────┐   ┌──────────┐   ┌─────────┐   ┌──────────┐   ┌──────────┐
//! │  Scan   │──▶│ Persist │──▶│ Lexical  │──▶│ Publish │──▶│ Analytics│──▶│ Semantic │
//! │ (disc.) │   │ (DB)    │   │ (rebuild)│   │ (commit)│   │ (stats)  │   │ (vectors)│
//! └─────────┘   └─────────┘   └──────────┘   └─────────┘   └──────────┘   └──────────┘
//!                                                               │
//!                                                               ▼
//!                                                          ┌──────────┐
//!                                                          │ Recovery │
//!                                                          │ (error)  │
//!                                                          └──────────┘
//! ```

use std::collections::BTreeMap;
use std::time::Instant;

use serde::{Deserialize, Serialize};

// ─── Phase model ───────────────────────────────────────────────────────────

/// Canonical phases of a stale-refresh cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefreshPhase {
    /// Discovery: scan filesystem for agent sessions.
    Scan,
    /// Persist new/updated conversations to the canonical SQLite DB.
    Persist,
    /// Rebuild the lexical (Tantivy/frankensearch) index from DB content.
    LexicalRebuild,
    /// Commit and publish the lexical index atomically.
    Publish,
    /// Record analytics (stats, aggregates, token usage).
    Analytics,
    /// Build/update semantic vector indices (fast + quality tiers).
    Semantic,
    /// Error recovery (rollback, checkpoint save, cleanup).
    Recovery,
}

impl RefreshPhase {
    /// All phases in pipeline order.
    pub const ALL: &'static [RefreshPhase] = &[
        Self::Scan,
        Self::Persist,
        Self::LexicalRebuild,
        Self::Publish,
        Self::Analytics,
        Self::Semantic,
        Self::Recovery,
    ];

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Scan => "scan",
            Self::Persist => "persist",
            Self::LexicalRebuild => "lexical_rebuild",
            Self::Publish => "publish",
            Self::Analytics => "analytics",
            Self::Semantic => "semantic",
            Self::Recovery => "recovery",
        }
    }
}

// ─── Phase record ──────────────────────────────────────────────────────────

/// Timing and counter data for a single phase.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhaseRecord {
    pub phase: RefreshPhase,
    /// Wall-clock duration in milliseconds.
    pub duration_ms: u64,
    /// Items processed (conversations, documents, vectors, etc.).
    pub items_processed: u64,
    /// Items skipped (already indexed, filtered, etc.).
    pub items_skipped: u64,
    /// Errors encountered (non-fatal).
    pub errors: u64,
    /// Phase-specific counters (e.g., "bytes_written", "connectors_scanned").
    pub counters: BTreeMap<String, u64>,
    /// Whether this phase completed successfully.
    pub success: bool,
    /// Error message if the phase failed.
    pub error_message: Option<String>,
}

impl PhaseRecord {
    fn new(phase: RefreshPhase) -> Self {
        Self {
            phase,
            duration_ms: 0,
            items_processed: 0,
            items_skipped: 0,
            errors: 0,
            counters: BTreeMap::new(),
            success: true,
            error_message: None,
        }
    }
}

// ─── Equivalence artifacts ─────────────────────────────────────────────────

/// Correctness artifacts captured after a refresh for equivalence checking.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EquivalenceArtifacts {
    /// Total conversations in DB after refresh.
    pub conversation_count: u64,
    /// Total messages in DB after refresh.
    pub message_count: u64,
    /// Total indexed documents in the lexical index.
    pub lexical_doc_count: u64,
    /// Lexical index storage fingerprint.
    pub lexical_fingerprint: Option<String>,
    /// Semantic manifest fingerprint (if semantic phase ran).
    pub semantic_manifest_fingerprint: Option<String>,
    /// Search-hit digest: sha256 of sorted doc IDs from a canonical query.
    pub search_hit_digest: Option<String>,
    /// Peak RSS in bytes during the refresh (if measured).
    pub peak_rss_bytes: Option<u64>,
    /// DB file size after refresh.
    pub db_size_bytes: Option<u64>,
    /// Lexical index size on disk.
    pub lexical_index_size_bytes: Option<u64>,
}

// ─── The evidence ledger ───────────────────────────────────────────────────

/// Complete evidence ledger for a single stale-refresh cycle.
///
/// Captures phase-exact timings, item counts, and correctness artifacts.
/// Serializable to JSON for benchmark comparison and CI artifact retention.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefreshLedger {
    /// Ledger format version.
    pub version: u32,
    /// Unix timestamp (ms) when the refresh started.
    pub started_at_ms: i64,
    /// Unix timestamp (ms) when the refresh completed.
    pub completed_at_ms: i64,
    /// Total wall-clock duration (ms).
    pub total_duration_ms: u64,
    /// Whether this was a full rebuild or incremental refresh.
    pub full_rebuild: bool,
    /// Corpus family identifier (for benchmark categorization).
    pub corpus_family: String,
    /// Per-phase records in pipeline order.
    pub phases: Vec<PhaseRecord>,
    /// Correctness artifacts captured after the refresh.
    pub equivalence: EquivalenceArtifacts,
    /// Free-form tags for filtering and grouping.
    pub tags: BTreeMap<String, String>,
}

/// User-facing readiness timing summary derived from a refresh ledger.
///
/// `time_to_lexical_ready_ms` means the lexical build phase finished
/// successfully; `time_to_search_ready_ms` means the publish phase finished
/// successfully and the refreshed lexical asset is visible to ordinary search.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefreshReadinessMilestones {
    pub time_to_lexical_ready_ms: Option<u64>,
    pub time_to_search_ready_ms: Option<u64>,
    pub time_to_full_settled_ms: Option<u64>,
    pub failed_phase: Option<String>,
    pub search_readiness_state: RefreshSearchReadinessState,
}

/// Why ordinary search can or cannot see the refreshed lexical asset yet.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefreshSearchReadinessState {
    /// The publish phase completed successfully, so refreshed lexical results
    /// are visible to search.
    Published,
    /// Earlier phases succeeded, but no publish phase has completed yet.
    #[default]
    WaitingForPublish,
    /// A phase before publish failed, so publish was never reached safely.
    BlockedBeforePublish,
    /// Publish itself failed, preserving the previous good lexical asset.
    PublishFailed,
}

impl Default for RefreshLedger {
    fn default() -> Self {
        Self {
            version: 1,
            started_at_ms: 0,
            completed_at_ms: 0,
            total_duration_ms: 0,
            full_rebuild: false,
            corpus_family: "default".to_owned(),
            phases: Vec::new(),
            equivalence: EquivalenceArtifacts::default(),
            tags: BTreeMap::new(),
        }
    }
}

impl RefreshLedger {
    /// Start a new ledger with the given corpus family.
    pub fn start(corpus_family: &str, full_rebuild: bool) -> LedgerBuilder {
        LedgerBuilder::new(corpus_family, full_rebuild)
    }

    /// Get the phase record for a specific phase (if it ran).
    pub fn phase(&self, phase: RefreshPhase) -> Option<&PhaseRecord> {
        self.phases.iter().find(|p| p.phase == phase)
    }

    /// Total items processed across all phases.
    pub fn total_items_processed(&self) -> u64 {
        self.phases
            .iter()
            .map(|p| p.items_processed)
            .fold(0u64, u64::saturating_add)
    }

    /// Total errors across all phases.
    pub fn total_errors(&self) -> u64 {
        self.phases
            .iter()
            .map(|p| p.errors)
            .fold(0u64, u64::saturating_add)
    }

    /// Whether all phases succeeded.
    pub fn all_phases_succeeded(&self) -> bool {
        self.phases.iter().all(|p| p.success)
    }

    /// Phases that failed.
    pub fn failed_phases(&self) -> Vec<&PhaseRecord> {
        self.phases.iter().filter(|p| !p.success).collect()
    }

    /// Duration breakdown: phase name → ms.
    pub fn duration_breakdown(&self) -> BTreeMap<String, u64> {
        self.phases
            .iter()
            .map(|p| (p.phase.as_str().to_owned(), p.duration_ms))
            .collect()
    }

    /// Derive the user-facing stale-refresh readiness milestones that robot
    /// surfaces and benchmark gates need to compare across runs.
    pub fn readiness_milestones(&self) -> RefreshReadinessMilestones {
        RefreshReadinessMilestones {
            time_to_lexical_ready_ms: self
                .successful_duration_through(RefreshPhase::LexicalRebuild),
            time_to_search_ready_ms: self.successful_duration_through(RefreshPhase::Publish),
            time_to_full_settled_ms: self.full_settlement_duration_ms(),
            failed_phase: self
                .failed_phases()
                .first()
                .map(|phase| phase.phase.as_str().to_owned()),
            search_readiness_state: self.search_readiness_state(),
        }
    }

    /// Serialize to pretty JSON.
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_else(|_| "{}".to_owned())
    }

    fn successful_duration_through(&self, target: RefreshPhase) -> Option<u64> {
        let mut elapsed_ms = 0u64;
        for phase in &self.phases {
            elapsed_ms = elapsed_ms.saturating_add(phase.duration_ms);
            if !phase.success {
                return None;
            }
            if phase.phase == target {
                return Some(elapsed_ms);
            }
        }
        None
    }

    fn sum_phase_durations(&self) -> u64 {
        self.phases
            .iter()
            .map(|phase| phase.duration_ms)
            .fold(0u64, u64::saturating_add)
    }

    fn full_settlement_duration_ms(&self) -> Option<u64> {
        (self.all_phases_succeeded()
            && self.search_readiness_state() == RefreshSearchReadinessState::Published)
            .then(|| {
                if self.total_duration_ms > 0 {
                    self.total_duration_ms
                } else {
                    self.sum_phase_durations()
                }
            })
    }

    fn search_readiness_state(&self) -> RefreshSearchReadinessState {
        let mut published = false;

        for phase in &self.phases {
            if !phase.success {
                return if phase.phase == RefreshPhase::Publish {
                    RefreshSearchReadinessState::PublishFailed
                } else if published {
                    RefreshSearchReadinessState::Published
                } else {
                    RefreshSearchReadinessState::BlockedBeforePublish
                };
            }
            if phase.phase == RefreshPhase::Publish {
                published = true;
            }
        }

        if published {
            RefreshSearchReadinessState::Published
        } else {
            RefreshSearchReadinessState::WaitingForPublish
        }
    }
}

// ─── Builder (ergonomic recording during refresh) ──────────────────────────

/// Builder for incrementally recording phase data during a refresh cycle.
pub struct LedgerBuilder {
    ledger: RefreshLedger,
    start_time: Instant,
    current_phase: Option<(RefreshPhase, Instant)>,
    current_record: Option<PhaseRecord>,
}

impl LedgerBuilder {
    fn new(corpus_family: &str, full_rebuild: bool) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        Self {
            ledger: RefreshLedger {
                started_at_ms: now,
                full_rebuild,
                corpus_family: corpus_family.to_owned(),
                ..Default::default()
            },
            start_time: Instant::now(),
            current_phase: None,
            current_record: None,
        }
    }

    /// Begin a new phase.  Automatically ends any in-progress phase.
    pub fn begin_phase(&mut self, phase: RefreshPhase) {
        self.end_current_phase();
        self.current_phase = Some((phase, Instant::now()));
        self.current_record = Some(PhaseRecord::new(phase));
    }

    /// Record items processed in the current phase.
    pub fn record_items(&mut self, processed: u64, skipped: u64) {
        if let Some(ref mut record) = self.current_record {
            record.items_processed = record.items_processed.saturating_add(processed);
            record.items_skipped = record.items_skipped.saturating_add(skipped);
        }
    }

    /// Record a non-fatal error in the current phase.
    ///
    /// Multiple errors are joined with "; " so no diagnostic info is lost.
    pub fn record_error(&mut self, message: &str) {
        if let Some(ref mut record) = self.current_record {
            record.errors = record.errors.saturating_add(1);
            match &mut record.error_message {
                Some(existing) => {
                    existing.push_str("; ");
                    existing.push_str(message);
                }
                None => record.error_message = Some(message.to_owned()),
            }
        }
    }

    /// Record a phase failure (the phase did not complete successfully).
    ///
    /// This replaces any previous error_message since the failure is the
    /// authoritative final state.
    pub fn record_failure(&mut self, message: &str) {
        if let Some(ref mut record) = self.current_record {
            record.success = false;
            record.errors = record.errors.saturating_add(1);
            record.error_message = Some(message.to_owned());
        }
    }

    /// Set a custom counter in the current phase.
    pub fn set_counter(&mut self, key: &str, value: u64) {
        if let Some(ref mut record) = self.current_record {
            record.counters.insert(key.to_owned(), value);
        }
    }

    /// Increment a custom counter in the current phase.
    pub fn inc_counter(&mut self, key: &str, delta: u64) {
        if let Some(ref mut record) = self.current_record {
            let entry = record.counters.entry(key.to_owned()).or_insert(0);
            *entry = entry.saturating_add(delta);
        }
    }

    /// Set equivalence artifacts.
    pub fn set_equivalence(&mut self, artifacts: EquivalenceArtifacts) {
        self.ledger.equivalence = artifacts;
    }

    /// Add a free-form tag.
    pub fn tag(&mut self, key: &str, value: &str) {
        self.ledger.tags.insert(key.to_owned(), value.to_owned());
    }

    /// Finalize the current phase and the ledger.
    pub fn finish(mut self) -> RefreshLedger {
        self.end_current_phase();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        self.ledger.completed_at_ms = now;
        self.ledger.total_duration_ms = self.start_time.elapsed().as_millis() as u64;
        self.ledger
    }

    fn end_current_phase(&mut self) {
        // Take each field separately so a .take() on one doesn't silently
        // discard the other if they're ever out of sync.
        let Some((_, phase_start)) = self.current_phase.take() else {
            return;
        };
        let Some(mut record) = self.current_record.take() else {
            return;
        };
        record.duration_ms = phase_start.elapsed().as_millis() as u64;
        self.ledger.phases.push(record);
    }
}

// ─── Benchmark corpus families ─────────────────────────────────────────────

/// Standard benchmark corpus family identifiers.
pub mod corpus_families {
    /// Small corpus: ~10 conversations, 40 messages.  Fast smoke test.
    pub const SMALL: &str = "small";
    /// Medium corpus: ~100 conversations, 500 messages.  Typical personal use.
    pub const MEDIUM: &str = "medium";
    /// Large corpus: ~1000 conversations, 5000 messages.  Power user.
    pub const LARGE: &str = "large";
    /// Duplicate-heavy: 50% duplicate messages across conversations.
    pub const DUPLICATE_HEAVY: &str = "duplicate_heavy";
    /// Pathological: very long messages, deep nesting, edge-case content.
    pub const PATHOLOGICAL: &str = "pathological";
    /// Mixed-agent: equal distribution across all 14 supported agents.
    pub const MIXED_AGENT: &str = "mixed_agent";
    /// Incremental: base corpus + small delta for incremental refresh testing.
    pub const INCREMENTAL: &str = "incremental";
}

/// Configuration for generating a benchmark corpus.
#[derive(Debug, Clone)]
pub struct BenchmarkCorpusConfig {
    pub family: String,
    pub num_conversations: usize,
    pub messages_per_conversation: usize,
    /// Fraction of messages that are duplicates (0.0–1.0).
    pub duplicate_fraction: f64,
    /// Maximum message content length in characters.
    pub max_message_length: usize,
    /// Number of distinct agents to cycle through.
    pub agent_count: usize,
}

impl BenchmarkCorpusConfig {
    pub fn small() -> Self {
        Self {
            family: corpus_families::SMALL.to_owned(),
            num_conversations: 10,
            messages_per_conversation: 4,
            duplicate_fraction: 0.0,
            max_message_length: 500,
            agent_count: 3,
        }
    }

    pub fn medium() -> Self {
        Self {
            family: corpus_families::MEDIUM.to_owned(),
            num_conversations: 100,
            messages_per_conversation: 5,
            duplicate_fraction: 0.05,
            max_message_length: 2000,
            agent_count: 5,
        }
    }

    pub fn large() -> Self {
        Self {
            family: corpus_families::LARGE.to_owned(),
            num_conversations: 1000,
            messages_per_conversation: 5,
            duplicate_fraction: 0.05,
            max_message_length: 2000,
            agent_count: 8,
        }
    }

    pub fn duplicate_heavy() -> Self {
        Self {
            family: corpus_families::DUPLICATE_HEAVY.to_owned(),
            num_conversations: 50,
            messages_per_conversation: 6,
            duplicate_fraction: 0.5,
            max_message_length: 1000,
            agent_count: 3,
        }
    }

    pub fn pathological() -> Self {
        Self {
            family: corpus_families::PATHOLOGICAL.to_owned(),
            num_conversations: 20,
            messages_per_conversation: 10,
            duplicate_fraction: 0.0,
            max_message_length: 50_000,
            agent_count: 2,
        }
    }

    pub fn mixed_agent() -> Self {
        Self {
            family: corpus_families::MIXED_AGENT.to_owned(),
            num_conversations: 70,
            messages_per_conversation: 4,
            duplicate_fraction: 0.0,
            max_message_length: 1000,
            agent_count: 14,
        }
    }

    pub fn incremental() -> Self {
        Self {
            family: corpus_families::INCREMENTAL.to_owned(),
            num_conversations: 50,
            messages_per_conversation: 4,
            duplicate_fraction: 0.0,
            max_message_length: 1000,
            agent_count: 3,
        }
    }
}

mod evidence;

pub use evidence::{
    RefreshLedgerEvidence, RefreshLedgerEvidenceComparison, RefreshPhaseDelta, RefreshPhaseShare,
    RefreshThroughputProfile, RegressionVerdict, RegressionVerdictThresholds,
};

#[cfg(test)]
mod tests;
