#[allow(unused_imports)]
use crate::franken_sync::compat::{
    ConnectionExt, OpenFlags as FrankenOpenFlags, RowExt,
    open_with_flags as open_franken_with_flags,
};
#[allow(unused_imports)]
use crate::indexer::IndexOptions;
#[allow(unused_imports)]
use crate::model::cli_error_kind::ErrorKind as CliErrorKind;
#[allow(unused_imports)]
use crate::robot_budget_envelope::BudgetSnapshot;
#[allow(unused_imports)]
use crate::{cli_app::*, cli_root::*, lib_doctor::*, *};
#[allow(unused_imports)]
use anyhow::Result;
#[allow(unused_imports)]
use base64::prelude::*;
#[allow(unused_imports)]
use chrono::Utc;
#[allow(unused_imports)]
use clap::{Arg, ArgAction, Command, CommandFactory, Parser, Subcommand, ValueEnum, ValueHint};
#[allow(unused_imports)]
use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
#[allow(unused_imports)]
use std::fs::{File, OpenOptions};
#[allow(unused_imports)]
use std::io::{self, BufRead, BufReader, IsTerminal, Read, Seek, SeekFrom, Write};
#[allow(unused_imports)]
use std::path::{Path, PathBuf};
#[allow(unused_imports)]
use std::sync::{Arc, Mutex};
#[allow(unused_imports)]
use std::time::{Duration, Instant};
#[allow(unused_imports)]
use tracing::{info, warn};
#[allow(unused_imports)]
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

/// Conversation-ingest quarantine inspection and management (#292 ask #3).
#[derive(Subcommand, Debug, Clone)]
pub enum QuarantineCommand {
    /// List quarantined conversations: id, schema version, attempt count, reason, age.
    List {
        /// Override data dir
        #[arg(long)]
        data_dir: Option<PathBuf>,

        /// Output as JSON (`--robot` also works)
        #[arg(long, visible_alias = "robot")]
        json: bool,
    },
    /// Remove quarantine entries (dry-run by default; `--apply` to clear). Optionally filter by a conversation-id substring.
    Clear {
        /// Override data dir
        #[arg(long)]
        data_dir: Option<PathBuf>,

        /// Only clear entries whose conversation_id contains this substring. Omit to target all entries.
        #[arg(long)]
        filter: Option<String>,

        /// Actually remove the matching entries. Without this, runs as a dry-run (inspect only).
        #[arg(long, default_value_t = false)]
        apply: bool,

        /// Output as JSON (`--robot` also works)
        #[arg(long, visible_alias = "robot")]
        json: bool,
    },
    /// Re-attempt retry-eligible quarantined conversations (bounded; dry-run by
    /// default). Apply reparses each exact source conversation and clears it
    /// only after persistence succeeds; irreducible same-version entries are
    /// reported but never attempted unless `--force-irreducible`.
    Retry {
        /// Override data dir
        #[arg(long)]
        data_dir: Option<PathBuf>,

        /// Cap the number of entries attempted in this pass (bounded, resumable).
        /// Omit to attempt every eligible entry. Deferred entries are reported,
        /// never dropped — re-run to resume.
        #[arg(long)]
        max_attempts: Option<usize>,

        /// Also retry irreducible same-version entries (an explicit operator
        /// override of the safe eligible-only default).
        #[arg(long, default_value_t = false)]
        force_irreducible: bool,

        /// Targeted re-ingest eligible entries now. Without this, emit a
        /// read-only retry plan.
        #[arg(long, default_value_t = false)]
        apply: bool,

        /// Output as JSON (`--robot` also works)
        #[arg(long, visible_alias = "robot")]
        json: bool,
    },
}

/// Raw-mirror maintenance commands.
#[derive(Subcommand, Debug, Clone)]
pub enum MirrorCommand {
    /// Plan or apply raw-mirror manifest/blob retention.
    Prune {
        /// Override data dir
        #[arg(long)]
        data_dir: Option<PathBuf>,

        /// Retire captures older than a duration such as 90d, 24h, or 3600s.
        #[arg(long)]
        older_than: Option<String>,

        /// Retire oldest captures until unique raw blob bytes are at or below this size.
        #[arg(long, alias = "target-size")]
        max_size: Option<String>,

        /// Limit retention to this provider slug. Repeat to select several providers.
        #[arg(long = "provider")]
        providers: Vec<String>,

        /// Limit retention to captures whose original source path matches this glob.
        #[arg(long)]
        source_path: Option<String>,

        /// Preserve raw mirror captures linked to conversations with this tag.
        #[arg(long = "keep-tag")]
        keep_tag: Vec<String>,

        /// Retire every capture of a source file except its newest N. Those N
        /// survive --older-than and --max-size too.
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
        keep_latest: Option<u64>,

        /// Refuse to prune blobs referenced by captures newer than this duration.
        #[arg(long, default_value = "7d")]
        safety_hold_down: String,

        /// Preview only. This is the default; accepted for explicit automation.
        #[arg(long, default_value_t = false, conflicts_with = "apply")]
        dry_run: bool,

        /// Apply the prune plan. Without this flag, cass returns a preview without appending audit records.
        #[arg(long, default_value_t = false)]
        apply: bool,

        /// Output as JSON (`--robot` also works)
        #[arg(long, visible_alias = "robot")]
        json: bool,
    },
}

/// Subcommands for importing external data
#[derive(Subcommand, Debug, Clone)]
pub enum ImportCommand {
    /// Import ChatGPT web export (conversations.json)
    ///
    /// Splits the exported conversations.json into individual files that
    /// the ChatGPT connector can index without encryption keys.
    /// After importing, run `cass index` to index the conversations.
    Chatgpt {
        /// Path to conversations.json from ChatGPT web export
        #[arg(value_hint = ValueHint::FilePath)]
        path: PathBuf,

        /// Output directory (default: ChatGPT app support dir on macOS, or ~/.local/share/cass/chatgpt/ on Linux)
        #[arg(long)]
        output_dir: Option<PathBuf>,

        /// Output as JSON (`{"success", "total", "imported", "skipped", "output_dir", "scan_root"}`)
        #[arg(long, visible_alias = "robot")]
        json: bool,
    },
}

/// Subcommands for managing sources.
///
/// `type = "local"` entries name extra local directories to index in place.
/// Historical `type = "ssh"` entries remain readable as inert archive
/// records: their previously mirrored conversations stay searchable, but
/// remote synchronization was removed — the remote surface (`sync`,
/// `setup`, `discover`, `reingest`, `artifact-manifest`, `schedule`)
/// errors through `REMOVED_COMMANDS`.
#[derive(Subcommand, Debug, Clone)]
pub enum SourcesCommand {
    /// List configured sources
    List {
        /// Show detailed information
        #[arg(long, short)]
        verbose: bool,
        /// Output as JSON (`--robot` also works)
        #[arg(long, visible_alias = "robot")]
        json: bool,
    },
    /// Add a local source: extra directories indexed in place under a name
    Add {
        /// Name for this source (becomes its source_id)
        name: String,
        /// Local paths to index under this source (repeatable, required)
        #[arg(long = "path", short = 'p')]
        paths: Vec<String>,
        /// Output as JSON (`--robot` also works)
        #[arg(long, visible_alias = "robot")]
        json: bool,
    },
    /// Remove a configured source
    Remove {
        /// Name of source to remove
        name: String,
        /// Also delete archived mirror data for this source
        #[arg(long)]
        purge: bool,
        /// Skip confirmation prompt
        #[arg(long, short = 'y')]
        yes: bool,
    },
    /// Diagnose source configuration issues (local checks only)
    Doctor {
        /// Check only specific source (defaults to all)
        #[arg(long, short)]
        source: Option<String>,
        /// Output as JSON (`--robot` also works)
        #[arg(long, visible_alias = "robot")]
        json: bool,
        /// Overall wall-clock budget in milliseconds
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
        budget_ms: Option<u64>,
        /// Retained for CLI compatibility; ignored (no remote probes remain)
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
        per_host_budget_ms: Option<u64>,
    },
    /// Manage path mappings for a source (P6.3)
    #[command(subcommand)]
    Mappings(MappingsAction),
    /// Manage persisted agent indexing exclusions
    #[command(subcommand)]
    Agents(AgentsAction),
}

/// Subcommands for managing semantic search models
#[derive(Subcommand, Debug, Clone)]
pub enum ModelsCommand {
    /// Show model installation status
    Status {
        /// Output as JSON (`--robot` also works)
        #[arg(long, visible_alias = "robot")]
        json: bool,
    },
    /// Build (or verify) the HNSW accelerator for an already-published
    /// semantic vector artifact without opening the archive or embedding
    BuildHnsw {
        /// Semantic tier whose published artifact to accelerate: fast or
        /// quality (default: quality when published, else fast)
        #[arg(long)]
        tier: Option<String>,
        /// Verify the current accelerator against the artifact; never write
        #[arg(long)]
        check: bool,
        /// Rebuild even when the current accelerator already matches
        #[arg(long)]
        force: bool,
        /// Override data dir
        #[arg(long)]
        data_dir: Option<PathBuf>,
        /// Output as JSON (`--robot` also works)
        #[arg(long, visible_alias = "robot")]
        json: bool,
    },
    /// Install the semantic search model from a local directory
    ///
    /// cass is local-only: model bytes always come from a directory you
    /// supply. The directory must contain every file the pinned manifest
    /// requires; each file is SHA256-verified before the install publishes.
    Install {
        /// Model to install (default: all-minilm-l6-v2)
        #[arg(long, default_value = "all-minilm-l6-v2")]
        model: String,
        /// Install from a local model directory (required)
        #[arg(long)]
        from_file: PathBuf,
        /// Override data dir
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
    /// Verify model integrity (SHA256 checksums)
    Verify {
        /// Model to verify (default: all-minilm-l6-v2)
        #[arg(long, default_value = "all-minilm-l6-v2")]
        model: String,
        /// Override data dir
        #[arg(long)]
        data_dir: Option<PathBuf>,
        /// Output as JSON (`--robot` also works)
        #[arg(long, visible_alias = "robot")]
        json: bool,
    },
    /// Run bounded semantic backfill batches from the canonical DB
    Backfill {
        /// Semantic tier to backfill: fast or quality
        #[arg(long, default_value = "fast")]
        tier: String,
        /// Embedder implementation: hash, minilm, or multilingual-minilm (local providers only)
        #[arg(long)]
        embedder: Option<String>,
        /// Maximum canonical conversations to process in this batch
        #[arg(long, default_value_t = 64)]
        batch_conversations: usize,
        /// Maximum batches to run while retaining one loaded embedder
        #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u32).range(1..))]
        max_batches: u32,
        /// Apply idle/load scheduler gates before running this batch
        #[arg(long, visible_alias = "background")]
        scheduled: bool,
        /// Override data dir
        #[arg(long)]
        data_dir: Option<PathBuf>,
        /// Override cass DB path
        #[arg(long)]
        db: Option<PathBuf>,
        /// Output as JSON (`--robot` also works)
        #[arg(long, visible_alias = "robot")]
        json: bool,
    },
    /// Remove model files to free disk space
    Remove {
        /// Model to remove (default: all-minilm-l6-v2)
        #[arg(long, default_value = "all-minilm-l6-v2")]
        model: String,
        /// Skip confirmation prompt
        #[arg(long, short = 'y')]
        yes: bool,
        /// Override data dir
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
}

/// Subcommands for managing path mappings (P6.3)
#[derive(Subcommand, Debug, Clone)]
pub enum MappingsAction {
    /// List path mappings for a source
    List {
        /// Source name
        source: Option<String>,
        /// Output as JSON (`--robot` also works)
        #[arg(long, visible_alias = "robot")]
        json: bool,
    },
    /// Add a path mapping
    Add {
        /// Source name
        source: String,
        /// Remote path prefix to match
        #[arg(long)]
        from: String,
        /// Local path prefix to replace with
        #[arg(long)]
        to: String,
        /// Only apply to specific agents (comma-separated)
        #[arg(long, value_delimiter = ',')]
        agents: Option<Vec<String>>,
    },
    /// Remove a path mapping by index
    Remove {
        /// Source name
        source: String,
        /// Index of mapping to remove (from list output, 0-based)
        index: usize,
    },
    /// Test how a path would be rewritten
    Test {
        /// Source name
        source: String,
        /// Path to test
        path: String,
        /// Optional agent to simulate (for agent-specific rules)
        #[arg(long)]
        agent: Option<String>,
    },
}

/// Subcommands for managing persisted agent indexing exclusions.
#[derive(Subcommand, Debug, Clone)]
pub enum AgentsAction {
    /// List globally excluded agents/connectors
    List {
        /// Output as JSON (`--robot` also works)
        #[arg(long, visible_alias = "robot")]
        json: bool,
    },
    /// Exclude an agent/connector from future indexing runs
    Exclude {
        /// Agent slug to exclude (e.g. openclaw, claude_code, codex)
        agent: String,
        /// Persist the exclusion but leave already indexed archive data untouched
        #[arg(long, default_value_t = false)]
        keep_indexed_data: bool,
    },
    /// Re-include an agent/connector in future indexing runs
    Include {
        /// Agent slug to re-enable
        agent: String,
    },
}

/// `cass index --rebuild-profile` (GH #483).
#[derive(Copy, Clone, Debug, Default, ValueEnum, PartialEq, Eq)]
pub enum RebuildProfileArg {
    /// Host-sized defaults
    #[default]
    Default,
    /// Single-core, low-memory rebuild for a wedged or memory-starved archive
    Bounded,
}

impl From<RebuildProfileArg> for indexer::LexicalRebuildProfile {
    fn from(profile: RebuildProfileArg) -> Self {
        match profile {
            RebuildProfileArg::Default => Self::Default,
            RebuildProfileArg::Bounded => Self::Bounded,
        }
    }
}

#[derive(Copy, Clone, Debug, ValueEnum, PartialEq, Eq)]
pub enum ColorPref {
    Auto,
    Never,
    Always,
}

#[derive(Copy, Clone, Debug, ValueEnum, PartialEq, Eq)]
pub enum ProgressMode {
    Auto,
    Bars,
    Plain,
    None,
}

#[derive(Copy, Clone, Debug, ValueEnum, PartialEq, Eq)]
pub enum RobotTopic {
    Commands,
    Env,
    Paths,
    Schemas,
    Guide,
    ExitCodes,
    Examples,
    Contracts,
    Wrap,
    Sources,
    /// `cass doctor` agent handbook (added in world-class-doctor pass-2).
    /// Body lives in `src/doctor_robot_docs.rs`.
    Doctor,
    /// Canonical recovery + workflow recipes from the 2026-06-08 fleet report
    /// (bead .11.3). Body lives in `src/recipes_robot_docs.rs`.
    Recipes,
}

#[allow(unused_imports)]
pub(crate) use crate::model::cli_error::{CLI_ERROR_ALREADY_REPORTED_SENTINEL, WrapConfig};
#[allow(unused_imports)]
pub use crate::model::cli_error::{CliError, CliResult, RobotFormat};

/// Human-readable display format for CLI output (non-JSON)
#[derive(Copy, Clone, Debug, Default, ValueEnum, PartialEq, Eq)]
pub enum DisplayFormat {
    /// Aligned columns with headers (default human-readable)
    #[default]
    Table,
    /// One-liner per result with key info
    Lines,
    /// Markdown with role headers and code blocks
    Markdown,
}

/// Conversation export format (for export command)
#[derive(Copy, Clone, Debug, Default, ValueEnum, PartialEq, Eq)]
pub enum ConvExportFormat {
    /// Markdown with headers and formatting
    #[default]
    Markdown,
    /// Plain text
    Text,
    /// JSON array of messages
    Json,
    /// HTML with styling
    Html,
}

/// Timeline grouping options
#[derive(Copy, Clone, Debug, Default, ValueEnum, PartialEq, Eq)]
pub enum TimelineGrouping {
    /// Group by hour
    #[default]
    Hour,
    /// Group by day
    Day,
    /// No grouping (flat list)
    None,
}

/// Deployment target for pages export. `local` is the only value that
/// exists: hosted values (`github`, `cloudflare`) are rejected by
/// `REMOVED_FLAGS` on the raw args before clap parses, so no variant is
/// kept for them — they cannot appear in `--help` either.
#[derive(Copy, Clone, Debug, ValueEnum, PartialEq, Eq)]
pub enum PagesDeployTarget {
    /// Local export only (the only target that exists)
    Local,
}

impl PagesDeployTarget {
    pub(crate) fn as_config_value(self) -> &'static str {
        match self {
            PagesDeployTarget::Local => "local",
        }
    }
}

/// Aggregation field types for --aggregate flag
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateField {
    Agent,
    Workspace,
    Date,
    MatchType,
}

impl AggregateField {
    /// Parse field name to enum
    pub(crate) fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "agent" => Some(Self::Agent),
            "workspace" => Some(Self::Workspace),
            "date" => Some(Self::Date),
            "match_type" | "matchtype" => Some(Self::MatchType),
            _ => None,
        }
    }

    /// Get the field name as a string
    #[allow(dead_code)]
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::Agent => "agent",
            Self::Workspace => "workspace",
            Self::Date => "date",
            Self::MatchType => "match_type",
        }
    }
}

/// A single bucket in an aggregation result
#[derive(Debug, Clone, Serialize)]
pub struct AggregationBucket {
    /// The grouped key value
    pub key: String,
    /// Count of items in this bucket
    pub count: u64,
}

/// Aggregation result for a single field
#[derive(Debug, Clone, Serialize)]
pub struct FieldAggregation {
    /// Top buckets (limited to 10 by default)
    pub buckets: Vec<AggregationBucket>,
    /// Total count of items that didn't fit in top buckets
    pub other_count: u64,
}

/// Container for all aggregation results
#[derive(Debug, Clone, Default, Serialize)]
pub struct Aggregations {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent: Option<FieldAggregation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace: Option<FieldAggregation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub date: Option<FieldAggregation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_type: Option<FieldAggregation>,
}

impl Aggregations {
    pub(crate) fn is_empty(&self) -> bool {
        self.agent.is_none()
            && self.workspace.is_none()
            && self.date.is_none()
            && self.match_type.is_none()
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ProgressResolved {
    Bars,
    Plain,
    None,
}
