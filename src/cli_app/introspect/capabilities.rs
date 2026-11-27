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

/// The binary's compiled completed-source reuse policy (GH#426). Reuse is
/// compiled in only when every dependency in the build's `Cargo.lock` has a
/// registry or git source: a local path dependency or `[patch]` can change a
/// parser without changing the lockfile, so such a build reparses every
/// source on every incremental run. Release binaries have it enabled.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SourceIngestReuseCapability {
    pub enabled: bool,
    /// The first unpinned package that disabled reuse; `None` when enabled.
    pub disabled_by: Option<String>,
}

impl SourceIngestReuseCapability {
    #[allow(dead_code)]
    pub(crate) fn compiled() -> Self {
        Self::compiled_with_metadata(
            crate::dependency_pin_correlation::build_metadata::BuildMetadata::current(),
        )
    }

    pub(crate) fn compiled_with_metadata(
        meta: &crate::dependency_pin_correlation::build_metadata::BuildMetadata,
    ) -> Self {
        let blocker = meta.ingest_reuse_blocker;
        Self {
            enabled: meta.ingest_reuse,
            disabled_by: (!blocker.is_empty()).then(|| blocker.to_string()),
        }
    }
}

/// Capabilities response for agent introspection.
/// Provides static information about CLI features, versions, and limits.
#[derive(Debug, Clone, Serialize)]
pub struct CapabilitiesResponse {
    /// Alias for `crate_version`; included for generic agent clients.
    pub version: String,
    /// Semantic version of the crate
    pub crate_version: String,
    /// Short commit hash the binary was built from (with `-dirty` suffix for
    /// uncommitted worktrees); `"unknown"` when built without git metadata
    /// (crates.io / tarball builds). Machine-readable build identity (#399).
    pub build_commit: String,
    /// Commit date (`YYYY-MM-DD`) of `build_commit`; `"unknown"` when built
    /// without git metadata.
    pub build_commit_date: String,
    /// Whether this binary skips re-parsing sources it already ingested
    /// completely (GH#426). Decided at build time.
    pub source_ingest_reuse: SourceIngestReuseCapability,
    /// API contract version (bumped on breaking changes)
    pub api_version: u32,
    /// Human-readable contract identifier
    pub contract_version: String,
    /// List of supported feature flags
    pub features: Vec<String>,
    /// List of supported agent connectors
    pub connectors: Vec<String>,
    /// Global flags that can appear before or after subcommands
    pub global_flags: Vec<ArgumentSchema>,
    /// All advertised commands with argument metadata
    pub commands: Vec<CommandSchema>,
    /// Exit-code handling guide for automation
    pub exit_codes: Vec<ExitCodeCapability>,
    /// Environment variables that affect command behavior
    pub env_vars: Vec<EnvVarCapability>,
    /// Copy-paste workflow recipes for common agent tasks
    pub workflows: Vec<WorkflowCapability>,
    /// Common mistakes that cass accepts or rejects with a teaching recovery
    pub mistake_recoveries: Vec<MistakeRecoveryCapability>,
    /// System limits
    pub limits: CapabilitiesLimits,
}

#[derive(Debug, Clone, Serialize)]
pub struct CapabilitiesLimits {
    /// Maximum --limit value
    pub max_limit: usize,
    /// Maximum --max-content-length value (0 = unlimited)
    pub max_content_length: usize,
    /// Maximum fields in --fields selection
    pub max_fields: usize,
    /// Maximum aggregation bucket count per field
    pub max_agg_buckets: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExitCodeCapability {
    /// Numeric code or compact range, e.g. "20-21"
    pub code: String,
    /// Stable human-readable meaning
    pub meaning: String,
    /// yes, no, depends, or maybe
    pub retryable: String,
    /// Agent-oriented handling instruction
    pub agent_action: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct EnvVarCapability {
    /// Environment variable name
    pub name: String,
    /// Default value when the contract has a meaningful one
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,
    /// Agent-oriented description of the behavior it controls
    pub description: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkflowCapability {
    /// Stable workflow identifier for automation clients
    pub name: String,
    /// Natural-language task this recipe covers
    pub intent: String,
    /// First command to run from a cold context
    pub first_command: String,
    /// Follow-up commands to run based on the first command's result
    pub follow_up_commands: Vec<String>,
    /// Fields or streams the caller should parse
    pub parse_contract: String,
    /// Agent-specific caveat that prevents common misuse
    pub note: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct MistakeRecoveryCapability {
    /// Common mistaken invocation
    pub wrong: String,
    /// Canonical spelling or safer replacement
    pub canonical: String,
    /// Whether cass currently accepts the mistaken form
    pub accepted: bool,
    /// What cass does with this form
    pub behavior: String,
}

// ============================================================================
// Introspect command schema structures
// ============================================================================

/// Full API introspection response
#[derive(Debug, Clone, Serialize)]
pub struct IntrospectResponse {
    /// API version (matches capabilities)
    pub api_version: u32,
    /// Contract version (human-visible)
    pub contract_version: String,
    /// Executable proof-coverage inventory for every report subsystem.
    pub subsystem_coverage: crate::subsystem_coverage_matrix::MatrixReport,
    /// Global flags (apply to all commands)
    pub global_flags: Vec<ArgumentSchema>,
    /// All available commands with arguments
    pub commands: Vec<CommandSchema>,
    /// Response schemas for JSON outputs.
    ///
    /// Uses `BTreeMap` so the serialized JSON key order is deterministic
    /// (alphabetical) across runs — agents that bind to cass programmatically
    /// rely on stable `introspect --json` output for typed-client generation,
    /// and golden-file regression tests require byte-stable output (bead
    /// coding_agent_session_search-8sl73).
    pub response_schemas: std::collections::BTreeMap<String, serde_json::Value>,
}

/// Schema for a single CLI command
#[derive(Debug, Clone, Serialize)]
pub struct CommandSchema {
    /// Command name (e.g., "search", "status")
    pub name: String,
    /// Short description
    pub description: String,
    /// Arguments and options
    pub arguments: Vec<ArgumentSchema>,
    /// Whether this command supports --json output
    pub has_json_output: bool,
}

/// Schema for a command argument/option
#[derive(Debug, Clone, Serialize)]
pub struct ArgumentSchema {
    /// Argument name (e.g., "query", "limit", "json")
    pub name: String,
    /// Short flag (e.g., 'n' for -n)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub short: Option<char>,
    /// Description
    pub description: String,
    /// Type: "flag", "option", "positional"
    pub arg_type: String,
    /// Value type: "string", "integer", "path", "boolean", "enum"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value_type: Option<String>,
    /// Whether required
    pub required: bool,
    /// Default value if any
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,
    /// Enum values if `value_type` is "enum"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enum_values: Option<Vec<String>>,
    /// Whether option can be repeated
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repeatable: Option<bool>,
}

/// Global flags that apply to all commands
pub(crate) fn build_global_flag_schemas() -> Vec<ArgumentSchema> {
    vec![
        ArgumentSchema {
            name: "db".to_string(),
            short: None,
            description: "Path to the SQLite database (defaults to platform data dir)".to_string(),
            arg_type: "option".to_string(),
            value_type: Some("path".to_string()),
            required: false,
            default: None,
            enum_values: None,
            repeatable: None,
        },
        ArgumentSchema {
            name: "robot-help".to_string(),
            short: None,
            description: "Deterministic machine-first help (no TUI)".to_string(),
            arg_type: "flag".to_string(),
            value_type: None,
            required: false,
            default: None,
            enum_values: None,
            repeatable: None,
        },
        ArgumentSchema {
            name: "trace-file".to_string(),
            short: None,
            description: "Trace command execution spans to JSONL file".to_string(),
            arg_type: "option".to_string(),
            value_type: Some("path".to_string()),
            required: false,
            default: None,
            enum_values: None,
            repeatable: None,
        },
        ArgumentSchema {
            name: "quiet".to_string(),
            short: Some('q'),
            description: "Reduce log noise (warnings and errors only)".to_string(),
            arg_type: "flag".to_string(),
            value_type: None,
            required: false,
            default: None,
            enum_values: None,
            repeatable: None,
        },
        ArgumentSchema {
            name: "verbose".to_string(),
            short: Some('v'),
            description: "Increase verbosity (debug information)".to_string(),
            arg_type: "flag".to_string(),
            value_type: None,
            required: false,
            default: None,
            enum_values: None,
            repeatable: None,
        },
        ArgumentSchema {
            name: "color".to_string(),
            short: None,
            description: "Color behavior for CLI output".to_string(),
            arg_type: "option".to_string(),
            value_type: Some("enum".to_string()),
            required: false,
            default: Some("auto".to_string()),
            enum_values: Some(vec![
                "auto".to_string(),
                "never".to_string(),
                "always".to_string(),
            ]),
            repeatable: None,
        },
        ArgumentSchema {
            name: "progress".to_string(),
            short: None,
            description: "Progress output style".to_string(),
            arg_type: "option".to_string(),
            value_type: Some("enum".to_string()),
            required: false,
            default: Some("auto".to_string()),
            enum_values: Some(vec![
                "auto".to_string(),
                "bars".to_string(),
                "plain".to_string(),
                "none".to_string(),
            ]),
            repeatable: None,
        },
        ArgumentSchema {
            name: "wrap".to_string(),
            short: None,
            description: "Wrap informational output to N columns".to_string(),
            arg_type: "option".to_string(),
            value_type: Some("integer".to_string()),
            required: false,
            default: None,
            enum_values: None,
            repeatable: None,
        },
        ArgumentSchema {
            name: "nowrap".to_string(),
            short: None,
            description: "Disable wrapping entirely".to_string(),
            arg_type: "flag".to_string(),
            value_type: None,
            required: false,
            default: None,
            enum_values: None,
            repeatable: None,
        },
    ]
}

pub(crate) fn exit_code_capability(
    code: &str,
    meaning: &str,
    retryable: &str,
    agent_action: &str,
) -> ExitCodeCapability {
    ExitCodeCapability {
        code: code.to_string(),
        meaning: meaning.to_string(),
        retryable: retryable.to_string(),
        agent_action: agent_action.to_string(),
    }
}

pub(crate) fn build_exit_code_capabilities() -> Vec<ExitCodeCapability> {
    vec![
        exit_code_capability("0", "success", "no", "Read stdout as the command result."),
        exit_code_capability(
            "1",
            "health check failed",
            "yes",
            "Inspect JSON recommended_action before retrying or indexing.",
        ),
        exit_code_capability(
            "2",
            "usage or parsing error",
            "no",
            "Fix argv syntax; follow the JSON err.hint or examples.",
        ),
        exit_code_capability(
            "3",
            "index or database missing",
            "yes",
            "Run cass index --full when health/status recommends it.",
        ),
        exit_code_capability(
            "4",
            "I/O failure or unsafe operation refused",
            "maybe",
            "Branch on err.kind: fix the path, permissions, or free space for io/output-not-writable; follow err.hint for refused-unsafe. Not a network code: source problems fail with 12, model errors with 20-22 or 24.",
        ),
        exit_code_capability(
            "5",
            "data corruption",
            "depends",
            "Inspect health/status/doctor JSON and rebuild only derived assets when recommended.",
        ),
        exit_code_capability(
            "6",
            "required input missing",
            "no",
            "Supply the missing input named by err.kind (e.g. a resume command), then rerun.",
        ),
        exit_code_capability(
            "7",
            "lock or busy",
            "yes",
            "Retry later with bounded backoff; another cass operation owns the lock.",
        ),
        exit_code_capability(
            "9",
            "unknown error",
            "maybe",
            "Read err.kind, err.message, and trace output before deciding.",
        ),
        exit_code_capability(
            "10",
            "config or timeout",
            "depends",
            "Branch on JSON err.kind; numeric code 10 is intentionally domain-specific.",
        ),
        exit_code_capability(
            "11",
            "config validation",
            "no",
            "Fix the reported configuration field before retrying.",
        ),
        exit_code_capability(
            "12",
            "source problem",
            "maybe",
            "Inspect source JSON fields and verify the configured source or path.",
        ),
        exit_code_capability(
            "13",
            "mapping or not-found",
            "depends",
            "Branch on err.kind and verify the requested source/session mapping.",
        ),
        exit_code_capability(
            "14",
            "I/O or mapping",
            "maybe",
            "Inspect err.kind and path details; retry only after the path is reachable.",
        ),
        exit_code_capability(
            "15",
            "semantic or embedder unavailable",
            "yes",
            "Install/verify models or continue with lexical fallback when acceptable.",
        ),
        exit_code_capability(
            "20-21",
            "model acquisition failure",
            "maybe",
            "Check err.kind and err.hint; fix the local model files and retry --from-file.",
        ),
        exit_code_capability(
            "22",
            "I/O during model handling",
            "maybe",
            "Verify model cache permissions and available disk before retrying.",
        ),
        exit_code_capability(
            "24",
            "I/O during model verify/install",
            "maybe",
            "Fix filesystem permissions or disk space, then retry.",
        ),
        exit_code_capability(
            "70",
            "index stalled and aborted",
            "yes",
            "Read the kind index-stalled envelope on stderr and cass status --json, then rerun cass index; the lock is reaped on the next start.",
        ),
        exit_code_capability(
            "124",
            "search-service request deadline",
            "yes",
            "Rerun the cass serve request; raise --request-timeout-ms only when the work legitimately needs longer.",
        ),
        exit_code_capability(
            "125",
            "search-service supervisor failure",
            "maybe",
            "The deadline watchdog, resident-memory monitor or bridge teardown failed; rerun the command and report a recurrence.",
        ),
        exit_code_capability(
            "126",
            "search-service resident-memory limit",
            "yes",
            "Rerun with a larger --max-resident-mib or a smaller request; the sampler is not a kernel allocation limit.",
        ),
        exit_code_capability(
            "130",
            "interrupted (SIGINT)",
            "yes",
            "Rerun the command; indexing and search resume from their checkpoints.",
        ),
        exit_code_capability(
            "143",
            "interrupted (SIGTERM)",
            "yes",
            "Rerun the command; indexing and semantic backfill resume from their checkpoints.",
        ),
    ]
}

pub(crate) fn env_var_capability(
    name: &str,
    default: Option<&str>,
    description: &str,
) -> EnvVarCapability {
    EnvVarCapability {
        name: name.to_string(),
        default: default.map(str::to_string),
        description: description.to_string(),
    }
}

pub(crate) fn build_env_var_capabilities() -> Vec<EnvVarCapability> {
    vec![
        env_var_capability(
            "TUI_HEADLESS",
            Some("0"),
            "Set to 1 for headless test runs and prompt-free TUI behavior.",
        ),
        env_var_capability("CASS_DATA_DIR", None, "Override the cass data directory."),
        env_var_capability("CASS_DB_PATH", None, "Override the SQLite database path."),
        env_var_capability(
            "CASS_EXCLUDE_PATHS",
            None,
            "Comma/newline-separated source files or directory prefixes to exclude from future ingestion and raw capture. Holds scan/watch watermarks; existing archived history remains searchable. Use cass forget to remove indexed copies.",
        ),
        env_var_capability(
            "CASS_EXCLUDE_WORKSPACES",
            None,
            "Comma/newline-separated workspace globs to exclude from future ingestion. Scan watermarks advance; changing the policy revisits sources. Existing history and shared raw-store captures remain; use cass forget --workspace-glob and cass mirror prune for explicit cleanup.",
        ),
        env_var_capability(
            "CASS_OUTPUT_FORMAT",
            None,
            "Default structured output format: json, jsonl, compact, sessions, or toon.",
        ),
        env_var_capability(
            "CASS_SEARCH_TIMEOUT_MS",
            None,
            "Default search/pack timeout in milliseconds (0 = no timeout). The --timeout flag overrides; also configurable via [search].timeout_ms in ~/.config/cass/cass.toml. Alias: CASS_SEARCH_TIMEOUT.",
        ),
        env_var_capability(
            "CASS_SEARCH_LIMIT",
            None,
            "Default search/pack result limit (0 = no limit, RAM-capped). The --limit flag overrides; also [search].limit in ~/.config/cass/cass.toml.",
        ),
        env_var_capability(
            "CASS_SEARCH_MODE",
            None,
            "Default search/pack mode: lexical, semantic, or hybrid. The --mode flag overrides; also [search].mode in ~/.config/cass/cass.toml.",
        ),
        env_var_capability(
            "CASS_STATUS_BUDGET_MS",
            Some("8000"),
            "Robot status execution budget in milliseconds. After expiry, optional or expensive sections are reported in budget.skipped_sections.",
        ),
        env_var_capability(
            "CASS_DOCTOR_BUDGET_MS",
            Some("8000"),
            "Structured doctor reporting budget in milliseconds. Reports whose elapsed time exceeds it set budget.timed_out.",
        ),
        env_var_capability(
            "CASS_FTS_DRYRUN_CAP",
            Some("4096"),
            "Row-ID comparison cap for the read-only 'doctor --rebuild-canonical-fts --dry-run' divergence scan (#345). At the cap the dry-run stops and reports a '>= N divergent' floor instead of an exact count; exact parity is deferred to the --yes apply path. Positive integer; invalid values fall back to the default.",
        ),
        env_var_capability(
            "CASS_VIEW_BUDGET_MS",
            Some("10000"),
            "Structured view read-only worker deadline in milliseconds. On expiry, view returns a partial budget envelope with an exact retry probe.",
        ),
        env_var_capability(
            "CASS_SEARCH_BUDGET_MS",
            Some("120000"),
            "Fallback structured search worker budget in milliseconds when --timeout, CASS_SEARCH_TIMEOUT_MS, and [search].timeout_ms do not supply a positive effective timeout.",
        ),
        env_var_capability(
            "CASS_TRIAGE_BUDGET_MS",
            Some("8000"),
            "Robot triage read-only probe budget in milliseconds. On expiry, incomplete readiness sections are reported in budget.skipped_sections.",
        ),
        env_var_capability(
            "CASS_PACK_BUDGET_MS",
            Some("10000"),
            "Fallback structured pack execution budget in milliseconds when --timeout, CASS_SEARCH_TIMEOUT_MS, and [search].timeout_ms do not supply a positive effective timeout.",
        ),
        env_var_capability(
            "TOON_DEFAULT_FORMAT",
            None,
            "Fallback structured output format for tools that support TOON.",
        ),
        env_var_capability(
            "TOON_INDENT",
            None,
            "Pretty-print TOON output with the requested indentation.",
        ),
        env_var_capability(
            "TOON_KEY_FOLDING",
            Some("safe"),
            "Controls TOON key folding: off or safe.",
        ),
        env_var_capability("CASS_NO_COLOR", None, "Force monochrome output."),
        env_var_capability(
            "CASS_RESPECT_NO_COLOR",
            Some("0"),
            "Set to 1 to honor the global NO_COLOR convention.",
        ),
        env_var_capability(
            "NO_COLOR",
            None,
            "Global no-color convention when respected.",
        ),
        env_var_capability(
            "CASS_TRACE_FILE",
            None,
            "Default cass-trace-v1 JSONL path for bounded diagnostics and command summaries.",
        ),
        env_var_capability(
            "CASS_TRACE_FILTER",
            Some(DEFAULT_TRACE_FILE_LOG_DIRECTIVE),
            "Trace-file EnvFilter. The default retains CASS DEBUG plus global WARN/ERROR events, while suppressing routine redaction-memo DEBUG churn.",
        ),
        env_var_capability(
            "CASS_TRACE_MAX_BYTES",
            Some("16777216"),
            "Hard trace-file byte ceiling, clamped to 4096..1073741824 bytes.",
        ),
        env_var_capability(
            "CASS_TRACE_MAX_EVENTS",
            Some("50000"),
            "Hard trace-file diagnostic-event ceiling, clamped to 16..10000000 events.",
        ),
        env_var_capability(
            "CASS_INDEX_NO_PROGRESS_EVENTS",
            Some("0"),
            "Suppress NDJSON progress events from cass index --json when set to 1.",
        ),
        env_var_capability(
            "CASS_INDEX_INGEST_WAL_CHECKPOINT_BYTES",
            Some("536870912"),
            "Reset a deferred bulk-ingest WAL after it reaches this many bytes; 0 disables the in-run bound.",
        ),
        env_var_capability(
            "CASS_INDEX_INGEST_WAL_CHECKPOINT_MODE",
            Some("TRUNCATE"),
            "Checkpoint mode for the single-connection in-run bulk WAL reset: PASSIVE, FULL, RESTART, or TRUNCATE.",
        ),
        env_var_capability(
            "CASS_DEFER_ANALYTICS_UPDATES",
            Some("0"),
            "Set to 1 to defer derived analytics writes during indexing. A legacy OMP identity migration keeps its analytics phase and durable cursor pending until a later index run with this unset.",
        ),
        env_var_capability(
            "CASS_RAW_MIRROR",
            Some("1"),
            "Raw-mirror capture of session source files. Set 0, false, no or off to stop copying sources whose provider keeps its own history; existing captures stay until `cass mirror prune`, indexing is unaffected, and doctor reports raw_mirror_capture_disabled because a file its provider deletes then survives only in the archive DB.",
        ),
        env_var_capability(
            "CASS_AUTO_REFRESH",
            Some("1"),
            "Stale-on-read catch-up: search/pack/TUI may spawn a detached low-priority `cass index --background` when the index is stale, partial, or behind. Set 0 to disable globally; `search --no-maintenance` always disables it for that request.",
        ),
        env_var_capability(
            "CASS_AUTO_REFRESH_COOLDOWN_SECS",
            Some("300"),
            "Minimum seconds between two auto-spawned catch-up index runs per data dir.",
        ),
        env_var_capability(
            "CASS_BACKGROUND_NICE",
            Some("15"),
            "nice value `cass index --background` applies to itself (0..=19).",
        ),
        env_var_capability(
            "CASS_BACKGROUND_IONICE_CLASS",
            Some("3"),
            "ionice class `cass index --background` applies to itself on Linux (3 = idle).",
        ),
        env_var_capability(
            "CASS_RESPONSIVENESS_MIN_USER_IDLE_SECS",
            Some("0"),
            "Require this many seconds of console idle time (macOS HIDIdleTime) before scheduled semantic backfill / nightly schedule jobs run; 0 disables, unknown idle fails open.",
        ),
        env_var_capability(
            "CASS_DAEMON_INDEX_INTERVAL_SECS",
            Some("0"),
            "While the semantic daemon is resident, spawn a detached incremental `cass index --background` every N seconds (0 = off; 900 recommended). Skipped under severe load and while another index run holds the lock.",
        ),
        env_var_capability(
            "CASS_SCHEDULE_MAX_BACKFILL_BATCHES",
            Some("200"),
            "Upper bound on `cass models backfill --scheduled` batches one nightly `cass schedule run` will execute.",
        ),
        env_var_capability(
            "CASS_SCHEDULE_MAX_LOAD_DEFERRAL_SECS",
            Some("10800"),
            "Longest severe machine load may keep skipping scheduled incremental runs, measured from the last incremental run that completed; past it the run goes ahead at scheduler priority. 0 = skip for load without limit.",
        ),
        env_var_capability(
            "CASS_INDEX_STALL_DETECT_SECS",
            Some("120"),
            "Seconds without measured phase progress before cass index emits diagnostics; 0 disables detection.",
        ),
        env_var_capability(
            "CASS_INDEX_STALL_ABORT_SECS",
            Some("300"),
            "Seconds without progress before an abort-eligible lexical stall exits 70; 0 keeps stalls report-only. Semantic phases are report-only unless CASS_INDEX_STALL_ABORT_ALL_PHASES=1.",
        ),
        env_var_capability(
            "CASS_INDEX_STALL_ABORT_ALL_PHASES",
            Some("0"),
            "Opt-in (#437): promote the stall watchdog's report-only warnings to a hard abort (exit 70) in ANY phase — including scribe/accumulate and semantic report-only lanes — once CASS_INDEX_STALL_ABORT_SECS elapses without progress. The finalize/persist grace thresholds still apply; phases whose detection is suppressed (opaque HNSW, quiescent watch idle) never warn and therefore never abort.",
        ),
        env_var_capability(
            "CASS_SEMANTIC_EMBEDDER",
            None,
            "Select the semantic embedder/model implementation when configured.",
        ),
        env_var_capability(
            "CASS_SEMANTIC_BATCH_SIZE",
            Some("128"),
            "Batch size for semantic embedding work.",
        ),
        env_var_capability(
            "CASS_SEMANTIC_EMBED_BATCH_WARN_AFTER_MS",
            Some("30000"),
            "Warn when one semantic embedder batch exceeds the cass#257-derived 30s healthy-run threshold.",
        ),
        env_var_capability(
            "CASS_SEMANTIC_EMBED_BATCH_FAIL_AFTER_MS",
            Some("300000"),
            "Abort after a returned semantic embedder batch exceeds the cass#257-derived 5m watchdog threshold; 0 disables.",
        ),
        env_var_capability(
            "CASS_SEMANTIC_MAX_MESSAGES_PER_CHECKPOINT",
            Some("10000"),
            "Soft cap for cass models backfill checkpoints at whole-conversation prefixes near 10k selected messages; 0 disables.",
        ),
        env_var_capability(
            "CASS_SEMANTIC_MAX_BYTES_PER_CHECKPOINT",
            Some("8388608"),
            "Soft cap for cass models backfill checkpoints at whole-conversation prefixes near 8MiB selected content; 0 disables.",
        ),
        env_var_capability(
            "CASS_RESPONSIVENESS_DISABLE",
            Some("0"),
            "Set to 1 to disable the indexer responsiveness governor.",
        ),
        env_var_capability(
            "CASS_RESPONSIVENESS_CALIBRATION",
            Some("conformal"),
            "Responsiveness-governor threshold policy: conformal or static.",
        ),
        env_var_capability(
            "CASS_STREAMING_CONSUMER_COMMIT_SECS",
            Some("5"),
            "Base lexical commit cadence for the streaming consumer.",
        ),
        env_var_capability(
            "CASS_STREAMING_CONSUMER_COMBINE",
            Some("1"),
            "Set to 0 to disable flat-combining drain in the streaming consumer.",
        ),
        env_var_capability(
            "CASS_OMP_DATA_ROOT",
            None,
            "Declare an OMP-only archive/store root; CASS assigns OMP identity and resume plans preserve its exact sessions root.",
        ),
        env_var_capability(
            "PI_SESSIONS_DIR",
            None,
            "Override the exact Pi Agent sessions directory.",
        ),
        env_var_capability(
            "PI_CODING_AGENT_DIR",
            None,
            "Override the shared Pi-family agent directory; ambiguous paths remain Pi Agent-owned. Use CASS_OMP_DATA_ROOT for OMP-only ownership.",
        ),
        env_var_capability(
            "PI_CODING_AGENT_SESSION_DIR",
            None,
            "Override the OMP session directory exactly; resume plans preserve this root with --session-dir.",
        ),
        env_var_capability(
            "PI_CONFIG_DIR",
            Some(".omp"),
            "Override the OMP config directory name beneath the user home.",
        ),
        env_var_capability(
            "OMP_PROFILE",
            None,
            "Select an isolated OMP profile; takes precedence over PI_PROFILE.",
        ),
        env_var_capability(
            "PI_PROFILE",
            None,
            "Legacy fallback for selecting an isolated OMP profile.",
        ),
        env_var_capability(
            "XDG_DATA_HOME",
            None,
            "Select the XDG data root, including OMP v18 session and profile stores.",
        ),
        env_var_capability("CODEX_HOME", None, "Override Codex session discovery root."),
        env_var_capability(
            "GEMINI_HOME",
            None,
            "Override Gemini session discovery root.",
        ),
        env_var_capability(
            "OPENCODE_STORAGE_ROOT",
            None,
            "Override OpenCode session discovery root.",
        ),
        env_var_capability(
            "CHATGPT_ENCRYPTION_KEY",
            None,
            "Provide the key used for encrypted ChatGPT conversation imports.",
        ),
        env_var_capability(
            "CASS_DAEMON_SOCKET",
            None,
            "Override the semantic daemon Unix socket path.",
        ),
    ]
}

pub(crate) fn workflow_capability(
    name: &str,
    intent: &str,
    first_command: &str,
    follow_up_commands: &[&str],
    parse_contract: &str,
    note: &str,
) -> WorkflowCapability {
    WorkflowCapability {
        name: name.to_string(),
        intent: intent.to_string(),
        first_command: first_command.to_string(),
        follow_up_commands: follow_up_commands
            .iter()
            .map(|command| (*command).to_string())
            .collect(),
        parse_contract: parse_contract.to_string(),
        note: note.to_string(),
    }
}

pub(crate) fn build_workflow_capabilities() -> Vec<WorkflowCapability> {
    vec![
        workflow_capability(
            "cold-start",
            "Discover readiness, exact next command, docs, schemas, workflows, and recoveries from zero context.",
            "cass triage --json",
            &[
                "cass capabilities --json",
                "cass --robot-help --color=never",
                "cass robot-docs guide --color=never",
                "cass introspect --json",
            ],
            "Parse status, healthy, initialized, next_command, recommended_commands[], discovery, starter_workflows[], and mistake_recoveries[].",
            "Use triage first when you do not know whether the archive is initialized; use capabilities/introspect for static API binding.",
        ),
        workflow_capability(
            "api-discovery",
            "Discover the static CLI contract for typed clients and prompt construction.",
            "cass capabilities --json",
            &[
                "cass introspect --json",
                "cass robot-docs commands --color=never",
                "cass robot-docs schemas --color=never",
            ],
            "Parse workflows, commands, global_flags, exit_codes, env_vars, limits, and response_schemas.",
            "Capabilities is static self-description; triage is the read-only runtime preflight.",
        ),
        workflow_capability(
            "health-preflight",
            "Check whether search is ready before issuing queries.",
            "cass health --json",
            &[
                "cass status --json",
                "cass index --full --json",
                "cass doctor --json",
            ],
            "Branch on healthy, status, fallback_mode, and recommended_action.",
            "Do not hand-delete index assets; index/doctor are the recovery surfaces.",
        ),
        workflow_capability(
            "bounded-search",
            "Search sessions without flooding the caller's token budget.",
            "cass search \"<query>\" --robot --limit 10 --fields summary,provenance,conversation_id --max-content-length 800 --robot-meta",
            &[
                "cass view <source_path> --message-index <line_number> --source <source_id> --conversation-id <conversation_id> --json",
                "cass expand <source_path> --message-index <line_number> --source <source_id> --conversation-id <conversation_id> -C 3 --json",
                "cass pack \"<query>\" --robot --max-tokens 4000 --limit 10",
            ],
            "Parse hits[], _meta.realized_mode, _meta.fallback_mode, cursor, and *_truncated flags.",
            "Prefer an explicit --limit in automation; limit 0 intentionally means unbounded.",
        ),
        workflow_capability(
            "answer-pack",
            "Build a deterministic cited handoff bundle for another agent.",
            "cass pack \"<question>\" --robot --max-tokens 4000 --max-evidence 8 --max-sessions 3 --require-evidence",
            &[
                "cass search \"<question>\" --robot --limit 10 --fields summary,provenance,conversation_id --robot-meta",
                "cass view <source_path> --message-index <line_number> --source <source_id> --conversation-id <conversation_id> --json",
            ],
            "Parse evidence[], warnings[], privacy, freshness, health, and omitted[].",
            "Use pack when the downstream consumer needs citations rather than raw search hits.",
        ),
        workflow_capability(
            "session-drilldown",
            "Move from a hit to surrounding conversation context.",
            "cass view <source_path> --message-index <line_number> --source <source_id> --conversation-id <conversation_id> --json",
            &[
                "cass expand <source_path> --message-index <line_number> --source <source_id> --conversation-id <conversation_id> -C 5 --json",
                "cass context <source_path> --json",
                "cass resume <source_path> --shell",
            ],
            "Parse coordinate_space, message_index, conversation_id, source_id, content, and is_target in the returned messages.",
            "Pass source_path, source_id, conversation_id, and line_number from the same hit; line_number belongs to --message-index, not raw --line.",
        ),
        workflow_capability(
            "semantic-models",
            "Understand or repair semantic-search readiness.",
            "cass models status --json",
            &[
                "cass models verify --json",
                "cass models install --from-file <dir> --model minilm",
                "cass models backfill",
            ],
            "Parse installed, available, cache_dir, verification status, and err.kind.",
            "Models are opt-in; lexical fallback is valid while the model is absent.",
        ),
    ]
}

pub(crate) fn mistake_recovery_capability(
    wrong: &str,
    canonical: &str,
    accepted: bool,
    behavior: &str,
) -> MistakeRecoveryCapability {
    MistakeRecoveryCapability {
        wrong: wrong.to_string(),
        canonical: canonical.to_string(),
        accepted,
        behavior: behavior.to_string(),
    }
}

pub(crate) fn build_mistake_recovery_capabilities() -> Vec<MistakeRecoveryCapability> {
    vec![
        mistake_recovery_capability(
            "cass searh \"query\" --robot --limit 10",
            "cass search \"query\" --robot --limit 10",
            true,
            "Levenshtein command recovery corrects the top-level subcommand typo.",
        ),
        mistake_recovery_capability(
            "cass -robot-help",
            "cass --robot-help",
            true,
            "Single-dash long global flags are normalized to double-dash form.",
        ),
        mistake_recovery_capability(
            "cass --Robot-help",
            "cass --robot-help",
            true,
            "Long flags are lowercased when the lowercase spelling is known.",
        ),
        mistake_recovery_capability(
            "cass --robot-docs=commands",
            "cass robot-docs commands",
            true,
            "The legacy flag-shaped spelling is normalized into the robot-docs subcommand.",
        ),
        mistake_recovery_capability(
            "cass commands --json",
            "cass robot-docs commands",
            true,
            "Non-command robot-docs topic shorthands such as commands, schemas, examples, env, paths, exit-codes, and quickstart route to robot-docs instead of search. The guide name remains the dedicated guided-operations planner.",
        ),
        mistake_recovery_capability(
            "cass search \"query\" limit=5",
            "cass search \"query\" --limit 5",
            true,
            "Bare key=value arguments are promoted to known long flags.",
        ),
        mistake_recovery_capability(
            "cass robot-docs --robot",
            "cass robot-docs guide",
            true,
            "robot-docs is already machine-readable; redundant robot/json flags are removed and the guide topic is used by default.",
        ),
        mistake_recovery_capability(
            "cass help --json",
            "cass robot-docs guide",
            true,
            "Structured help requests route to the agent guide instead of clap's human help path.",
        ),
        mistake_recovery_capability(
            "cass help commands --json",
            "cass robot-docs commands",
            true,
            "Structured help for robot-docs topics routes directly to the matching robot-docs topic.",
        ),
        mistake_recovery_capability(
            "cass search --help --json",
            "cass robot-docs commands",
            true,
            "Structured command help routes to the machine-readable command reference while plain --help remains native clap help.",
        ),
        mistake_recovery_capability(
            "cass ready --json",
            "cass triage --json",
            true,
            "ready is accepted as the one-shot agent triage alias.",
        ),
        mistake_recovery_capability(
            "cass preflight --json",
            "cass triage --json",
            true,
            "preflight is accepted as the one-shot agent triage alias.",
        ),
        mistake_recovery_capability(
            "cass --json",
            "cass triage --json",
            true,
            "A top-level structured-output request defaults to read-only agent triage instead of launching the TUI or failing usage.",
        ),
        mistake_recovery_capability(
            "cass --robot",
            "cass triage --json",
            true,
            "A top-level robot-mode request defaults to read-only agent triage.",
        ),
        mistake_recovery_capability(
            "cass --json search auth",
            "cass search auth --json",
            true,
            "A leading structured-output flag is moved after the robot-capable subcommand.",
        ),
        mistake_recovery_capability(
            "cass --robot status",
            "cass status --json",
            true,
            "A leading robot-mode flag is canonicalized to the subcommand's JSON output flag.",
        ),
        mistake_recovery_capability(
            "cass --data-dir /tmp/cass health --json",
            "cass health --data-dir /tmp/cass --json",
            true,
            "A leading data-dir override is moved onto the data-dir-aware subcommand.",
        ),
        mistake_recovery_capability(
            "cass search --query auth --json",
            "cass search auth --json",
            true,
            "A named query option is converted to the required positional query for agent-facing commands.",
        ),
        mistake_recovery_capability(
            "cass search --q auth --json",
            "cass search auth --json",
            true,
            "Short and familiar query aliases such as --q, --text, and --pattern are converted to the required positional query.",
        ),
        mistake_recovery_capability(
            "cass search auth error --json",
            "cass search \"auth error\" --json",
            true,
            "Adjacent bare words after search/pack are folded into the query positional before filters are parsed.",
        ),
        mistake_recovery_capability(
            "cass answer auth --json",
            "cass pack auth --json",
            true,
            "Answer/handoff/bundle/evidence aliases route cited-handoff intent to the answer-pack command instead of implicit search.",
        ),
        mistake_recovery_capability(
            "cass why auth failed --json --max-evidence 3",
            "cass pack \"auth failed\" --json --max-evidence 3",
            true,
            "Question/RC prompt aliases route answer intent to pack, so pack-only evidence flags do not get misparsed as search flags.",
        ),
        mistake_recovery_capability(
            "cass html-export session.jsonl --json",
            "cass export-html session.jsonl --json",
            true,
            "Reversed HTML export spellings such as html-export, html_export, and exporthtml route to the self-contained HTML export command.",
        ),
        mistake_recovery_capability(
            "cass current --json",
            "cass sessions --current --json",
            true,
            "Current-session shorthands route to session discovery instead of being treated as a search query.",
        ),
        mistake_recovery_capability(
            "cass sessions current --json",
            "cass sessions --current --json",
            true,
            "The sessions command accepts current as a positional shorthand for the --current flag.",
        ),
        mistake_recovery_capability(
            "cass auth failed --json --max-evidence 3",
            "cass pack \"auth failed\" --json --max-evidence 3",
            true,
            "A bare robot query with pack-only flags is treated as answer-pack intent instead of implicit search.",
        ),
        mistake_recovery_capability(
            "cass search auth failed --json --max-evidence 3",
            "cass pack \"auth failed\" --json --max-evidence 3",
            true,
            "An explicit structured-output search with pack-only flags is treated as answer-pack intent instead of failing on search-only parsing.",
        ),
        mistake_recovery_capability(
            "cass auth error --json",
            "cass search \"auth error\" --json",
            true,
            "Unquoted top-level words in robot mode are folded into an implicit search query unless they look like a subcommand typo.",
        ),
        mistake_recovery_capability(
            "cass search query=auth --json",
            "cass search auth --json",
            true,
            "A bare query=<value> assignment is converted to the required positional query.",
        ),
        mistake_recovery_capability(
            "cass view --path session.jsonl --line 42 --json",
            "cass view session.jsonl --line 42 --json",
            true,
            "A named path option is converted to the required positional path for drill-down commands.",
        ),
        mistake_recovery_capability(
            "cass view session.jsonl:42 --json",
            "cass view session.jsonl --line 42 --json",
            true,
            "A grep-style path:line argument is split into the positional path plus --line.",
        ),
        mistake_recovery_capability(
            "cass view session.jsonl line=42 --json",
            "cass view session.jsonl --line 42 --json",
            true,
            "A bare line=<value> assignment is converted to the drill-down --line option.",
        ),
        mistake_recovery_capability(
            "cass view session.jsonl --line-number 42 --json",
            "cass view session.jsonl --line 42 --json",
            true,
            "Legacy --line-number/--line_number flags still inspect raw lines; use --message-index for search hits.",
        ),
        mistake_recovery_capability(
            "cass view source_path=session.jsonl source_id=local line_number=42 --json",
            "cass view session.jsonl --source local --message-index 42 --json",
            true,
            "Pasted line_number fields select canonical messages, not raw lines; include conversation_id when a source path contains multiple conversations.",
        ),
        mistake_recovery_capability(
            "cass search auth --format json",
            "cass search auth --robot-format json",
            true,
            "Familiar structured-output spellings are converted to cass's robot-format flag on robot-capable commands.",
        ),
        mistake_recovery_capability(
            "cass search auth --output json",
            "cass search auth --robot-format json",
            true,
            "Output-format aliases such as --output json and --output-format json are converted to cass's robot-format flag on robot-capable commands.",
        ),
        mistake_recovery_capability(
            "cass --format json status",
            "cass status --robot-format json",
            true,
            "A leading structured-output format request is moved onto the robot-capable subcommand.",
        ),
        mistake_recovery_capability(
            "cass search auth --max-results 5 --json",
            "cass search auth --limit 5 --json",
            true,
            "A common result-count alias is converted to the canonical limit flag.",
        ),
        mistake_recovery_capability(
            "cass search auth --max_results 5 --json",
            "cass search auth --limit 5 --json",
            true,
            "Snake_case long flags are normalized to kebab-case before alias recovery runs.",
        ),
        mistake_recovery_capability(
            "cass search auth max_results=5 --json",
            "cass search auth --limit 5 --json",
            true,
            "A bare result-count assignment is converted to the canonical limit flag.",
        ),
        mistake_recovery_capability(
            "cass search auth agent=codex days=7 fields=minimal --json",
            "cass search auth --agent codex --days 7 --fields minimal --json",
            true,
            "Known bare filter assignments are converted to canonical long flags.",
        ),
        mistake_recovery_capability(
            "cass search auth last=7d before=now --json",
            "cass search auth --since -7d --until now --json",
            true,
            "Familiar time-window assignments are converted to canonical since/until filters.",
        ),
        mistake_recovery_capability(
            "cass search auth --last 7 --before now --json",
            "cass search auth --since -7d --until now --json",
            true,
            "Familiar time-window alias flags are converted to canonical since/until filters.",
        ),
        mistake_recovery_capability(
            "cass search auth --provider codex --json",
            "cass search auth --agent codex --json",
            true,
            "Provider/tool/connector filter aliases are converted to the canonical agent filter.",
        ),
        mistake_recovery_capability(
            "cass search auth provider codex limit 5 last 7d --json",
            "cass search auth --agent codex --limit 5 --since -7d --json",
            true,
            "Bare filter key/value pairs after a query are converted before remaining words are folded into the query.",
        ),
        mistake_recovery_capability(
            "cass search --agent codex --limit 5 auth error --json",
            "cass search \"auth error\" --agent codex --limit 5 --json",
            true,
            "A query placed after leading search/pack filters is moved back to the required query positional.",
        ),
        mistake_recovery_capability(
            "cass search auth -n 5 --json",
            "cass search auth --limit 5 --json",
            true,
            "A familiar short result-count spelling is converted to the canonical limit flag.",
        ),
    ]
}

/// Discover the first-stop self-description contract for agent introspection.
pub(crate) fn run_capabilities(output_format: Option<RobotFormat>) -> CliResult<()> {
    let meta = crate::dependency_pin_correlation::build_metadata::BuildMetadata::current();
    let crate_version = env!("CARGO_PKG_VERSION").to_string();
    let response = CapabilitiesResponse {
        version: crate_version.clone(),
        crate_version,
        build_commit: meta.build_commit.unwrap_or("unknown").to_string(),
        build_commit_date: meta.build_commit_date.unwrap_or("unknown").to_string(),
        source_ingest_reuse: SourceIngestReuseCapability::compiled_with_metadata(meta),
        api_version: 1,
        contract_version: CONTRACT_VERSION.to_string(),
        features: vec![
            "json_output".to_string(),
            "jsonl_output".to_string(),
            "robot_meta".to_string(),
            "time_filters".to_string(),
            "field_selection".to_string(),
            "content_truncation".to_string(),
            "aggregations".to_string(),
            "wildcard_fallback".to_string(),
            "timeout".to_string(),
            "cursor_pagination".to_string(),
            "request_id".to_string(),
            "answer_pack_command".to_string(),
            "dry_run".to_string(),
            "session_retention_selectors".to_string(),
            "durable_session_exclusions".to_string(),
            "exclusion_listing".to_string(),
            "explicit_session_reinclusion".to_string(),
            "raw_capture_session_exclusions".to_string(),
            "query_explain".to_string(),
            "view_command".to_string(),
            "status_command".to_string(),
            "state_command".to_string(),
            "doctor_v2_robot_contract".to_string(),
            "doctor_v2_response_schemas".to_string(),
            "doctor_v2_redacted_examples".to_string(),
            "doctor_v2_fingerprint_repairs".to_string(),
            "doctor_archive_first_safety".to_string(),
            "api_version_command".to_string(),
            "introspect_command".to_string(),
            "triage_command".to_string(),
            "export_command".to_string(),
            "expand_command".to_string(),
            "timeline_command".to_string(),
            "highlight_matches".to_string(),
            "self_describing_capabilities".to_string(),
        ],
        connectors: capabilities_connector_names(),
        global_flags: build_global_flag_schemas(),
        commands: build_command_schemas(),
        exit_codes: build_exit_code_capabilities(),
        env_vars: build_env_var_capabilities(),
        workflows: build_workflow_capabilities(),
        mistake_recoveries: build_mistake_recovery_capabilities(),
        limits: CapabilitiesLimits {
            max_limit: 0,          // 0 = no hard cap
            max_content_length: 0, // 0 = unlimited
            max_fields: 50,
            max_agg_buckets: 10,
        },
    };

    let structured_format = output_format.or_else(robot_format_from_env).map(|fmt| {
        // sessions is search-only; for other commands treat it as compact JSON.
        if matches!(fmt, RobotFormat::Sessions) {
            RobotFormat::Compact
        } else {
            fmt
        }
    });

    if let Some(fmt) = structured_format {
        let payload = serde_json::to_value(&response).unwrap_or_default();
        return output_structured_value(payload, fmt);
    }

    // Human-readable output
    println!("CASS Capabilities");
    println!("=================");
    println!();
    println!(
        "Version: {} (api v{}, contract v{})",
        response.crate_version, response.api_version, response.contract_version
    );
    if response.build_commit != "unknown" {
        println!(
            "Build commit: {} ({})",
            response.build_commit, response.build_commit_date
        );
    }
    match &response.source_ingest_reuse.disabled_by {
        None => println!("Completed-source reuse: on"),
        Some(blocker) => println!("Completed-source reuse: off ({blocker})"),
    }
    println!();
    println!("Features:");
    for feature in &response.features {
        println!("  - {feature}");
    }
    println!();
    println!("Connectors:");
    for connector in &response.connectors {
        println!("  - {connector}");
    }
    println!();
    println!("Commands: {}", response.commands.len());
    println!("Global flags: {}", response.global_flags.len());
    println!("Workflow recipes: {}", response.workflows.len());
    println!("Mistake recoveries: {}", response.mistake_recoveries.len());
    println!();
    println!("Exit Codes:");
    for exit_code in &response.exit_codes {
        println!(
            "  {}: {} (retryable: {})",
            exit_code.code, exit_code.meaning, exit_code.retryable
        );
    }
    println!();
    println!("Environment Variables:");
    for env_var in &response.env_vars {
        match &env_var.default {
            Some(default) => println!("  {} (default: {})", env_var.name, default),
            None => println!("  {}", env_var.name),
        }
    }
    println!();
    println!("Limits:");
    println!("  max_limit: {}", response.limits.max_limit);
    println!(
        "  max_content_length: {} (0 = unlimited)",
        response.limits.max_content_length
    );
    println!("  max_fields: {}", response.limits.max_fields);
    println!("  max_agg_buckets: {}", response.limits.max_agg_buckets);

    Ok(())
}
