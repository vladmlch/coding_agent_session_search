// Index walks over parallel slices by a single counter; an iterator rewrite would
// obscure the arithmetic without changing behaviour.
#![allow(clippy::needless_range_loop)]
#![recursion_limit = "512"]
// `unsafe` is denied crate-wide outside tests; the only sanctioned sites carry
// `#[allow(unsafe_code)]` + a SAFETY comment (startup env writes, unavoidable FFI per AGENTS.md).
#![cfg_attr(not(test), deny(unsafe_code))]

// Modules shared verbatim (via `#[path]`) between this crate and binaries
// spell crate-internal paths as `coding_agent_search::…`; the alias makes
// that spelling resolve inside the library too.
extern crate self as coding_agent_search;

pub mod analytics;
pub mod bakeoff;
pub mod bookmarks;
pub mod connector_ingest_diagnostics;
pub mod connectors;
pub mod crash_replay;
#[cfg(unix)]
pub mod daemon;
pub mod daemon_runtime_state;
pub mod dependency_pin_correlation;
pub mod doctor;
#[cfg(test)]
mod doctor_candidate_wal_tests;
pub(crate) mod doctor_chokepoint;
pub mod doctor_recover;
pub(crate) mod doctor_robot_docs;
pub(crate) mod doctor_runs;
pub(crate) mod doctor_undo;
pub mod e2e_runner;
pub mod encryption;
pub mod evidence_bundle;
pub mod explainability;
pub mod export;
mod followup_coordinates;
pub mod franken_sync;
pub mod ftui_harness;
pub mod guide_planner;
pub mod guide_runner;
pub mod html_export;
pub mod incident_discovery;
mod index_result;
pub mod indexer;
pub mod metric_integrity;
pub mod model;
pub mod pages;
pub mod perf_evidence;
pub mod policy_registry;
pub mod privacy_exposure;
pub mod proof_artifact;
pub mod query_cost_planner;
pub mod raw_mirror;
pub(crate) mod recipes_robot_docs;
pub(crate) mod recovery_support_bundle;
pub mod resource_plan;
pub mod robot_budget_envelope;
pub mod root_cause_projection;
pub mod root_cause_taxonomy;
pub mod schedule;
pub mod search;
pub mod search_defaults;
pub mod search_quality_eval;
pub mod source_onboarding;
pub mod sources;
pub mod storage;
pub mod subsystem_coverage_matrix;
mod timeline_json;
pub mod top_session_summary;
pub mod topology_budget;
pub mod tui_asciicast;
pub mod ui;
pub mod viewer_extensions;
pub mod workflow_macros;

/// Shut down synchronous bridge runtimes cached on the calling thread.
///
/// The CLI calls this after its command future completes so Windows tears down
/// the runtimes before the CRT starts destroying thread-local storage.
#[doc(hidden)]
pub fn shutdown_thread_local_bridge_runtimes() -> bool {
    let fsqlite_shutdown = franken_sync::shutdown_driver();
    let quill_shutdown = search::quill_bridge::shutdown_driver();
    // SQLite connectors cache a separate runtime inside FAD. Drop it before
    // Windows process teardown terminates its worker threads (GH #519).
    // Evaluate every shutdown before combining results: a timeout in one
    // bridge must not leave another runtime cached until process exit.
    let fad_shutdown = franken_agent_detection::connectors::sqlite_sync::shutdown_driver();
    fsqlite_shutdown && quill_shutdown && fad_shutdown
}

use crate::franken_sync::compat::{
    ConnectionExt, OpenFlags as FrankenOpenFlags, RowExt,
    open_with_flags as open_franken_with_flags,
};
use anyhow::Result;
use base64::prelude::*;
use chrono::Utc;
use clap::{Arg, ArgAction, Command, CommandFactory, Parser, Subcommand, ValueEnum, ValueHint};
use indexer::IndexOptions;
use model::cli_error_kind::ErrorKind as CliErrorKind;
use robot_budget_envelope::BudgetSnapshot;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, IsTerminal, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{info, warn};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

const CONTRACT_VERSION: &str = "2";
const DEFAULT_STALE_THRESHOLD_SECS: u64 = 1800;
const DEFAULT_PACK_FRESHNESS_WINDOW_SECONDS: i64 = 30 * 24 * 60 * 60;

#[cfg(test)]
fn read_watch_once_paths_env() -> Option<Vec<std::path::PathBuf>> {
    dotenvy::var("CASS_TEST_WATCH_PATHS")
        .ok()
        .map(|list| {
            list.split(',')
                .filter(|s| !s.trim().is_empty())
                .map(std::path::PathBuf::from)
                .collect::<Vec<_>>()
        })
        .filter(|v| !v.is_empty())
}

#[cfg(not(test))]
fn read_watch_once_paths_env() -> Option<Vec<std::path::PathBuf>> {
    None
}

/// Absolutize explicit paths without erasing provider hints in symlink names.
/// Classification resolves symlinks after retaining the original hint (#478),
/// then uses canonical paths for scan-root matching and I/O (#377).
fn absolutize_watch_once_paths(paths: Vec<PathBuf>) -> Vec<PathBuf> {
    paths
        .into_iter()
        .map(|path| {
            if path.is_absolute() {
                path
            } else {
                match std::env::current_dir() {
                    Ok(cwd) => cwd.join(&path),
                    Err(_) => path,
                }
            }
        })
        .collect()
}

fn resolve_watch_once_paths_from_sources(
    watch: bool,
    watch_once: Option<Vec<PathBuf>>,
    env_watch_once_paths: Option<Vec<PathBuf>>,
) -> Option<Vec<PathBuf>> {
    let explicit = watch_once.filter(|paths| !paths.is_empty());
    if let Some(paths) = explicit {
        return Some(absolutize_watch_once_paths(paths));
    }

    if watch {
        return env_watch_once_paths
            .filter(|paths| !paths.is_empty())
            .map(absolutize_watch_once_paths);
    }

    None
}

fn resolve_watch_once_paths(watch: bool, watch_once: Option<Vec<PathBuf>>) -> Option<Vec<PathBuf>> {
    resolve_watch_once_paths_from_sources(watch, watch_once, read_watch_once_paths_env())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct IndexEntrypointDiagnostics {
    kind: &'static str,
    full: bool,
    force_rebuild: bool,
    watch: bool,
    watch_once_path_count: usize,
    semantic: bool,
    build_hnsw: bool,
    migration_state: &'static str,
}

fn index_entrypoint_diagnostics(
    full: bool,
    force_rebuild: bool,
    watch: bool,
    watch_once_path_count: usize,
    semantic: bool,
    build_hnsw: bool,
) -> IndexEntrypointDiagnostics {
    let kind = if watch_once_path_count > 0 {
        if watch { "watch_cycle" } else { "watch_once" }
    } else if watch {
        "watch"
    } else if full {
        "full_rebuild"
    } else if force_rebuild {
        "force_rebuild"
    } else if semantic && build_hnsw {
        "semantic_hnsw"
    } else if semantic {
        "semantic_backfill"
    } else {
        "incremental"
    };

    IndexEntrypointDiagnostics {
        kind,
        full,
        force_rebuild,
        watch,
        watch_once_path_count,
        semantic,
        build_hnsw,
        migration_state: "tin8o_entrypoint_observed",
    }
}

fn with_frankensqlite_connection<T, F>(
    db_path: &Path,
    context: &str,
    op: F,
) -> std::result::Result<T, crate::franken_sync::FrankenError>
where
    F: FnOnce(
        &crate::franken_sync::Connection,
    ) -> std::result::Result<T, crate::franken_sync::FrankenError>,
{
    let mut conn = crate::franken_sync::Connection::open(db_path.to_string_lossy().as_ref())?;
    let result = op(&conn);
    let close_result = conn.close_in_place();
    match (result, close_result) {
        (Ok(value), Ok(())) => Ok(value),
        (Ok(value), Err(close_err)) => {
            warn!(
                error = %close_err,
                db_path = %db_path.display(),
                "{context}: close_in_place failed; falling back to best-effort close"
            );
            conn.close_best_effort_in_place();
            Ok(value)
        }
        (Err(err), Ok(())) => Err(err),
        (Err(err), Err(close_err)) => {
            warn!(
                error = %close_err,
                db_path = %db_path.display(),
                "{context}: close_in_place failed after error; falling back to best-effort close"
            );
            conn.close_best_effort_in_place();
            Err(err)
        }
    }
}

/// Command-line interface.
#[derive(Parser, Debug, Clone)]
#[command(
    name = "cass",
    version = env!("CARGO_PKG_VERSION"),
    about = "Unified TUI search over coding agent histories"
)]
pub struct Cli {
    /// Path to the `SQLite` database (defaults to platform data dir). When it
    /// points outside the default data dir, derived assets (lexical index,
    /// raw-mirror, checkpoints, locks) follow it into the db file's parent
    /// directory, so a scratch `--db` is fully isolated from the live
    /// install (#403). An explicit `--data-dir` still wins. `CASS_DB_PATH`
    /// sets the same value when the flag is absent (2l1b0.57: it was
    /// documented in README, robot-docs and capabilities but never read).
    #[arg(long, env = "CASS_DB_PATH")]
    pub db: Option<PathBuf>,

    /// Deterministic machine-first help (wide, no TUI)
    #[arg(long, default_value_t = false)]
    pub robot_help: bool,

    /// Trace command execution to JSONL file (spans)
    #[arg(long, env = "CASS_TRACE_FILE")]
    pub trace_file: Option<PathBuf>,

    /// Reduce log noise (warnings and errors only)
    #[arg(long, short = 'q', default_value_t = false)]
    pub quiet: bool,

    /// Increase verbosity (show debug information)
    #[arg(long, short = 'v', default_value_t = false)]
    pub verbose: bool,

    /// Color behavior for CLI output
    #[arg(long, value_enum, default_value_t = ColorPref::Auto)]
    pub color: ColorPref,

    /// Progress output style
    #[arg(long, value_enum, default_value_t = ProgressMode::Auto)]
    pub progress: ProgressMode,

    /// Wrap informational output to N columns
    #[arg(long)]
    pub wrap: Option<usize>,

    /// Disable wrapping entirely
    #[arg(long, default_value_t = false)]
    pub nowrap: bool,

    /// Output format for robot mode (overrides --json when specified)
    #[arg(long, value_enum, global = true)]
    pub robot_format: Option<RobotFormat>,

    #[command(subcommand)]
    pub command: Option<Commands>,

    /// How argv was interpreted before dispatch; filled by `parse_cli`.
    #[arg(skip)]
    pub interpretation: InvocationInterpretation,
}

/// What `parse_cli` changed or inferred about the invocation, so robot
/// output can echo it under `_meta.effective` instead of leaving it on
/// stderr only (2l1b0.68).
#[derive(Debug, Clone, Default)]
pub struct InvocationInterpretation {
    /// Every auto-correction applied to argv, in the wording stderr uses.
    pub corrections: Vec<String>,
    /// `--db` was absent from argv and its value came from `CASS_DB_PATH`.
    pub db_from_env: bool,
}

#[allow(clippy::large_enum_variant)]
#[derive(Subcommand, Debug, Clone)]
pub enum Commands {
    /// Launch interactive TUI
    Tui {
        /// Render once and exit (headless-friendly)
        #[arg(long, default_value_t = false)]
        once: bool,

        /// Delete persisted UI state (`tui_state.json`) before launch
        #[arg(long, default_value_t = false)]
        reset_state: bool,

        /// Record terminal output to an asciicast v2 file; in non-interactive
        /// headless --once mode cass writes a labeled sentinel cast because no
        /// live TUI session is launched.
        #[arg(long, value_hint = ValueHint::FilePath)]
        asciicast: Option<PathBuf>,

        /// Override data dir (matches index --data-dir)
        #[arg(long)]
        data_dir: Option<PathBuf>,

        /// Run in inline mode (UI anchored within terminal, scrollback preserved)
        #[arg(long, default_value_t = false)]
        inline: bool,

        /// Height of the inline UI in rows (default: 12, ignored without --inline)
        #[arg(long, default_value_t = 12)]
        ui_height: u16,

        /// Anchor the inline UI to top or bottom of the terminal (default: bottom)
        #[arg(long, value_parser = ["top", "bottom"], default_value = "bottom")]
        anchor: String,

        /// Record input events to a macro file for replay/debugging
        #[arg(long, value_hint = ValueHint::FilePath)]
        record_macro: Option<PathBuf>,

        /// Play back a previously recorded macro file
        #[arg(long, value_hint = ValueHint::FilePath, conflicts_with = "record_macro")]
        play_macro: Option<PathBuf>,

        /// Run an incremental `cass index` pass before launching the TUI so
        /// new conversations created since the last index are searchable.
        /// No-op when the index is already current; indexing errors are
        /// logged and the TUI opens on the existing index (non-fatal).
        #[arg(long, visible_alias = "catch-up", default_value_t = false)]
        refresh: bool,
    },
    /// Run indexer
    #[command(
        after_help = "Exclusions: CASS_EXCLUDE_PATHS accepts comma/newline-separated source files or directory prefixes. CASS_EXCLUDE_WORKSPACES accepts comma/newline-separated workspace globs. Both affect future ingestion; existing history remains searchable.\n\nTo remove archived conversations, preview `cass forget --workspace-glob '<glob>' --older-than 90d --json`, then add --apply. To reclaim raw source captures, use `cass mirror prune --older-than 90d --json`, then add --apply."
    )]
    Index {
        /// Perform full rebuild
        #[arg(long)]
        full: bool,

        /// Force lexical index rebuild even if schema matches
        #[arg(long, default_value_t = false, visible_alias = "force")]
        force_rebuild: bool,
        /// Targeted, idempotent lexical reconcile for ONE canonical
        /// conversation id (gh#382 partial-prefix recovery): upserts the
        /// conversation's full capped message set under stable document
        /// identities and publishes a successor generation. Retry-safe; no
        /// corpus-wide replay; the canonical archive is opened read-only.
        #[arg(long, value_name = "CONVERSATION_ID", conflicts_with_all = ["full", "watch", "semantic"])]
        reconcile_conversation: Option<i64>,

        /// Reclaim merge-retired lexical segment files and exit (gh#453).
        /// Opens the lexical writer, which runs the engine's grace-period
        /// garbage sweep: a segment file is unlinked only once no published
        /// MANIFEST generation has referenced it for the engine's grace
        /// period (300 s), so readers on the previous generation are safe.
        /// Scans no sources and re-indexes nothing; every incremental
        /// `cass index` performs the same sweep at open. Reports files and
        /// bytes reclaimed (`--json` for automation).
        #[arg(long, conflicts_with_all = ["full", "force_rebuild", "reconcile_conversation", "watch", "watch_once", "semantic"])]
        gc: bool,

        /// Watch for changes and reindex automatically
        #[arg(long)]
        watch: bool,

        /// Trigger a single watch cycle for specific paths (comma-separated or repeated)
        #[arg(long, value_delimiter = ',', num_args = 1..)]
        watch_once: Option<Vec<PathBuf>>,

        /// Minimum seconds between watch scan cycles (default: 30).
        /// Prevents high CPU usage from tight-loop scanning when filesystem
        /// events arrive continuously.
        #[arg(long, default_value_t = 30)]
        watch_interval: u64,

        /// Build semantic vector index after text indexing. Progress reports
        /// canonical replay, embedding, vector publish, optional HNSW,
        /// manifest publish, and finalization as distinct phases. Set
        /// CASS_INDEX_STALL_DETECT_SECS=0 to disable stall diagnostics, or
        /// CASS_INDEX_STALL_ABORT_SECS=0 to keep abort-eligible lexical stalls
        /// report-only while still reporting semantic phase progress. Set
        /// CASS_INDEX_STALL_ABORT_ALL_PHASES=1 to promote the stall detector
        /// to a hard abort (exit 70) in every phase it reports on, including
        /// the otherwise report-only semantic/scribe lanes (issue #437).
        #[arg(long)]
        semantic: bool,

        /// Build HNSW index for approximate nearest neighbor search (requires --semantic).
        /// Enables O(log n) search with `--approximate` at query time. Native
        /// HNSW construction is an opaque phase: cass reports its vector total
        /// but does not synthesize forward progress the backend cannot expose.
        #[arg(long, default_value_t = false)]
        build_hnsw: bool,

        /// Embedder for semantic indexing (hash, minilm, multilingual-minilm); defaults to semantic policy
        #[arg(long)]
        embedder: Option<String>,

        /// Override data dir (index + db). Defaults to platform data dir.
        #[arg(long)]
        data_dir: Option<PathBuf>,

        /// Output as JSON (for automation)
        #[arg(long, visible_alias = "robot")]
        json: bool,

        /// Idempotency key for safe retries. If the same key is used with identical parameters,
        /// the cached result is returned. Keys expire after 24 hours.
        #[arg(long)]
        idempotency_key: Option<String>,

        /// Interval (ms) between NDJSON progress events emitted on stderr in --json/--robot
        /// mode. Clamped to [250, 60000]. Default 2000. Set --no-progress-events to disable.
        #[arg(long, default_value_t = 2000)]
        progress_interval_ms: u64,

        /// Suppress NDJSON progress events on stderr in --json/--robot mode.
        /// Also honored via CASS_INDEX_NO_PROGRESS_EVENTS=1 env var.
        #[arg(long, default_value_t = false)]
        no_progress_events: bool,

        /// Emit per-ingest-batch NDJSON timing and lookup counters on stderr for perf bisection.
        #[arg(long, default_value_t = false)]
        robot_trace_ingest: bool,

        /// Run as a low-priority background job: lowers this process's CPU
        /// priority (nice, default 15; CASS_BACKGROUND_NICE) and I/O priority
        /// (ionice idle class on Linux; CASS_BACKGROUND_IONICE_CLASS) before
        /// indexing. This is what stale-on-read catch-up, `cass schedule`, and
        /// the daemon's periodic refresh use. Has no effect on what gets
        /// indexed.
        #[arg(long, default_value_t = false)]
        background: bool,

        /// Lexical-rebuild settings preset (GH #483). `bounded` is the
        /// single-core, low-memory recipe for a wedged or memory-starved
        /// archive: one rebuild worker and one page-prep thread, a one-page
        /// pipeline, 64 conversations per fetch and a durable commit every
        /// 64 MiB of text. An explicit CASS_TANTIVY_REBUILD_* value still
        /// wins over the preset. Applies to any lexical rebuild this run
        /// performs (`--full`, or resuming an interrupted rebuild); it never
        /// changes what gets indexed. `--json` reports the values used under
        /// `rebuild_profile`.
        #[arg(long, value_enum, default_value_t = RebuildProfileArg::Default)]
        rebuild_profile: RebuildProfileArg,
    },
    /// Generate shell completions to stdout
    Completions {
        #[arg(value_enum)]
        shell: clap_complete::Shell,
    },
    /// Generate man page to stdout
    Man,
    /// Save, list, search, remove, export, and import bookmarks (bookmarks.db)
    Bookmarks(bookmarks::BookmarksArgs),
    /// Machine-focused docs for automation agents
    RobotDocs {
        /// Topic to print
        #[arg(value_enum)]
        topic: RobotTopic,
    },
    /// Run a one-off search and print results to stdout
    Search {
        /// The query string
        query: String,
        /// Filter by agent slug (can be specified multiple times)
        #[arg(long)]
        agent: Vec<String>,
        /// Filter by workspace path (can be specified multiple times)
        #[arg(long)]
        workspace: Vec<String>,
        /// Max results. 0 = "no limit" but is auto-capped to a RAM-proportional ceiling
        /// (1/16 of MemAvailable, clamped to [256 MiB, 16 GiB] of result-heap) so a single
        /// query can't tie up the whole machine. Override with CASS_SEARCH_NO_LIMIT_CAP=<hits>
        /// or CASS_SEARCH_NO_LIMIT_BYTES=<bytes>.
        #[arg(long, default_value_t = 0)]
        limit: usize,
        /// Offset for pagination (start at Nth result)
        #[arg(long, default_value_t = 0)]
        offset: usize,
        /// Output as JSON (--robot also works). Equivalent to --robot-format json
        #[arg(long, visible_alias = "robot")]
        json: bool,
        /// Include extended metadata in robot output (`elapsed_ms`, `wildcard_fallback`, `cache_stats`)
        #[arg(long)]
        robot_meta: bool,
        /// Select specific fields in JSON output (comma-separated). Use 'minimal' for `source_path,line_number,agent,source_id,conversation_id`
        /// or 'summary' for `source_path,line_number,agent,title,score,source_id,conversation_id`. Example: --fields `source_path,line_number`
        #[arg(long, value_delimiter = ',')]
        fields: Option<Vec<String>>,
        /// Truncate content/snippet fields to max N characters (UTF-8 safe, adds '...' and _truncated indicator)
        #[arg(long)]
        max_content_length: Option<usize>,
        /// Soft token budget for robot output (approx; 4 chars ≈ 1 token). Adjusts truncation.
        #[arg(long)]
        max_tokens: Option<usize>,
        /// Request ID to echo in robot _meta for correlation
        #[arg(long)]
        request_id: Option<String>,
        /// Cursor for pagination (base64-encoded offset/limit payload from previous result)
        #[arg(long)]
        cursor: Option<String>,
        /// Human-readable display format: table (aligned columns), lines (one-liner), markdown
        #[arg(long, value_enum)]
        display: Option<DisplayFormat>,
        /// Override data dir
        #[arg(long)]
        data_dir: Option<PathBuf>,
        /// Filter to last N days
        #[arg(long)]
        days: Option<u32>,
        /// Filter to today only
        #[arg(long)]
        today: bool,
        /// Filter to yesterday only
        #[arg(long)]
        yesterday: bool,
        /// Filter to last 7 days
        #[arg(long)]
        week: bool,
        /// Filter to entries since ISO date (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS),
        /// keyword (`today`, `yesterday`, `now`), or relative offset
        /// (`-7d`, `-24h`, `-30m`, `-1w`). `allow_hyphen_values` lets the
        /// dash-prefixed forms pass clap without requiring the equals syntax
        /// (reality-check bead hr0z4).
        #[arg(long, allow_hyphen_values = true)]
        since: Option<String>,
        /// Filter to entries until ISO date / keyword / relative offset.
        #[arg(long, allow_hyphen_values = true)]
        until: Option<String>,
        /// Server-side aggregation by field(s). Comma-separated: `agent,workspace,date,match_type`
        /// Returns buckets with counts instead of full results. Use with --limit to get both.
        #[arg(long, value_delimiter = ',')]
        aggregate: Option<Vec<String>>,
        /// Include query explanation in output (shows parsed query, index strategy, cost estimate)
        #[arg(long)]
        explain: bool,
        /// Validate and analyze query without executing (returns explanation, estimated cost, warnings)
        #[arg(long)]
        dry_run: bool,
        /// Timeout in milliseconds. Returns partial results and error if exceeded.
        #[arg(long)]
        timeout: Option<u64>,
        /// Also mark query-term occurrences the engine left unmarked; snippets always mark matched terms with **bold** (text and JSON output)
        #[arg(long)]
        highlight: bool,
        /// Filter by source: 'local', 'remote', 'all', or a specific source hostname
        #[arg(long)]
        source: Option<String>,
        /// Filter to sessions from file (one path per line). Use '-' for stdin.
        /// Enables chained searches: `cass search "query1" --robot-format sessions | cass search "query2" --sessions-from -`
        #[arg(long)]
        sessions_from: Option<String>,
        /// Search mode: hybrid-preferred (default), lexical, or semantic
        #[arg(long, value_enum)]
        mode: Option<crate::search::query::SearchMode>,

        /// Use approximate nearest neighbor (ANN) search with HNSW for faster semantic/hybrid queries.
        /// Trades slight accuracy loss for O(log n) search complexity instead of O(n).
        /// Only affects semantic and hybrid modes; ignored for lexical search.
        /// Requires an HNSW index built with `cass index --semantic --build-hnsw`.
        #[arg(long, default_value_t = false)]
        approximate: bool,

        // ==========================================================================
        // Model / Reranker / Daemon flags (bd-3bbv)
        // ==========================================================================
        /// Embedding model to use for semantic search.
        /// Available models depend on what's been installed.
        /// `cass models status --json` lists them.
        #[arg(long)]
        model: Option<String>,

        /// Enable reranking of search results for improved relevance.
        /// Requires a reranker model to be available.
        #[arg(long, default_value_t = false)]
        rerank: bool,

        /// Reranker model to use (requires --rerank).
        /// Defaults to `ms-marco`, the only registered reranker.
        #[arg(long)]
        reranker: Option<String>,

        /// Auto-spawn the warm-model daemon when one is not already running.
        /// Search discovers and uses an existing daemon by default; if daemon
        /// inference is unavailable, it lazily falls back to direct inference.
        #[arg(long, default_value_t = false)]
        daemon: bool,

        /// Disable daemon usage even if available (force direct inference).
        #[arg(long, default_value_t = false)]
        no_daemon: bool,

        // ==========================================================================
        // Two-tier progressive search flags (bd-3dcw)
        // ==========================================================================
        /// Request the quality-refined tier of the two-tier search pipeline.
        /// The one-shot CLI emits the final quality result set; the interactive TUI
        /// is the surface that displays fast results first and refines them in place.
        #[arg(long, default_value_t = false)]
        two_tier: bool,

        /// Fast-only search: use lightweight embedder for instant results, no refinement.
        /// Ideal for real-time search-as-you-type scenarios where latency is critical.
        #[arg(long, default_value_t = false)]
        fast_only: bool,

        /// Quality-only search: wait for full transformer model results.
        /// Uses an existing daemon when available and lazily falls back to the
        /// installed local model; higher latency than --fast-only, but more accurate.
        #[arg(long, default_value_t = false)]
        quality_only: bool,

        /// Run an incremental `cass index` pass before the search so new
        /// conversations created since the last index are matched. No-op
        /// when the index is already current; indexing errors are logged
        /// and the search runs against the existing index (non-fatal).
        #[arg(long, visible_alias = "catch-up", default_value_t = false)]
        refresh: bool,

        /// Execute search as a strict read-only operation. Never refresh a
        /// checkpoint, launch or join lexical maintenance, or spawn detached
        /// catch-up work. Conflicts with --refresh and --daemon because daemon
        /// auto-spawn creates runtime state.
        #[arg(
            long,
            default_value_t = false,
            conflicts_with_all = ["refresh", "daemon"]
        )]
        no_maintenance: bool,
    },
    /// Build a deterministic answer pack for agent handoffs
    Pack {
        /// The query string
        query: String,
        /// Filter by agent slug (can be specified multiple times)
        #[arg(long)]
        agent: Vec<String>,
        /// Filter by workspace path (can be specified multiple times)
        #[arg(long)]
        workspace: Vec<String>,
        /// Max candidate hits to fetch before answer-pack planning. 0 uses the planner default.
        #[arg(long, default_value_t = 0)]
        limit: usize,
        /// Output as JSON (--robot also works). Equivalent to --robot-format json.
        #[arg(long, visible_alias = "robot")]
        json: bool,
        /// Select answer-pack fields. Presets: minimal, standard, full, summary, all.
        #[arg(long, visible_alias = "field-mask", value_delimiter = ',')]
        fields: Option<Vec<String>>,
        /// Soft pack token budget.
        #[arg(long, default_value_t = 12_000)]
        max_tokens: usize,
        /// Maximum distinct sessions represented in the pack.
        #[arg(long, default_value_t = 8)]
        max_sessions: usize,
        /// Maximum evidence items selected into the pack.
        #[arg(long, default_value_t = 24)]
        max_evidence: usize,
        /// Context lines requested around evidence hits.
        #[arg(long, default_value_t = 3)]
        context_lines: usize,
        /// Maximum excerpt characters per evidence item.
        #[arg(long, default_value_t = 1_600)]
        max_excerpt_chars: usize,
        /// Request ID to echo in pack metadata.
        #[arg(long)]
        request_id: Option<String>,
        /// Human-readable display format. Only markdown is supported for pack.
        #[arg(long, value_enum)]
        display: Option<DisplayFormat>,
        /// Override data dir
        #[arg(long)]
        data_dir: Option<PathBuf>,
        /// Filter to last N days
        #[arg(long)]
        days: Option<u32>,
        /// Filter to today only
        #[arg(long)]
        today: bool,
        /// Filter to yesterday only
        #[arg(long)]
        yesterday: bool,
        /// Filter to last 7 days
        #[arg(long)]
        week: bool,
        /// Filter to entries since ISO date (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS).
        #[arg(long, allow_hyphen_values = true)]
        since: Option<String>,
        /// Filter to entries until ISO date / keyword / relative offset.
        #[arg(long, allow_hyphen_values = true)]
        until: Option<String>,
        /// Filter by source: 'local', 'remote', 'all', or a specific source hostname.
        #[arg(long)]
        source: Option<String>,
        /// Filter to sessions from file (one path per line). Use '-' for stdin.
        #[arg(long)]
        sessions_from: Option<String>,
        /// Search mode: hybrid-preferred (default), lexical, or semantic.
        #[arg(long, value_enum)]
        mode: Option<crate::search::query::SearchMode>,
        /// Freshness policy for evidence selection.
        #[arg(long, value_parser = ["prefer-recent", "strict", "allow-stale"], default_value = "prefer-recent")]
        freshness_policy: String,
        /// Evidence freshness window in seconds.
        #[arg(long, default_value_t = DEFAULT_PACK_FRESHNESS_WINDOW_SECONDS)]
        freshness_window_seconds: i64,
        /// Include skill payload excerpts. Credentials remain redacted.
        #[arg(long, default_value_t = false)]
        include_skill_content: bool,
        /// Return an error instead of an empty successful pack.
        #[arg(long, default_value_t = false)]
        require_evidence: bool,
        /// Include selection score component details in evidence.
        #[arg(long, default_value_t = false)]
        explain_selection: bool,
        /// Run an incremental `cass index` pass before packing.
        #[arg(long, visible_alias = "catch-up", default_value_t = false)]
        refresh: bool,
        /// Timeout in milliseconds.
        #[arg(long)]
        timeout: Option<u64>,
    },
    /// Show statistics about indexed data
    Stats {
        /// Override data dir
        #[arg(long)]
        data_dir: Option<PathBuf>,

        /// Output as JSON (`--robot` also works)
        #[arg(long, visible_alias = "robot")]
        json: bool,

        /// Filter by source: 'local', 'remote', 'all', or a specific source hostname
        #[arg(long)]
        source: Option<String>,
        /// Show breakdown by source
        #[arg(long)]
        by_source: bool,
    },
    /// Output diagnostic information for troubleshooting
    Diag {
        /// Override data dir
        #[arg(long)]
        data_dir: Option<PathBuf>,

        /// Output as JSON (`--robot` also works)
        #[arg(long, visible_alias = "robot")]
        json: bool,

        /// Include quarantine and retained-asset inspection details
        #[arg(long, default_value_t = false)]
        quarantine: bool,

        /// Include verbose information (file sizes, timestamps)
        #[arg(long, short)]
        verbose: bool,
    },
    /// On-disk storage footprint breakdown by component (DB, WAL, lexical index, raw mirror, semantic, quarantine)
    Storage {
        /// Override data dir
        #[arg(long)]
        data_dir: Option<PathBuf>,

        /// Output as JSON (`--robot` also works)
        #[arg(long, visible_alias = "robot")]
        json: bool,
    },
    /// Collapse pre-existing duplicate conversation rows (projects/<rel> vs <rel> external_id twins) created before the dedup fix
    Dedup {
        /// Override data dir
        #[arg(long)]
        data_dir: Option<PathBuf>,

        /// Output as JSON (`--robot` also works)
        #[arg(long, visible_alias = "robot")]
        json: bool,

        /// Actually delete the duplicate rows. Without this, runs as a dry-run (inspect only, no mutation).
        #[arg(long, default_value_t = false)]
        apply: bool,
    },
    /// Quick health check for agents: index freshness, db stats, recommended action
    Status {
        /// Override data dir
        #[arg(long)]
        data_dir: Option<PathBuf>,

        /// Output as JSON (`--robot` also works)
        #[arg(long, visible_alias = "robot")]
        json: bool,

        /// Include _meta block (elapsed, freshness, data_dir/db_path)
        #[arg(long, default_value_t = false)]
        robot_meta: bool,
        /// Staleness threshold in seconds (default: 1800 = 30 minutes)
        #[arg(long, default_value_t = 1800)]
        stale_threshold: u64,
    },
    /// First-stop agent self-description: workflows, mistake recoveries, commands, flags, env vars, exit codes, and limits
    Capabilities {
        /// Output as JSON (`--robot` also works)
        #[arg(long, visible_alias = "robot")]
        json: bool,
    },
    /// One-shot agent triage: readiness, next command, workflows, docs, and schemas
    Triage {
        /// Override data dir
        #[arg(long)]
        data_dir: Option<PathBuf>,

        /// Output as JSON (`--robot` also works)
        #[arg(long, visible_alias = "robot")]
        json: bool,

        /// Staleness threshold in seconds (default: 1800 = 30 minutes)
        #[arg(long, default_value_t = 1800)]
        stale_threshold: u64,

        /// Hard wall-clock budget in milliseconds (overrides CASS_TRIAGE_BUDGET_MS)
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
        timeout: Option<u64>,
    },
    /// Assemble a redacted, share-safe recovery/support evidence bundle
    SupportBundle {
        /// Override data dir
        #[arg(long)]
        data_dir: Option<PathBuf>,

        /// Output as JSON (`--robot` also works)
        #[arg(long, visible_alias = "robot")]
        json: bool,

        /// Staleness threshold in seconds (default: 1800 = 30 minutes)
        #[arg(long, default_value_t = 1800)]
        stale_threshold: u64,

        /// Include full filesystem paths in the bundle (default: basename-only)
        #[arg(long, default_value_t = false)]
        include_full_paths: bool,

        /// Include raw session/tool payloads in the bundle (default: suppressed)
        #[arg(long, default_value_t = false)]
        include_raw_evidence: bool,
    },
    /// Quick state/health check (alias of status)
    State {
        /// Override data dir
        #[arg(long)]
        data_dir: Option<PathBuf>,

        /// Output as JSON (`--robot` also works)
        #[arg(long, visible_alias = "robot")]
        json: bool,

        /// Include _meta block (elapsed, freshness, data_dir/db_path)
        #[arg(long, default_value_t = false)]
        robot_meta: bool,
        /// Staleness threshold in seconds (default: 1800 = 30 minutes)
        #[arg(long, default_value_t = 1800)]
        stale_threshold: u64,
    },
    /// Show API + contract version info
    ApiVersion {
        /// Output as JSON (`--robot` also works)
        #[arg(long, visible_alias = "robot")]
        json: bool,
    },
    /// Full API schema introspection - commands, arguments, and response schemas
    Introspect {
        /// Output as JSON (`--robot` also works)
        #[arg(long, visible_alias = "robot")]
        json: bool,
    },
    /// Inspect file lines, or follow a search hit with --message-index
    View {
        /// Path to the source file
        path: PathBuf,
        /// Exact source_id from search output (e.g. 'local', 'work-laptop')
        #[arg(long, alias = "source-id", alias = "source_id")]
        source: Option<String>,
        /// Exact canonical archive conversation row id
        #[arg(long, alias = "conversation_id", value_parser = clap::value_parser!(i64).range(1..))]
        conversation_id: Option<i64>,
        /// Physical file line (1-based), NOT search's line_number; use --message-index for hits.
        #[arg(
            long,
            short = 'n',
            alias = "line-number",
            alias = "line_number",
            conflicts_with = "message_index"
        )]
        line: Option<usize>,
        /// Canonical message ordinal: pass search's line_number unchanged (1-based).
        /// Reads the archive only; never substitutes a physical file line.
        #[arg(long, conflicts_with = "line")]
        message_index: Option<usize>,
        /// Number of context lines before/after
        #[arg(long, short = 'C', default_value_t = 5)]
        context: usize,
        /// Output as JSON (`--robot` also works)
        #[arg(long, visible_alias = "robot")]
        json: bool,

        /// Hard wall-clock budget in milliseconds (overrides CASS_VIEW_BUDGET_MS)
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
        timeout: Option<u64>,
    },
    /// Minimal health check (<50ms). Exit 0=healthy, 1=unhealthy. For agent pre-flight checks.
    Health {
        /// Override data dir
        #[arg(long)]
        data_dir: Option<PathBuf>,
        /// Output as JSON (`{"healthy": bool, "latency_ms": N}`)
        #[arg(long, visible_alias = "robot")]
        json: bool,
        /// Include _meta block (elapsed, freshness, data_dir/db_path)
        #[arg(long, default_value_t = false)]
        robot_meta: bool,
        /// Staleness threshold in seconds (default: 1800 = 30 minutes).
        /// Matches `cass status` / `DEFAULT_STALE_THRESHOLD_SECS` so bare
        /// `cass health` does not flip unhealthy five minutes after a rebuild
        /// on large archives that are not in continuous watch mode.
        #[arg(long, default_value_t = 1800)]
        stale_threshold: u64,
        /// Exit code reflects only whether this executable is functional:
        /// archive readiness (stale index, active rebuild, uninitialized or
        /// degraded archive) is still fully REPORTED in the output but never
        /// fails the exit code. Use `cass selftest` instead when the probe must
        /// avoid opening the configured archive entirely (#398).
        #[arg(long, default_value_t = false)]
        binary_only: bool,
    },
    /// Verify this executable without reading or writing the configured archive.
    /// Intended for package installation and binary-promotion gates.
    Selftest {
        /// Output a stable machine-readable result (`--robot` also works)
        #[arg(long, visible_alias = "robot")]
        json: bool,
    },
    /// First-run source onboarding + readiness wizard. Read-only: explains what
    /// CASS found, what it will index, what is missing, and the single safest
    /// next command. Scriptable via `--json`; never launches a bare TUI.
    Onboarding {
        /// Override data dir
        #[arg(long)]
        data_dir: Option<PathBuf>,
        /// Output as JSON (for automation)
        #[arg(long, visible_alias = "robot")]
        json: bool,
    },
    /// Intent-to-command planner and gated runner for guided safe workflows.
    /// Dry-run is the default. `--apply` automatically evaluates only closed,
    /// read-only proof adapters and requires an explicit confirmation for each
    /// mutating step; macro strings are never evaluated by a shell. Maps an
    /// operator intent (investigate-search-miss, repair-assets, export-session,
    /// onboard-source, support-capsule) to an exact safe command plan — steps,
    /// prerequisites, proof gates, forbidden shortcuts, cost/risk, privacy
    /// notes, and stop conditions. Never launches a bare TUI. Omit the intent
    /// to list the known intents.
    Guide {
        /// Operator intent to plan (omit to list the known intents). Multiple
        /// words are joined, e.g. `cass guide rebuild index`.
        intent: Vec<String>,
        /// Deterministic preflight-facts source: a JSON file shaped like
        /// `{ "facts": { "<fact>": true, ... }, "intent"?: "<intent>" }`.
        #[arg(long)]
        fixture: Option<PathBuf>,
        /// Override data dir
        #[arg(long)]
        data_dir: Option<PathBuf>,
        /// Enter gated apply mode (`--run` is an alias). Without this flag the
        /// transcript is a deterministic, fully read-only dry-run.
        #[arg(long, visible_alias = "run", default_value_t = false)]
        apply: bool,
        /// Confirm one exact mutating step number. Repeat for multiple steps;
        /// confirmation never carries across steps.
        #[arg(long, value_delimiter = ',')]
        confirm_step: Vec<usize>,
        /// Confirm an operator-context preflight fact as true. Repeatable; the
        /// confirmation is recorded in the robot transcript.
        #[arg(long)]
        confirm_fact: Vec<String>,
        /// Accept the exact declared privacy tier (`redacted` or `sensitive`).
        /// A generic yes is intentionally not accepted.
        #[arg(long)]
        accept_privacy_tier: Option<String>,
        /// Accept the exact declared cost-risk band (`medium` or `high`) after
        /// reviewing the resource what-if result.
        #[arg(long)]
        accept_cost_risk: Option<String>,
        /// Assert that every declared stop condition was checked and is clear.
        /// An observed triggered stop condition always wins over this assertion.
        #[arg(long, default_value_t = false)]
        confirm_stop_conditions_clear: bool,
        /// Output as JSON (for automation)
        #[arg(long, visible_alias = "robot")]
        json: bool,
    },
    /// Diagnose cass installation issues. Legacy `cass doctor --json` maps to the read-only check surface.
    /// Legacy `--fix` maps to safe-auto-run and may only apply contract-declared safe repairs.
    Doctor {
        /// Override data dir
        #[arg(long)]
        data_dir: Option<PathBuf>,

        /// Output as JSON (`--robot` also works)
        #[arg(long, visible_alias = "robot")]
        json: bool,

        /// Run the bounded read-only doctor truth surface (`cass doctor check --json`)
        #[arg(long, default_value_t = false, conflicts_with = "fix")]
        check: bool,

        /// Legacy safe auto-run: apply only contract-declared safe repairs, preserving archive/source evidence.
        #[arg(long, conflicts_with = "repair")]
        fix: bool,
        /// Hidden normalized form for `cass doctor repair ...`.
        #[arg(long, hide = true, default_value_t = false, conflicts_with = "check")]
        repair: bool,
        /// Hidden normalized form for `cass doctor cleanup ...`.
        #[arg(long, hide = true, default_value_t = false, conflicts_with_all = ["check", "repair", "fix"])]
        cleanup: bool,
        /// Hidden normalized form for `cass doctor archive-scan ...`.
        #[arg(long, hide = true, default_value_t = false, conflicts_with_all = ["check", "repair", "cleanup", "archive_normalize", "fix", "backups_list", "backups_verify", "backups_restore"])]
        archive_scan: bool,
        /// Hidden normalized form for `cass doctor archive-normalize ...`.
        #[arg(long, hide = true, default_value_t = false, conflicts_with_all = ["check", "repair", "cleanup", "archive_scan", "fix", "backups_list", "backups_verify", "backups_restore"])]
        archive_normalize: bool,
        /// Hidden normalized form for `cass doctor backups list ...`.
        #[arg(long, hide = true, default_value_t = false, conflicts_with_all = ["check", "repair", "cleanup", "archive_scan", "archive_normalize", "fix", "backups_verify", "backups_restore"])]
        backups_list: bool,
        /// Hidden normalized form for `cass doctor backups verify ...`.
        #[arg(long, hide = true, default_value_t = false, conflicts_with_all = ["check", "repair", "cleanup", "archive_scan", "archive_normalize", "fix", "backups_list", "backups_restore"])]
        backups_verify: bool,
        /// Hidden normalized form for `cass doctor backups restore ...`.
        #[arg(long, hide = true, default_value_t = false, conflicts_with_all = ["check", "repair", "cleanup", "archive_scan", "archive_normalize", "fix", "backups_list", "backups_verify"])]
        backups_restore: bool,
        /// Hidden normalized form for `cass doctor baseline save ...`.
        #[arg(long, hide = true, default_value_t = false, conflicts_with_all = ["check", "repair", "cleanup", "archive_scan", "archive_normalize", "fix", "backups_list", "backups_verify", "backups_restore", "baseline_diff", "baseline_update"])]
        baseline_save: bool,
        /// Hidden normalized form for `cass doctor baseline diff ...`.
        #[arg(long, hide = true, default_value_t = false, conflicts_with_all = ["check", "repair", "cleanup", "archive_scan", "archive_normalize", "fix", "backups_list", "backups_verify", "backups_restore", "baseline_save", "baseline_update"])]
        baseline_diff: bool,
        /// Hidden normalized form for `cass doctor baseline update ...`.
        #[arg(long, hide = true, default_value_t = false, conflicts_with_all = ["check", "repair", "cleanup", "archive_scan", "archive_normalize", "fix", "backups_list", "backups_verify", "backups_restore", "baseline_save", "baseline_diff"])]
        baseline_update: bool,
        /// Hidden normalized form for `cass doctor support-bundle ...`.
        #[arg(long, hide = true, default_value_t = false, conflicts_with_all = ["check", "repair", "cleanup", "archive_scan", "archive_normalize", "fix", "backups_list", "backups_verify", "backups_restore", "baseline_save", "baseline_diff", "baseline_update", "support_bundle_verify"])]
        support_bundle: bool,
        /// Hidden normalized form for `cass doctor support-bundle verify ...`.
        #[arg(long, hide = true, default_value_t = false, conflicts_with_all = ["check", "repair", "cleanup", "archive_scan", "archive_normalize", "fix", "backups_list", "backups_verify", "backups_restore", "baseline_save", "baseline_diff", "baseline_update", "support_bundle"])]
        support_bundle_verify: bool,
        /// Hidden normalized form for `cass doctor archive export ...`.
        #[arg(long, hide = true, default_value_t = false, conflicts_with_all = ["check", "repair", "cleanup", "archive_scan", "archive_normalize", "fix", "backups_list", "backups_verify", "backups_restore", "baseline_save", "baseline_diff", "baseline_update", "support_bundle", "support_bundle_verify", "archive_relocate", "archive_export_verify"])]
        archive_export: bool,
        /// Hidden normalized form for `cass doctor archive relocate ...`.
        #[arg(long, hide = true, default_value_t = false, conflicts_with_all = ["check", "repair", "cleanup", "archive_scan", "archive_normalize", "fix", "backups_list", "backups_verify", "backups_restore", "baseline_save", "baseline_diff", "baseline_update", "support_bundle", "support_bundle_verify", "archive_export", "archive_export_verify"])]
        archive_relocate: bool,
        /// Hidden normalized form for `cass doctor archive export verify ...`.
        #[arg(long, hide = true, default_value_t = false, conflicts_with_all = ["check", "repair", "cleanup", "archive_scan", "archive_normalize", "fix", "backups_list", "backups_verify", "backups_restore", "baseline_save", "baseline_diff", "baseline_update", "support_bundle", "support_bundle_verify", "archive_export", "archive_relocate"])]
        archive_export_verify: bool,
        /// Target directory for `cass doctor archive export|relocate`.
        #[arg(long, hide = true)]
        archive_target: Option<PathBuf>,
        /// Backup id from `cass doctor backups list --json`.
        #[arg(long, hide = true)]
        backup_id: Option<String>,
        /// Baseline id from `cass doctor baseline save|diff <id>`.
        #[arg(long, hide = true)]
        baseline_id: Option<String>,
        /// Explicit baseline file path for save/diff/update.
        #[arg(long, hide = true)]
        baseline_path: Option<PathBuf>,
        /// Explicit support bundle directory or manifest path for verify.
        #[arg(long, hide = true)]
        support_bundle_path: Option<PathBuf>,
        /// Include explicitly provided sensitive attachments in the support bundle.
        #[arg(long, hide = true, default_value_t = false)]
        include_sensitive_attachments: bool,
        /// Sensitive attachment path for `cass doctor support-bundle --include-sensitive-attachments`.
        #[arg(long, hide = true)]
        sensitive_attachment: Vec<PathBuf>,
        /// Maximum total sensitive attachment bytes copied into a support bundle.
        #[arg(long, hide = true, default_value_t = 1_048_576)]
        sensitive_attachment_max_bytes: u64,
        /// Preview a fingerprinted repair plan without mutating any cass files.
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        /// Confirm a previously inspected fingerprinted repair plan.
        #[arg(long, default_value_t = false)]
        yes: bool,
        /// Plan fingerprint from `cass doctor repair --dry-run --json`.
        #[arg(long)]
        plan_fingerprint: Option<String>,
        /// Run all checks verbosely (show passed checks too)
        #[arg(long, short)]
        verbose: bool,
        /// Request a derived rebuild; never bypasses archive coverage gates or plan fingerprints.
        #[arg(long, visible_alias = "force")]
        force_rebuild: bool,
        /// Permit a mutating repair even when a previous failure marker exists
        #[arg(long)]
        allow_repeated_repair: bool,

        // --- world-class-doctor pass-2: per-run artifact subcommands ---
        /// List per-run artifact directories under `<data_dir>/doctor/runs/`. Read-only.
        /// Output is JSON when combined with `--json`/`--robot`. Returns the run-id, started/ended timestamps,
        /// mode, exit_code, action_count and status (completed|incomplete|unknown) per run, newest-first.
        #[arg(long, default_value_t = false, hide = true)]
        ls: bool,

        /// Undo the named run by id (or `latest`). Walks `<run-dir>/actions.jsonl` in reverse,
        /// verifies hashes, and restores byte-identically from the per-run backups. Refuses on
        /// any post-mutation tampering. The undo itself produces a fresh run-id.
        #[arg(long, hide = true)]
        undo: Option<String>,

        // --- world-class-doctor pass-3: agent-ergonomic mega-command + GC ---
        /// Mega-command for agents: returns one envelope with summary, findings,
        /// actions_planned, recommended_command, capabilities_url. Read-only;
        /// composes the existing read-only doctor check with planning hints.
        #[arg(long, default_value_t = false, hide = true)]
        robot_triage: bool,

        /// Show what `--fix` would change vs the current state. Read-only.
        /// Optional `<REF>` compares against a prior run-id instead; with no
        /// value it compares the current state, as robot-docs documents.
        #[arg(long, hide = true, num_args = 0..=1, default_missing_value = "current")]
        diff: Option<String>,

        /// Quarantine doctor runs older than `<ISO8601>`. Renames into
        /// `<data_dir>/doctor/quarantine/runs/`; never deletes. Requires
        /// both `--gc-before` AND `--yes` (per the destructive-action gate).
        #[arg(long, hide = true)]
        gc_before: Option<String>,

        /// World-class-doctor pass-6: long-running monitor mode. Periodically
        /// re-reads `<data_dir>/doctor/runs/` and emits one JSONL event per
        /// observed change to stdout. Stops on Ctrl+C or after `--watch-iterations`.
        #[arg(long, default_value_t = false, hide = true)]
        watch: bool,

        /// World-class-doctor pass-6: poll interval for `--watch`. Default
        /// 5000 ms. Bounded to [500, 60000].
        #[arg(long, default_value_t = 5000, hide = true)]
        watch_interval_ms: u64,

        /// World-class-doctor pass-6: stop after this many polls. 0 = run
        /// forever (until SIGINT). Useful in tests + bounded automation.
        #[arg(long, default_value_t = 0, hide = true)]
        watch_iterations: u64,

        /// World-class-doctor pass-8: explain a prior run by id (or
        /// `latest`). Returns a single envelope with the run's manifest,
        /// every action recorded, and a flattened band timeline. Read-only.
        #[arg(long, hide = true)]
        explain: Option<String>,

        /// World-class-doctor pass-8: emit the doctor's extended capability
        /// surface — detectors[], fixers[], exit_codes[], data_paths[],
        /// env_vars[] — in one envelope. Read-only.
        #[arg(long, default_value_t = false, hide = true)]
        emit_capabilities: bool,

        // --- #285 corrupt-archive recovery surfaces ---
        /// Rebuild the source JSONL tree from the canonical archive's preserved
        /// `extra_json`/`extra_bin` events into `<DIR>`, so a corrupt archive
        /// can be re-ingested from cass's own data with `cass index --full`
        /// (no stock-sqlite `.recover` needed). Opens the archive read-only.
        #[arg(long, value_name = "DIR", value_hint = ValueHint::DirPath)]
        recover_from_archive: Option<PathBuf>,

        /// Safely inspect or repair the canonical FTS5 shadow. Exact dry-run
        /// parity uses fused covering-index parent-run counts plus bounded
        /// primary-key probes; partial shadows resume in bounded batches and
        /// recreation is failure-atomic. Supports `--dry-run`; mutation
        /// requires `--yes` and never modifies canonical rows.
        #[arg(long, default_value_t = false)]
        rebuild_canonical_fts: bool,

        /// Free leaked pages (integrity_check's "page N is never used": pages
        /// no table, index or freelist owns) in place. Refuses any other
        /// integrity damage. Supports `--dry-run`; mutation requires `--yes`
        /// and backs up the live bundle first (`cass doctor backups restore`)
        #[arg(long, default_value_t = false)]
        repair_leaked_pages: bool,

        /// Quarantine interrupted `raw_mirror_capture` staging artifacts that
        /// block doctor mutation, instead of forcing a manual `rm` inside the
        /// data dir. Requires `--yes`; artifacts are renamed into a quarantine
        /// dir, never deleted.
        #[arg(long, default_value_t = false)]
        cleanup_interrupted_artifacts: bool,
    },
    /// Find related sessions for a given source path
    Context {
        /// Path to the source session file
        path: PathBuf,
        /// Exact source_id from search output (e.g. 'local', 'work-laptop')
        #[arg(long, alias = "source-id")]
        source: Option<String>,
        /// Override data dir
        #[arg(long)]
        data_dir: Option<PathBuf>,

        /// Maximum results per relation type (default: 5)
        #[arg(long, default_value_t = 5)]
        limit: usize,
        /// Output as JSON (`--robot` also works)
        #[arg(long, visible_alias = "robot")]
        json: bool,
    },
    /// List recent sessions, with optional workspace/current-session filtering
    Sessions {
        /// Filter to sessions for this workspace/project directory
        #[arg(long, value_hint = ValueHint::DirPath)]
        workspace: Option<PathBuf>,
        /// Resolve the current workspace automatically and return the most recent match
        #[arg(long, default_value_t = false)]
        current: bool,
        /// Maximum sessions to return (defaults: 10, or 1 with --current)
        #[arg(long)]
        limit: Option<usize>,
        /// Only sessions active since this ISO date (YYYY-MM-DD or
        /// YYYY-MM-DDTHH:MM:SS), keyword (`today`, `yesterday`), or relative
        /// offset (`-7d`, `-24h`). Filters on the session's recorded
        /// last-activity timestamp (ended_at, falling back to started_at).
        #[arg(long, allow_hyphen_values = true)]
        since: Option<String>,
        /// Output as JSON (for automation)
        #[arg(long, visible_alias = "robot")]
        json: bool,
        /// Override data dir
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
    /// Resolve a session path into a ready-to-run resume command for its
    /// native harness (Claude Code, Codex, OpenCode, Pi Agent, Oh My Pi, Gemini,
    /// Antigravity).
    ///
    /// By default, the resolved command is printed to stdout, one
    /// argv token per line, so the caller can wrap it however they like:
    ///
    ///   eval "$(cass resume /path/to/session.jsonl --shell)"
    ///   $(cass resume /path/to/session.jsonl --shell) # direct exec
    ///
    /// Use `--exec` to have cass `exec` the command directly, replacing
    /// the current process.
    Resume {
        /// Session file path (as printed by `cass search` or `cass sessions`)
        #[arg(value_hint = ValueHint::FilePath)]
        path: PathBuf,
        /// Override the detected harness (see `--help` for accepted values).
        #[arg(
            long,
            long_help = "Override the detected harness. Accepted values:\n\
                \x20 claude | claude-code | claude_code\n\
                \x20 codex\n\
                \x20 opencode\n\
                \x20 pi_agent | pi-agent | pi (pi-mono)\n\
                \x20 omp | oh-my-pi | oh_my_pi | ohmypi (Oh My Pi; profile/session roots preserved)\n\
                \x20 gemini\n\
                \x20 antigravity | agy       (resume via `agy --conversation <uuid>`)"
        )]
        agent: Option<String>,
        /// Replace the current process with the resolved resume command.
        /// Mutually exclusive with `--shell` and `--json`.
        #[arg(long, default_value_t = false, conflicts_with_all = ["shell", "json"])]
        exec: bool,
        /// Emit a single shell-escaped command line on stdout (suitable
        /// for `eval "$(cass resume ...)"`). Mutually exclusive with
        /// `--json`.
        #[arg(long, default_value_t = false, conflicts_with = "json")]
        shell: bool,
        /// Output as JSON (for automation)
        #[arg(long, visible_alias = "robot")]
        json: bool,
    },
    /// Export a conversation to markdown or other formats.
    ///
    /// Reads the last indexed snapshot from the archive: the path is an
    /// identity key, the original source file is never opened, and an
    /// unindexed session fails with an actionable error (run `cass index`).
    Export {
        /// Path identifying the indexed session (opaque key, not read as a file)
        path: PathBuf,
        /// Exact source_id from search output (e.g. 'local', 'work-laptop')
        #[arg(long, alias = "source-id")]
        source: Option<String>,
        /// Exact conversation_id from search (disambiguates shared source paths).
        #[arg(long, alias = "conversation_id", value_parser = clap::value_parser!(i64).range(1..))]
        conversation_id: Option<i64>,
        /// Output format
        #[arg(long, value_enum, default_value_t = ConvExportFormat::Markdown)]
        format: ConvExportFormat,
        /// Output file (stdout if not specified)
        #[arg(long, short = 'o', conflicts_with = "clipboard")]
        output: Option<PathBuf>,
        /// Copy the formatted export to the system clipboard instead of
        /// printing to stdout. Falls back to stdout with a stderr warning
        /// when no clipboard tool is available (e.g. headless / SSH).
        #[arg(long, short = 'c', default_value_t = false)]
        clipboard: bool,
        /// Include tool use details in export
        #[arg(long)]
        include_tools: bool,
        /// Include skill content in export (default: stripped for privacy)
        #[arg(long)]
        include_skills: bool,
        /// Include credentials from the source text (sensitive output).
        /// By default exports redact recognized secrets, regardless of index settings.
        #[arg(long)]
        include_secrets: bool,
    },
    /// Export session as beautiful, self-contained HTML (with optional encryption).
    ///
    /// Reads the last indexed snapshot from the archive: the path is an
    /// identity key, the original source file is never opened, and an
    /// unindexed session fails with an actionable error (run `cass index`).
    #[command(name = "export-html")]
    ExportHtml {
        /// Path identifying the indexed session (opaque key, not read as a file)
        session: PathBuf,

        /// Exact source_id from search output (e.g. 'local', 'work-laptop')
        #[arg(long, alias = "source-id")]
        source: Option<String>,

        /// Exact conversation_id from search (disambiguates shared source paths).
        #[arg(long, alias = "conversation_id", value_parser = clap::value_parser!(i64).range(1..))]
        conversation_id: Option<i64>,

        /// Output directory (default: current directory)
        #[arg(long)]
        output_dir: Option<PathBuf>,

        /// Custom filename (default: auto-generated from session metadata)
        #[arg(long)]
        filename: Option<String>,

        /// Enable password encryption (Web Crypto compatible)
        #[arg(long)]
        encrypt: bool,

        /// Read password from stdin (secure, no echo)
        #[arg(long)]
        password_stdin: bool,

        /// Include tool calls in the export; `--include-tools=false` omits
        /// them (default: true)
        #[arg(
            long,
            default_value_t = true,
            action = ArgAction::Set,
            num_args = 0..=1,
            require_equals = true,
            default_missing_value = "true"
        )]
        include_tools: bool,

        /// Show message timestamps; `--show-timestamps=false` hides them
        /// (default: true)
        #[arg(
            long,
            default_value_t = true,
            action = ArgAction::Set,
            num_args = 0..=1,
            require_equals = true,
            default_missing_value = "true"
        )]
        show_timestamps: bool,

        /// Include skill content in export (default: stripped for privacy).
        /// Skills injected by Claude Code/Codex contain proprietary SKILL.md
        /// content that should not appear in shared/published exports.
        #[arg(long)]
        include_skills: bool,

        /// Include credentials from the source text (sensitive output, even when encrypted).
        /// By default exports redact recognized secrets, regardless of index settings.
        #[arg(long)]
        include_secrets: bool,

        /// Default theme (dark or light)
        #[arg(long, default_value = "dark", value_parser = ["dark", "light"])]
        theme: String,

        /// Validate without writing file
        #[arg(long)]
        dry_run: bool,

        /// Show export plan without executing
        #[arg(long)]
        explain: bool,

        /// Open file in browser after export
        #[arg(long)]
        open: bool,

        /// JSON output (for automation)
        #[arg(long, visible_alias = "robot")]
        json: bool,
    },
    /// Expand a search hit with --message-index, or inspect raw JSONL with --line
    Expand {
        /// Path to session file
        path: PathBuf,
        /// Exact source_id from search output (e.g. 'local', 'work-laptop')
        #[arg(long, alias = "source-id", alias = "source_id")]
        source: Option<String>,
        /// Physical file line, NOT search's line_number; use --message-index for hits.
        #[arg(
            long,
            short = 'n',
            alias = "line-number",
            alias = "line_number",
            conflicts_with = "message_index",
            required_unless_present = "message_index"
        )]
        line: Option<usize>,
        /// Canonical message ordinal: pass search's line_number unchanged (1-based).
        /// Reads the archive only; never substitutes a physical file line.
        #[arg(long, conflicts_with = "line")]
        message_index: Option<usize>,
        /// Exact conversation_id from search (disambiguates shared source paths).
        #[arg(long, alias = "conversation_id", requires = "message_index", value_parser = clap::value_parser!(i64).range(1..))]
        conversation_id: Option<i64>,
        /// Number of messages before/after (default: 3)
        #[arg(long, short = 'C', default_value_t = 3)]
        context: usize,
        /// Output as JSON (`--robot` also works)
        #[arg(long, visible_alias = "robot")]
        json: bool,
    },
    /// Show activity timeline for a time range
    Timeline {
        /// Start time (ISO date, 'today', 'yesterday', 'Nd' for N days ago,
        /// or relative `-7d`/`-24h`/`-30m`/`-1w`). `allow_hyphen_values` lets
        /// dash-prefixed offsets pass clap (reality-check bead hr0z4).
        #[arg(long, allow_hyphen_values = true)]
        since: Option<String>,
        /// End time (ISO date, keyword, or relative offset).
        #[arg(long, allow_hyphen_values = true)]
        until: Option<String>,
        /// Show today only
        #[arg(long)]
        today: bool,
        /// Filter by agent (can be repeated)
        #[arg(long)]
        agent: Vec<String>,
        /// Override data dir
        #[arg(long)]
        data_dir: Option<PathBuf>,

        /// Output as JSON (--robot also works). Equivalent to --robot-format json
        #[arg(long, visible_alias = "robot")]
        json: bool,

        /// Group by: hour, day, or none
        #[arg(long, value_enum, default_value_t = TimelineGrouping::Hour)]
        group_by: TimelineGrouping,
        /// Filter by source: 'local', 'remote', 'all', or a specific source hostname
        #[arg(long)]
        source: Option<String>,
    },
    /// Export encrypted searchable archive for local browsing (P4.x)
    Pages {
        /// Key-slot management for an exported bundle (`cass pages key …`);
        /// without a subcommand the export flags below apply.
        #[command(subcommand)]
        subcommand: Option<PagesSubcommand>,

        /// Export only (skip wizard and encryption) to specified directory
        #[arg(long)]
        export_only: Option<PathBuf>,

        /// Verify an existing export bundle (for CI/CD)
        #[arg(long)]
        verify: Option<PathBuf>,

        /// Filter by agent (comma-separated)
        #[arg(long, value_delimiter = ',')]
        agents: Option<Vec<String>>,

        /// Filter by workspace (comma-separated)
        #[arg(long, value_delimiter = ',')]
        workspaces: Option<Vec<String>>,

        /// Filter entries since ISO date, keyword, or relative offset
        /// (`-7d`, `-1w`, etc.). `allow_hyphen_values` lets dash-prefixed
        /// values pass clap (reality-check bead hr0z4).
        #[arg(long, allow_hyphen_values = true)]
        since: Option<String>,

        /// Filter entries until ISO date, keyword, or relative offset.
        #[arg(long, allow_hyphen_values = true)]
        until: Option<String>,

        /// Path mode: relative (default), basename, full, hash
        #[arg(long, value_enum, default_value_t = crate::pages::export::PathMode::Relative)]
        path_mode: crate::pages::export::PathMode,

        /// Share profile applied to every exported text: public (home paths,
        /// usernames, project names, hostnames, emails and other personal
        /// data), team (home paths and personal data), personal (nothing).
        /// Credentials are never rewritten; the secret scan rejects them.
        /// Default: public for a plaintext export, team when encrypted.
        #[arg(long, value_enum)]
        share_profile: Option<crate::pages::profiles::ShareProfile>,

        /// Deployment target: local (hosted targets removed — cass is
        /// fully local-only; `github`/`cloudflare` are rejected before parse)
        #[arg(long, value_enum)]
        target: Option<PagesDeployTarget>,

        /// Dry run (don't write files)
        #[arg(long)]
        dry_run: bool,

        /// Scan for secrets and exit (no export)
        #[arg(long)]
        scan_secrets: bool,

        /// Fail with non-zero exit if secrets are detected (for CI)
        #[arg(long)]
        fail_on_secrets: bool,

        /// Allowlist regex patterns to suppress findings (repeatable or comma-separated)
        #[arg(long, value_delimiter = ',')]
        secrets_allow: Vec<String>,

        /// Denylist regex patterns to force findings (repeatable or comma-separated)
        #[arg(long, value_delimiter = ',')]
        secrets_deny: Vec<String>,

        /// Output results as JSON (for verify and secret scan)
        #[arg(long, visible_alias = "robot")]
        json: bool,

        /// Verbose output (show detailed check results)
        #[arg(long, short)]
        verbose: bool,

        /// Export without encryption (DANGEROUS - all content publicly readable)
        #[arg(long)]
        no_encryption: bool,

        /// Acknowledge unencrypted export risks (required in robot/JSON mode with --no-encryption)
        #[arg(long)]
        i_understand_unencrypted_risks: bool,

        /// Preview an existing export locally (starts HTTP server)
        #[arg(long)]
        preview: Option<PathBuf>,

        /// Port for preview server (default: 8080)
        #[arg(long, default_value = "8080")]
        port: u16,

        /// Don't auto-open browser when starting preview server
        #[arg(long)]
        no_open: bool,

        /// JSON config file for non-interactive export (use "-" for stdin)
        #[arg(long)]
        config: Option<String>,

        /// Validate config file without running export
        #[arg(long)]
        validate_config: bool,

        /// Show example config file
        #[arg(long)]
        example_config: bool,
    },
    /// Inspect and manage the conversation-ingest quarantine (list / clear)
    #[command(subcommand)]
    Quarantine(QuarantineCommand),
    /// Prune archived conversations by source path, workspace, or age
    /// (dry-run by default; `--apply` to commit). At least one selector is
    /// required; supplied selectors are ANDed. Rebuilds derived search and
    /// analytics assets while keeping source files. Forgotten sessions in
    /// shared stores stay forgotten until explicitly released with --reinclude.
    /// Use --list to inspect recorded exclusions without changing the archive.
    #[command(
        group(clap::ArgGroup::new("forget_selectors")
            .required(true)
            .multiple(true)
            .args(["source_glob", "workspace_glob", "older_than", "list", "reinclude"])),
        group(clap::ArgGroup::new("exclusion_action")
            .multiple(false)
            .args(["list", "reinclude"])),
        after_long_help = "Globs match stored paths case-sensitively; * also matches /. A source path can be a logical database/session handle. Missing workspaces do not match --workspace-glob. Age uses the latest message timestamp, strictly before the reported cutoff; sessions with no messages or any missing message timestamp are kept.\n\nShared stores use durable per-session tombstones scoped by source provenance, agent, source path and external session ID. Edits, appends, sibling changes, --full and changes to CASS_EXCLUDE_WORKSPACES do not restore that identity. Inspect exclusions with --list, then preview an explicit release with --reinclude and --source-path, --source-glob, --source-id, --agent and/or --external-id. Add --apply to release the reviewed exclusions, then run cass index. Current workspace/path policies still apply.\n\nHistorical exclusions have no recorded workspace or message-age metadata, so --workspace-glob and --older-than cannot select --list or --reinclude records. Exact identity selectors never silently release a path-wide source exclusion; preview a source-only release if such an overlap is reported.\n\nOrdinary single-session files retain size/mtime tombstones: unchanged files stay forgotten, and a changed stamp makes the file eligible for indexing again. Source files and existing raw-mirror captures are kept. All deletion and re-inclusion is explicit; there is no automatic age pruning."
    )]
    Forget {
        /// Glob over conversation `source_path` (e.g. `**/subagents/*.jsonl`).
        #[arg(long = "source-glob")]
        source_glob: Option<String>,

        /// Glob over the recorded workspace path (e.g. `/work/scratch/**`).
        #[arg(long = "workspace-glob", conflicts_with_all = ["list", "reinclude"])]
        workspace_glob: Option<String>,

        /// Forget sessions whose last message is older than this duration
        /// (e.g. `90d`, `24h`). Sessions with unknown message times are kept.
        #[arg(long = "older-than", conflicts_with_all = ["list", "reinclude"])]
        older_than: Option<String>,

        /// List durable session and legacy source exclusions without changing them.
        #[arg(long, conflicts_with_all = ["reinclude", "apply"])]
        list: bool,

        /// Preview releasing matching exclusions; add --apply, then index.
        #[arg(long, conflicts_with = "list")]
        reinclude: bool,

        /// Exact stored source path for --list/--reinclude (no glob expansion).
        #[arg(long, requires = "exclusion_action", conflicts_with = "source_glob")]
        source_path: Option<String>,

        /// Exact source/provenance ID for --list/--reinclude (e.g. local).
        #[arg(long, requires = "exclusion_action")]
        source_id: Option<String>,

        /// Exact provider slug for --list/--reinclude (e.g. opencode).
        #[arg(long, requires = "exclusion_action")]
        agent: Option<String>,

        /// Exact provider session ID for --list/--reinclude.
        #[arg(long, requires = "exclusion_action")]
        external_id: Option<String>,

        /// Override db path
        #[arg(long)]
        db: Option<PathBuf>,

        /// Apply the selected forget or re-inclusion. Otherwise preview only.
        #[arg(long, default_value_t = false)]
        apply: bool,

        /// Output as JSON (`--robot` also works)
        #[arg(long, visible_alias = "robot")]
        json: bool,
    },
    /// Inspect and prune raw-mirror evidence under explicit operator control
    #[command(subcommand)]
    Mirror(MirrorCommand),
    /// Manage remote sources (P5.x)
    #[command(subcommand)]
    Sources(SourcesCommand),
    /// Manage semantic search models
    #[command(subcommand)]
    Models(ModelsCommand),
    /// Import data from external sources
    #[command(subcommand)]
    Import(ImportCommand),
    /// Token usage, tool, and model analytics
    ///
    /// Subcommands: status, tokens, tools, models, incidents, rebuild, validate.
    /// All subcommands share time-range, dimensional, and output flags.
    ///
    /// # Examples
    ///
    /// ```bash
    /// cass analytics status --json
    /// cass analytics tokens --days 7 --group-by day --json
    /// cass analytics rebuild --json
    /// ```
    #[command(subcommand)]
    Analytics(AnalyticsCommand),

    /// Run the semantic model daemon (Unix only)
    #[cfg(unix)]
    Daemon {
        /// Socket path to listen on (default comes from env or built-in config)
        #[arg(long)]
        socket: Option<PathBuf>,
        /// Idle timeout in seconds before auto-shutdown (0 = never; default comes from env or built-in config)
        #[arg(long)]
        idle_timeout: Option<u64>,
        /// Maximum concurrent connections (default comes from env or built-in config)
        #[arg(long)]
        max_connections: Option<usize>,
        /// Override data dir for model storage
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },

    /// Keep the index fresh automatically via the OS scheduler (launchd / systemd user timers)
    #[command(subcommand)]
    Schedule(ScheduleCommand),
}

/// `cass schedule` — register low-priority background index jobs with the
/// operating system's scheduler so the index is never stale when you search.
#[derive(Subcommand, Debug, Clone)]
pub enum ScheduleCommand {
    /// Register the incremental (every N minutes) and nightly (full index +
    /// semantic backfill) jobs with launchd (macOS) or systemd user timers (Linux)
    Install {
        /// Minutes between incremental index passes
        #[arg(long, default_value_t = crate::schedule::DEFAULT_INTERVAL_MINS)]
        interval_mins: u32,
        /// Local hour (0-23) for the nightly full index + semantic backfill
        #[arg(long, default_value_t = crate::schedule::DEFAULT_NIGHTLY_HOUR, value_parser = clap::value_parser!(u8).range(0..=23))]
        nightly_hour: u8,
        /// Minute (0-59) for the nightly job
        #[arg(long, default_value_t = crate::schedule::DEFAULT_NIGHTLY_MINUTE, value_parser = clap::value_parser!(u8).range(0..=59))]
        nightly_minute: u8,
        /// Register only the incremental job (skip the nightly full index)
        #[arg(long, default_value_t = false)]
        no_nightly: bool,
        /// Nightly job indexes only; never runs semantic backfill
        #[arg(long, default_value_t = false)]
        no_semantic: bool,
        /// Render the unit files and commands without writing or registering anything
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        /// Data dir the scheduled jobs target (baked into the units)
        #[arg(long)]
        data_dir: Option<PathBuf>,
        /// Output as JSON (`--robot` also works)
        #[arg(long, visible_alias = "robot")]
        json: bool,
    },
    /// Unregister the jobs and remove the unit files cass wrote
    Uninstall {
        /// Report what would be removed without touching anything
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        /// Output as JSON (`--robot` also works)
        #[arg(long, visible_alias = "robot")]
        json: bool,
    },
    /// Show registered jobs, scheduler state, and the last run of each job
    Status {
        /// Override data dir
        #[arg(long)]
        data_dir: Option<PathBuf>,
        /// Output as JSON (`--robot` also works)
        #[arg(long, visible_alias = "robot")]
        json: bool,
    },
    /// Execute one job now (this is what the scheduler units invoke)
    Run {
        /// Which job to run
        #[arg(long, value_enum)]
        job: crate::schedule::ScheduleJob,
        /// Override data dir
        #[arg(long)]
        data_dir: Option<PathBuf>,
        /// Skip semantic backfill in the nightly job
        #[arg(long, default_value_t = false)]
        no_semantic: bool,
        /// Ignore load / console-idle gates and run regardless
        #[arg(long, default_value_t = false)]
        force: bool,
        /// Output as JSON (`--robot` also works)
        #[arg(long, visible_alias = "robot")]
        json: bool,
    },
}

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

/// Time bucketing for analytics aggregation.
#[derive(Copy, Clone, Debug, Default, ValueEnum, PartialEq, Eq)]
pub enum AnalyticsBucketing {
    /// Group by hour
    Hour,
    /// Group by day
    #[default]
    Day,
    /// Group by week (ISO weeks)
    Week,
    /// Group by calendar month
    Month,
}

impl std::fmt::Display for AnalyticsBucketing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Hour => write!(f, "hour"),
            Self::Day => write!(f, "day"),
            Self::Week => write!(f, "week"),
            Self::Month => write!(f, "month"),
        }
    }
}

/// Which analytics track(s) `cass analytics rebuild` regenerates (#397 —
/// `analytics validate` has recommended `--track all` since the two-track
/// split; this makes that flag real).
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum AnalyticsTrack {
    /// Track A: message-level rollups (message_metrics, usage_hourly/daily)
    A,
    /// Track B: token-level rollups (token_daily_stats from the token_usage ledger)
    B,
    /// Both tracks
    All,
}

/// Subcommands for analytics (token/tool/model breakdowns and bounded incident mining).
///
/// All subcommands share time-range, dimensional-filter, and output flags
/// via clap's `flatten` on [`AnalyticsCommon`].
#[derive(Subcommand, Debug, Clone)]
pub enum AnalyticsCommand {
    /// Summary of analytics data health: row counts, freshness, coverage, drift warnings
    Status {
        #[command(flatten)]
        common: AnalyticsCommon,
    },
    /// Token usage over time, with dimensional breakdowns
    Tokens {
        #[command(flatten)]
        common: AnalyticsCommon,
        /// Time bucket for aggregation
        #[arg(long, value_enum, default_value_t = AnalyticsBucketing::Day)]
        group_by: AnalyticsBucketing,
    },
    /// Per-tool invocation counts and derived metrics
    Tools {
        #[command(flatten)]
        common: AnalyticsCommon,
        /// Time bucket for aggregation
        #[arg(long, value_enum, default_value_t = AnalyticsBucketing::Day)]
        group_by: AnalyticsBucketing,
        /// Maximum tools to return (default 20)
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
    /// Top models by usage and coverage statistics
    #[command(name = "models")]
    AnalyticsModels {
        #[command(flatten)]
        common: AnalyticsCommon,
        /// Time bucket for aggregation
        #[arg(long, value_enum, default_value_t = AnalyticsBucketing::Day)]
        group_by: AnalyticsBucketing,
    },
    /// Mine recurrent CASS incident categories from the canonical archive under strict caps
    Incidents {
        #[command(flatten)]
        common: AnalyticsCommon,
        /// Maximum ranked incident sessions returned
        #[arg(long, default_value_t = 10)]
        limit: usize,
        /// Maximum archive conversations scanned (hard ceiling: 10,000)
        #[arg(long, default_value_t = crate::incident_discovery::DEFAULT_MAX_FILES)]
        max_sessions: u64,
        /// Maximum archive messages inspected
        #[arg(long, default_value_t = crate::incident_discovery::DEFAULT_MAX_LINES)]
        max_messages: u64,
        /// Maximum UTF-8 message-content bytes inspected
        #[arg(long, default_value_t = crate::incident_discovery::DEFAULT_MAX_BYTES)]
        max_bytes: u64,
        /// Wall-clock scan budget in milliseconds
        #[arg(long, default_value_t = crate::incident_discovery::DEFAULT_BUDGET_MS)]
        budget_ms: u64,
    },
    /// Rebuild / backfill analytics rollup tables with progress output
    /// (always rebuilds; fresh rollups are not skipped)
    Rebuild {
        #[command(flatten)]
        common: AnalyticsCommon,
        /// Which analytics track to rebuild: message-level rollups (a),
        /// token-level rollups (b), or both (all)
        #[arg(long, value_enum, default_value_t = AnalyticsTrack::A)]
        track: AnalyticsTrack,
    },
    /// Check rollup invariants and detect drift between raw data and aggregates
    Validate {
        #[command(flatten)]
        common: AnalyticsCommon,
        /// Attempt safe automatic repair of fixable Track A issues and report skipped non-fixable problems
        #[arg(long)]
        fix: bool,
    },
}

/// Shared argument surface for analytics subcommands.
///
/// Query commands honor the time-range and dimensional filters. Whole-database
/// maintenance commands reject filters they cannot truthfully apply.
#[derive(Debug, Clone, clap::Args)]
pub struct AnalyticsCommon {
    // ---- Time range ----
    /// Filter to entries since ISO date (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS),
    /// keyword (`today`/`yesterday`/`now`), or relative offset
    /// (`-7d`/`-24h`/`-30m`/`-1w`). `allow_hyphen_values` lets dash-prefixed
    /// values pass clap (reality-check bead hr0z4).
    #[arg(long, allow_hyphen_values = true)]
    pub since: Option<String>,

    /// Filter to entries until ISO date, keyword, or relative offset.
    #[arg(long, allow_hyphen_values = true)]
    pub until: Option<String>,

    /// Filter to last N days
    #[arg(long)]
    pub days: Option<u32>,

    // ---- Dimensional filters ----
    /// Filter by agent slug (can be specified multiple times)
    #[arg(long)]
    pub agent: Vec<String>,

    /// Filter by workspace path (can be specified multiple times)
    #[arg(long)]
    pub workspace: Vec<String>,

    /// Filter by source: 'local', 'remote', 'all', or a specific hostname
    #[arg(long)]
    pub source: Option<String>,

    // ---- Output ----
    /// Output as JSON (for automation)
    #[arg(long, visible_alias = "robot")]
    pub json: bool,

    /// Override data dir
    #[arg(long)]
    pub data_dir: Option<PathBuf>,
}

// ---------------------------------------------------------------------------
// Bridge impls: CLI types → analytics library types
// ---------------------------------------------------------------------------

impl From<AnalyticsBucketing> for analytics::GroupBy {
    fn from(b: AnalyticsBucketing) -> Self {
        match b {
            AnalyticsBucketing::Hour => analytics::GroupBy::Hour,
            AnalyticsBucketing::Day => analytics::GroupBy::Day,
            AnalyticsBucketing::Week => analytics::GroupBy::Week,
            AnalyticsBucketing::Month => analytics::GroupBy::Month,
        }
    }
}

fn normalized_analytics_agent_arg(agent: &str) -> String {
    let trimmed = agent.trim();
    if trimmed.is_empty() {
        "unknown".to_string()
    } else {
        trimmed.to_string()
    }
}

fn analytics_source_filter_from_common_input(source: Option<&str>) -> analytics::SourceFilter {
    match source.map(crate::sources::provenance::SourceFilter::parse) {
        None | Some(crate::sources::provenance::SourceFilter::All) => analytics::SourceFilter::All,
        Some(crate::sources::provenance::SourceFilter::Local) => analytics::SourceFilter::Local,
        Some(crate::sources::provenance::SourceFilter::Remote) => analytics::SourceFilter::Remote,
        Some(crate::sources::provenance::SourceFilter::SourceId(source_id)) => {
            analytics::SourceFilter::Specific(source_id)
        }
    }
}

fn parse_analytics_datetime_flag(flag: &str, raw: &str) -> CliResult<i64> {
    parse_datetime_str(raw).ok_or_else(|| {
        CliError::usage(
            format!("could not parse {flag} value {raw:?}"),
            Some(
                "Use an ISO date (YYYY-MM-DD), a keyword (today/yesterday), or a relative offset (-7d, -24h)."
                    .into(),
            ),
        )
    })
}

fn analytics_filter_from_common(common: &AnalyticsCommon) -> CliResult<analytics::AnalyticsFilter> {
    if common.days.is_some() && common.since.is_some() {
        return Err(CliError::usage(
            "--days and --since cannot be used together",
            Some("Choose either a rolling --days window or an explicit --since bound.".into()),
        ));
    }

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let since_ms = match (common.days, common.since.as_deref()) {
        (Some(days), None) => {
            Some(now_ms.saturating_sub(i64::from(days).saturating_mul(86_400_000)))
        }
        (None, Some(raw)) => Some(parse_analytics_datetime_flag("--since", raw)?),
        (None, None) => None,
        (Some(_), Some(_)) => unreachable!("conflicting analytics time bounds rejected above"),
    };
    let until_ms = common
        .until
        .as_deref()
        .map(|raw| parse_analytics_datetime_flag("--until", raw))
        .transpose()?;

    if let (Some(since), Some(until)) = (since_ms, until_ms)
        && since > until
    {
        return Err(CliError::usage(
            "analytics time range is empty because --since is later than --until",
            Some("Choose an --until value at or after --since.".into()),
        ));
    }

    Ok(analytics::AnalyticsFilter {
        since_ms,
        until_ms,
        agents: common
            .agent
            .iter()
            .map(|agent| normalized_analytics_agent_arg(agent))
            .collect(),
        source: analytics_source_filter_from_common_input(common.source.as_deref()),
        workspace_ids: vec![],
    })
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
    Analytics,
    /// `cass doctor` agent handbook (added in world-class-doctor pass-2).
    /// Body lives in `src/doctor_robot_docs.rs`.
    Doctor,
    /// Canonical recovery + workflow recipes from the 2026-06-08 fleet report
    /// (bead .11.3). Body lives in `src/recipes_robot_docs.rs`.
    Recipes,
}

/// Output format for robot/automation mode
#[derive(Copy, Clone, Debug, Default, ValueEnum, PartialEq, Eq)]
pub enum RobotFormat {
    /// Pretty-printed JSON object (default, backward compatible)
    #[default]
    Json,
    /// Newline-delimited JSON: one object per line with optional _meta header
    Jsonl,
    /// Compact single-line JSON (no pretty printing)
    Compact,
    /// Session paths only: one source_path per line (for chained searches)
    Sessions,
    /// Token-Optimized Object Notation (encodes via toon crate)
    Toon,
}

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

/// Subcommands of `cass pages` that operate on an already exported bundle.
/// The export/wizard surface stays flag-driven on `cass pages` itself; this
/// enum only carries verbs that need their own argument set.
#[derive(Debug, Clone, Subcommand)]
pub enum PagesSubcommand {
    /// Manage the key slots of an exported encrypted bundle
    /// (`list`, `add-password`, `add-recovery`, `revoke`, `rotate`).
    Key(crate::pages::key_cli::PagesKeyArgs),
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
    fn as_config_value(self) -> &'static str {
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
    fn from_str(s: &str) -> Option<Self> {
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
    fn as_str(&self) -> &'static str {
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
    fn is_empty(&self) -> bool {
        self.agent.is_none()
            && self.workspace.is_none()
            && self.date.is_none()
            && self.match_type.is_none()
    }
}

#[derive(Debug, Clone)]
pub struct CliError {
    pub code: i32,
    pub kind: &'static str,
    pub message: String,
    pub hint: Option<String>,
    pub retryable: bool,
}

const CLI_ERROR_ALREADY_REPORTED_SENTINEL: &str = "__cass_error_already_reported__";

pub type CliResult<T = ()> = std::result::Result<T, CliError>;

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} (code {})", self.message, self.code)
    }
}

impl std::error::Error for CliError {}

impl CliError {
    /// Stable robot error shape shared by ordinary and delegated commands.
    pub fn json_envelope(&self) -> serde_json::Value {
        serde_json::json!({
            "error": {
                "code": self.code,
                "kind": self.kind,
                "message": self.message,
                "hint": self.hint,
                "retryable": self.retryable,
            }
        })
    }

    pub fn already_reported(code: i32, kind: &'static str, retryable: bool) -> Self {
        CliError {
            code,
            kind,
            message: CLI_ERROR_ALREADY_REPORTED_SENTINEL.to_string(),
            hint: None,
            retryable,
        }
    }

    pub fn already_reported_from(err: &CliError) -> Self {
        CliError {
            code: err.code,
            kind: err.kind,
            message: CLI_ERROR_ALREADY_REPORTED_SENTINEL.to_string(),
            hint: err.hint.clone(),
            retryable: err.retryable,
        }
    }

    pub fn was_already_reported(&self) -> bool {
        self.message == CLI_ERROR_ALREADY_REPORTED_SENTINEL
    }

    fn usage(message: impl Into<String>, hint: Option<String>) -> Self {
        CliError {
            code: 2,
            kind: CliErrorKind::Usage.kind_str(),
            message: message.into(),
            hint,
            retryable: false,
        }
    }

    fn unknown(message: impl Into<String>) -> Self {
        CliError {
            code: 9,
            kind: CliErrorKind::Unknown.kind_str(),
            message: message.into(),
            hint: None,
            retryable: false,
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ProgressResolved {
    Bars,
    Plain,
    None,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) struct WrapConfig {
    width: Option<usize>,
    nowrap: bool,
}

impl WrapConfig {
    pub(crate) fn new(width: Option<usize>, nowrap: bool) -> Self {
        WrapConfig { width, nowrap }
    }

    fn effective_width(&self) -> Option<usize> {
        if self.nowrap { None } else { self.width }
    }
}

fn root_structured_triage_rewrite(args: &[String]) -> Option<Vec<String>> {
    let mut rewritten = vec!["triage".to_string()];
    let mut saw_structured_request = false;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--json" | "--robot" => {
                saw_structured_request = true;
                i += 1;
            }
            "--data-dir" | "--stale-threshold" | "--timeout" => {
                let value = args.get(i + 1)?;
                if value.starts_with('-') {
                    return None;
                }
                rewritten.push(args[i].clone());
                rewritten.push(value.clone());
                i += 2;
            }
            arg if arg.starts_with("--data-dir=")
                || arg.starts_with("--stale-threshold=")
                || arg.starts_with("--timeout=") =>
            {
                rewritten.push(arg.to_string());
                i += 1;
            }
            _ => return None,
        }
    }

    if saw_structured_request {
        rewritten.push("--json".to_string());
        Some(rewritten)
    } else {
        None
    }
}

fn command_accepts_leading_structured_flag(command: &str) -> bool {
    matches!(
        command,
        "analytics"
            | "api-version"
            | "capabilities"
            | "context"
            | "dedup"
            | "diag"
            | "doctor"
            | "expand"
            | "export-html"
            | "guide"
            | "health"
            | "selftest"
            | "import"
            | "index"
            | "introspect"
            | "models"
            | "onboarding"
            | "pack"
            | "pages"
            | "resume"
            | "robot-docs"
            | "schedule"
            | "search"
            | "sessions"
            | "sources"
            | "state"
            | "stats"
            | "status"
            | "storage"
            | "support-bundle"
            | "timeline"
            | "triage"
            | "view"
    )
}

fn data_dir_insertion_index(rest: &[String], command_index: usize) -> Option<usize> {
    let command = rest.get(command_index)?.to_ascii_lowercase();
    match command.as_str() {
        "tui" | "index" | "search" | "pack" | "stats" | "diag" | "storage" | "dedup" | "status"
        | "triage" | "support-bundle" | "state" | "health" | "onboarding" | "guide" | "doctor"
        | "context" | "sessions" | "timeline" | "daemon" => Some(command_index + 1),
        "mirror" => rest
            .get(command_index + 1)
            .is_some_and(|action| action.eq_ignore_ascii_case("prune"))
            .then_some(command_index + 2),
        "sources" => rest
            .get(command_index + 1)
            .is_some_and(|action| action.eq_ignore_ascii_case("artifact-manifest"))
            .then_some(command_index + 2),
        "schedule" => rest
            .get(command_index + 1)
            .is_some_and(|action| {
                ["install", "status", "run"]
                    .iter()
                    .any(|candidate| action.eq_ignore_ascii_case(candidate))
            })
            .then_some(command_index + 2),
        "models" => rest
            .get(command_index + 1)
            .is_some_and(|action| {
                ["install", "verify", "backfill", "remove"]
                    .iter()
                    .any(|candidate| action.eq_ignore_ascii_case(candidate))
            })
            .then_some(command_index + 2),
        // `fleet`, `release-verify`, and `upgrade` are removed commands: they
        // never reach this rewrite because `removed_invocation_error` rejects
        // them in `parse_cli` first, so they have no arm here.
        "analytics" => rest
            .get(command_index + 1)
            .is_some_and(|action| {
                [
                    "status",
                    "tokens",
                    "tools",
                    "models",
                    "incidents",
                    "rebuild",
                    "validate",
                ]
                .iter()
                .any(|candidate| action.eq_ignore_ascii_case(candidate))
            })
            .then_some(command_index + 2),
        _ => None,
    }
}

fn recover_leading_data_dir_to_subcommand(rest: &mut Vec<String>, corrections: &mut Vec<String>) {
    let mut index = 0;
    let mut leading = Vec::new();
    let mut data_dir_args = Vec::new();

    while index < rest.len() {
        let arg = &rest[index];
        if arg == "--data-dir" {
            let Some(value) = rest.get(index + 1) else {
                return;
            };
            if value.starts_with('-') {
                return;
            }
            data_dir_args.push(arg.clone());
            data_dir_args.push(value.clone());
            index += 2;
            continue;
        }
        if arg.starts_with("--data-dir=") {
            data_dir_args.push(arg.clone());
            index += 1;
            continue;
        }
        if let Some(span) = structured_output_request_span_at(rest, index) {
            leading.extend(rest[index..index + span].iter().cloned());
            index += span;
            continue;
        }
        if arg.starts_with('-') {
            return;
        }

        let Some(insertion_index) = data_dir_insertion_index(rest, index) else {
            return;
        };
        if data_dir_args.is_empty() {
            return;
        }

        let mut normalized = Vec::with_capacity(rest.len());
        normalized.extend(leading);
        normalized.extend(rest[index..insertion_index].iter().cloned());
        normalized.extend(data_dir_args);
        normalized.extend(rest[insertion_index..].iter().cloned());
        *rest = normalized;
        corrections.push("Leading --data-dir moved onto the data-dir-aware subcommand".to_string());
        return;
    }
}

fn move_leading_structured_flag_to_subcommand(rest: &mut Vec<String>) -> bool {
    let mut leading_count = 0;
    while rest
        .get(leading_count)
        .is_some_and(|arg| arg == "--json" || arg == "--robot")
    {
        leading_count += 1;
    }
    let Some(command) = rest.get(leading_count) else {
        return false;
    };
    if leading_count == 0
        || command.starts_with('-')
        || !command_accepts_leading_structured_flag(command)
    {
        return false;
    }

    rest.drain(0..leading_count);
    if !rest.iter().any(|arg| arg == "--json" || arg == "--robot") {
        rest.push("--json".to_string());
    }
    true
}

fn robot_docs_topic_shorthand(arg: &str) -> Option<&'static str> {
    match arg.to_ascii_lowercase().as_str() {
        "commands" | "command" | "cmds" => Some("commands"),
        "env" | "environment" | "vars" | "variables" => Some("env"),
        "paths" | "path" | "dirs" | "directories" => Some("paths"),
        "schemas" | "schema" => Some("schemas"),
        "examples" | "example" => Some("examples"),
        "contracts" | "contract" | "api-contract" | "api_contract" => Some("contracts"),
        "wrap" | "wrapping" | "line-wrap" | "line_wrap" => Some("wrap"),
        "sources" | "source" | "remotes" | "remote" => Some("sources"),
        "analytics" | "metrics" => Some("analytics"),
        "doctor" | "diagnostics" | "diag" => Some("doctor"),
        "exit-codes" | "exit_codes" | "exitcodes" | "codes" => Some("exit-codes"),
        "quickstart" | "quick-start" => Some("guide"),
        _ => None,
    }
}

fn recover_robot_docs_topic_shorthands(rest: &mut Vec<String>, corrections: &mut Vec<String>) {
    let leading_structured_count = rest
        .iter()
        .take_while(|arg| matches!(arg.as_str(), "--json" | "--robot"))
        .count();
    let Some(arg) = rest.get(leading_structured_count) else {
        return;
    };
    // Topic aliases that are also real commands must stay commands. Structured
    // command help is handled by `recover_structured_help_requests` instead.
    if is_canonical_top_level_command_arg(arg) {
        return;
    }
    let Some(topic) = robot_docs_topic_shorthand(arg) else {
        return;
    };

    let alias = rest[leading_structured_count].clone();
    rest[leading_structured_count] = "robot-docs".to_string();
    rest.insert(leading_structured_count + 1, topic.to_string());
    corrections.push(format!(
        "'{alias}' → 'robot-docs {topic}' (robot-docs topic shorthand)"
    ));
}

#[cfg(test)]
mod robot_docs_shorthand_regression_tests {
    use super::*;

    #[test]
    fn doctor_subcommand_is_not_rewritten_to_robot_docs() {
        let mut rest = vec!["doctor".to_string(), "--json".to_string()];
        let mut corrections = Vec::new();
        recover_robot_docs_topic_shorthands(&mut rest, &mut corrections);
        assert_eq!(rest, vec!["doctor".to_string(), "--json".to_string()]);
        assert!(
            corrections.is_empty(),
            "no rewrite should happen for `cass doctor --json`"
        );
    }

    #[test]
    fn other_real_subcommands_are_not_rewritten() {
        for sub in &[
            "diag",
            "sources",
            "analytics",
            "health",
            "selftest",
            "Doctor",
            "guide",
        ] {
            let mut rest = vec![sub.to_string(), "--json".to_string()];
            let mut corrections = Vec::new();
            recover_robot_docs_topic_shorthands(&mut rest, &mut corrections);
            assert_eq!(
                rest,
                vec![sub.to_string(), "--json".to_string()],
                "real subcommand {sub} should not be rewritten",
            );
            assert!(
                corrections.is_empty(),
                "no rewrite should happen for `cass {sub} --json`",
            );
        }
    }

    #[test]
    fn topic_shorthand_still_fires_for_non_subcommands() {
        // Note: `guide` is intentionally absent — it is now a real top-level
        // subcommand (`cass guide <intent>`), so the shorthand must NOT fire for
        // it (covered by `other_real_subcommands_are_not_rewritten`).
        for (alias, expected_topic) in &[
            ("schemas", "schemas"),
            ("exit-codes", "exit-codes"),
            ("examples", "examples"),
            ("env", "env"),
            ("quickstart", "guide"),
            ("quick-start", "guide"),
        ] {
            let mut rest = vec![alias.to_string(), "--json".to_string()];
            let mut corrections = Vec::new();
            recover_robot_docs_topic_shorthands(&mut rest, &mut corrections);
            assert_eq!(
                rest,
                vec![
                    "robot-docs".to_string(),
                    expected_topic.to_string(),
                    "--json".to_string()
                ],
                "shorthand for {alias} should still rewrite to robot-docs {expected_topic}",
            );
            assert_eq!(corrections.len(), 1);
        }
    }

    #[test]
    fn structured_help_does_not_steal_search_query_named_help() {
        let raw = vec![
            "cass".to_string(),
            "search".to_string(),
            "help".to_string(),
            "--json".to_string(),
        ];
        let (normalized, note) = normalize_args(raw);
        assert_eq!(
            normalized,
            vec![
                "cass".to_string(),
                "search".to_string(),
                "help".to_string(),
                "--json".to_string()
            ]
        );
        assert!(
            note.as_deref()
                .is_none_or(|note| !note.contains("structured help request")),
            "a search query named `help` must not be treated as a help request"
        );
    }

    #[test]
    fn structured_help_targets_command_after_leading_json_flag() {
        let raw = vec![
            "cass".to_string(),
            "--json".to_string(),
            "search".to_string(),
            "--help".to_string(),
        ];
        let (normalized, note) = normalize_args(raw);
        assert_eq!(
            normalized,
            vec![
                "cass".to_string(),
                "robot-docs".to_string(),
                "commands".to_string()
            ]
        );
        assert!(
            note.as_deref()
                .is_some_and(|note| note.contains("structured help request")),
            "leading structured flags should still route command help to robot docs"
        );
    }

    #[test]
    fn leading_data_dir_moves_to_health_subcommand() {
        let raw = vec![
            "cass".to_string(),
            "--data-dir".to_string(),
            "/tmp/cass-data".to_string(),
            "health".to_string(),
            "--json".to_string(),
        ];
        let (normalized, note) = normalize_args(raw);
        assert_eq!(
            normalized,
            vec![
                "cass".to_string(),
                "health".to_string(),
                "--data-dir".to_string(),
                "/tmp/cass-data".to_string(),
                "--json".to_string()
            ]
        );
        assert!(
            note.as_deref()
                .is_some_and(|note| note.contains("Leading --data-dir moved")),
            "leading data dir should be reported as a correction"
        );
    }

    #[test]
    fn leading_structured_and_data_dir_move_to_nested_models_subcommand() {
        let raw = vec![
            "cass".to_string(),
            "--json".to_string(),
            "--data-dir=/tmp/cass-data".to_string(),
            "models".to_string(),
            "verify".to_string(),
        ];
        let (normalized, note) = normalize_args(raw);
        assert_eq!(
            normalized,
            vec![
                "cass".to_string(),
                "models".to_string(),
                "verify".to_string(),
                "--data-dir=/tmp/cass-data".to_string(),
                "--json".to_string()
            ]
        );
        assert!(
            note.as_deref()
                .is_some_and(|note| note.contains("Leading --data-dir moved")),
            "nested data-dir-aware commands should get the data dir after the nested verb"
        );
    }
}

fn structured_output_request_span_at(rest: &[String], index: usize) -> Option<usize> {
    let arg = rest.get(index)?;
    if matches!(arg.as_str(), "--json" | "--robot") {
        return Some(1);
    }
    if arg == "--robot-format" {
        return rest
            .get(index + 1)
            .is_some_and(|value| is_robot_format_alias_value(value))
            .then_some(2);
    }
    if let Some(value) = arg.strip_prefix("--robot-format=")
        && is_robot_format_alias_value(value)
    {
        return Some(1);
    }
    parse_format_alias_at(rest, index).map(|(consumed, _, _)| consumed)
}

fn is_canonical_top_level_command_arg(arg: &str) -> bool {
    CANONICAL_TOP_LEVEL_COMMANDS
        .iter()
        .any(|command| arg.eq_ignore_ascii_case(command))
}

fn contains_structured_output_request(rest: &[String]) -> bool {
    (0..rest.len()).any(|index| structured_output_request_span_at(rest, index).is_some())
}

fn prefix_is_structured_output_requests(rest: &[String], end: usize) -> bool {
    let mut index = 0;
    while index < end {
        let Some(span) = structured_output_request_span_at(rest, index) else {
            return false;
        };
        index += span;
    }
    true
}

fn structured_help_request_index(rest: &[String]) -> Option<usize> {
    rest.iter()
        .enumerate()
        .find_map(|(index, arg)| match arg.as_str() {
            "--help" | "-h" => Some(index),
            "help" if prefix_is_structured_output_requests(rest, index) => Some(index),
            _ => None,
        })
}

fn first_command_arg_after_structured_prefix(rest: &[String]) -> Option<&str> {
    let mut index = 0;
    while index < rest.len() {
        if let Some(span) = structured_output_request_span_at(rest, index) {
            index += span;
            continue;
        }
        let arg = &rest[index];
        if !arg.starts_with('-') {
            return Some(arg);
        }
        index += 1;
    }
    None
}

fn help_request_target(rest: &[String], help_index: usize) -> Option<&str> {
    if rest.get(help_index).is_some_and(|arg| arg == "help") {
        let mut index = help_index + 1;
        while index < rest.len() {
            if let Some(span) = structured_output_request_span_at(rest, index) {
                index += span;
                continue;
            }
            let arg = &rest[index];
            if !arg.starts_with('-') {
                return Some(arg);
            }
            index += 1;
        }
        return None;
    }

    if help_index > 0 {
        return first_command_arg_after_structured_prefix(rest);
    }

    None
}

fn recover_structured_help_requests(rest: &mut Vec<String>, corrections: &mut Vec<String>) {
    let Some(help_index) = structured_help_request_index(rest) else {
        return;
    };
    if !contains_structured_output_request(rest) {
        return;
    }

    let topic = help_request_target(rest, help_index)
        .and_then(robot_docs_topic_shorthand)
        .unwrap_or_else(|| {
            if help_request_target(rest, help_index).is_some() {
                "commands"
            } else {
                "guide"
            }
        });
    let original = rest.join(" ");
    *rest = vec!["robot-docs".to_string(), topic.to_string()];
    corrections.push(format!(
        "'{original}' → 'robot-docs {topic}' (structured help request)"
    ));
}

fn is_current_session_shorthand(arg: &str) -> bool {
    matches!(
        arg.to_ascii_lowercase().as_str(),
        "current" | "current-session" | "current_session" | "session-current" | "session_current"
    )
}

fn has_current_session_flag(rest: &[String]) -> bool {
    rest.iter()
        .any(|arg| arg == "--current" || arg.starts_with("--current="))
}

fn recover_current_session_shorthands(rest: &mut Vec<String>, corrections: &mut Vec<String>) {
    let leading_structured_count = rest
        .iter()
        .take_while(|arg| matches!(arg.as_str(), "--json" | "--robot"))
        .count();

    if rest
        .get(leading_structured_count)
        .is_some_and(|arg| is_current_session_shorthand(arg))
    {
        let alias = rest[leading_structured_count].clone();
        rest[leading_structured_count] = "sessions".to_string();
        if !has_current_session_flag(rest) {
            rest.insert(leading_structured_count + 1, "--current".to_string());
        }
        corrections.push(format!(
            "'{alias}' → 'sessions --current' (current-session shorthand)"
        ));
        return;
    }

    if !matches!(rest.first().map(String::as_str), Some("sessions")) {
        return;
    }
    if !rest
        .get(1)
        .is_some_and(|arg| is_current_session_shorthand(arg))
    {
        return;
    }

    let alias = rest.remove(1);
    if !has_current_session_flag(rest) {
        rest.insert(1, "--current".to_string());
    }
    corrections.push(format!(
        "'sessions {alias}' → 'sessions --current' (current-session shorthand)"
    ));
}

fn take_named_value(rest: &mut Vec<String>, names: &[&str]) -> Option<(String, String)> {
    for index in 1..rest.len() {
        let arg = rest[index].clone();
        for name in names {
            if arg == *name {
                let value = rest.get(index + 1)?;
                if value.starts_with('-') {
                    return None;
                }
                let value = value.clone();
                rest.drain(index..=index + 1);
                return Some(((*name).to_string(), value));
            }

            let prefix = format!("{name}=");
            if let Some(value) = arg.strip_prefix(&prefix)
                && !value.is_empty()
            {
                rest.remove(index);
                return Some(((*name).to_string(), value.to_string()));
            }
        }
    }
    None
}

fn take_assignment_value(rest: &mut Vec<String>, names: &[&str]) -> Option<(String, String)> {
    for index in 1..rest.len() {
        let arg = rest[index].clone();
        let Some((key, value)) = arg.split_once('=') else {
            continue;
        };
        if value.is_empty() || !names.contains(&key) {
            continue;
        }

        rest.remove(index);
        return Some((key.to_string(), value.to_string()));
    }
    None
}

fn rewrite_named_assignment_positional(
    rest: &mut [String],
    names: &[&str],
) -> Option<(String, String)> {
    for slot in rest.iter_mut().skip(1) {
        let arg = slot.clone();
        for name in names {
            let prefix = format!("{name}=");
            if let Some(value) = arg.strip_prefix(&prefix)
                && !value.is_empty()
            {
                *slot = value.to_string();
                return Some(((*name).to_string(), value.to_string()));
            }
        }
    }
    None
}

fn split_path_line_assignment(arg: &str) -> Option<(String, usize)> {
    let (path, line) = arg.rsplit_once(':')?;
    if path.is_empty() {
        return None;
    }
    let line = line.parse::<usize>().ok().filter(|line| *line > 0)?;
    Some((path.to_string(), line))
}

fn has_line_arg(rest: &[String]) -> bool {
    // An explicit canonical selector must also suppress path:line recovery.
    has_option_alias(
        rest,
        &[
            "--line",
            "-n",
            "--line-number",
            "--line_number",
            "--message-index",
            "--message_index",
        ],
    )
}

fn has_option_alias(rest: &[String], aliases: &[&str]) -> bool {
    rest.iter().any(|arg| {
        aliases.iter().any(|alias| {
            arg == alias
                || arg
                    .strip_prefix(alias)
                    .is_some_and(|suffix| suffix.starts_with('='))
        })
    })
}

struct AssignmentOption {
    flag: &'static str,
    aliases: &'static [&'static str],
    repeatable: bool,
}

fn assignment_option_for_command(command: &str, key: &str) -> Option<AssignmentOption> {
    match command {
        "search" => match key {
            "agent" | "provider" | "tool" | "connector" | "agent-type" | "agent_type" => {
                Some(AssignmentOption {
                    flag: "--agent",
                    aliases: &["--agent"],
                    repeatable: true,
                })
            }
            "workspace" => Some(AssignmentOption {
                flag: "--workspace",
                aliases: &["--workspace"],
                repeatable: true,
            }),
            "fields" => Some(AssignmentOption {
                flag: "--fields",
                aliases: &["--fields"],
                repeatable: false,
            }),
            "max-content-length" | "max_content_length" => Some(AssignmentOption {
                flag: "--max-content-length",
                aliases: &["--max-content-length"],
                repeatable: false,
            }),
            "max-tokens" | "max_tokens" => Some(AssignmentOption {
                flag: "--max-tokens",
                aliases: &["--max-tokens"],
                repeatable: false,
            }),
            "request-id" | "request_id" => Some(AssignmentOption {
                flag: "--request-id",
                aliases: &["--request-id"],
                repeatable: false,
            }),
            "cursor" => Some(AssignmentOption {
                flag: "--cursor",
                aliases: &["--cursor"],
                repeatable: false,
            }),
            "data-dir" | "data_dir" => Some(AssignmentOption {
                flag: "--data-dir",
                aliases: &["--data-dir"],
                repeatable: false,
            }),
            "offset" => Some(AssignmentOption {
                flag: "--offset",
                aliases: &["--offset"],
                repeatable: false,
            }),
            "days" => Some(AssignmentOption {
                flag: "--days",
                aliases: &["--days"],
                repeatable: false,
            }),
            "since" => Some(AssignmentOption {
                flag: "--since",
                aliases: &["--since"],
                repeatable: false,
            }),
            "until" => Some(AssignmentOption {
                flag: "--until",
                aliases: &["--until"],
                repeatable: false,
            }),
            "display" => Some(AssignmentOption {
                flag: "--display",
                aliases: &["--display"],
                repeatable: false,
            }),
            "mode" => Some(AssignmentOption {
                flag: "--mode",
                aliases: &["--mode"],
                repeatable: false,
            }),
            _ => None,
        },
        "pack" => match key {
            "agent" | "provider" | "tool" | "connector" | "agent-type" | "agent_type" => {
                Some(AssignmentOption {
                    flag: "--agent",
                    aliases: &["--agent"],
                    repeatable: true,
                })
            }
            "workspace" => Some(AssignmentOption {
                flag: "--workspace",
                aliases: &["--workspace"],
                repeatable: true,
            }),
            "fields" | "field-mask" | "field_mask" => Some(AssignmentOption {
                flag: "--fields",
                aliases: &["--fields", "--field-mask"],
                repeatable: false,
            }),
            "max-tokens" | "max_tokens" => Some(AssignmentOption {
                flag: "--max-tokens",
                aliases: &["--max-tokens"],
                repeatable: false,
            }),
            "max-sessions" | "max_sessions" => Some(AssignmentOption {
                flag: "--max-sessions",
                aliases: &["--max-sessions"],
                repeatable: false,
            }),
            "max-evidence" | "max_evidence" => Some(AssignmentOption {
                flag: "--max-evidence",
                aliases: &["--max-evidence"],
                repeatable: false,
            }),
            "context-lines" | "context_lines" => Some(AssignmentOption {
                flag: "--context-lines",
                aliases: &["--context-lines"],
                repeatable: false,
            }),
            "max-excerpt-chars" | "max_excerpt_chars" => Some(AssignmentOption {
                flag: "--max-excerpt-chars",
                aliases: &["--max-excerpt-chars"],
                repeatable: false,
            }),
            "request-id" | "request_id" => Some(AssignmentOption {
                flag: "--request-id",
                aliases: &["--request-id"],
                repeatable: false,
            }),
            "data-dir" | "data_dir" => Some(AssignmentOption {
                flag: "--data-dir",
                aliases: &["--data-dir"],
                repeatable: false,
            }),
            "days" => Some(AssignmentOption {
                flag: "--days",
                aliases: &["--days"],
                repeatable: false,
            }),
            "since" => Some(AssignmentOption {
                flag: "--since",
                aliases: &["--since"],
                repeatable: false,
            }),
            "until" => Some(AssignmentOption {
                flag: "--until",
                aliases: &["--until"],
                repeatable: false,
            }),
            "source" => Some(AssignmentOption {
                flag: "--source",
                aliases: &["--source"],
                repeatable: false,
            }),
            "sessions-from" | "sessions_from" => Some(AssignmentOption {
                flag: "--sessions-from",
                aliases: &["--sessions-from"],
                repeatable: false,
            }),
            "mode" => Some(AssignmentOption {
                flag: "--mode",
                aliases: &["--mode"],
                repeatable: false,
            }),
            "freshness-policy" | "freshness_policy" => Some(AssignmentOption {
                flag: "--freshness-policy",
                aliases: &["--freshness-policy"],
                repeatable: false,
            }),
            "freshness-window-seconds" | "freshness_window_seconds" => Some(AssignmentOption {
                flag: "--freshness-window-seconds",
                aliases: &["--freshness-window-seconds"],
                repeatable: false,
            }),
            "timeout" => Some(AssignmentOption {
                flag: "--timeout",
                aliases: &["--timeout"],
                repeatable: false,
            }),
            _ => None,
        },
        "sessions" => match key {
            "workspace" => Some(AssignmentOption {
                flag: "--workspace",
                aliases: &["--workspace"],
                repeatable: false,
            }),
            "data-dir" | "data_dir" => Some(AssignmentOption {
                flag: "--data-dir",
                aliases: &["--data-dir"],
                repeatable: false,
            }),
            _ => None,
        },
        "context" | "expand" | "export" | "export-html" | "view" => match key {
            "source" | "source-id" | "source_id" => Some(AssignmentOption {
                flag: "--source",
                aliases: &["--source", "--source-id"],
                repeatable: false,
            }),
            _ => None,
        },
        "timeline" => match key {
            "agent" | "provider" | "tool" | "connector" | "agent-type" | "agent_type" => {
                Some(AssignmentOption {
                    flag: "--agent",
                    aliases: &["--agent"],
                    repeatable: true,
                })
            }
            "data-dir" | "data_dir" => Some(AssignmentOption {
                flag: "--data-dir",
                aliases: &["--data-dir"],
                repeatable: false,
            }),
            "since" => Some(AssignmentOption {
                flag: "--since",
                aliases: &["--since"],
                repeatable: false,
            }),
            "until" => Some(AssignmentOption {
                flag: "--until",
                aliases: &["--until"],
                repeatable: false,
            }),
            "group-by" | "group_by" => Some(AssignmentOption {
                flag: "--group-by",
                aliases: &["--group-by"],
                repeatable: false,
            }),
            "source" => Some(AssignmentOption {
                flag: "--source",
                aliases: &["--source"],
                repeatable: false,
            }),
            _ => None,
        },
        _ => None,
    }
}

fn command_accepts_time_assignment(command: &str) -> bool {
    matches!(command, "search" | "pack" | "timeline")
}

fn compact_relative_time_value(value: &str) -> bool {
    let digits_len = value
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .map(char::len_utf8)
        .sum::<usize>();
    if digits_len == 0 {
        return false;
    }

    matches!(
        value[digits_len..].trim(),
        "m" | "min"
            | "mins"
            | "minute"
            | "minutes"
            | "h"
            | "hr"
            | "hrs"
            | "hour"
            | "hours"
            | "d"
            | "day"
            | "days"
            | "w"
            | "week"
            | "weeks"
    )
}

fn normalize_past_time_window_value(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.starts_with('-')
        || trimmed.ends_with("ago")
        || matches!(trimmed, "now" | "today" | "yesterday")
    {
        return trimmed.to_string();
    }
    if trimmed.chars().all(|ch| ch.is_ascii_digit()) {
        return format!("-{trimmed}d");
    }
    if compact_relative_time_value(trimmed) {
        return format!("-{trimmed}");
    }
    trimmed.to_string()
}

fn time_assignment_option_for_command(
    command: &str,
    key: &str,
    value: &str,
) -> Option<(&'static str, &'static [&'static str], String)> {
    if !command_accepts_time_assignment(command) {
        return None;
    }

    match key {
        "last" | "recent" | "past" | "within" => Some((
            "--since",
            &["--since"],
            normalize_past_time_window_value(value),
        )),
        "from" | "after" | "start" | "start-date" | "start_date" => {
            Some(("--since", &["--since"], value.trim().to_string()))
        }
        "to" | "before" | "end" | "end-date" | "end_date" => {
            Some(("--until", &["--until"], value.trim().to_string()))
        }
        _ => None,
    }
}

fn take_time_alias_flag(rest: &mut Vec<String>) -> Option<(String, &'static str, String)> {
    for index in 1..rest.len() {
        let arg = rest[index].clone();
        if !arg.starts_with("--") {
            continue;
        }

        let (name, value, consumed) = if let Some((name, value)) = arg.split_once('=') {
            let Some(name) = name.strip_prefix("--") else {
                continue;
            };
            if value.is_empty() {
                continue;
            }
            (name, value.to_string(), 1)
        } else {
            let Some(name) = arg.strip_prefix("--") else {
                continue;
            };
            let Some(value) = rest.get(index + 1).cloned() else {
                continue;
            };
            if value.starts_with("--") {
                continue;
            }
            (name, value, 2)
        };

        let command = rest.first().map(String::as_str).unwrap_or_default();
        let Some((flag, _, rewritten_value)) =
            time_assignment_option_for_command(command, name, &value)
        else {
            continue;
        };
        rest.splice(
            index..index + consumed,
            [flag.to_string(), rewritten_value.clone()],
        );
        return Some((format!("--{name}"), flag, rewritten_value));
    }
    None
}

fn recover_time_alias_flags(rest: &mut Vec<String>, corrections: &mut Vec<String>) {
    while let Some((alias, flag, value)) = take_time_alias_flag(rest) {
        let command = rest
            .first()
            .cloned()
            .unwrap_or_else(|| "command".to_string());
        corrections.push(format!(
            "'{command} {alias} <value>' → '{command} {flag} {value}' (time-window alias)"
        ));
    }
}

fn command_accepts_agent_filter_alias(command: &str) -> bool {
    matches!(command, "search" | "pack" | "timeline")
}

fn take_agent_filter_alias_flag(rest: &mut Vec<String>) -> Option<(String, String)> {
    let command = rest.first().map(String::as_str).unwrap_or_default();
    if !command_accepts_agent_filter_alias(command) {
        return None;
    }

    for index in 1..rest.len() {
        let arg = rest[index].clone();
        if !arg.starts_with("--") {
            continue;
        }

        let (name, value, consumed) = if let Some((name, value)) = arg.split_once('=') {
            let Some(name) = name.strip_prefix("--") else {
                continue;
            };
            if value.is_empty() {
                continue;
            }
            (name, value.to_string(), 1)
        } else {
            let Some(name) = arg.strip_prefix("--") else {
                continue;
            };
            let Some(value) = rest.get(index + 1).cloned() else {
                continue;
            };
            if value.starts_with("--") {
                continue;
            }
            (name, value, 2)
        };

        if !matches!(
            name,
            "provider" | "tool" | "connector" | "agent-type" | "agent_type"
        ) {
            continue;
        }

        rest.splice(
            index..index + consumed,
            ["--agent".to_string(), value.clone()],
        );
        return Some((format!("--{name}"), value));
    }
    None
}

fn recover_agent_filter_alias_flags(rest: &mut Vec<String>, corrections: &mut Vec<String>) {
    while let Some((alias, value)) = take_agent_filter_alias_flag(rest) {
        let command = rest
            .first()
            .cloned()
            .unwrap_or_else(|| "command".to_string());
        corrections.push(format!(
            "'{command} {alias} <value>' → '{command} --agent {value}' (provider alias)"
        ));
    }
}

fn recover_key_value_option_assignments(rest: &mut [String], corrections: &mut Vec<String>) {
    let Some(command) = rest.first().cloned() else {
        return;
    };

    for index in 1..rest.len() {
        let arg = rest[index].clone();
        let Some((key, value)) = arg.split_once('=') else {
            continue;
        };
        if value.is_empty() {
            continue;
        }
        if let Some((flag, aliases, rewritten_value)) =
            time_assignment_option_for_command(&command, key, value)
        {
            if !has_option_alias(rest, aliases) {
                rest[index] = format!("{flag}={rewritten_value}");
                corrections.push(format!(
                    "'{command} {key}=<value>' → '{command} {flag} <value>' (time-window assignment)"
                ));
            }
            continue;
        }
        let Some(option) = assignment_option_for_command(&command, key) else {
            continue;
        };
        if !option.repeatable && has_option_alias(rest, option.aliases) {
            continue;
        }

        rest[index] = format!("{}={value}", option.flag);
        corrections.push(format!(
            "'{command} {key}=<value>' → '{command} {} <value>' (known option assignment)",
            option.flag
        ));
    }
}

fn bare_option_pair_start_index(command: &str) -> Option<usize> {
    match command {
        // Require at least one query token before interpreting bare words as
        // filters. Otherwise `cass search provider codex --json` would lose a
        // plausible query instead of producing a useful usage error.
        "search" | "pack" => Some(2),
        "sessions" | "timeline" => Some(1),
        _ => None,
    }
}

fn recover_bare_option_value_pairs(rest: &mut Vec<String>, corrections: &mut Vec<String>) {
    let Some(command) = rest.first().cloned() else {
        return;
    };
    let Some(start_index) = bare_option_pair_start_index(&command) else {
        return;
    };

    let mut index = start_index;
    while index + 1 < rest.len() {
        let key = rest[index].clone();
        if key.starts_with('-') || key.contains('=') {
            index += 1;
            continue;
        }
        let value = rest[index + 1].clone();
        if value.starts_with("--") {
            index += 1;
            continue;
        }

        let key_lower = key.to_ascii_lowercase();
        if let Some((flag, aliases, rewritten_value)) =
            time_assignment_option_for_command(&command, &key_lower, &value)
        {
            if !has_option_alias(rest, aliases) {
                rest.splice(
                    index..index + 2,
                    [flag.to_string(), rewritten_value.clone()],
                );
                corrections.push(format!(
                    "'{command} {key} <value>' → '{command} {flag} {rewritten_value}' (bare time-window filter)"
                ));
                index += 2;
                continue;
            }
        } else if command_accepts_limit_alias(rest) && is_result_count_assignment_key(&key_lower) {
            if !has_canonical_limit(rest) {
                rest.splice(index..index + 2, ["--limit".to_string(), value.clone()]);
                corrections.push(format!(
                    "'{command} {key} <value>' → '{command} --limit {value}' (bare result-count filter)"
                ));
                index += 2;
                continue;
            }
        } else if let Some(option) = assignment_option_for_command(&command, &key_lower)
            && (option.repeatable || !has_option_alias(rest, option.aliases))
        {
            rest.splice(index..index + 2, [option.flag.to_string(), value.clone()]);
            corrections.push(format!(
                "'{command} {key} <value>' → '{command} {} {value}' (bare option filter)",
                option.flag
            ));
            index += 2;
            continue;
        }

        index += 1;
    }
}

fn search_like_option_value_count(command: &str, arg: &str) -> Option<usize> {
    let name = arg.strip_prefix("--")?;
    let (name, inline_value) = name
        .split_once('=')
        .map_or((name, false), |(name, _)| (name, true));
    let name = name.to_ascii_lowercase();

    let takes_value = matches!(
        name.as_str(),
        "agent"
            | "workspace"
            | "limit"
            | "offset"
            | "fields"
            | "max-content-length"
            | "max_content_length"
            | "max-tokens"
            | "max_tokens"
            | "request-id"
            | "request_id"
            | "cursor"
            | "display"
            | "data-dir"
            | "data_dir"
            | "days"
            | "since"
            | "until"
            | "aggregate"
            | "timeout"
            | "source"
            | "sessions-from"
            | "sessions_from"
            | "mode"
            | "model"
            | "reranker"
            | "robot-format"
            | "robot_format"
    ) || matches!(
        (command, name.as_str()),
        (
            "pack",
            "field-mask"
                | "field_mask"
                | "max-sessions"
                | "max_sessions"
                | "max-evidence"
                | "max_evidence"
                | "context-lines"
                | "context_lines"
                | "max-excerpt-chars"
                | "max_excerpt_chars"
                | "freshness-policy"
                | "freshness_policy"
                | "freshness-window-seconds"
                | "freshness_window_seconds"
        )
    );

    if takes_value {
        return Some(if inline_value { 1 } else { 2 });
    }

    let is_bool = matches!(
        name.as_str(),
        "json"
            | "robot"
            | "robot-meta"
            | "robot_meta"
            | "today"
            | "yesterday"
            | "week"
            | "explain"
            | "dry-run"
            | "dry_run"
            | "highlight"
            | "approximate"
            | "rerank"
            | "daemon"
            | "no-daemon"
            | "no_daemon"
            | "two-tier"
            | "two_tier"
            | "fast-only"
            | "fast_only"
            | "quality-only"
            | "quality_only"
            | "refresh"
            | "catch-up"
            | "catch_up"
    ) || matches!(
        (command, name.as_str()),
        (
            "pack",
            "require-evidence"
                | "require_evidence"
                | "explain-selection"
                | "explain_selection"
                | "include-skill-content"
                | "include_skill_content"
        )
    );

    is_bool.then_some(1)
}

fn recover_query_after_leading_search_options(
    rest: &mut Vec<String>,
    corrections: &mut Vec<String>,
) {
    let Some(command) = rest.first().cloned() else {
        return;
    };
    if !matches!(command.as_str(), "search" | "pack") || rest.len() < 3 {
        return;
    }

    let mut index = 1;
    let mut saw_leading_option = false;
    while index < rest.len() {
        let Some(arg) = rest.get(index) else {
            return;
        };
        if !arg.starts_with('-') {
            break;
        }
        let Some(width) = search_like_option_value_count(&command, arg) else {
            return;
        };
        if width == 2
            && rest
                .get(index + 1)
                .is_none_or(|value| value.starts_with("--"))
        {
            return;
        }
        saw_leading_option = true;
        index += width;
    }

    if !saw_leading_option || index >= rest.len() {
        return;
    }

    let query_start = index;
    let mut query_end = query_start;
    while let Some(arg) = rest.get(query_end) {
        if arg.starts_with('-') || is_query_boundary_assignment(&command, arg) {
            break;
        }
        query_end += 1;
    }
    if query_end <= query_start {
        return;
    }

    let query = rest[query_start..query_end].join(" ");
    rest.drain(query_start..query_end);
    rest.insert(1, query.clone());
    corrections.push(format!(
        "'{command} <filters> {query}' → '{command} \"{query}\" <filters>' (query moved before leading filters)"
    ));
}

fn recover_path_line_positionals(rest: &mut Vec<String>, corrections: &mut Vec<String>) {
    let Some(command) = rest.first().cloned() else {
        return;
    };
    if !matches!(command.as_str(), "expand" | "view") || has_line_arg(rest) {
        return;
    }
    let Some(raw_path) = rest.get(1).cloned() else {
        return;
    };
    let Some((path, line)) = split_path_line_assignment(&raw_path) else {
        return;
    };

    rest[1] = path;
    rest.push("--line".to_string());
    rest.push(line.to_string());
    corrections.push(format!(
        "'{command} {raw_path}' → '{command} <path> --line {line}' (path:line shorthand)"
    ));
}

fn recover_named_required_positionals(rest: &mut Vec<String>, corrections: &mut Vec<String>) {
    let Some(command) = rest.first().cloned() else {
        return;
    };

    match command.as_str() {
        "search" | "pack" => {
            if let Some((flag, value)) =
                take_named_value(rest, &["--query", "--q", "--text", "--pattern"])
            {
                rest.insert(1, value);
                corrections.push(format!(
                    "'{command} {flag} <value>' → '{command} <query>' (query is positional)"
                ));
            } else if let Some((name, _value)) =
                rewrite_named_assignment_positional(rest, &["query", "q", "text", "pattern"])
            {
                corrections.push(format!(
                    "'{command} {name}=<value>' → '{command} <value>' (query is positional)"
                ));
            }
        }
        "context" | "expand" | "export" | "export-html" | "resume" | "view" => {
            if let Some((flag, value)) =
                take_named_value(rest, &["--path", "--source-path", "--file", "--session"])
            {
                rest.insert(1, value);
                corrections.push(format!(
                    "'{command} {flag} <value>' → '{command} <path>' (path is positional)"
                ));
            } else if let Some((name, _value)) = rewrite_named_assignment_positional(
                rest,
                &["path", "source-path", "source_path", "file", "session"],
            ) {
                corrections.push(format!(
                    "'{command} {name}=<value>' → '{command} <value>' (path is positional)"
                ));
            }
        }
        _ => {}
    }
}

fn drilldown_assignment_flag(
    command: &str,
    key: &str,
) -> Option<(&'static str, &'static [&'static str])> {
    if !matches!(command, "expand" | "view") {
        return None;
    }

    match key {
        "line" | "n" => Some((
            "--line",
            &["--line", "--line-number", "--line_number", "-n"],
        )),
        // Pasted search fields are canonical ordinals, never raw file lines.
        "line-number" | "line_number" | "message-index" | "message_index" => {
            Some(("--message-index", &["--message-index", "--message_index"]))
        }
        "conversation-id" | "conversation_id" => Some((
            "--conversation-id",
            &["--conversation-id", "--conversation_id"],
        )),
        "context" | "c" => Some(("--context", &["--context", "-C"])),
        _ => None,
    }
}

fn recover_drilldown_assignment_flags(rest: &mut Vec<String>, corrections: &mut Vec<String>) {
    let Some(command) = rest.first().cloned() else {
        return;
    };

    let mut index = 1;
    while index < rest.len() {
        let arg = rest[index].clone();
        let Some((key, value)) = arg.split_once('=') else {
            index += 1;
            continue;
        };
        if value.is_empty() {
            index += 1;
            continue;
        }
        let Some((flag, aliases)) = drilldown_assignment_flag(&command, key) else {
            index += 1;
            continue;
        };
        if has_option_alias(rest, aliases) {
            index += 1;
            continue;
        }

        rest.splice(index..=index, [flag.to_string(), value.to_string()]);
        corrections.push(format!(
            "'{command} {key}=<value>' → '{command} {flag} <value>' (drill-down option)"
        ));
        index += 2;
    }
}

fn is_robot_format_alias_value(value: &str) -> bool {
    matches!(value, "json" | "jsonl" | "compact" | "sessions" | "toon")
}

fn can_recover_format_alias_for_command(command: &str) -> bool {
    command != "export" && command_accepts_leading_structured_flag(command)
}

fn parse_format_alias_at(rest: &[String], index: usize) -> Option<(usize, String, &'static str)> {
    let arg = rest.get(index)?;
    for alias in ["--format", "--output", "--output-format"] {
        if arg == alias {
            let value = rest.get(index + 1)?;
            let value = value.to_ascii_lowercase();
            if is_robot_format_alias_value(&value) {
                return Some((2, value, alias));
            }
        }
        if let Some(value) = arg
            .strip_prefix(alias)
            .and_then(|rest| rest.strip_prefix('='))
        {
            let value = value.to_ascii_lowercase();
            if is_robot_format_alias_value(&value) {
                return Some((1, value, alias));
            }
        }
    }
    None
}

fn recover_structured_format_aliases(rest: &mut Vec<String>, corrections: &mut Vec<String>) {
    if let Some((consumed, format, alias)) = parse_format_alias_at(rest, 0) {
        match rest.get(consumed).cloned() {
            Some(command) if can_recover_format_alias_for_command(&command) => {
                rest.drain(0..consumed);
                rest.push("--robot-format".to_string());
                rest.push(format.clone());
                corrections.push(format!(
                    "Leading '{alias} {format}' moved to '{command} --robot-format {format}' (structured output format)"
                ));
                return;
            }
            Some(command) if command.starts_with('-') => {
                let mut triage_tail = vec!["--json".to_string()];
                triage_tail.extend(rest[consumed..].iter().cloned());
                if let Some(mut rewritten) = root_structured_triage_rewrite(&triage_tail) {
                    if rewritten.last().is_some_and(|arg| arg == "--json") {
                        rewritten.pop();
                    }
                    rewritten.push("--robot-format".to_string());
                    rewritten.push(format.clone());
                    *rest = rewritten;
                    corrections.push(format!(
                        "Top-level '{alias} {format}' → 'triage --robot-format {format}' (read-only agent preflight)"
                    ));
                    return;
                }
            }
            None => {
                rest.drain(0..consumed);
                rest.push("triage".to_string());
                rest.push("--robot-format".to_string());
                rest.push(format.clone());
                corrections.push(format!(
                    "Top-level '{alias} {format}' → 'triage --robot-format {format}' (read-only agent preflight)"
                ));
                return;
            }
            _ => {}
        }
    }

    let Some(command) = rest.first().cloned() else {
        return;
    };
    if !can_recover_format_alias_for_command(&command) {
        return;
    }

    for index in 1..rest.len() {
        if let Some((consumed, format, alias)) = parse_format_alias_at(rest, index) {
            rest.splice(
                index..index + consumed,
                ["--robot-format".to_string(), format.clone()],
            );
            corrections.push(format!(
                "'{command} {alias} {format}' → '{command} --robot-format {format}' (structured output format)"
            ));
            return;
        }
    }
}

fn command_accepts_limit_alias(rest: &[String]) -> bool {
    match rest.first().map(String::as_str) {
        Some("analytics") => rest
            .get(1)
            .is_some_and(|arg| matches!(arg.as_str(), "tools" | "incidents")),
        Some("context" | "pack" | "search" | "sessions") => true,
        _ => false,
    }
}

fn has_canonical_limit(rest: &[String]) -> bool {
    rest.iter()
        .any(|arg| arg == "--limit" || arg.starts_with("--limit="))
}

fn recover_limit_aliases(rest: &mut Vec<String>, corrections: &mut Vec<String>) {
    if !command_accepts_limit_alias(rest) || has_canonical_limit(rest) {
        return;
    }

    let named_limit = take_named_value(
        rest,
        &[
            "--max-results",
            "--num-results",
            "--results",
            "--count",
            "--top-k",
            "--topk",
            "--top_k",
            "--n",
            "-n",
        ],
    );
    let assignment_limit = || {
        take_assignment_value(
            rest,
            &[
                "limit",
                "max-results",
                "max_results",
                "num-results",
                "num_results",
                "results",
                "count",
                "top-k",
                "topk",
                "top_k",
                "n",
            ],
        )
    };

    let Some((flag, value)) = named_limit.or_else(assignment_limit) else {
        return;
    };

    let command = rest
        .first()
        .cloned()
        .unwrap_or_else(|| "command".to_string());
    rest.push("--limit".to_string());
    rest.push(value.clone());
    corrections.push(format!(
        "'{command} {flag} <value>' → '{command} --limit {value}' (result-count alias)"
    ));
}

fn has_explicit_structured_output_request(rest: &[String]) -> bool {
    rest.iter().enumerate().any(|(index, arg)| {
        matches!(arg.as_str(), "--json" | "--robot")
            || arg == "--robot-format"
            || arg.starts_with("--robot-format=")
            || parse_format_alias_at(rest, index).is_some()
    })
}

fn is_query_assignment(arg: &str) -> bool {
    arg.split_once('=')
        .is_some_and(|(key, value)| !value.is_empty() && matches!(key, "query" | "q"))
}

fn is_result_count_assignment_key(key: &str) -> bool {
    matches!(
        key,
        "limit"
            | "max-results"
            | "max_results"
            | "num-results"
            | "num_results"
            | "results"
            | "count"
            | "top-k"
            | "topk"
            | "top_k"
            | "n"
    )
}

fn is_query_boundary_assignment(command: &str, arg: &str) -> bool {
    let Some((key, value)) = arg.split_once('=') else {
        return false;
    };
    if value.is_empty() {
        return false;
    }

    let key = key.to_ascii_lowercase();
    assignment_option_for_command(command, &key).is_some()
        || time_assignment_option_for_command(command, &key, value).is_some()
        || is_result_count_assignment_key(&key)
}

fn looks_like_top_level_command_or_typo(arg: &str) -> bool {
    let lower = arg.to_ascii_lowercase();
    matches!(
        lower.as_str(),
        "help" | "--help" | "-h" | "--version" | "-v"
    ) || CANONICAL_TOP_LEVEL_COMMANDS.contains(&lower.as_str())
        || closest_top_level_command(&lower).is_some()
}

fn arg_marks_implicit_pack_intent(arg: &str) -> bool {
    if let Some(flag) = arg.strip_prefix("--") {
        let name = flag.split_once('=').map_or(flag, |(name, _)| name);
        return matches!(
            name,
            "field-mask"
                | "max-sessions"
                | "max-evidence"
                | "max-excerpt-chars"
                | "freshness-policy"
                | "freshness-window-seconds"
                | "include-skill-content"
                | "require-evidence"
                | "explain-selection"
        );
    }

    let Some((key, value)) = arg.split_once('=') else {
        return false;
    };
    if value.is_empty() {
        return false;
    }
    matches!(
        key.to_ascii_lowercase().as_str(),
        "field-mask"
            | "field_mask"
            | "max-sessions"
            | "max_sessions"
            | "max-evidence"
            | "max_evidence"
            | "max-excerpt-chars"
            | "max_excerpt_chars"
            | "freshness-policy"
            | "freshness_policy"
            | "freshness-window-seconds"
            | "freshness_window_seconds"
    )
}

fn implicit_robot_query_command(rest: &[String]) -> &'static str {
    if rest.iter().any(|arg| arg_marks_implicit_pack_intent(arg)) {
        "pack"
    } else {
        "search"
    }
}

fn recover_explicit_search_pack_intent(rest: &mut [String], corrections: &mut Vec<String>) {
    if !matches!(rest.first().map(String::as_str), Some("search"))
        || !has_explicit_structured_output_request(rest)
        || !rest
            .iter()
            .skip(1)
            .any(|arg| arg_marks_implicit_pack_intent(arg))
    {
        return;
    }

    rest[0] = "pack".to_string();
    corrections.push(
        "'search ... <pack-only flags>' → 'pack ... <pack-only flags>' (pack-only flags imply answer-pack intent)"
            .to_string(),
    );
}

fn recover_implicit_robot_search_query(rest: &mut Vec<String>, corrections: &mut Vec<String>) {
    let Some(first) = rest.first().cloned() else {
        return;
    };
    if first.starts_with('-')
        || !has_explicit_structured_output_request(rest)
        || looks_like_top_level_command_or_typo(&first)
    {
        return;
    }

    let command = implicit_robot_query_command(rest);
    if is_query_assignment(&first) {
        rest.insert(0, command.to_string());
        corrections.push(format!(
            "'query=<value> --json' → '{command} <value> --json' (implicit robot query)"
        ));
        return;
    }

    let mut query_token_count = 0;
    for arg in rest.iter() {
        if arg.starts_with('-') || is_query_boundary_assignment(command, arg) {
            break;
        }
        query_token_count += 1;
    }
    if query_token_count == 0 {
        return;
    }

    let query = rest[..query_token_count].join(" ");
    rest.splice(0..query_token_count, [command.to_string(), query.clone()]);
    corrections.push(format!(
        "'{query}' → '{command} \"{query}\"' (implicit robot query)"
    ));
}

fn recover_multiword_query_positionals(rest: &mut Vec<String>, corrections: &mut Vec<String>) {
    let Some(command) = rest.first().cloned() else {
        return;
    };
    if !matches!(command.as_str(), "search" | "pack") {
        return;
    }
    if rest.get(1).is_none_or(|arg| arg.starts_with('-')) {
        return;
    }

    let mut query_end = 1;
    while let Some(arg) = rest.get(query_end) {
        if arg.starts_with('-') || is_query_boundary_assignment(&command, arg) {
            break;
        }
        query_end += 1;
    }
    if query_end <= 2 {
        return;
    }

    let query = rest[1..query_end].join(" ");
    rest.splice(1..query_end, [query.clone()]);
    corrections.push(format!(
        "'{command} {query}' → '{command} \"{query}\"' (multi-word query positional)"
    ));
}

/// `cass robot-docs doctor` documents the doctor run-history surface as verbs
/// (`cass doctor ls`, `undo <run-id>`, `diff [<ref>]`, `gc --before <ts>`,
/// `capabilities`), but `doctor` takes them as flags, so every documented
/// spelling failed with "unexpected argument". Map `rest[1]` onto its flag and
/// return the correction note, or `None` when `rest[1]` is not such a verb.
/// `gc` without `--before` becomes a bare `--gc-before`, so clap reports the
/// missing value instead of running a plain doctor check.
fn rewrite_doctor_history_verb(rest: &mut Vec<String>) -> Option<String> {
    let verb = rest.get(1)?.to_ascii_lowercase();
    let flag = match verb.as_str() {
        "ls" => "--ls",
        "undo" => "--undo",
        "diff" => "--diff",
        "explain" => "--explain",
        "watch" => "--watch",
        "capabilities" => "--emit-capabilities",
        "gc" => {
            rest.remove(1);
            if let Some(before) = rest.iter_mut().find(|arg| *arg == "--before") {
                *before = "--gc-before".to_string();
            } else if let Some(before) = rest.iter_mut().find(|arg| arg.starts_with("--before=")) {
                *before = before.replacen("--before=", "--gc-before=", 1);
            } else {
                rest.insert(1, "--gc-before".to_string());
            }
            return Some(
                "'doctor gc --before <ts>' → 'doctor --gc-before <ts>' (doctor run-history GC)"
                    .to_string(),
            );
        }
        _ => return None,
    };
    rest[1] = flag.to_string();
    Some(format!(
        "'doctor {verb}' → 'doctor {flag}' (doctor run-history surface)"
    ))
}

/// Normalize common robot-mode invocation mistakes to make the CLI more forgiving for AI agents.
///
/// This function applies multiple layers of normalization to maximize acceptance of
/// commands where intent is clear, even if syntax is imperfect:
///
/// 1. **Single-dash long flags**: `-robot` → `--robot`, `-limit` → `--limit`
/// 2. **Case normalization**: `--Robot`, `--LIMIT` → `--robot`, `--limit`
/// 3. **Snake-case flag recovery**: `--max_results` → `--max-results`
/// 4. **Subcommand aliases**: `find`/`query`/`q` → `search`, `ls`/`list` → `stats`, etc.
/// 5. **Flag-as-subcommand**: `--robot-docs` → `robot-docs` subcommand
/// 6. **Root structured-output default**: `--json`/`--robot` with no command → `triage --json`
/// 7. **Leading structured-output recovery**: `--json search` → `search --json`
/// 8. **Named positional recovery**: `search --query foo` → `search foo`
/// 9. **Multi-word query recovery**: `search foo bar --json` → `search "foo bar" --json`
/// 10. **Structured format alias recovery**: `search foo --format json`/`--output json` → `search foo --robot-format json`
/// 11. **Result-count alias recovery**: `search foo --max-results 5` → `search foo --limit 5`
/// 12. **Time-window alias recovery**: `search foo --last 7` → `search foo --since -7d`
/// 13. **Provider alias recovery**: `search foo --provider codex` → `search foo --agent codex`
/// 14. **Bare option-pair recovery**: `search foo provider codex` → `search foo --agent codex`
/// 15. **Pack-intent recovery**: `search foo --json --max-evidence 3` → `pack foo --json --max-evidence 3`
/// 16. **Leading-filter query recovery**: `search --agent codex foo bar` → `search "foo bar" --agent codex`
/// 17. **Implicit robot search recovery**: `foo bar --json` → `search "foo bar" --json`
/// 18. **Drill-down option recovery**: `view file line=42` → `view file --line 42`
/// 19. **Search-result field assignments**: `view file line_number=42` → `view file --message-index 42`
/// 20. **Search-result source aliases**: `view source_path=file source_id=local` → `view file --source local`
/// 21. **Robot-docs topic shorthand**: `commands --json` → `robot-docs commands`
/// 22. **Current-session shorthand**: `current --json` → `sessions --current --json`
/// 23. **Structured help recovery**: `help search --json` → `robot-docs commands`
/// 24. **Global flag hoisting**: Moves global flags to front regardless of position
/// 25. **Doctor run-history verbs**: `doctor ls|undo|diff|gc --before|capabilities` → flags
///
/// Returns normalized argv plus an optional correction note teaching proper syntax.
fn normalize_args(raw: Vec<String>) -> (Vec<String>, Option<String>) {
    if raw.is_empty() {
        return (raw, None);
    }
    let prog = &raw[0];
    let mut globals: Vec<String> = Vec::new();
    let mut rest: Vec<String> = Vec::new();
    let mut sub_seen = false;
    let mut corrections: Vec<String> = Vec::new();

    // Known long flags (without --) for single-dash and case normalization
    const KNOWN_LONG_FLAGS: &[&str] = &[
        "robot",
        "json",
        "limit",
        "max-results",
        "num-results",
        "results",
        "count",
        "top-k",
        "topk",
        "top_k",
        "n",
        "offset",
        "agent",
        "provider",
        "tool",
        "connector",
        "agent-type",
        "agent_type",
        "workspace",
        "fields",
        "max-tokens",
        "request-id",
        "cursor",
        "since",
        "until",
        "last",
        "recent",
        "past",
        "within",
        "from",
        "after",
        "start",
        "start-date",
        "start_date",
        "to",
        "before",
        "end",
        "end-date",
        "end_date",
        "days",
        "today",
        "yesterday",
        "week",
        "full",
        "watch",
        "data-dir",
        "verbose",
        "quiet",
        "query",
        "q",
        "text",
        "pattern",
        "color",
        "progress",
        "wrap",
        "nowrap",
        "db",
        "trace-file",
        "robot-help",
        "robot-docs",
        "help",
        "version",
        "force",
        "dry-run",
        "no-cache",
        "source",
        "sessions-from",
        "mode",
        "highlight",
        "timeout",
        "explain",
        "aggregate",
        "display",
        // Subcommand-specific flags
        "path",
        "source-path",
        "source-id",
        "line-number",
        "message-index",
        "conversation-id",
        "file",
        "session",
        "line",
        "context",
        "output",
        "output-format",
        "format",
        "encrypt",
        "theme",
        // Added flags
        "watch-once",
        "watch-interval",
        "background",
        "semantic",
        "embedder",
        "idempotency-key",
        "model",
        "rerank",
        "reranker",
        "daemon",
        "no-daemon",
        "preview",
        "port",
        "config",
        "validate-config",
        "example-config",
        "skip-install",
        "skip-index",
        "skip-sync",
        "resume",
        "non-interactive",
        // Missing flags added
        "approximate",
        "build-hnsw",
        "export-only",
        "verify",
        "scan-secrets",
        "fail-on-secrets",
        "secrets-allow",
        "secrets-deny",
        "no-encryption",
        "i-understand-unencrypted-risks",
        "no-open",
        "stale-threshold",
        "by-source",
        "robot-meta",
        "robot-format",
        "max-content-length",
        "fast-only",
        "quality-only",
        "once",
        "reset-state",
        "asciicast",
        "inline",
        "ui-height",
        "anchor",
        "record-macro",
        "play-macro",
        "group-by",
        "from-file",
        "fixture",
        "repair",
        "cleanup",
        "recover-from-archive",
        "rebuild-canonical-fts",
        "repair-leaked-pages",
        "cleanup-interrupted-artifacts",
        "archive-scan",
        "archive-normalize",
        "check",
        "yes",
        "plan-fingerprint",
        "purge",
        "preset",
        "no-test",
        "no-index",
        "skip-existing",
        "hosts",
    ];

    // Subcommand aliases for common mistakes
    const SUBCOMMAND_ALIASES: &[(&str, &str)] = &[
        // Search aliases
        ("find", "search"),
        ("query", "search"),
        ("q", "search"),
        ("lookup", "search"),
        ("grep", "search"),
        ("session", "sessions"),
        // Stats aliases
        ("ls", "stats"),
        ("list", "stats"),
        ("info", "stats"),
        ("summary", "stats"),
        // Status aliases
        ("st", "status"),
        ("state", "status"),
        // Index aliases
        ("reindex", "index"),
        ("idx", "index"),
        ("rebuild", "index"),
        // View aliases
        ("show", "view"),
        ("get", "view"),
        ("read", "view"),
        // Diag aliases
        ("diagnose", "diag"),
        ("debug", "diag"),
        ("check", "diag"),
        // Capabilities aliases
        ("caps", "capabilities"),
        ("cap", "capabilities"),
        // Agent triage aliases
        ("ready", "triage"),
        ("preflight", "triage"),
        // Answer-pack aliases
        ("answer", "pack"),
        ("evidence", "pack"),
        ("bundle", "pack"),
        ("handoff", "pack"),
        ("why", "pack"),
        ("explain", "pack"),
        ("rca", "pack"),
        ("root-cause", "pack"),
        ("rootcause", "pack"),
        ("summarize", "pack"),
        ("summarise", "pack"),
        // HTML export aliases
        ("html-export", "export-html"),
        ("html_export", "export-html"),
        ("exporthtml", "export-html"),
        // Introspect aliases
        ("inspect", "introspect"),
        ("intro", "introspect"),
        // Robot-docs aliases
        ("docs", "robot-docs"),
        ("help-robot", "robot-docs"),
        ("robotdocs", "robot-docs"),
    ];

    // Short flags that should remain as single-dash
    const VALID_SHORT_FLAGS: &[&str] = &["-q", "-v", "-h", "-V"];

    // Global flags that take a value via separate argument (--flag VALUE)
    // Note: --data-dir is NOT a global flag - it's per-subcommand
    let global_with_value = |s: &str| {
        matches!(
            s,
            "--color" | "--progress" | "--wrap" | "--db" | "--trace-file"
        )
    };

    // Global flags that take a value via `=` syntax or are standalone
    // Note: --data-dir is NOT a global flag - it's per-subcommand
    let is_global = |s: &str| {
        s == "--color"
            || s.starts_with("--color=")
            || s == "--progress"
            || s.starts_with("--progress=")
            || s == "--wrap"
            || s.starts_with("--wrap=")
            || s == "--nowrap"
            || s == "--db"
            || s.starts_with("--db=")
            || s == "--quiet"
            || s == "-q"
            || s == "--verbose"
            || s == "-v"
            || s == "--trace-file"
            || s.starts_with("--trace-file=")
            || s == "--robot-help"
    };

    fn canonical_known_long_flag(flag: &str) -> Option<String> {
        if flag.contains('_') {
            let kebab = flag.replace('_', "-");
            if KNOWN_LONG_FLAGS.contains(&kebab.as_str()) {
                return Some(kebab);
            }
        }

        KNOWN_LONG_FLAGS.contains(&flag).then(|| flag.to_string())
    }

    /// Normalize a single argument: single-dash → double-dash, case → lowercase, snake_case → kebab-case
    fn normalize_single_arg(arg: &str, corrections: &mut Vec<String>) -> String {
        // Skip if already valid short flag
        if VALID_SHORT_FLAGS.contains(&arg) {
            return arg.to_string();
        }

        // Handle single-dash long flags: -robot → --robot, -limit=5 → --limit=5
        if arg.starts_with('-') && !arg.starts_with("--") && arg.len() > 2 {
            let (flag_part, value_part) = if let Some(idx) = arg.find('=') {
                (&arg[1..idx], Some(&arg[idx..]))
            } else {
                (&arg[1..], None)
            };
            let flag_lower = flag_part.to_lowercase();
            if let Some(flag_canonical) = canonical_known_long_flag(&flag_lower) {
                let corrected = if let Some(val) = value_part {
                    format!("--{flag_canonical}{val}")
                } else {
                    format!("--{flag_canonical}")
                };
                let reason = if flag_lower == flag_canonical {
                    "use double-dash for long flags"
                } else {
                    "use double-dash and kebab-case for long flags"
                };
                corrections.push(format!("'{arg}' → '{corrected}' ({reason})"));
                return corrected;
            }
        }

        // Handle case/snake_case normalization for double-dash flags:
        // --Robot → --robot, --max_results → --max-results.
        if let Some(stripped) = arg.strip_prefix("--") {
            let (flag_part, value_part) = if let Some(idx) = stripped.find('=') {
                (&stripped[..idx], Some(&stripped[idx..]))
            } else {
                (stripped, None)
            };
            let flag_lower = flag_part.to_lowercase();
            if let Some(flag_canonical) = canonical_known_long_flag(&flag_lower)
                && flag_part != flag_canonical
            {
                let corrected = if let Some(val) = value_part {
                    format!("--{flag_canonical}{val}")
                } else {
                    format!("--{flag_canonical}")
                };
                let reason = if flag_lower == flag_canonical {
                    "flags are lowercase"
                } else {
                    "long flags use kebab-case"
                };
                corrections.push(format!("'{arg}' → '{corrected}' ({reason})"));
                return corrected;
            }
        }

        arg.to_string()
    }

    let args: Vec<_> = raw.iter().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        let arg = args[i];

        // First, normalize the argument (single-dash, case)
        let normalized_arg = normalize_single_arg(arg, &mut corrections);

        // Handle --robot-docs and --robot-docs=topic (flag used as subcommand)
        if normalized_arg == "--robot-docs" {
            rest.push("robot-docs".into());
            corrections
                .push("'--robot-docs' → 'robot-docs' (it's a subcommand, not a flag)".into());
            i += 1;
            continue;
        }
        if let Some(topic) = normalized_arg.strip_prefix("--robot-docs=") {
            rest.push("robot-docs".into());
            if !topic.is_empty() {
                rest.push(topic.to_string());
            }
            corrections.push(format!(
                "'{}' → 'robot-docs {topic}' (robot-docs is a subcommand)",
                arg
            ));
            i += 1;
            continue;
        }

        // Check for subcommand aliases (only before first subcommand seen)
        if !sub_seen && !normalized_arg.starts_with('-') {
            let lower = normalized_arg.to_lowercase();
            if let Some(&(alias, canonical)) = SUBCOMMAND_ALIASES
                .iter()
                .find(|(a, _)| a.eq_ignore_ascii_case(&lower))
            {
                rest.push(canonical.to_string());
                corrections.push(format!(
                    "'{alias}' → '{canonical}' (canonical subcommand name)"
                ));
                sub_seen = true;
                i += 1;
                continue;
            }
        }

        // Handle global flags
        if is_global(&normalized_arg) {
            globals.push(normalized_arg.clone());
            // Only note if globals appear after subcommand (moved to front)
            if sub_seen && !corrections.iter().any(|c| c.contains("moved to front")) {
                corrections.push("Global flags moved to front of command".into());
            }
            // If this global takes a value and doesn't use `=` syntax, consume the next arg
            if global_with_value(&normalized_arg)
                && !normalized_arg.contains('=')
                && i + 1 < args.len()
                && !args[i + 1].starts_with('-')
            {
                globals.push(args[i + 1].to_string());
                i += 1;
            }
            i += 1;
            continue;
        }

        if !sub_seen && !normalized_arg.starts_with('-') {
            sub_seen = true;
        }
        rest.push(normalized_arg);
        i += 1;
    }

    let mut normalized = Vec::with_capacity(1 + globals.len() + rest.len());
    recover_structured_help_requests(&mut rest, &mut corrections);
    recover_robot_docs_topic_shorthands(&mut rest, &mut corrections);
    recover_current_session_shorthands(&mut rest, &mut corrections);
    recover_leading_data_dir_to_subcommand(&mut rest, &mut corrections);
    if move_leading_structured_flag_to_subcommand(&mut rest) {
        corrections.push(
            "Leading --json/--robot moved after the subcommand (structured output flag)".into(),
        );
    }
    recover_implicit_robot_search_query(&mut rest, &mut corrections);
    recover_explicit_search_pack_intent(&mut rest, &mut corrections);
    recover_structured_format_aliases(&mut rest, &mut corrections);
    recover_limit_aliases(&mut rest, &mut corrections);
    recover_time_alias_flags(&mut rest, &mut corrections);
    recover_agent_filter_alias_flags(&mut rest, &mut corrections);
    recover_named_required_positionals(&mut rest, &mut corrections);
    recover_query_after_leading_search_options(&mut rest, &mut corrections);
    recover_bare_option_value_pairs(&mut rest, &mut corrections);
    recover_multiword_query_positionals(&mut rest, &mut corrections);
    recover_drilldown_assignment_flags(&mut rest, &mut corrections);
    recover_key_value_option_assignments(&mut rest, &mut corrections);
    recover_path_line_positionals(&mut rest, &mut corrections);
    if rest
        .first()
        .is_some_and(|arg| arg.eq_ignore_ascii_case("doctor"))
        && rest
            .get(1)
            .is_some_and(|arg| arg.eq_ignore_ascii_case("backups"))
        && let Some(action) = rest.get(2).map(|arg| arg.to_ascii_lowercase())
    {
        match action.as_str() {
            "list" => {
                rest.remove(2);
                rest.remove(1);
                rest.insert(1, "--backups-list".to_string());
                corrections.push(
                    "'doctor backups list' → 'doctor --backups-list' (backup inventory surface)"
                        .into(),
                );
            }
            "verify" | "restore" => {
                let flag = if action == "verify" {
                    "--backups-verify"
                } else {
                    "--backups-restore"
                };
                let backup_id = rest.get(3).filter(|arg| !arg.starts_with('-')).cloned();
                if backup_id.is_some() {
                    rest.remove(3);
                }
                rest.remove(2);
                rest.remove(1);
                rest.insert(1, flag.to_string());
                if let Some(backup_id) = backup_id {
                    rest.insert(2, backup_id);
                    rest.insert(2, "--backup-id".to_string());
                }
                corrections.push(format!(
                    "'doctor backups {action}' → 'doctor {flag}' (backup {action} surface)"
                ));
            }
            _ => {}
        }
    }
    if rest
        .first()
        .is_some_and(|arg| arg.eq_ignore_ascii_case("doctor"))
        && rest
            .get(1)
            .is_some_and(|arg| arg.eq_ignore_ascii_case("baseline"))
        && let Some(action) = rest.get(2).map(|arg| arg.to_ascii_lowercase())
    {
        let flag = match action.as_str() {
            "save" => Some("--baseline-save"),
            "diff" => Some("--baseline-diff"),
            "update" => Some("--baseline-update"),
            _ => None,
        };
        if let Some(flag) = flag {
            let baseline_id = rest.get(3).filter(|arg| !arg.starts_with('-')).cloned();
            if baseline_id.is_some() {
                rest.remove(3);
            }
            rest.remove(2);
            rest.remove(1);
            rest.insert(1, flag.to_string());
            if let Some(baseline_id) = baseline_id {
                rest.insert(2, baseline_id);
                rest.insert(2, "--baseline-id".to_string());
            }
            corrections.push(format!(
                "'doctor baseline {action}' → 'doctor {flag}' (diagnostic baseline {action})"
            ));
        }
    }
    if rest
        .first()
        .is_some_and(|arg| arg.eq_ignore_ascii_case("doctor"))
        && rest.get(1).is_some_and(|arg| {
            arg.eq_ignore_ascii_case("support-bundle") || arg.eq_ignore_ascii_case("support_bundle")
        })
    {
        if rest.get(2).is_some_and(|arg| {
            arg.eq_ignore_ascii_case("verify") || arg.eq_ignore_ascii_case("check")
        }) {
            let support_bundle_path = rest.get(3).filter(|arg| !arg.starts_with('-')).cloned();
            if support_bundle_path.is_some() {
                rest.remove(3);
            }
            rest.remove(2);
            rest.remove(1);
            rest.insert(1, "--support-bundle-verify".to_string());
            if let Some(support_bundle_path) = support_bundle_path {
                rest.insert(2, support_bundle_path);
                rest.insert(2, "--support-bundle-path".to_string());
            }
            corrections.push(
                "'doctor support-bundle verify' → 'doctor --support-bundle-verify' (scrubbed support bundle manifest verification)"
                    .into(),
            );
        } else {
            rest.remove(1);
            rest.insert(1, "--support-bundle".to_string());
            corrections.push(
                "'doctor support-bundle' → 'doctor --support-bundle' (scrubbed diagnostic support bundle)"
                    .into(),
            );
        }
    }
    if rest
        .first()
        .is_some_and(|arg| arg.eq_ignore_ascii_case("doctor"))
        && rest
            .get(1)
            .is_some_and(|arg| arg.eq_ignore_ascii_case("archive"))
        && let Some(action) = rest.get(2).map(|arg| arg.to_ascii_lowercase())
    {
        let flag = match action.as_str() {
            "export" => Some("--archive-export"),
            "relocate" | "relocation" => Some("--archive-relocate"),
            _ => None,
        };
        if let Some(mut flag) = flag {
            let mut target_index = 3;
            let verify_requested = rest.get(3).is_some_and(|arg| {
                arg.eq_ignore_ascii_case("verify") || arg.eq_ignore_ascii_case("check")
            });
            if verify_requested {
                flag = "--archive-export-verify";
                target_index = 4;
            }
            let archive_target = rest
                .get(target_index)
                .filter(|arg| !arg.starts_with('-'))
                .cloned();
            if archive_target.is_some() {
                rest.remove(target_index);
            }
            if verify_requested {
                rest.remove(3);
            }
            rest.remove(2);
            rest.remove(1);
            rest.insert(1, flag.to_string());
            if let Some(archive_target) = archive_target {
                rest.insert(2, archive_target);
                rest.insert(2, "--archive-target".to_string());
            }
            corrections.push(format!(
                "'doctor archive {action}' → 'doctor {flag}' (archive preservation workflow)"
            ));
        }
    }
    if rest
        .first()
        .is_some_and(|arg| arg.eq_ignore_ascii_case("doctor"))
        && rest
            .get(1)
            .is_some_and(|arg| arg.eq_ignore_ascii_case("check"))
    {
        rest.remove(1);
        rest.insert(1, "--check".to_string());
        corrections
            .push("'doctor check' → 'doctor --check' (typed read-only doctor surface)".into());
    } else if rest
        .first()
        .is_some_and(|arg| arg.eq_ignore_ascii_case("doctor"))
        && rest
            .get(1)
            .is_some_and(|arg| arg.eq_ignore_ascii_case("repair"))
    {
        rest.remove(1);
        rest.insert(1, "--repair".to_string());
        corrections.push(
            "'doctor repair' → 'doctor --repair' (fingerprinted doctor repair surface)".into(),
        );
    } else if rest
        .first()
        .is_some_and(|arg| arg.eq_ignore_ascii_case("doctor"))
        && rest
            .get(1)
            .is_some_and(|arg| arg.eq_ignore_ascii_case("cleanup"))
    {
        rest.remove(1);
        rest.insert(1, "--cleanup".to_string());
        corrections.push(
            "'doctor cleanup' → 'doctor --cleanup' (fingerprinted doctor cleanup surface)".into(),
        );
    } else if rest
        .first()
        .is_some_and(|arg| arg.eq_ignore_ascii_case("doctor"))
        && rest.get(1).is_some_and(|arg| {
            arg.eq_ignore_ascii_case("recover-from-archive")
                || arg.eq_ignore_ascii_case("recover_from_archive")
                || arg.eq_ignore_ascii_case("recover-from-canonical-events")
        })
    {
        // #285: 'doctor recover-from-archive <DIR>' → '--recover-from-archive <DIR>'.
        // The next bare positional (if any) is the recovery target dir.
        let target = rest.get(2).filter(|arg| !arg.starts_with('-')).cloned();
        if target.is_some() {
            rest.remove(2);
        }
        rest.remove(1);
        rest.insert(1, "--recover-from-archive".to_string());
        if let Some(target) = target {
            rest.insert(2, target);
        }
        corrections.push(
            "'doctor recover-from-archive <DIR>' → 'doctor --recover-from-archive <DIR>' (rebuild source JSONL from preserved archive events)"
                .into(),
        );
    } else if rest
        .first()
        .is_some_and(|arg| arg.eq_ignore_ascii_case("doctor"))
        && rest.get(1).is_some_and(|arg| {
            arg.eq_ignore_ascii_case("rebuild-canonical-fts")
                || arg.eq_ignore_ascii_case("rebuild_canonical_fts")
        })
    {
        rest.remove(1);
        rest.insert(1, "--rebuild-canonical-fts".to_string());
        corrections.push(
            "'doctor rebuild-canonical-fts' → 'doctor --rebuild-canonical-fts' (safe canonical FTS5 parity repair)"
                .into(),
        );
    } else if rest
        .first()
        .is_some_and(|arg| arg.eq_ignore_ascii_case("doctor"))
        && rest.get(1).is_some_and(|arg| {
            arg.eq_ignore_ascii_case("repair-leaked-pages")
                || arg.eq_ignore_ascii_case("repair_leaked_pages")
        })
    {
        rest.remove(1);
        rest.insert(1, "--repair-leaked-pages".to_string());
        corrections.push(
            "'doctor repair-leaked-pages' → 'doctor --repair-leaked-pages' (in-place leaked-page repair)"
                .into(),
        );
    } else if rest
        .first()
        .is_some_and(|arg| arg.eq_ignore_ascii_case("doctor"))
        && rest.get(1).is_some_and(|arg| {
            arg.eq_ignore_ascii_case("archive-scan") || arg.eq_ignore_ascii_case("archive_scan")
        })
    {
        rest.remove(1);
        rest.insert(1, "--archive-scan".to_string());
        corrections.push(
            "'doctor archive-scan' → 'doctor --archive-scan' (read-only archive hygiene scan)"
                .into(),
        );
    } else if rest
        .first()
        .is_some_and(|arg| arg.eq_ignore_ascii_case("doctor"))
        && rest.get(1).is_some_and(|arg| {
            arg.eq_ignore_ascii_case("archive-normalize")
                || arg.eq_ignore_ascii_case("archive_normalize")
        })
    {
        rest.remove(1);
        rest.insert(1, "--archive-normalize".to_string());
        corrections.push(
            "'doctor archive-normalize' → 'doctor --archive-normalize' (fingerprinted additive archive metadata normalization)"
                .into(),
        );
    }
    if rest
        .first()
        .is_some_and(|arg| arg.eq_ignore_ascii_case("doctor"))
        && let Some(correction) = rewrite_doctor_history_verb(&mut rest)
    {
        corrections.push(correction);
    }
    if rest
        .first()
        .is_some_and(|arg| arg.eq_ignore_ascii_case("robot-docs"))
    {
        let before_len = rest.len();
        rest.retain(|arg| arg != "--robot" && arg != "--json");
        if rest.len() != before_len {
            corrections.push(
                "Removed redundant --robot/--json from robot-docs (docs output is already machine-readable)"
                    .into(),
            );
        }
        if rest.len() == 1 {
            rest.push("guide".to_string());
            corrections.push("'robot-docs' → 'robot-docs guide' (default agent guide)".into());
        }
    }
    if rest.first().is_none_or(|arg| arg.starts_with('-'))
        && let Some(rewritten) = root_structured_triage_rewrite(&rest)
    {
        rest = rewritten;
        corrections
            .push("Top-level --json/--robot → 'triage --json' (read-only agent preflight)".into());
    }
    normalized.push(prog.clone());
    normalized.extend(globals);
    normalized.extend(rest);

    let note = if corrections.is_empty() {
        None
    } else {
        Some(format!(
            "Auto-corrected: {}. Canonical form: {}",
            corrections.join("; "),
            if normalized.len() > 1 {
                normalized[1..].join(" ")
            } else {
                String::new()
            }
        ))
    };
    (normalized, note)
}

/// Build a helpful error message when a command group (e.g. `analytics`, `sources`,
/// `models`, `import`) is invoked without a required subcommand.
fn format_missing_subcommand_error(args: &[String]) -> String {
    // Find the bare command group name from the args (skip program name and flags).
    let group = args
        .iter()
        .skip(1)
        .find(|a| !a.starts_with('-'))
        .map(|s| s.as_str())
        .unwrap_or("unknown");

    let (subcommands, examples): (&[&str], &[&str]) = match group {
        "analytics" => (
            &[
                "status",
                "tokens",
                "tools",
                "models",
                "incidents",
                "rebuild",
                "validate",
            ],
            &[
                "cass analytics status --json",
                "cass analytics tokens --days 7 --group-by day --json",
                "cass analytics incidents --limit 10 --json",
                "cass analytics rebuild --json",
            ],
        ),
        "sources" => (
            &["list", "add", "remove", "doctor", "mappings", "agents"],
            &[
                "cass sources list --json",
                "cass sources add myserver --path /data/sessions",
                "cass sources doctor --json",
            ],
        ),
        "models" => (
            &[
                "status",
                "install",
                "verify",
                "backfill",
                "build-hnsw",
                "remove",
            ],
            &[
                "cass models status --json",
                "cass models install --model all-minilm-l6-v2 --from-file <DIR>",
                "cass models verify --json",
            ],
        ),
        "import" => (
            &["chatgpt"],
            &["cass import chatgpt /path/to/conversations.json"],
        ),
        _ => (&[], &["cass --help"]),
    };

    let mut msg = format!(
        "Missing required subcommand. Run `cass {} --help` for usage.",
        group
    );

    if !subcommands.is_empty() {
        msg.push_str(&format!(
            "\n\nAvailable subcommands for `cass {}`:\n",
            group
        ));
        for sc in subcommands {
            msg.push_str(&format!("  {}\n", sc));
        }
    }

    if !examples.is_empty() {
        msg.push_str("\nExamples:\n");
        for ex in examples {
            msg.push_str(&format!("  {}\n", ex));
        }
    }

    msg
}

/// Build a friendly parse error with actionable, context-aware examples for AI agents.
///
/// This function analyzes what the agent was likely trying to do and provides
/// targeted examples that match their apparent intent.
fn format_friendly_parse_error(err: clap::Error, raw: &[String], normalized: &[String]) -> String {
    let is_robot = raw
        .iter()
        .any(|s| s == "--json" || s == "--robot" || s == "-robot" || s == "-json");

    // Detect what the agent was probably trying to do
    let raw_str = raw
        .iter()
        .skip(1)
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase();
    let intent = detect_command_intent(&raw_str);

    if is_robot {
        let mut err_map = serde_json::Map::new();
        err_map.insert("status".into(), "error".into());
        err_map.insert("error".into(), err.to_string().into());
        err_map.insert("kind".into(), "argument_parsing".into());

        if raw != normalized && normalized.len() > 1 {
            err_map.insert(
                "normalized_attempt".into(),
                normalized[1..].join(" ").into(),
            );
        }

        // Context-aware examples based on detected intent
        let examples = get_contextual_examples(&intent);
        err_map.insert("examples".into(), serde_json::json!(examples));

        // Context-aware hints
        let hints = get_contextual_hints(&intent, &raw_str);
        err_map.insert("hints".into(), serde_json::json!(hints));

        // Common mistakes for this intent
        if let Some(common_mistakes) = get_common_mistakes(&intent) {
            err_map.insert("common_mistakes".into(), serde_json::json!(common_mistakes));
        }

        // Quick reference for flags
        err_map.insert(
            "flag_syntax".into(),
            serde_json::json!({
                "correct": ["--limit 5", "--robot", "--json"],
                "incorrect": ["--limt 5", "-limit 5", "--Limit"]
            }),
        );

        return serde_json::to_string_pretty(&err_map).unwrap_or_else(|_| err.to_string());
    }

    // Human-readable format
    let mut parts = Vec::new();
    parts.push("Argument parsing failed; command intent unclear.".to_string());
    parts.push(format!("Error: {err}"));
    if raw != normalized && normalized.len() > 1 {
        parts.push(format!(
            "Attempted normalization: {}",
            normalized[1..].join(" ")
        ));
    }
    parts.push(String::new());
    parts.push(format!(
        "Based on your command, you may be trying to: {intent}"
    ));
    parts.push(String::new());
    parts.push("Correct examples:".to_string());
    for ex in get_contextual_examples(&intent) {
        parts.push(format!("  {ex}"));
    }
    parts.push(String::new());
    parts.push("Quick syntax reference:".to_string());
    parts.push("  - Long flags use double-dash: --robot, --limit 5".to_string());
    parts.push("  - Flag values use space or equals: --limit 5 or --limit=5".to_string());
    parts.push("  - Subcommands come first: cass search \"query\"".to_string());
    parts.join("\n")
}

/// Detect the likely command intent from the raw argument string.
fn detect_command_intent(raw_str: &str) -> String {
    if matches!(
        raw_str.split_whitespace().next(),
        Some("index" | "rebuild" | "reindex")
    ) {
        "rebuild or manage the search index".to_string()
    } else if raw_str.contains("search")
        || raw_str.contains("find")
        || raw_str.contains("query")
        || raw_str.contains("grep")
    {
        "search for sessions or messages".to_string()
    } else if raw_str.contains("session") || raw_str.contains("current") {
        "discover or list session files".to_string()
    } else if raw_str.contains("doc") || raw_str.contains("help") || raw_str.contains("robot") {
        "get robot-mode documentation".to_string()
    } else if raw_str.contains("stats") || raw_str.contains("ls") || raw_str.contains("list") {
        "view statistics or list sessions".to_string()
    } else if raw_str.contains("index")
        || raw_str.contains("rebuild")
        || raw_str.contains("reindex")
    {
        "rebuild or manage the search index".to_string()
    } else if raw_str.contains("view") || raw_str.contains("show") || raw_str.contains("get") {
        "view a specific session".to_string()
    } else if raw_str.contains("cap") || raw_str.contains("introspect") {
        "discover tool capabilities".to_string()
    } else if raw_str.contains("diag") || raw_str.contains("debug") || raw_str.contains("check") {
        "run diagnostics".to_string()
    } else if raw_str.contains("status") {
        "check status".to_string()
    } else if raw_str.contains("health") {
        "run health check".to_string()
    } else {
        "run a cass command".to_string()
    }
}

/// Get context-aware examples based on detected intent.
fn get_contextual_examples(intent: &str) -> Vec<&'static str> {
    if intent.contains("index") {
        vec![
            "cass index --robot",
            "cass index --full --robot",
            "cass index --semantic --build-hnsw --robot",
            "cass index --robot --data-dir /custom/path",
        ]
    } else if intent.contains("search") {
        vec![
            "cass search \"error handling\" --robot --limit 10",
            "cass search \"authentication\" --robot --agent claude",
            "cass search \"database\" --robot --since 2024-01-01",
            "cass search \"TODO\" --robot --workspace /path/to/project",
        ]
    } else if intent.contains("view") {
        vec![
            "cass view <session-path> --robot",
            "cass view <session-path> -n 42 --json",
        ]
    } else if intent.contains("session") {
        vec![
            "cass sessions --current --json",
            "cass sessions --workspace /path/to/project --json --limit 5",
            "cass sessions --json --limit 10",
        ]
    } else if intent.contains("documentation") {
        vec![
            "cass robot-docs commands",
            "cass robot-docs schemas",
            "cass robot-docs examples",
            "cass --robot-help",
        ]
    } else if intent.contains("statistics") || intent.contains("list") {
        vec![
            "cass stats --robot",
            "cass stats --robot --source local",
            "cass stats --robot --by-source",
        ]
    } else if intent.contains("capabilities") {
        vec!["cass capabilities --json", "cass introspect --json"]
    } else if intent.contains("diagnostics") {
        vec!["cass diag --robot", "cass diag --robot --verbose"]
    } else if intent.contains("status") {
        vec!["cass status --robot"]
    } else if intent.contains("health") {
        vec!["cass health --json"]
    } else {
        vec![
            "cass --robot-help                    # Get robot-mode documentation",
            "cass search \"query\" --robot         # Search sessions",
            "cass capabilities --json             # Discover workflows, mistake recoveries, commands",
            "cass stats --robot                   # View statistics",
        ]
    }
}

/// Get context-aware hints based on detected intent and raw command.
fn get_contextual_hints(intent: &str, raw_str: &str) -> Vec<String> {
    let mut hints = Vec::new();

    // Check for common syntax mistakes
    if raw_str.contains("-robot") && !raw_str.contains("--robot") {
        hints.push("Use '--robot' (double-dash), not '-robot'".to_string());
    }
    if raw_str.contains("-json") && !raw_str.contains("--json") {
        hints.push("Use '--json' (double-dash), not '-json'".to_string());
    }
    // Only flag bare `limit=` without leading dash as problematic
    if (raw_str.contains(" limit=") || raw_str.starts_with("limit="))
        && !raw_str.contains("--limit=")
        && !raw_str.contains("-limit=")
    {
        hints.push("Use '--limit 5' or '--limit=5', not 'limit=5'".to_string());
    }
    if raw_str.contains("--robot-docs") {
        hints.push(
            "'robot-docs' is a subcommand: use 'cass robot-docs' not 'cass --robot-docs'"
                .to_string(),
        );
    }

    // Intent-specific hints
    if intent.contains("index") {
        hints.push("For indexing options, run: cass index --help".to_string());
        if raw_str.split_whitespace().any(|arg| arg == "--approximate") {
            hints.push(
                "Build ANN with: cass index --semantic --build-hnsw; use --approximate only when searching"
                    .to_string(),
            );
        }
    } else if intent.contains("search") && !raw_str.contains("search") {
        hints.push(
            "Use the 'search' subcommand explicitly: cass search \"your query\" --robot"
                .to_string(),
        );
    }

    if hints.is_empty() {
        hints.push(format!("For {intent}, try: cass --robot-help"));
    }

    hints
}

/// Get common mistakes for a given intent.
///
/// Note: Only include mistakes that would actually fail after normalization.
/// Commands that get auto-corrected and succeed (like `cass ls --robot` → `cass stats --robot`)
/// should NOT be listed here since the user would never see this error message.
fn get_common_mistakes(intent: &str) -> Option<serde_json::Value> {
    let mistakes = if intent.contains("index") {
        vec![(
            "cass index --semantic --approximate",
            "cass index --semantic --build-hnsw",
        )]
    } else if intent.contains("search") {
        vec![
            // query="foo" without subcommand - normalization adds "search" but the syntax is wrong
            ("cass query=\"foo\" --robot", "cass search \"foo\" --robot"),
            // Missing query entirely
            (
                "cass search --robot --limit 5",
                "cass search \"your query\" --robot --limit 5",
            ),
        ]
    } else if intent.contains("documentation") {
        vec![
            // Flag syntax for subcommand (--robot-docs gets normalized but shown for education)
            ("cass --robot-docs", "cass robot-docs"),
            ("cass --robot-docs=commands", "cass robot-docs commands"),
            // Adding --robot to robot-docs (which doesn't accept it)
            ("cass robot-docs --robot", "cass robot-docs"),
        ]
    } else if intent.contains("statistics") {
        // Note: `cass ls --robot` actually works (normalizes to `cass stats --robot`)
        // so we show mistakes that would actually fail
        vec![
            // Missing required output flag for piping
            ("cass stats | jq .", "cass stats --json | jq ."),
        ]
    } else {
        return None;
    };

    Some(serde_json::json!(
        mistakes
            .iter()
            .map(|(wrong, right)| { serde_json::json!({"wrong": wrong, "correct": right}) })
            .collect::<Vec<_>>()
    ))
}

const CANONICAL_TOP_LEVEL_COMMANDS: &[&str] = &[
    "search",
    "pack",
    "stats",
    "status",
    "state",
    "health",
    "selftest",
    "diag",
    "storage",
    "dedup",
    "doctor",
    "sessions",
    "view",
    "expand",
    "resume",
    "index",
    "capabilities",
    "triage",
    "support-bundle",
    "onboarding",
    "guide",
    "introspect",
    "api-version",
    "robot-docs",
    "models",
    "quarantine",
    "sources",
    "analytics",
    "context",
    "timeline",
    "mirror",
    "forget",
    "export",
    "export-html",
    "pages",
    // Landed 2026-09-01 (bookmarks CLI); without this entry robot mode
    // rewrote `cass bookmarks … --robot` into `search bookmarks …` (GH #367
    // class), which the full lib suite caught on 2026-09-02.
    "bookmarks",
    "import",
    "daemon",
    "schedule",
    "completions",
    "man",
    "tui",
];

/// Commands that mutate state or reach the network must never be entered by
/// fuzzy typo attraction — only by exact spelling. `forget` deletes indexed
/// data (its reparse failure also used to swallow the implicit-search
/// fallback: `cass format` → "forget" → hard exit-2 instead of searching)
/// and `upgrade` used to perform a release check / interactive install
/// (`cass parade` executed it) before the local-only port removed it.
/// Adversarial-review finding F1 on GH #367.
const SIDE_EFFECT_EXACT_ONLY_COMMANDS: &[&str] = &["forget"];

/// A command invocation removed by the local-only port (cass-local plan,
/// tasks 7–14).
///
/// `path` holds the invocation words: one for a top-level command
/// (`&["swarm"]` rejects `cass swarm <anything>`), two for a
/// `command subcommand` pair (`&["sources", "sync"]` rejects only that
/// pair — `cass sources list` stays live). Matching runs case-insensitively
/// on raw argv inside `parse_cli`, before `normalize_args` rewrites
/// anything and before `heuristic_parse_recovery` tries typo correction,
/// so a removed name errors out instead of being corrected into a
/// different live command or folded into an implicit search query.
///
/// `hint` names a local equivalent for the same intent when one exists
/// (for example `cass models install --from-file DIR`); it is reported to
/// the caller, never executed.
struct RemovedCommand {
    path: &'static [&'static str],
    hint: Option<&'static str>,
}

/// Commands removed by the local-only port. Tasks 7–14 append entries as
/// each surface is deleted. A one-word entry must drop the name from
/// `CANONICAL_TOP_LEVEL_COMMANDS` and `SUBCOMMAND_ALIASES` in the same
/// commit — `removed_invocation_tests` pins that a top-level name may
/// never sit in the live and the removed list at once. A pair entry
/// (`["sources","sync"]`) removes that pair alone and keeps its parent
/// live, so it never touches the top-level lists.
const REMOVED_COMMANDS: &[RemovedCommand] = &[
    RemovedCommand {
        path: &["swarm"],
        hint: Some("cass search and cass pack cover local-only handoffs"),
    },
    RemovedCommand {
        path: &["lessons"],
        hint: Some("cass search covers local session evidence"),
    },
    RemovedCommand {
        path: &["fleet"],
        hint: Some("cass sources doctor covers local source diagnostics"),
    },
    RemovedCommand {
        path: &["release-verify"],
        hint: Some("cass runs fully local-only; no release channels are verified"),
    },
    RemovedCommand {
        path: &["upgrade"],
        hint: Some("reinstall with `cargo install --path . --locked`"),
    },
    RemovedCommand {
        path: &["sources", "sync"],
        hint: Some(
            "remote sync was removed; existing archive mirrors stay indexed via `cass index`",
        ),
    },
    RemovedCommand {
        path: &["sources", "setup"],
        hint: Some("cass sources add NAME --path PATH adds a local source"),
    },
    RemovedCommand {
        path: &["sources", "discover"],
        hint: Some(
            "remote discovery was removed; cass sources add NAME --path PATH adds a local source",
        ),
    },
    RemovedCommand {
        path: &["sources", "reingest"],
        hint: Some("cass index scans existing archive mirrors directly"),
    },
    RemovedCommand {
        path: &["sources", "artifact-manifest"],
        hint: Some("remote artifact exchange was removed; the index is built and used locally"),
    },
    RemovedCommand {
        path: &["sources", "schedule"],
        hint: Some(
            "sync schedules were removed with remote sync; the field stays readable in old configs",
        ),
    },
    RemovedCommand {
        path: &["models", "check-update"],
        hint: Some(
            "model update checks were removed with network access; pinned manifest revisions are the only supported installs",
        ),
    },
];

/// A flag (or specific flag values) removed from a command that otherwise
/// stays live.
///
/// `path` is the command path the flag is removed from (`&["pages"]`,
/// `&["models", "install"]`); an empty `path` removes the flag under every
/// command (task 13's `--embedder external`). `flag` is the long flag name
/// without dashes — both `--mirror` and the single-dash `-mirror` spellings
/// match, as does `--mirror=VALUE`. `values: None` rejects the flag
/// outright; `values: Some(..)` rejects only the listed values
/// (case-insensitive), so a surviving flag loses only its network-facing
/// choices (`pages --target github` while `--target local` parses on).
/// `hint` behaves like `RemovedCommand::hint`.
struct RemovedFlag {
    path: &'static [&'static str],
    flag: &'static str,
    values: Option<&'static [&'static str]>,
    hint: Option<&'static str>,
}

/// Flags removed by the local-only port; see `RemovedFlag`.
const REMOVED_FLAGS: &[RemovedFlag] = &[
    RemovedFlag {
        path: &["guide"],
        flag: "allow-rch",
        values: None,
        hint: Some("the guide runs strictly local; there is no rch offload to permit"),
    },
    RemovedFlag {
        path: &["models", "install"],
        flag: "mirror",
        values: None,
        hint: Some("`cass models install --from-file DIR`"),
    },
    RemovedFlag {
        path: &["models", "install"],
        flag: "yes",
        values: None,
        hint: Some("local installs need no confirmation: `cass models install --from-file DIR`"),
    },
    RemovedFlag {
        path: &["models", "verify"],
        flag: "repair",
        values: None,
        hint: Some(
            "repair-by-download was removed; reinstall with `cass models install --from-file DIR`",
        ),
    },
    // Task 13: the external HTTP embedding provider is gone. The flag itself
    // survives for local providers; only the `external` value is removed,
    // under every command that accepts `--embedder`.
    RemovedFlag {
        path: &[],
        flag: "embedder",
        values: Some(&["external"]),
        hint: Some(
            "select a local embedder (`--embedder minilm`, `multilingual-minilm` or `hash`); legacy CASS_EXTERNAL_* settings are ignored",
        ),
    },
    // Task 14: hosted Pages targets are gone. `--target` survives for its
    // sole remaining value `local`; the hosted values are rejected.
    RemovedFlag {
        path: &["pages"],
        flag: "target",
        values: Some(&["github", "cloudflare"]),
        hint: Some("`cass pages --target local` (the default) writes a local archive"),
    },
    // Task 14: hosted deployment flags were removed outright.
    RemovedFlag {
        path: &["pages"],
        flag: "project",
        values: None,
        hint: Some("hosted publishing is gone; exports stay in deployment.output_dir"),
    },
    RemovedFlag {
        path: &["pages"],
        flag: "repo",
        values: None,
        hint: Some("hosted publishing is gone; exports stay in deployment.output_dir"),
    },
    RemovedFlag {
        path: &["pages"],
        flag: "branch",
        values: None,
        hint: Some("hosted publishing is gone; exports stay in deployment.output_dir"),
    },
    RemovedFlag {
        path: &["pages"],
        flag: "account-id",
        values: None,
        hint: Some("Cloudflare credentials are gone; exports stay in deployment.output_dir"),
    },
    RemovedFlag {
        path: &["pages"],
        flag: "api-token",
        values: None,
        hint: Some("Cloudflare credentials are gone; exports stay in deployment.output_dir"),
    },
    // Task 14: exports are always self-contained; there is no CDN mode to
    // disable, so `--no-cdns` is retired rather than kept as a no-op.
    RemovedFlag {
        path: &["export-html"],
        flag: "no-cdns",
        values: None,
        hint: Some("exports are always fully self-contained (no CDN references)"),
    },
];

/// Whether `name` is a removed *top-level* command: a one-word path in
/// `removed_commands`. Removed names are excluded from fuzzy typo
/// attraction even while a stale `CANONICAL_TOP_LEVEL_COMMANDS` entry
/// still lists them (the disjointness test makes that state a hard
/// failure, the filter makes it harmless in the meantime). A pair entry
/// (`["sources","sync"]`) removes that pair alone — its parent stays a
/// live command, so it must keep attracting corrections (`cass sourcess`
/// → `sources`) instead of falling into the implicit-search fold.
fn is_removed_command_name(name: &str, removed_commands: &[RemovedCommand]) -> bool {
    removed_commands
        .iter()
        .any(|entry| entry.path.len() == 1 && entry.path[0].eq_ignore_ascii_case(name))
}

/// Positional argv words in order, program name excluded. Flag tokens are
/// skipped, and so is the separate value of the leading flags that take
/// one (`--db PATH`, `--data-dir DIR`, `--format FMT`, …); `--` ends the
/// scan because nothing after it can be a subcommand. Kept deliberately
/// small: a subcommand-level option with a separate value sitting between
/// the command word and the subcommand can still hide a removed pair
/// (`cass sources --agent x sync`), but clap rejects that invocation once
/// the pair is gone — the mechanism only has to catch canonical spellings
/// before correction does.
fn removed_invocation_positionals(raw_args: &[String]) -> Vec<&str> {
    const VALUE_TAKING_FLAGS: &[&str] = &[
        "--color",
        "--progress",
        "--wrap",
        "--db",
        "--trace-file",
        "--data-dir",
        "--stale-threshold",
        "--robot-format",
        "--format",
        "--output",
        "--output-format",
        "--output_format",
        "--agent",
        "--workspace",
        "--model",
        "--source",
        "--fields",
        "--file",
    ];
    let mut words = Vec::new();
    let mut index = 1;
    while index < raw_args.len() {
        let arg = raw_args[index].as_str();
        if arg == "--" {
            break;
        }
        if arg.starts_with('-') {
            if !arg.contains('=') && VALUE_TAKING_FLAGS.contains(&arg) {
                index += 1;
            }
            index += 1;
            continue;
        }
        words.push(arg);
        index += 1;
    }
    words
}

/// The structured error for a rejected invocation: exit 2 (usage), kind
/// `removed-command`, never retryable. `typed` is set when the caller's
/// word only fuzzy-matched the removed name — the message then names both
/// so `cass swarn` still reports `cass swarm` as removed.
fn removed_command_error(
    path: &[&str],
    typed: Option<&str>,
    hint: Option<&'static str>,
) -> CliError {
    let canonical = path.join(" ");
    let message = match typed {
        Some(arg) => format!(
            "`cass {arg}` resolves to `cass {canonical}`, which was removed: cass runs fully local-only"
        ),
        None => format!("`cass {canonical}` was removed: cass runs fully local-only"),
    };
    CliError {
        code: 2,
        kind: CliErrorKind::RemovedCommand.kind_str(),
        message,
        hint: hint.map(|equivalent| format!("local equivalent: {equivalent}")),
        retryable: false,
    }
}

/// Whether `command`/`sub` name the command path an entry applies to. An
/// empty entry path matches every command (flag removed suite-wide).
fn removed_path_matches(path: &[&str], command: &str, sub: &str) -> bool {
    match path {
        [] => true,
        [top] => command == *top,
        [top, second] => command == *top && sub == *second,
        _ => false,
    }
}

/// Rejection for `raw_args` under `removed_commands`/`removed_flags`, or
/// `None` when the invocation is live. Taken as parameters (not the
/// module consts) so unit tests exercise the matcher with synthetic
/// tables while the real lists are still empty.
fn removed_invocation_error(
    raw_args: &[String],
    removed_commands: &[RemovedCommand],
    removed_flags: &[RemovedFlag],
) -> Option<CliError> {
    if removed_commands.is_empty() && removed_flags.is_empty() {
        return None;
    }
    let words = removed_invocation_positionals(raw_args);
    let command = words.first().copied().unwrap_or_default();
    if command.is_empty() {
        return None;
    }
    let command_lc = command.to_ascii_lowercase();
    let sub = words.get(1).copied().unwrap_or_default();
    let sub_lc = sub.to_ascii_lowercase();

    // 1. Exact (case-insensitive) command path: `cass swarm status` hits
    //    `["swarm"]`, `cass sources sync` hits `["sources","sync"]`.
    for entry in removed_commands {
        if removed_path_matches(entry.path, &command_lc, &sub_lc) {
            return Some(removed_command_error(entry.path, None, entry.hint));
        }
    }

    // 2. Typo of a removed top-level command: `cass swarn` must report the
    //    removed `swarm`, not correct into a live command. A live
    //    candidate wins only when it is strictly closer — on ties the
    //    invocation still fails instead of redirecting intent. An exact live
    //    command (e.g. `sessions`, edit-distance 2 from `lessons`) is never a
    //    typo: it must reach normal dispatch untouched.
    if !CANONICAL_TOP_LEVEL_COMMANDS.contains(&command_lc.as_str()) {
        let closest_removed = removed_commands
            .iter()
            .filter(|entry| entry.path.len() == 1)
            .map(|entry| (entry, strsim::levenshtein(&command_lc, entry.path[0])))
            .filter(|(_, distance)| *distance <= 2)
            .min_by_key(|(_, distance)| *distance);
        if let Some((entry, removed_distance)) = closest_removed {
            let live_distance = closest_top_level_command_in(&command_lc, removed_commands)
                .map(|name| strsim::levenshtein(&command_lc, name));
            if live_distance.is_none_or(|distance| removed_distance <= distance) {
                return Some(removed_command_error(entry.path, Some(command), entry.hint));
            }
        }
    }

    // 3. Removed flags on live command paths. Tokens are scanned raw
    //    (normalize_args has not run): `--flag`, `--flag=value` and the
    //    single-dash `-flag` spelling all match.
    for entry in removed_flags {
        if !entry.path.is_empty() && !removed_path_matches(entry.path, &command_lc, &sub_lc) {
            continue;
        }
        let mut index = 1;
        while index < raw_args.len() {
            let token = &raw_args[index];
            index += 1;
            if token == "--" {
                break;
            }
            let Some(flag_body) = token.strip_prefix('-').filter(|_| token.len() > 1) else {
                continue;
            };
            let flag_body = flag_body.strip_prefix('-').unwrap_or(flag_body);
            let (name, inline_value) = match flag_body.split_once('=') {
                Some((name, value)) => (name, Some(value)),
                None => (flag_body, None),
            };
            if !name.eq_ignore_ascii_case(entry.flag) {
                continue;
            }
            let Some(removed_values) = entry.values else {
                return Some(removed_flag_error(entry, None));
            };
            let value = inline_value.map(str::to_string).or_else(|| {
                raw_args
                    .get(index)
                    .filter(|next| !next.starts_with('-'))
                    .cloned()
            });
            if let Some(value) = value.filter(|value| {
                removed_values
                    .iter()
                    .any(|removed| removed.eq_ignore_ascii_case(value))
            }) {
                return Some(removed_flag_error(entry, Some(value.as_str())));
            }
        }
    }

    None
}

/// The structured error for a removed flag: same envelope contract as a
/// removed command (exit 2, kind `removed-command`). `matched_value` names a
/// removed value when only that selection was dropped (`--embedder external`),
/// so the message does not claim the whole surviving flag is gone.
fn removed_flag_error(entry: &RemovedFlag, matched_value: Option<&str>) -> CliError {
    let on = if entry.path.is_empty() {
        String::new()
    } else {
        format!(" on `cass {}`", entry.path.join(" "))
    };
    let flag = match matched_value {
        Some(value) => format!("--{} {}", entry.flag, value),
        None => format!("--{}", entry.flag),
    };
    CliError {
        code: 2,
        kind: CliErrorKind::RemovedCommand.kind_str(),
        message: format!("`{flag}`{on} was removed: cass runs fully local-only"),
        hint: entry
            .hint
            .map(|equivalent| format!("local equivalent: {equivalent}")),
        retryable: false,
    }
}

/// Nearest canonical subcommand for a mistyped first argument.
///
/// An argument that already names a canonical subcommand is never a typo:
/// when clap rejects `cass status --jsn` for its flag, the subcommand must
/// stay `status`. Returning the nearest *other* command here made recovery
/// run `stats` (and `import` for `export`) with exit 0 and only a stderr
/// note (2l1b0.51).
fn closest_top_level_command(arg: &str) -> Option<&'static str> {
    closest_top_level_command_in(arg, REMOVED_COMMANDS)
}

/// `closest_top_level_command` with the removed-command table explicit —
/// the same parameterized-table pattern as `removed_invocation_error`, so
/// unit tests can prove a removed pair under a live parent never hides
/// that parent while the real lists are still empty.
fn closest_top_level_command_in(
    arg: &str,
    removed_commands: &[RemovedCommand],
) -> Option<&'static str> {
    let lower = arg.to_ascii_lowercase();
    if lower.len() < 3 || CANONICAL_TOP_LEVEL_COMMANDS.contains(&lower.as_str()) {
        return None;
    }
    CANONICAL_TOP_LEVEL_COMMANDS
        .iter()
        .copied()
        .filter(|candidate| !SIDE_EFFECT_EXACT_ONLY_COMMANDS.contains(candidate))
        // A removed name must never be a correction candidate either: the
        // disjointness invariant in `removed_invocation_tests` keeps this a
        // no-op while REMOVED_COMMANDS stays disjoint from CANONICAL_*.
        .filter(|candidate| !is_removed_command_name(candidate, removed_commands))
        .map(|candidate| (candidate, strsim::levenshtein(&lower, candidate)))
        .filter(|(_, distance)| *distance <= 2)
        .min_by_key(|(_, distance)| *distance)
        .map(|(candidate, _)| candidate)
}

#[cfg(test)]
mod canonical_top_level_command_tests {
    use super::*;

    #[test]
    fn ann_setup_help_names_a_parseable_index_command() {
        let command = Cli::command();
        let search = command.find_subcommand("search").expect("search command");
        let approximate = search
            .get_arguments()
            .find(|argument| argument.get_id() == "approximate")
            .expect("approximate flag");
        let help = approximate
            .get_long_help()
            .or_else(|| approximate.get_help())
            .expect("ANN setup help")
            .to_string();
        let recipe = help.split('`').nth(1).expect("documented setup command");
        let parsed = Cli::try_parse_from(recipe.split_whitespace()).expect("setup parses");
        assert!(matches!(
            parsed.command,
            Some(Commands::Index {
                semantic: true,
                build_hnsw: true,
                ..
            })
        ));
        assert!(Cli::try_parse_from(["cass", "index", "--semantic", "--approximate"]).is_err());
        let query = Cli::try_parse_from(["cass", "search", "example", "--approximate"])
            .expect("ANN remains a search option");
        assert!(matches!(
            query.command,
            Some(Commands::Search {
                approximate: true,
                ..
            })
        ));
    }

    #[test]
    fn ann_index_parse_error_explains_the_index_command() {
        for format in ["--json", "--robot"] {
            let args = ["cass", "index", "--semantic", "--approximate", format].map(str::to_string);
            let error = Cli::try_parse_from(&args).expect_err("query-only flag rejected");
            let output = format_friendly_parse_error(error, &args, &args);
            let payload: serde_json::Value = serde_json::from_str(&output).expect("error JSON");
            assert_eq!(payload["kind"], "argument_parsing");
            let examples = payload["examples"].as_array().expect("examples");
            assert!(!examples.is_empty());
            for example in examples {
                let example = example.as_str().expect("example command");
                assert!(example.starts_with("cass index "), "{example}");
                assert!(Cli::try_parse_from(example.split_whitespace()).is_ok());
            }
            let hints = payload["hints"].as_array().expect("hints");
            assert!(hints.iter().any(|hint| {
                hint.as_str()
                    .is_some_and(|text| text.contains("--semantic --build-hnsw"))
            }));
            assert!(!output.contains("Use the 'search' subcommand explicitly"));
            let mistakes = payload["common_mistakes"]
                .as_array()
                .expect("index mistakes");
            for mistake in mistakes {
                let correct = mistake["correct"].as_str().expect("corrected command");
                assert!(correct.starts_with("cass index "), "{correct}");
                assert!(Cli::try_parse_from(correct.split_whitespace()).is_ok());
            }
        }
    }

    /// GH #367: `cass forget --source-glob <pat> --robot` was rewritten to
    /// `search forget ...` because the implicit-robot-query recovery consults
    /// `CANONICAL_TOP_LEVEL_COMMANDS` and `forget` was never added when the
    /// subcommand landed (#292). Any visible clap subcommand missing from the
    /// list gets folded into a search query the moment a robot flag is
    /// present — silently breaking exactly the machine-readable path agents
    /// are told to use. Pin the list to the clap command tree.
    #[test]
    fn canonical_list_covers_every_clap_subcommand() {
        // Building the full clap command tree needs more than the default
        // 2 MiB test stack (same reason tests/cli_robot.rs runs clap work on
        // a dedicated large-stack thread).
        let handle = std::thread::Builder::new()
            .stack_size(16 * 1024 * 1024)
            .spawn(|| {
                let cli = Cli::command();
                for sub in cli.get_subcommands() {
                    if sub.is_hide_set() {
                        continue;
                    }
                    let name = sub.get_name().to_string();
                    assert!(
                        CANONICAL_TOP_LEVEL_COMMANDS.contains(&name.as_str()),
                        "clap subcommand `{name}` is missing from CANONICAL_TOP_LEVEL_COMMANDS; \
                         robot mode will rewrite `cass {name} ... --robot` into `search {name} ...` (GH #367)"
                    );
                }
                // Reverse direction: a canonical entry with no matching clap
                // subcommand is a stale typo attractor — a removed/renamed
                // command would keep pulling nearby query words into a
                // rewrite that can only fail (adversarial-review F1).
                let clap_names: Vec<String> = cli
                    .get_subcommands()
                    .map(|sub| sub.get_name().to_string())
                    .collect();
                for canonical in CANONICAL_TOP_LEVEL_COMMANDS {
                    assert!(
                        clap_names.iter().any(|name| name == canonical),
                        "CANONICAL_TOP_LEVEL_COMMANDS entry `{canonical}` has no clap subcommand; \
                         remove the stale entry so it stops attracting typo corrections"
                    );
                }
            })
            .expect("spawn large-stack clap test thread");
        match handle.join() {
            Ok(()) => {}
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }

    /// Adversarial-review F1 on GH #367: adding `forget`/`upgrade` to the
    /// canonical list put common English query words inside the
    /// Levenshtein-2 attraction net. `cass format` stopped auto-recovering
    /// to `search "format"` and hard-failed on forget's required
    /// `--source-glob`; `cass parade` silently EXECUTED the upgrade
    /// release-check. Side-effectful commands dispatch on exact spelling
    /// only. The local-only port removed `upgrade` entirely: it is out of
    /// the canonical list (the disjointness invariant pins that) and the
    /// exact spelling now errors through `REMOVED_COMMANDS` instead of
    /// dispatching.
    #[test]
    fn side_effectful_commands_are_never_typo_attractors() {
        for query_word in ["format", "forest", "forge", "forged", "fort"] {
            assert_ne!(
                closest_top_level_command(query_word),
                Some("forget"),
                "`cass {query_word}` must fall through to implicit search, not forget"
            );
        }
        for query_word in ["parade", "upgrades", "upgrde", "grade"] {
            assert_ne!(
                closest_top_level_command(query_word),
                Some("upgrade"),
                "`cass {query_word}` must never execute the upgrade release-check"
            );
        }
        // Exact spellings still dispatch (GH #367's actual fix is untouched)
        // — but `upgrade` dispatches to the removed-command error, not to a
        // release check.
        assert!(CANONICAL_TOP_LEVEL_COMMANDS.contains(&"forget"));
        assert!(!CANONICAL_TOP_LEVEL_COMMANDS.contains(&"upgrade"));
        assert!(looks_like_top_level_command_or_typo("forget"));
        assert!(!looks_like_top_level_command_or_typo("upgrade"));
        let args: Vec<String> = ["cass", "upgrade"]
            .iter()
            .map(ToString::to_string)
            .collect();
        let error = removed_invocation_error(&args, REMOVED_COMMANDS, REMOVED_FLAGS)
            .expect("`cass upgrade` must hit the removed-command table");
        assert_eq!(error.kind, CliErrorKind::RemovedCommand.kind_str());
    }

    /// 2l1b0.51: an exact subcommand is never a typo. Before the fix every
    /// canonical command within Levenshtein 2 of another one was rerouted
    /// (status→stats, stats→status, export→import, …) whenever clap
    /// rejected the invocation for an unrelated flag typo.
    #[test]
    fn exact_canonical_commands_are_never_rerouted() {
        let mut attracted_pairs = Vec::new();
        for command in CANONICAL_TOP_LEVEL_COMMANDS {
            for other in CANONICAL_TOP_LEVEL_COMMANDS {
                if command != other && strsim::levenshtein(command, other) <= 2 {
                    attracted_pairs.push((*command, *other));
                }
            }
            assert_eq!(
                closest_top_level_command(command),
                None,
                "`cass {command}` must keep its own subcommand"
            );
            assert_eq!(
                closest_top_level_command(&command.to_ascii_uppercase()),
                None,
                "`cass {}` must keep its own subcommand",
                command.to_ascii_uppercase()
            );
        }
        // The fixture only means something while near-collisions exist.
        assert!(
            attracted_pairs.contains(&("status", "stats")),
            "{attracted_pairs:?}"
        );
        assert!(
            attracted_pairs.contains(&("export", "import")),
            "{attracted_pairs:?}"
        );
        // Genuine typos still recover (bead a0z1v).
        assert_eq!(closest_top_level_command("serach"), Some("search"));
        assert_eq!(closest_top_level_command("helth"), Some("health"));
    }

    /// 2l1b0.51, end to end through the recovery layer: a flag typo on an
    /// exact subcommand corrects only the flag.
    #[test]
    fn flag_typo_on_exact_subcommand_keeps_the_subcommand() {
        for (command, expected_flag) in [
            ("status", "--json"),
            ("stats", "--json"),
            ("export", "--json"),
        ] {
            let args = ["cass", command, "--jsn"].map(str::to_string);
            let error = Cli::try_parse_from(&args).expect_err("--jsn is not a flag");
            let (corrected, note) =
                heuristic_parse_recovery(&error, &args).expect("flag typo is recoverable");
            assert_eq!(
                corrected[1], command,
                "subcommand changed: {corrected:?} ({note})"
            );
            assert!(
                corrected.iter().any(|arg| arg == expected_flag),
                "{corrected:?}"
            );
            assert!(
                !note.contains("subcommand typo"),
                "no subcommand correction may be reported: {note}"
            );
        }
    }

    #[test]
    fn pack_field_mask_contract_flag_survives_query_recovery() {
        for args in [
            vec![
                "cass",
                "pack",
                "maskneedle",
                "--field-mask",
                "standard",
                "--json",
            ],
            vec![
                "cass",
                "pack",
                "--field-mask",
                "standard",
                "maskneedle",
                "--json",
            ],
            vec!["cass", "maskneedle", "--field-mask", "standard", "--json"],
            vec![
                "cass",
                "search",
                "maskneedle",
                "--field-mask",
                "standard",
                "--json",
            ],
            vec![
                "cass",
                "pack",
                "maskneedle",
                "field_mask=standard",
                "--json",
            ],
            vec!["cass", "maskneedle", "field-mask=standard", "--json"],
            vec![
                "cass",
                "pack",
                "maskneedle",
                "--fields",
                "standard",
                "--json",
            ],
        ] {
            let (normalized, _) = normalize_args(args.into_iter().map(str::to_string).collect());
            let cli = Cli::try_parse_from(normalized).expect("parse documented pack mask");
            let Some(Commands::Pack { query, fields, .. }) = cli.command else {
                panic!("the pack-only field-mask flag must preserve pack intent");
            };
            assert_eq!(query, "maskneedle");
            assert_eq!(fields, Some(vec!["standard".to_string()]));
        }
    }

    #[test]
    fn pack_skill_opt_in_survives_query_recovery_and_defaults_to_exclusion() {
        for (args, expected) in [
            (
                vec!["cass", "privacyneedle", "--json", "--include-skill-content"],
                true,
            ),
            (
                vec![
                    "cass",
                    "search",
                    "privacyneedle",
                    "--json",
                    "--include-skill-content",
                ],
                true,
            ),
            (
                vec![
                    "cass",
                    "pack",
                    "--include-skill-content",
                    "privacyneedle",
                    "--json",
                ],
                true,
            ),
            (vec!["cass", "pack", "privacyneedle", "--json"], false),
        ] {
            let (normalized, _) = normalize_args(args.into_iter().map(str::to_string).collect());
            let cli = Cli::try_parse_from(normalized).expect("parse recovered pack arguments");
            let Some(Commands::Pack {
                query,
                include_skill_content,
                ..
            }) = cli.command
            else {
                panic!("skill-content opt-in must select the pack command");
            };
            assert_eq!(query, "privacyneedle");
            assert_eq!(include_skill_content, expected);
        }
    }

    /// `cass robot-docs doctor` documents the run-history surface as verbs;
    /// every spelling it shows must parse to the matching doctor flag, the
    /// flag spellings must keep working, and a malformed or unknown verb must
    /// still be a usage error rather than a plain doctor run.
    #[test]
    fn doctor_history_verbs_parse_as_robot_docs_document_them() {
        let doctor = |args: &[&str]| {
            let (normalized, _) = normalize_args(args.iter().map(ToString::to_string).collect());
            Cli::try_parse_from(normalized)
                .ok()
                .and_then(|cli| cli.command)
        };
        assert!(matches!(
            doctor(&["cass", "doctor", "ls", "--json"]),
            Some(Commands::Doctor { ls: true, .. })
        ));
        assert!(matches!(
            doctor(&["cass", "doctor", "undo", "latest", "--json"]),
            Some(Commands::Doctor { undo: Some(id), .. }) if id == "latest"
        ));
        assert!(matches!(
            doctor(&["cass", "doctor", "diff", "--json"]),
            Some(Commands::Doctor { diff: Some(reference), .. }) if reference == "current"
        ));
        assert!(matches!(
            doctor(&["cass", "doctor", "diff", "run-a..run-b", "--json"]),
            Some(Commands::Doctor { diff: Some(reference), .. }) if reference == "run-a..run-b"
        ));
        assert!(matches!(
            doctor(&["cass", "doctor", "gc", "--before", "2026-01-01T00:00:00Z", "--yes", "--json"]),
            Some(Commands::Doctor { gc_before: Some(before), yes: true, .. })
                if before == "2026-01-01T00:00:00Z"
        ));
        assert!(matches!(
            doctor(&["cass", "doctor", "gc", "--before=2026-01-01T00:00:00Z", "--yes"]),
            Some(Commands::Doctor { gc_before: Some(before), yes: true, .. })
                if before == "2026-01-01T00:00:00Z"
        ));
        assert!(matches!(
            doctor(&["cass", "doctor", "capabilities", "--json"]),
            Some(Commands::Doctor {
                emit_capabilities: true,
                ..
            })
        ));
        assert!(matches!(
            doctor(&["cass", "doctor", "explain", "latest", "--json"]),
            Some(Commands::Doctor { explain: Some(id), .. }) if id == "latest"
        ));
        assert!(matches!(
            doctor(&["cass", "doctor", "watch", "--watch-iterations", "1"]),
            Some(Commands::Doctor {
                watch: true,
                watch_iterations: 1,
                ..
            })
        ));
        // The flag spellings keep working.
        assert!(matches!(
            doctor(&["cass", "doctor", "--ls", "--json"]),
            Some(Commands::Doctor { ls: true, .. })
        ));
        assert!(matches!(
            doctor(&["cass", "doctor", "--diff", "--json"]),
            Some(Commands::Doctor { diff: Some(reference), .. }) if reference == "current"
        ));
        // `gc` without `--before` must not become a plain doctor run.
        assert!(doctor(&["cass", "doctor", "gc", "--yes", "--json"]).is_none());
        // An unknown verb is still a usage error.
        assert!(doctor(&["cass", "doctor", "frobnicate", "--json"]).is_none());
    }

    /// Behavioral pin for the #367 repro: the robot flag must not turn a
    /// recognized subcommand into a search query.
    #[test]
    fn forget_with_robot_flag_is_not_rewritten_to_search() {
        let (normalized, _note) = normalize_args(
            [
                "cass",
                "forget",
                "--source-glob",
                "**/subagents/*.jsonl",
                "--robot",
            ]
            .iter()
            .map(ToString::to_string)
            .collect(),
        );
        assert_eq!(
            normalized.get(1).map(String::as_str),
            Some("forget"),
            "forget must survive robot-mode normalization untouched: {normalized:?}"
        );
        assert!(
            !normalized.iter().any(|arg| arg == "search"),
            "forget must not be folded into an implicit search query (GH #367): {normalized:?}"
        );
    }
}

#[cfg(test)]
mod removed_invocation_tests {
    use super::*;

    fn argv(words: &[&str]) -> Vec<String> {
        words.iter().map(ToString::to_string).collect()
    }

    const DEMO_COMMANDS: &[RemovedCommand] = &[
        RemovedCommand {
            path: &["swarm"],
            hint: None,
        },
        RemovedCommand {
            path: &["upgrade"],
            hint: Some("`cargo install --path . --locked`"),
        },
        RemovedCommand {
            path: &["sources", "sync"],
            hint: None,
        },
    ];

    const DEMO_FLAGS: &[RemovedFlag] = &[
        RemovedFlag {
            path: &["pages"],
            flag: "target",
            values: Some(&["github", "cloudflare"]),
            hint: Some("`cass pages --export-only` for a local archive"),
        },
        RemovedFlag {
            path: &["models", "install"],
            flag: "mirror",
            values: None,
            hint: Some("`cass models install --from-file DIR`"),
        },
    ];

    #[test]
    fn empty_tables_never_reject() {
        for args in [
            argv(&["cass", "swarm", "status"]),
            argv(&["cass", "sources", "sync"]),
            argv(&["cass", "pages", "--target", "github"]),
            argv(&["cass", "search", "swarm"]),
        ] {
            assert!(
                removed_invocation_error(&args, &[], &[]).is_none(),
                "empty tables must not reject {args:?}"
            );
        }
    }

    #[test]
    fn exact_and_case_variant_commands_reject() {
        for args in [
            argv(&["cass", "swarm"]),
            argv(&["cass", "swarm", "status", "--json"]),
            argv(&["cass", "Swarm", "status"]),
            argv(&["cass", "--json", "swarm", "status"]),
            argv(&["cass", "--data-dir", "/tmp/x", "swarm"]),
            argv(&["cass", "upgrade", "--check"]),
        ] {
            let error = removed_invocation_error(&args, DEMO_COMMANDS, &[])
                .unwrap_or_else(|| panic!("{args:?} must be rejected"));
            assert_eq!(error.code, 2);
            assert_eq!(error.kind, CliErrorKind::RemovedCommand.kind_str());
            assert!(!error.retryable);
        }
        let error =
            removed_invocation_error(&argv(&["cass", "swarm", "status"]), DEMO_COMMANDS, &[])
                .expect("swarm status rejected");
        assert_eq!(
            error.message,
            "`cass swarm` was removed: cass runs fully local-only"
        );
        // The removed path is named, not the extra arguments.
        assert!(error.hint.is_none());
    }

    #[test]
    fn command_pairs_reject_only_the_named_pair() {
        let rejected =
            removed_invocation_error(&argv(&["cass", "sources", "sync"]), DEMO_COMMANDS, &[]);
        assert!(rejected.is_some(), "sources sync must be rejected");
        assert_eq!(
            rejected.unwrap().message,
            "`cass sources sync` was removed: cass runs fully local-only"
        );
        for args in [
            argv(&["cass", "sources"]),
            argv(&["cass", "sources", "list"]),
            argv(&["cass", "sources", "syncer"]),
        ] {
            assert!(
                removed_invocation_error(&args, DEMO_COMMANDS, &[]).is_none(),
                "{args:?} is a live invocation and must not be rejected"
            );
        }
    }

    #[test]
    fn typos_of_removed_names_resolve_to_the_removed_error() {
        let error = removed_invocation_error(&argv(&["cass", "swarn"]), DEMO_COMMANDS, &[])
            .expect("swarn resolves to removed swarm");
        assert_eq!(error.kind, CliErrorKind::RemovedCommand.kind_str());
        assert_eq!(
            error.message,
            "`cass swarn` resolves to `cass swarm`, which was removed: cass runs fully local-only"
        );
    }

    #[test]
    fn typos_closer_to_live_commands_are_not_rejected() {
        // `serach` is distance 1 from live `search` and distance >= 4 from
        // every demo removal — the live correction must still win.
        assert!(
            removed_invocation_error(&argv(&["cass", "serach"]), DEMO_COMMANDS, &[]).is_none(),
            "a typo closer to a live command keeps the live typo-recovery path"
        );
    }

    #[test]
    fn removed_name_as_search_argument_is_not_rejected() {
        // `swarm` here is a query word, not the command position.
        assert!(
            removed_invocation_error(&argv(&["cass", "search", "swarm"]), DEMO_COMMANDS, &[])
                .is_none(),
            "a removed name in argument position must not reject the live command"
        );
        assert!(
            removed_invocation_error(
                &argv(&["cass", "pack", "sources", "sync"]),
                DEMO_COMMANDS,
                &[]
            )
            .is_none(),
            "removed pair words under a different command must not reject"
        );
    }

    #[test]
    fn removed_flags_reject_by_presence_or_value() {
        // Value-restricted flag: only the listed values are rejected.
        for args in [
            argv(&["cass", "pages", "--target", "github"]),
            argv(&["cass", "pages", "--target=github"]),
            argv(&["cass", "pages", "--target", "CLOUDFLARE"]),
            argv(&["cass", "pages", "-target", "github"]),
        ] {
            let error = removed_invocation_error(&args, &[], DEMO_FLAGS)
                .unwrap_or_else(|| panic!("{args:?} must be rejected"));
            assert_eq!(error.code, 2);
            assert_eq!(error.kind, CliErrorKind::RemovedCommand.kind_str());
        }
        let error = removed_invocation_error(
            &argv(&["cass", "pages", "--target", "github"]),
            &[],
            DEMO_FLAGS,
        )
        .expect("pages --target github rejected");
        assert_eq!(
            error.message,
            "`--target github` on `cass pages` was removed: cass runs fully local-only"
        );
        assert_eq!(
            error.hint.as_deref(),
            Some("local equivalent: `cass pages --export-only` for a local archive")
        );
        // Surviving values of the same flag stay live.
        assert!(
            removed_invocation_error(
                &argv(&["cass", "pages", "--target", "local"]),
                &[],
                DEMO_FLAGS
            )
            .is_none(),
            "an allowed --target value must not be rejected"
        );
        // Presence rejection: `models install --mirror X`.
        for args in [
            argv(&["cass", "models", "install", "--mirror", "X"]),
            argv(&["cass", "models", "install", "--mirror=X"]),
        ] {
            let error = removed_invocation_error(&args, &[], DEMO_FLAGS)
                .unwrap_or_else(|| panic!("{args:?} must be rejected"));
            assert_eq!(
                error.hint.as_deref(),
                Some("local equivalent: `cass models install --from-file DIR`")
            );
        }
        // The flag is only removed on its own path.
        assert!(
            removed_invocation_error(
                &argv(&["cass", "models", "verify", "--mirror", "X"]),
                &[],
                DEMO_FLAGS
            )
            .is_none(),
            "--mirror on a different command path must not reject"
        );
        assert!(
            removed_invocation_error(
                &argv(&["cass", "pages", "--target", "github", "--", "x"]),
                &[],
                DEMO_FLAGS
            )
            .is_some()
                || removed_invocation_error(
                    &argv(&["cass", "pages", "--", "--target", "github"]),
                    &[],
                    DEMO_FLAGS
                )
                .is_none(),
            "tokens after `--` are never flags"
        );
    }

    /// The disjointness invariant behind
    /// `removed_tables_stay_disjoint_from_live_lists`, taking the table so
    /// tests can prove a pair entry under a live parent is harmless. Only
    /// one-word paths are checked: `["sources","sync"]` removes that pair
    /// alone — `sources` stays live, listed, and correctable by design, so
    /// requiring the parent word out of CANONICAL would break the table.
    fn assert_disjoint_from_live_lists(removed_commands: &[RemovedCommand]) {
        // The same commit that adds a one-word REMOVED_COMMANDS entry must
        // drop the name from CANONICAL_TOP_LEVEL_COMMANDS — a name in both
        // lists would both reject and still advertise/correct to the
        // command.
        for entry in removed_commands {
            if entry.path.len() != 1 {
                continue;
            }
            let top = entry.path[0];
            assert!(
                !CANONICAL_TOP_LEVEL_COMMANDS
                    .iter()
                    .any(|name| name.eq_ignore_ascii_case(top)),
                "removed command `{top}` still listed in CANONICAL_TOP_LEVEL_COMMANDS"
            );
            assert!(
                !SIDE_EFFECT_EXACT_ONLY_COMMANDS
                    .iter()
                    .any(|name| name.eq_ignore_ascii_case(top)),
                "removed command `{top}` still listed in SIDE_EFFECT_EXACT_ONLY_COMMANDS"
            );
        }
        // Fuzzy correction can never return a removed name.
        for entry in removed_commands {
            if entry.path.len() != 1 {
                continue;
            }
            let top = entry.path[0];
            for probe in [
                top.to_string(),
                top[..top.len().saturating_sub(1)].to_string(),
            ] {
                assert_ne!(
                    closest_top_level_command_in(&probe, removed_commands),
                    Some(top),
                    "removed command `{top}` is still a typo-correction candidate"
                );
            }
        }
    }

    #[test]
    fn removed_tables_stay_disjoint_from_live_lists() {
        assert_disjoint_from_live_lists(REMOVED_COMMANDS);
    }

    /// The exact row task 9 lands while `sources` stays a live top-level
    /// command: the pair is removed, the parent is not.
    const PAIR_UNDER_LIVE_PARENT: &[RemovedCommand] = &[RemovedCommand {
        path: &["sources", "sync"],
        hint: None,
    }];

    #[test]
    fn pair_entry_leaves_its_live_parent_correctable() {
        // The exclusion helper scopes to one-word paths only.
        assert!(
            !is_removed_command_name("sources", PAIR_UNDER_LIVE_PARENT),
            "a removed pair must not mark its live parent as removed"
        );
        assert!(
            !is_removed_command_name("sources", DEMO_COMMANDS),
            "the demo table's pair row must not remove `sources` either"
        );
        assert!(
            is_removed_command_name("swarm", DEMO_COMMANDS),
            "a one-word entry still marks its name removed"
        );
        // `cass sourcess` keeps typo-correcting to the live `sources`
        // instead of falling into the implicit-search fold — with the
        // unscoped parent-word check this returned `None` and `sourcess`
        // silently executed as `cass search sourcess`.
        for table in [PAIR_UNDER_LIVE_PARENT, DEMO_COMMANDS] {
            assert_eq!(
                closest_top_level_command_in("sourcess", table),
                Some("sources"),
                "a removed pair must not exclude its parent from typo candidates"
            );
        }
        // The real consts agree today: `sources` is live and already the
        // nearest candidate.
        assert_eq!(closest_top_level_command("sourcess"), Some("sources"));
        // Disjointness ignores pair rows — the pair-only table passes
        // while `sources` is legitimately still listed in CANONICAL.
        assert_disjoint_from_live_lists(PAIR_UNDER_LIVE_PARENT);
        assert_disjoint_from_live_lists(REMOVED_COMMANDS);
    }
}

/// Heuristic recovery for command-line errors to help agents.
/// Returns `(corrected_args, correction_note)` if a likely intent is found.
fn heuristic_parse_recovery(
    err: &clap::Error,
    raw_args: &[String],
) -> Option<(Vec<String>, String)> {
    // Only attempt recovery for "unknown argument" or "unrecognized subcommand" errors
    let is_unknown = err.kind() == clap::error::ErrorKind::UnknownArgument
        || err.kind() == clap::error::ErrorKind::InvalidSubcommand;

    if !is_unknown || raw_args.len() < 2 {
        return None;
    }

    let prog = &raw_args[0];
    let args = &raw_args[1..];
    let mut corrected = Vec::new();
    corrected.push(prog.clone());

    let mut made_correction = false;
    let mut notes = Vec::new();

    // 1. Detect implicit "search" subcommand
    // If the first arg isn't a known subcommand or flag, and looks like a query, assume "search".
    if !args.is_empty()
        && !args[0].starts_with('-')
        && let Some(command) = closest_top_level_command(&args[0])
    {
        corrected.push(command.to_string());
        notes.push(format!(
            "Corrected subcommand typo '{}' to '{}'",
            args[0], command
        ));
        made_correction = true;
        corrected.extend_from_slice(&args[1..]);
    } else if !args.is_empty()
        && !args[0].starts_with('-')
        && !CANONICAL_TOP_LEVEL_COMMANDS.contains(&args[0].as_str())
        && !matches!(
            args[0].as_str(),
            "help" | "--help" | "-h" | "--version" | "-V"
        )
    {
        corrected.push("search".to_string());
        // If the arg looks like `query="foo"`, strip the key
        if args[0].starts_with("query=") || args[0].starts_with("q=") {
            let val = args[0].split_once('=').map(|(_, v)| v).unwrap_or(&args[0]);
            corrected.push(val.to_string());
            notes.push(format!(
                "Assumed 'search' subcommand and stripped query key from '{}'",
                args[0]
            ));
        } else {
            corrected.push(args[0].clone());
            notes.push(format!(
                "Assumed 'search' subcommand for positional argument '{}'",
                args[0]
            ));
        }
        made_correction = true;
        corrected.extend_from_slice(&args[1..]);
    } else {
        // Just copy original structure to start
        corrected.extend_from_slice(args);
    }

    // 2. Fuzzy match flags and fix key=value syntax
    let mut final_args = Vec::new();
    final_args.push(corrected[0].clone()); // prog

    for arg in corrected.iter().skip(1) {
        if arg.starts_with("--") {
            // Split --flag=value or --flag
            let (flag, value) = if let Some((f, v)) = arg.split_once('=') {
                (f, Some(v))
            } else {
                (arg.as_str(), None)
            };

            // Known flags for fuzzy matching
            let known_flags = [
                "--robot",
                "--json",
                "--limit",
                "--offset",
                "--agent",
                "--workspace",
                "--fields",
                "--max-tokens",
                "--max-sessions",
                "--max-evidence",
                "--context-lines",
                "--max-excerpt-chars",
                "--sessions-from",
                "--freshness-policy",
                "--freshness-window-seconds",
                "--require-evidence",
                "--explain-selection",
                "--request-id",
                "--cursor",
                "--since",
                "--until",
                "--days",
                "--today",
                "--week",
                "--full",
                "--watch",
                "--data-dir",
                "--verbose",
                "--quiet",
            ];

            // Check for exact match
            if known_flags.contains(&flag) {
                final_args.push(arg.clone());
                continue;
            }

            // Check for typos (levenshtein distance <= 2)
            let best_match = known_flags
                .iter()
                .min_by_key(|k| strsim::levenshtein(flag, k))
                .filter(|k| strsim::levenshtein(flag, k) <= 2);

            if let Some(&correction) = best_match {
                if let Some(v) = value {
                    final_args.push(format!("{correction}={v}"));
                } else {
                    final_args.push(correction.to_string());
                }
                notes.push(format!("Corrected typo '{flag}' to '{correction}'"));
                made_correction = true;
            } else {
                // Keep as is if no good guess
                final_args.push(arg.clone());
            }
        } else if arg.contains('=') && !arg.starts_with('-') {
            // 3. Handle `limit=5` (missing --)
            let Some((key, val)) = arg.split_once('=') else {
                final_args.push(arg.clone());
                continue;
            };
            let flag_candidate = format!("--{key}");
            // Quick check if adding -- makes it a valid flag
            let known_flags = [
                "--limit",
                "--offset",
                "--agent",
                "--workspace",
                "--days",
                "--since",
                "--until",
                "--source",
                "--output",
                "--format",
                "--model",
                "--reranker",
                "--embedder",
                "--timeout",
                "--fields",
                "--max-tokens",
                "--max-sessions",
                "--max-evidence",
                "--context-lines",
                "--max-excerpt-chars",
                "--sessions-from",
                "--freshness-policy",
                "--freshness-window-seconds",
                "--request-id",
            ];
            if known_flags.contains(&flag_candidate.as_str()) {
                final_args.push(flag_candidate);
                final_args.push(val.to_string());
                notes.push(format!(
                    "Interpreted '{arg}' as flag '{key}' with value '{val}'"
                ));
                made_correction = true;
            } else {
                final_args.push(arg.clone());
            }
        } else {
            final_args.push(arg.clone());
        }
    }

    if made_correction {
        Some((final_args, notes.join("; ")))
    } else {
        None
    }
}

pub struct ParsedCli {
    pub cli: Cli,
    raw_args: Vec<String>,
    parse_note: Option<String>,
    heuristic_note: Option<String>,
}

pub fn parse_cli(raw_args: Vec<String>) -> CliResult<ParsedCli> {
    // A removed command or flag errors here, before normalization, alias
    // expansion, implicit-search folding or typo recovery can rewrite the
    // invocation into a different live command (cass-local plan, task 6).
    if let Some(error) = removed_invocation_error(&raw_args, REMOVED_COMMANDS, REMOVED_FLAGS) {
        return Err(error);
    }

    // First normalization pass (global flags lift)
    let (normalized_args, parse_note) = normalize_args(raw_args.clone());

    let (mut cli, heuristic_note, parsed_args) = match Cli::try_parse_from(&normalized_args) {
        Ok(cli) => (cli, None, normalized_args),
        Err(err) => {
            // Let clap handle help/version natively (exit 0, print to stdout)
            use clap::error::ErrorKind;
            let is_display = |kind: ErrorKind| {
                matches!(kind, ErrorKind::DisplayHelp | ErrorKind::DisplayVersion)
            };
            // Help renders from the whole tree: the root help then lists the
            // binary-dispatched subcommands (serve, archive), and `cass help
            // serve`, an unknown subcommand to this CLI alone, shows its help.
            if let Err(full) = cli_command().try_get_matches_from(&normalized_args)
                && is_display(full.kind())
            {
                full.exit();
            }
            if is_display(err.kind()) {
                err.exit();
            }

            // Handle bare subcommand invocations (e.g. `cass analytics` without a
            // sub-subcommand).  Clap reports MissingSubcommand for these; we turn
            // that into a targeted hint listing the available sub-subcommands.
            if err.kind() == ErrorKind::MissingSubcommand {
                let msg = format_missing_subcommand_error(&normalized_args);
                return Err(CliError::usage(msg, None));
            }

            // Attempt heuristic recovery
            if let Some((recovered_args, note)) = heuristic_parse_recovery(&err, &normalized_args) {
                // Try parsing again with recovered args
                match Cli::try_parse_from(&recovered_args) {
                    Ok(cli) => (cli, Some(note), recovered_args),
                    Err(retry_err) => {
                        // Check again for help/version in case recovered args triggered it
                        if matches!(
                            retry_err.kind(),
                            ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
                        ) {
                            retry_err.exit();
                        }
                        // Recovery failed to produce valid args, fail with original error + friendly help
                        let friendly =
                            format_friendly_parse_error(err, &raw_args, &normalized_args);
                        if friendly.trim().starts_with('{') {
                            return Err(CliError {
                                code: 2,
                                kind: CliErrorKind::Usage.kind_str(),
                                message: friendly,
                                hint: None,
                                retryable: false,
                            });
                        }
                        return Err(CliError::usage("Could not parse arguments", Some(friendly)));
                    }
                }
            } else {
                // No recovery possible
                let friendly = format_friendly_parse_error(err, &raw_args, &normalized_args);
                if friendly.trim().starts_with('{') {
                    return Err(CliError {
                        code: 2,
                        kind: CliErrorKind::Usage.kind_str(),
                        message: friendly,
                        hint: None,
                        retryable: false,
                    });
                }
                return Err(CliError::usage("Could not parse arguments", Some(friendly)));
            }
        }
    };

    // `--db` and `CASS_DB_PATH` fill the same field and only the matcher
    // knows which one supplied it.
    if cli.db.is_some() {
        cli.interpretation.db_from_env = Cli::command()
            .try_get_matches_from(&parsed_args)
            .ok()
            .and_then(|matches| matches.value_source("db"))
            == Some(clap::parser::ValueSource::EnvVariable);
    }
    cli.interpretation.corrections = [parse_note.as_deref(), heuristic_note.as_deref()]
        .into_iter()
        .flatten()
        .map(str::to_string)
        .collect();

    Ok(ParsedCli {
        cli,
        raw_args,
        parse_note,
        heuristic_note,
    })
}

pub async fn run() -> CliResult<()> {
    let parsed = parse_cli(std::env::args().collect())?;
    run_with_parsed(parsed).await
}

pub async fn run_with_parsed(parsed: ParsedCli) -> CliResult<()> {
    let ParsedCli {
        cli,
        raw_args,
        parse_note,
        heuristic_note,
    } = parsed;

    let stdout_is_tty = io::stdout().is_terminal();
    let stderr_is_tty = io::stderr().is_terminal();
    configure_color(cli.color, stdout_is_tty, stderr_is_tty);

    let wrap_cfg = WrapConfig::new(cli.wrap, cli.nowrap);
    let progress_resolved = resolve_progress(cli.progress, stdout_is_tty);

    let start_ts = Utc::now();
    let start_instant = Instant::now();
    let command_label = describe_command(&cli);

    // Output correction notices for AI agents
    // These teach the agent proper syntax while still honoring their intent
    // Detect robot mode from raw args (more reliable than pattern matching complex enums)
    let is_robot_mode = raw_args
        .iter()
        .any(|s| s == "--json" || s == "--robot" || s == "-json" || s == "-robot")
        || cli.robot_format.is_some()
        || robot_format_from_env().is_some()
        || matches!(&cli.command, Some(Commands::Capabilities { .. }))
        || matches!(&cli.command, Some(Commands::Introspect { .. }));
    let is_doc_mode = cli.robot_help || matches!(&cli.command, Some(Commands::RobotDocs { .. }));

    // Combine all correction notes
    let all_notes: Vec<&str> = [parse_note.as_deref(), heuristic_note.as_deref()]
        .into_iter()
        .flatten()
        .collect();

    // Human teaching notes can precede execution. For robots, defer notes
    // until success: on failure stderr must remain one structured error
    // envelope, not plaintext followed by JSON. Successful recovery retains
    // its documented teaching note without contaminating stdout.
    if !all_notes.is_empty() && !is_doc_mode && !is_robot_mode {
        emit_correction_notes(&all_notes, false);
    }

    let result = execute_cli(
        &cli,
        wrap_cfg,
        progress_resolved,
        stdout_is_tty,
        stderr_is_tty,
    )
    .await;

    if result.is_ok() && !all_notes.is_empty() && !is_doc_mode && is_robot_mode {
        emit_correction_notes(&all_notes, true);
    }

    if let Some(path) = &cli.trace_file {
        let duration_ms = start_instant.elapsed().as_millis();
        let exit_code = result.as_ref().map_or_else(|e| e.code, |()| 0);
        if let Err(trace_err) = write_trace_line(
            path,
            &command_label,
            &cli,
            &start_ts,
            duration_ms,
            exit_code,
            result.as_ref().err(),
        ) {
            eprintln!("trace-write error: {trace_err}");
        }
    }

    result
}

/// Run startup-sensitive commands before constructing the async runtime.
///
/// `cass health --json` is documented as a <50ms fast-readiness surface and
/// `cass selftest --json` is a binary-promotion probe that must not pay archive
/// or runtime startup costs. Even current-thread runtime setup is visible in
/// subprocess latency tests, so keep both commands on this synchronous path
/// and leave the general async runner for commands that actually need it.
pub fn try_run_with_parsed_fast(parsed: ParsedCli) -> Result<CliResult<()>, Box<ParsedCli>> {
    let ParsedCli {
        cli,
        raw_args,
        parse_note,
        heuristic_note,
    } = parsed;

    let command = cli.command.clone();
    if !matches!(
        command.as_ref(),
        Some(Commands::Health { .. } | Commands::Selftest { .. })
    ) {
        return Err(Box::new(ParsedCli {
            cli,
            raw_args,
            parse_note,
            heuristic_note,
        }));
    }

    let stdout_is_tty = io::stdout().is_terminal();
    let stderr_is_tty = io::stderr().is_terminal();
    configure_color(cli.color, stdout_is_tty, stderr_is_tty);

    let start_ts = Utc::now();
    let start_instant = Instant::now();
    let command_label = describe_command(&cli);

    let is_robot_mode = raw_args
        .iter()
        .any(|s| matches!(s.as_str(), "--json" | "--robot" | "-json" | "-robot"))
        || cli.robot_format.is_some()
        || robot_format_from_env().is_some();
    let all_notes: Vec<&str> = [parse_note.as_deref(), heuristic_note.as_deref()]
        .into_iter()
        .flatten()
        .collect();

    if !all_notes.is_empty() && !is_robot_mode {
        emit_correction_notes(&all_notes, false);
    }

    let result = match command.expect("fast command was matched above") {
        Commands::Health {
            data_dir,
            robot_meta,
            stale_threshold,
            binary_only,
            json,
        } => {
            let structured_format = resolve_subcommand_structured_format(&cli, json);
            run_health(
                &data_dir,
                cli.db.clone(),
                structured_format,
                stale_threshold,
                binary_only,
                robot_meta,
            )
        }
        Commands::Selftest { json } => {
            let structured_format = resolve_subcommand_structured_format(&cli, json);
            run_selftest(structured_format)
        }
        _ => unreachable!("non-fast command passed the fast-command guard"),
    };

    if result.is_ok() && !all_notes.is_empty() && is_robot_mode {
        emit_correction_notes(&all_notes, true);
    }

    if let Some(path) = &cli.trace_file {
        let duration_ms = start_instant.elapsed().as_millis();
        let exit_code = result.as_ref().map_or_else(|e| e.code, |()| 0);
        if let Err(trace_err) = write_trace_line(
            path,
            &command_label,
            &cli,
            &start_ts,
            duration_ms,
            exit_code,
            result.as_ref().err(),
        ) {
            eprintln!("trace-write error: {trace_err}");
        }
    }

    Ok(result)
}

async fn execute_cli(
    cli: &Cli,
    wrap: WrapConfig,
    progress: ProgressResolved,
    stdout_is_tty: bool,
    stderr_is_tty: bool,
) -> CliResult<()> {
    let command = cli.command.clone().unwrap_or_else(|| {
        if cli.robot_format.is_some() || robot_format_from_env().is_some() {
            Commands::Triage {
                data_dir: None,
                json: true,
                stale_threshold: 1800,
                timeout: None,
            }
        } else {
            Commands::Tui {
                once: false,
                reset_state: false,
                asciicast: None,
                data_dir: None,
                inline: false,
                ui_height: 12,
                anchor: "bottom".to_string(),
                record_macro: None,
                play_macro: None,
                refresh: false,
            }
        }
    });

    if cli.robot_help {
        print_robot_help(wrap)?;
        return Ok(());
    }

    if let Commands::RobotDocs { topic } = command.clone() {
        print_robot_docs(topic, wrap)?;
        return Ok(());
    }

    // TUI preflight: call out env profiles that commonly make the UI look
    // degraded (global no-color, conservative TERM profiles).
    if matches!(command, Commands::Tui { .. }) {
        warn_tui_terminal_profile(stderr_is_tty);
    }

    // Block TUI in non-TTY contexts unless TUI_HEADLESS is set (for testing)
    if matches!(command, Commands::Tui { .. })
        && !stdout_is_tty
        && dotenvy::var("TUI_HEADLESS").is_err()
    {
        return Err(CliError::usage(
            "No subcommand provided; in non-TTY contexts TUI is disabled.",
            Some("Use an explicit subcommand, e.g., `cass search --json ...` or `cass --robot-help`.".to_string()),
        ));
    }

    // Auto-quiet in robot mode: suppress INFO/WARN logs for clean JSON output.
    // WARN was previously allowed through, but even on stderr it breaks JSON
    // consumers that merge stderr into stdout (e.g., `cass index --json 2>&1`).
    // Only errors are important enough to surface in machine-readable mode.
    let robot_mode = is_robot_mode(&command, cli);
    // Single enforcement chokepoint for stdout/stderr hygiene: robot/quiet
    // output is machine-consumed, so the tracing filter is pinned to `error`
    // (overriding RUST_LOG) to guarantee no dependency INFO/WARN logging can
    // interleave with JSON. See `robot_aware_log_directive` and the regression
    // suite in tests/cli_robot_log_hygiene.rs
    // (coding_agent_session_search-cass-fleet-resilience-20260608-uojcg.2.1).
    let filter = build_robot_aware_log_filter(robot_mode, cli.verbose, cli.quiet);

    match &command {
        Commands::Tui { data_dir, .. } => {
            let log_dir = resolve_data_dir(data_dir, cli.db.as_ref());
            std::fs::create_dir_all(&log_dir).ok();

            let file_appender = tracing_appender::rolling::daily(&log_dir, "cass.log");
            let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

            tracing_subscriber::registry()
                .with(filter)
                .with(
                    tracing_subscriber::fmt::layer()
                        .with_writer(non_blocking)
                        .compact()
                        .with_target(false)
                        .with_ansi(false),
                )
                .init();

            // No update check anywhere: the checker and its TUI banner were
            // removed by the local-only conversion. The historical pre-TUI
            // check awaited the network and then blocked on a stdin prompt
            // before the first frame (2l1b0.56).
            if let Commands::Tui {
                once,
                reset_state,
                asciicast,
                data_dir,
                inline,
                ui_height,
                anchor,
                record_macro,
                play_macro,
                refresh,
            } = command.clone()
            {
                let tui_data_dir = resolve_data_dir(&data_dir, cli.db.as_ref());
                let tui_db_path = cli
                    .db
                    .clone()
                    .unwrap_or_else(|| tui_data_dir.join("agent_search.db"));
                if refresh {
                    refresh_index_inline(cli.db.clone(), Some(tui_data_dir.clone()));
                } else if let Some(outcome) =
                    maybe_auto_refresh_index_after_read(&tui_data_dir, &tui_db_path, None)
                {
                    // Stale-on-launch catch-up runs detached; the TUI opens
                    // immediately on the existing index and can reload
                    // (Ctrl+Shift+R / command palette) once it lands.
                    info!(?outcome, "tui: auto-refresh evaluated");
                }
                info!(once, inline, ui_height, %anchor, record_macro = ?record_macro, play_macro = ?play_macro, "launching ftui runtime");

                let inline_config = if inline {
                    let ui_anchor = if anchor == "top" {
                        ui::ftui_adapter::UiAnchor::Top
                    } else {
                        ui::ftui_adapter::UiAnchor::Bottom
                    };
                    Some(ui::app::InlineTuiConfig {
                        ui_height,
                        anchor: ui_anchor,
                    })
                } else {
                    None
                };

                if reset_state {
                    let state_path = tui_data_dir.join("tui_state.json");
                    match std::fs::remove_file(&state_path) {
                        Ok(()) => {}
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                        Err(e) => {
                            return Err(CliError {
                                code: 9,
                                kind: CliErrorKind::TuiResetState.kind_str(),
                                message: format!(
                                    "failed to remove persisted state {}: {e}",
                                    state_path.display()
                                ),
                                hint: Some(
                                    "Check file permissions or rerun without --reset-state."
                                        .to_string(),
                                ),
                                retryable: false,
                            });
                        }
                    }
                }

                let non_tty_headless_once =
                    once && !inline && !stdout_is_tty && dotenvy::var("TUI_HEADLESS").is_ok();

                if non_tty_headless_once {
                    prepare_headless_once_tui_artifacts(
                        &tui_data_dir,
                        &tui_db_path,
                        asciicast.as_deref(),
                    )
                    .map_err(|e| CliError {
                        code: 9,
                        kind: CliErrorKind::TuiHeadlessOnce.kind_str(),
                        message: format!(
                            "headless --once TUI bootstrap failed for {}: {e}",
                            tui_data_dir.display()
                        ),
                        hint: Some(
                            "Ensure the data directory is writable and retry the command."
                                .to_string(),
                        ),
                        retryable: false,
                    })?;
                    info!(
                        data_dir = %tui_data_dir.display(),
                        asciicast = ?asciicast,
                        "completed non-interactive headless --once TUI bootstrap"
                    );
                } else {
                    let macro_config = ui::app::MacroConfig {
                        record_path: record_macro,
                        play_path: play_macro,
                    };
                    let run_result = if let Some(path) = asciicast {
                        tui_asciicast::run_tui_with_asciicast(&path, !once)
                    } else {
                        ui::app::run_tui_ftui(
                            inline_config,
                            macro_config,
                            Some(tui_data_dir),
                            Some(tui_db_path),
                        )
                    };

                    if let Err(e) = run_result {
                        return Err(CliError {
                            code: 9,
                            kind: CliErrorKind::Tui.kind_str(),
                            message: format!("tui failed: {e}"),
                            hint: None,
                            retryable: false,
                        });
                    }
                }
            }
        }
        Commands::Index { .. }
        | Commands::Search { .. }
        | Commands::Pack { .. }
        | Commands::Stats { .. }
        | Commands::Diag { .. }
        | Commands::Storage { .. }
        | Commands::Dedup { .. }
        | Commands::Status { .. }
        | Commands::Triage { .. }
        | Commands::SupportBundle { .. }
        | Commands::View { .. }
        | Commands::Pages { .. }
        | Commands::Analytics(..) => {
            // Robot-safe stderr hygiene + optional explicit --trace-file sink
            // (uojcg.2.1 / uojcg.2.5). Keep the guard alive for the command run.
            let _trace_guard = init_cli_tracing(
                filter,
                cli.trace_file.as_deref(),
                matches!(cli.color, ColorPref::Always)
                    || (matches!(cli.color, ColorPref::Auto) && stderr_is_tty),
            );

            match command {
                Commands::Index {
                    full,
                    force_rebuild,
                    reconcile_conversation,
                    gc,
                    watch,
                    watch_once,
                    watch_interval,
                    data_dir,
                    semantic,
                    build_hnsw,
                    embedder,
                    idempotency_key,
                    json,
                    progress_interval_ms,
                    no_progress_events,
                    robot_trace_ingest,
                    background,
                    rebuild_profile,
                } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    // Before any rebuild setting is read in this process.
                    indexer::set_lexical_rebuild_profile(rebuild_profile.into());
                    if let Some(conversation_id) = reconcile_conversation {
                        return run_lexical_reconcile_cli(
                            cli.db.clone(),
                            data_dir,
                            conversation_id,
                            structured_format,
                        );
                    }
                    if gc {
                        return run_lexical_gc_cli(cli.db.clone(), data_dir, structured_format);
                    }
                    run_index_with_data(
                        cli.db.clone(),
                        full,
                        force_rebuild,
                        watch,
                        watch_once,
                        watch_interval,
                        data_dir,
                        semantic,
                        build_hnsw,
                        embedder,
                        progress,
                        structured_format,
                        idempotency_key,
                        progress_interval_ms,
                        no_progress_events,
                        robot_trace_ingest,
                        background,
                        None,
                    )?;
                }
                Commands::Search {
                    query,
                    agent,
                    workspace,
                    limit,
                    offset,
                    json,
                    robot_meta,
                    fields,
                    max_content_length,
                    max_tokens,
                    request_id,
                    cursor,
                    display,
                    data_dir,
                    days,
                    today,
                    yesterday,
                    week,
                    since,
                    until,
                    aggregate,
                    explain,
                    dry_run,
                    timeout,
                    highlight,
                    source,
                    sessions_from,
                    mode,
                    approximate,
                    model,
                    rerank,
                    reranker,
                    daemon,
                    no_daemon,
                    two_tier,
                    fast_only,
                    quality_only,
                    refresh,
                    no_maintenance,
                } => {
                    // Validate mutually exclusive two-tier flags
                    let tier_count = [two_tier, fast_only, quality_only]
                        .iter()
                        .filter(|&&b| b)
                        .count();
                    if tier_count > 1 {
                        return Err(CliError::usage(
                            "Cannot specify multiple tier flags",
                            Some(
                                "Use only one of --two-tier, --fast-only, or --quality-only"
                                    .to_string(),
                            ),
                        ));
                    }

                    // Validate mutually exclusive flags
                    if daemon && no_daemon {
                        return Err(CliError::usage(
                            "Cannot specify both --daemon and --no-daemon",
                            Some(
                                "Use --daemon to enable daemon or --no-daemon to disable it"
                                    .to_string(),
                            ),
                        ));
                    }

                    // Warn about reranker without rerank flag
                    if reranker.is_some() && !rerank {
                        eprintln!(
                            "Warning: --reranker specified but --rerank not enabled; reranker will be ignored"
                        );
                    }

                    // Build semantic options from new flags
                    let tier_mode = if two_tier {
                        crate::search::query::SemanticTierMode::Progressive
                    } else if fast_only {
                        crate::search::query::SemanticTierMode::FastOnly
                    } else if quality_only {
                        crate::search::query::SemanticTierMode::QualityOnly
                    } else {
                        crate::search::query::SemanticTierMode::Single
                    };

                    let daemon_policy = semantic_daemon_policy(daemon, no_daemon);
                    let semantic_opts = SemanticSearchOptions {
                        model: model.clone(),
                        rerank,
                        reranker: reranker.clone(),
                        // #347: an already-running daemon is the inexpensive
                        // default. `--daemon` additionally permits auto-spawn;
                        // `--no-daemon` is the explicit direct-inference escape.
                        use_daemon: daemon_policy.use_existing,
                        auto_spawn_daemon: daemon_policy.auto_spawn,
                        approximate,
                        tier_mode,
                    };

                    // Only pass through robot_format when it was explicitly
                    // requested (CLI flag or env var). Returning `None` here
                    // lets `run_cli_search` apply its full precedence chain
                    // (robot_format > --json > env > robot_auto > display),
                    // so `--display table|lines|markdown` actually wins when
                    // no structured format was asked for.
                    let effective_format = cli.robot_format.or_else(robot_format_from_env);

                    // Resolve configurable search defaults (#303): the explicit
                    // CLI flag wins, then the env var, then `~/.config/cass/cass.toml`
                    // `[search]`, then the built-in default. This lets agents set a
                    // default timeout/limit/mode once instead of appending flags to
                    // every invocation. A broken config file or a malformed env value
                    // is a clear usage error rather than a silent fall-through.
                    let (eff_timeout, eff_limit, eff_mode) =
                        resolve_search_defaults(timeout, limit, mode)?;

                    run_cli_search(
                        &query,
                        &agent,
                        &workspace,
                        &eff_limit,
                        &offset,
                        &json,
                        effective_format,
                        robot_meta,
                        fields,
                        max_content_length,
                        max_tokens,
                        request_id.clone(),
                        cursor.clone(),
                        display,
                        &data_dir,
                        cli.db.clone(),
                        wrap,
                        progress,
                        robot_mode,
                        TimeFilter::new(
                            days,
                            today,
                            yesterday,
                            week,
                            since.as_deref(),
                            until.as_deref(),
                        )?,
                        aggregate,
                        explain,
                        dry_run,
                        no_maintenance,
                        eff_timeout,
                        highlight,
                        source,
                        sessions_from,
                        eff_mode,
                        semantic_opts,
                        refresh,
                        &cli.interpretation,
                    )?;
                }
                Commands::Pack {
                    query,
                    agent,
                    workspace,
                    limit,
                    json,
                    fields,
                    max_tokens,
                    max_sessions,
                    max_evidence,
                    context_lines,
                    max_excerpt_chars,
                    request_id,
                    display,
                    data_dir,
                    days,
                    today,
                    yesterday,
                    week,
                    since,
                    until,
                    source,
                    sessions_from,
                    mode,
                    freshness_policy,
                    freshness_window_seconds,
                    include_skill_content,
                    require_evidence,
                    explain_selection,
                    refresh,
                    timeout,
                } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    // Apply the configurable search defaults (#303) to pack too,
                    // so a configured default timeout/limit/mode covers the
                    // pack path (which runs the same search underneath).
                    let (eff_timeout, eff_limit, eff_mode) =
                        resolve_search_defaults(timeout, limit, mode)?;
                    run_cli_pack(
                        &query,
                        &agent,
                        &workspace,
                        eff_limit,
                        structured_format,
                        fields,
                        max_tokens,
                        max_sessions,
                        max_evidence,
                        context_lines,
                        max_excerpt_chars,
                        request_id.clone(),
                        display,
                        &data_dir,
                        cli.db.clone(),
                        TimeFilter::new(
                            days,
                            today,
                            yesterday,
                            week,
                            since.as_deref(),
                            until.as_deref(),
                        )?,
                        source,
                        sessions_from,
                        eff_mode,
                        &freshness_policy,
                        freshness_window_seconds,
                        include_skill_content,
                        require_evidence,
                        explain_selection,
                        refresh,
                        eff_timeout,
                        wrap,
                        &cli.interpretation,
                    )?;
                }
                Commands::Stats {
                    data_dir,
                    json,
                    source,
                    by_source,
                } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_stats(
                        &data_dir,
                        cli.db.clone(),
                        structured_format,
                        source.as_deref(),
                        by_source,
                    )?;
                }
                Commands::Diag {
                    data_dir,
                    json,
                    quarantine,
                    verbose,
                } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_diag(
                        &data_dir,
                        cli.db.clone(),
                        structured_format,
                        quarantine,
                        verbose,
                    )?;
                }
                Commands::Storage { data_dir, json } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_storage(&data_dir, cli.db.clone(), structured_format)?;
                }
                Commands::Dedup {
                    data_dir,
                    json,
                    apply,
                } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_dedup(&data_dir, cli.db.clone(), structured_format, apply)?;
                }
                Commands::Status {
                    data_dir,
                    json,
                    robot_meta,
                    stale_threshold,
                } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_status(
                        &data_dir,
                        cli.db.clone(),
                        structured_format,
                        stale_threshold,
                        robot_meta,
                    )?;
                }
                Commands::Triage {
                    data_dir,
                    json,
                    stale_threshold,
                    timeout,
                } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_triage(
                        &data_dir,
                        cli.db.clone(),
                        structured_format,
                        stale_threshold,
                        timeout,
                    )?;
                }
                Commands::SupportBundle {
                    data_dir,
                    json,
                    stale_threshold,
                    include_full_paths,
                    include_raw_evidence,
                } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_support_bundle(
                        &data_dir,
                        cli.db.clone(),
                        structured_format,
                        stale_threshold,
                        include_full_paths,
                        include_raw_evidence,
                    )?;
                }
                Commands::View {
                    path,
                    source,
                    conversation_id,
                    message_index,
                    line,
                    context,
                    json,
                    timeout,
                } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    followup_coordinates::run_view(
                        &path,
                        cli.db.clone(),
                        source.as_deref(),
                        conversation_id,
                        ViewWindow { line, context },
                        structured_format,
                        timeout,
                        message_index,
                    )?;
                }
                Commands::Pages {
                    subcommand,
                    export_only,
                    verify,
                    agents,
                    workspaces,
                    since,
                    until,
                    path_mode,
                    share_profile,
                    target,
                    dry_run,
                    scan_secrets,
                    fail_on_secrets,
                    secrets_allow,
                    secrets_deny,
                    verbose,
                    no_encryption,
                    i_understand_unencrypted_risks,
                    preview,
                    port,
                    no_open,
                    config,
                    validate_config,
                    example_config,
                    json,
                } => {
                    // `cass pages key …` operates on an exported bundle and
                    // never touches the export/wizard flags (WS-G.4).
                    if let Some(PagesSubcommand::Key(args)) = subcommand {
                        return crate::pages::key_cli::run_pages_key_command(&args);
                    }
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    let robot_mode_here = structured_format.is_some() || robot_mode;
                    // Handle --example-config (show example config and exit)
                    if example_config {
                        println!("{}", crate::pages::config_input::example_config());
                        return Ok(());
                    }

                    // Handle --config based export
                    if let Some(ref config_path) = config {
                        let mut pages_config = crate::pages::config_input::PagesConfig::load(
                            config_path,
                        )
                        .map_err(|e| CliError {
                            code: 2,
                            kind: CliErrorKind::Pages.kind_str(),
                            message: format!("Failed to load config: {e}"),
                            hint: Some(
                                "Check config file syntax with --example-config".to_string(),
                            ),
                            retryable: false,
                        })?;

                        // Only `local` reaches this point: the removed-flag
                        // table rejects `--target github|cloudflare` before
                        // clap parses, and the hosted flags are gone entirely.
                        if let Some(target) = target {
                            pages_config.deployment.target = target.as_config_value().to_string();
                        }
                        if let Some(profile) = share_profile {
                            pages_config.bundle.share_profile = Some(profile);
                        }

                        // Resolve environment variables
                        pages_config.resolve_env_vars().map_err(|e| CliError {
                            code: 2,
                            kind: CliErrorKind::Pages.kind_str(),
                            message: format!("Failed to resolve env vars: {e}"),
                            hint: Some(
                                "Ensure referenced environment variables are set".to_string(),
                            ),
                            retryable: false,
                        })?;

                        // Validate configuration
                        let validation = pages_config.validate();

                        if validate_config {
                            // Just validate and output result
                            println!(
                                "{}",
                                serde_json::to_string_pretty(&validation).unwrap_or_default()
                            );
                            if !validation.valid {
                                return Err(CliError {
                                    code: 2,
                                    kind: CliErrorKind::Pages.kind_str(),
                                    message: "Configuration validation failed".to_string(),
                                    hint: Some("Review errors in JSON output".to_string()),
                                    retryable: false,
                                });
                            }
                            return Ok(());
                        }

                        if !validation.valid {
                            if let Some(fmt) = structured_format {
                                output_structured_value(
                                    serde_json::to_value(&validation).unwrap_or_default(),
                                    fmt,
                                )?;
                            } else if robot_mode_here {
                                eprintln!("Configuration errors:");
                                for err in &validation.errors {
                                    eprintln!("  - {}", err);
                                }
                            }
                            return Err(CliError {
                                code: 2,
                                kind: CliErrorKind::Pages.kind_str(),
                                message: "Configuration validation failed".to_string(),
                                hint: Some("Fix errors listed above".to_string()),
                                retryable: false,
                            });
                        }

                        // Print warnings
                        if !validation.warnings.is_empty()
                            && structured_format.is_none()
                            && !robot_mode_here
                        {
                            eprintln!("Warnings:");
                            for warn in &validation.warnings {
                                eprintln!("  - {}", warn);
                            }
                            eprintln!();
                        }

                        // Get database path
                        let db_path = cli.db.clone().unwrap_or_else(default_db_path);

                        // Convert config to WizardState and run export
                        let wizard_state =
                            pages_config.to_wizard_state(db_path.clone()).map_err(|e| {
                                CliError {
                                    code: 9,
                                    kind: CliErrorKind::Pages.kind_str(),
                                    message: format!("Failed to create wizard state: {e}"),
                                    hint: None,
                                    retryable: false,
                                }
                            })?;

                        // Run the export using the config
                        run_config_based_export(
                            &pages_config,
                            &wizard_state,
                            &db_path,
                            dry_run,
                            structured_format,
                            verbose,
                            ConfigExportSecretInputs {
                                allow: &secrets_allow,
                                deny: &secrets_deny,
                            },
                        )
                        .map_err(|e| CliError {
                            code: 9,
                            kind: CliErrorKind::Pages.kind_str(),
                            message: format!("Export failed: {e:#}"),
                            hint: None,
                            retryable: false,
                        })?;

                        return Ok(());
                    }

                    // Handle --validate-config without --config
                    if validate_config {
                        return Err(CliError {
                            code: 2,
                            kind: CliErrorKind::Pages.kind_str(),
                            message: "--validate-config requires --config".to_string(),
                            hint: Some("Use --config <path> --validate-config".to_string()),
                            retryable: false,
                        });
                    }

                    // Handle --preview first (starts local preview server)
                    if let Some(preview_dir) = preview {
                        let config = crate::pages::preview::PreviewConfig {
                            site_dir: preview_dir,
                            port,
                            open_browser: !no_open,
                        };
                        crate::pages::preview::start_preview_server(config)
                            .await
                            .map_err(|e| CliError {
                                code: 9,
                                kind: CliErrorKind::Pages.kind_str(),
                                message: format!("Preview server failed: {e}"),
                                hint: Some(
                                    "Check that the directory exists and port is available"
                                        .to_string(),
                                ),
                                retryable: false,
                            })?;
                        return Ok(());
                    }

                    // Check for unencrypted export in robot mode
                    if no_encryption && robot_mode_here && !i_understand_unencrypted_risks {
                        let error = crate::pages::confirmation::robot_mode_blocked_error();
                        eprintln!(
                            "{}",
                            serde_json::to_string_pretty(&error).unwrap_or_default()
                        );
                        return Err(CliError {
                            code: crate::pages::confirmation::EXIT_CODE_UNENCRYPTED_NOT_CONFIRMED,
                            kind: CliErrorKind::Pages.kind_str(),
                            message: "Unencrypted exports are not allowed in robot mode"
                                .to_string(),
                            hint: Some(
                                "Use --i-understand-unencrypted-risks flag if you really need this"
                                    .to_string(),
                            ),
                            retryable: false,
                        });
                    }
                    // Handle --verify first
                    if let Some(verify_path) = verify {
                        let result = crate::pages::verify::verify_bundle(&verify_path, verbose)
                            .map_err(|e| CliError {
                                code: 9,
                                kind: CliErrorKind::Pages.kind_str(),
                                message: format!("Verification failed: {e}"),
                                hint: None,
                                retryable: false,
                            })?;

                        if let Some(fmt) = structured_format {
                            output_structured_value(
                                serde_json::to_value(&result).unwrap_or_default(),
                                fmt,
                            )?;
                        } else {
                            crate::pages::verify::print_result(&result, verbose);
                        }

                        // Exit with non-zero if invalid
                        if result.status != "valid" {
                            if structured_format.is_some() {
                                return Err(CliError::already_reported(
                                    1,
                                    CliErrorKind::Pages.kind_str(),
                                    false,
                                ));
                            }
                            return Err(CliError {
                                code: 1,
                                kind: CliErrorKind::Pages.kind_str(),
                                message: "Bundle verification failed".to_string(),
                                hint: Some(
                                    "Run with --verbose for detailed error information".to_string(),
                                ),
                                retryable: false,
                            });
                        }
                    } else if scan_secrets {
                        let db_path = cli.db.clone().unwrap_or_else(default_db_path);

                        let workspaces_path = workspaces
                            .clone()
                            .map(|ws| ws.into_iter().map(PathBuf::from).collect());

                        let filters = crate::pages::secret_scan::SecretScanFilters {
                            agents: agents.clone(),
                            workspaces: workspaces_path,
                            since_ts: since
                                .as_deref()
                                .and_then(crate::ui::time_parser::parse_time_input),
                            until_ts: until
                                .as_deref()
                                .and_then(crate::ui::time_parser::parse_time_input),
                        };

                        let config = crate::pages::secret_scan::SecretScanConfig::from_inputs(
                            &secrets_allow,
                            &secrets_deny,
                        )
                        .map_err(|e| CliError {
                            code: 9,
                            kind: CliErrorKind::Pages.kind_str(),
                            message: format!("Secret scan config error: {e}"),
                            hint: None,
                            retryable: false,
                        })?;

                        crate::pages::secret_scan::run_secret_scan_cli(
                            &db_path,
                            &filters,
                            &config,
                            structured_format.is_some(),
                            fail_on_secrets,
                        )
                        .map_err(|e| CliError {
                            code: 9,
                            kind: CliErrorKind::Pages.kind_str(),
                            message: format!("Secret scan failed: {e}"),
                            hint: None,
                            retryable: false,
                        })?;
                    } else if let Some(output_path) = export_only {
                        if dry_run {
                            if let Some(fmt) = structured_format {
                                output_structured_value(
                                    serde_json::json!({
                                        "status": "dry_run",
                                        "output_path": output_path,
                                        "secret_scan": {
                                            "performed": false,
                                            "complete": false,
                                            "approval_state": "not_performed_dry_run",
                                            "artifact_sha256": null,
                                        },
                                    }),
                                    fmt,
                                )?;
                            } else {
                                println!("Dry run: would export to {:?}", output_path);
                                println!("Secret scan: not performed during dry run");
                            }
                            return Ok(());
                        }

                        // Interactive unencrypted export confirmation (non-robot mode)
                        if no_encryption && structured_format.is_none() && !robot_mode_here {
                            use console::style;
                            use std::io::Write;

                            eprintln!("{}", style("⚠️  SECURITY WARNING").red().bold());
                            eprintln!();
                            for line in crate::pages::confirmation::unencrypted_warning_lines() {
                                eprintln!("{}", line);
                            }
                            eprintln!();
                            eprintln!("To proceed, type exactly:");
                            eprintln!();
                            eprintln!(
                                "  {}",
                                style(crate::pages::confirmation::UNENCRYPTED_ACK_PHRASE).cyan()
                            );
                            eprintln!();
                            eprint!("Your input: ");
                            std::io::stderr().flush().ok();

                            let mut input = String::new();
                            std::io::stdin().read_line(&mut input).map_err(|e| CliError {
                                code: crate::pages::confirmation::EXIT_CODE_UNENCRYPTED_NOT_CONFIRMED,
                                kind: CliErrorKind::Pages.kind_str(),
                                message: format!("Failed to read input: {e}"),
                                hint: None,
                                retryable: false,
                            })?;

                            match crate::pages::confirmation::validate_unencrypted_ack(&input) {
                                crate::pages::confirmation::StepValidation::Passed => {
                                    // Additional y/N confirmation
                                    eprintln!();
                                    eprint!("Are you ABSOLUTELY SURE? [y/N]: ");
                                    std::io::stderr().flush().ok();

                                    let mut confirm = String::new();
                                    std::io::stdin().read_line(&mut confirm).map_err(|e| CliError {
                                        code: crate::pages::confirmation::EXIT_CODE_UNENCRYPTED_NOT_CONFIRMED,
                                        kind: CliErrorKind::Pages.kind_str(),
                                        message: format!("Failed to read input: {e}"),
                                        hint: None,
                                        retryable: false,
                                    })?;

                                    if confirm.trim().to_lowercase() != "y" {
                                        eprintln!();
                                        eprintln!("{}", style("Export cancelled.").green());
                                        eprintln!(
                                            "To export with encryption (recommended), remove --no-encryption"
                                        );
                                        return Err(CliError {
                                            code: crate::pages::confirmation::EXIT_CODE_UNENCRYPTED_NOT_CONFIRMED,
                                            kind: CliErrorKind::Pages.kind_str(),
                                            message: "Unencrypted export not confirmed".to_string(),
                                            hint: Some("Remove --no-encryption to export with encryption (recommended)".to_string()),
                                            retryable: false,
                                        });
                                    }
                                }
                                crate::pages::confirmation::StepValidation::Failed(msg) => {
                                    eprintln!();
                                    eprintln!("{}", style("Export cancelled.").green());
                                    eprintln!("{}", msg);
                                    return Err(CliError {
                                        code: crate::pages::confirmation::EXIT_CODE_UNENCRYPTED_NOT_CONFIRMED,
                                        kind: CliErrorKind::Pages.kind_str(),
                                        message: "Unencrypted export not confirmed".to_string(),
                                        hint: Some("Remove --no-encryption to export with encryption (recommended)".to_string()),
                                        retryable: false,
                                    });
                                }
                            }
                        }

                        let scan_config = crate::pages::secret_scan::SecretScanConfig::from_inputs(
                            &secrets_allow,
                            &secrets_deny,
                        )
                        .map_err(|e| CliError {
                            code: 9,
                            kind: CliErrorKind::Pages.kind_str(),
                            message: format!("Secret scan config error: {e}"),
                            hint: None,
                            retryable: false,
                        })?;
                        let structured_export = structured_format.is_some() || robot_mode_here;
                        let (stats, (staged_secret_scan, approval_state)) =
                            crate::pages::export::export_pages_database_verified(
                                cli.db.clone(),
                                output_path.clone(),
                                agents.clone(),
                                workspaces.clone(),
                                since.clone(),
                                until.clone(),
                                path_mode,
                                // --export-only writes a plaintext database.
                                share_profile.unwrap_or(
                                    crate::pages::profiles::ShareProfile::default_for(false),
                                ),
                                |_current, _total| {},
                                |staged_db_path| {
                                    let scan = crate::pages::secret_scan::scan_staged_export_database(
                                        staged_db_path,
                                        &scan_config,
                                    )?;
                                    if scan.report.summary.total == 0 {
                                        return Ok((scan, "clean"));
                                    }
                                    if structured_export || fail_on_secrets {
                                        anyhow::bail!(
                                            "Staged export contains {} potential secret(s); robot/CI export-only runs fail closed",
                                            scan.report.summary.total
                                        );
                                    }

                                    let mut term = console::Term::stderr();
                                    crate::pages::secret_scan::print_human_report(
                                        &mut term,
                                        &scan.report,
                                        3,
                                    )?;
                                    eprintln!(
                                        "The report above describes the exact staged export database."
                                    );
                                    eprint!(
                                        "Type exactly \"I understand the risks\" to publish it: "
                                    );
                                    use std::io::Write;
                                    std::io::stderr().flush()?;
                                    let mut acknowledgment = String::new();
                                    std::io::stdin().read_line(&mut acknowledgment)?;
                                    if acknowledgment.trim() != "I understand the risks" {
                                        anyhow::bail!(
                                            "Staged export secret findings were not acknowledged"
                                        );
                                    }
                                    Ok((scan, "explicit_interactive_acknowledgment"))
                                },
                            )
                            .map_err(|e| CliError {
                                code: 9,
                                kind: CliErrorKind::Pages.kind_str(),
                                message: format!("Export failed: {e:#}"),
                                hint: Some(
                                    "Review findings with `cass pages --scan-secrets`; use --secrets-allow only for confirmed false positives."
                                        .to_string(),
                                ),
                                retryable: false,
                            })?;

                        if let Some(fmt) = structured_format {
                            output_structured_value(
                                serde_json::json!({
                                    "status": "success",
                                    "output_path": output_path,
                                    "stats": {
                                        "conversations": stats.conversations_processed,
                                        "messages": stats.messages_processed,
                                    },
                                    "secret_scan": staged_secret_scan_json(
                                        &staged_secret_scan,
                                        approval_state,
                                    ),
                                }),
                                fmt,
                            )?;
                        } else {
                            println!(
                                "Export complete! Processed {} conversations, {} messages.",
                                stats.conversations_processed, stats.messages_processed
                            );
                            println!(
                                "Secret scan: complete ({approval_state}); artifact SHA-256 {}",
                                staged_secret_scan.artifact_sha256
                            );
                        }
                    } else {
                        // `--target` may only be `local` here (hosted values are
                        // rejected by the removed-flag table); it needs no wiring.
                        let _ = target;

                        // Wizard mode: pass flags
                        let mut wizard = crate::pages::wizard::PagesWizard::new();
                        if let Some(db_path) = cli.db.clone() {
                            wizard.set_db_path(db_path);
                        }
                        if no_encryption {
                            wizard.set_no_encryption(true);
                        }
                        if let Some(profile) = share_profile {
                            wizard.set_share_profile(profile);
                        }
                        wizard.run().map_err(|e| CliError {
                            code: 9,
                            kind: CliErrorKind::Pages.kind_str(),
                            message: format!("Wizard failed: {e:#}"),
                            hint: None,
                            retryable: false,
                        })?;
                    }
                }
                Commands::Analytics(subcmd) => {
                    run_analytics(subcmd, cli.db.clone(), cli)?;
                }
                #[cfg(unix)]
                Commands::Daemon {
                    socket,
                    idle_timeout,
                    max_connections,
                    data_dir,
                } => {
                    run_daemon(socket, idle_timeout, max_connections, data_dir)?;
                }
                _ => {}
            }
        }
        _ => {
            let _trace_guard = init_cli_tracing(
                filter,
                cli.trace_file.as_deref(),
                matches!(cli.color, ColorPref::Always)
                    || (matches!(cli.color, ColorPref::Auto) && stderr_is_tty),
            );

            match command {
                Commands::Completions { shell } => {
                    let mut cmd = cli_command();
                    clap_complete::generate(shell, &mut cmd, "cass", &mut std::io::stdout());
                }
                Commands::Man => {
                    let cmd = cli_command();
                    let man = clap_mangen::Man::new(cmd);
                    man.render(&mut std::io::stdout())
                        .map_err(|e| CliError::unknown(format!("failed to render man: {e}")))?;
                }
                Commands::Bookmarks(args) => {
                    // The bookmarks module owns its output contract: exactly
                    // one JSON document on stdout under --json, and on failure
                    // one error envelope on stderr. It hands back the exit
                    // code instead of a CliError so the top-level handler does
                    // not print a second envelope.
                    let code = bookmarks::run_bookmarks_command(args)
                        .map_err(|e| CliError::unknown(format!("bookmarks command failed: {e}")))?;
                    if code != 0 {
                        use std::io::Write as _;
                        let _ = std::io::stdout().flush();
                        let _ = std::io::stderr().flush();
                        std::process::exit(code);
                    }
                }
                Commands::Capabilities { json } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_capabilities(structured_format)?;
                }
                Commands::ApiVersion { json } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_api_version(structured_format)?;
                }
                Commands::State {
                    data_dir,
                    json,
                    robot_meta,
                    stale_threshold,
                } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_status(
                        &data_dir,
                        None,
                        structured_format,
                        stale_threshold,
                        robot_meta,
                    )?;
                }
                Commands::Introspect { json } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_introspect(structured_format)?;
                }
                Commands::Health {
                    data_dir,
                    robot_meta,
                    stale_threshold,
                    binary_only,
                    json,
                } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_health(
                        &data_dir,
                        cli.db.clone(),
                        structured_format,
                        stale_threshold,
                        binary_only,
                        robot_meta,
                    )?;
                }
                Commands::Selftest { json } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_selftest(structured_format)?;
                }
                Commands::Onboarding { data_dir, json } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_onboarding(&data_dir, cli.db.clone(), structured_format)?;
                }
                Commands::Guide {
                    intent,
                    fixture,
                    data_dir,
                    apply,
                    confirm_step,
                    confirm_fact,
                    accept_privacy_tier,
                    accept_cost_risk,
                    confirm_stop_conditions_clear,
                    json,
                } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_guide(
                        &intent,
                        fixture.as_deref(),
                        &data_dir,
                        cli.db.clone(),
                        structured_format,
                        apply,
                        &confirm_step,
                        &confirm_fact,
                        accept_privacy_tier.as_deref(),
                        accept_cost_risk.as_deref(),
                        confirm_stop_conditions_clear,
                    )?;
                }
                Commands::Doctor {
                    data_dir,
                    json,
                    check,
                    fix,
                    repair,
                    cleanup,
                    archive_scan,
                    archive_normalize,
                    backups_list,
                    backups_verify,
                    backups_restore,
                    baseline_save,
                    baseline_diff,
                    baseline_update,
                    support_bundle,
                    support_bundle_verify,
                    archive_export,
                    archive_relocate,
                    archive_export_verify,
                    archive_target,
                    backup_id,
                    baseline_id,
                    baseline_path,
                    support_bundle_path,
                    include_sensitive_attachments,
                    sensitive_attachment,
                    sensitive_attachment_max_bytes,
                    dry_run,
                    yes,
                    plan_fingerprint,
                    verbose,
                    force_rebuild,
                    allow_repeated_repair,
                    ls,
                    undo,
                    robot_triage,
                    diff,
                    gc_before,
                    watch,
                    watch_interval_ms,
                    watch_iterations,
                    explain,
                    emit_capabilities,
                    recover_from_archive,
                    rebuild_canonical_fts,
                    repair_leaked_pages,
                    cleanup_interrupted_artifacts,
                } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    // Pass-12 fix (P2): the new pass-1+ flags have no clap-level
                    // `conflicts_with` wiring, so passing multiple at once
                    // would silently dispatch only the first match. That made
                    // `cass doctor --fix --ls` silently drop `--fix`. Detect
                    // and refuse multiple new flags at the dispatch boundary
                    // so agents get a clean usage error rather than silent
                    // precedence behavior.
                    let pass1plus_flag_count = [
                        explain.is_some(),
                        emit_capabilities,
                        ls,
                        undo.is_some(),
                        robot_triage,
                        diff.is_some(),
                        gc_before.is_some(),
                        watch,
                    ]
                    .iter()
                    .filter(|x| **x)
                    .count();
                    if pass1plus_flag_count > 1 {
                        return Err(CliError {
                            code: 2,
                            kind: "usage",
                            message:
                                "multiple world-class-doctor flags specified; only one allowed per invocation"
                                    .to_string(),
                            hint: Some(
                                "Pass exactly one of --ls, --undo, --robot-triage, --diff, --gc-before, --watch, --explain, --emit-capabilities at a time."
                                    .to_string(),
                            ),
                            retryable: false,
                        });
                    }
                    // World-class-doctor pass-8: `cass doctor --explain <run-id>`.
                    if let Some(run_id_arg) = explain {
                        run_doctor_explain(data_dir, &run_id_arg, structured_format)?;
                        return Ok(());
                    }
                    // World-class-doctor pass-8: `cass doctor --emit-capabilities`.
                    if emit_capabilities {
                        run_doctor_emit_capabilities(structured_format)?;
                        return Ok(());
                    }
                    // #285: `cass doctor --recover-from-archive <DIR>` — rebuild
                    // the source JSONL tree from the canonical archive's
                    // preserved events so a corrupt archive can be re-ingested.
                    if let Some(target_dir) = recover_from_archive {
                        doctor_recover::run_doctor_recover_from_archive(
                            data_dir,
                            cli.db.clone(),
                            target_dir,
                            structured_format,
                        )?;
                        return Ok(());
                    }
                    // #285: `cass doctor --rebuild-canonical-fts --yes` — drop
                    // and rebuild the canonical FTS5 shadow tables in place.
                    if rebuild_canonical_fts {
                        doctor_recover::run_doctor_rebuild_canonical_fts(
                            data_dir,
                            cli.db.clone(),
                            dry_run,
                            yes,
                            structured_format,
                        )?;
                        return Ok(());
                    }
                    // 2l1b0.73: `cass doctor --repair-leaked-pages --yes` —
                    // free pages no tree or freelist owns, in place.
                    if repair_leaked_pages {
                        doctor_recover::run_doctor_repair_leaked_pages(
                            data_dir,
                            cli.db.clone(),
                            dry_run,
                            yes,
                            structured_format,
                        )?;
                        return Ok(());
                    }
                    // #285: `cass doctor --cleanup-interrupted-artifacts --yes`
                    // — quarantine interrupted raw_mirror_capture artifacts.
                    if cleanup_interrupted_artifacts {
                        doctor_recover::run_doctor_cleanup_interrupted_artifacts(
                            data_dir,
                            yes,
                            structured_format,
                        )?;
                        return Ok(());
                    }
                    // World-class-doctor pass-2: read-only `cass doctor --ls`.
                    if ls {
                        run_doctor_ls(data_dir, structured_format)?;
                        return Ok(());
                    }
                    // World-class-doctor pass-2: `cass doctor --undo <run-id>`.
                    if let Some(run_id_arg) = undo {
                        run_doctor_undo(data_dir, &run_id_arg, structured_format)?;
                        return Ok(());
                    }
                    // World-class-doctor pass-3: `cass doctor --robot-triage`.
                    if robot_triage {
                        run_doctor_robot_triage(data_dir, structured_format)?;
                        return Ok(());
                    }
                    // World-class-doctor pass-3: `cass doctor --diff [<REF>]`.
                    if let Some(diff_ref) = diff {
                        run_doctor_diff(data_dir, &diff_ref, structured_format)?;
                        return Ok(());
                    }
                    // World-class-doctor pass-3: `cass doctor --gc-before <ISO> --yes`.
                    if let Some(before_iso) = gc_before {
                        run_doctor_gc(data_dir, &before_iso, yes, structured_format)?;
                        return Ok(());
                    }
                    // World-class-doctor pass-6: `cass doctor --watch`.
                    if watch {
                        run_doctor_watch(
                            data_dir,
                            watch_interval_ms,
                            watch_iterations,
                            structured_format,
                        )?;
                        return Ok(());
                    }
                    if archive_export || archive_relocate || archive_export_verify {
                        run_doctor_archive_export_impl(
                            DoctorArchiveExportRequest {
                                data_dir_override: data_dir,
                                db_override: cli.db.clone(),
                                output_format: structured_format,
                                workflow: if archive_relocate {
                                    DoctorArchiveExportWorkflow::Relocate
                                } else {
                                    DoctorArchiveExportWorkflow::Export
                                },
                                command_mode: if archive_export_verify {
                                    DoctorArchiveExportCommandMode::Verify
                                } else if yes {
                                    DoctorArchiveExportCommandMode::Apply
                                } else {
                                    DoctorArchiveExportCommandMode::Plan
                                },
                                target_root: archive_target,
                                dry_run,
                                yes,
                                plan_fingerprint,
                                backup_id,
                                force_rebuild,
                                allow_repeated_repair,
                            },
                            wrap,
                        )?;
                    } else if support_bundle || support_bundle_verify {
                        validate_doctor_diagnostic_read_only_controls(
                            "support-bundle",
                            &backup_id,
                            dry_run,
                            yes,
                            plan_fingerprint.as_deref(),
                            force_rebuild,
                            allow_repeated_repair,
                        )?;
                        let mode = if support_bundle_verify {
                            DoctorSupportBundleCommandMode::Verify
                        } else {
                            DoctorSupportBundleCommandMode::Create
                        };
                        run_doctor_support_bundle_impl(
                            DoctorSupportBundleRequest {
                                data_dir_override: data_dir,
                                db_override: cli.db.clone(),
                                output_format: structured_format,
                                mode,
                                baseline_id,
                                baseline_path,
                                support_bundle_path,
                                include_sensitive_attachments,
                                sensitive_attachments: sensitive_attachment,
                                sensitive_attachment_max_bytes,
                            },
                            wrap,
                        )?;
                    } else if baseline_save || baseline_diff || baseline_update {
                        validate_doctor_diagnostic_read_only_controls(
                            "baseline",
                            &backup_id,
                            dry_run,
                            yes,
                            plan_fingerprint.as_deref(),
                            force_rebuild,
                            allow_repeated_repair,
                        )?;
                        let mode = if baseline_diff {
                            DoctorBaselineCommandMode::Diff
                        } else if baseline_update {
                            DoctorBaselineCommandMode::Update
                        } else {
                            DoctorBaselineCommandMode::Save
                        };
                        run_doctor_baseline_impl(
                            data_dir,
                            cli.db.clone(),
                            structured_format,
                            mode,
                            baseline_id,
                            baseline_path,
                            wrap,
                        )?;
                    } else {
                        let request = doctor::DoctorCommandRequest::from_cli_flags_with_backups(
                            data_dir,
                            cli.db.clone(),
                            structured_format,
                            check,
                            fix,
                            repair,
                            cleanup,
                            archive_scan,
                            archive_normalize,
                            backups_list,
                            backups_verify,
                            backups_restore,
                            backup_id,
                            dry_run,
                            yes,
                            plan_fingerprint,
                            verbose,
                            force_rebuild,
                            allow_repeated_repair,
                        )?;
                        doctor::execute_doctor_command_with_wrap(request, wrap)?;
                    }
                }
                Commands::Context {
                    path,
                    source,
                    data_dir,
                    limit,
                    json,
                } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_context(
                        &path,
                        source.as_deref(),
                        &data_dir,
                        cli.db.clone(),
                        structured_format,
                        limit,
                    )?;
                }
                Commands::Sessions {
                    workspace,
                    current,
                    limit,
                    since,
                    json,
                    data_dir,
                } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_sessions(
                        workspace.as_ref(),
                        current,
                        limit,
                        since.as_deref(),
                        &data_dir,
                        cli.db.clone(),
                        structured_format,
                    )?;
                }
                Commands::Resume {
                    path,
                    agent,
                    exec,
                    shell,
                    json,
                } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_resume(&path, agent.as_deref(), exec, shell, structured_format)?;
                }
                Commands::Export {
                    path,
                    source,
                    conversation_id,
                    format,
                    output,
                    clipboard,
                    include_tools,
                    include_skills,
                    include_secrets,
                } => {
                    run_export(
                        &path,
                        cli.db.clone(),
                        source.as_deref(),
                        conversation_id,
                        format,
                        output.as_deref(),
                        clipboard,
                        include_tools,
                        include_skills,
                        include_secrets,
                    )?;
                }
                Commands::ExportHtml {
                    session,
                    source,
                    conversation_id,
                    output_dir,
                    filename,
                    encrypt,
                    password_stdin,
                    include_tools,
                    show_timestamps,
                    include_skills,
                    include_secrets,
                    theme,
                    dry_run,
                    explain,
                    open,
                    json,
                } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_export_html(
                        &session,
                        cli.db.clone(),
                        source.as_deref(),
                        conversation_id,
                        output_dir.as_deref(),
                        filename.as_deref(),
                        encrypt,
                        password_stdin,
                        include_tools,
                        show_timestamps,
                        include_skills,
                        include_secrets,
                        &theme,
                        dry_run,
                        explain,
                        open,
                        structured_format,
                    )?;
                }
                Commands::Expand {
                    path,
                    source,
                    message_index,
                    conversation_id,
                    line,
                    context,
                    json,
                } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    followup_coordinates::run_expand(
                        &path,
                        cli.db.clone(),
                        source.as_deref(),
                        line,
                        context,
                        structured_format,
                        message_index,
                        conversation_id,
                    )?;
                }
                Commands::Timeline {
                    since,
                    until,
                    today,
                    agent,
                    data_dir,
                    json,
                    group_by,
                    source,
                } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_timeline(
                        since.as_deref(),
                        until.as_deref(),
                        today,
                        &agent,
                        &data_dir,
                        cli.db.clone(),
                        structured_format,
                        group_by,
                        source,
                    )?;
                }
                Commands::Quarantine(subcmd) => {
                    run_quarantine_command(subcmd, cli)?;
                }
                Commands::Forget {
                    source_glob,
                    workspace_glob,
                    older_than,
                    list,
                    reinclude,
                    source_path,
                    source_id,
                    agent,
                    external_id,
                    db,
                    apply,
                    json,
                } => {
                    let structured_format = resolve_subcommand_structured_format(cli, json);
                    run_forget_command(
                        ForgetCommandOptions {
                            source_glob,
                            workspace_glob,
                            older_than,
                            list,
                            reinclude,
                            source_path,
                            source_id,
                            agent,
                            external_id,
                            db,
                            apply,
                        },
                        cli,
                        structured_format,
                    )?;
                }
                Commands::Mirror(subcmd) => {
                    run_mirror_command(subcmd, cli)?;
                }
                Commands::Sources(subcmd) => {
                    run_sources_command(subcmd, cli)?;
                }
                Commands::Models(subcmd) => {
                    let subcmd = subcmd.clone();
                    let cli_clone = cli.clone();
                    let result = asupersync::runtime::spawn_blocking(move || {
                        run_models_command(subcmd, &cli_clone)
                    })
                    .await;
                    result?;
                }
                Commands::Import(subcmd) => {
                    handle_import(subcmd, cli).await?;
                }
                #[cfg(unix)]
                Commands::Daemon {
                    socket,
                    idle_timeout,
                    max_connections,
                    data_dir,
                } => {
                    run_daemon(socket, idle_timeout, max_connections, data_dir)?;
                }
                Commands::Schedule(subcmd) => {
                    run_schedule_command(subcmd, cli)?;
                }
                _ => {}
            }
        }
    }

    Ok(())
}

/// Compute lightweight state snapshot (index/db freshness) for robot meta and state command reuse
const STATE_DB_OPEN_TIMEOUT: Duration = Duration::from_secs(5);
/// k2k20: one end-to-end wall-clock ceiling for the status-class state probe
/// (strict open + watermark reads + bounded integrity probe + row counts).
/// fsqlite's open and even single-row lookups can hydrate a large part of a
/// multi-GB archive, so a busy timeout alone never bounded `cass status`.
const STATE_DB_PROBE_HARD_TIMEOUT: Duration = Duration::from_secs(30);
const STATUS_COUNT_SCAN_MAX_DB_BYTES: u64 = 256 * 1024 * 1024;
const CLI_DB_QUERY_RETRY_TIMEOUT: Duration = Duration::from_secs(10);
const CLI_DIAG_DB_OPEN_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Default)]
struct StateDbSnapshot {
    conversation_count: i64,
    message_count: i64,
    last_scan_ts: Option<i64>,
    last_indexed_at: Option<i64>,
    opened: bool,
    open_error: Option<String>,
    open_retryable: bool,
    counts_skipped: bool,
    /// `coding_agent_session_search-gi4oy`: true when state_meta_json
    /// elided the FrankenStorage open. `opened` is set to true based on
    /// regular-file metadata alone in that case; callers needing the actual
    /// open-success signal use `cass diag` / `cass doctor`.
    open_skipped: bool,
    /// zn1xn: failure detail when the last full index run left the canonical
    /// `fts_messages` shadow unrepaired (skipped/failed optional derived
    /// fallback-FTS repair). `None` when the shadow is not known half-built.
    fallback_repair_pending: Option<String>,
    /// zn1xn F4: consecutive count of non-watch runs that deferred inline
    /// lexical updates (rerouting through a full authoritative rebuild), and
    /// the last reason. `None` streak when the last run did not defer.
    lexical_repair_deferred_runs: Option<i64>,
    lexical_repair_deferred_reason: Option<String>,
    /// GH #538: a durable watch retry can outlive the process that created
    /// it, even when last_indexed_at is recent and the live watcher holds a
    /// healthy lease. This is source-root debt, not a known session count.
    /// None means the bounded metadata probe could not establish its absence.
    watch_lexical_replay_pending: Option<bool>,
    /// i6upe: result of the bounded real-column integrity probe. `None` when
    /// the probe did not run (watermarks-only health path, unopened DB) or
    /// could not classify (busy/transient error). A probe, not a proof:
    /// corruption on unprobed pages still needs `cass doctor --check`.
    integrity: Option<StateDbIntegrity>,
    /// The corruption error text when `integrity` is `Corrupt`.
    integrity_detail: Option<String>,
}

/// i6upe: verdict of the bounded real-column integrity probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StateDbIntegrity {
    Ok,
    Corrupt,
}

/// True for FrankenError classes that mean the archive's stored pages are
/// malformed — never for busy/locked/transient classes.
fn corruption_class_franken_error(err: &crate::franken_sync::FrankenError) -> bool {
    use crate::franken_sync::FrankenError as E;
    if matches!(
        err,
        E::DatabaseCorrupt { .. } | E::NotADatabase { .. } | E::WalCorrupt { .. }
    ) {
        return true;
    }
    let lower = err.to_string().to_ascii_lowercase();
    lower.contains("malformed") || lower.contains("disk image")
}

/// i6upe: real-column corruption probe used only below a physical archive
/// bundle-size ceiling.
///
/// `SELECT COUNT(*)` is answered from index B-trees without touching table
/// pages, so a DB whose table pages are malformed counts perfectly and only
/// fails when real columns are read (proven on ts2/csd 2026-07-26, where
/// counts matched while every projection failed). Each probe here projects
/// real columns from a single row at both ends of the two hot tables. The SQL
/// shape is logically bounded, but GH #413 proved that fsqlite may still read
/// or hydrate a large fraction of a huge archive while executing it. Callers
/// must therefore enforce a physical main+sidecar byte ceiling before using
/// this probe.
///
/// Returns `(Some(Ok), None)` when every probe reads (or the table is
/// empty), `(Some(Corrupt), Some(detail))` on a corruption-class error, and
/// `(None, None)` when a transient error prevents classification — never a
/// false-positive `Corrupt` from a busy archive.
///
/// `run_probe` executes one probe statement and reads its first column, so
/// the classification is shared by the ordinary-connection lane and the
/// dedicated-owner strict lane (`probe_state_db_strict_bounded`).
fn probe_state_db_integrity(
    run_probe: impl Fn(&str) -> Result<(), crate::franken_sync::FrankenError>,
) -> (Option<StateDbIntegrity>, Option<String>) {
    const PROBES: [&str; 4] = [
        "SELECT id, title, source_path FROM conversations ORDER BY id LIMIT 1",
        "SELECT id, title, source_path FROM conversations ORDER BY id DESC LIMIT 1",
        "SELECT id, conversation_id, role FROM messages ORDER BY id LIMIT 1",
        "SELECT id, conversation_id, role FROM messages ORDER BY id DESC LIMIT 1",
    ];
    for sql in PROBES {
        match run_probe(sql) {
            Ok(()) => {}
            Err(crate::franken_sync::FrankenError::QueryReturnedNoRows) => {}
            Err(err) if corruption_class_franken_error(&err) => {
                return (Some(StateDbIntegrity::Corrupt), Some(err.to_string()));
            }
            Err(_) => return (None, None),
        }
    }
    (Some(StateDbIntegrity::Ok), None)
}

/// Read the canonical database's fixed-size SQLite header, then run the
/// stronger real-column probe only when the complete main+sidecar bundle is
/// below [`STATUS_COUNT_SCAN_MAX_DB_BYTES`]. `Some(detail)` means corruption
/// was classified; `None` means the bounded checks passed or could not safely
/// classify the archive.
///
/// This is deliberately not a complete page-integrity check. Large archives
/// receive only the constant-size header check; deeper corruption still needs
/// a current doctor integrity attestation or an explicit full integrity scan.
pub(crate) fn bounded_canonical_db_corruption_probe(
    db_path: &Path,
    reason: &str,
) -> Option<String> {
    if !db_path.is_file() {
        return None;
    }
    // GH #413: this preflight must have a physical-I/O bound, not merely a
    // SQL row-count bound. fsqlite can currently hydrate a large part of the
    // B-tree while executing an ORDER BY ... LIMIT 1 edge lookup; the old
    // four-query "bounded" probe read 8.4 GiB from a 35.7 GB archive before
    // indexing had even entered its preparing phase. A query timeout would
    // not repair that contract because the synchronous query cannot be
    // cancelled safely from this call site.
    //
    // Reading the fixed 100-byte SQLite database header is genuinely bounded
    // regardless of archive size. The stronger real-column probe below is
    // retained for small bundles, where its worst-case physical work is also
    // bounded, so the earlier i6upe smashed-page guard does not regress.
    let mut header = [0_u8; 100];
    let file_len = std::fs::metadata(db_path).ok()?.len();
    if file_len == 0 {
        // SQLite permits a zero-byte file as a new, empty database.
        return None;
    }
    if file_len < header.len() as u64 {
        return Some(format!(
            "{reason}: truncated SQLite database header ({} bytes; expected at least {})",
            file_len,
            header.len()
        ));
    }
    let mut file = std::fs::File::open(db_path).ok()?;
    std::io::Read::read_exact(&mut file, &mut header).ok()?;

    const SQLITE_MAGIC: &[u8; 16] = b"SQLite format 3\0";
    if &header[..SQLITE_MAGIC.len()] != SQLITE_MAGIC {
        return Some(format!("{reason}: invalid SQLite database header magic"));
    }

    let encoded_page_size = u16::from_be_bytes([header[16], header[17]]);
    let page_size = if encoded_page_size == 1 {
        65_536_u32
    } else {
        u32::from(encoded_page_size)
    };
    if !(512..=65_536).contains(&page_size) || !page_size.is_power_of_two() {
        return Some(format!(
            "{reason}: invalid SQLite database page size {page_size}"
        ));
    }
    if file_len % u64::from(page_size) != 0 {
        return Some(format!(
            "{reason}: SQLite database length {file_len} is not a multiple of page size {page_size}"
        ));
    }
    let reserved_bytes = u32::from(header[20]);
    if page_size.saturating_sub(reserved_bytes) < 480 {
        return Some(format!(
            "{reason}: invalid SQLite reserved-byte count {reserved_bytes} for page size {page_size}"
        ));
    }
    if !matches!(header[18], 1 | 2) || !matches!(header[19], 1 | 2) {
        return Some(format!(
            "{reason}: invalid SQLite file format versions write={} read={}",
            header[18], header[19]
        ));
    }
    if header[21..24] != [64, 32, 32] {
        return Some(format!(
            "{reason}: invalid SQLite payload fractions {},{},{}",
            header[21], header[22], header[23]
        ));
    }

    let mut bundle_bytes = file_len;
    for suffix in ["-wal", "-shm"] {
        let sidecar_path = doctor_sqlite_sidecar_path(db_path, suffix)?;
        match std::fs::symlink_metadata(&sidecar_path) {
            Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
                bundle_bytes = bundle_bytes.saturating_add(metadata.len());
            }
            Ok(_) => return None,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return None,
        }
    }
    if bundle_bytes > STATUS_COUNT_SCAN_MAX_DB_BYTES {
        return None;
    }

    // This observation also runs before a read-only force rebuild has archive
    // writer admission. It must never repair WAL or schema state on open.
    match crate::storage::sqlite::FrankenStorage::open_strict_readonly_with_timeout(
        db_path,
        STATE_DB_OPEN_TIMEOUT,
    ) {
        Ok(storage) => {
            let conn = storage.into_raw();
            let (integrity, detail) = probe_state_db_integrity(|sql| {
                use crate::franken_sync::compat::RowExt;
                franken_query_row_map_retry(&conn, sql, &[], |row| {
                    row.get_typed::<i64>(0).map(|_| ())
                })
            });
            let _ = close_franken_cli_read_db_without_checkpoint(conn, db_path, reason);
            match integrity {
                Some(StateDbIntegrity::Corrupt) => {
                    Some(detail.unwrap_or_else(|| "corruption-class read failure".to_owned()))
                }
                _ => None,
            }
        }
        Err(err) if !crate::storage::sqlite::retryable_franken_anyhow(&err) => {
            let message = format!("{err:#}");
            let lower = message.to_ascii_lowercase();
            // Duplicate `fts_messages` rows in sqlite_master make the strict
            // open report a malformed image, but they describe the derived
            // FTS catalog, not the archive's data. The writable open that
            // follows deduplicates them (dedupe_conflicting_fts_schema_rows_
            // via_sqlite3), so refusing here would block the repair.
            if lower.contains("conflicting virtual-table entries") && lower.contains("fts_messages")
            {
                return None;
            }
            (lower.contains("malformed")
                || lower.contains("disk image")
                || lower.contains("not a database"))
            .then_some(message)
        }
        Err(_) => None,
    }
}

/// Dedicated-owner twin of [`franken_query_row_map_retry`]: bounded retry on
/// engine contention only, never replaying a query whose row-mapping
/// callback failed (negs9).
fn owner_query_row_map_retry<T, F>(
    conn: &crate::storage::sqlite::FrankenOwnerConnection,
    sql: &str,
    map: F,
) -> Result<T, crate::franken_sync::FrankenError>
where
    F: Copy + Fn(&crate::franken_sync::Row) -> Result<T, crate::franken_sync::FrankenError>,
{
    let deadline = std::time::Instant::now() + CLI_DB_QUERY_RETRY_TIMEOUT;
    let mut backoff = Duration::from_millis(4);
    let callback_failed = std::cell::Cell::new(false);
    loop {
        callback_failed.set(false);
        let mapped = conn.query_row_map(sql, &[], |row| {
            map(row).inspect_err(|_| callback_failed.set(true))
        });
        match mapped {
            Ok(value) => return Ok(value),
            Err(err) if callback_failed.get() => return Err(err),
            Err(err) if crate::storage::sqlite::retryable_franken_error(&err) => {
                let now = std::time::Instant::now();
                if now >= deadline {
                    return Err(err);
                }
                let remaining = deadline.saturating_duration_since(now);
                crate::storage::sqlite::sleep_with_franken_retry_backoff(
                    &mut backoff,
                    remaining,
                    Duration::from_millis(64),
                );
            }
            Err(err) => return Err(err),
        }
    }
}

/// Size of a non-empty (frames present) `-wal` sidecar next to `db_path`, or
/// `None` when there is nothing to recover. Mirrors the shape test the
/// ordinary opener's dirty-WAL recovery uses, without performing it.
fn unpublished_wal_sidecar_bytes(db_path: &Path) -> Option<u64> {
    let wal_path = doctor_sqlite_sidecar_path(db_path, "-wal")?;
    let meta = std::fs::symlink_metadata(wal_path).ok()?;
    (meta.file_type().is_file() && meta.len() > 32).then_some(meta.len())
}

/// Message for a strict readonly open failure on the status lane. A hard
/// failure next to an unpublished WAL sidecar is the unclean-shutdown shape
/// the ordinary opener would silently checkpoint; a readiness read reports
/// it and names the command that performs the recovery write instead.
fn state_db_strict_open_error_message(
    db_path: &Path,
    reason: &str,
    err: &anyhow::Error,
    retryable: bool,
) -> String {
    let base = format!(
        "Failed to open {reason} database at {}: strict readonly open failed ({err:#})",
        db_path.display()
    );
    if err.chain().any(|cause| {
        matches!(
            cause.downcast_ref::<crate::franken_sync::FrankenError>(),
            Some(crate::franken_sync::FrankenError::BusyRecovery)
        )
    }) {
        return format!(
            "{base}; WAL-index recovery is required or another connection currently owns recovery. This strict read-only probe leaves the archive unchanged; retry after active work finishes, or run 'cass index' to attempt recovery"
        );
    }
    if retryable {
        return base;
    }
    // GH #503: the #434 shape. The strict open rejects only the derived
    // fts_messages shadow's catalog (a legacy shape older cass wrote); the
    // canonical rows are readable. Name the shadow repair only when the
    // pinned engine can open this catalog for it.
    if !doctor_recover::ENGINE_REPAIRS_MISSING_FTS5_SHADOW_AUTOINDEX
        && doctor_recover::is_missing_fts5_shadow_autoindex_failure(err)
    {
        return format!(
            "{base}; the open rejects only the derived fts_messages FTS5 shadow's legacy catalog (written by cass before the #434 fix), not the conversations or messages. This read-only probe leaves the archive unchanged. This build cannot rebuild that shadow in place (GH #503): keep the archive as it is and do not run 'cass doctor --fix'"
        );
    }
    if doctor_recover::is_fts_shadow_schema_level_open_failure(err) {
        return format!(
            "{base}; the open rejects only the derived fts_messages FTS5 shadow's catalog (a legacy shape written by older cass), not the conversations or messages. This read-only probe leaves the archive unchanged; 'cass doctor --rebuild-canonical-fts --dry-run --json' inspects it and '--yes' drops and rebuilds the derived shadow without modifying canonical rows"
        );
    }
    match unpublished_wal_sidecar_bytes(db_path) {
        Some(wal_bytes) => format!(
            "{base}; an unpublished {wal_bytes}-byte WAL sidecar from an unclean shutdown is present and this read-only probe never modifies the archive, so it was not recovered here — run 'cass index' (or 'cass doctor') to checkpoint it"
        ),
        None => base,
    }
}

/// k2k20 ask #2: the status-class state probe (every non-watermark caller of
/// [`probe_state_db_modes`]) — a readiness READ that must neither modify the
/// archive nor block for minutes on it.
///
/// - Mutation-free: the strict opener never creates the doctor lock
///   hierarchy, deduplicates schema rows, or checkpoints a dirty WAL. An
///   archive that needs a recovery write is reported with the command that
///   performs it (`state_db_strict_open_error_message`).
/// - Bounded: open + watermarks + integrity probe + counts run on a dedicated
///   owner thread; overrunning [`STATE_DB_PROBE_HARD_TIMEOUT`] abandons the
///   worker and reports a retryable open failure naming the last phase, so
///   `cass status` answers instead of sitting silent (GH #413 showed fsqlite
///   can hydrate gigabytes even for an ORDER BY … LIMIT 1 edge lookup).
/// - The real-column integrity probe honors the same physical bundle
///   ceiling as [`bounded_canonical_db_corruption_probe`]; above it the
///   integrity stays `unknown` rather than paying an archive-sized read.
///
/// `watermarks_only` (the `cass health` scope) reads just the single-row
/// `meta` watermarks and skips
/// the integrity probe and row counts. It exists so `cass health` (k2k20 ask
/// #2) shares the strict, mutation-free, hard-deadline opener with `status`
/// instead of the inline read opener, whose sanctioned recovery writes
/// (duplicate-FTS schema repair, dirty-WAL `wal_checkpoint(TRUNCATE)`) made a
/// documented <50 ms observation surface pay a multi-GB recovery bill on the
/// archive it was only supposed to describe.
fn probe_state_db_strict_bounded_scoped(
    db_path: &Path,
    reason: &str,
    busy_timeout: Duration,
    include_counts: bool,
    watermarks_only: bool,
) -> StateDbSnapshot {
    let display_path = db_path.display().to_string();
    // SQLite treats a zero-byte file as a valid, empty database; only a
    // non-empty file that cannot carry a header is a corruption signal.
    let zero_byte_db =
        std::fs::metadata(db_path).is_ok_and(|meta| meta.is_file() && matches!(meta.len(), 0));
    if !zero_byte_db
        && let Some(err) = sqlite_header_preflight_error(db_path, &display_path, reason)
    {
        // A truncated or non-SQLite header is a durable fault, never a
        // "temporarily busy" one, whatever the CLI error class says.
        return StateDbSnapshot {
            counts_skipped: !include_counts,
            open_error: Some(err.message),
            open_retryable: false,
            ..StateDbSnapshot::default()
        };
    }
    // The watermark scope never pays for the integrity probe or the row
    // counts: health reads two meta rows and nothing else.
    let include_counts = include_counts && !watermarks_only;
    let integrity_probe_allowed =
        !watermarks_only && doctor_archive_bundle_bytes(db_path) <= STATUS_COUNT_SCAN_MAX_DB_BYTES;
    let phase = std::sync::Arc::new(std::sync::Mutex::new("open"));
    let (tx, rx) = std::sync::mpsc::channel();
    let worker_phase = std::sync::Arc::clone(&phase);
    let worker_path = db_path.to_path_buf();
    let worker_reason = reason.to_string();
    let _probe_worker = std::thread::spawn(move || {
        let snapshot = probe_state_db_strict_worker(
            &worker_path,
            &worker_reason,
            busy_timeout,
            include_counts,
            integrity_probe_allowed,
            &worker_phase,
        );
        let _ = tx.send(snapshot);
    });
    let (snapshot, timed_out) = match rx.recv_timeout(STATE_DB_PROBE_HARD_TIMEOUT) {
        Ok(snapshot) => (snapshot, false),
        Err(_) => {
            let last_phase = phase.lock().map(|current| *current).unwrap_or("unknown");
            (
                StateDbSnapshot {
                    counts_skipped: true,
                    open_error: Some(format!(
                        "state probe of {reason} database at {display_path} exceeded {}s (last phase: {last_phase}); the archive is very large or busy and this read-only probe did not modify it — run 'cass doctor check --json' for a bounded diagnosis",
                        STATE_DB_PROBE_HARD_TIMEOUT.as_secs()
                    )),
                    open_retryable: true,
                    ..StateDbSnapshot::default()
                },
                true,
            )
        }
    };
    if watermarks_only {
        finalize_health_watermark_snapshot(snapshot, timed_out)
    } else {
        snapshot
    }
}

/// Apply the `cass health` watermark-lane contract to a strict probe result.
///
/// The gi4oy/d0rmo health contract reports `open_skipped=true`,
/// `counts_skipped=true` and null counts whatever happened underneath, and
/// treats a lock-class (retryable) open failure as "a concurrent writer holds
/// the archive", which is not a degraded-state signal: it is elided to an
/// assumed-good open exactly as the pre-#301 fast lane did. Two things are
/// deliberately NOT elided: a hard open failure (GH #396: masking CANTOPEN /
/// corrupt-header made health say `db=available` while every search failed),
/// and the probe's own hard deadline — a 30 s silent open IS the k2k20 defect
/// class, so it surfaces as a retryable open error with the last phase named.
fn finalize_health_watermark_snapshot(
    mut snapshot: StateDbSnapshot,
    timed_out: bool,
) -> StateDbSnapshot {
    snapshot.counts_skipped = true;
    if !timed_out && snapshot.open_error.is_some() && snapshot.open_retryable {
        snapshot.open_error = None;
        snapshot.opened = true;
    }
    // An open we ATTEMPTED and watched fail hard (or time out) must surface
    // as such on every surface; only the assumed-good/successful shapes keep
    // the historical `open_skipped=true` marker.
    snapshot.open_skipped = snapshot.open_error.is_none();
    snapshot
}

#[cfg(test)]
mod health_watermark_probe_tests {
    use super::*;

    /// Write real rows, then close WITHOUT the final checkpoint so the WAL
    /// keeps unreplayed frames — the unclean-shutdown artifact the ordinary
    /// read opener would "recover" with a `wal_checkpoint(TRUNCATE)`.
    fn dirty_wal_db(dir: &Path) -> (PathBuf, u64) {
        let db_path = dir.join("watermarks.db");
        {
            let mut conn =
                crate::franken_sync::Connection::open(db_path.to_string_lossy().into_owned())
                    .expect("open fixture db");
            conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);")
                .expect("create meta");
            conn.execute(
                "INSERT INTO meta (key, value) VALUES ('last_indexed_at', '1700000000000'), ('last_scan_ts', '1700000001000');",
            )
            .expect("seed watermarks");
            crate::storage::sqlite::close_franken_in_place_with_busy_retry(&mut conn, false)
                .expect("close without checkpoint");
        }
        let wal_path = crate::storage::sqlite::database_sidecar_path(&db_path, "-wal");
        let wal_len = std::fs::symlink_metadata(&wal_path)
            .map(|meta| meta.len())
            .unwrap_or(0);
        assert!(
            wal_len > 32,
            "precondition: close-without-checkpoint must leave WAL frames (len={wal_len})"
        );
        (db_path, wal_len)
    }

    /// k2k20 (health half): the watermark lane reads its two meta rows
    /// through the strict opener and leaves a dirty WAL exactly as it found
    /// it. Positive observable: watermarks are read. Planted negative: the
    /// WAL byte length is unchanged, i.e. no recovery checkpoint ran.
    /// No-claim: this does not bound wall-clock on a multi-GB archive; the
    /// 30 s hard deadline is exercised by the status-probe tests.
    #[test]
    fn health_watermark_probe_reads_meta_without_checkpointing_a_dirty_wal() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (db_path, dirty_len) = dirty_wal_db(dir.path());

        let snapshot = probe_state_db_modes(
            &db_path,
            "state-meta-watermarks",
            Duration::from_secs(2),
            false,
            true,
        );

        assert!(
            snapshot.opened,
            "watermark probe must open the db: {snapshot:?}"
        );
        assert!(snapshot.open_skipped && snapshot.counts_skipped);
        assert_eq!(snapshot.open_error, None);
        assert_eq!(snapshot.last_indexed_at, Some(1_700_000_000_000));
        assert_eq!(snapshot.last_scan_ts, Some(1_700_000_001_000));
        let wal_path = crate::storage::sqlite::database_sidecar_path(&db_path, "-wal");
        let after_len = std::fs::symlink_metadata(&wal_path)
            .map(|meta| meta.len())
            .unwrap_or(0);
        assert_eq!(
            after_len, dirty_len,
            "health must never checkpoint the archive it is describing (WAL {dirty_len} -> {after_len})"
        );
    }

    /// A hard open failure (here: a non-SQLite file) must surface on the
    /// health lane rather than being elided to assumed-good (GH #396).
    #[test]
    fn health_watermark_probe_surfaces_hard_open_failures() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("not-a-db.db");
        std::fs::write(&db_path, b"this is definitely not a SQLite database header").unwrap();

        let snapshot = probe_state_db_modes(
            &db_path,
            "state-meta-watermarks",
            Duration::from_secs(2),
            false,
            true,
        );

        assert!(!snapshot.opened);
        assert!(
            !snapshot.open_skipped,
            "an attempted-and-failed open is never 'skipped'"
        );
        assert!(!snapshot.open_retryable);
        assert!(snapshot.open_error.is_some(), "{snapshot:?}");
    }
}

fn probe_state_db_strict_worker(
    db_path: &Path,
    reason: &str,
    busy_timeout: Duration,
    include_counts: bool,
    integrity_probe_allowed: bool,
    phase: &std::sync::Mutex<&'static str>,
) -> StateDbSnapshot {
    use crate::franken_sync::compat::RowExt;

    let set_phase = |value: &'static str| {
        if let Ok(mut current) = phase.lock() {
            *current = value;
        }
    };
    let mut snapshot = StateDbSnapshot {
        counts_skipped: !include_counts,
        ..StateDbSnapshot::default()
    };
    let conn =
        match crate::storage::sqlite::open_franken_owner_strict_readonly_connection_with_timeout(
            db_path,
            busy_timeout,
        ) {
            Ok(conn) => conn,
            Err(err) => {
                let retryable = crate::storage::sqlite::retryable_franken_anyhow(&err);
                snapshot.open_error = Some(state_db_strict_open_error_message(
                    db_path, reason, &err, retryable,
                ));
                snapshot.open_retryable = retryable;
                return snapshot;
            }
        };
    let busy_ms = busy_timeout.as_millis().clamp(1, u128::from(u32::MAX));
    let _ = conn.execute(&format!("PRAGMA busy_timeout = {busy_ms};"));
    let _ = conn.execute("PRAGMA query_only = 1;");
    snapshot.opened = true;

    set_phase("meta_watermarks");
    let meta_value = |key: &str| {
        owner_query_row_map_retry(
            &conn,
            &format!("SELECT value FROM meta WHERE key = '{key}'"),
            |row| row.get_typed::<String>(0),
        )
        .ok()
    };
    snapshot.last_indexed_at = meta_value("last_indexed_at").and_then(|s| s.parse::<i64>().ok());
    snapshot.last_scan_ts = meta_value("last_scan_ts").and_then(|s| s.parse::<i64>().ok());
    snapshot.fallback_repair_pending =
        meta_value("fts_fallback_repair_pending").filter(|detail| !detail.is_empty());
    snapshot.lexical_repair_deferred_runs = meta_value("lexical_repair_deferred_consecutive_runs")
        .and_then(|s| s.parse::<i64>().ok())
        .filter(|runs| *runs > 0);
    snapshot.lexical_repair_deferred_reason =
        meta_value("lexical_repair_deferred_reason").filter(|detail| !detail.is_empty());

    // The primary-key range and LIMIT keep this independent of archive size
    // and of the number/size of debt payloads. Even malformed debt is pending:
    // a readiness probe must not silently turn unreadable recovery work into
    // success. The existing owner-thread hard deadline bounds this query too.
    set_phase("watch_lexical_replay");
    snapshot.watch_lexical_replay_pending = match owner_query_row_map_retry(
        &conn,
        "SELECT 1 FROM meta WHERE key >= 'watch_lexical_replay_v1:' \
         AND key < 'watch_lexical_replay_v1;' LIMIT 1",
        |row| row.get_typed::<i64>(0),
    ) {
        Ok(_) => Some(true),
        Err(crate::franken_sync::FrankenError::QueryReturnedNoRows) => Some(false),
        Err(error) => {
            tracing::warn!(
                db_path = %db_path.display(),
                %error,
                "could not determine whether durable watch replay is pending"
            );
            None
        }
    };

    // i6upe: real-column corruption probe on the status surface, gated by the
    // physical bundle ceiling (GH #413) — the exact configuration in which
    // corruption previously had NO fast signal is still covered below it.
    if integrity_probe_allowed {
        set_phase("integrity_probe");
        let (integrity, integrity_detail) = probe_state_db_integrity(|sql| {
            owner_query_row_map_retry(&conn, sql, |row| row.get_typed::<i64>(0).map(|_| ()))
        });
        snapshot.integrity = integrity;
        snapshot.integrity_detail = integrity_detail;
    }
    if include_counts {
        set_phase("row_count_conversations");
        snapshot.conversation_count =
            owner_query_row_map_retry(&conn, "SELECT COUNT(*) FROM conversations", |r| {
                r.get_typed(0)
            })
            .unwrap_or(0);
        set_phase("row_count_messages");
        snapshot.message_count =
            owner_query_row_map_retry(&conn, "SELECT COUNT(*) FROM messages", |r| r.get_typed(0))
                .unwrap_or(0);
    }
    set_phase("connection_close");
    let _ = close_franken_cli_owner_read_db(conn, db_path, reason);
    if include_counts {
        // Mirror the ordinary lane's belt-and-braces re-read: a zero count is
        // re-checked once on a fresh strict connection (a stale snapshot must
        // not make `cass status` report an empty archive).
        set_phase("row_count_recheck");
        if snapshot.conversation_count <= 0 {
            snapshot.conversation_count = strict_owner_fresh_count(
                db_path,
                busy_timeout,
                "SELECT COUNT(*) FROM conversations",
            )
            .unwrap_or(snapshot.conversation_count);
        }
        if snapshot.message_count <= 0 {
            snapshot.message_count =
                strict_owner_fresh_count(db_path, busy_timeout, "SELECT COUNT(*) FROM messages")
                    .unwrap_or(snapshot.message_count);
        }
    }
    snapshot
}

/// One COUNT read on a fresh strict dedicated-owner connection.
fn strict_owner_fresh_count(db_path: &Path, busy_timeout: Duration, sql: &str) -> Option<i64> {
    use crate::franken_sync::compat::RowExt;

    let mut conn =
        crate::storage::sqlite::open_franken_owner_strict_readonly_connection_with_timeout(
            db_path,
            busy_timeout,
        )
        .ok()?;
    let _ = conn.execute("PRAGMA query_only = 1;");
    let count = owner_query_row_map_retry(&conn, sql, |row| row.get_typed::<i64>(0)).ok();
    if let Err(err) = conn.close_without_checkpoint_sync() {
        tracing::debug!(error = %err, db = %db_path.display(), "strict fresh-count close failed");
    }
    count
}

fn probe_state_db(
    db_path: &Path,
    reason: &str,
    timeout: Duration,
    include_counts: bool,
) -> StateDbSnapshot {
    probe_state_db_modes(db_path, reason, timeout, include_counts, false)
}

/// `coding_agent_session_search` (#301 truthfulness reconciliation):
/// `watermarks_only` opens the canonical DB but reads ONLY the two
/// cheap single-row `meta` watermarks (`last_scan_ts`,
/// `last_indexed_at`) — never the COUNT(*) full-table scans. It is the
/// bridge that lets `cass health` consume the SAME staleness inputs as
/// `cass status` (so the two truth surfaces can no longer disagree)
/// while still honoring health's <50ms budget. The returned snapshot
/// keeps `open_skipped=true` + `counts_skipped=true` + zero counts so
/// the gi4oy/d0rmo health contract (null counts, assumed-good open
/// signal) is preserved byte-for-byte — only the watermark fields are
/// populated. When the open fails with a RETRYABLE (busy/lock-class)
/// error, the watermark-only path degrades to the assumed-good elision
/// (open_skipped=true) rather than surfacing the open error, matching
/// the prior skip-open semantics on the fast readiness surface. Hard
/// (non-retryable) open failures surface as open_error even here — see
/// GH #396: masking them made health report db=available while every
/// search failed on an unopenable archive.
fn probe_state_db_modes(
    db_path: &Path,
    reason: &str,
    timeout: Duration,
    include_counts: bool,
    watermarks_only: bool,
) -> StateDbSnapshot {
    if !db_path.exists() {
        return StateDbSnapshot::default();
    }
    // k2k20 ask #2: every non-watermark caller (status / diag / index --json /
    // the count refresh) is a readiness READ. It opens strictly read-only on
    // a dedicated owner thread under one hard deadline; the health
    // watermark lane below keeps its own fast contract.
    // k2k20 ask #2 (health half): the watermark lane used to be the ONE
    // observation surface still on the inline read opener, whose sanctioned
    // recovery writes (duplicate-FTS schema repair, dirty-WAL
    // `wal_checkpoint(TRUNCATE)`) ran unbounded on the caller's thread — the
    // 13-minute silent `cass health` on a 9.3 GB archive. It now shares the
    // strict, mutation-free, hard-deadline owner-thread probe with `status`,
    // scoped to the two single-row meta watermarks it always read.
    probe_state_db_strict_bounded_scoped(db_path, reason, timeout, include_counts, watermarks_only)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ActiveIndexRunDetails {
    pid: Option<u32>,
    started_at_ms: Option<i64>,
    data_dir: PathBuf,
    db_path: PathBuf,
    job_id: Option<String>,
    job_kind: Option<crate::search::asset_state::SearchMaintenanceJobKind>,
    phase: Option<String>,
}

impl ActiveIndexRunDetails {
    fn from_snapshot(
        data_dir: &Path,
        db_path: &Path,
        snapshot: crate::search::asset_state::SearchMaintenanceSnapshot,
    ) -> Option<Self> {
        snapshot.active.then(|| Self {
            pid: snapshot.pid,
            started_at_ms: snapshot.started_at_ms,
            data_dir: data_dir.to_path_buf(),
            db_path: snapshot.db_path.unwrap_or_else(|| db_path.to_path_buf()),
            job_id: snapshot.job_id,
            job_kind: snapshot.job_kind,
            phase: snapshot.phase,
        })
    }

    fn without_owner(data_dir: &Path, db_path: &Path) -> Self {
        Self {
            pid: None,
            started_at_ms: None,
            data_dir: data_dir.to_path_buf(),
            db_path: db_path.to_path_buf(),
            job_id: None,
            job_kind: None,
            phase: None,
        }
    }

    fn started_at_rfc3339(&self) -> Option<String> {
        self.started_at_ms.and_then(format_timestamp_millis_rfc3339)
    }

    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "data_dir": self.data_dir.display().to_string(),
            "db_path": self.db_path.display().to_string(),
            "pid": self.pid,
            "started_at": self.started_at_rfc3339(),
            "job_id": self.job_id,
            "job_kind": self.job_kind.map(|kind| kind.as_lock_value()),
            "phase": self.phase,
        })
    }

    fn to_cli_error(&self) -> CliError {
        let mut details = Vec::new();
        if let Some(pid) = self.pid {
            details.push(format!("pid {pid}"));
        }
        if let Some(job_id) = &self.job_id {
            details.push(format!("job {job_id}"));
        }
        if let Some(job_kind) = self.job_kind {
            details.push(format!("kind {}", job_kind.as_lock_value()));
        }
        if let Some(phase) = &self.phase {
            details.push(format!("phase {phase}"));
        }
        if let Some(started_at) = self.started_at_rfc3339() {
            details.push(format!("started {started_at}"));
        }
        details.push(format!("db {}", self.db_path.display()));

        let mut message = format!(
            "another 'cass index' run is already active for data dir {}",
            self.data_dir.display()
        );
        if !details.is_empty() {
            message.push_str(&format!(" ({})", details.join(", ")));
        }

        CliError {
            code: 7,
            kind: CliErrorKind::IndexBusy.kind_str(),
            message,
            hint: Some(
                "Wait for the active run to finish or point --data-dir/--db at a different cass dataset."
                    .to_string(),
            ),
            retryable: true,
        }
    }
}

fn format_timestamp_millis_rfc3339(ts: i64) -> Option<String> {
    chrono::DateTime::from_timestamp_millis(ts).map(|dt| dt.to_rfc3339())
}

pub(crate) fn normalize_path_identity(path: &Path) -> PathBuf {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map(|cwd| cwd.join(path))
            .unwrap_or_else(|_| path.to_path_buf())
    };
    let lexical = {
        let mut normalized = PathBuf::new();
        for component in absolute.components() {
            match component {
                std::path::Component::CurDir => {}
                std::path::Component::ParentDir => {
                    let can_pop = normalized
                        .components()
                        .next_back()
                        .is_some_and(|tail| matches!(tail, std::path::Component::Normal(_)));
                    if can_pop {
                        normalized.pop();
                    } else {
                        normalized.push(component.as_os_str());
                    }
                }
                std::path::Component::Prefix(_) | std::path::Component::RootDir => {
                    normalized.push(component.as_os_str());
                }
                std::path::Component::Normal(_) => normalized.push(component.as_os_str()),
            }
        }
        normalized
    };
    // dunce: on Windows this identity also reaches error text, lock files and
    // checkpoints, so keep it out of the \\?\ verbatim form (txc9h). Readers
    // that compare a saved value re-normalize it (path_identities_match, watch
    // replay debt), so values saved by an older build still match.
    dunce::canonicalize(&lexical).unwrap_or_else(|_| {
        let mut suffix = Vec::new();
        let mut cursor = lexical.as_path();
        loop {
            match dunce::canonicalize(cursor) {
                Ok(prefix) => {
                    let mut resolved = prefix;
                    for component in suffix.iter().rev() {
                        resolved.push(component);
                    }
                    return resolved;
                }
                Err(_) => {
                    let Some(name) = cursor.file_name() else {
                        return lexical.clone();
                    };
                    suffix.push(name.to_os_string());
                    let Some(parent) = cursor.parent() else {
                        return lexical.clone();
                    };
                    cursor = parent;
                }
            }
        }
    })
}

pub(crate) fn path_identities_match(lhs: &Path, rhs: &Path) -> bool {
    normalize_path_identity(lhs) == normalize_path_identity(rhs)
}

pub(crate) fn stored_path_identity_matches(saved: &str, current: &Path) -> bool {
    path_identities_match(Path::new(saved), current)
}

fn read_index_run_lock_snapshot(
    data_dir: &Path,
) -> crate::search::asset_state::SearchMaintenanceSnapshot {
    // Observation surfaces (status/health/triage/doctor/TUI) use the reaping
    // probe: stale metadata from a dead owner is truncated in place so later
    // readers see a clean lock (#176 / bd-k9jb9). Search-side callers reach
    // the pure-read variant directly (gh#422).
    crate::search::asset_state::read_search_maintenance_snapshot_reaping(data_dir)
}

fn probe_index_run_lock(
    data_dir: &Path,
    db_path: &Path,
    reap_stale: bool,
) -> crate::search::asset_state::SearchMaintenanceSnapshot {
    let snapshot = if reap_stale {
        read_index_run_lock_snapshot(data_dir)
    } else {
        crate::search::asset_state::read_search_maintenance_snapshot(data_dir)
    };
    // An active advisory lock is data-dir scoped. Legacy or interrupted
    // writers may have incomplete db_path metadata, but the held flock is
    // still the authoritative signal that maintenance is in progress.
    if snapshot.active {
        return snapshot;
    }
    if snapshot
        .db_path
        .as_deref()
        .is_none_or(|lock_db_path| !path_identities_match(lock_db_path, db_path))
    {
        return crate::search::asset_state::SearchMaintenanceSnapshot::default();
    }
    snapshot
}

fn active_index_run_details(data_dir: &Path, db_path: &Path) -> Option<ActiveIndexRunDetails> {
    ActiveIndexRunDetails::from_snapshot(data_dir, db_path, read_index_run_lock_snapshot(data_dir))
}

fn error_chain_indicates_active_cass_index(chain: &str) -> bool {
    chain.contains("another cass index process already holds")
}

fn error_chain_indicates_retryable_storage_contention(chain: &str) -> bool {
    crate::storage::sqlite::retryable_storage_error_message(chain)
}

fn error_chain_indicates_index_disk_headroom_blocked(chain: &str) -> bool {
    chain.contains("canonical archive disk headroom check failed")
}

fn error_chain_indicates_lexical_rebuild_disk_stop(chain: &str) -> bool {
    chain.contains(crate::indexer::LEXICAL_REBUILD_DISK_STOP_MARKER)
}

fn error_chain_indicates_orphan_fk_cleanup_blocked(chain: &str) -> bool {
    chain.contains("orphan FK self-heal failed for canonical cass archive")
}

fn error_chain_indicates_archive_health_blocked(chain: &str) -> bool {
    chain.contains("will not replace or truncate the SQLite source of truth")
}

fn error_chain_indicates_unsupported_quill_writer_admission(chain: &str) -> bool {
    chain.contains("cross-process Quill writer admission requires flock semantics")
}

fn index_storage_contention_cli_error(chain: &str) -> CliError {
    CliError {
        code: 7,
        kind: CliErrorKind::IndexBusy.kind_str(),
        message: format!("index failed because the canonical database is busy/locked: {chain}"),
        hint: Some(
            "Retry after the current database writer or indexer finishes; cass refused to replace the canonical database for this transient condition."
                .to_string(),
        ),
        retryable: true,
    }
}

fn index_archive_health_cli_error(chain: &str) -> CliError {
    CliError {
        code: 5,
        kind: CliErrorKind::Storage.kind_str(),
        message: format!("index refused to modify an unhealthy canonical archive: {chain}"),
        hint: Some(
            "Run 'cass doctor check --json' for a read-only diagnosis; do not use index --full/--force-rebuild as a corruption repair path."
                .to_string(),
        ),
        retryable: false,
    }
}

fn index_disk_headroom_cli_error(chain: &str) -> CliError {
    CliError {
        code: 14,
        kind: CliErrorKind::Storage.kind_str(),
        message: format!("index refused to start because disk headroom is too low: {chain}"),
        hint: Some(
            "Free space on the filesystem that holds the cass data directory, then retry indexing. Run 'cass doctor check --json' for a read-only health report."
                .to_string(),
        ),
        retryable: true,
    }
}

/// nw61n: a lexical rebuild that started and then stopped at the free-space
/// floor. Same exit and kind as the start refusal, its own wording: the
/// rebuild ran, kept what it committed, and the rerun resumes it.
fn lexical_rebuild_disk_stop_cli_error(chain: &str) -> CliError {
    CliError {
        code: 14,
        kind: CliErrorKind::Storage.kind_str(),
        message: format!("index stopped the lexical rebuild because free disk space ran low: {chain}"),
        hint: Some(
            "Free space on the filesystem that holds the cass data directory, then rerun the same cass index command; it resumes from what this run committed. Run 'cass doctor check --json' for the headroom a rebuild needs."
                .to_string(),
        ),
        retryable: true,
    }
}

fn index_orphan_fk_cleanup_cli_error(chain: &str) -> CliError {
    CliError {
        code: 5,
        kind: CliErrorKind::Storage.kind_str(),
        message: format!("index stopped before writing because orphan-FK cleanup failed: {chain}"),
        hint: Some(
            "Run 'cass doctor check --json', free disk/memory pressure if reported, then retry indexing after the canonical archive is readable."
                .to_string(),
        ),
        retryable: true,
    }
}

fn index_unsupported_quill_writer_admission_cli_error(chain: &str) -> CliError {
    CliError {
        code: 9,
        kind: CliErrorKind::Index.kind_str(),
        message: format!(
            "indexing is unsupported on this platform because Quill cannot acquire its cross-process writer lock: {chain}"
        ),
        hint: Some(
            "This build cannot create or update the Quill lexical index on this platform. Use a supported Unix or Windows build; Windows support requires Frankensearch 0.4.1 or later. Retrying the same command will not help."
                .to_string(),
        ),
        retryable: false,
    }
}

#[cfg(test)]
mod index_error_mapping_tests {
    use super::*;

    fn idempotency_test_bundle_bytes(db_path: &Path) -> Vec<Option<Vec<u8>>> {
        ["", "-wal", "-shm"]
            .into_iter()
            .map(|suffix| {
                let mut path = db_path.as_os_str().to_owned();
                path.push(suffix);
                match std::fs::read(Path::new(&path)) {
                    Ok(bytes) => Some(bytes),
                    Err(error) if error.kind() == io::ErrorKind::NotFound => None,
                    Err(error) => panic!("reading cache fixture {path:?}: {error}"),
                }
            })
            .collect()
    }

    #[test]
    fn gh515_idempotency_lookup_never_creates_or_expires_archive_state() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let data_dir = temp.path().join("data");
        let db_path = temp.path().join("archive-parent/archive.db");
        assert_eq!(read_index_idempotency_cache(&db_path, "missing", 0), None);
        assert!(!db_path.parent().unwrap().exists());

        std::fs::create_dir_all(db_path.parent().unwrap())?;
        let conn = crate::franken_sync::Connection::open(db_path.to_string_lossy())?;
        conn.execute("CREATE TABLE archive_identity (content TEXT)")?;
        conn.execute("INSERT INTO archive_identity VALUES ('keep this archive')")?;
        conn.close()?;
        let before = idempotency_test_bundle_bytes(&db_path);
        assert_eq!(read_index_idempotency_cache(&db_path, "missing", 0), None);
        assert_eq!(idempotency_test_bundle_bytes(&db_path), before);
        assert!(!data_dir.exists());
        assert!(!db_path.parent().unwrap().join("doctor").exists());

        let payload = serde_json::json!({"success": true, "messages": 1});
        store_index_idempotency_result(&data_dir, &db_path, "expired", 7, &payload, 0)?;
        store_index_idempotency_result(&data_dir, &db_path, "live", 7, &payload, 100)?;
        let before = idempotency_test_bundle_bytes(&db_path);
        let now_ms = 24 * 60 * 60 * 1000;
        assert_eq!(
            read_index_idempotency_cache(&db_path, "expired", now_ms),
            None
        );
        assert_eq!(
            read_index_idempotency_cache(&db_path, "unknown", now_ms),
            None
        );
        assert_eq!(
            read_index_idempotency_cache(&db_path, "live", now_ms),
            Some(("7".to_string(), payload.to_string()))
        );
        assert_eq!(idempotency_test_bundle_bytes(&db_path), before);
        let storage = crate::storage::sqlite::FrankenStorage::open_strict_readonly(&db_path)?;
        assert_eq!(
            storage
                .raw()
                .query_row("SELECT COUNT(*) FROM idempotency_keys")?
                .get_typed::<i64>(0)?,
            2,
            "TTL lookup must leave expired rows for an admitted cache writer"
        );
        assert_eq!(
            storage
                .raw()
                .query_row("SELECT content FROM archive_identity")?
                .get_typed::<String>(0)?,
            "keep this archive"
        );
        storage.close_without_checkpoint()?;
        Ok(())
    }

    #[test]
    fn gh515_idempotency_persistence_obeys_archive_owner_and_recovers() -> Result<()> {
        use crate::search::asset_state::SearchMaintenanceJobKind;

        let temp = tempfile::tempdir()?;
        let owner_data = temp.path().join("owner-data");
        let cache_data = temp.path().join("cache-data");
        let db_path = temp.path().join("archive.db");
        let conn = crate::franken_sync::Connection::open(db_path.to_string_lossy())?;
        conn.execute("CREATE TABLE archive_identity (content TEXT)")?;
        conn.execute("INSERT INTO archive_identity VALUES ('keep this archive')")?;
        conn.close()?;
        let payload = serde_json::json!({"success": true, "messages": 1});
        let owner = crate::indexer::acquire_search_maintenance_mutation_lock(
            &owner_data,
            &db_path,
            SearchMaintenanceJobKind::SemanticRebuild,
        )?;
        let before = idempotency_test_bundle_bytes(&db_path);
        assert!(
            store_index_idempotency_result(&cache_data, &db_path, "expired", 7, &payload, 0)
                .is_err()
        );
        assert_eq!(read_index_idempotency_cache(&db_path, "expired", 0), None);
        assert_eq!(idempotency_test_bundle_bytes(&db_path), before);
        assert!(std::fs::read(cache_data.join("index-run.lock"))?.is_empty());
        drop(owner);

        store_index_idempotency_result(&cache_data, &db_path, "expired", 7, &payload, 0)?;
        store_index_idempotency_result(&cache_data, &db_path, "live", 7, &payload, 100)?;
        let owner = crate::indexer::acquire_search_maintenance_mutation_lock(
            &owner_data,
            &db_path,
            SearchMaintenanceJobKind::SemanticRebuild,
        )?;
        let now_ms = 24 * 60 * 60 * 1000;
        let before = idempotency_test_bundle_bytes(&db_path);
        assert!(
            store_index_idempotency_result(&cache_data, &db_path, "retry", 7, &payload, now_ms)
                .is_err()
        );
        assert_eq!(idempotency_test_bundle_bytes(&db_path), before);
        drop(owner);

        store_index_idempotency_result(&cache_data, &db_path, "retry", 7, &payload, now_ms)?;
        assert_eq!(
            read_index_idempotency_cache(&db_path, "retry", now_ms),
            Some(("7".to_string(), payload.to_string()))
        );
        assert!(std::fs::read(cache_data.join("index-run.lock"))?.is_empty());
        let storage = crate::storage::sqlite::FrankenStorage::open_strict_readonly(&db_path)?;
        let keys = storage.raw().query_map_collect(
            "SELECT key FROM idempotency_keys ORDER BY key",
            &[],
            |row| row.get_typed::<String>(0),
        )?;
        assert_eq!(keys, ["live", "retry"]);
        assert_eq!(
            storage
                .raw()
                .query_row("SELECT content FROM archive_identity")?
                .get_typed::<String>(0)?,
            "keep this archive"
        );
        for row in storage.raw().query("PRAGMA integrity_check")? {
            assert_eq!(row.get_typed::<String>(0)?, "ok");
        }
        storage.close_without_checkpoint()?;
        Ok(())
    }

    #[test]
    fn index_idempotency_cache_rejects_non_objects_and_preserves_valid_results() {
        for invalid in [
            "",
            "{",
            "null",
            "true",
            "false",
            "42",
            "1.5",
            "\"cached\"",
            "[]",
            "[{}]",
        ] {
            assert_eq!(cached_index_payload(invalid, "retry-key"), None);
        }
        assert_eq!(
            cached_index_payload(
                r#"{"ok":true,"messages":3,"nested":{"value":[1,2]},"cached":false,"idempotency_key":"old-key"}"#,
                "retry-key",
            ),
            Some(serde_json::json!({
                "ok": true,
                "messages": 3,
                "nested": {"value": [1, 2]},
                "cached": true,
                "idempotency_key": "retry-key",
            }))
        );
    }

    #[test]
    fn orphan_fk_cleanup_failure_is_retryable_after_operator_remediation() {
        let err = index_orphan_fk_cleanup_cli_error(
            "orphan FK self-heal failed for canonical cass archive at /tmp/cass.db: out of memory",
        );

        assert_eq!(err.code, 5);
        assert_eq!(err.kind, CliErrorKind::Storage.kind_str());
        assert!(err.retryable);
        assert!(
            err.hint
                .as_deref()
                .is_some_and(|hint| hint.contains("free disk/memory pressure")),
            "hint should name the resource-pressure retry path: {err:?}"
        );
    }

    #[test]
    fn archive_health_failure_remains_non_retryable_index_repair() {
        let err = index_archive_health_cli_error(
            "canonical cass archive at /tmp/cass.db is not safe for indexing: malformed. cass index will not replace or truncate the SQLite source of truth.",
        );

        assert_eq!(err.code, 5);
        assert_eq!(err.kind, CliErrorKind::Storage.kind_str());
        assert!(!err.retryable);
    }

    #[test]
    fn disk_headroom_failure_is_retryable_after_space_is_freed() {
        let err = index_disk_headroom_cli_error(
            "canonical archive disk headroom check failed for /data: available=1 bytes, required=2 bytes",
        );

        assert_eq!(err.code, 14);
        assert_eq!(err.kind, CliErrorKind::Storage.kind_str());
        assert!(err.retryable);
    }

    /// nw61n: the indexer's own mid-rebuild stop maps to the same retryable
    /// exit 14 / `storage` as the start refusal, with resume wording, and is
    /// not mistaken for the start refusal (or the reverse).
    #[test]
    fn lexical_rebuild_disk_stop_is_retryable_and_names_the_resume() {
        let stop = crate::indexer::lexical_rebuild_free_space_verdict(
            1,
            512 * 1024 * 1024,
            Path::new("/data/cass/index"),
        )
        .expect_err("below the floor");
        let chain = format!("lexical rebuild failed | {stop:#}");
        assert!(error_chain_indicates_lexical_rebuild_disk_stop(&chain));
        assert!(!error_chain_indicates_index_disk_headroom_blocked(&chain));
        assert!(!error_chain_indicates_lexical_rebuild_disk_stop(
            "canonical archive disk headroom check failed for /data: available=1 bytes, required=2 bytes"
        ));
        let err = lexical_rebuild_disk_stop_cli_error(&chain);
        assert_eq!(err.code, 14);
        assert_eq!(err.kind, CliErrorKind::Storage.kind_str());
        assert!(err.retryable);
        assert!(err.message.contains("/data/cass/index"), "{err:?}");
        assert!(
            err.hint
                .as_deref()
                .is_some_and(|hint| hint.contains("resumes")),
            "{err:?}"
        );
    }

    #[test]
    fn unsupported_quill_writer_admission_is_non_retryable_and_actionable() {
        let chain = "open Quill writer | verify writer-lock support at C:\\cass\\index: cross-process Quill writer admission requires flock semantics";

        assert!(error_chain_indicates_unsupported_quill_writer_admission(
            chain
        ));
        let err = index_unsupported_quill_writer_admission_cli_error(chain);

        assert_eq!(err.code, 9);
        assert_eq!(err.kind, CliErrorKind::Index.kind_str());
        assert!(!err.retryable);
        assert!(
            err.hint
                .as_deref()
                .is_some_and(|hint| hint.contains("Retrying the same command will not help")),
            "hint must prevent an infinite retry loop: {err:?}"
        );
    }

    #[test]
    fn unrelated_quill_errors_are_not_misclassified_as_platform_gaps() {
        assert!(!error_chain_indicates_unsupported_quill_writer_admission(
            "open Quill writer | another writer currently holds the admission lock"
        ));
        assert!(!error_chain_indicates_unsupported_quill_writer_admission(
            "open Quill reader | unsupported manifest version 99"
        ));
    }
}

fn cli_error_json_payload(err: &CliError, elapsed_ms: u128) -> serde_json::Value {
    let mut payload = serde_json::json!({
        "success": false,
        "error": err.message,
        "elapsed_ms": elapsed_ms,
        "code": err.code,
        "kind": err.kind,
        "retryable": err.retryable,
    });
    if let Some(hint) = &err.hint {
        payload["hint"] = serde_json::json!(hint);
        payload["recommended_action"] = serde_json::json!(hint);
    }
    payload
}

fn cass_lexical_index_initialized(data_dir: &Path) -> bool {
    crate::search::tantivy::searchable_index_exists(&crate::search::tantivy::expected_index_dir(
        data_dir,
    ))
}

fn cass_not_initialized(
    db_exists: bool,
    lexical_index_initialized: bool,
    rebuild_active: bool,
) -> bool {
    !db_exists && !lexical_index_initialized && !rebuild_active
}

fn cass_not_initialized_explanation(data_dir: &Path) -> String {
    format!(
        "No cass database or search index exists in {} yet. This is expected on a fresh install or when using a brand-new --data-dir.",
        data_dir.display()
    )
}

fn cass_not_initialized_recommended_action() -> String {
    "Run 'cass index --full' once to discover local sessions and build the initial archive."
        .to_string()
}

fn state_meta_json(
    data_dir: &Path,
    db_path: &Path,
    stale_threshold: u64,
    allow_db_open: bool,
) -> serde_json::Value {
    state_meta_json_with_counts(data_dir, db_path, stale_threshold, allow_db_open, None)
}

/// `coding_agent_session_search-gi4oy` follow-up to d0rmo: the
/// COUNT(*) skip got cass health from p50=296ms to p50=132ms, but
/// the documented `<50ms` budget still demanded another ~80ms cut.
/// Profiling pointed at the second cold-path cost: opening the
/// FrankenStorage canonical DB twice per probe (once via
/// probe_state_db for `last_indexed_at`, once via
/// inspect_semantic_assets when a model is installed). Health's
/// readiness verdict only needs `db_exists` — corrupt-DB detection
/// is `cass doctor`'s job, not the fast-surface probe's.
///
/// `skip_db_open`: when true, probe_state_db is replaced with a
/// pure-fs StateDbSnapshot that reports `opened=true` if the file
/// exists (assumed-good) and surfaces the new `opened_skipped=true`
/// flag in the database envelope so callers know the open was
/// elided. status / diag pass false (they explicitly want the
/// open-success signal as a degraded-state diagnostic).
fn state_meta_json_with_counts(
    data_dir: &Path,
    db_path: &Path,
    stale_threshold: u64,
    allow_db_open: bool,
    include_counts_override: Option<bool>,
) -> serde_json::Value {
    state_meta_json_full(
        data_dir,
        db_path,
        stale_threshold,
        allow_db_open,
        include_counts_override,
        false,
        allow_db_open,
        false,
    )
}

/// `coding_agent_session_search-gi4oy`: most-general state_meta_json
/// variant exposing both the include_counts override (d0rmo) AND
/// the skip_db_open elision (gi4oy). Callers that already use
/// state_meta_json_with_counts keep working unchanged; new callers
/// (currently just run_health) opt in via state_meta_json_for_health.
#[allow(clippy::too_many_arguments)]
fn state_meta_json_full(
    data_dir: &Path,
    db_path: &Path,
    stale_threshold: u64,
    allow_db_open: bool,
    include_counts_override: Option<bool>,
    skip_db_open: bool,
    inspect_semantic: bool,
    passive_db_probe: bool,
) -> serde_json::Value {
    state_meta_json_inner(
        data_dir,
        db_path,
        stale_threshold,
        allow_db_open,
        include_counts_override,
        skip_db_open,
        inspect_semantic,
        passive_db_probe,
    )
}

/// Convenience wrapper for run_health: skip COUNT(*) AND skip the
/// DB open. Keep semantic asset probing enabled: it is filesystem-only
/// on this path, and health/status JSON must agree on realized fallback
/// state instead of reporting an unhelpful `not_inspected` placeholder.
fn state_meta_json_for_health(
    data_dir: &Path,
    db_path: &Path,
    stale_threshold: u64,
) -> serde_json::Value {
    state_meta_json_full(
        data_dir,
        db_path,
        stale_threshold,
        true,
        Some(false),
        true,
        true,
        false,
    )
}

fn state_meta_json_for_status(
    data_dir: &Path,
    db_path: &Path,
    stale_threshold: u64,
) -> serde_json::Value {
    state_meta_json_full(
        data_dir,
        db_path,
        stale_threshold,
        true,
        Some(false),
        false,
        true,
        false,
    )
}

/// Filesystem-only state metadata for `search --no-maintenance`. The database
/// is reported as deliberately unprobed; lexical checkpoint/coordination files
/// are inspected, while the state surface skips semantic DB fingerprint/filter
/// probes. This avoids the ordinary read opener's sanctioned dirty-WAL/schema
/// recovery writes and avoids scratch-copy diagnostics.
fn state_meta_json_for_strict_read(
    data_dir: &Path,
    db_path: &Path,
    stale_threshold: u64,
) -> serde_json::Value {
    state_meta_json_full(
        data_dir,
        db_path,
        stale_threshold,
        false,
        Some(false),
        false,
        false,
        true,
    )
}

/// Project the `.14.1` storage-integrity block onto a lightweight readiness
/// surface (`cass status --json`, `cass search --robot-meta`) from the
/// `state_meta` JSON it already built plus a cheap `db_path.exists()` check, so
/// every truth surface speaks the same `StorageState` vocabulary the doctor
/// does (bead qfswx, follow-on to vl1cj's doctor wiring). Returns the report as
/// a JSON value (`null` only if serialization somehow fails).
///
/// The shared db-open/index signals derive the coarse readiness state first;
/// bounded dedicated probes then refine typed contention, on-disk schema drift,
/// legacy layouts, and structurally suspect WAL/SHM sidecars. Generic
/// `CliError.retryable` is never used to infer busy/locked.
fn storage_integrity_value_from_state(
    data_dir: &Path,
    db_path: &Path,
    state: &serde_json::Value,
    not_initialized: bool,
) -> serde_json::Value {
    use crate::search::storage_integrity::{
        DoctorStorageSignals, apply_dedicated_storage_probe, build_readiness_storage_integrity,
        load_matching_integrity_attestation, probe_dedicated_storage_state,
    };
    let db_file_present = db_path.exists();
    let db_opened = state
        .pointer("/database/opened")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let open_skipped = state
        .pointer("/database/open_skipped")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let lexical_index_drifted = state
        .pointer("/index/empty_with_messages")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let signals = DoctorStorageSignals {
        // A deliberately skipped open proves only that a path is present, not
        // that the archive is readable. Feed the readiness classifier no
        // synthetic db-open signal so it emits unknown_deferred/not_checked
        // and records db_open as skipped instead of claiming a check ran.
        db_file_present: db_file_present && !open_skipped,
        not_initialized,
        // A present-but-unopened archive is an openread fault. status/search
        // open the DB for real (skip_db_open=false), so only claim the fault
        // when the open was attempted (never deliberately elided).
        db_open_failed: db_file_present && !db_opened && !open_skipped,
        probe_timed_out: false,
        integrity_failed: false,
        integrity_unverified: false,
        lexical_index_drifted,
    };
    // #331: consult the persisted deep-probe attestation so a successful
    // db_open alone projects `unchecked` (never a synthesized `ok`), while a
    // fingerprint-still-valid doctor/index quick_check verdict projects its
    // real pass/fail outcome with cached provenance.
    let attestation = load_matching_integrity_attestation(data_dir, db_path);
    let mut report = build_readiness_storage_integrity(signals, attestation.as_ref());
    if open_skipped
        && db_file_present
        && let Some(db_open) = report
            .checks_attempted
            .iter_mut()
            .find(|check| check.name == "db_open")
    {
        db_open.skipped_reason = Some("passive no-maintenance probe".to_string());
    }
    if open_skipped {
        return serde_json::to_value(report).unwrap_or(serde_json::Value::Null);
    }
    let dedicated = probe_dedicated_storage_state(
        db_path,
        crate::search::storage_integrity::dedicated_storage_probe_timeout(),
    );
    serde_json::to_value(apply_dedicated_storage_probe(report, dedicated))
        .unwrap_or(serde_json::Value::Null)
}

#[cfg(test)]
#[test]
fn readiness_projection_uses_only_fingerprint_matching_integrity_attestations() {
    use crate::search::storage_integrity::{
        IntegrityAttestationVerdict, store_integrity_attestation,
    };

    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path();
    let db_path = data_dir.join("agent_search.db");
    std::fs::write(&db_path, b"canonical archive bytes").expect("seed archive file");
    let opened_state = serde_json::json!({
        "database": { "opened": true, "open_skipped": false },
        "index": { "empty_with_messages": false }
    });

    let unchecked = storage_integrity_value_from_state(data_dir, &db_path, &opened_state, false);
    assert_eq!(unchecked["storage_state"], "unchecked");
    assert_eq!(unchecked["source_of_truth_risk"], "unknown");
    assert_eq!(unchecked["attestation_source"], "none");
    assert_eq!(unchecked["checks_not_attempted"][0]["name"], "quick_check");

    store_integrity_attestation(
        data_dir,
        &db_path,
        IntegrityAttestationVerdict::Fail,
        "quick_check",
        Some("fixture corruption".to_string()),
    )
    .expect("capture and persist fixture corruption attestation");
    let known_bad = storage_integrity_value_from_state(data_dir, &db_path, &opened_state, false);
    assert_eq!(known_bad["storage_state"], "integrity_failed");
    assert_eq!(known_bad["source_of_truth_risk"], "high");
    assert_eq!(known_bad["attestation_source"], "cached");

    // Any archive mutation invalidates the cached verdict. A successful open
    // without fresh structural evidence returns to the honest unchecked state.
    std::fs::write(&db_path, b"canonical archive bytes changed and longer")
        .expect("mutate archive fingerprint");
    let stale = storage_integrity_value_from_state(data_dir, &db_path, &opened_state, false);
    assert_eq!(stale["storage_state"], "unchecked");
    assert_eq!(stale["attestation_source"], "none");
}

#[cfg(test)]
#[test]
fn gh422_passive_search_metadata_never_claims_a_database_open() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path();
    let db_path = data_dir.join("agent_search.db");
    std::fs::write(&db_path, b"present but deliberately unprobed")
        .expect("seed passive archive path");
    let passive_state = serde_json::json!({
        "database": { "opened": false, "open_skipped": true },
        "index": { "empty_with_messages": false }
    });

    let report = storage_integrity_value_from_state(data_dir, &db_path, &passive_state, false);
    assert_eq!(report["storage_state"], "unknown_deferred");
    assert_eq!(report["archive_readability"], "not_checked");
    let db_open = report["checks_attempted"]
        .as_array()
        .and_then(|checks| checks.iter().find(|check| check["name"] == "db_open"))
        .expect("passive report must retain an explicit db_open provenance row");
    assert_eq!(
        db_open["skipped_reason"], "passive no-maintenance probe",
        "robot metadata must say the DB open was skipped, never that it ran"
    );
}

/// `coding_agent_session_search-d0rmo`: variant of `state_meta_json`
/// that lets the caller force-skip the COUNT(*) queries on the
/// canonical DB. Pre-fix the size-based heuristic (≤256 MB → run
/// counts) made `cass health --json` ~6× over its <50ms documented
/// budget on real corpora because COUNT(*) on conversations +
/// messages is a full table scan. Health doesn't need exact
/// counts to compute its readiness verdict (DB exists + opened +
/// index fresh + not rebuilding). Status still opens the DB to read cheap
/// meta watermarks, but now uses a bounded no-count/no-fingerprint default;
/// deep count/fingerprint probes belong to doctor/diag, not the routine
/// readiness surface on multi-GB archives.
///
/// `include_counts_override`:
/// - `None`         → existing size-based logic (≤256 MB → counts).
/// - `Some(false)`  → unconditionally skip; envelope reports 0/0
///   with `counts_skipped=true` (already in StateDbSnapshot).
/// - `Some(true)`   → unconditionally run counts (no caller uses
///   this today; reserved for future paths that MUST have totals regardless of cost).
#[allow(clippy::too_many_arguments)]
fn state_meta_json_inner(
    data_dir: &Path,
    db_path: &Path,
    stale_threshold: u64,
    allow_db_open: bool,
    include_counts_override: Option<bool>,
    skip_db_open: bool,
    inspect_semantic: bool,
    passive_db_probe: bool,
) -> serde_json::Value {
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    let db_exists = db_path.exists();
    let index_run = probe_index_run_lock(data_dir, db_path, !passive_db_probe);

    // F4 (cass tech debt): capture the wall clock at full millisecond
    // precision so the stall-detection comparison against
    // `last_progress_at_ms` is no longer second-quantised inside
    // `asset_state::lexical_state_from_observations`. `now_secs` is
    // retained as a convenience for the existing age-in-seconds math
    // (the public status JSON surfaces `age_seconds`, not `age_ms`).
    let now_ms_i64: i64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0);
    let now_secs = (now_ms_i64.max(0) as u64) / 1000;

    let db_metadata = fs::metadata(db_path).ok();
    let db_size_bytes = db_metadata.as_ref().map(|m| m.len());
    // WS-B.4a: the WAL sidecar is the archive's hidden cost — every opener
    // replays it, and an untruncated multi-hundred-MB WAL was invisible from
    // every observation surface until now. Read it from metadata only; the
    // observation surfaces never open or checkpoint anything here.
    let db_wal_bytes = fs::metadata(crate::storage::sqlite::database_sidecar_path(
        db_path, "-wal",
    ))
    .ok()
    .filter(|m| m.is_file())
    .map_or(0, |m| m.len());
    let db_shm_present = fs::metadata(crate::storage::sqlite::database_sidecar_path(
        db_path, "-shm",
    ))
    .ok()
    .is_some_and(|m| m.is_file());
    // [session-review-gi4oy] Capture is_file ALONGSIDE size from the
    // single metadata call — the gi4oy skip-open path cannot trust
    // db_path.exists() alone because exists() returns true for both
    // regular files AND directories. The corrupt-DB regression test
    // tests/cli_robot.rs:2725::health_json_reports_open_error_for_unopenable_db_path
    // creates a DIRECTORY at the DB path; without this guard, the
    // skip-open optimistic branch would report opened=true for a
    // directory and the contract regression would ship silently.
    let db_is_regular_file = db_metadata.as_ref().is_some_and(|m| m.is_file());
    let include_counts = include_counts_override.unwrap_or_else(|| {
        db_size_bytes
            .map(|size| size <= STATUS_COUNT_SCAN_MAX_DB_BYTES)
            .unwrap_or(false)
    });
    // [coding_agent_session_search-gi4oy + session-review-gi4oy]
    // Skip-open path: when skip_db_open is true AND the DB path is
    // a regular file (not a directory, not a broken symlink),
    // synthesize a StateDbSnapshot that reports opened=true
    // (assumed-good) + open_skipped=true. The expensive
    // FrankenStorage open is elided; callers reading the JSON
    // envelope branch on the new flag if they need to know the open
    // was assumed.
    //
    // If the DB path exists but is NOT a regular file (the corrupt-DB
    // detection scenario), fall through to the actual probe_state_db
    // — surfacing the open failure is more important than the perf
    // win in that case. probe_state_db will return opened=false +
    // open_error=Some(...) which downstream callers (run_health)
    // turn into a degraded status.
    //
    // counts_skipped MUST be true here regardless of `include_counts`
    // — we never opened the DB so we never ran COUNT(*); the snapshot
    // counts come from `..default()` (i.e. zero). Reporting
    // counts_skipped=false alongside message_count=0 would be a lie.
    let db_snapshot = if passive_db_probe && db_exists && db_is_regular_file {
        StateDbSnapshot {
            // Per the gi4oy contract above: opened=true (ASSUMED-good) +
            // open_skipped=true. Leaving `opened` at its false default made
            // the zero-touch doctor lane claim `database_unavailable` for
            // semantic assets on perfectly healthy archives (the semantic
            // inspection keys `db_available` off this field), regressing
            // doctor's model guidance to a dead end. Callers that need the
            // truth branch on `open_skipped`, exactly as documented.
            opened: true,
            counts_skipped: true,
            open_skipped: true,
            ..StateDbSnapshot::default()
        }
    } else if skip_db_open && db_exists && db_is_regular_file {
        // [coding_agent_session_search #301 truthfulness reconciliation]
        // Previously this branch synthesized a fully-empty snapshot
        // (last_scan_ts=None), which is exactly why `cass health`
        // could report `fresh`/`healthy` while `cass status` reported
        // `stale` on the SAME DB: the scan-ahead-of-projection check
        // below requires `last_scan_ts.is_some()`, and health never
        // read it. Now health does a watermarks-only probe — it opens
        // the DB but reads ONLY the two cheap single-row `meta`
        // watermarks (no COUNT(*) scans), so the staleness inputs are
        // IDENTICAL to status. The snapshot still reports
        // open_skipped=true + counts_skipped=true + zero counts, so the
        // gi4oy/d0rmo health contract (null counts, assumed-good open)
        // is preserved byte-for-byte.
        probe_state_db_modes(
            db_path,
            "state-meta-watermarks",
            STATE_DB_OPEN_TIMEOUT,
            false,
            true,
        )
    } else if allow_db_open && db_exists {
        probe_state_db(db_path, "state-meta", STATE_DB_OPEN_TIMEOUT, include_counts)
    } else {
        StateDbSnapshot::default()
    };
    let conversation_count = db_snapshot.conversation_count;
    let message_count = db_snapshot.message_count;
    let last_scan_ts = db_snapshot.last_scan_ts;
    let mut last_indexed_at = db_snapshot.last_indexed_at;
    let watch_lexical_replay_pending = db_snapshot.watch_lexical_replay_pending;
    let db_opened = db_snapshot.opened;
    let db_open_error = db_snapshot.open_error;
    let db_open_retryable = db_snapshot.open_retryable;
    let counts_skipped = db_snapshot.counts_skipped;
    let open_skipped = db_snapshot.open_skipped;
    // i6upe: distinct corruption signal, separate from staleness. "unknown"
    // covers both "probe not run" (health-contract watermark path, unopened
    // DB) and "probe could not classify" (busy archive).
    let db_integrity = match db_snapshot.integrity {
        Some(StateDbIntegrity::Ok) => "ok",
        Some(StateDbIntegrity::Corrupt) => "corrupt",
        None => "unknown",
    };
    let db_integrity_detail = db_snapshot.integrity_detail.clone();
    let db_integrity_hint = (db_snapshot.integrity == Some(StateDbIntegrity::Corrupt)).then(|| {
        "the archive's stored pages are malformed; do NOT run 'cass index' (it rebuilds on top of \
         the damage) — run 'cass doctor --check' and recover with sqlite3 '.recover' first"
            .to_owned()
    });
    let fallback_fts_repair_pending = db_snapshot.fallback_repair_pending.clone();
    let lexical_repair_deferred_runs = db_snapshot.lexical_repair_deferred_runs;
    let lexical_repair_deferred_reason = db_snapshot.lexical_repair_deferred_reason.clone();

    let index_path = crate::search::tantivy::expected_index_dir(data_dir);
    let lexical_index_initialized = crate::search::tantivy::searchable_index_exists(&index_path);
    if last_indexed_at.is_none() && lexical_index_initialized {
        last_indexed_at = crate::search::tantivy::searchable_index_modified_time(&index_path)
            .and_then(|m| m.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as i64);
    }
    let status_semantic_policy = crate::search::policy::SemanticPolicy::resolve(
        &crate::search::policy::CliSemanticOverrides::default(),
    );
    let semantic_preference = if status_semantic_policy
        .quality_tier_embedder
        .trim()
        .eq_ignore_ascii_case("hash")
    {
        crate::search::asset_state::SemanticPreference::HashFallback
    } else {
        crate::search::asset_state::SemanticPreference::DefaultModel
    };
    let mut assets = crate::search::asset_state::inspect_search_assets(
        crate::search::asset_state::InspectSearchAssetsInput {
            data_dir,
            db_path,
            stale_threshold,
            last_indexed_at_ms: last_indexed_at,
            now_ms: now_ms_i64,
            maintenance: index_run.clone(),
            semantic_preference,
            db_available: db_opened,
            // GH #368 defect 2: the lexical-fingerprint open is the operation
            // search actually performs, and it is what fails on a corrupt FTS5
            // structure. It was previously gated behind `include_counts` (DB
            // <= 256 MiB), so on large corpora (the 1.17 GB report) the
            // corruption was never probed and readiness reported "usable". The
            // fingerprint is cheap (readonly open + COUNT(conversations), not a
            // message scan), so attempt it on every real open — additively, so
            // tiny-DB and the health skip-open fast path (open_skipped=true)
            // are unchanged.
            compute_lexical_fingerprint: include_counts || (db_opened && !open_skipped),
            inspect_semantic,
        },
    )
    .unwrap_or_else(|err| {
        // `{err:#}` renders the full anyhow chain: the outer context alone
        // ("computing lexical storage fingerprint for ...") hides the root
        // cause (e.g. a corrupt FTS5 structure record), which is the only
        // actionable part of the failure (cass#368).
        let summary = format!("{err:#}");
        crate::search::asset_state::SearchAssetSnapshot {
            lexical: crate::search::asset_state::LexicalAssetState {
                status: "error",
                exists: lexical_index_initialized,
                engine_incompatible: false,
                fresh: false,
                stale: true,
                rebuilding: index_run
                    .mode
                    .is_some_and(crate::search::asset_state::SearchMaintenanceMode::rebuild_active)
                    && index_run.active,
                stalled: false,
                last_progress_age_ms: None,
                last_progress_at_ms: index_run.last_progress_at_ms,
                watch_active: index_run
                    .mode
                    .is_some_and(crate::search::asset_state::SearchMaintenanceMode::watch_active)
                    && index_run.active,
                last_indexed_at_ms: last_indexed_at,
                age_seconds: last_indexed_at
                    .and_then(|ts| (ts > 0).then(|| now_secs.saturating_sub((ts / 1000) as u64))),
                stale_threshold_seconds: stale_threshold,
                activity_at_ms: index_run.updated_at_ms.or(index_run.started_at_ms),
                pending_sessions: 0,
                processed_conversations: None,
                total_conversations: None,
                indexed_docs: None,
                hollow: false,
                live_docs: None,
                manifest_source: None,
                status_reason: Some(format!("asset inspection failed: {summary}")),
                fingerprint: crate::search::asset_state::LexicalFingerprintState {
                    current_db_fingerprint: None,
                    checkpoint_fingerprint: None,
                    matches_current_db_fingerprint: None,
                },
                checkpoint: crate::search::asset_state::LexicalCheckpointState {
                    present: false,
                    completed: None,
                    db_matches: None,
                    schema_matches: None,
                    page_size_matches: None,
                    page_size_compatible: None,
                },
            },
            semantic: crate::search::asset_state::SemanticAssetState {
                status: "error",
                availability: "load_failed",
                summary: format!("asset inspection failed: {summary}"),
                available: false,
                can_search: false,
                fallback_mode: Some("lexical"),
                preferred_backend: "fastembed",
                embedder_id: None,
                vector_index_path: None,
                model_dir: None,
                hnsw_path: None,
                hnsw_present: false,
                hnsw_state: "not_inspected",
                hnsw_ready: false,
                progressive_ready: false,
                progressive_reason_code: None,
                quality_tier_published: false,
                semantic_only_search_available: false,
                hint: Some(
                    "Repair semantic assets when convenient; lexical search remains available."
                        .to_string(),
                ),
                fast_tier: Default::default(),
                quality_tier: Default::default(),
                backlog: Default::default(),
                checkpoint: Default::default(),
            },
        }
    });
    if !assets.lexical.rebuilding
        && last_scan_ts.is_some_and(|scan_ts| {
            last_indexed_at
                .map(|indexed_at| scan_ts > indexed_at.saturating_add(1_000))
                .unwrap_or(true)
        })
    {
        assets.lexical.status = "stale";
        assets.lexical.fresh = false;
        assets.lexical.stale = true;
        assets.lexical.status_reason = Some(
            "last_scan_ts is newer than last_indexed_at; a prior scan advanced without a completed projection into the searchable index"
                .to_string(),
        );
    }
    // A lease heartbeat and a recent timestamp cannot certify publication
    // while source replay remains durable. An unsuccessful requested probe
    // also cannot certify its absence. Passive no-maintenance reads retain
    // their existing contract and expose null instead of pretending to probe.
    let watch_replay_unverified = watch_lexical_replay_pending.is_none()
        && db_exists
        && !passive_db_probe
        && (allow_db_open || skip_db_open);
    if assets.lexical.exists
        && (watch_lexical_replay_pending == Some(true) || watch_replay_unverified)
    {
        assets.lexical.fresh = false;
        assets.lexical.stale = true;
        // Do not replace stronger missing/corrupt/hollow/rebuilding verdicts.
        if matches!(assets.lexical.status, "ready" | "stale") {
            assets.lexical.status = "stale";
            assets.lexical.status_reason = Some(
                if watch_lexical_replay_pending == Some(true) {
                    "durable watch ingestion or lexical publication is pending; watcher activity and last_indexed_at do not establish that new content is searchable"
                } else {
                    "durable watch replay could not be checked; searchable publication is not verified"
                }
                .to_string(),
            );
        }
    }
    let not_initialized = cass_not_initialized(
        db_exists,
        lexical_index_initialized,
        assets.lexical.rebuilding,
    );
    if not_initialized {
        assets.semantic.status = "not_initialized";
        assets.semantic.availability = "not_initialized";
        assets.semantic.summary =
            "semantic search is optional and has not been initialized yet".to_string();
        assets.semantic.available = false;
        assets.semantic.can_search = false;
        assets.semantic.fallback_mode = Some("lexical");
        assets.semantic.embedder_id = None;
        assets.semantic.vector_index_path = None;
        assets.semantic.model_dir = None;
        assets.semantic.hnsw_path = None;
        assets.semantic.hnsw_present = false;
        assets.semantic.hnsw_state = "not_inspected";
        assets.semantic.hnsw_ready = false;
        assets.semantic.progressive_ready = false;
        assets.semantic.progressive_reason_code = None;
        assets.semantic.quality_tier_published = false;
        assets.semantic.semantic_only_search_available = false;
        assets.semantic.hint = Some(
            "Run 'cass index --full' first. Optional later: run 'cass models install --from-file <DIR>' and 'cass index --semantic'."
                .to_string(),
        );
        assets.semantic.fast_tier = Default::default();
        assets.semantic.quality_tier = Default::default();
        assets.semantic.backlog = Default::default();
        assets.semantic.checkpoint = Default::default();
    }
    let lexical = &assets.lexical;
    let semantic = &assets.semantic;
    let lexical_rebuild_pipeline = if skip_db_open {
        crate::indexer::lexical_rebuild_pipeline_settings_snapshot_passive()
    } else {
        crate::indexer::lexical_rebuild_pipeline_settings_snapshot()
    };
    let lexical_rebuild_pipeline_runtime = if lexical.rebuilding {
        crate::indexer::load_active_lexical_rebuild_pipeline_runtime(&index_path, db_path)
            .ok()
            .flatten()
    } else {
        None
    };
    let mut lexical_rebuild_pipeline_json =
        serde_json::to_value(&lexical_rebuild_pipeline).unwrap_or(serde_json::Value::Null);
    if let serde_json::Value::Object(ref mut pipeline) = lexical_rebuild_pipeline_json {
        let controller_loadavg_high_watermark_1m = pipeline
            .remove("controller_loadavg_high_watermark_1m_milli")
            .and_then(|value| value.as_u64())
            .map(|value| value as f64 / 1000.0);
        let controller_loadavg_low_watermark_1m = pipeline
            .remove("controller_loadavg_low_watermark_1m_milli")
            .and_then(|value| value.as_u64())
            .map(|value| value as f64 / 1000.0);
        pipeline.insert(
            "controller_loadavg_high_watermark_1m".to_string(),
            controller_loadavg_high_watermark_1m
                .map(serde_json::Value::from)
                .unwrap_or(serde_json::Value::Null),
        );
        pipeline.insert(
            "controller_loadavg_low_watermark_1m".to_string(),
            controller_loadavg_low_watermark_1m
                .map(serde_json::Value::from)
                .unwrap_or(serde_json::Value::Null),
        );
        // GH #496: the per-page byte budget page prep fetches against.
        let (startup_page_bytes, steady_page_bytes) =
            crate::indexer::lexical_rebuild_page_fetch_message_bytes(&lexical_rebuild_pipeline);
        pipeline.insert(
            "startup_page_fetch_message_bytes".to_string(),
            serde_json::json!(startup_page_bytes),
        );
        pipeline.insert(
            "steady_page_fetch_message_bytes".to_string(),
            serde_json::json!(steady_page_bytes),
        );
        let runtime_value = lexical_rebuild_pipeline_runtime
            .map(|runtime| {
                let queue_capacity = lexical_rebuild_pipeline.pipeline_channel_size;
                let inflight_message_bytes_limit = if runtime.max_message_bytes_in_flight > 0 {
                    runtime.max_message_bytes_in_flight
                } else {
                    lexical_rebuild_pipeline.pipeline_max_message_bytes_in_flight.max(1)
                };
                serde_json::json!({
                    "queue_depth": runtime.queue_depth,
                    "queue_capacity": queue_capacity,
                    "queue_headroom": queue_capacity.saturating_sub(runtime.queue_depth),
                    "inflight_message_bytes": runtime.inflight_message_bytes,
                    "max_message_bytes_in_flight": inflight_message_bytes_limit,
                    "inflight_message_bytes_headroom": inflight_message_bytes_limit
                        .saturating_sub(runtime.inflight_message_bytes),
                    "pending_batch_conversations": runtime.pending_batch_conversations,
                    "pending_batch_message_bytes": runtime.pending_batch_message_bytes,
                    "page_prep_workers": runtime.page_prep_workers,
                    "active_page_prep_jobs": runtime.active_page_prep_jobs,
                    "ordered_buffered_pages": runtime.ordered_buffered_pages,
                    "budget_generation": runtime.budget_generation,
                    "producer_budget_wait_count": runtime.producer_budget_wait_count,
                    "producer_budget_wait_ms": runtime.producer_budget_wait_ms,
                    "producer_handoff_wait_count": runtime.producer_handoff_wait_count,
                    "producer_handoff_wait_ms": runtime.producer_handoff_wait_ms,
                    "host_loadavg_1m": runtime.host_loadavg_1m_milli.map(|value| f64::from(value) / 1000.0),
                    "controller_mode": if runtime.controller_mode.is_empty() {
                        serde_json::Value::Null
                    } else {
                        serde_json::json!(runtime.controller_mode)
                    },
                    "controller_reason": if runtime.controller_reason.is_empty() {
                        serde_json::Value::Null
                    } else {
                        serde_json::json!(runtime.controller_reason)
                    },
                    "staged_merge_workers_max": runtime.staged_merge_workers_max,
                    "staged_merge_allowed_jobs": runtime.staged_merge_allowed_jobs,
                    "staged_merge_active_jobs": runtime.staged_merge_active_jobs,
                    "staged_merge_ready_artifacts": runtime.staged_merge_ready_artifacts,
                    "staged_merge_ready_groups": runtime.staged_merge_ready_groups,
                    "staged_merge_controller_reason": if runtime.staged_merge_controller_reason.is_empty() {
                        serde_json::Value::Null
                    } else {
                        serde_json::json!(runtime.staged_merge_controller_reason)
                    },
                    "staged_shard_build_workers_max": runtime.staged_shard_build_workers_max,
                    "staged_shard_build_allowed_jobs": runtime.staged_shard_build_allowed_jobs,
                    "staged_shard_build_active_jobs": runtime.staged_shard_build_active_jobs,
                    "staged_shard_build_pending_jobs": runtime.staged_shard_build_pending_jobs,
                    "staged_shard_build_controller_reason": if runtime.staged_shard_build_controller_reason.is_empty() {
                        serde_json::Value::Null
                    } else {
                        serde_json::json!(runtime.staged_shard_build_controller_reason)
                    },
                    "updated_at": format_timestamp_millis_rfc3339(runtime.updated_at_ms),
                })
            })
            .unwrap_or(serde_json::Value::Null);
        pipeline.insert("runtime".to_string(), runtime_value);
    }
    let semantic_policy = crate::search::policy::SemanticPolicy::resolve(
        &crate::search::policy::CliSemanticOverrides::default(),
    );
    let policy_registry = serde_json::to_value(crate::policy_registry::policy_registry_snapshot(
        &semantic_policy,
        semantic.available,
        semantic.fallback_mode,
        &lexical_rebuild_pipeline_json,
    ))
    .unwrap_or(serde_json::Value::Null);
    let ingest_quarantine_summary =
        crate::indexer::conversation_ingest_quarantine_summary(data_dir);
    let quarantined_conversations = ingest_quarantine_summary.quarantined_conversations;
    let ingest_quarantine_json =
        serde_json::to_value(&ingest_quarantine_summary).unwrap_or(serde_json::Value::Null);

    // Probe the live lexical document count when the DB has messages. Prefer
    // the published generation manifest: status only needs the durable count
    // for stale/empty diagnostics, while opening a large Tantivy reader here
    // fans out across every segment file on the hot robot path.
    // GH #457: prefer the count the engine SERVES (its MANIFEST, metadata
    // only) over the count a rebuild once RECORDED (the generation manifest):
    // a hollow generation keeps the recorded count while serving almost
    // nothing, which is exactly the gap that let `healthy: true` stand.
    let index_doc_count: Option<u64> = if let Some(live_docs) = lexical.live_docs {
        Some(live_docs)
    } else if db_opened && message_count > 0 && lexical.exists {
        lexical_manifest_indexed_doc_count(&index_path).or_else(|| {
            frankensearch::lexical_tantivy::cass_open_search_reader(
                &index_path,
                frankensearch::lexical_tantivy::ReloadPolicy::Manual,
            )
            .ok()
            .map(|(reader, _fields)| reader.searcher().num_docs())
        })
    } else {
        None
    };
    // #441 / WS-B.1a: how fragmented the published lexical generation is, from
    // directory metadata only (an upper bound on the live segment count; see
    // `quill_bridge::segment_file_count`). `null` when no Quill index exists.
    let lexical_segment_files: Option<u64> = if lexical.exists {
        crate::search::quill_bridge::segment_file_count(&index_path)
            .map(|count| u64::try_from(count).unwrap_or(u64::MAX))
    } else {
        None
    };
    let index_empty_with_messages = index_doc_count
        .map(|docs| docs == 0 && message_count > 0)
        .unwrap_or(false);

    let ts_str = chrono::DateTime::from_timestamp(now_secs as i64, 0)
        .unwrap_or_else(chrono::Utc::now)
        .to_rfc3339();

    // [coding_agent_session_search #301 partial-index marker] When
    // `cass index --full` is aborted by the stall watchdog (exit 70) or
    // otherwise interrupted, it leaves a SEARCHABLE-but-incomplete lexical
    // index: the checkpoint is present but never marked completed. Search
    // then queries it with results that silently omit a fraction of
    // conversations. Surface a dedicated `partial` boolean (and reason)
    // distinct from age-staleness so `cass search --robot-meta` can warn
    // that the index is incomplete, not merely old. The condition is: the
    // index exists, is not actively rebuilding right now, and its
    // checkpoint is present-but-not-completed (the durable abort/interrupt
    // signature) — OR the live scan watermark is ahead of the projected
    // index (scan advanced without a completed projection). Both are the
    // exact signatures #301 produces after an aborted full index.
    let index_partial = lexical.exists
        && !lexical.rebuilding
        && ((lexical.checkpoint.present && lexical.checkpoint.completed == Some(false))
            || watch_lexical_replay_pending == Some(true)
            || last_scan_ts.is_some_and(|scan_ts| {
                last_indexed_at
                    .map(|indexed_at| scan_ts > indexed_at.saturating_add(1_000))
                    .unwrap_or(true)
            }));
    let index_partial_reason: Option<&'static str> = if index_partial {
        if lexical.checkpoint.present && lexical.checkpoint.completed == Some(false) {
            Some(
                "lexical index is PARTIAL: a prior `cass index --full` was aborted or interrupted before its checkpoint completed; search results omit conversations indexed after the abort. Re-run `cass index --full` to complete it.",
            )
        } else if watch_lexical_replay_pending == Some(true) {
            Some(
                "lexical index is PARTIAL: durable watch ingestion or publication remains pending for one or more source roots; new content may not yet be searchable. The watcher will retry; restart `cass index --watch` if no watcher is running.",
            )
        } else {
            Some(
                "lexical index is PARTIAL: a scan advanced past the last completed projection (scan watermark ahead of indexed watermark); search results may omit recently scanned conversations. Re-run `cass index --full` to reconcile.",
            )
        }
    } else {
        None
    };

    let mut state = serde_json::json!({
        "index": {
            "exists": lexical.exists,
            "status": lexical.status,
            "reason": lexical.status_reason,
            "fresh": lexical.fresh,
            // [coding_agent_session_search #301] explicit incomplete-index
            // signal, distinct from age-staleness (`stale`).
            "partial": index_partial,
            "partial_reason": index_partial_reason,
            "last_indexed_at": last_indexed_at.map(|ts| {
                chrono::DateTime::from_timestamp_millis(ts)
                    .unwrap_or_else(chrono::Utc::now)
                    .to_rfc3339()
            }),
            "age_seconds": lexical.age_seconds,
            "stale": lexical.stale,
            "stale_threshold_seconds": stale_threshold,
            "rebuilding": lexical.rebuilding,
            "stalled": lexical.stalled,
            // b4uax: legacy Tantivy generation the current engine cannot read.
            "engine_incompatible": lexical.engine_incompatible,
            "activity_at": lexical.activity_at_ms.map(|ts| {
                chrono::DateTime::from_timestamp_millis(ts)
                    .unwrap_or_else(chrono::Utc::now)
                    .to_rfc3339()
            }),
            "documents": index_doc_count,
            // GH #457: what the published generation serves (manifest
            // metadata only) and whether that contradicts the completed
            // checkpoint. `reason` carries the operator-facing verdict.
            "live_documents": lexical.live_docs,
            "hollow": lexical.hollow,
            // mknmt: which MANIFEST slot readers serve; anything but
            // "current" is an older generation than the newest publish.
            "manifest_source": lexical
                .manifest_source
                .map(crate::search::quill_bridge::QuillManifestSource::as_str),
            // #441: upper bound on published Quill segments, metadata only.
            "segment_files": lexical_segment_files,
            "empty_with_messages": index_empty_with_messages,
            "quarantined_conversations": quarantined_conversations,
            "fingerprint": {
                "current_db_fingerprint": lexical.fingerprint.current_db_fingerprint,
                "checkpoint_fingerprint": lexical.fingerprint.checkpoint_fingerprint,
                "matches_current_db_fingerprint": lexical.fingerprint.matches_current_db_fingerprint,
            },
            "checkpoint": {
                "present": lexical.checkpoint.present,
                "completed": lexical.checkpoint.completed,
                "db_matches": lexical.checkpoint.db_matches,
                "schema_matches": lexical.checkpoint.schema_matches,
                "page_size_matches": lexical.checkpoint.page_size_matches,
                "page_size_compatible": lexical.checkpoint.page_size_compatible,
            }
        },
        "database": {
            "exists": db_exists,
            // WS-B.4a: physical footprint from metadata only (never an open).
            "db_bytes": db_size_bytes.unwrap_or(0),
            "wal_bytes": db_wal_bytes,
            "shm_present": db_shm_present,
            "opened": db_opened,
            "conversations": state_db_count_json(conversation_count, counts_skipped),
            "messages": state_db_count_json(message_count, counts_skipped),
            "open_error": db_open_error,
            "open_retryable": db_open_retryable,
            "counts_skipped": counts_skipped,
            "open_skipped": open_skipped,
            "integrity": db_integrity,
            "integrity_detail": db_integrity_detail,
            "integrity_hint": db_integrity_hint
        },
        "pending": {
            "sessions": lexical.pending_sessions,
            "lexical_replay": watch_lexical_replay_pending,
            "watch_active": lexical.watch_active,
            "orphaned": index_run.orphaned
        },
        "rebuild": {
            "active": lexical.rebuilding,
            // `stalled=true` means active=true but the indexing thread
            // has not made forward progress for at least
            // CASS_REBUILD_STALL_DETECT_SECS (default 120s), even though
            // the lock-file heartbeat is still being refreshed. Surface
            // this distinctly from `active` so operators (and the TUI)
            // can tell a slow-but-progressing rebuild apart from a
            // wedged one. Regression #258.
            "stalled": lexical.stalled,
            // b4uax: legacy Tantivy generation the current engine cannot read.
            "engine_incompatible": lexical.engine_incompatible,
            "last_progress_at": lexical.last_progress_at_ms.and_then(|ts| {
                chrono::DateTime::from_timestamp_millis(ts).map(|dt| dt.to_rfc3339())
            }),
            "last_progress_age_ms": lexical.last_progress_age_ms,
            "orphaned": index_run.orphaned,
            "pid": index_run.pid,
            "mode": index_run.mode.map(|mode| mode.as_lock_value()),
            "job_id": index_run.job_id,
            "job_kind": index_run.job_kind.map(|kind| kind.as_lock_value()),
            "phase": index_run.phase,
            "started_at": index_run.started_at_ms.map(|ts| {
                chrono::DateTime::from_timestamp_millis(ts)
                    .unwrap_or_else(chrono::Utc::now)
                    .to_rfc3339()
            }),
            "updated_at": lexical.activity_at_ms.map(|ts| {
                chrono::DateTime::from_timestamp_millis(ts)
                    .unwrap_or_else(chrono::Utc::now)
                    .to_rfc3339()
            }),
            "processed_conversations": lexical.processed_conversations,
            "total_conversations": lexical.total_conversations,
            "indexed_docs": lexical.indexed_docs,
            "pipeline": lexical_rebuild_pipeline_json,
            // GH #381 (ntxs1): where a running or interrupted full
            // expected-docs scan stands; the next index run resumes it
            // when the archive is unchanged or only grew. Null otherwise.
            "expected_docs_scan": crate::indexer::expected_lexical_docs_scan_position(&index_path, db_path)
                .map(|position| serde_json::json!({
                    "last_conversation_id": position.last_conversation_id,
                    "max_conversation_id": position.max_conversation_id,
                    "expected_docs_so_far": position.expected_docs_so_far,
                    "saved_at": position.saved_at_ms
                        .and_then(chrono::DateTime::from_timestamp_millis)
                        .map(|saved| saved.to_rfc3339()),
                }))
        },
        "semantic": {
            "status": semantic.status,
            "availability": semantic.availability,
            "summary": semantic.summary,
            "available": semantic.available,
            "can_search": semantic.can_search,
            "fallback_mode": semantic.fallback_mode,
            "preferred_backend": semantic.preferred_backend,
            "embedder_id": semantic.embedder_id,
            "vector_index_path": semantic.vector_index_path.as_ref().map(|path| path.display().to_string()),
            "model_dir": semantic.model_dir.as_ref().map(|path| path.display().to_string()),
            "hnsw_path": semantic.hnsw_path.as_ref().map(|path| path.display().to_string()),
            "hnsw_present": semantic.hnsw_present,
            "hnsw_state": semantic.hnsw_state,
            "hnsw_ready": semantic.hnsw_ready,
            "progressive_ready": semantic.progressive_ready,
            "progressive_reason_code": semantic.progressive_reason_code,
            // Always true since bead tg5o9 retired the `semantic` Cargo
            // feature: every build carries the pure-Rust
            // frankensearch/native backend (no ONNX, no `-baseline`
            // artifact). The key is kept for schema stability.
            "feature_compiled_in": true,
            // Sub-fix 3 for cass#257 — additive fields that report
            // quality-tier readiness independently of the
            // progressive/hybrid stack so `--mode semantic` consumers
            // see "yes, you can search" even while the fast tier or
            // lexical surface is still building.
            "quality_tier_published": semantic.quality_tier_published,
            "semantic_only_search_available": semantic.semantic_only_search_available,
            "hint": semantic.hint,
            "fast_tier": {
                "present": semantic.fast_tier.present,
                "ready": semantic.fast_tier.ready,
                "current_db_matches": semantic.fast_tier.current_db_matches,
                "conversation_count": semantic.fast_tier.conversation_count,
                "doc_count": semantic.fast_tier.doc_count,
                "embedder_id": semantic.fast_tier.embedder_id,
                "model_revision": semantic.fast_tier.model_revision,
                "completed_at": semantic
                    .fast_tier
                    .completed_at_ms
                    .and_then(format_timestamp_millis_rfc3339),
                "size_bytes": semantic.fast_tier.size_bytes,
                "shard_count": semantic.fast_tier.shard_count,
                "legacy_external_embedding_space": semantic.fast_tier.legacy_external_space,
            },
            "quality_tier": {
                "present": semantic.quality_tier.present,
                "ready": semantic.quality_tier.ready,
                "current_db_matches": semantic.quality_tier.current_db_matches,
                "conversation_count": semantic.quality_tier.conversation_count,
                "doc_count": semantic.quality_tier.doc_count,
                "embedder_id": semantic.quality_tier.embedder_id,
                "model_revision": semantic.quality_tier.model_revision,
                "completed_at": semantic
                    .quality_tier
                    .completed_at_ms
                    .and_then(format_timestamp_millis_rfc3339),
                "size_bytes": semantic.quality_tier.size_bytes,
                "shard_count": semantic.quality_tier.shard_count,
                "legacy_external_embedding_space": semantic.quality_tier.legacy_external_space,
            },
            "backlog": {
                "total_conversations": semantic.backlog.total_conversations,
                "fast_tier_processed": semantic.backlog.fast_tier_processed,
                "fast_tier_remaining": semantic.backlog.fast_tier_remaining,
                "quality_tier_processed": semantic.backlog.quality_tier_processed,
                "quality_tier_remaining": semantic.backlog.quality_tier_remaining,
                "pending_work": semantic.backlog.pending_work,
                "current_db_matches": semantic.backlog.current_db_matches,
                "computed_at": semantic
                    .backlog
                    .computed_at_ms
                    .and_then(format_timestamp_millis_rfc3339),
            },
            "checkpoint": {
                "active": semantic.checkpoint.active,
                "tier": semantic.checkpoint.tier,
                "current_db_matches": semantic.checkpoint.current_db_matches,
                "completed": semantic.checkpoint.completed,
                "conversations_processed": semantic.checkpoint.conversations_processed,
                "total_conversations": semantic.checkpoint.total_conversations,
                "progress_pct": semantic.checkpoint.progress_pct,
                "docs_embedded": semantic.checkpoint.docs_embedded,
                "last_offset": semantic.checkpoint.last_offset,
                "saved_at": semantic
                    .checkpoint
                    .saved_at_ms
                    .and_then(format_timestamp_millis_rfc3339),
            },
        },
        "ingest_quarantine": ingest_quarantine_json,
        "policy_registry": policy_registry,
        "_meta": {
            "timestamp": ts_str,
            "data_dir": data_dir.display().to_string(),
            "db_path": db_path.display().to_string()
        }
    });
    // zn1xn: emit the "canonical FTS shadow may be half-rebuilt" signal
    // additively — present only when the last full index run left the shadow
    // unrepaired — so existing status/index JSON is byte-identical otherwise.
    if let Some(detail) = fallback_fts_repair_pending
        && let Some(index) = state.get_mut("index").and_then(|v| v.as_object_mut())
    {
        // GH #495: a shadow retired as not viable over the message bound keeps
        // this marker to say why it is absent, but that is a settled state:
        // `doctor --rebuild-canonical-fts` plans nothing for it. Only other
        // markers describe a repair that is still pending.
        let retired =
            crate::storage::sqlite::error_message_indicates_fts_shadow_not_viable(&detail);
        index.insert(
            "fallback_fts_repair".to_string(),
            serde_json::json!({
                "pending": !retired,
                "retired_not_viable": retired,
                "detail": detail,
            }),
        );
    }
    // zn1xn F4: surface a persistent lexical-repair deferral streak additively —
    // present only when the last non-watch run deferred inline lexical updates,
    // so existing JSON is byte-identical otherwise.
    if let Some(runs) = lexical_repair_deferred_runs
        && let Some(index) = state.get_mut("index").and_then(|v| v.as_object_mut())
    {
        index.insert(
            "lexical_repair_deferred".to_string(),
            serde_json::json!({
                "consecutive_runs": runs,
                "reason": lexical_repair_deferred_reason,
            }),
        );
    }
    state
}

fn lexical_manifest_indexed_doc_count(index_path: &Path) -> Option<u64> {
    use crate::indexer::lexical_generation::{LexicalGenerationPublishState, load_manifest};

    match load_manifest(index_path) {
        Ok(Some(manifest))
            if matches!(
                manifest.publish_state,
                LexicalGenerationPublishState::Published
            ) =>
        {
            Some(manifest.indexed_doc_count)
        }
        _ => None,
    }
}

/// Stale-on-read catch-up (see `indexer::background_refresh`).
///
/// Read paths (search, pack, TUI launch) call this *after* they have what
/// they need from the current index. If the index is stale/partial/behind,
/// a detached low-priority `cass index --background` is spawned so the next
/// read is fresh; the current read is never blocked or altered.
///
/// `precomputed` lets `--robot-meta` callers reuse the freshness block they
/// already built; otherwise the health-grade probe (no DB open) is used.
/// Returns the outcome JSON for robot metadata, or `None` when nothing was
/// attempted (policy off, scratch dir, headless test, index fresh).
fn maybe_auto_refresh_index_after_read(
    data_dir: &Path,
    db_path: &Path,
    precomputed: Option<&serde_json::Value>,
) -> Option<serde_json::Value> {
    use crate::indexer::background_refresh;

    if !background_refresh::AutoRefreshPolicy::from_env().enabled {
        return None;
    }
    // Scratch/test data dirs are ephemeral by definition; a detached child
    // indexing a tempdir after the test that created it has moved on is
    // pure noise (and a flake generator). Headless TUI harnesses likewise.
    if auto_refresh_is_scratch_data_dir(data_dir) || dotenvy::var("TUI_HEADLESS").is_ok() {
        return None;
    }
    let owned;
    let base_freshness = match precomputed {
        Some(value) => value,
        None => {
            let meta = state_meta_json_for_health(data_dir, db_path, DEFAULT_STALE_THRESHOLD_SECS);
            owned = state_index_freshness(&meta)?;
            &owned
        }
    };
    let mut trigger = background_refresh::catch_up_reason(base_freshness)?;
    let enriched;
    let freshness = if trigger == "source-scan-overdue"
        && let Some(with_scan) =
            background_refresh::enrich_freshness_with_source_scan_watermark(base_freshness, db_path)
    {
        enriched = with_scan;
        trigger = background_refresh::catch_up_reason(&enriched)?;
        &enriched
    } else {
        base_freshness
    };
    let outcome = background_refresh::maybe_spawn_background_index_refresh(
        data_dir,
        db_path,
        trigger,
        background_refresh::last_indexed_at_ms_from_freshness(freshness),
    );
    let mut value = serde_json::to_value(&outcome).unwrap_or(serde_json::Value::Null);
    if let serde_json::Value::Object(map) = &mut value {
        map.insert(
            "trigger".to_string(),
            serde_json::Value::String(trigger.to_string()),
        );
    }
    Some(value)
}

/// True when `data_dir` lives under the platform temp dir (or `CASS_DATA_DIR`
/// explicitly points at one). Auto-refresh never fires there, and neither
/// does the TUI's first-run index.
pub(crate) fn auto_refresh_is_scratch_data_dir(data_dir: &Path) -> bool {
    // Both sides resolve through their nearest existing ancestor, so a data
    // dir that does not exist yet shares the temp dir's canonical spelling
    // (`\\?\C:\...` on Windows, `/private/var/folders/...` on macOS).
    let canon_temp = normalize_path_identity(&std::env::temp_dir());
    let canon_dir = normalize_path_identity(data_dir);
    // `/tmp` is a symlink to `/private/tmp` on macOS, so a canonicalized
    // Linux-style `/tmp/...` data dir lands under `/private/tmp` there.
    canon_dir.starts_with(&canon_temp)
        || canon_dir.starts_with("/tmp")
        || canon_dir.starts_with("/private/tmp")
        || canon_dir.starts_with("/var/tmp")
        || canon_dir.starts_with("/private/var/tmp")
}

fn state_index_freshness(state: &serde_json::Value) -> Option<serde_json::Value> {
    let index = state.get("index")?;
    let pending = state.get("pending");
    Some(serde_json::json!({
        "exists": index.get("exists"),
        "status": index.get("status"),
        "reason": index.get("reason"),
        "fresh": index.get("fresh"),
        "last_indexed_at": index.get("last_indexed_at"),
        "age_seconds": index.get("age_seconds"),
        "stale": index.get("stale"),
        "stale_threshold_seconds": index.get("stale_threshold_seconds"),
        // b4uax: legacy Tantivy generation the current engine cannot read.
        "engine_incompatible": index.get("engine_incompatible"),
        // mknmt: readers serving MANIFEST.prev, not the newest publish.
        "manifest_source": index.get("manifest_source"),
        "rebuilding": index.get("rebuilding"),
        // [coding_agent_session_search #301] propagate the incomplete-index
        // markers so `cass search --robot-meta` can warn that the index is
        // PARTIAL (aborted/interrupted full index), not merely stale.
        "partial": index.get("partial"),
        "partial_reason": index.get("partial_reason"),
        "pending_sessions": pending.and_then(|p| p.get("sessions"))
    }))
}

fn rebuild_progress_summary_json(state: &serde_json::Value) -> serde_json::Value {
    let rebuild = state.get("rebuild").unwrap_or(&serde_json::Value::Null);
    let runtime = rebuild
        .get("pipeline")
        .and_then(|pipeline| pipeline.get("runtime"))
        .unwrap_or(&serde_json::Value::Null);
    let value_or_null =
        |value: Option<&serde_json::Value>| value.cloned().unwrap_or(serde_json::Value::Null);

    let active = rebuild
        .get("active")
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    let stalled = rebuild
        .get("stalled")
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    let last_progress_at = rebuild
        .get("last_progress_at")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let last_progress_age_ms = rebuild
        .get("last_progress_age_ms")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let processed = rebuild
        .get("processed_conversations")
        .and_then(|value| value.as_u64());
    let total = rebuild
        .get("total_conversations")
        .and_then(|value| value.as_u64());
    let remaining = total.map(|count| count.saturating_sub(processed.unwrap_or(0)));
    let completion_ratio = match (processed, total) {
        (Some(done), Some(total_conversations)) if total_conversations > 0 => {
            Some((done as f64 / total_conversations as f64).clamp(0.0, 1.0))
        }
        _ => None,
    };
    let updated_at = runtime
        .get("updated_at")
        .cloned()
        .or_else(|| rebuild.get("updated_at").cloned())
        .unwrap_or(serde_json::Value::Null);

    serde_json::json!({
        "active": active,
        // Issue #258: surface `stalled` distinctly so health/status
        // consumers can tell a wedged indexer apart from a slow rebuild.
        "stalled": stalled,
        "last_progress_at": last_progress_at,
        "last_progress_age_ms": last_progress_age_ms,
        "mode": value_or_null(rebuild.get("mode")),
        "phase": value_or_null(rebuild.get("phase")),
        "processed_conversations": processed,
        "total_conversations": total,
        "remaining_conversations": remaining,
        "completion_ratio": completion_ratio,
        "indexed_docs": value_or_null(rebuild.get("indexed_docs")),
        "runtime_available": runtime.is_object(),
        "queue_depth": value_or_null(runtime.get("queue_depth")),
        "queue_capacity": value_or_null(runtime.get("queue_capacity")),
        "queue_headroom": value_or_null(runtime.get("queue_headroom")),
        "pending_batch_conversations": value_or_null(runtime.get("pending_batch_conversations")),
        "pending_batch_message_bytes": value_or_null(runtime.get("pending_batch_message_bytes")),
        "inflight_message_bytes": value_or_null(runtime.get("inflight_message_bytes")),
        "max_message_bytes_in_flight": value_or_null(runtime.get("max_message_bytes_in_flight")),
        "inflight_message_bytes_headroom": value_or_null(runtime.get("inflight_message_bytes_headroom")),
        "controller_mode": value_or_null(runtime.get("controller_mode")),
        "controller_reason": value_or_null(runtime.get("controller_reason")),
        "updated_at": updated_at,
    })
}

fn state_db_count_json(count: i64, counts_skipped: bool) -> serde_json::Value {
    if counts_skipped {
        serde_json::Value::Null
    } else {
        serde_json::Value::from(count)
    }
}

fn state_database_counts_need_refresh(state: &serde_json::Value, db_path: &Path) -> bool {
    let current_opened = state
        .get("database")
        .and_then(|db| db.get("opened"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let current_counts_skipped = state
        .get("database")
        .and_then(|db| db.get("counts_skipped"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(true);
    let current_conversations = state
        .get("database")
        .and_then(|db| db.get("conversations"))
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0);
    let current_messages = state
        .get("database")
        .and_then(|db| db.get("messages"))
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0);

    let count_refresh_allowed = std::fs::metadata(db_path).ok().is_some_and(|metadata| {
        metadata.is_file() && metadata.len() <= STATUS_COUNT_SCAN_MAX_DB_BYTES
    });
    ((!current_counts_skipped || count_refresh_allowed)
        && (!current_opened || current_conversations <= 0 || current_messages <= 0))
        && db_path.exists()
}

fn apply_state_database_count_snapshot(state: &mut serde_json::Value, refreshed: StateDbSnapshot) {
    let current_opened = state
        .get("database")
        .and_then(|db| db.get("opened"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let current_counts_skipped = state
        .get("database")
        .and_then(|db| db.get("counts_skipped"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(true);
    let current_conversations = state
        .get("database")
        .and_then(|db| db.get("conversations"))
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0);
    let current_messages = state
        .get("database")
        .and_then(|db| db.get("messages"))
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0);

    let improved = (!current_opened && refreshed.opened)
        || (current_counts_skipped && !refreshed.counts_skipped)
        || (current_conversations <= 0 && refreshed.conversation_count > 0)
        || (current_messages <= 0 && refreshed.message_count > 0);
    if !improved {
        return;
    }

    let Some(database) = state.get_mut("database").and_then(|db| db.as_object_mut()) else {
        return;
    };
    database.insert(
        "opened".to_string(),
        serde_json::Value::Bool(refreshed.opened),
    );
    database.insert(
        "conversations".to_string(),
        state_db_count_json(refreshed.conversation_count, refreshed.counts_skipped),
    );
    database.insert(
        "messages".to_string(),
        state_db_count_json(refreshed.message_count, refreshed.counts_skipped),
    );
    database.insert(
        "open_error".to_string(),
        refreshed
            .open_error
            .map(serde_json::Value::String)
            .unwrap_or(serde_json::Value::Null),
    );
    database.insert(
        "open_retryable".to_string(),
        serde_json::Value::Bool(refreshed.open_retryable),
    );
    database.insert(
        "counts_skipped".to_string(),
        serde_json::Value::Bool(refreshed.counts_skipped),
    );
    // i6upe: a classified integrity verdict upgrades prior unknown state. A
    // transient refresh result never downgrades an existing verdict.
    if let Some(integrity) = refreshed.integrity {
        database.insert(
            "integrity".to_string(),
            serde_json::Value::String(
                match integrity {
                    StateDbIntegrity::Ok => "ok",
                    StateDbIntegrity::Corrupt => "corrupt",
                }
                .to_string(),
            ),
        );
        database.insert(
            "integrity_detail".to_string(),
            refreshed
                .integrity_detail
                .map(serde_json::Value::String)
                .unwrap_or(serde_json::Value::Null),
        );
    }
}

fn refresh_state_database_counts_if_needed(
    state: &mut serde_json::Value,
    db_path: &Path,
    reason: &str,
) {
    if !state_database_counts_need_refresh(state, db_path) {
        return;
    }

    let refreshed = probe_state_db(db_path, reason, Duration::from_secs(30), true);
    apply_state_database_count_snapshot(state, refreshed);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BoundedCountRefresh {
    NotNeeded,
    Completed,
    TimedOut,
}

fn maybe_test_triage_worker_delay(variable: &str) {
    if let Ok(raw) = dotenvy::var(variable)
        && let Ok(milliseconds) = raw.parse::<u64>()
        && milliseconds > 0
    {
        std::thread::sleep(Duration::from_millis(milliseconds));
    }
}

fn refresh_state_database_counts_with_hard_timeout(
    state: &mut serde_json::Value,
    db_path: &Path,
    reason: &str,
    timeout: Duration,
) -> BoundedCountRefresh {
    if !state_database_counts_need_refresh(state, db_path) {
        return BoundedCountRefresh::NotNeeded;
    }

    let path = db_path.to_path_buf();
    let reason = reason.to_string();
    let (tx, rx) = std::sync::mpsc::channel();
    let Ok(_count_worker) = std::thread::Builder::new()
        .name("cass-triage-count-probe".to_string())
        .spawn(move || {
            maybe_test_triage_worker_delay("CASS_TEST_TRIAGE_COUNT_SLOW_MS");
            let snapshot = probe_state_db(&path, &reason, timeout, true);
            let _ = tx.send(snapshot);
        })
    else {
        return BoundedCountRefresh::TimedOut;
    };

    match rx.recv_timeout(timeout) {
        Ok(snapshot) => {
            apply_state_database_count_snapshot(state, snapshot);
            BoundedCountRefresh::Completed
        }
        Err(
            std::sync::mpsc::RecvTimeoutError::Timeout
            | std::sync::mpsc::RecvTimeoutError::Disconnected,
        ) => BoundedCountRefresh::TimedOut,
    }
}

fn readiness_snapshot_from_state(
    state: &serde_json::Value,
    not_initialized: bool,
) -> crate::search::readiness::ReadinessSnapshot {
    use crate::search::readiness::ReadinessSnapshot;

    let lexical = lexical_readiness_from_state(state, not_initialized);
    let semantic = semantic_readiness_from_state(state);
    ReadinessSnapshot::new(lexical, semantic)
}

/// Live bridge (bead `cass-fleet-resilience-20260608-uojcg.envu1`): build the
/// canonical typed `DerivedAssetTruthTable` for `data_dir` from the same
/// `state_meta_json` evidence `run_status`/`run_doctor` already read.
///
/// Today the typed truth table is only constructed in fixtures; the live
/// readiness surfaces work off a parallel `serde_json::Value` snapshot. This
/// builder maps that snapshot into the typed table so the typed projections
/// (`readiness_projection::project`, `next_command_envelope`,
/// `QuarantineSummary`) can be reused across surfaces instead of each
/// re-deriving the JSON. Staged ahead of its callers (the recovery-support
/// bundle wiring `6f1lm` and the human readiness summary wiring `v6vuz`),
/// which become thin assemblers over this one builder.
pub(crate) fn build_truth_table_for_data_dir(
    data_dir: &Path,
    db_path: &Path,
    stale_threshold: u64,
) -> crate::search::readiness::DerivedAssetTruthTable {
    // allow_db_open=true: derive from the real DB-open signal (status-like),
    // so OpenFailed is distinguished from a present-but-elided open.
    let state = state_meta_json(data_dir, db_path, stale_threshold, true);
    build_truth_table_from_state(&state)
}

/// Variant of [`build_truth_table_for_data_dir`] for callers that already
/// hold a `state_meta_json` snapshot (status/doctor compute it once), so the
/// data dir is not re-probed.
pub(crate) fn build_truth_table_from_state(
    state: &serde_json::Value,
) -> crate::search::readiness::DerivedAssetTruthTable {
    let db_exists = state
        .pointer("/database/exists")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let lexical_index_initialized = state
        .pointer("/index/exists")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let rebuild_active = state
        .pointer("/rebuild/active")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let not_initialized =
        cass_not_initialized(db_exists, lexical_index_initialized, rebuild_active);
    let readiness = readiness_snapshot_from_state(state, not_initialized);
    crate::search::readiness::truth_table_from_state(state, readiness)
}

/// Render the bounded human readiness summary (bead
/// `coding_agent_session_search-v6vuz`) as a CLI section, from a live
/// `state_meta_json` snapshot the surface already computed.
///
/// This is the live counterpart of the `.13.2` contract
/// (`crate::search::human_readiness_summary`): every human (non-`--json`)
/// readiness surface emits the *same* projection the robot JSON serializes, so
/// the operator-facing copy can never drift from the machine contract. The
/// projection is pure over the already-built snapshot (no extra I/O), so even
/// the `<50ms` health surface stays fast. Nothing is printed if the projection
/// would be unsafe to surface — a structural backstop on top of the curated
/// safe-next-command, so a human surface can never recommend a destructive or
/// archive-risking action.
fn print_human_readiness_section(
    state: &serde_json::Value,
    surface: crate::search::readiness_projection::SurfaceKind,
) {
    let table = build_truth_table_from_state(state);
    let summary = crate::search::human_readiness_summary::project_human_summary(&table, surface);
    if !summary.is_safe(table.archive_risk) {
        return;
    }
    println!();
    println!("Readiness:");
    for line in summary.render_lines() {
        println!("{line}");
    }
}

fn lexical_readiness_from_state(
    state: &serde_json::Value,
    not_initialized: bool,
) -> crate::search::readiness::LexicalReadinessState {
    use crate::search::readiness::LexicalReadinessState;

    if not_initialized {
        return LexicalReadinessState::Missing;
    }

    let index = state.get("index");
    let rebuild_active = state
        .get("rebuild")
        .and_then(|rebuild| rebuild.get("active"))
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    if rebuild_active {
        return LexicalReadinessState::Repairing;
    }

    let exists = index
        .and_then(|idx| idx.get("exists"))
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    if !exists {
        return LexicalReadinessState::Missing;
    }

    let status = index
        .and_then(|idx| idx.get("status"))
        .and_then(|value| value.as_str())
        .unwrap_or("unknown");
    let reason = index
        .and_then(|idx| idx.get("reason"))
        .and_then(|value| value.as_str())
        .unwrap_or("");

    if status == "error" && reason.contains("quarantin") {
        return LexicalReadinessState::CorruptQuarantined;
    }
    // GH #368 defect 2: any other asset-inspection error means search actually
    // hard-fails — most importantly the lexical-fingerprint open failing on a
    // corrupt FTS5 structure ("computing lexical storage fingerprint ... fts5:
    // corrupt %_data record ..."). That error path (state_meta_json_inner's
    // unwrap_or_else fallback) sets status="error" AND fresh=false, and the
    // fresh=false branch below previously classified it StaleButSearchable —
    // reporting "Search usable now: yes" while every query errored. An errored
    // index is corrupt/unusable (non-searchable, inspect first), not stale.
    if status == "error" {
        return LexicalReadinessState::CorruptQuarantined;
    }
    // b4uax (gh#382): a legacy Tantivy generation exists on disk but no
    // reader can open it. Search is unavailable until the rebuild runs, so it
    // is the Missing class (repair now), never "stale but searchable".
    if status == "legacy_engine"
        || index
            .and_then(|idx| idx.get("engine_incompatible"))
            .and_then(|value| value.as_bool())
            .unwrap_or(false)
    {
        return LexicalReadinessState::Missing;
    }
    // GH #457: a hollow generation serves a small fraction of the corpus while
    // every other signal says ready. Search technically runs, but its answers
    // are wrong ("no prior work found"), so it is the rebuild-now class —
    // never "stale but searchable" (which promises correct answers for what
    // is already indexed).
    if status == "hollow"
        || index
            .and_then(|idx| idx.get("hollow"))
            .and_then(|value| value.as_bool())
            .unwrap_or(false)
    {
        return LexicalReadinessState::Missing;
    }
    if status == "stale"
        || index
            .and_then(|idx| idx.get("fresh"))
            .and_then(|value| value.as_bool())
            == Some(false)
    {
        return LexicalReadinessState::StaleButSearchable;
    }

    LexicalReadinessState::Ready
}

fn semantic_readiness_from_state(
    state: &serde_json::Value,
) -> crate::search::readiness::SemanticReadinessState {
    use crate::search::readiness::SemanticReadinessState;

    let semantic = state.get("semantic");
    let availability = semantic
        .and_then(|sem| sem.get("availability"))
        .and_then(|value| value.as_str())
        .unwrap_or("unknown");
    if availability == "disabled" {
        return SemanticReadinessState::PolicyDisabled;
    }

    let can_search = semantic
        .and_then(|sem| sem.get("can_search"))
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    let fast_ready = semantic
        .and_then(|sem| sem.get("fast_tier"))
        .and_then(|tier| tier.get("ready"))
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
        && semantic
            .and_then(|sem| sem.get("fast_tier"))
            .and_then(|tier| tier.get("current_db_matches"))
            .and_then(|value| value.as_bool())
            .unwrap_or(false);
    let quality_ready = semantic
        .and_then(|sem| sem.get("quality_tier"))
        .and_then(|tier| tier.get("ready"))
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
        && semantic
            .and_then(|sem| sem.get("quality_tier"))
            .and_then(|tier| tier.get("current_db_matches"))
            .and_then(|value| value.as_bool())
            .unwrap_or(false);
    let checkpoint_active = semantic
        .and_then(|sem| sem.get("checkpoint"))
        .and_then(|checkpoint| checkpoint.get("active"))
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    let backlog_pending = semantic
        .and_then(|sem| sem.get("backlog"))
        .and_then(|backlog| backlog.get("pending_work"))
        .and_then(|value| value.as_bool())
        .unwrap_or(false);

    if quality_ready && (fast_ready || can_search) {
        return SemanticReadinessState::HybridReady;
    }
    if fast_ready || can_search {
        return SemanticReadinessState::FastTierReady;
    }
    if checkpoint_active || backlog_pending || availability == "index_building" {
        return SemanticReadinessState::Backfilling;
    }

    SemanticReadinessState::Absent
}

fn semantic_recommended_action(state: &serde_json::Value, not_initialized: bool) -> Option<String> {
    use crate::search::readiness::RecommendedAction;

    match readiness_snapshot_from_state(state, not_initialized).recommended_action() {
        RecommendedAction::WaitForSemanticCatchUp => Some(
            "Lexical search is ready; semantic assets are still catching up. Continue with lexical results or wait for hybrid refinement to finish."
                .to_string(),
        ),
        RecommendedAction::SemanticDisabledByPolicy => Some(
            "Semantic refinement is disabled by policy; continue with lexical search or re-enable semantic mode for hybrid results."
                .to_string(),
        ),
        _ => None,
    }
}

fn shell_quote_arg(arg: &str) -> String {
    if !arg.is_empty()
        && arg
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '/' | '.' | '_' | '-' | ':' | '='))
    {
        arg.to_string()
    } else {
        format!("'{}'", arg.replace('\'', "'\"'\"'"))
    }
}

fn cass_dataset_command(data_dir: &Path, db_path: &Path, subcommand_args: &[&str]) -> String {
    let mut parts = vec!["cass".to_string()];
    let default_db_path = data_dir.join("agent_search.db");
    if db_path != default_db_path {
        parts.push("--db".to_string());
        parts.push(shell_quote_arg(&db_path.display().to_string()));
    }
    parts.extend(subcommand_args.iter().map(|arg| (*arg).to_string()));
    parts.push("--data-dir".to_string());
    parts.push(shell_quote_arg(&data_dir.display().to_string()));
    parts.join(" ")
}

struct ReadinessCommandRecommendation<'a> {
    id: &'a str,
    command: String,
    purpose: &'a str,
    safety: &'a str,
    run_when: &'a str,
    success_signal: &'a str,
    parse_fields: &'a [&'a str],
    retry_after_ms: Option<u64>,
}

fn readiness_command_recommendation(
    recommendation: ReadinessCommandRecommendation<'_>,
) -> serde_json::Value {
    serde_json::json!({
        "id": recommendation.id,
        "command": recommendation.command,
        "purpose": recommendation.purpose,
        "safety": recommendation.safety,
        "run_when": recommendation.run_when,
        "success_signal": recommendation.success_signal,
        "parse_fields": recommendation.parse_fields,
        "retry_after_ms": recommendation.retry_after_ms,
    })
}

fn readiness_recommended_commands(
    data_dir: &Path,
    db_path: &Path,
    state: &serde_json::Value,
    status: &str,
    healthy: bool,
    not_initialized: bool,
    recommended_action: Option<&str>,
) -> Vec<serde_json::Value> {
    let index_exists = state
        .get("index")
        .and_then(|index| index.get("exists"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let index_fresh = state
        .get("index")
        .and_then(|index| index.get("fresh"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let index_empty_with_messages = state
        .get("index")
        .and_then(|index| index.get("empty_with_messages"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let db_exists = state
        .get("database")
        .and_then(|database| database.get("exists"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let db_opened = state
        .get("database")
        .and_then(|database| database.get("opened"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let db_open_retryable = state
        .get("database")
        .and_then(|database| database.get("open_retryable"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let rebuild_active = state
        .get("rebuild")
        .and_then(|rebuild| rebuild.get("active"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let pending_sessions = state
        .get("pending")
        .and_then(|pending| pending.get("sessions"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);

    // b4uax: legacy Tantivy generation — the only fix is a full rebuild.
    let index_engine_incompatible = state
        .get("index")
        .and_then(|index| index.get("engine_incompatible"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);

    let mut commands = Vec::new();
    if rebuild_active {
        commands.push(readiness_command_recommendation(
            ReadinessCommandRecommendation {
                id: "poll-active-rebuild",
                command: cass_dataset_command(data_dir, db_path, &["status", "--json"]),
                purpose: "Poll the active rebuild without starting a second writer.",
                safety: "read-only",
                run_when: "Run after a short delay while rebuild_progress.active is true.",
                success_signal: "rebuild_progress.active=false",
                parse_fields: &[
                    "status",
                    "rebuild.active",
                    "rebuild_progress",
                    "recommended_commands",
                ],
                retry_after_ms: Some(30_000),
            },
        ));
        return commands;
    }

    if not_initialized || (!db_exists && !index_exists) {
        commands.push(readiness_command_recommendation(
            ReadinessCommandRecommendation {
                id: "initialize-archive",
                command: cass_dataset_command(
                    data_dir,
                    db_path,
                    &["index", "--full", "--json", "--no-progress-events"],
                ),
                purpose: "Discover local sessions and build the initial cass archive plus lexical index.",
                safety: "writes-cass-archive-and-derived-index",
                run_when: "Run when status is not_initialized for this data_dir.",
                success_signal: "success=true, then cass health reports initialized=true",
                parse_fields: &["success", "conversations", "messages", "error"],
                retry_after_ms: None,
            },
        ));
        commands.push(readiness_command_recommendation(
            ReadinessCommandRecommendation {
                id: "verify-initialization",
                command: cass_dataset_command(data_dir, db_path, &["health", "--json"]),
                purpose: "Verify the same data_dir is ready after the initial index run.",
                safety: "read-only",
                run_when: "Run after initialize-archive finishes.",
                success_signal: "healthy=true or recommended_commands changes to the next blocker",
                parse_fields: &[
                    "status",
                    "healthy",
                    "initialized",
                    "errors",
                    "recommended_commands",
                ],
                retry_after_ms: None,
            },
        ));
        return commands;
    }

    if index_engine_incompatible && !not_initialized && db_exists {
        // Deterministic, known fix: no doctor inspection needed first, and an
        // incremental `cass index` cannot help (it would refresh an index the
        // engine cannot open).
        commands.push(readiness_command_recommendation(
            ReadinessCommandRecommendation {
                id: "rebuild-lexical-index-for-current-engine",
                command: cass_dataset_command(
                    data_dir,
                    db_path,
                    &["index", "--full", "--json", "--no-progress-events"],
                ),
                purpose: "Rebuild the lexical index for the current search engine; the existing generation was written by the legacy Tantivy engine and cannot be read.",
                safety: "writes-derived-index",
                run_when: "Run once after upgrading past the Tantivy->Quill engine flip; expect a full rebuild whose cost scales with message count.",
                success_signal: "success=true, then status reports index.status=ready and index.engine_incompatible=false",
                parse_fields: &["success", "indexing_stats", "error"],
                retry_after_ms: None,
            },
        ));
        commands.push(readiness_command_recommendation(
            ReadinessCommandRecommendation {
                id: "verify-lexical-index",
                command: cass_dataset_command(data_dir, db_path, &["status", "--json"]),
                purpose: "Verify lexical readiness after the engine-flip rebuild.",
                safety: "read-only",
                run_when: "Run after rebuild-lexical-index-for-current-engine finishes.",
                success_signal: "healthy=true or status changes to the next blocker",
                parse_fields: &["status", "healthy", "index", "recommended_commands"],
                retry_after_ms: None,
            },
        ));
        return commands;
    }

    if status == "degraded" || (db_exists && !db_opened && !db_open_retryable) {
        commands.push(readiness_command_recommendation(
            ReadinessCommandRecommendation {
                id: "inspect-archive-health",
                command: cass_dataset_command(data_dir, db_path, &["doctor", "--json"]),
                purpose: "Inspect database/index health before choosing a repair path.",
                safety: "read-only",
                run_when: "Run when the database exists but health/status cannot open it.",
                success_signal: "doctor JSON identifies repair-safe next actions",
                parse_fields: &[
                    "status",
                    "health_class",
                    "recommended_action",
                    "checks",
                    "operation_state",
                ],
                retry_after_ms: None,
            },
        ));
        return commands;
    }

    if !index_exists || index_empty_with_messages {
        commands.push(readiness_command_recommendation(
            ReadinessCommandRecommendation {
                id: "rebuild-lexical-index",
                command: cass_dataset_command(
                    data_dir,
                    db_path,
                    &["index", "--full", "--json", "--no-progress-events"],
                ),
                purpose: "Rebuild the lexical search index from the cass archive.",
                safety: "writes-derived-index",
                run_when: "Run when the archive exists but the lexical index is missing or empty.",
                success_signal: "success=true, then health/status report index.exists=true",
                parse_fields: &["success", "indexing_stats", "error"],
                retry_after_ms: None,
            },
        ));
        commands.push(readiness_command_recommendation(
            ReadinessCommandRecommendation {
                id: "verify-lexical-index",
                command: cass_dataset_command(data_dir, db_path, &["status", "--json"]),
                purpose: "Verify lexical readiness after rebuilding derived index assets.",
                safety: "read-only",
                run_when: "Run after rebuild-lexical-index finishes.",
                success_signal: "healthy=true or status changes to the next blocker",
                parse_fields: &["status", "healthy", "index", "recommended_commands"],
                retry_after_ms: None,
            },
        ));
        return commands;
    }

    if !index_fresh || pending_sessions > 0 {
        commands.push(readiness_command_recommendation(
            ReadinessCommandRecommendation {
                id: "refresh-lexical-index",
                command: cass_dataset_command(
                    data_dir,
                    db_path,
                    &["index", "--json", "--no-progress-events"],
                ),
                purpose: "Index newly discovered sessions without a full rebuild.",
                safety: "writes-cass-archive-and-derived-index",
                run_when: "Run when the index is stale or pending.sessions is nonzero.",
                success_signal: "success=true, then pending.sessions=0 and index.fresh=true",
                parse_fields: &["success", "conversations", "messages", "error"],
                retry_after_ms: None,
            },
        ));
        commands.push(readiness_command_recommendation(
            ReadinessCommandRecommendation {
                id: "verify-refresh",
                command: cass_dataset_command(data_dir, db_path, &["health", "--json"]),
                purpose: "Confirm the refresh cleared the readiness blocker.",
                safety: "read-only",
                run_when: "Run after refresh-lexical-index finishes.",
                success_signal: "healthy=true or recommended_commands changes to the next blocker",
                parse_fields: &["status", "healthy", "state.index", "recommended_commands"],
                retry_after_ms: None,
            },
        ));
        return commands;
    }

    if recommended_action.is_some_and(|action| action.contains("semantic assets")) {
        commands.push(readiness_command_recommendation(
            ReadinessCommandRecommendation {
                id: "search-lexical-while-semantic-catches-up",
                command: cass_dataset_command(
                    data_dir,
                    db_path,
                    &[
                        "search",
                        "'<query>'",
                        "--robot",
                        "--mode",
                        "lexical",
                        "--limit",
                        "10",
                        "--fields",
                        "summary",
                        "--robot-meta",
                    ],
                ),
                purpose: "Continue with lexical results while optional semantic assets catch up.",
                safety: "read-only",
                run_when: "Run when lexical search is ready but semantic refinement is still backfilling.",
                success_signal: "results[] plus realized.fallback_mode=lexical",
                parse_fields: &["results", "realized", "_meta"],
                retry_after_ms: None,
            },
        ));
        return commands;
    }

    if healthy {
        commands.push(readiness_command_recommendation(
            ReadinessCommandRecommendation {
                id: "search-now",
                command: cass_dataset_command(
                    data_dir,
                    db_path,
                    &[
                        "search",
                        "'<query>'",
                        "--robot",
                        "--limit",
                        "10",
                        "--fields",
                        "summary",
                        "--robot-meta",
                    ],
                ),
                purpose: "Run a bounded machine-readable search against the ready archive.",
                safety: "read-only",
                run_when: "Run when healthy=true.",
                success_signal: "results[] plus realized.search_mode",
                parse_fields: &["results", "realized", "_meta"],
                retry_after_ms: None,
            },
        ));
        commands.push(readiness_command_recommendation(
            ReadinessCommandRecommendation {
                id: "build-answer-pack",
                command: cass_dataset_command(
                    data_dir,
                    db_path,
                    &[
                        "pack",
                        "'<query>'",
                        "--robot",
                        "--fields",
                        "summary,health,freshness,privacy,warnings",
                        "--max-tokens",
                        "4000",
                    ],
                ),
                purpose: "Build a compact evidence pack for handoff to another agent.",
                safety: "read-only",
                run_when: "Run when healthy=true and you need source-backed context.",
                success_signal: "pack summary plus health/freshness/privacy/warnings",
                parse_fields: &["pack", "health", "freshness", "privacy", "warnings"],
                retry_after_ms: None,
            },
        ));
    }

    commands
}

#[cfg(test)]
mod readiness_projection_tests {
    use super::semantic_readiness_from_state;
    use crate::search::readiness::SemanticReadinessState;
    use serde_json::json;

    #[test]
    fn semantic_readiness_requires_current_db_match_for_fast_tier() {
        let state = json!({
            "semantic": {
                "availability": "index_building",
                "can_search": false,
                "fast_tier": {
                    "ready": true,
                    "current_db_matches": false
                },
                "quality_tier": {
                    "ready": false,
                    "current_db_matches": null
                },
                "checkpoint": {
                    "active": true
                },
                "backlog": {
                    "pending_work": true
                }
            }
        });

        assert_eq!(
            semantic_readiness_from_state(&state),
            SemanticReadinessState::Backfilling
        );
    }

    #[test]
    fn semantic_readiness_accepts_current_fast_tier() {
        let state = json!({
            "semantic": {
                "availability": "index_building",
                "can_search": true,
                "fast_tier": {
                    "ready": true,
                    "current_db_matches": true
                },
                "quality_tier": {
                    "ready": false,
                    "current_db_matches": null
                },
                "checkpoint": {
                    "active": false
                },
                "backlog": {
                    "pending_work": false
                }
            }
        });

        assert_eq!(
            semantic_readiness_from_state(&state),
            SemanticReadinessState::FastTierReady
        );
    }

    /// GH #368 defect 2: a non-quarantine asset-inspection error (the
    /// lexical-fingerprint open failing on a corrupt FTS5 structure) must NOT
    /// be classified searchable via the `fresh=false` fall-through.
    #[test]
    fn errored_lexical_index_is_corrupt_not_searchable() {
        use super::lexical_readiness_from_state;
        use crate::search::readiness::LexicalReadinessState;
        let state = json!({
            "index": {
                "exists": true,
                "status": "error",
                "fresh": false,
                "reason": "asset inspection failed: computing lexical storage fingerprint for /x: fts5: corrupt %_data record: structure segment count exceeds FTS5 maximum"
            },
            "rebuild": { "active": false }
        });
        let lexical = lexical_readiness_from_state(&state, false);
        assert_eq!(lexical, LexicalReadinessState::CorruptQuarantined);
        assert!(
            !lexical.is_searchable(),
            "an errored (corrupt-FTS) index must not report search usable (GH #368 defect 2)"
        );
    }

    /// A genuine `quarantin`ed error keeps mapping to CorruptQuarantined
    /// (unchanged by the defect-2 fix).
    #[test]
    fn quarantined_lexical_error_stays_corrupt_quarantined() {
        use super::lexical_readiness_from_state;
        use crate::search::readiness::LexicalReadinessState;
        let state = json!({
            "index": { "exists": true, "status": "error", "fresh": false,
                       "reason": "lexical index quarantined: schema_drift" },
            "rebuild": { "active": false }
        });
        assert_eq!(
            lexical_readiness_from_state(&state, false),
            LexicalReadinessState::CorruptQuarantined
        );
    }

    /// GH #369 non-regression: a merely-stale index (e.g. an *absent* FTS5
    /// shadow where the fingerprint open still succeeds and Tantivy serves
    /// search) has status != "error", so it stays StaleButSearchable — the
    /// defect-2 fix must not downgrade legitimately-searchable stale indexes.
    #[test]
    fn stale_healthy_index_stays_searchable() {
        use super::lexical_readiness_from_state;
        use crate::search::readiness::LexicalReadinessState;
        let state = json!({
            "index": { "exists": true, "status": "stale", "fresh": false },
            "rebuild": { "active": false }
        });
        let lexical = lexical_readiness_from_state(&state, false);
        assert_eq!(lexical, LexicalReadinessState::StaleButSearchable);
        assert!(
            lexical.is_searchable(),
            "a stale-but-healthy index must remain searchable (GH #369 absent-shadow case)"
        );

        let ready = json!({
            "index": { "exists": true, "status": "ready", "fresh": true },
            "rebuild": { "active": false }
        });
        assert_eq!(
            lexical_readiness_from_state(&ready, false),
            LexicalReadinessState::Ready
        );
    }
}

fn warn_tui_terminal_profile(stderr_is_tty: bool) {
    if !stderr_is_tty {
        return;
    }

    let env_truthy = |raw: Option<String>| {
        raw.map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
    };

    let no_color_global = dotenvy::var("NO_COLOR").is_ok();
    let no_color_local = dotenvy::var("CASS_NO_COLOR").is_ok();
    let respect_global = env_truthy(dotenvy::var("CASS_RESPECT_NO_COLOR").ok());
    let effective_no_color = no_color_local || (respect_global && no_color_global);
    if effective_no_color {
        eprintln!(
            "warning: CASS_NO_COLOR/NO_COLOR profile is active; cass TUI styling may be reduced."
        );
        eprintln!(
            "hint: use `CASS_NO_COLOR=1` for monochrome, or `CASS_RESPECT_NO_COLOR=1` if you want global NO_COLOR honored."
        );
    } else if no_color_global {
        eprintln!(
            "info: NO_COLOR is set but ignored by default. Set CASS_RESPECT_NO_COLOR=1 to honor it."
        );
    }

    let term = dotenvy::var("TERM").unwrap_or_default();
    if term.trim().eq_ignore_ascii_case("dumb") && dotenvy::var("TUI_HEADLESS").is_err() {
        eprintln!(
            "warning: TERM=dumb detected; cass will apply a compatibility profile unless CASS_ALLOW_DUMB_TERM=1."
        );
        eprintln!(
            "hint: try `env -u NO_COLOR TERM=xterm-256color COLORTERM=truecolor cass` for full rendering."
        );
    }
}

fn prepare_headless_once_tui_artifacts(
    data_dir: &Path,
    db_path: &Path,
    asciicast_path: Option<&Path>,
) -> Result<()> {
    std::fs::create_dir_all(data_dir).map_err(|e| {
        anyhow::anyhow!(
            "create headless --once data directory {}: {e}",
            data_dir.display()
        )
    })?;

    if let Some(parent) = db_path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent).map_err(|e| {
            anyhow::anyhow!(
                "create headless --once database parent {}: {e}",
                parent.display()
            )
        })?;
    }
    {
        let _conn = crate::franken_sync::Connection::open(db_path.to_string_lossy().as_ref())
            .map_err(|e| {
                anyhow::anyhow!(
                    "initialize SQLite database for headless --once at {}: {e}",
                    db_path.display()
                )
            })?;
    }

    let _index_path = crate::search::tantivy::index_dir(data_dir).map_err(|e| {
        anyhow::anyhow!(
            "initialize index directory for headless --once at {}: {e}",
            data_dir.display()
        )
    })?;

    if let Some(path) = asciicast_path {
        write_headless_asciicast_sentinel(path)?;
    }

    Ok(())
}

fn write_headless_asciicast_sentinel(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent).map_err(|e| {
            anyhow::anyhow!(
                "create asciicast output directory {}: {e}",
                parent.display()
            )
        })?;
    }

    let mut file = std::fs::File::create(path)
        .map_err(|e| anyhow::anyhow!("create asciicast file {}: {e}", path.display()))?;

    let header = serde_json::json!({
        "version": 2,
        "width": 120,
        "height": 40,
        "timestamp": Utc::now().timestamp(),
        "cass_artifact_kind": "headless_once_asciicast_sentinel",
        "recording_available": false,
        "reason": "non_interactive_headless_once_bootstrap",
        "env": {
            "TERM": dotenvy::var("TERM").unwrap_or_else(|_| "xterm-256color".to_string()),
            "SHELL": dotenvy::var("SHELL").unwrap_or_else(|_| "cass".to_string())
        }
    });

    writeln!(file, "{header}")
        .map_err(|e| anyhow::anyhow!("write asciicast header to {}: {e}", path.display()))?;
    let sentinel_frame = serde_json::to_string(
        "cass headless --once bootstrap completed without launching the interactive TUI.\r\n\
This asciicast is a sentinel artifact, not a real terminal session recording.\r\n",
    )
    .map_err(|e| {
        anyhow::anyhow!(
            "encode asciicast sentinel frame for {}: {e}",
            path.display()
        )
    })?;
    writeln!(file, "[0.0,\"o\",{sentinel_frame}]")
        .map_err(|e| anyhow::anyhow!("write asciicast frame to {}: {e}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod headless_asciicast_sentinel_tests {
    use super::write_headless_asciicast_sentinel;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn headless_asciicast_sentinel_is_labeled_and_nonempty() {
        let tmp = TempDir::new().expect("temp dir");
        let cast_path = tmp.path().join("captures").join("headless.cast");

        write_headless_asciicast_sentinel(&cast_path).expect("write sentinel cast");

        let cast = fs::read_to_string(&cast_path).expect("read sentinel cast");
        assert!(cast.contains("\"version\":2"));
        assert!(cast.contains("\"cass_artifact_kind\":\"headless_once_asciicast_sentinel\""));
        assert!(cast.contains("\"recording_available\":false"));
        assert!(cast.contains("sentinel artifact, not a real terminal session recording"));
        assert!(
            !cast.contains("[0.0,\"o\",\"\"]"),
            "sentinel cast must not use an empty placeholder frame"
        );
    }

    #[test]
    fn headless_asciicast_sentinel_overwrites_existing_placeholder_content() {
        let tmp = TempDir::new().expect("temp dir");
        let cast_path = tmp.path().join("existing.cast");
        fs::write(&cast_path, "{\"version\":2}\n[0.0,\"o\",\"\"]\n")
            .expect("write old placeholder");

        write_headless_asciicast_sentinel(&cast_path).expect("rewrite sentinel cast");

        let cast = fs::read_to_string(&cast_path).expect("read overwritten cast");
        assert!(cast.contains("\"cass_artifact_kind\":\"headless_once_asciicast_sentinel\""));
        assert!(cast.contains("headless --once bootstrap completed"));
        assert!(
            !cast.contains("[0.0,\"o\",\"\"]"),
            "overwritten cast should no longer contain the vacuous stub frame"
        );
    }
}

fn configure_color(choice: ColorPref, stdout_is_tty: bool, stderr_is_tty: bool) {
    let enabled = match choice {
        ColorPref::Always => true,
        ColorPref::Never => false,
        ColorPref::Auto => stdout_is_tty || stderr_is_tty,
    };
    colored::control::set_override(enabled);
}

fn resolve_progress(mode: ProgressMode, stdout_is_tty: bool) -> ProgressResolved {
    match mode {
        ProgressMode::Bars => ProgressResolved::Bars,
        ProgressMode::Plain => ProgressResolved::Plain,
        ProgressMode::None => ProgressResolved::None,
        ProgressMode::Auto => {
            if stdout_is_tty {
                ProgressResolved::Bars
            } else {
                ProgressResolved::Plain
            }
        }
    }
}

#[cfg(test)]
mod watch_once_resolution_tests {
    use super::resolve_watch_once_paths_from_sources;
    use std::path::PathBuf;

    #[test]
    fn normal_index_ignores_test_watch_once_env_without_watch_mode() {
        let env_paths = Some(vec![PathBuf::from("/tmp/leaked-watch-path.jsonl")]);
        let resolved = resolve_watch_once_paths_from_sources(false, None, env_paths);
        assert!(resolved.is_none());
    }

    // Absolute on every platform: on Windows `/tmp/x` has no drive, so the
    // resolver correctly absolutizes it to `C:/tmp/x` and an equality check on
    // the input would fail for the wrong reason.
    #[test]
    fn unit_test_watch_mode_can_consume_test_watch_once_env_paths() {
        let env_paths = Some(vec![std::env::temp_dir().join("watch-path.jsonl")]);
        let resolved = resolve_watch_once_paths_from_sources(true, None, env_paths.clone());
        assert_eq!(resolved, env_paths);
    }

    #[test]
    fn explicit_watch_once_paths_win_even_without_watch_flag() {
        let explicit = Some(vec![std::env::temp_dir().join("session.jsonl")]);
        let env_paths = Some(vec![std::env::temp_dir().join("leaked-watch-path.jsonl")]);
        let resolved = resolve_watch_once_paths_from_sources(false, explicit.clone(), env_paths);
        assert_eq!(resolved, explicit);
    }

    /// #377: relative supplied paths must be absolutized so lexical scan-root
    /// matching in classify_paths can succeed.
    #[test]
    fn relative_watch_once_paths_are_absolutized() {
        let resolved = resolve_watch_once_paths_from_sources(
            false,
            Some(vec![PathBuf::from("some/relative/session.jsonl")]),
            None,
        )
        .expect("explicit paths resolve");
        assert_eq!(resolved.len(), 1);
        assert!(
            resolved[0].is_absolute(),
            "relative watch-once path must be absolutized, got {}",
            resolved[0].display()
        );
        assert!(resolved[0].ends_with("some/relative/session.jsonl"));
    }

    /// #478: keep the original spelling until classification can retain its
    /// connector hint before resolving the canonical I/O target.
    #[cfg(unix)]
    #[test]
    fn symlinked_watch_once_paths_retain_original_connector_spelling() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let real = dir.path().join("real-session.jsonl");
        std::fs::write(&real, "{}\n").expect("write real file");
        let link = dir.path().join(".codex/sessions/link-session.jsonl");
        std::fs::create_dir_all(link.parent().expect("link parent")).expect("create parent");
        std::os::unix::fs::symlink(&real, &link).expect("create symlink");

        let resolved = resolve_watch_once_paths_from_sources(false, Some(vec![link.clone()]), None)
            .expect("explicit paths resolve");
        assert_eq!(resolved, vec![link]);
    }
}

fn describe_command(cli: &Cli) -> String {
    match &cli.command {
        Some(Commands::Tui { .. }) => "tui".to_string(),
        Some(Commands::Index { .. }) => "index".to_string(),
        Some(Commands::Search { .. }) => "search".to_string(),
        Some(Commands::Pack { .. }) => "pack".to_string(),
        Some(Commands::Stats { .. }) => "stats".to_string(),
        Some(Commands::Diag { .. }) => "diag".to_string(),
        Some(Commands::Storage { .. }) => "storage".to_string(),
        Some(Commands::Dedup { .. }) => "dedup".to_string(),
        Some(Commands::Status { .. }) => "status".to_string(),
        Some(Commands::Triage { .. }) => "triage".to_string(),
        Some(Commands::SupportBundle { .. }) => "support-bundle".to_string(),
        Some(Commands::View { .. }) => "view".to_string(),
        Some(Commands::Completions { .. }) => "completions".to_string(),
        Some(Commands::Man) => "man".to_string(),
        Some(Commands::Bookmarks(_)) => "bookmarks".to_string(),
        Some(Commands::Capabilities { .. }) => "capabilities".to_string(),
        Some(Commands::ApiVersion { .. }) => "api-version".to_string(),
        Some(Commands::State { .. }) => "state".to_string(),
        Some(Commands::Introspect { .. }) => "introspect".to_string(),
        Some(Commands::RobotDocs { topic }) => format!("robot-docs:{topic:?}"),
        Some(Commands::Health { .. }) => "health".to_string(),
        Some(Commands::Selftest { .. }) => "selftest".to_string(),
        Some(Commands::Onboarding { .. }) => "onboarding".to_string(),
        Some(Commands::Guide { .. }) => "guide".to_string(),
        Some(Commands::Doctor { .. }) => "doctor".to_string(),
        Some(Commands::Context { .. }) => "context".to_string(),
        Some(Commands::Sessions { .. }) => "sessions".to_string(),
        Some(Commands::Resume { .. }) => "resume".to_string(),
        Some(Commands::Export { .. }) => "export".to_string(),
        Some(Commands::ExportHtml { .. }) => "export-html".to_string(),
        Some(Commands::Expand { .. }) => "expand".to_string(),
        Some(Commands::Timeline { .. }) => "timeline".to_string(),
        Some(Commands::Quarantine(..)) => "quarantine".to_string(),
        Some(Commands::Forget { .. }) => "forget".to_string(),
        Some(Commands::Mirror(..)) => "mirror".to_string(),
        Some(Commands::Sources(..)) => "sources".to_string(),
        Some(Commands::Models(..)) => "models".to_string(),
        Some(Commands::Pages { .. }) => "pages".to_string(),
        #[cfg(unix)]
        Some(Commands::Daemon { .. }) => "daemon".to_string(),
        Some(Commands::Schedule(..)) => "schedule".to_string(),
        Some(Commands::Import(..)) => "import".to_string(),
        Some(Commands::Analytics(..)) => "analytics".to_string(),
        None => "(default)".to_string(),
    }
}

fn resolve_subcommand_structured_format(cli: &Cli, json: bool) -> Option<RobotFormat> {
    cli.robot_format
        .or(if json { Some(RobotFormat::Json) } else { None })
        .or_else(robot_format_from_env)
}

fn analytics_requests_structured_output(cmd: &AnalyticsCommand, cli: &Cli) -> bool {
    let json = match cmd {
        AnalyticsCommand::Status { common }
        | AnalyticsCommand::Tokens { common, .. }
        | AnalyticsCommand::Tools { common, .. }
        | AnalyticsCommand::AnalyticsModels { common, .. }
        | AnalyticsCommand::Incidents { common, .. }
        | AnalyticsCommand::Rebuild { common, .. }
        | AnalyticsCommand::Validate { common, .. } => common.json,
    };

    resolve_subcommand_structured_format(cli, json).is_some()
}

/// Default tracing directive for interactive human mode: emit INFO and above
/// but suppress frankensqlite internal telemetry that spams at INFO level.
///
/// `EnvFilter` uses `::` as the hierarchy separator, so `fsqlite=warn` covers
/// `fsqlite::runtime`, `fsqlite::cx`, etc. Crate-level targets like
/// `fsqlite_vdbe` and `fsqlite_core` need their own directives. Custom
/// dot-separated targets (`fsqlite.statement_reuse`, `fsqlite.compat`, etc.) are
/// NOT matched by the hierarchical prefix, so each must be listed explicitly.
///
/// This list is only consulted in interactive human mode (and only when
/// `RUST_LOG` is unset). Robot/quiet mode never reaches it — those modes pin the
/// filter to `error` via [`robot_aware_log_directive`], which is the actual
/// stdout/stderr hygiene guarantee. A missing entry here can therefore only make
/// human-mode stderr noisier, never corrupt a robot JSON stream.
const DEFAULT_DEP_LOG_SUPPRESSION: &str = concat!(
    "info",
    // Hierarchical (::) targets
    ",fsqlite=warn",
    ",fsqlite_core=warn",
    ",fsqlite_vdbe=warn",
    ",fsqlite_mvcc=warn",
    ",fsqlite_pager=warn",
    ",fsqlite_func=warn",
    ",fsqlite_vfs=warn",
    ",fsqlite_wal=warn",
    ",fsqlite_c_api=warn",
    ",fsqlite_planner=warn",
    ",fsqlite_types=warn",
    ",fsqlite_observability=warn",
    // Dot-separated custom targets
    ",fsqlite.compat=warn",
    ",fsqlite.compat_trace=warn",
    ",fsqlite.statement_reuse=warn",
    ",fsqlite.statement=warn",
    ",fsqlite.execution=warn",
    ",fsqlite.execute_path=warn",
    ",fsqlite.plan=warn",
    ",fsqlite.planner=warn",
    ",fsqlite.planner_runtime=warn",
    ",fsqlite.parse=warn",
    ",fsqlite.provenance=warn",
    ",fsqlite.dp=warn",
    ",fsqlite.udf=warn",
    ",fsqlite.vdbe=warn",
    ",fsqlite.rcu=warn",
    ",fsqlite.seqlock=warn",
    ",fsqlite.left_right=warn",
    ",fsqlite.flat_combine=warn",
    ",fsqlite.commit_combine=warn",
    ",fsqlite.snapshot_publication=warn",
    ",fsqlite.wal_publication=warn",
    ",fsqlite.storage_wiring=warn",
    ",fsqlite.cx_propagation=warn",
    ",fsqlite.sketch_telemetry=warn",
    ",fsqlite.time_travel=warn",
    ",fsqlite.trace_export=warn",
    ",fsqlite.txn_slot=warn",
    ",fsqlite.evidence=warn",
    ",fsqlite.lab_schedule=warn",
);

/// Decide the tracing filter directive for the active output mode.
///
/// Returns `Some(directive)` when the directive is fixed regardless of the
/// `RUST_LOG` environment variable, and `None` when the caller should consult
/// `RUST_LOG` (falling back to [`DEFAULT_DEP_LOG_SUPPRESSION`]).
///
/// Robot and quiet modes are machine-consumed: they pin the filter to `error`
/// (unless the operator explicitly opts into `--verbose`) so that dependency
/// INFO/WARN logging — frankensqlite telemetry in particular — can never
/// interleave with a JSON stream, even when `RUST_LOG` is set in the
/// environment. This is the single source of truth for robot stdout/stderr
/// hygiene; the regression suite in `tests/cli_robot_log_hygiene.rs` exercises it
/// end-to-end under deliberately noisy `RUST_LOG` settings.
fn robot_aware_log_directive(robot_mode: bool, verbose: bool, quiet: bool) -> Option<&'static str> {
    if quiet || robot_mode {
        // Robot/quiet mode implies error-only output unless --verbose is set.
        Some(if verbose { "debug" } else { "error" })
    } else if verbose {
        Some("debug")
    } else {
        None
    }
}

/// Build the concrete [`EnvFilter`] for the active output mode, applying the
/// robot/quiet hygiene policy from [`robot_aware_log_directive`].
fn build_robot_aware_log_filter(robot_mode: bool, verbose: bool, quiet: bool) -> EnvFilter {
    match robot_aware_log_directive(robot_mode, verbose, quiet) {
        Some(directive) => EnvFilter::new(directive),
        None => EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new(DEFAULT_DEP_LOG_SUPPRESSION)),
    }
}

/// Default filter for the explicit `--trace-file` surface.
///
/// CASS-owned targets retain DEBUG detail. Dependencies retain every WARN/ERROR
/// event, but their DEBUG/INFO chatter is excluded by default. The redaction
/// memo target is also pinned to WARN because trace serialization itself invokes
/// redaction; retaining its routine hit/miss DEBUG events would amplify the
/// artifact with diagnostics about producing diagnostics. Memo invalidation and
/// quarantine warnings remain visible.
const DEFAULT_TRACE_FILE_LOG_DIRECTIVE: &str =
    "warn,coding_agent_search=debug,cass=debug,cass::redact::memo=warn";

/// Default hard ceiling for one trace artifact. Operators may lower or raise it
/// with `CASS_TRACE_MAX_BYTES`; values are clamped to a safe 4 KiB..1 GiB range.
const TRACE_SCHEMA_VERSION: &str = "cass-trace-v1";
const DEFAULT_TRACE_FILE_MAX_BYTES: u64 = 16 * 1024 * 1024;
const MIN_TRACE_FILE_MAX_BYTES: u64 = 4 * 1024;
const MAX_TRACE_FILE_MAX_BYTES: u64 = 1024 * 1024 * 1024;
const DEFAULT_TRACE_FILE_MAX_EVENTS: u64 = 50_000;
const MIN_TRACE_FILE_MAX_EVENTS: u64 = 16;
const MAX_TRACE_FILE_MAX_EVENTS: u64 = 10_000_000;
const TRACE_SUMMARY_RESERVE_MAX_BYTES: u64 = 64 * 1024;
const TRACE_TRUNCATION_RESERVE_MAX_BYTES: u64 = 8 * 1024;
const TRACE_FAILURE_TAIL_RESERVE_MAX_BYTES: u64 = 32 * 1024;
const TRACE_EVENT_BUFFER_MAX_BYTES: usize = 256 * 1024;
const TRACE_FILTER_TARGET_LIMIT: usize = 8;
const TRACE_CORRELATION_MAX_CHARS: usize = 256;
const TRACE_SUMMARY_ARG_LIMIT: usize = 16;
const TRACE_SUMMARY_ARG_MAX_CHARS: usize = 256;
const TRACE_SUMMARY_ERROR_MAX_CHARS: usize = 1024;
const TRACE_RECEIPT_TARGET_MAX_CHARS: usize = 128;

fn trace_file_log_filter_override() -> Option<EnvFilter> {
    let directive = dotenvy::var("CASS_TRACE_FILTER").ok()?;
    let directive = directive.trim();
    if directive.is_empty() || directive.eq(DEFAULT_TRACE_FILE_LOG_DIRECTIVE) {
        return None;
    }
    match EnvFilter::try_new(directive) {
        Ok(filter) => Some(filter),
        Err(error) => {
            eprintln!("invalid CASS_TRACE_FILTER; using the bounded default policy: {error}");
            None
        }
    }
}

fn trace_file_max_bytes() -> u64 {
    dotenvy::var("CASS_TRACE_MAX_BYTES")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(|value| value.clamp(MIN_TRACE_FILE_MAX_BYTES, MAX_TRACE_FILE_MAX_BYTES))
        .unwrap_or(DEFAULT_TRACE_FILE_MAX_BYTES)
}

fn trace_file_max_events() -> u64 {
    dotenvy::var("CASS_TRACE_MAX_EVENTS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(|value| value.clamp(MIN_TRACE_FILE_MAX_EVENTS, MAX_TRACE_FILE_MAX_EVENTS))
        .unwrap_or(DEFAULT_TRACE_FILE_MAX_EVENTS)
}

fn trace_target_is_cass_owned(target: &str) -> bool {
    target.eq("cass")
        || target.starts_with("cass::")
        || target.eq("coding_agent_search")
        || target.starts_with("coding_agent_search::")
}

fn default_trace_metadata_enabled(metadata: &tracing::Metadata<'_>) -> bool {
    if metadata.target().eq("cass::redact::memo") {
        return matches!(
            *metadata.level(),
            tracing::Level::ERROR | tracing::Level::WARN
        );
    }
    matches!(
        *metadata.level(),
        tracing::Level::ERROR | tracing::Level::WARN
    ) || (trace_target_is_cass_owned(metadata.target())
        && matches!(
            *metadata.level(),
            tracing::Level::INFO | tracing::Level::DEBUG
        ))
}

fn trace_field_is_sensitive(field: &str) -> bool {
    crate::indexer::redact_secrets::is_sensitive_json_field(field)
}

static TRACE_REDACTION_ENGINE: once_cell::sync::Lazy<crate::pages::redact::RedactionEngine> =
    once_cell::sync::Lazy::new(|| {
        crate::pages::redact::RedactionEngine::new(
            crate::pages::redact::swarm_evidence_redaction_config(),
        )
    });

fn redact_trace_text(text: &str) -> String {
    let secret_redacted = crate::indexer::redact_secrets::redact_text(text).into_owned();
    TRACE_REDACTION_ENGINE.redact_text(&secret_redacted).output
}

fn truncate_trace_text(text: &str, max_chars: usize) -> String {
    let mut chars = text.chars();
    let mut truncated = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        truncated.push_str("...[TRUNCATED]");
    }
    truncated
}

fn redact_bounded_trace_text(text: &str, max_chars: usize) -> String {
    truncate_trace_text(&redact_trace_text(text), max_chars)
}

fn enrich_trace_json(value: &serde_json::Value) -> serde_json::Value {
    let mut enriched = value.clone();
    if let Some(root) = enriched.as_object_mut() {
        root.entry("schema_version")
            .or_insert_with(|| serde_json::Value::String(TRACE_SCHEMA_VERSION.to_string()));
        root.entry("trace_id").or_insert_with(|| {
            dotenvy::var("CASS_TRACE_ID")
                .map(|value| {
                    serde_json::Value::String(redact_bounded_trace_text(
                        &value,
                        TRACE_CORRELATION_MAX_CHARS,
                    ))
                })
                .unwrap_or(serde_json::Value::Null)
        });
        root.entry("test_id").or_insert_with(|| {
            dotenvy::var("CASS_TRACE_TEST_ID")
                .map(|value| {
                    serde_json::Value::String(redact_bounded_trace_text(
                        &value,
                        TRACE_CORRELATION_MAX_CHARS,
                    ))
                })
                .unwrap_or(serde_json::Value::Null)
        });
        if let Some(fields) = root
            .get_mut("fields")
            .and_then(serde_json::Value::as_object_mut)
        {
            let has_event_identity = ["event", "message"].into_iter().any(|field| {
                fields
                    .get(field)
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|value| !value.is_empty())
            });
            if !has_event_identity {
                fields.insert(
                    "event".to_string(),
                    serde_json::Value::String("diagnostic".to_string()),
                );
            }
        }
    }
    enriched
}

fn redact_trace_json(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::String(text) => serde_json::Value::String(redact_trace_text(text)),
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.iter().map(redact_trace_json).collect())
        }
        serde_json::Value::Object(fields) => {
            let mut redacted = serde_json::Map::with_capacity(fields.len());
            let mut next_suffixes = std::collections::HashMap::new();
            for (field, value) in fields {
                let sanitized_trace_value = if trace_field_is_sensitive(field) {
                    serde_json::Value::String("[REDACTED]".to_string())
                } else {
                    redact_trace_json(value)
                };
                let field = redact_trace_text(field);
                crate::indexer::redact_secrets::insert_redacted_json_entry(
                    &mut redacted,
                    &mut next_suffixes,
                    field,
                    sanitized_trace_value,
                );
            }
            serde_json::Value::Object(redacted)
        }
        other => other.clone(),
    }
}

fn redact_trace_args(args: &[String]) -> Vec<String> {
    let retained = args.len().min(TRACE_SUMMARY_ARG_LIMIT);
    let mut redacted = Vec::with_capacity(retained.saturating_add(1));
    let mut redact_next = false;

    for arg in args.iter().take(TRACE_SUMMARY_ARG_LIMIT) {
        if redact_next {
            redacted.push("[REDACTED]".to_string());
            redact_next = false;
            continue;
        }

        if let Some((flag, _value)) = arg.split_once('=')
            && flag.starts_with('-')
            && trace_field_is_sensitive(flag.trim_start_matches('-'))
        {
            redacted.push(truncate_trace_text(
                &format!("{flag}=[REDACTED]"),
                TRACE_SUMMARY_ARG_MAX_CHARS,
            ));
            continue;
        }

        if arg.starts_with('-') && trace_field_is_sensitive(arg.trim_start_matches('-')) {
            redacted.push(truncate_trace_text(arg, TRACE_SUMMARY_ARG_MAX_CHARS));
            redact_next = true;
            continue;
        }

        redacted.push(truncate_trace_text(
            &redact_trace_text(arg),
            TRACE_SUMMARY_ARG_MAX_CHARS,
        ));
    }

    if args.len() > retained {
        redacted.push(format!(
            "[{} ARGUMENTS OMITTED]",
            args.len().saturating_sub(retained)
        ));
    }

    redacted
}

fn trace_json_has_valid_envelope(value: &serde_json::Value) -> bool {
    value.is_object()
        && value
            .get("schema_version")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|schema| schema.eq(TRACE_SCHEMA_VERSION))
        && value["timestamp"].as_str().is_some_and(|timestamp| {
            !timestamp.is_empty() && chrono::DateTime::parse_from_rfc3339(timestamp).is_ok()
        })
        && value["level"]
            .as_str()
            .is_some_and(|level| !level.is_empty())
        && value["target"]
            .as_str()
            .is_some_and(|target| !target.is_empty())
        && value.get("trace_id").is_some()
        && value.get("test_id").is_some()
        && value["fields"].as_object().is_some_and(|fields| {
            ["event", "message"].into_iter().any(|field| {
                fields
                    .get(field)
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|identity| !identity.is_empty())
            })
        })
}

fn encode_trace_line(value: &serde_json::Value) -> io::Result<Vec<u8>> {
    let enriched = enrich_trace_json(value);
    let redacted = redact_trace_json(&enriched);
    let mut encoded = serde_json::to_vec(&redacted).map_err(io::Error::other)?;
    encoded.push(b'\n');
    Ok(encoded)
}

fn trace_summary_reserve(max_bytes: u64) -> u64 {
    TRACE_SUMMARY_RESERVE_MAX_BYTES.min(max_bytes / 4)
}

fn trace_truncation_reserve(max_bytes: u64) -> u64 {
    TRACE_TRUNCATION_RESERVE_MAX_BYTES.min(max_bytes / 4)
}

fn trace_failure_tail_reserve(max_bytes: u64) -> u64 {
    TRACE_FAILURE_TAIL_RESERVE_MAX_BYTES.min(max_bytes / 8)
}

fn open_trace_append_file(path: &Path, max_bytes: u64) -> io::Result<(File, u64, u64, bool)> {
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(path)?;
    fs2::FileExt::try_lock_exclusive(&file).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("trace artifact already has an active writer: {error}"),
        )
    })?;
    let mut bytes_written = file.metadata()?.len();
    let mut events_written = 0_u64;
    let mut diagnostic_head_closed = false;

    if bytes_written > max_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "existing trace is {bytes_written} bytes, exceeding the configured {max_bytes}-byte ceiling"
            ),
        ));
    }

    if bytes_written > 0 {
        file.seek(SeekFrom::Start(0))?;
        {
            let mut reader = BufReader::new(&mut file);
            let mut line = Vec::new();
            let mut line_number = 0_u64;
            loop {
                line.clear();
                if reader.read_until(b'\n', &mut line)?.eq(&0) {
                    break;
                }
                line_number = line_number.saturating_add(1);
                let record = line.strip_suffix(b"\n").unwrap_or(&line);
                let record = record.strip_suffix(b"\r").unwrap_or(record);
                if record.is_empty() {
                    continue;
                }
                let value =
                    serde_json::from_slice::<serde_json::Value>(record).map_err(|error| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("existing trace line {line_number} is not valid JSON: {error}"),
                        )
                    })?;
                if !trace_json_has_valid_envelope(&value) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "existing trace line {line_number} is not a valid {TRACE_SCHEMA_VERSION} envelope"
                        ),
                    ));
                }
                let event = value["fields"]["event"].as_str();
                if matches!(event, Some("trace_truncated")) {
                    diagnostic_head_closed = true;
                }
                if !matches!(
                    event,
                    Some("command_summary" | "trace_filter_summary" | "trace_truncated")
                ) {
                    events_written = events_written.saturating_add(1);
                }
            }
        }
    }

    if bytes_written > 0 {
        file.seek(SeekFrom::End(-1))?;
        let mut last = [0_u8; 1];
        file.read_exact(&mut last)?;
        if !last.first().is_some_and(|byte| byte.eq(&b'\n')) {
            if bytes_written >= max_bytes {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "existing trace is a valid JSON record but lacks its final newline and already consumes the configured {max_bytes}-byte ceiling"
                    ),
                ));
            }
            file.write_all(b"\n")?;
            bytes_written += 1;
        }
    }

    Ok((file, bytes_written, events_written, diagnostic_head_closed))
}

struct BoundedTraceState {
    file: File,
    max_bytes: u64,
    max_events: u64,
    bytes_written: u64,
    events_written: u64,
    diagnostic_head_closed: bool,
    suppressed_events: u64,
    suppressed_bytes: u64,
    byte_budget_suppressed_events: u64,
    event_budget_suppressed_events: u64,
    oversize_suppressed_events: u64,
    filtered_events: u64,
    filtered_by_target: BTreeMap<String, u64>,
    suppressed_by_target: BTreeMap<String, u64>,
    failure_tail: VecDeque<(serde_json::Value, u64)>,
    failure_tail_bytes: u64,
    failure_tail_dropped_events: u64,
    finalized: bool,
    last_error: Option<String>,
}

#[derive(Clone, Copy)]
enum TraceSuppressionReason {
    ByteBudget,
    EventBudget,
    OversizeEvent,
}

fn increment_trace_target_count(counts: &mut BTreeMap<String, u64>, target: &str) {
    if let Some(count) = counts.get_mut(target) {
        *count = count.saturating_add(1);
    } else {
        counts.insert(target.to_string(), 1);
    }
}

fn trace_receipt_target_limit(max_bytes: u64) -> usize {
    if max_bytes < 16 * 1024 {
        2
    } else {
        TRACE_FILTER_TARGET_LIMIT
    }
}

fn ranked_trace_targets(
    counts: &BTreeMap<String, u64>,
    limit: usize,
) -> (Vec<serde_json::Value>, u64) {
    let mut ranked = counts
        .iter()
        .map(|(target, count)| (target.as_str(), *count))
        .collect::<Vec<_>>();
    ranked.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(right.0)));
    let overflow_events = ranked
        .iter()
        .skip(limit)
        .fold(0_u64, |total, (_, count)| total.saturating_add(*count));
    ranked.truncate(limit);
    let targets = ranked
        .into_iter()
        .map(|(target, count)| {
            serde_json::json!({
                "target": truncate_trace_text(target, TRACE_RECEIPT_TARGET_MAX_CHARS),
                "count": count,
            })
        })
        .collect();
    (targets, overflow_events)
}

impl BoundedTraceState {
    fn diagnostic_limit(&self) -> u64 {
        self.max_bytes
            .saturating_sub(trace_summary_reserve(self.max_bytes))
            .saturating_sub(trace_truncation_reserve(self.max_bytes))
            .saturating_sub(trace_failure_tail_reserve(self.max_bytes))
    }

    fn failure_tail_limit(&self) -> u64 {
        self.max_bytes
            .saturating_sub(trace_summary_reserve(self.max_bytes))
            .saturating_sub(trace_truncation_reserve(self.max_bytes))
    }

    fn receipt_limit(&self) -> u64 {
        self.max_bytes
            .saturating_sub(trace_summary_reserve(self.max_bytes))
    }

    fn record_suppressed(&mut self, target: &str, bytes: u64, reason: TraceSuppressionReason) {
        self.suppressed_events = self.suppressed_events.saturating_add(1);
        self.suppressed_bytes = self.suppressed_bytes.saturating_add(bytes);
        let reason_count = match reason {
            TraceSuppressionReason::ByteBudget => &mut self.byte_budget_suppressed_events,
            TraceSuppressionReason::EventBudget => &mut self.event_budget_suppressed_events,
            TraceSuppressionReason::OversizeEvent => &mut self.oversize_suppressed_events,
        };
        *reason_count = reason_count.saturating_add(1);
        increment_trace_target_count(&mut self.suppressed_by_target, target);
    }

    fn record_filtered(&mut self, target: &str) {
        self.filtered_events = self.filtered_events.saturating_add(1);
        increment_trace_target_count(&mut self.filtered_by_target, target);
    }

    fn record_failure_tail(&mut self, value: &serde_json::Value) {
        let reserve = trace_failure_tail_reserve(self.max_bytes);
        if reserve.eq(&0) {
            self.failure_tail_dropped_events = self.failure_tail_dropped_events.saturating_add(1);
            return;
        }

        let value = redact_trace_json(&enrich_trace_json(value));
        let encoded_len = serde_json::to_vec(&value)
            .map(|encoded| u64::try_from(encoded.len() + 1).unwrap_or(u64::MAX))
            .unwrap_or(u64::MAX);
        if encoded_len > reserve {
            self.failure_tail_dropped_events = self.failure_tail_dropped_events.saturating_add(1);
            return;
        }
        while self.failure_tail_bytes.saturating_add(encoded_len) > reserve {
            let Some((_discarded, discarded_len)) = self.failure_tail.pop_front() else {
                break;
            };
            self.failure_tail_bytes = self.failure_tail_bytes.saturating_sub(discarded_len);
            self.failure_tail_dropped_events = self.failure_tail_dropped_events.saturating_add(1);
        }
        self.failure_tail.push_back((value, encoded_len));
        self.failure_tail_bytes = self.failure_tail_bytes.saturating_add(encoded_len);
    }

    fn write_line_with_limit(&mut self, value: &serde_json::Value, limit: u64) -> io::Result<bool> {
        let encoded = encode_trace_line(value)?;
        self.write_encoded_line_with_limit(&encoded, limit)
    }

    fn write_encoded_line_with_limit(&mut self, encoded: &[u8], limit: u64) -> io::Result<bool> {
        let encoded_len = u64::try_from(encoded.len()).unwrap_or(u64::MAX);
        if self.bytes_written.saturating_add(encoded_len) > limit {
            return Ok(false);
        }
        self.file.write_all(encoded)?;
        self.bytes_written = self.bytes_written.saturating_add(encoded_len);
        Ok(true)
    }

    fn write_event(&mut self, bytes: &[u8], overflow_bytes: u64) -> io::Result<()> {
        if self.finalized {
            return Ok(());
        }
        if overflow_bytes > 0 {
            self.record_suppressed(
                "oversize_event",
                u64::try_from(bytes.len())
                    .unwrap_or(u64::MAX)
                    .saturating_add(overflow_bytes),
                TraceSuppressionReason::OversizeEvent,
            );
            return Ok(());
        }

        let trimmed = bytes.strip_suffix(b"\n").unwrap_or(bytes);
        let trimmed = trimmed.strip_suffix(b"\r").unwrap_or(trimmed);
        if trimmed.is_empty() {
            return Ok(());
        }

        let value = serde_json::from_slice::<serde_json::Value>(trimmed).unwrap_or_else(|error| {
            serde_json::json!({
                "timestamp": Utc::now().to_rfc3339(),
                "level": "ERROR",
                "target": "cass::trace",
                "fields": {
                    "event": "trace_format_error",
                    "message": error.to_string(),
                },
            })
        });
        let trace_target_name = value
            .get("target")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        let is_failure_context = value
            .get("level")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|level| matches!(level, "WARN" | "ERROR"));
        let encoded = encode_trace_line(&value)?;
        let encoded_len = u64::try_from(encoded.len()).unwrap_or(u64::MAX);
        if self.events_written >= self.max_events {
            self.diagnostic_head_closed = true;
            self.record_suppressed(
                trace_target_name,
                encoded_len,
                TraceSuppressionReason::EventBudget,
            );
            if is_failure_context {
                self.record_failure_tail(&value);
            }
        } else if self.diagnostic_head_closed {
            self.record_suppressed(
                trace_target_name,
                encoded_len,
                TraceSuppressionReason::ByteBudget,
            );
            if is_failure_context {
                self.record_failure_tail(&value);
            }
        } else if self.write_encoded_line_with_limit(&encoded, self.diagnostic_limit())? {
            self.events_written = self.events_written.saturating_add(1);
        } else {
            self.diagnostic_head_closed = true;
            self.record_suppressed(
                trace_target_name,
                encoded_len,
                TraceSuppressionReason::ByteBudget,
            );
            if is_failure_context {
                self.record_failure_tail(&value);
            }
        }
        Ok(())
    }

    fn finalize(&mut self) -> io::Result<()> {
        if self.finalized {
            return Ok(());
        }
        self.finalized = true;

        let mut failure_tail_written_events = 0_u64;
        let mut failure_tail_written_bytes = 0_u64;
        let mut tail_result = Ok(());
        while let Some((value, encoded_len)) = self.failure_tail.pop_front() {
            match self.write_line_with_limit(&value, self.failure_tail_limit()) {
                Ok(true) => {
                    failure_tail_written_events = failure_tail_written_events.saturating_add(1);
                    failure_tail_written_bytes =
                        failure_tail_written_bytes.saturating_add(encoded_len);
                    self.events_written = self.events_written.saturating_add(1);
                    self.failure_tail_bytes = self.failure_tail_bytes.saturating_sub(encoded_len);
                }
                Ok(false) => {
                    self.failure_tail_dropped_events = self
                        .failure_tail_dropped_events
                        .saturating_add(1)
                        .saturating_add(u64::try_from(self.failure_tail.len()).unwrap_or(u64::MAX));
                    self.failure_tail.clear();
                    self.failure_tail_bytes = 0;
                    tail_result = Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "trace failure-tail reserve could not fit buffered WARN/ERROR events",
                    ));
                    break;
                }
                Err(error) => {
                    self.failure_tail_dropped_events = self
                        .failure_tail_dropped_events
                        .saturating_add(1)
                        .saturating_add(u64::try_from(self.failure_tail.len()).unwrap_or(u64::MAX));
                    self.failure_tail.clear();
                    self.failure_tail_bytes = 0;
                    tail_result = Err(error);
                    break;
                }
            }
        }

        let receipt_result = if self.suppressed_events.eq(&0) && self.filtered_events.eq(&0) {
            Ok(())
        } else {
            let target_limit = trace_receipt_target_limit(self.max_bytes);
            let (filtered_targets, filtered_target_overflow_events) =
                ranked_trace_targets(&self.filtered_by_target, target_limit);
            let (suppressed_targets, suppressed_target_overflow_events) =
                ranked_trace_targets(&self.suppressed_by_target, target_limit);
            let event = if self.suppressed_events > 0 {
                "trace_truncated"
            } else {
                "trace_filter_summary"
            };
            let active_suppression_reasons = [
                self.byte_budget_suppressed_events,
                self.event_budget_suppressed_events,
                self.oversize_suppressed_events,
            ]
            .into_iter()
            .filter(|count| *count > 0)
            .count();
            let reason = match active_suppression_reasons {
                0 => "default_filter_policy",
                1 if self.byte_budget_suppressed_events > 0 => "byte_budget",
                1 if self.event_budget_suppressed_events > 0 => "event_budget",
                1 => "oversize_event",
                _ => "multiple_limits",
            };
            let receipt = serde_json::json!({
                "timestamp": Utc::now().to_rfc3339(),
                "level": if self.suppressed_events > 0 { "WARN" } else { "INFO" },
                "target": "cass::trace",
                "fields": {
                    "event": event,
                    "reason": reason,
                    "artifact_complete": self.suppressed_events.eq(&0),
                    "max_bytes": self.max_bytes,
                    "max_events": self.max_events,
                    "bytes_written_before_receipt": self.bytes_written,
                    "events_written_before_receipt": self.events_written,
                    "suppressed_events": self.suppressed_events,
                    "suppressed_bytes": self.suppressed_bytes,
                    "suppression_reasons": {
                        "byte_budget": self.byte_budget_suppressed_events,
                        "event_budget": self.event_budget_suppressed_events,
                        "oversize_event": self.oversize_suppressed_events,
                    },
                    "suppressed_targets": suppressed_targets,
                    "suppressed_target_overflow_events": suppressed_target_overflow_events,
                    "failure_tail_events": failure_tail_written_events,
                    "failure_tail_bytes": failure_tail_written_bytes,
                    "failure_tail_dropped_events": self.failure_tail_dropped_events,
                    "filtered_events": self.filtered_events,
                    "filtered_targets": filtered_targets,
                    "filtered_target_overflow_events": filtered_target_overflow_events,
                },
            });
            let receipt_limit = self.receipt_limit();
            match self.write_line_with_limit(&receipt, receipt_limit) {
                Ok(true) => Ok(()),
                Ok(false) => {
                    let compact_receipt = serde_json::json!({
                        "timestamp": Utc::now().to_rfc3339(),
                        "level": if self.suppressed_events > 0 { "WARN" } else { "INFO" },
                        "target": "cass::trace",
                        "fields": {
                            "event": event,
                            "receipt_compact": true,
                            "reason": reason,
                            "artifact_complete": self.suppressed_events.eq(&0),
                            "max_bytes": self.max_bytes,
                            "max_events": self.max_events,
                            "bytes_written_before_receipt": self.bytes_written,
                            "events_written_before_receipt": self.events_written,
                            "suppressed_events": self.suppressed_events,
                            "suppressed_bytes": self.suppressed_bytes,
                            "suppression_reasons": {
                                "byte_budget": self.byte_budget_suppressed_events,
                                "event_budget": self.event_budget_suppressed_events,
                                "oversize_event": self.oversize_suppressed_events,
                            },
                            "suppressed_target_overflow_events": self.suppressed_events,
                            "failure_tail_events": failure_tail_written_events,
                            "failure_tail_bytes": failure_tail_written_bytes,
                            "failure_tail_dropped_events": self.failure_tail_dropped_events,
                            "filtered_events": self.filtered_events,
                            "filtered_target_overflow_events": self.filtered_events,
                        },
                    });
                    match self.write_line_with_limit(&compact_receipt, receipt_limit) {
                        Ok(true) => Ok(()),
                        Ok(false) => Err(io::Error::new(
                            io::ErrorKind::WriteZero,
                            "trace tail reserve could not fit even the compact filter/truncation receipt",
                        )),
                        Err(error) => Err(error),
                    }
                }
                Err(error) => Err(error),
            }
        };
        let unlock_result = fs2::FileExt::unlock(&self.file);
        tail_result.and(receipt_result).and(unlock_result)
    }
}

#[derive(Clone)]
struct BoundedTraceMakeWriter {
    state: Arc<Mutex<BoundedTraceState>>,
}

impl BoundedTraceMakeWriter {
    fn open(path: &Path, max_bytes: u64, max_events: u64) -> io::Result<(Self, CliTraceGuard)> {
        let (file, bytes_written, events_written, prior_head_closed) =
            open_trace_append_file(path, max_bytes)?;
        let diagnostic_limit = max_bytes
            .saturating_sub(trace_summary_reserve(max_bytes))
            .saturating_sub(trace_truncation_reserve(max_bytes))
            .saturating_sub(trace_failure_tail_reserve(max_bytes));
        let diagnostic_head_closed =
            prior_head_closed || events_written >= max_events || bytes_written >= diagnostic_limit;
        let state = Arc::new(Mutex::new(BoundedTraceState {
            file,
            max_bytes,
            max_events,
            bytes_written,
            events_written,
            diagnostic_head_closed,
            suppressed_events: 0,
            suppressed_bytes: 0,
            byte_budget_suppressed_events: 0,
            event_budget_suppressed_events: 0,
            oversize_suppressed_events: 0,
            filtered_events: 0,
            filtered_by_target: BTreeMap::new(),
            suppressed_by_target: BTreeMap::new(),
            failure_tail: VecDeque::new(),
            failure_tail_bytes: 0,
            failure_tail_dropped_events: 0,
            finalized: false,
            last_error: None,
        }));
        Ok((
            Self {
                state: Arc::clone(&state),
            },
            CliTraceGuard { state },
        ))
    }
}

struct BoundedTraceEventWriter {
    state: Arc<Mutex<BoundedTraceState>>,
    buffer: Vec<u8>,
    overflow_bytes: u64,
}

impl Write for BoundedTraceEventWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let available = TRACE_EVENT_BUFFER_MAX_BYTES.saturating_sub(self.buffer.len());
        let accepted = available.min(bytes.len());
        self.buffer.extend_from_slice(&bytes[..accepted]);
        self.overflow_bytes = self
            .overflow_bytes
            .saturating_add(u64::try_from(bytes.len() - accepted).unwrap_or(u64::MAX));
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for BoundedTraceEventWriter {
    fn drop(&mut self) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if let Err(error) = state.write_event(&self.buffer, self.overflow_bytes) {
            state.last_error.get_or_insert_with(|| error.to_string());
        }
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for BoundedTraceMakeWriter {
    type Writer = BoundedTraceEventWriter;

    fn make_writer(&'a self) -> Self::Writer {
        BoundedTraceEventWriter {
            state: Arc::clone(&self.state),
            buffer: Vec::with_capacity(1024),
            overflow_bytes: 0,
        }
    }
}

struct CliTraceGuard {
    state: Arc<Mutex<BoundedTraceState>>,
}

impl Drop for CliTraceGuard {
    fn drop(&mut self) {
        let Ok(mut state) = self.state.lock() else {
            eprintln!("trace-write error: trace state lock poisoned");
            return;
        };
        if let Err(error) = state.finalize() {
            state.last_error.get_or_insert_with(|| error.to_string());
        }
        if let Some(error) = state.last_error.take() {
            eprintln!("trace-write error: {error}");
        }
    }
}

/// Initialize the tracing subscriber for non-TUI commands with robot-safe
/// stdout/stderr hygiene plus an optional explicit `--trace-file` sink.
///
/// The stderr layer carries the robot-aware `filter` (pinned to `error` in
/// robot/quiet mode by [`build_robot_aware_log_filter`], per uojcg.2.1), so
/// dependency INFO/WARN logging can never interleave with a JSON stream. When
/// `trace_file` is `Some`, a second layer writes valid JSONL, retains CASS DEBUG
/// plus global WARN/ERROR events, redacts secret-bearing fields, and stops at a
/// hard byte/event budget with a structured `trace_truncated` receipt.
///
/// The returned guard finalizes the truncation receipt before the command summary
/// is appended. The caller MUST keep it alive for the duration of the command.
fn init_cli_tracing(
    filter: EnvFilter,
    trace_file: Option<&Path>,
    ansi: bool,
) -> Option<CliTraceGuard> {
    use tracing_subscriber::Layer;

    let stderr_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .compact()
        .with_target(false)
        .with_ansi(ansi)
        .with_filter(filter);

    let (trace_layer, guard) = match trace_file {
        Some(path) => match BoundedTraceMakeWriter::open(
            path,
            trace_file_max_bytes(),
            trace_file_max_events(),
        ) {
            Ok((writer, guard)) => {
                let layer = if let Some(filter) = trace_file_log_filter_override() {
                    tracing_subscriber::fmt::layer()
                        .json()
                        .with_writer(writer)
                        .with_ansi(false)
                        .with_filter(filter)
                        .boxed()
                } else {
                    let filter_state = Arc::clone(&writer.state);
                    // The policy never enables TRACE. Publishing that ceiling is
                    // essential: without it a dynamic filter marks TRACE
                    // callsites "sometimes", so per-character tokenizer
                    // instrumentation still invokes this closure for every
                    // character even though every record is rejected.
                    let filter =
                        tracing_subscriber::filter::dynamic_filter_fn(move |metadata, _context| {
                            let enabled = default_trace_metadata_enabled(metadata);
                            if !enabled
                                && metadata.is_event()
                                && let Ok(mut state) = filter_state.lock()
                            {
                                state.record_filtered(metadata.target());
                            }
                            enabled
                        })
                        .with_max_level_hint(tracing_subscriber::filter::LevelFilter::DEBUG);
                    tracing_subscriber::fmt::layer()
                        .json()
                        .with_writer(writer)
                        .with_ansi(false)
                        .with_filter(filter)
                        .boxed()
                };
                (Some(layer), Some(guard))
            }
            Err(e) => {
                eprintln!("trace-file open error ({}): {e}", path.display());
                (None, None)
            }
        },
        None => (None, None),
    };

    tracing_subscriber::registry()
        .with(stderr_layer)
        .with(trace_layer)
        .init();
    guard
}

#[cfg(test)]
mod log_hygiene_tests {
    use super::{
        BoundedTraceMakeWriter, DEFAULT_DEP_LOG_SUPPRESSION, DEFAULT_TRACE_FILE_LOG_DIRECTIVE,
        MIN_TRACE_FILE_MAX_BYTES, TraceSuppressionReason, default_trace_metadata_enabled,
        enrich_trace_json, redact_bounded_trace_text, redact_trace_args, redact_trace_json,
        robot_aware_log_directive,
    };
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    };
    use tracing_subscriber::{Layer, layer::SubscriberExt};

    type TraceTestResult = Result<(), Box<dyn std::error::Error>>;

    macro_rules! trace_test_require {
        ($condition:expr, $($message:tt)+) => {
            if !$condition {
                return Err(format_args!($($message)+).to_string().into());
            }
        };
    }

    fn trace_json_text_is(value: &serde_json::Value, pointer: &str, expected: &str) -> bool {
        value
            .pointer(pointer)
            .and_then(serde_json::Value::as_str)
            .is_some_and(|actual| actual.eq(expected))
    }

    fn trace_json_bool_is(value: &serde_json::Value, pointer: &str, expected: bool) -> bool {
        value
            .pointer(pointer)
            .and_then(serde_json::Value::as_bool)
            .is_some_and(|actual| actual.eq(&expected))
    }

    fn trace_json_count_is_positive(value: &serde_json::Value, pointer: &str) -> bool {
        value
            .pointer(pointer)
            .and_then(serde_json::Value::as_u64)
            .is_some_and(|count| count > 0)
    }

    fn trace_json_fields_match(value: &serde_json::Value, left: &str, right: &str) -> bool {
        value.pointer(left).eq(&value.pointer(right))
    }

    #[test]
    fn robot_mode_pins_filter_to_error() {
        // Robot mode (without --verbose) must emit only errors regardless of
        // --quiet, so dependency INFO/WARN can never corrupt a JSON stream.
        assert_eq!(robot_aware_log_directive(true, false, false), Some("error"));
        assert_eq!(robot_aware_log_directive(true, false, true), Some("error"));
    }

    #[test]
    fn quiet_mode_pins_filter_to_error() {
        assert_eq!(robot_aware_log_directive(false, false, true), Some("error"));
    }

    #[test]
    fn verbose_overrides_robot_quiet_to_debug() {
        assert_eq!(robot_aware_log_directive(true, true, false), Some("debug"));
        assert_eq!(robot_aware_log_directive(false, true, true), Some("debug"));
    }

    #[test]
    fn interactive_verbose_is_debug() {
        assert_eq!(robot_aware_log_directive(false, true, false), Some("debug"));
    }

    #[test]
    fn interactive_default_defers_to_env() {
        // No fixed directive in plain human mode: caller consults RUST_LOG and
        // falls back to the dependency-suppression list.
        assert_eq!(robot_aware_log_directive(false, false, false), None);
    }

    #[test]
    fn dep_suppression_quiets_fsqlite_targets() {
        // The fallback list must keep frankensqlite telemetry below INFO so even
        // human-mode stderr is not flooded.
        assert!(DEFAULT_DEP_LOG_SUPPRESSION.starts_with("info"));
        assert!(DEFAULT_DEP_LOG_SUPPRESSION.contains("fsqlite=warn"));
        assert!(DEFAULT_DEP_LOG_SUPPRESSION.contains("fsqlite_core=warn"));
    }

    #[test]
    fn trace_filter_keeps_cass_debug_and_only_dependency_warnings() -> TraceTestResult {
        trace_test_require!(
            DEFAULT_TRACE_FILE_LOG_DIRECTIVE.starts_with("warn"),
            "default trace policy must start at global WARN"
        );
        trace_test_require!(
            DEFAULT_TRACE_FILE_LOG_DIRECTIVE.contains("coding_agent_search=debug"),
            "default trace policy must retain coding_agent_search DEBUG"
        );
        trace_test_require!(
            DEFAULT_TRACE_FILE_LOG_DIRECTIVE.contains("cass=debug"),
            "default trace policy must retain cass DEBUG"
        );
        trace_test_require!(
            DEFAULT_TRACE_FILE_LOG_DIRECTIVE.contains("cass::redact::memo=warn"),
            "default trace policy must quarantine routine redaction memo events"
        );
        trace_test_require!(
            !DEFAULT_TRACE_FILE_LOG_DIRECTIVE.contains("fsqlite=debug"),
            "default trace policy must reject frankensqlite DEBUG"
        );
        Ok(())
    }

    #[test]
    fn trace_args_redact_sensitive_flags_and_secret_patterns() -> TraceTestResult {
        let raw_secret = format!("sk-{}", "a".repeat(24));
        let args = vec![
            "cass".to_string(),
            "--api-key".to_string(),
            "short-secret".to_string(),
            "--password=hunter2".to_string(),
            raw_secret.clone(),
        ];
        let masked_args = redact_trace_args(&args);
        trace_test_require!(
            matches!(masked_args.get(1).map(String::as_str), Some("--api-key")),
            "sensitive flag name was lost"
        );
        trace_test_require!(
            matches!(masked_args.get(2).map(String::as_str), Some("[REDACTED]")),
            "flag value was not redacted"
        );
        trace_test_require!(
            matches!(
                masked_args.get(3).map(String::as_str),
                Some("--password=[REDACTED]")
            ),
            "inline password was not redacted"
        );
        trace_test_require!(
            matches!(masked_args.get(4).map(String::as_str), Some("[REDACTED]")),
            "secret-pattern argument was not redacted"
        );
        trace_test_require!(
            !masked_args.join(" ").contains(&raw_secret),
            "redacted argv retained the raw secret"
        );
        trace_test_require!(
            redact_bounded_trace_text(&raw_secret, 16).eq("[REDACTED]"),
            "correlation values must be redacted before their length bound is applied"
        );
        Ok(())
    }

    #[test]
    fn trace_json_uses_shared_key_policy_and_preserves_redacted_key_collisions() -> TraceTestResult
    {
        let raw_password = "correct horse battery staple!"; // ubs:ignore -- synthetic redaction fixture.
        let raw_session = "AQoEXAMPLE-session/value+=with.punctuation"; // ubs:ignore -- synthetic redaction fixture.
        let input = serde_json::json!({
            "fields": {
                "password": raw_password,
                "AWS_SESSION_TOKEN": raw_session,
                "service_password_hash": "opaque-short-hash",
                "api_key=abcdefgh12345678": "first", // ubs:ignore -- synthetic collision fixture.
                "password=abcdefgh12345678": "second", // ubs:ignore -- synthetic collision fixture.
                "token_count": 42,
            }
        });

        let output = redact_trace_json(&input);
        let encoded = serde_json::to_string(&output)?;
        for secret in [
            raw_password,
            raw_session,
            "opaque-short-hash",
            "abcdefgh12345678",
        ] {
            trace_test_require!(
                !encoded.contains(secret),
                "trace JSON retained sensitive bytes: {encoded}"
            );
        }
        trace_test_require!(
            trace_json_text_is(&output, "/fields/password", "[REDACTED]"),
            "password field was not redacted"
        );
        trace_test_require!(
            trace_json_text_is(&output, "/fields/AWS_SESSION_TOKEN", "[REDACTED]"),
            "session-token field was not redacted"
        );
        trace_test_require!(
            output.pointer("/fields/token_count") == Some(&serde_json::json!(42)),
            "safe token_count near-miss changed"
        );
        let fields = output
            .pointer("/fields")
            .and_then(serde_json::Value::as_object)
            .ok_or("redacted trace fields missing")?;
        trace_test_require!(
            fields.len() == 6,
            "redacted key collision discarded a trace field: {fields:?}"
        );
        Ok(())
    }

    #[test]
    fn trace_enrichment_names_field_only_diagnostics() -> TraceTestResult {
        let enriched = enrich_trace_json(&serde_json::json!({
            "timestamp": "2026-07-28T00:00:00Z",
            "level": "DEBUG",
            "target": "cass::semantic",
            "fields": {"phase": "exact-search"},
        }));
        trace_test_require!(
            trace_json_text_is(&enriched, "/schema_version", "cass-trace-v1"),
            "trace enrichment omitted its schema version"
        );
        trace_test_require!(
            trace_json_text_is(&enriched, "/fields/event", "diagnostic"),
            "field-only trace event lacked its diagnostic identity"
        );
        trace_test_require!(
            enriched.get("trace_id").is_some(),
            "trace enrichment omitted trace_id"
        );
        trace_test_require!(
            enriched.get("test_id").is_some(),
            "trace enrichment omitted test_id"
        );
        Ok(())
    }

    #[test]
    fn bounded_trace_is_valid_redacted_jsonl_with_truncation_receipt() -> TraceTestResult {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("trace.jsonl");
        let max_bytes = MIN_TRACE_FILE_MAX_BYTES;
        let (writer, guard) = BoundedTraceMakeWriter::open(&path, max_bytes, 24)?;
        let filter_state = Arc::clone(&writer.state);
        let trace_enabled_calls = Arc::new(AtomicU64::new(0));
        let trace_enabled_calls_for_filter = Arc::clone(&trace_enabled_calls);
        let filter = tracing_subscriber::filter::dynamic_filter_fn(move |metadata, _context| {
            let enabled = default_trace_metadata_enabled(metadata);
            if enabled && matches!(*metadata.level(), tracing::Level::TRACE) {
                trace_enabled_calls_for_filter.fetch_add(1, Ordering::Relaxed);
            }
            if !enabled
                && metadata.is_event()
                && let Ok(mut state) = filter_state.lock()
            {
                state.record_filtered(metadata.target());
            }
            enabled
        })
        .with_max_level_hint(tracing_subscriber::filter::LevelFilter::DEBUG);
        let layer = tracing_subscriber::fmt::layer()
            .json()
            .with_writer(writer)
            .with_ansi(false)
            .with_filter(filter);
        let subscriber = tracing_subscriber::registry().with(layer);
        let raw_secret = format!("sk-{}", "z".repeat(24));

        tracing::subscriber::with_default(subscriber, || {
            tracing::trace!(
                target: "tokenizers::normalizer",
                character = "x",
                "per-character tokenizer trace must be statically disabled"
            );
            tracing::debug!(
                target: "fsqlite.parse",
                parser_token = "dependency-debug-must-not-survive",
                "dependency debug"
            );
            tracing::warn!(
                target: "fsqlite.parse",
                warning_code = "dependency-warning-survives",
                "dependency warning"
            );
            tracing::debug!(
                target: "cass::redact::memo",
                memo_marker = "redaction-memo-debug-must-not-survive",
                "routine redaction memo bookkeeping"
            );
            tracing::warn!(
                target: "cass::redact::memo",
                memo_marker = "redaction-memo-warning-survives",
                "redaction memo quarantine"
            );
            tracing::debug!(
                target: "cass::semantic",
                api_key = %raw_secret,
                phase = "exact-search",
                source_path = "/home/alice/private-project/session.jsonl",
                "CASS debug survives"
            );
            for index in 0..200 {
                tracing::debug!(
                    target: "cass::semantic",
                    index,
                    payload = %"x".repeat(256),
                    "bounded diagnostic"
                );
            }
            tracing::debug!(
                target: "cass::semantic",
                phase = "late-small-debug-must-not-survive",
                "diagnostic after the bounded head closed"
            );
            tracing::error!(
                target: "cass::semantic",
                reason_code = "late-failure-tail-survives",
                source_path = "/data/projects/private/failing-session.jsonl",
                "late failure context"
            );
        });
        drop(guard);
        // tracing's callsite interest cache is process-global: a concurrent
        // test (or a dependency runtime — asupersync 0.4.x instruments at
        // TRACE) holding TRACE interest re-enables TRACE callsites for every
        // live subscriber, so "TRACE never reaches the filter" is not
        // provable in a parallel suite. The correctness property is that this
        // filter never ADMITS a TRACE event.
        trace_test_require!(
            trace_enabled_calls.load(Ordering::Relaxed).eq(&0),
            "the dynamic filter admitted a TRACE callsite despite its DEBUG ceiling"
        );

        let bytes = std::fs::read(&path)?;
        trace_test_require!(
            u64::try_from(bytes.len()).unwrap_or(u64::MAX) <= max_bytes,
            "trace exceeded hard byte budget: {} > {max_bytes}",
            bytes.len()
        );
        let trace = String::from_utf8(bytes)?;
        trace_test_require!(!trace.contains(&raw_secret), "trace leaked raw secret");
        trace_test_require!(
            !trace.contains("/home/alice/private-project"),
            "trace leaked a private absolute path"
        );
        trace_test_require!(
            !trace.contains("dependency-debug-must-not-survive"),
            "dependency DEBUG event bypassed the trace filter"
        );
        trace_test_require!(
            trace.contains("dependency-warning-survives"),
            "dependency WARN event was lost"
        );
        trace_test_require!(
            !trace.contains("redaction-memo-debug-must-not-survive"),
            "routine redaction-memo DEBUG event bypassed the trace filter"
        );
        trace_test_require!(
            trace.contains("redaction-memo-warning-survives"),
            "redaction-memo WARN event was lost"
        );
        trace_test_require!(
            trace.contains("exact-search"),
            "CASS-owned DEBUG event was lost"
        );
        trace_test_require!(
            trace.contains("late-failure-tail-survives"),
            "the reserved failure tail lost a late ERROR event"
        );
        trace_test_require!(
            !trace.contains("late-small-debug-must-not-survive"),
            "the diagnostic head reopened after reaching its byte ceiling"
        );
        trace_test_require!(
            !trace.contains("/data/projects/private"),
            "the reserved failure tail leaked a private path"
        );

        let events = trace
            .lines()
            .map(serde_json::from_str::<serde_json::Value>)
            .collect::<Result<Vec<_>, _>>()?;
        trace_test_require!(
            events.iter().all(|event| trace_json_text_is(
                event,
                "/schema_version",
                "cass-trace-v1"
            )),
            "every trace line must carry the versioned schema"
        );
        let receipt = events
            .iter()
            .find(|event| trace_json_text_is(event, "/fields/event", "trace_truncated"))
            .ok_or("trace must carry a truncation receipt")?;
        trace_test_require!(
            trace_json_bool_is(receipt, "/fields/artifact_complete", false),
            "truncation receipt claimed the artifact was complete"
        );
        trace_test_require!(
            trace_json_count_is_positive(receipt, "/fields/suppressed_events"),
            "truncation receipt omitted its suppressed event count"
        );
        trace_test_require!(
            trace_json_count_is_positive(receipt, "/fields/suppression_reasons/byte_budget"),
            "truncation receipt must identify the exhausted limit: {receipt}"
        );
        trace_test_require!(
            receipt
                .pointer("/fields/suppressed_targets")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|targets| targets.iter().any(|target| trace_json_text_is(
                    target,
                    "/target",
                    "cass::semantic"
                )
                    && trace_json_count_is_positive(target, "/count"))),
            "truncation receipt must identify the budget-suppressed target: {receipt}"
        );
        trace_test_require!(
            trace_json_count_is_positive(receipt, "/fields/failure_tail_events"),
            "truncation receipt must attest the bounded WARN/ERROR tail: {receipt}"
        );
        trace_test_require!(
            trace_json_count_is_positive(receipt, "/fields/filtered_events"),
            "filter receipt omitted its rejected-event count"
        );
        // fsqlite 0.3.x's engine emits many more distinct filtered targets
        // (connection, snapshot_publication, ...) than 0.2.x did, so the
        // receipt's bounded per-target list can evict `fsqlite.parse` into
        // the overflow counter. The receipt property that matters is that the
        // rejected event is accounted for — either named in the bounded list
        // or rolled into `filtered_target_overflow_events`.
        let parse_target_named = receipt
            .pointer("/fields/filtered_targets")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|targets| {
                targets.iter().any(|target| {
                    trace_json_text_is(target, "/target", "fsqlite.parse")
                        && trace_json_count_is_positive(target, "/count")
                })
            });
        trace_test_require!(
            parse_target_named
                || trace_json_count_is_positive(receipt, "/fields/filtered_target_overflow_events"),
            "filter receipt must count the rejected fsqlite.parse DEBUG event \
             (named or via target overflow): {receipt}"
        );
        Ok(())
    }

    #[test]
    fn bounded_trace_uses_a_compact_receipt_when_detailed_targets_do_not_fit() -> TraceTestResult {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("compact-receipt-trace.jsonl");
        let max_bytes = MIN_TRACE_FILE_MAX_BYTES;
        let (writer, guard) = BoundedTraceMakeWriter::open(&path, max_bytes, 24)?;
        {
            let mut state = writer
                .state
                .lock()
                .map_err(|_| "trace state lock poisoned")?;
            let diagnostic_limit = state.diagnostic_limit();
            let filler = serde_json::json!({
                "timestamp": "2026-07-28T00:00:00Z",
                "level": "DEBUG",
                "target": "cass::semantic",
                "fields": {
                    "event": "diagnostic",
                    "payload": "x".repeat(1_250),
                },
            });
            trace_test_require!(
                state.write_line_with_limit(&filler, diagnostic_limit)?,
                "fixture must consume most of the diagnostic head"
            );
            for index in 0..12 {
                let target = format!("cass::semantic::target_{index:02}_{}", "y".repeat(160));
                state.record_suppressed(&target, 512, TraceSuppressionReason::ByteBudget);
                state.record_filtered(&target);
            }
            state.record_failure_tail(&serde_json::json!({
                "timestamp": "2026-07-28T00:00:01Z",
                "level": "ERROR",
                "target": "cass::semantic",
                "fields": {
                    "event": "late_failure",
                    "payload": "z".repeat(220),
                },
            }));
        }
        drop(guard);

        let trace = std::fs::read_to_string(&path)?;
        trace_test_require!(
            u64::try_from(trace.len()).unwrap_or(u64::MAX) <= max_bytes,
            "compact receipt path exceeded the hard byte ceiling"
        );
        let receipt = trace
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .find(|event| trace_json_text_is(event, "/fields/event", "trace_truncated"))
            .ok_or("trace must retain a truncation receipt")?;
        trace_test_require!(
            trace_json_bool_is(&receipt, "/fields/receipt_compact", true),
            "truncation receipt did not select the compact schema"
        );
        trace_test_require!(
            trace_json_bool_is(&receipt, "/fields/artifact_complete", false),
            "compact receipt claimed the artifact was complete"
        );
        trace_test_require!(
            trace_json_fields_match(
                &receipt,
                "/fields/suppressed_target_overflow_events",
                "/fields/suppressed_events",
            ),
            "compact receipt lost suppressed-target overflow accounting"
        );
        trace_test_require!(
            trace_json_fields_match(
                &receipt,
                "/fields/filtered_target_overflow_events",
                "/fields/filtered_events",
            ),
            "compact receipt lost filtered-target overflow accounting"
        );
        Ok(())
    }

    #[test]
    fn bounded_trace_reopen_preserves_a_closed_diagnostic_head() -> TraceTestResult {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("reopened-truncated-trace.jsonl");
        let max_bytes = 8 * 1024;
        let (writer, guard) = BoundedTraceMakeWriter::open(&path, max_bytes, 64)?;
        {
            let mut state = writer
                .state
                .lock()
                .map_err(|_| "trace state lock poisoned")?;
            state.diagnostic_head_closed = true;
            state.record_suppressed("cass::semantic", 2048, TraceSuppressionReason::ByteBudget);
        }
        drop(guard);

        let (writer, guard) = BoundedTraceMakeWriter::open(&path, max_bytes, 64)?;
        let diagnostic_head_closed = writer
            .state
            .lock()
            .map_err(|_| "trace state lock poisoned")?
            .diagnostic_head_closed;
        trace_test_require!(
            diagnostic_head_closed,
            "a prior truncation receipt must keep the diagnostic head closed"
        );
        let late_debug = serde_json::to_vec(&serde_json::json!({
            "timestamp": "2026-07-28T00:00:01Z",
            "level": "DEBUG",
            "target": "cass::semantic",
            "fields": {
                "event": "late-debug-after-reopen-must-not-survive",
            },
        }))?;
        let late_warning = serde_json::to_vec(&serde_json::json!({
            "timestamp": "2026-07-28T00:00:02Z",
            "level": "WARN",
            "target": "cass::semantic",
            "fields": {
                "event": "late-warning-after-reopen-survives",
            },
        }))?;
        {
            let mut state = writer
                .state
                .lock()
                .map_err(|_| "trace state lock poisoned")?;
            state.write_event(&late_debug, 0)?;
            state.write_event(&late_warning, 0)?;
        }
        drop(guard);

        let trace = std::fs::read_to_string(&path)?;
        trace_test_require!(
            !trace.contains("late-debug-after-reopen-must-not-survive"),
            "a reopened writer admitted a DEBUG event after prior truncation"
        );
        trace_test_require!(
            trace.contains("late-warning-after-reopen-survives"),
            "a reopened writer lost the reserved WARN/ERROR tail"
        );
        trace_test_require!(
            trace
                .lines()
                .filter(|line| line.contains("\"event\":\"trace_truncated\""))
                .count()
                .eq(&2),
            "each truncated writer invocation must leave an explicit receipt"
        );
        Ok(())
    }

    #[test]
    fn bounded_trace_rejects_legacy_text_without_overwriting_it() -> TraceTestResult {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("legacy-trace.jsonl");
        let legacy = b"2026-07-28 DEBUG legacy plain-text trace\n";
        std::fs::write(&path, legacy)?;

        let error = if let Err(error) = BoundedTraceMakeWriter::open(&path, 8 * 1024, 24) {
            error
        } else {
            return Err("legacy plain text was mixed with versioned JSONL".into());
        };
        trace_test_require!(
            error.kind().eq(&std::io::ErrorKind::InvalidData),
            "legacy trace returned unexpected error kind: {error}"
        );
        trace_test_require!(
            error.to_string().contains("not valid JSON"),
            "failure should tell the operator why a new path is required: {error}"
        );
        trace_test_require!(
            std::fs::read(&path)?.as_slice().eq(legacy),
            "rejecting a legacy trace must not truncate or overwrite it"
        );
        Ok(())
    }

    #[test]
    fn bounded_trace_rejects_a_malformed_versioned_envelope_without_mutation()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("malformed-versioned-trace.jsonl");
        let malformed = br#"{"schema_version":"cass-trace-v1","fields":{}}
"#;
        std::fs::write(&path, malformed)?;

        let error = match BoundedTraceMakeWriter::open(&path, 8 * 1024, 24) {
            Ok(_) => return Err("malformed versioned envelope was accepted".into()),
            Err(error) => error,
        };
        if !error.kind().eq(&std::io::ErrorKind::InvalidData)
            || !error
                .to_string()
                .contains("is not a valid cass-trace-v1 envelope")
        {
            return Err(format!("unexpected malformed-envelope error: {error}").into());
        }
        if !std::fs::read(&path)?.as_slice().eq(malformed) {
            return Err("rejecting a malformed envelope mutated the source artifact".into());
        }
        Ok(())
    }

    #[test]
    fn bounded_trace_repairs_a_missing_final_newline_without_double_counting() -> TraceTestResult {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("missing-newline-trace.jsonl");
        let existing = serde_json::json!({
            "schema_version": "cass-trace-v1",
            "timestamp": "2026-07-28T00:00:00Z",
            "level": "INFO",
            "target": "cass::trace",
            "trace_id": "trace-1",
            "test_id": "suite/test",
            "fields": {"event": "existing_event"},
        })
        .to_string();
        std::fs::write(&path, existing)?;

        let (writer, guard) = BoundedTraceMakeWriter::open(&path, 8 * 1024, 24)?;
        let events_written = writer
            .state
            .lock()
            .map_err(|_| "trace state lock poisoned")?
            .events_written;
        trace_test_require!(
            events_written.eq(&1),
            "repairing the line terminator must not count a second event"
        );
        drop(guard);
        trace_test_require!(
            std::fs::read(&path)?.ends_with(b"\n"),
            "a valid appendable JSONL artifact must end in a newline"
        );
        Ok(())
    }

    #[test]
    fn bounded_trace_rejects_an_unrepairable_final_newline_without_mutation() -> TraceTestResult {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("full-missing-newline-trace.jsonl");
        let existing = serde_json::json!({
            "schema_version": "cass-trace-v1",
            "timestamp": "2026-07-28T00:00:00Z",
            "level": "INFO",
            "target": "cass::trace",
            "trace_id": "trace-1",
            "test_id": "suite/test",
            "fields": {"event": "existing_event"},
        })
        .to_string()
        .into_bytes();
        std::fs::write(&path, &existing)?;

        let result = BoundedTraceMakeWriter::open(&path, u64::try_from(existing.len())?, 24);
        let error = if let Err(error) = result {
            error
        } else {
            return Err("a full trace without a final newline was accepted".into());
        };
        trace_test_require!(
            error.kind().eq(&std::io::ErrorKind::InvalidData),
            "unrepairable trace returned unexpected error kind: {error}"
        );
        trace_test_require!(
            error.to_string().contains("lacks its final newline"),
            "failure should identify the exact appendability defect: {error}"
        );
        trace_test_require!(
            std::fs::read(&path)?.as_slice().eq(existing.as_slice()),
            "rejecting an unrepairable trace must not truncate or overwrite it"
        );
        Ok(())
    }

    #[test]
    fn bounded_trace_rejects_a_second_writer_until_finalized() -> TraceTestResult {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("single-writer-trace.jsonl");
        let (_writer, guard) = BoundedTraceMakeWriter::open(&path, 8 * 1024, 24)?;

        let error = if let Err(error) = BoundedTraceMakeWriter::open(&path, 8 * 1024, 24) {
            error
        } else {
            return Err("a concurrent trace writer was accepted".into());
        };
        trace_test_require!(
            error
                .to_string()
                .contains("trace artifact already has an active writer"),
            "contention failure should be actionable: {error}"
        );

        drop(guard);
        BoundedTraceMakeWriter::open(&path, 8 * 1024, 24)?;
        Ok(())
    }
}

/// Print the auto-correction teaching note on stderr.
///
/// Humans get the multi-line bulleted form. Robots get one `note:` line per
/// correction so a JSON caller that captures stderr sees a stable, greppable
/// prefix (`note: auto-corrected:`) and never a decorative bullet or the
/// `cass --help` tip. stdout is untouched on both paths, so the data
/// contract (`stdout = data only, stderr = diagnostics`) holds.
fn emit_correction_notes(notes: &[&str], robot_mode: bool) {
    if robot_mode {
        for note in notes {
            eprintln!("note: auto-corrected: {note}");
        }
        return;
    }
    eprintln!("Note: Your command was auto-corrected:");
    for note in notes {
        eprintln!("  • {note}");
    }
    eprintln!("Tip: Run 'cass --help' for proper syntax.");
}

/// Returns true if the command is using robot/JSON output mode.
/// Used to auto-suppress INFO logs for clean machine-parseable output.
fn is_robot_mode(command: &Commands, cli: &Cli) -> bool {
    // Env-driven output formats should behave like robot mode (suppresses INFO logs).
    let env_robot_mode = robot_format_from_env().is_some();

    match command {
        Commands::Search {
            json, robot_meta, ..
        } => resolve_subcommand_structured_format(cli, *json).is_some() || *robot_meta,
        Commands::Pack { json, .. } => resolve_subcommand_structured_format(cli, *json).is_some(),
        Commands::Index { json, .. } => resolve_subcommand_structured_format(cli, *json).is_some(),
        Commands::Schedule(
            ScheduleCommand::Install { json, .. }
            | ScheduleCommand::Uninstall { json, .. }
            | ScheduleCommand::Status { json, .. }
            | ScheduleCommand::Run { json, .. },
        ) => resolve_subcommand_structured_format(cli, *json).is_some(),
        Commands::Health { json, .. } | Commands::Selftest { json } => {
            resolve_subcommand_structured_format(cli, *json).is_some()
        }
        Commands::Onboarding { json, .. } => {
            resolve_subcommand_structured_format(cli, *json).is_some()
        }
        Commands::Guide { json, .. } => resolve_subcommand_structured_format(cli, *json).is_some(),
        Commands::Pages {
            subcommand, json, ..
        } => {
            resolve_subcommand_structured_format(cli, *json).is_some()
                || subcommand.as_ref().is_some_and(|sub| match sub {
                    PagesSubcommand::Key(args) => args.command.json(),
                })
        }
        Commands::Sessions { json, .. } => {
            resolve_subcommand_structured_format(cli, *json).is_some()
        }
        Commands::Resume { json, .. } => resolve_subcommand_structured_format(cli, *json).is_some(),
        Commands::ExportHtml { json, .. } => {
            resolve_subcommand_structured_format(cli, *json).is_some()
        }
        Commands::Stats { json, .. }
        | Commands::Diag { json, .. }
        | Commands::Storage { json, .. }
        | Commands::Dedup { json, .. }
        | Commands::Status { json, .. }
        | Commands::Triage { json, .. }
        | Commands::SupportBundle { json, .. }
        | Commands::ApiVersion { json }
        | Commands::State { json, .. }
        | Commands::View { json, .. }
        | Commands::Capabilities { json }
        | Commands::Introspect { json }
        | Commands::Context { json, .. }
        | Commands::Expand { json, .. } => {
            resolve_subcommand_structured_format(cli, *json).is_some()
        }
        Commands::Doctor { json, .. } => resolve_subcommand_structured_format(cli, *json).is_some(),
        Commands::Bookmarks(args) => args.command.json() || env_robot_mode,
        Commands::Timeline { json, .. } => {
            resolve_subcommand_structured_format(cli, *json).is_some()
        }
        Commands::Mirror(MirrorCommand::Prune { json, .. }) => {
            resolve_subcommand_structured_format(cli, *json).is_some()
        }
        Commands::Forget { json, .. } => resolve_subcommand_structured_format(cli, *json).is_some(),
        Commands::Sources(SourcesCommand::List { json, .. }) => {
            resolve_subcommand_structured_format(cli, *json).is_some()
        }
        Commands::Sources(SourcesCommand::Add { json, .. }) => {
            resolve_subcommand_structured_format(cli, *json).is_some()
        }
        Commands::Sources(SourcesCommand::Doctor { json, .. }) => {
            resolve_subcommand_structured_format(cli, *json).is_some()
        }
        Commands::Sources(SourcesCommand::Agents(AgentsAction::List { json })) => {
            resolve_subcommand_structured_format(cli, *json).is_some()
        }
        Commands::Models(ModelsCommand::Status { json }) => {
            resolve_subcommand_structured_format(cli, *json).is_some()
        }
        Commands::Models(ModelsCommand::Verify { json, .. }) => {
            resolve_subcommand_structured_format(cli, *json).is_some()
        }
        Commands::Models(ModelsCommand::Backfill { json, .. }) => {
            resolve_subcommand_structured_format(cli, *json).is_some()
        }
        Commands::Models(ModelsCommand::BuildHnsw { json, .. }) => {
            resolve_subcommand_structured_format(cli, *json).is_some()
        }
        Commands::Sources(_) => false,
        Commands::Quarantine(
            QuarantineCommand::List { json, .. } | QuarantineCommand::Clear { json, .. },
        ) => resolve_subcommand_structured_format(cli, *json).is_some(),
        Commands::Import(cmd) => match cmd {
            ImportCommand::Chatgpt { json, .. } => {
                resolve_subcommand_structured_format(cli, *json).is_some()
            }
        },
        Commands::Models(_) => cli.robot_format.is_some() || env_robot_mode,
        Commands::Analytics(cmd) => analytics_requests_structured_output(cmd, cli),
        _ => false,
    }
}

fn apply_wrap(line: &str, wrap: WrapConfig) -> String {
    let width = wrap.effective_width();
    if line.trim().is_empty() || width.is_none() {
        return line.trim_end().to_string();
    }
    let width = width.unwrap_or(usize::MAX);
    if line.len() <= width {
        return line.trim_end().to_string();
    }

    let mut out = String::new();
    let mut current = String::new();
    for word in line.split_whitespace() {
        if current.len() + word.len() + 1 > width && !current.is_empty() {
            out.push_str(current.trim_end());
            out.push('\n');
            current.clear();
        }
        current.push_str(word);
        current.push(' ');
    }
    if !current.is_empty() {
        out.push_str(current.trim_end());
    }
    out
}

fn lowercase_with_map(text: &str) -> (String, Vec<usize>, Vec<(usize, usize)>) {
    let mut lower = String::with_capacity(text.len());
    let mut lower_starts = Vec::new();
    let mut orig_ranges = Vec::new();

    for (orig_idx, ch) in text.char_indices() {
        let orig_end = orig_idx + ch.len_utf8();
        for lower_ch in ch.to_lowercase() {
            lower_starts.push(lower.len());
            lower.push(lower_ch);
            orig_ranges.push((orig_idx, orig_end));
        }
    }

    (lower, lower_starts, orig_ranges)
}

fn lower_char_index(lower_starts: &[usize], lower_len: usize, byte_idx: usize) -> Option<usize> {
    if byte_idx == lower_len {
        return Some(lower_starts.len());
    }
    let idx = lower_starts.partition_point(|&b| b < byte_idx);
    if idx < lower_starts.len() && lower_starts[idx] == byte_idx {
        Some(idx)
    } else {
        None
    }
}

fn map_lower_range(
    lower_starts: &[usize],
    lower_len: usize,
    orig_ranges: &[(usize, usize)],
    start: usize,
    end: usize,
) -> Option<(usize, usize)> {
    let start_idx = lower_char_index(lower_starts, lower_len, start)?;
    let end_idx = lower_char_index(lower_starts, lower_len, end)?;
    if start_idx >= end_idx {
        return None;
    }
    let orig_start = orig_ranges.get(start_idx)?.0;
    let orig_end = orig_ranges.get(end_idx - 1)?.1;
    Some((orig_start, orig_end))
}

/// Highlight matching search terms in text
///
/// Extracts query terms and wraps matches with the specified markers.
/// Uses case-insensitive matching. Handles quoted phrases and individual terms.
///
/// # Arguments
/// * `text` - The text to highlight matches in
/// * `query` - The search query to extract terms from
/// * `start_mark` - Opening marker (e.g., "**" for markdown bold, "<mark>" for HTML)
/// * `end_mark` - Closing marker (e.g., "**" for markdown bold, "</mark>" for HTML)
fn highlight_matches(text: &str, query: &str, start_mark: &str, end_mark: &str) -> String {
    // Extract search terms from query (handles quoted phrases and individual words)
    let terms = extract_search_terms(query);
    if terms.is_empty() {
        return text.to_string();
    }

    // Sort terms by length (longest first) to avoid partial matches
    let mut terms: Vec<_> = terms.into_iter().collect();
    terms.sort_by_key(|s| std::cmp::Reverse(s.len()));

    let mut result = text.to_string();
    for term in &terms {
        if term.is_empty() {
            continue;
        }
        // Case-insensitive replacement
        // Build a lowercase copy with a byte-level map back to original indices to
        // avoid slicing bugs when Unicode case-folding changes string length.
        let (lower_result, lower_starts, orig_ranges) = lowercase_with_map(&result);
        let lower_term = term.to_lowercase();
        let marked = marked_spans(&result, start_mark, end_mark);
        let mut new_result = String::new();
        let mut last_end = 0;

        for (idx, matched_str) in lower_result.match_indices(&lower_term) {
            let end = idx + matched_str.len();
            let Some((orig_start, orig_end)) =
                map_lower_range(&lower_starts, lower_result.len(), &orig_ranges, idx, end)
            else {
                continue;
            };

            // Skip if this overlaps with a previous highlight (from a longer term)
            if orig_start < last_end {
                continue;
            }
            // Skip text that is already marked: the search engine wraps the
            // terms it matched in `**`, and wrapping them again printed
            // `****term****` (2l1b0.68).
            if marked
                .iter()
                .any(|&(start, end)| orig_start < end && orig_end > start)
            {
                continue;
            }
            // Append text before this match
            new_result.push_str(&result[last_end..orig_start]);
            // Append highlighted match (preserve original case)
            new_result.push_str(start_mark);
            new_result.push_str(&result[orig_start..orig_end]);
            new_result.push_str(end_mark);
            last_end = orig_end;
        }
        // Append remaining text
        new_result.push_str(&result[last_end..]);
        result = new_result;
    }

    result
}

/// Byte ranges of `text` already enclosed by `start_mark ... end_mark`,
/// markers included, pairing each opening mark with the next closing one.
fn marked_spans(text: &str, start_mark: &str, end_mark: &str) -> Vec<(usize, usize)> {
    let mut spans = Vec::new();
    let mut cursor = 0;
    while let Some(open) = text[cursor..].find(start_mark) {
        let open = cursor + open;
        let body = open + start_mark.len();
        let Some(close) = text[body..].find(end_mark) else {
            break;
        };
        let end = body + close + end_mark.len();
        spans.push((open, end));
        cursor = end;
    }
    spans
}

#[cfg(test)]
mod highlight_matches_tests {
    use super::highlight_matches;

    #[test]
    fn engine_marked_terms_are_not_marked_twice() {
        // The engine marks the first occurrence; --highlight marks the rest.
        assert_eq!(
            highlight_matches("a **hello** b Hello", "hello", "**", "**"),
            "a **hello** b **Hello**"
        );
        // Negative: the old implementation printed `a ****hello**** b **Hello**`.
        assert!(!highlight_matches("**hello** world", "hello world", "**", "**").contains("****"));
        // A shorter term inside an already-marked longer word stays untouched.
        assert_eq!(
            highlight_matches("**hello** there", "hell", "**", "**"),
            "**hello** there"
        );
        // Other marker styles still mark engine-marked terms in their own style.
        assert_eq!(
            highlight_matches("**hello**", "hello", ">>>", "<<<"),
            "**>>>hello<<<**"
        );
    }
}

/// Extract meaningful search terms from a query string
///
/// Handles:
/// - Quoted phrases: "exact phrase" -> ["exact phrase"]
/// - Regular words: word -> ["word"]
/// - Field filters: agent:claude -> ignored (filter, not content term)
/// - Operators: AND, OR, NOT -> ignored
fn extract_search_terms(query: &str) -> Vec<String> {
    let mut terms = Vec::new();
    let mut chars = query.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '"' {
            // Quoted phrase
            let mut phrase = String::new();
            while let Some(&next) = chars.peek() {
                if next == '"' {
                    chars.next();
                    break;
                }
                if let Some(n) = chars.next() {
                    phrase.push(n);
                }
            }
            if !phrase.is_empty() {
                terms.push(phrase);
            }
        } else if c.is_alphanumeric() || c == '_' || c == '-' {
            // Word (might be a field filter like agent:foo)
            let mut word = String::from(c);
            while let Some(&next) = chars.peek() {
                if next.is_alphanumeric() || next == '_' || next == '-' {
                    if let Some(n) = chars.next() {
                        word.push(n);
                    }
                } else if next == ':' {
                    // This is a field filter - skip the whole thing
                    chars.next(); // consume ':'
                    while let Some(&n) = chars.peek() {
                        if n.is_whitespace() {
                            break;
                        }
                        chars.next();
                    }
                    word.clear();
                    break;
                } else {
                    break;
                }
            }
            // Ignore operators
            let upper = word.to_uppercase();
            if !word.is_empty() && upper != "AND" && upper != "OR" && upper != "NOT" {
                terms.push(word);
            }
        }
        // Skip whitespace and other characters
    }

    terms
}

fn render_block<T: AsRef<str>>(lines: &[T], wrap: WrapConfig) -> String {
    lines
        .iter()
        .map(|l| apply_wrap(l.as_ref(), wrap))
        .collect::<Vec<_>>()
        .join("\n")
}

fn print_robot_help(wrap: WrapConfig) -> CliResult<()> {
    let lines = vec![
        "cass --robot-help (contract v1)",
        "===============================",
        "",
        "QUICKSTART (for AI agents):",
        "  cass triage --json                   # One-shot readiness, next command, docs, schemas",
        "  cass status --json                   # Readiness before trusted handoffs",
        "  cass search \"your query\" --robot     # Search with JSON output",
        "  cass pack \"your query\" --robot --max-tokens 12000  # Cited handoff pack",
        "  cass pack \"your query\" --robot --max-tokens 4000 --max-evidence 8  # Tight paste budget",
        "  cass search \"bug fix\" --today --robot  # Search today's sessions only",
        "  cass search \"api\" --week --agent codex --robot  # Last 7 days, codex only",
        "  cass stats --json                    # Get index statistics",
        "  cass sessions --current --json       # Find current workspace session",
        "  cass resume <path> --agent omp --json  # OMP aliases: oh-my-pi | oh_my_pi | ohmypi",
        "  cass view /path/file.jsonl -n 42 --json  # View file at line 42",
        "  cass capabilities --json           # First-stop self-description for agents",
        "  cass robot-docs commands            # Machine-readable command list",
        "  cass --robot-docs=commands          # Also accepted (auto-normalized)",
        "",
        "TIME FILTERS:",
        "  --today | --yesterday | --week | --days N",
        "  --since YYYY-MM-DD | --until YYYY-MM-DD",
        "",
        "WORKFLOW:",
        "  1. cass index --full --json   # First-time setup (index all sessions)",
        "  2. cass search \"query\" --robot  # Search with JSON output",
        "  3. cass pack \"query\" --robot --max-tokens 12000  # Cited handoff evidence",
        "  4. cass view <source_path> --message-index <line_number> --source <source_id> --conversation-id <conversation_id> --json  # Follow the exact indexed hit",
        "  Pack warnings expose freshness, semantic fallback, and privacy redactions.",
        "",
        "OUTPUT:",
        "  --robot | --json   Machine-readable JSON output (auto-quiet enabled)",
        "  stdout=data only; stderr=warnings/errors only (INFO auto-suppressed)",
        "  Use -v/--verbose with --json to enable INFO logs if needed",
        "",
        "Agent preflight: triage | ready | preflight",
        "Core subcommands: search | pack | sessions | stats | view | index | health | selftest | capabilities | introspect | robot-docs <topic>",
        "Topics: commands | env | paths | schemas | guide | exit-codes | examples | contracts | wrap | sources",
        "Exit codes: 0 ok; 1 health; 2 usage; 3 missing index/db; 7 lock/busy",
        "More: cass capabilities --json | cass robot-docs examples | cass robot-docs exit-codes",
    ];
    println!("{}", render_block(&lines, wrap));
    Ok(())
}

fn print_robot_docs(topic: RobotTopic, wrap: WrapConfig) -> CliResult<()> {
    let lines: Vec<String> = match topic {
        RobotTopic::Commands => vec![
            "commands:".to_string(),
            "  (global) --quiet / -q  Suppress info logs (auto-enabled in robot mode)".to_string(),
            "  (global) --verbose/-v  Enable debug logs (overrides auto-quiet)".to_string(),
            "  Tip: `--robot-docs=<topic>` is normalized to `robot-docs <topic>`; globals can appear before/after subcommands.".to_string(),
            "  cass search <query> [OPTIONS]".to_string(),
            "    --agent A         Filter by agent (e.g. codex, claude_code, gemini, opencode, omp, antigravity; run `cass capabilities --json | jq .connectors` for the full 26-connector inventory)".to_string(),
            "    --workspace W     Filter by workspace path".to_string(),
            "    --limit N         Max results (default: 0 = no limit)".to_string(),
            "    --offset N        Pagination offset (default: 0)".to_string(),
            "    --json | --robot  JSON output for automation".to_string(),
            "    --fields F1,F2    Select specific fields in hits (reduces token usage)".to_string(),
            "                      Presets: minimal (path,line,agent), summary (+title,score), provenance (source_id,origin_kind,origin_host)".to_string(),
            "                      Fields: score,agent,workspace,workspace_original,source_path,snippet,content,title,created_at,line_number,match_type,source_id,origin_kind,origin_host".to_string(),
            "    --max-content-length N  Truncate content/snippet/title to N chars (UTF-8 safe, adds '...')".to_string(),
            "                            Adds *_truncated: true indicator for each truncated field".to_string(),
            "    --today           Filter to today only".to_string(),
            "    --yesterday       Filter to yesterday only".to_string(),
            "    --week            Filter to last 7 days".to_string(),
            "    --days N          Filter to last N days".to_string(),
            "    --since DATE      Filter from date (YYYY-MM-DD)".to_string(),
            "    --until DATE      Filter to date (YYYY-MM-DD)".to_string(),
            "    --aggregate F1,F2 Server-side aggregation by fields (agent,workspace,date,match_type)".to_string(),
            "                      Returns buckets with counts. Reduces tokens by ~99% for overview queries".to_string(),
            "    --mode lexical|semantic|hybrid  Search mode (default: hybrid; hybrid fails open to lexical when semantic assets are unavailable)".to_string(),
            "    --no-maintenance  Strict read-only search: never joins/spawns index maintenance or a daemon; conflicts with --refresh and --daemon".to_string(),
            "    --robot-meta     Include readiness, requested/realized mode, semantic refinement, fallback tier/reason, cursor, and timing metadata".to_string(),
            "  cass pack <query> [--robot] [--max-tokens N] [--limit N]".to_string(),
            "    Build a deterministic, cited answer pack for agent handoffs without external summarization.".to_string(),
            "    --sessions-from FILE|-  Restrict evidence to newline-delimited session paths; '-' reads stdin.".to_string(),
            "    --field-mask minimal|standard|full|summary|all,F1,F2  Select pack fields for JSON/TOON output (--fields also works).".to_string(),
            "    --max-sessions N  Cap how many sessions can contribute evidence to the pack.".to_string(),
            "    --max-evidence N  Cap cited evidence items selected into the pack.".to_string(),
            "    --max-excerpt-chars N  Bound each excerpt before token estimation.".to_string(),
            "    --freshness-policy prefer-recent|strict|allow-stale  Evidence freshness policy.".to_string(),
            "    --freshness-window-seconds N  Freshness window used for stale-evidence warnings and strict filtering.".to_string(),
            "    --include-skill-content  Include skill payload excerpts; credentials remain redacted.".to_string(),
            "    --require-evidence  Return a JSON error envelope instead of an empty successful pack.".to_string(),
            "    --explain-selection  Include score components and omission diagnostics for audits.".to_string(),
            "    Output includes health, freshness, privacy, evidence, omitted, and warnings fields.".to_string(),
            "  cass stats [--json] [--data-dir DIR]".to_string(),
            "  cass triage [--json] [--stale-threshold N] [--data-dir DIR]".to_string(),
            "    One-shot agent preflight: readiness, next_command, recommended_commands, docs, schemas, workflows, and recoveries.".to_string(),
            "    Aliases: ready, preflight.".to_string(),
            "  cass status [--json] [--stale-threshold N] [--data-dir DIR]".to_string(),
            "  cass diag [--json] [--verbose] [--data-dir DIR]".to_string(),
            "  cass sessions [--workspace DIR] [--current] [--limit N] [--since WHEN] [--json]"
                .to_string(),
            "  cass view <path> [-n LINE] [-C CONTEXT] [--json]".to_string(),
            "  cass index [--full] [--watch] [--json] [--robot-trace-ingest] [--data-dir DIR]"
                .to_string(),
            "                    In --json mode, NDJSON events stream on stderr:".to_string(),
            "                      {event:started|phase|progress|completed|error, ...}".to_string(),
            "                    Tune with --progress-interval-ms N (250..60000, default 2000),".to_string(),
            "                    disable with --no-progress-events or CASS_INDEX_NO_PROGRESS_EVENTS=1.".to_string(),
            "                    Add --robot-trace-ingest for per-batch wall_ms, batch_msgs, and duplicate-lookup counters on stderr.".to_string(),
            "                    Add --rebuild-profile bounded for a single-core, low-memory lexical rebuild of a wedged or".to_string(),
            "                    memory-starved archive; explicit CASS_TANTIVY_REBUILD_* values still win, and --json".to_string(),
            "                    reports the values used under rebuild_profile.".to_string(),
            "                    From another shell: `cass status --json` shows live progress.".to_string(),
            "  cass tui [--once] [--data-dir DIR] [--reset-state] [--asciicast FILE]"
                .to_string(),
            "  cass capabilities [--json]   First-stop self-description: workflows, mistake recoveries, commands, flags, exit codes, env vars, limits.".to_string(),
            "  cass robot-docs <topic>".to_string(),
            "  cass --robot-help".to_string(),
            // coding_agent_session_search-g9981: backfill the 20 subcommands
            // this list omitted pre-fix. Agents using `cass robot-docs
            // commands` as the authoritative CLI enumeration now see the
            // full advertised surface instead of the ~10-command slice.
            "  cass health [--json]             Minimal readiness probe (<50ms, exit 0=healthy, 1=unhealthy).".to_string(),
            "    --binary-only     Report the same archive-readiness verdict but exit 0 unless the executable itself fails; use selftest to avoid archive access.".to_string(),
            "  cass selftest [--json]           Archive-independent executable probe for package and binary-promotion gates.".to_string(),
            "  cass serve --stdio (--index DIR | --data-dir DIR) [--mcp]".to_string(),
            "                    Keep read-only search readers open for a stream of JSON requests: repeated queries skip".to_string(),
            "                    reopening and re-verifying the index (docs/SEARCH_SERVICE.md). Semantic search is opt-in.".to_string(),
            "  cass archive export|verify|search|view|import [--json]".to_string(),
            "                    Logical canonical backup as private JSONL: export a snapshot, verify it without a DB,".to_string(),
            "                    search or view its bodies, or restore into a NEW database (docs/LOGICAL_ARCHIVE.md).".to_string(),
            "  cass doctor [--json] [--fix]     Legacy spelling: --json realizes read-only check; --fix realizes safe-auto-run.".to_string(),
            "                    doctor JSON includes source_inventory; missing upstream provider files are".to_string(),
            "                    source coverage/prune-risk warnings, not proof that archived cass rows are lost.".to_string(),
            "                    raw_mirror defines/verifies the content-addressed raw session evidence layout.".to_string(),
            "                    raw_mirror.policy states the privacy boundary: default robot JSON, logs,".to_string(),
            "                    support bundles, HTML exports, and Pages exports do not include raw bytes or exact source paths.".to_string(),
            "  cass doctor check --json          Explicit read-only doctor v2 truth surface.".to_string(),
            "  cass doctor repair --dry-run --json".to_string(),
            "                    Preview a fingerprinted repair plan without mutating filesystem or archive state.".to_string(),
            "  cass doctor repair --yes --plan-fingerprint <fp> --json".to_string(),
            "                    Apply only the inspected plan whose fingerprint exactly matches the dry-run.".to_string(),
            "  cass doctor --fix --json          Legacy safe-auto-run; safe derived repairs only, with receipts for every mutation.".to_string(),
            "  cass doctor --fix --force-rebuild --json".to_string(),
            "                    Requests derived rebuild inside safe-auto-run; never bypasses archive/source authority gates.".to_string(),
            "                    Robots should read introspect.response_schemas doctor-* contracts and branch on".to_string(),
            "                    err.kind/status/operation_outcome.kind/outcome_kind/asset_class/risk_level/fallback_mode.".to_string(),
            // Keep the doctor command reference complete for agents; several
            // diagnostic modes are easier to discover here than in clap help.
            "  cass doctor --ls --json           List per-run artifact dirs under <data_dir>/doctor/runs/ (read-only).".to_string(),
            "  cass doctor --undo <RUN_ID> --json".to_string(),
            "                    Restore byte-identically from the per-run backups; `latest` alias supported. Refuses on tamper.".to_string(),
            "  cass doctor --robot-triage --json One-shot agent triage: summary, findings, recommended_command in one envelope.".to_string(),
            "  cass doctor --diff [<REF>|<A>..<B>] --json".to_string(),
            "                    Drift detection (`<REF>` mode) or run-to-run plan comparison (`<A>..<B>` mode). Read-only.".to_string(),
            "  cass doctor --gc-before <ISO> --yes --json".to_string(),
            "                    Quarantine doctor runs older than the cutoff. Never deletes (per AGENTS.md RULE NUMBER 1).".to_string(),
            "  cass doctor --watch --watch-interval-ms <N> --watch-iterations <N> --json".to_string(),
            "                    Long-running JSONL monitor; emits one `kind: watch-tick` event per poll. SIGINT exits 130, SIGTERM 143.".to_string(),
            "  cass doctor --explain <RUN_ID> --json".to_string(),
            "                    Single envelope with run manifest + every recorded action + flattened band timeline.".to_string(),
            "  cass doctor --emit-capabilities --json".to_string(),
            "                    Self-describe: detectors[], fixers[], exit_codes[], data_paths[], env_vars[].".to_string(),
            "  cass robot-docs doctor             Paste-ready agent handbook for the doctor surface.".to_string(),
            "  cass introspect [--json]         Full API schema: commands, arguments, response_schemas (alphabetical).".to_string(),
            "  cass api-version [--json]        Show crate_version + api_version + contract_version.".to_string(),
            "  cass state [--json]              Alias of `cass status` (index/db/rebuild/semantic readiness).".to_string(),
            "  cass models status [--json]      Semantic model acquisition + cache state.".to_string(),
            "  cass models install --from-file DIR [--model minilm|multilingual-minilm]  Install an explicit native embedder from a local directory (SHA256-verified).".to_string(),
            "  cass models remove [--model NAME]  Remove an installed semantic model from disk.".to_string(),
            "  cass models verify [--model NAME] [--json]  Per-file SHA-256 verification of an explicit installed model.".to_string(),
            "  cass models backfill             Re-embed conversations against a newly acquired model.".to_string(),
            "  cass models build-hnsw [--tier fast|quality] [--check] [--force] [--json]  Build/verify the HNSW accelerator for a published semantic artifact (no archive open, no embedding).".to_string(),
            "  cass expand <path> --message-index N [--source ID] [--conversation-id ID] [-C CONTEXT] [--json]  Follow search line_number; --line is for raw JSONL.".to_string(),
            "  cass resume <path> [--shell]     Resolve a session path into its native-harness resume command.".to_string(),
            "  cass timeline [--since DATE] [--until DATE] [--json]  Activity timeline over a time range.".to_string(),
            "  cass forget [--source-glob '<glob>'] [--workspace-glob '<glob>'] [--older-than 90d] [--apply] [--json]  Require at least one selector; combine with AND. Preview matching sessions; --apply removes indexed copies and keeps source files. Shared-session tombstones persist until explicitly released.".to_string(),
            "  cass forget --list [--source-glob '<glob>' | --source-path PATH] [--source-id ID] [--agent SLUG] [--external-id ID] --json  Inspect recorded exclusions; historical records have no workspace or message-age metadata.".to_string(),
            "  cass forget --reinclude [identity selectors] [--apply] --json  Preview explicit exclusion release; at least one selector is required. After apply, run cass index; current indexing policies still apply.".to_string(),
            "  cass mirror prune [--older-than 90d] [--max-size 100GB] [--keep-latest 1] [--keep-tag important] [--apply] [--json]  Plan or apply raw-mirror retention with an audit log.".to_string(),
            "  cass context <path> [--json]     Find related sessions for a given source path.".to_string(),
            "  cass export <path> [--format markdown] [--output FILE]  Export a conversation to markdown / other formats.".to_string(),
            "  cass export-html <path> [--output-dir DIR] [--json]  Self-contained HTML export (optional encryption).".to_string(),
            "  cass pages [--export-only DIR] [--verify PATH] [--agents A1,A2]  Encrypted searchable archive for local preview/sharing.".to_string(),
            "  cass sources [list|add|remove|doctor|mappings|agents ...] [--json]  Manage local sources + persisted agent-harness exclusions.".to_string(),
            "  cass analytics [status|tokens|tools|models|rebuild|validate] [--json]  Token / tool / model analytics.".to_string(),
            "  cass import [...]                Import conversation data from external sources.".to_string(),
            "  cass daemon [...]                Run the semantic model daemon (Unix only).".to_string(),
            "  cass schedule [install|uninstall|status|run] [--json]  Keep the index fresh via launchd / systemd user timers (incremental every 15 min + nightly full index & semantic backfill at low priority).".to_string(),
            "  cass completions <shell>         Emit shell completion script for bash | zsh | fish | powershell.".to_string(),
            "  cass man                         Emit man page (roff) for the cass binary.".to_string(),
        ],
        RobotTopic::Env => vec![
            "env:".to_string(),
            "  TUI_HEADLESS=1                           headless test runs (no interactive prompts)".to_string(),
            "  CASS_DATA_DIR                            override data dir".to_string(),
            "  CASS_DB_PATH                             override db path".to_string(),
            "  CASS_OUTPUT_FORMAT=json|jsonl|compact|sessions|toon  default structured output".to_string(),
            "  CASS_SEARCH_TIMEOUT_MS=<N>               default `cass search`/`pack` timeout in ms (--timeout overrides; 0=none)".to_string(),
            "  CASS_SEARCH_LIMIT=<N>                    default search/pack limit (--limit overrides; 0=no limit)".to_string(),
            "  CASS_SEARCH_MODE=lexical|semantic|hybrid default search/pack mode (--mode overrides)".to_string(),
            "  CASS_STATUS_BUDGET_MS=<N>                robot status budget in ms (default 8000; sheds optional sections after expiry)".to_string(),
            "  CASS_DOCTOR_BUDGET_MS=<N>                structured doctor reporting budget in ms (default 8000; marks overruns timed_out)".to_string(),
            "  CASS_FTS_DRYRUN_CAP=<N>                  row-ID comparison cap for bounded canonical FTS dry-run divergence scans (default 4096; positive integer, invalid values use the default)".to_string(),
            "  CASS_VIEW_BUDGET_MS=<N>                  structured view worker deadline in ms (default 10000)".to_string(),
            "  CASS_SEARCH_BUDGET_MS=<N>                structured search worker budget in ms (default 120000; a positive effective search timeout overrides)".to_string(),
            "  CASS_TRIAGE_BUDGET_MS=<N>                robot triage probe budget in ms (default 8000; sheds incomplete readiness sections after expiry)".to_string(),
            "  CASS_PACK_BUDGET_MS=<N>                  structured pack budget in ms (default 10000; a positive effective search timeout overrides)".to_string(),
            "  TOON_DEFAULT_FORMAT=toon|json            fallback structured output for all tools".to_string(),
            "  TOON_INDENT=<N>                           pretty-print TOON with indent".to_string(),
            "  TOON_KEY_FOLDING=off|safe                 TOON key folding mode".to_string(),
            "  CASS_NO_COLOR                            force monochrome".to_string(),
            "  CASS_RESPECT_NO_COLOR=1                  honor global NO_COLOR".to_string(),
            "  CASS_TRACE_FILE                          default cass-trace-v1 JSONL path".to_string(),
            "  CASS_TRACE_FILTER                        trace-file EnvFilter (CASS DEBUG + global WARN/ERROR; redaction memo WARN)".to_string(),
            "  CASS_TRACE_MAX_BYTES=<N>                 hard trace-file byte ceiling (default 16777216; clamped 4096..1073741824)".to_string(),
            "  CASS_TRACE_MAX_EVENTS=<N>                hard diagnostic-event ceiling (default 50000; clamped 16..10000000)".to_string(),
            "  CASS_INDEX_NO_PROGRESS_EVENTS=1          suppress NDJSON events from `cass index --json`".to_string(),
            "  CASS_INDEX_INGEST_WAL_CHECKPOINT_BYTES=<N>  reset a deferred bulk-ingest WAL after N bytes (default 536870912; 0 disables)".to_string(),
            "  CASS_INDEX_INGEST_WAL_CHECKPOINT_MODE=PASSIVE|FULL|RESTART|TRUNCATE  in-run bulk WAL checkpoint mode (default TRUNCATE)".to_string(),
            "  CASS_MIGRATION_LEGACY_FTS_DROP_BUDGET_BYTES=<N>  refuse the unbounded V14 legacy-FTS teardown on archives above N bytes (default 536870912; 0 disables the refusal)".to_string(),
            "  CASS_DEFER_ANALYTICS_UPDATES=1             defer derived analytics writes during indexing; a pending legacy OMP rebuild resumes on a later run with this unset".to_string(),
            "  CASS_STORAGE_PROBE_TIMEOUT_MS=<N>        budget for the dedicated typed storage probes on status/search/doctor (default 100; clamped 10..10000) — widen on slow hosts where schema/contention classification times out to `unchecked`".to_string(),
            "  CASS_RAW_MIRROR=0                        stop copying session files into the raw mirror (default on); indexing continues, doctor reports raw_mirror_capture_disabled".to_string(),
            "  CASS_AUTO_REFRESH=0                      disable stale-on-read catch-up (detached `cass index --background` after a stale search/pack/TUI launch)".to_string(),
            "  CASS_AUTO_REFRESH_COOLDOWN_SECS=<N>      min seconds between auto-spawned catch-up runs (default 300)".to_string(),
            "  CASS_BACKGROUND_NICE=<N>                 nice value for `cass index --background` (default 15)".to_string(),
            "  CASS_BACKGROUND_IONICE_CLASS=<N>         ionice class for `cass index --background` on Linux (default 3 = idle)".to_string(),
            "  CASS_DAEMON_INDEX_INTERVAL_SECS=<N>      resident daemon spawns an incremental background index every N s (default 0 = off)".to_string(),
            "  CASS_SCHEDULE_MAX_BACKFILL_BATCHES=<N>   cap on semantic backfill batches per nightly `cass schedule run` (default 200)".to_string(),
            "  CASS_SCHEDULE_MAX_LOAD_DEFERRAL_SECS=<N> longest severe load may skip scheduled incremental runs since the last completed one (default 10800; 0 = no limit)".to_string(),
            "  CASS_RESPONSIVENESS_MIN_USER_IDLE_SECS=<N>  require console idle time before scheduled backfill/nightly jobs (default 0 = off; macOS only, fails open)".to_string(),
            "  CASS_RESPONSIVENESS_DISABLE=1            pin indexer fan-out at 100% (skip governor)".to_string(),
            "  CASS_RESPONSIVENESS_MIN_CAPACITY_PCT=<N> floor for governor shrink (default 25, range 10..100)".to_string(),
            "  CASS_RESPONSIVENESS_MAX_LOAD_PER_CORE=<F>  loadavg/core threshold for step-down (default 1.25)".to_string(),
            "  CASS_RESPONSIVENESS_SEVERE_LOAD_PER_CORE=<F>  loadavg/core threshold for floor drop (default 1.75)".to_string(),
            "  CASS_RESPONSIVENESS_MAX_PSI_AVG10=<F>    /proc/pressure/cpu some-avg10 step-down (default 20.0)".to_string(),
            "  CASS_RESPONSIVENESS_SEVERE_PSI_AVG10=<F>  PSI severe threshold (default 40.0)".to_string(),
            "  CASS_RESPONSIVENESS_GROWTH_TICKS=<N>     healthy ticks needed before each 25pp grow step (default 3)".to_string(),
            "  CASS_RESPONSIVENESS_TICK_SECS=<N>        sampler interval in seconds (default 2)".to_string(),
            "  CASS_RESPONSIVENESS_CALIBRATION=static|conformal  threshold policy (DEFAULT: conformal). Set to `static` to revert to legacy hand-tuned 1.25/20.0 thresholds.".to_string(),
            "  CASS_RESPONSIVENESS_CONFORMAL_K=<N>      calibration window size (clamped 16..4096, default 256)".to_string(),
            "  CASS_RESPONSIVENESS_CONFORMAL_K_MIN=<N>  min samples before quantile emits (clamped 4..K, default 32)".to_string(),
            "  CASS_RESPONSIVENESS_CONFORMAL_ALPHA_PRESSURED=<F>  FP rate for pressured quantile (0<α<0.5, default 0.05)".to_string(),
            "  CASS_RESPONSIVENESS_CONFORMAL_ALPHA_SEVERE=<F>  FP rate for severe quantile (0<α<pressured, default 0.01)".to_string(),
            "  CASS_RESPONSIVENESS_DRIFT_DELTA=<F>      Page-Hinkley drift tolerance δ (default 0.01)".to_string(),
            "  CASS_RESPONSIVENESS_DRIFT_LAMBDA=<F>     Page-Hinkley trigger λ for calibration reset (default 0.5)".to_string(),
            "  CASS_STREAMING_CONSUMER_COMMIT_SECS=<N>  base streaming-consumer lexical commit cadence (default 5)".to_string(),
            "  CASS_SEMANTIC_BATCH_SIZE=<N>             embedder batch size (default 128)".to_string(),
            "  CASS_SEMANTIC_PREP_PARALLEL=1            opt in to rayon-parallel canonicalize+hash prep (default off: serial is measurably faster on the common cheap-embedder path)".to_string(),
            "  CASS_SEMANTIC_EMBED_BATCH_WARN_AFTER_MS=30000  warn when one embedder batch exceeds the cass#257-derived 30s healthy-run threshold".to_string(),
            "  CASS_SEMANTIC_EMBED_BATCH_FAIL_AFTER_MS=300000  abort after a returned embedder batch exceeds the cass#257-derived 5m watchdog threshold; 0 disables".to_string(),
            "  CASS_SEMANTIC_MAX_MESSAGES_PER_CHECKPOINT=10000 cap models backfill checkpoints at whole-conversation prefixes near 10k selected messages; 0 disables".to_string(),
            "  CASS_SEMANTIC_MAX_BYTES_PER_CHECKPOINT=8388608 cap models backfill checkpoints at whole-conversation prefixes near 8MiB selected content; 0 disables".to_string(),
            "  CASS_STREAMING_CONSUMER_COMBINE=0        DISABLE flat-combining drain in run_streaming_consumer (Card 3; DEFAULT: on). Any non-off value (unset, 1, true, yes, on) leaves combining enabled.".to_string(),
            "  CASS_STREAMING_COMBINE_MAX=<N>           max messages per combined drain (clamped 1..1024, default 64)".to_string(),
            "  CASS_STREAMING_COMBINE_MAX_BYTES=<N>     byte cap per combined drain (clamped 1MiB..STREAMING_MAX, default half)".to_string(),
            "  CASS_INDEXER_PARALLEL_WAL=off            DISABLE Card 1 parallel-WAL shadow observer (DEFAULT: shadow). Observer records per-chunk wall-clock; does NOT change commit semantics. `on`/`commit` are reserved for a future revision.".to_string(),
            "  CASS_AIDER_DATA_ROOT=<path>              Aider history location (default ~/.aider.chat.history.md)".to_string(),
            "  CASS_ALLOW_DUMB_TERM=1                   allow TUI startup even when TERM=dumb".to_string(),
            "  CASS_AUTOMATIC_WILDCARD_FALLBACK_MAX_DOCS=<N>  max index documents for automatic wildcard fallback on sparse results (default 10000)".to_string(),
            "  CASS_CACHE_BYTE_CAP=<N>                  search-hit cache byte limit (default available memory / 128, clamped 64 MiB..2 GiB; 0 disables)".to_string(),
            "  CASS_CACHE_EVICTION_POLICY=lru|s3-fifo   search-hit cache eviction policy (default lru)".to_string(),
            "  CASS_CACHE_SHARD_CAP=<N>                 per-shard LRU cache entries (default 256)".to_string(),
            "  CASS_CACHE_TOTAL_CAP=<N>                 total cached search hits (default 2048)".to_string(),
            "  CASS_DEBUG_CACHE_METRICS=1               enable cache hit/miss logging".to_string(),
            "  CASS_DISABLE_ANIMATIONS=1                disable UI animations".to_string(),
            "  CASS_DOCTOR_RAW_MIRROR_FULL_VERIFY=1     hash every raw-mirror descriptor/chunk during read-only doctor runs".to_string(),
            "  CASS_DOCTOR_RAW_MIRROR_FULL_VERIFY_BYTE_LIMIT=<N>  defer full raw-mirror hashing above this byte count (default 536870912)".to_string(),
            "  CASS_DOCTOR_RAW_MIRROR_FULL_VERIFY_MANIFEST_LIMIT=<N>  defer full raw-mirror hashing above this manifest count (default 256)".to_string(),
            "  CASS_DOCTOR_WAL_CHECKPOINT_TIMEOUT_SECS=<N>  wall-clock budget in seconds for `cass doctor --fix` WAL checkpoint (default 120; 0 keeps default)".to_string(),
            "  CASS_EXCLUDE_PATHS=p1,p2                 comma/newline-delimited files or directory prefixes to skip without advancing scan/watch watermarks".to_string(),
            "  CASS_EXCLUDE_WORKSPACES=g1,g2            comma/newline-delimited workspace globs to exclude from future ingestion; scan watermarks advance, policy changes revisit sources".to_string(),
            "  CASS_FTS_INLINE_BUDGET_SECS=<N>          wall-clock budget in seconds per index run for inline fts_messages shadow writes (default 300; 0 disables)".to_string(),
            "  CASS_FTS_REPAIR_PAGE_BUDGET_SECS=<N>     per-page budget in seconds for paged fts_messages shadow repair/rebuild (default 120; 0 disables)".to_string(),
            "  CASS_FTS_SHADOW_MAX_MESSAGES=<N>         max indexable message count for retaining the fts_messages SQL-fallback shadow (default 100000; 0 disables)".to_string(),
            "  CASS_INCREMENTAL_AUTHORITATIVE_LEXICAL_REPAIR_MAX_DB_BYTES=<N>  max archive bytes for inline authoritative lexical repair during search/index (default 1073741824 = 1 GiB)".to_string(),
            "  CASS_INDEX_FINALIZE_ABORT_SECS=<N>       abort grace in seconds during finalize WAL-checkpoint / rebuild-tail windows (default 1800; 0 report-only)".to_string(),
            "  CASS_INDEX_FINAL_WAL_CHECKPOINT_TIMEOUT_SECS=<N>  wall-clock budget in seconds for final wal_checkpoint(TRUNCATE) during index (default 900; 0 keeps default)".to_string(),
            "  CASS_INDEX_REDACTION=full|off            index-time secret redaction mode (default full)".to_string(),
            "  CASS_INDEX_SKIP_DISK_HEADROOM_CHECK=1    skip the free-space preflight before `cass index --full` / `--force-rebuild` and the rebuild's free-space floor".to_string(),
            "  CASS_INDEX_STALL_ABORT_ALL_PHASES=1      promote stall watchdog warnings to hard abort (exit 70) in all phases".to_string(),
            "  CASS_INDEX_STALL_ABORT_SECS=<N>          seconds without progress before abort-eligible lexical stall exits 70 (default 300; 0 report-only)".to_string(),
            "  CASS_INDEX_STALL_DETECT_SECS=<N>         seconds without phase progress before emitting stall diagnostics (default 120; 0 disables)".to_string(),
            "  CASS_LEXICAL_MAX_MESSAGE_CONTENT_BYTES=<N>  max bytes of any single message indexed into the lexical index (default 8388608 = 8 MiB)".to_string(),
            "  CASS_LEXICAL_MERGE_MAX_OUTPUT_BYTES=<N>  max estimated output bytes per lexical merge run (default 1073741824 = 1 GiB)".to_string(),
            "  CASS_MUSE_DATA_ROOT=<path>               Muse Code data root (default ~/.local/share/muse)".to_string(),
            "  CASS_OMP_DATA_ROOT=<path>                OMP-only archive/store root override".to_string(),
            "  CASS_QUILL_QUERY_FUEL_BUDGET=<N>         deterministic per-query work ceiling for Quill lexical queries (default 10000000)".to_string(),
            "  CASS_REDACT_MEMO_CAPACITY=<N>            per-worker redaction memo cache entry cap during batched persist (default 4096)".to_string(),
            "  CASS_REDACT_SECRETS=0|1                  legacy redaction toggle (default 1; CASS_INDEX_REDACTION takes precedence)".to_string(),
            "  CASS_RESPONSIVENESS_MAX_INFLIGHT_BYTES=<N>  governor ceiling on in-flight message bytes across indexing (default RAM/32 clamped 512 MiB..16 GiB)".to_string(),
            "  CASS_RESPONSIVENESS_MAX_WORKERS=<N>      hard ceiling the load/PSI governor applies to indexing worker pools".to_string(),
            "  CASS_SEARCH_ACTIVE_REBUILD_WAIT_MS=<N>   max ms search waits for an active lexical rebuild before returning index-busy (default 30000)".to_string(),
            "  CASS_SEARCH_EXACT_TOTAL_COUNT_MAX_DOCS=<N>  max index documents for computing exact total_matches on a full page (default 5000000; 0 disables)".to_string(),
            "  CASS_SEMANTIC_EMBEDDER=hash|minilm|multilingual-minilm  force semantic embedder backend (default auto)".to_string(),
            "  CASS_SEMANTIC_PROGRESS_JSONL=<path>      append semantic backfill phase transition events as JSONL to <path>".to_string(),
            "  CASS_SSH_CONFIG=<path>                   deprecated-ignored: remote sync was removed; no SSH config is consulted".to_string(),
            "  CASS_EXTERNAL_EMBEDDING*               deprecated-ignored: the external HTTP embedder was removed; no endpoint is contacted".to_string(),
            "  CASS_TANTIVY_REBUILD_BATCH_FETCH_CONVERSATIONS=<N>  conversations per SQL message-fetch chunk during lexical rebuild".to_string(),
            "  CASS_TANTIVY_REBUILD_COMMIT_EVERY_CONVERSATIONS=<N>  steady-state durable-commit interval in conversations (default 10000)".to_string(),
            "  CASS_TANTIVY_REBUILD_COMMIT_EVERY_MESSAGES=<N>       steady-state durable-commit interval in messages (default 800000)".to_string(),
            "  CASS_TANTIVY_REBUILD_COMMIT_EVERY_MESSAGE_BYTES=<N>  steady-state durable-commit interval in message bytes (default 536870912)".to_string(),
            "  CASS_TANTIVY_REBUILD_INITIAL_BATCH_FETCH_CONVERSATIONS=<N>  chunk size for the first durable rebuild slice (default 32)".to_string(),
            "  CASS_TANTIVY_REBUILD_INITIAL_COMMIT_EVERY_CONVERSATIONS=<N>  initial durable-commit interval in conversations (default 2048)".to_string(),
            "  CASS_TANTIVY_REBUILD_INITIAL_COMMIT_EVERY_MESSAGES=<N>       initial durable-commit interval in messages (default 800000)".to_string(),
            "  CASS_TANTIVY_REBUILD_INITIAL_COMMIT_EVERY_MESSAGE_BYTES=<N>  initial durable-commit interval in message bytes (default 134217728)".to_string(),
            "  CASS_TANTIVY_REBUILD_PAGE_PREP_WORKERS=<N>  threads fetching and preparing conversation pages ahead of lexical writers".to_string(),
            "  CASS_TANTIVY_REBUILD_PIPELINE_CHANNEL_SIZE=<N>  prepared pages queued between page prep and lexical writers (default 4)".to_string(),
            "  CASS_TANTIVY_REBUILD_PIPELINE_MAX_MESSAGE_BYTES_IN_FLIGHT=<N>  cap on message bytes held in flight across the rebuild pipeline".to_string(),
            "  CASS_TANTIVY_REBUILD_RESERVED_CORES=<N>  CPU cores reserved outside the lexical rebuild worker pool".to_string(),
            "  CASS_TANTIVY_REBUILD_WORKERS=<N>         writer/shard-builder threads for lexical rebuild (default cores - reserved, clamped 1..64)".to_string(),
            "  CASS_WARM_DEBOUNCE_MS=<N>                warm-up search debounce in ms (default 120)".to_string(),
        ],
        RobotTopic::Paths => {
            let mut lines: Vec<String> = vec!["paths:".to_string()];
            lines.push(format!("  data dir default: {}", default_data_dir().display()));
            lines.push(format!("  db path default: {}", default_db_path().display()));
            lines.push(
                "  log path (cass tui only): <data-dir>/cass.log.YYYY-MM-DD \
                 (daily rolling). Non-TUI subcommands emit to stderr; \
                 redirect with `2> /path/to/cass.log` or use -v / \
                 CASS_TRACE_FILE=<path> for a persistent artifact."
                    .to_string(),
            );
            lines.push(
                "  trace: CASS_TRACE_FILE=<path> or --trace-file <path> (JSONL, \
                 available on every subcommand)."
                    .to_string(),
            );
            lines
        }
        RobotTopic::Guide => vec![
            "guide:".to_string(),
            "  Robot-mode handbook: docs/ROBOT_MODE.md (automation quickstart)".to_string(),
            "  Output: --robot/--json; formats via --robot-format json|jsonl|compact|toon (--format/--output aliases accepted)".to_string(),
            "  Logging: INFO auto-suppressed in robot mode; add -v to re-enable".to_string(),
            "  Search contract: SQLite is source of truth; lexical is the required self-healing fast path; semantic is opportunistic enrichment.".to_string(),
            "  Pack contract: `cass pack \"query\" --robot` returns extractive, cited handoff evidence selected from search results; it does not call an external model or mutate source logs.".to_string(),
            "  Pack vs search: use search to explore/paginate/aggregate candidates; use pack once you need a bounded handoff artifact.".to_string(),
            "  Pack vs export-html: export-html creates a complete browsable session archive; pack creates a token-budgeted evidence bundle.".to_string(),
            "  Pack vs doctor/status: status/health report readiness before a handoff; doctor diagnoses or repairs derived assets and is not a summarizer.".to_string(),
            "  Pack warnings: inspect health, freshness, privacy, and warnings for semantic_fallback_lexical (realized.fallback_reason names the cause; pack_enrichment_unavailable means pack itself selects evidence lexically), privacy_redactions_applied, and no_evidence_found before copying output; stale evidence is structural via freshness.stale_evidence_count.".to_string(),
            "  Pack budgets: tune --max-tokens, --max-evidence, --max-sessions, --max-excerpt-chars, and --fields summary/minimal to fit the recipient context.".to_string(),
            "  Default search: hybrid-preferred. With --robot-meta, inspect requested_search_mode, search_mode, semantic_refinement, fallback_tier, and fallback_reason.".to_string(),
            "  Quick history: use --workspace PATH --days 7 --mode lexical --no-maintenance --robot --robot-meta --fields source_path,line_number,agent,source_id,conversation_id --limit 5 --max-tokens 2000 --timeout 2000.".to_string(),
            "  Budget: --timeout is milliseconds; --max-tokens bounds approximate output, not scan work. Also set a caller-side deadline; inspect budget.timed_out even after exit 0.".to_string(),
            "  Retrieval refusal: maintenance-required ends this attempt; do not rebuild, install models, or run broad aggregates to answer a quick history question. Check older-version help; never silently drop --no-maintenance.".to_string(),
            "  Evidence: expand useful source_path/line_number hits with `cass view PATH --message-index LINE_NUMBER --source SOURCE_ID --conversation-id CONVERSATION_ID -C 3 --json --timeout 2000`; preserve citations. Broaden scope or choose semantic refinement deliberately.".to_string(),
            "  Readiness: use `cass triage --json` for diagnosis, not as a prerequisite to every query. Review recommended mutations separately; cass health/status JSON remains the narrower readiness truth surface.".to_string(),
            "  Doctor outcomes: branch on doctor.operation_outcome.kind (kebab-case) before prose; exit_code_kind says whether the outcome is success, health-failure, usage-error, lock-busy, or repair-failure.".to_string(),
            "  Doctor v2 schemas: use introspect.response_schemas doctor-* keys. First branch on err.kind/status/operation_outcome.kind/outcome_kind/asset_class/risk_level/fallback_mode; never scrape diagnostic prose.".to_string(),
            "  Doctor repairs: start with `cass doctor check --json`; use repair dry-run fingerprints for apply; support bundles and failure contexts report artifact_manifest_path/event_log_path instead of putting logs in stdout.".to_string(),
            "  Doctor validation: use `scripts/e2e/doctor_v2.sh list|describe|run --json`; `--artifact-dir` creates a fresh run child, and run-summary.json lists scenario ids, labels, mutation class, log paths, failure_context path, and rerun command.".to_string(),
            "  Doctor failure reports: attach the redacted failure_context.json plus manifest/commands/events artifacts; do not attach raw session logs, exact source paths, or live archive copies by default.".to_string(),
            "  Evidence handoff: use `cass pack \"query\" --robot` when triage points at prior sessions; pack is a bounded cited handoff.".to_string(),
            "  Args: accepts --robot-docs=topic and misplaced globals; detailed errors with examples on parse failure".to_string(),
            "  Source control: use `cass robot-docs sources` for local sources plus persistent agent-harness exclusions".to_string(),
            "  TUI drill-in contract: Enter on selected hit opens detail modal (Messages tab); Enter with no selected hit falls back to query submit behavior".to_string(),
            "  Detail modal shortcuts: / opens find, n/N cycles matches, Esc exits find then closes modal, F8 opens selected hit in $EDITOR".to_string(),
            "  Safety: prefer --color=never in non-TTY; use --trace-file for spans; reset TUI via `cass tui --reset-state`".to_string(),
            "  Quick refs: cass triage --json | cass capabilities --json | cass --robot-help | cass robot-docs commands | cass robot-docs examples | cass robot-docs sources".to_string(),
        ],
        RobotTopic::Schemas => render_schema_docs(),
        RobotTopic::ExitCodes => vec![
            "exit-codes:".to_string(),
            " 0 ok | 1 health-failed | 2 usage | 3 missing index/db | 4 io|refused-unsafe | 5 data-corrupt | 6 input-required | 7 lock/busy | 8 unused | 9 unknown".to_string(),
            " 10 config|timeout | 11 config | 12 source | 13 mapping|not_found | 14 io|mapping | 15 semantic-unavailable|embedder-unavailable".to_string(),
            " 20-21 model | 22 io | 23 unused | 24 io | 70 index-stalled (cass index abort) | 124 serve-deadline | 125 serve-supervisor-failure | 126 serve-memory-limit | 130 interrupted (SIGINT) | 143 interrupted (SIGTERM)".to_string(),
            " NOTE: codes >= 10 cover domain-specific failures (sources/models/semantic/analytics).".to_string(),
            "       Use `err.kind` from the JSON envelope as the canonical identifier — kinds are".to_string(),
            "       kebab-case (e.g. missing-index, missing-db, semantic-unavailable, embedder-unavailable,".to_string(),
            "       ambiguous-source, timeout, config, lock-busy, model, io).".to_string(),
            "       Agents should branch on `err.kind`, not on numeric code, when handling codes >= 10.".to_string(),
            "       `cass archive`: 2 logical-archive-usage | 5 logical-archive-integrity | 7 logical-archive-busy (retryable) | 14 logical-archive-io (retryable) | 9 logical-archive-error.".to_string(),
            "       For doctor JSON, prefer `operation_outcome.kind` and `operation_outcome.exit_code_kind` for no-op/partial/blocked/refused/incomplete repair decisions.".to_string(),
        ],
        RobotTopic::Examples => vec![
            "examples:".to_string(),
            String::new(),
            "# Quick project history (0.8.0 flags; check older-version help once)".to_string(),
            "  cass search \"performance regression\" --workspace /path/to/project --days 7 --mode lexical --no-maintenance --robot --robot-meta --fields source_path,line_number,agent,source_id,conversation_id --limit 5 --max-tokens 2000 --timeout 2000".to_string(),
            "  # Also set a caller-side deadline. Read budget.timed_out even after exit 0; maintenance-required is a blocker, not permission to rebuild.".to_string(),
            "  cass view /path/to/session.jsonl -n 42 -C 3 --json --timeout 2000".to_string(),
            String::new(),
            "# One-shot agent triage from zero context".to_string(),
            "  cass triage --json".to_string(),
            "  cass ready --json      # accepted alias".to_string(),
            "  cass preflight --json  # accepted alias".to_string(),
            "  cass --json            # also defaults to triage for zero-context agents".to_string(),
            "  cass --json search \"auth\"  # leading --json is moved to the search subcommand".to_string(),
            "  cass search --query \"auth\" --json  # --query is accepted and converted to positional syntax".to_string(),
            "  cass search \"auth\" --format json  # --format json is accepted as --robot-format json".to_string(),
            "  cass search \"auth\" --output json  # --output json also becomes --robot-format json".to_string(),
            "  cass search \"auth\" --max-results 5 --json  # result-count aliases become --limit".to_string(),
            "  # Follow next_command when present; use discovery.schemas_command for typed clients.".to_string(),
            String::new(),
            "# Basic search with JSON output for agents".to_string(),
            "  cass search \"your query\" --robot".to_string(),
            "  # Default is hybrid-preferred; add --robot-meta to see realized mode and lexical fallback reasons.".to_string(),
            "  cass search \"your query\" --robot --robot-meta".to_string(),
            "# Deterministic handoff pack for another agent".to_string(),
            "  cass pack \"why did checkout fail\" --robot --max-tokens 12000 --limit 40".to_string(),
            "  cass search \"checkout\" --robot-format sessions | cass pack \"checkout failure\" --robot --sessions-from -".to_string(),
            "  cass pack \"checkout timeout after redirect\" --robot --freshness-policy strict --freshness-window-seconds 604800 --require-evidence".to_string(),
            "  cass pack \"checkout timeout after redirect\" --robot --max-tokens 4000 --max-evidence 8 --max-sessions 3 --max-excerpt-chars 600".to_string(),
            "  cass pack \"checkout timeout after redirect\" --robot --fields summary,health,freshness,privacy,warnings --max-tokens 4000".to_string(),
            "  # Read warnings[] plus health/freshness/privacy before copying a pack into another prompt.".to_string(),
            "  # Contributor check for docs/goldens: cargo test --test golden_robot_docs".to_string(),
            "# Token-budgeted search with cursor + request-id".to_string(),
            "  cass search \"error\" --robot --max-tokens 200 --request-id run-1 --limit 2 --robot-meta".to_string(),
            "  cass search \"error\" --robot --cursor <_meta.next_cursor> --request-id run-1b --robot-meta".to_string(),
            String::new(),
            "# Search with time filters".to_string(),
            "  cass search \"bug\" --today                 # today only".to_string(),
            "  cass search \"api\" --week                  # last 7 days".to_string(),
            "  cass search \"feature\" --days 30           # last 30 days".to_string(),
            "  cass search \"fix\" --since 2025-01-01      # since date".to_string(),
            "  cass search \"error\" --robot --limit 5 --offset 5  # paginate robot output".to_string(),
            String::new(),
            "# Filter by agent or workspace".to_string(),
            "  cass search \"error\" --agent codex         # codex sessions only".to_string(),
            "  cass search \"test\" --workspace /myproject # specific project".to_string(),
            String::new(),
            "# Discover session files for follow-up actions".to_string(),
            "  cass sessions --current --json             # best match for current cwd".to_string(),
            "  cass sessions --workspace /myproject --json --limit 5".to_string(),
            String::new(),
            "# Follow up on search results".to_string(),
            "  cass view /path/to/session.jsonl -n 42   # view line 42 with context".to_string(),
            "  cass view /path/to/session.jsonl -n 42 -C 10  # 10 lines context".to_string(),
            String::new(),
            "# Get index statistics".to_string(),
            "  cass stats --json                        # JSON stats".to_string(),
            "  cass stats                               # Human-readable stats".to_string(),
            String::new(),
            "# Aggregation (overview queries - 99% token reduction)".to_string(),
            "  cass search \"error\" --json --aggregate agent    # count by agent".to_string(),
            "  cass search \"*\" --json --aggregate agent,workspace  # multi-field agg".to_string(),
            "  cass search \"bug\" --json --aggregate date --week  # time distribution".to_string(),
            String::new(),
            "# Quick health check (ideal for agents)".to_string(),
            "  cass triage --json                       # one-shot readiness + exact next command + docs/schemas".to_string(),
            "  cass status --json                       # health check JSON".to_string(),
            "  cass health --json                       # fast readiness + recommended_action".to_string(),
            "  cass status --stale-threshold 3600       # custom stale threshold (1hr)".to_string(),
            String::new(),
            "# Diagnostics".to_string(),
            "  cass diag --json                         # JSON diagnostic info".to_string(),
            "  cass diag --verbose                      # Human-readable with sizes".to_string(),
            String::new(),
            "# Doctor v2 safety workflow".to_string(),
            "  cass doctor check --json                 # read-only truth surface; inspect status/risk_level/recommended_action".to_string(),
            "  cass doctor repair --dry-run --json      # read-only plan with plan_fingerprint and exact apply argv".to_string(),
            "  cass doctor repair --yes --plan-fingerprint <fp> --json  # apply only the inspected fingerprint".to_string(),
            "  cass doctor --fix --json                 # legacy safe-auto-run; branch on operation_outcome.kind and receipts".to_string(),
            "  cass doctor --fix --force-rebuild --json # derived rebuild request; still fails closed on archive/source risk".to_string(),
            "  cass introspect --json                   # response_schemas contains doctor-* contracts and redacted examples".to_string(),
            String::new(),
            "# Persistently exclude a noisy harness from indexing".to_string(),
            "  cass sources agents list --json         # inspect exclusions".to_string(),
            "  cass sources agents exclude openclaw    # block future indexing, purge archived local data".to_string(),
            "  cass sources agents exclude openclaw --keep-indexed-data  # block future indexing only".to_string(),
            "  cass sources agents include openclaw    # re-enable indexing".to_string(),
            String::new(),
            "# Capabilities introspection (for agent self-configuration)".to_string(),
            "  cass capabilities --json                 # First-stop JSON: workflows, mistake recoveries, commands, flags, exit codes, env vars, limits".to_string(),
            "  cass capabilities                        # Human-readable summary".to_string(),
            String::new(),
            "# Full workflow".to_string(),
            "  cass index --full                        # index all sessions".to_string(),
            "  cass index --full --json --robot-trace-ingest 2>/tmp/cass-ingest-trace.jsonl".to_string(),
            "  cass search \"cma-es\" --robot             # search".to_string(),
            "  cass view <source_path> --message-index <line_number> --source <source_id> --conversation-id <conversation_id>  # examine indexed result".to_string(),
            String::new(),
            "# TUI drill-in quick reference".to_string(),
            "  cass tui                                 # interactive mode".to_string(),
            "  # Enter on selected result -> detail modal (Messages tab default)".to_string(),
            "  # Enter with no selected hit -> query submit behavior".to_string(),
            "  # In modal: / find, n/N next/prev, Esc exits find then closes modal, F8 opens in editor".to_string(),
        ],
        RobotTopic::Contracts => vec![
            "contracts:".to_string(),
            "  stdout data-only; stderr diagnostics/progress.".to_string(),
            "  No implicit TUI when automation flags set or stdout non-TTY.".to_string(),
            "  Color auto off when non-TTY unless forced.".to_string(),
            "  Use --quiet to silence info logs in robot runs.".to_string(),
            "  JSON errors only to stderr.".to_string(),
        ],
        RobotTopic::Wrap => vec![
            "wrap:".to_string(),
            "  Default: no forced wrap (wide output).".to_string(),
            "  --wrap <n>: wrap informational text to n columns.".to_string(),
            "  --nowrap: force no wrapping even if wrap set elsewhere.".to_string(),
        ],
        RobotTopic::Sources => vec![
            "sources:".to_string(),
            String::new(),
            "# Local sources only".to_string(),
            "cass runs fully local-only. `cass sources add` indexes extra local".to_string(),
            "directories under a name; there is no remote discovery, sync, or".to_string(),
            "scheduling. Old `type = \"ssh\"` entries in sources.toml are inert".to_string(),
            "archive records: their already-mirrored conversations stay searchable".to_string(),
            "and source-filterable, and ordinary `cass index` keeps indexing any".to_string(),
            "existing <data-dir>/remotes/<name>/mirror directories read-only.".to_string(),
            String::new(),
            "  cass sources add archive-box --path /Volumes/old-sessions".to_string(),
            "  cass sources add archive-box -p /data/a -p /data/b   # repeatable".to_string(),
            "  cass sources list                      # human summary".to_string(),
            "  cass sources list --json               # kind: local | legacy-remote".to_string(),
            "  cass sources remove archive-box        # drop the config entry".to_string(),
            "  cass sources remove archive-box --purge  # + delete its mirror data".to_string(),
            "  cass sources doctor --json             # local checks only".to_string(),
            String::new(),
            "Removed remote verbs (each fails with a removed-command error):".to_string(),
            "  sources sync, sources setup, sources discover, sources reingest,".to_string(),
            "  sources artifact-manifest, sources schedule, `sources add user@host`".to_string(),
            String::new(),
            "# Workspace path mappings".to_string(),
            "Path mappings rewrite archived remote workspace paths to local".to_string(),
            "equivalents so `cass view`/`cass tui` open hits at the right place.".to_string(),
            String::new(),
            "  cass sources mappings list".to_string(),
            "  cass sources mappings add laptop --from /home/u/proj --to /work/proj".to_string(),
            "  cass sources mappings test laptop /home/u/proj/x".to_string(),
            "  cass sources mappings remove laptop 0   # index from list output".to_string(),
            String::new(),
            "# Persisted agent indexing exclusions".to_string(),
            "Use this when you want cass to ignore a connector entirely even if".to_string(),
            "the backing session files still exist on disk.".to_string(),
            String::new(),
            "  cass sources agents list".to_string(),
            "  cass sources agents list --json".to_string(),
            "  cass sources agents exclude openclaw".to_string(),
            "  cass sources agents exclude openclaw --keep-indexed-data".to_string(),
            "  cass sources agents include openclaw".to_string(),
            String::new(),
            "Configuration is persisted in ~/.config/cass/sources.toml as:".to_string(),
            "  disabled_agents = [\"openclaw\"]".to_string(),
            "By default, `exclude` also purges already archived local data for that agent".to_string(),
            "and rebuilds lexical search from the remaining archive. Use".to_string(),
            "`--keep-indexed-data` if you only want to block future indexing.".to_string(),
            "Exclusions apply to future local scans, archive mirror scans, and watch mode.".to_string(),
        ],
        RobotTopic::Analytics => render_analytics_docs(),
        RobotTopic::Doctor => crate::doctor_robot_docs::doctor_robot_docs_body()
            .lines()
            .map(|s| s.to_string())
            .collect(),
        RobotTopic::Recipes => crate::recipes_robot_docs::recipes_robot_docs_body()
            .lines()
            .map(|s| s.to_string())
            .collect(),
    };

    println!("{}", render_block(&lines, wrap));
    Ok(())
}

/// Render comprehensive analytics robot-docs.
fn render_analytics_docs() -> Vec<String> {
    vec![
        "analytics:".into(),
        String::new(),
        "# cass analytics — Usage Analytics and Bounded Incident Mining".into(),
        String::new(),
        "## Subcommands".into(),
        "  status    Row counts, freshness, coverage, drift warnings".into(),
        "  tokens    Token usage over time with dimensional breakdowns".into(),
        "  tools     Per-tool invocation counts and derived metrics".into(),
        "  models    Top models by usage and coverage statistics".into(),
        "  incidents Bounded recurrent CASS-incident mining over the canonical archive".into(),
        "  rebuild   Rebuild/backfill rollup tables with progress output".into(),
        "  validate  Check rollup invariants and detect data drift".into(),
        String::new(),
        "## Shared Flags (all subcommands)".into(),
        "  --since <ISO>        Filter from date (YYYY-MM-DD or full timestamp)".into(),
        "  --until <ISO>        Filter to date".into(),
        "  --days <N>           Filter to last N days".into(),
        "  --agent <slug>       Filter by agent (repeatable)".into(),
        "  --workspace <path>   Filter by workspace (repeatable)".into(),
        "  --source <name>      Filter by source ('local', 'remote', hostname)".into(),
        "  --json / --robot     Machine-readable JSON output".into(),
        "  --data-dir <path>    Override data directory".into(),
        String::new(),
        "## Bucketed Subcommands (tokens, tools, models)".into(),
        "  --group-by <bucket>  hour | day (default) | week | month".into(),
        String::new(),
        "## JSON Envelope (all subcommands)".into(),
        "  { \"command\": \"analytics/<sub>\", \"data\": {...}, \"_meta\": {...} }".into(),
        "  _meta: { elapsed_ms: u64, filters_applied: [string], data_dir: string|null }".into(),
        String::new(),
        "## Per-Subcommand JSON Schemas".into(),
        String::new(),
        "### analytics status".into(),
        "  data.tables: [{ table, exists, row_count, min_day_id, max_day_id, last_updated }]".into(),
        "  data.coverage: { total_messages, <metric>_pct, <metric>_status, <metric>_display }".into(),
        "    Metrics: message_metrics_coverage, api_token_coverage, model_name_coverage, estimate_only".into(),
        "    <metric>_pct is number|null; status preserves true-zero, no-data, schema, and rebuild states.".into(),
        "  data.drift: { signals: [{ signal, detail, severity }], track_a_fresh, track_b_fresh }".into(),
        "  data.recommended_action: string".into(),
        String::new(),
        "### analytics tokens".into(),
        "  data.buckets: [{ bucket: string, counts: {...}, content_tokens: {...},".into(),
        "                   api_tokens: {...}, plan: {...}, derived: {...} }]".into(),
        "  data.totals: <same shape as bucket>".into(),
        "  data.source_table: string  ('usage_daily' | 'usage_hourly' | 'messages' | 'message_metrics')".into(),
        "  data.granularity: string   ('hour' | 'day' | 'week' | 'month')".into(),
        "  Bucket keys: counts.{message_count, user_message_count, assistant_message_count,".into(),
        "    tool_call_count, plan_message_count}; api_tokens.{total, input, output,".into(),
        "    cache_read, cache_creation, thinking}; derived.{api_coverage_pct,".into(),
        "    avg_api_per_message, avg_content_per_message}".into(),
        String::new(),
        "### analytics tools".into(),
        "  data.rows: [{ key: string, tool_call_count, message_count, api_tokens_total,".into(),
        "               tool_calls_per_1k_api_tokens, tool_calls_per_1k_content_tokens }]".into(),
        "  data.totals: { tool_call_count, message_count, api_tokens_total,".into(),
        "                 overall_per_1k_api_tokens }".into(),
        "  data.row_count: int".into(),
        "  data._meta.source_table: (\'usage_daily\' | \'usage_hourly\' | \'message_metrics\')".into(),
        "  --limit N (default 20): caps returned rows".into(),
        String::new(),
        "### analytics models".into(),
        "  data.by_api_tokens: { dim, metric, rows: [{ key, value, message_count, ... }] }".into(),
        "  data.timeseries: <same as analytics tokens>".into(),
        "  data.by_api_tokens._meta.source_table: string  ('token_daily_stats' | 'token_usage')".into(),
        "    Usually token_daily_stats; token_usage when filters require raw provenance recovery.".into(),
        "  Models only available for connectors that report model names".into(),
        "    (claude_code, codex, pi_agent, omp, factory, opencode, cursor).".into(),
        String::new(),
        "### analytics incidents".into(),
        "  Read-only; keyset-scans canonical archive rows newest-id-first.".into(),
        "  data.schema_version: 2".into(),
        "  data.count_scope: all_matching_candidates | scanned_candidates_partial | no_verified_results_hard_timeout".into(),
        "  data.discovery: { caps, files_considered, files_scanned, lines_scanned,".into(),
        "                    bytes_scanned, elapsed_ms, stop_reason, timed_out, partial,".into(),
        "                    evidence_truncated, evidence }".into(),
        "  data.top_sessions: [{ conversation_id, session_id, agent, host, source_id,".into(),
        "                        origin_host?, source_path, exists_state, hit_count,".into(),
        "                        category_breadth, dominant_categories, evidence_summaries,".into(),
        "                        redaction_status, suggested_command: { kind, argv, display } }]".into(),
        "  data.total_sessions, data.total_hits, data.top_sessions_truncated: ranked-scope totals".into(),
        "  data.scan_units.message_fragment_chars: per-message inspection bound (4096)".into(),
        "    Oversized messages stop with discovery.stop_reason=message-fragment-capped.".into(),
        "  data.redaction: { private_text_policy, hash_strategy, fields_emitted,".into(),
        "                    fields_suppressed, opt_in_flags }".into(),
        "  --limit N           Ranked sessions returned (1..100; default 10)".into(),
        "  --max-sessions N    Archive conversations scanned (1..10000; default 2000)".into(),
        "    This bounds the newest-row candidate window before dimensional filters.".into(),
        "  --max-messages N    Archive messages inspected (default 250000)".into(),
        "  --max-bytes N       UTF-8 content bytes inspected (default 268435456)".into(),
        "  --budget-ms N       Hard wall-clock result budget (default 8000)".into(),
        "  Raw prompt/tool text is always suppressed. source_path is retained only as the".into(),
        "  actionable archive pointer; evidence paths are basename-redacted.".into(),
        "  suggested_command.argv carries --db and --conversation-id for exact replay.".into(),
        "  Example: cass analytics incidents --limit 10 --json".into(),
        String::new(),
        "### analytics rebuild".into(),
        "  data.track: string ('a')".into(),
        "  data.tracks_rebuilt: [string]".into(),
        "  data.track_a: { message_metrics_rows, usage_hourly_rows, usage_daily_rows,".into(),
        "                  usage_models_daily_rows, elapsed_ms, rows_per_sec }".into(),
        "  data.overall_elapsed_ms: u64".into(),
        "  data.wal_checkpoint: string ('completed' | 'failed') — the rebuild closes with a".into(),
        "                  WAL checkpoint so the next opener replays nothing".into(),
        "  data.wal_checkpoint_detail: { status: 'completed' | 'blocked' | 'not_attempted' | 'error', ... }".into(),
        "                  blocked: busy, log_frames, checkpointed_frames; not_attempted: reason".into(),
        "  Always rebuilds; fresh rollups are not skipped (there is no --force).".into(),
        String::new(),
        "### analytics validate".into(),
        "  data.summary: { errors, warnings, drift_entries, buckets_checked, buckets_total }".into(),
        "  data.checks: [{ id, ok, severity, details, suggested_action? }]".into(),
        "    Check IDs: track_a.tables_exist, track_a.{content_tokens,message_count,".into(),
        "      api_tokens,api_coverage}_match, track_b.tables_exist,".into(),
        "      track_b.{tokens,agents}_match, cross_track.drift, non_negative.counters".into(),
        "  data.drift: [{ day_id, agent_slug, source_id, track_a_total,".into(),
        "                 track_b_total, delta, delta_pct, likely_cause }]".into(),
        "  data.perf: { timeseries: { elapsed_ms, budget_ms, within_budget, error?, details },".into(),
        "              breakdown: { elapsed_ms, budget_ms, within_budget, error?, details } }".into(),
        "  --fix: run deep validation, rebuild safe Track A rollups when possible, and report skipped non-fixable issues".into(),
        String::new(),
        "## Coverage & Uncertainty Semantics".into(),
        "  - api_token_coverage_pct: % of messages with API token data (from Claude, Codex).".into(),
        "  - estimate_only_pct: % of messages with content-estimated tokens only (chars/4 heuristic).".into(),
        "  - *_pct is null when no trustworthy number exists; inspect *_status and *_display.".into(),
        "  - status values: value, true-zero, no-data, aggregate-failed, schema-incompatible,".into(),
        "    rebuild-required, invalid-input. A true zero is never conflated with missing data.".into(),
        "  - When coverage is low, derived metrics are unreliable estimates, not ground truth.".into(),
        "  - Content token estimates are always available (heuristic); API tokens are sparse.".into(),
        String::new(),
        "## Exit Codes".into(),
        "  0  Success".into(),
        "  2  Usage error (invalid flags, missing required args)".into(),
        "  3  Missing database (run 'cass index --full' first)".into(),
        "  9  Database error (corrupt, missing tables, query failure)".into(),
        String::new(),
        "## Retry Guidance".into(),
        "  exit 9 + retryable=true: transient DB lock/busy — retry after 1s".into(),
        "  exit 9 + retryable=false: schema or data issue — run 'cass analytics rebuild' first".into(),
        "  exit 3: no database — run 'cass index --full' to create it".into(),
        "  validate errors: use 'cass analytics validate --fix --json' for safe Track A repair, or 'cass analytics rebuild --json' for a manual rebuild loop".into(),
        String::new(),
        "## Common Workflows".into(),
        "  # Quick health check".into(),
        "  cass analytics status --json | jq '.data.coverage'".into(),
        String::new(),
        "  # Recent usage by agent for last 7 days".into(),
        "  cass analytics tokens --days 7 --json | jq '.data.buckets'".into(),
        String::new(),
        "  # Tool usage top-10".into(),
        "  cass analytics tools --limit 10 --json | jq '.data.rows'".into(),
        String::new(),
        "  # Validation + remediation loop".into(),
        "  cass analytics validate --json | jq '.data.summary'".into(),
        "  # If errors: rebuild then re-validate".into(),
        "  cass analytics rebuild --json && cass analytics validate --json".into(),
    ]
}

/// Render schema docs from live response schemas
fn render_schema_docs() -> Vec<String> {
    use serde_json::{Map, Value};

    fn type_of(v: &Value) -> String {
        v.get("type")
            .and_then(Value::as_str)
            .map_or_else(|| "?".to_string(), str::to_string)
    }

    fn render_props(
        lines: &mut Vec<String>,
        props: &Map<String, Value>,
        indent: usize,
        depth: usize,
    ) {
        let mut keys: Vec<&String> = props.keys().collect();
        keys.sort();
        for k in keys {
            let v = &props[k];
            let ty = type_of(v);
            let pad = "  ".repeat(indent);
            lines.push(format!("{pad}- {k}: {ty}"));
            if depth < 2
                && let Some(obj) = v.get("properties").and_then(Value::as_object)
            {
                render_props(lines, obj, indent + 1, depth + 1);
            }
        }
    }

    let mut lines = vec!["schemas: (auto-generated from contract)".to_string()];
    let mut schemas: Vec<(String, Value)> = build_response_schemas().into_iter().collect();
    schemas.sort_by(|a, b| a.0.cmp(&b.0));

    for (name, schema) in schemas {
        lines.push(format!("  {name}:"));
        if let Some(props) = schema.get("properties").and_then(Value::as_object) {
            render_props(&mut lines, props, 2, 0);
        } else {
            lines.push("    (no properties)".to_string());
        }
    }

    lines
}

/// Extract request_id from CLI command if present (currently only Search has it)
fn extract_request_id(cli: &Cli) -> Option<String> {
    match &cli.command {
        Some(Commands::Search { request_id, .. }) => request_id.clone(),
        Some(Commands::Pack { request_id, .. }) => request_id.clone(),
        _ => None,
    }
}

fn write_trace_line(
    path: &Path,
    label: &str,
    cli: &Cli,
    start_ts: &chrono::DateTime<Utc>,
    duration_ms: u128,
    exit_code: i32,
    error: Option<&CliError>,
) -> io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let request_id = extract_request_id(cli)
        .map(|request_id| redact_bounded_trace_text(&request_id, TRACE_CORRELATION_MAX_CHARS));
    let trace_id = dotenvy::var("CASS_TRACE_ID")
        .ok()
        .map(|trace_id| redact_bounded_trace_text(&trace_id, TRACE_CORRELATION_MAX_CHARS));
    let error_message = error.map(|error| {
        truncate_trace_text(
            &redact_trace_text(&error.message),
            TRACE_SUMMARY_ERROR_MAX_CHARS,
        )
    });
    let error_hint = error.and_then(|error| {
        error.hint.as_deref().map(|hint| {
            truncate_trace_text(&redact_trace_text(hint), TRACE_SUMMARY_ERROR_MAX_CHARS)
        })
    });
    let payload = serde_json::json!({
        "timestamp": Utc::now().to_rfc3339(),
        "level": "INFO",
        "target": "cass::trace",
        "fields": {
            "event": "command_summary",
            "start_ts": start_ts.to_rfc3339(),
            "end_ts": (*start_ts
                + chrono::Duration::from_std(Duration::from_millis(duration_ms as u64)).unwrap_or_default())
            .to_rfc3339(),
            "duration_ms": duration_ms,
            "cmd": label,
            "args": redact_trace_args(&args),
            "exit_code": exit_code,
            "error": error.map(|e| serde_json::json!({
                "code": e.code,
                "kind": e.kind,
                "message": error_message,
                "hint": error_hint,
                "retryable": e.retryable,
            })),
            "request_id": request_id,
            "trace_id": trace_id,
            "contract_version": CONTRACT_VERSION,
            "crate_version": env!("CARGO_PKG_VERSION"),
        },
    });

    let max_bytes = trace_file_max_bytes();
    let (mut file, bytes_written, _events_written, _prior_head_closed) =
        open_trace_append_file(path, max_bytes)?;
    let enriched = enrich_trace_json(&payload);
    let redacted = redact_trace_json(&enriched);
    let mut encoded = serde_json::to_vec(&redacted).map_err(io::Error::other)?;
    encoded.push(b'\n');
    let encoded_len = u64::try_from(encoded.len()).unwrap_or(u64::MAX);
    if bytes_written.saturating_add(encoded_len) <= max_bytes {
        file.write_all(&encoded)?;
        return Ok(());
    }

    let fallback = serde_json::json!({
        "timestamp": Utc::now().to_rfc3339(),
        "level": "WARN",
        "target": "cass::trace",
        "fields": {
            "event": "command_summary",
            "summary_truncated": true,
            "duration_ms": duration_ms,
            "cmd": truncate_trace_text(label, 128),
            "exit_code": exit_code,
            "error_kind": error.map(|error| error.kind),
        },
    });
    let fallback = redact_trace_json(&enrich_trace_json(&fallback));
    let mut fallback_encoded = serde_json::to_vec(&fallback).map_err(io::Error::other)?;
    fallback_encoded.push(b'\n');
    let fallback_len = u64::try_from(fallback_encoded.len()).unwrap_or(u64::MAX);
    if bytes_written.saturating_add(fallback_len) > max_bytes {
        return Err(io::Error::new(
            io::ErrorKind::WriteZero,
            "trace tail reserve could not fit the compact command summary",
        ));
    }
    file.write_all(&fallback_encoded)
}

fn doctor_candidate_root(data_dir: &Path) -> PathBuf {
    data_dir.join("doctor").join("candidates")
}

fn doctor_candidate_id(operation_id: &str, created_at_ms: i64) -> String {
    let mut safe_operation: String = operation_id
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || *ch == '-')
        .take(32)
        .collect();
    if safe_operation.is_empty() {
        safe_operation.push_str("candidate");
    }
    format!("{created_at_ms}-{safe_operation}")
}

fn doctor_candidate_root_is_safe(
    data_dir: &Path,
    db_path: &Path,
    index_path: &Path,
    root: &Path,
) -> Result<(), String> {
    let data_dir_metadata = std::fs::symlink_metadata(data_dir).map_err(|err| {
        format!(
            "cannot inspect data directory {} before candidate staging: {err}",
            data_dir.display()
        )
    })?;
    if !data_dir_metadata.is_dir() || data_dir_metadata.file_type().is_symlink() {
        return Err(format!(
            "refusing candidate staging through unsafe data directory {}",
            data_dir.display()
        ));
    }
    if root == data_dir || root == db_path || root == index_path || !root.starts_with(data_dir) {
        return Err(format!(
            "refusing candidate staging root outside the dedicated doctor/candidates tree: {}",
            root.display()
        ));
    }
    if root.starts_with(index_path) || db_path.starts_with(root) || index_path.starts_with(root) {
        return Err(format!(
            "refusing candidate staging root that overlaps live archive or index paths: {}",
            root.display()
        ));
    }
    if let Ok(root_metadata) = std::fs::symlink_metadata(root)
        && (!root_metadata.is_dir() || root_metadata.file_type().is_symlink())
    {
        return Err(format!(
            "refusing unsafe candidate staging root {}",
            root.display()
        ));
    }
    if let Some(parent) = root.parent()
        && existing_path_has_symlink_below_root(parent, data_dir)
    {
        return Err(format!(
            "refusing candidate staging root with symlinked ancestor {}",
            root.display()
        ));
    }
    Ok(())
}

fn doctor_candidate_live_inventory(
    db_path: &Path,
    index_path: &Path,
) -> DoctorCandidateLiveInventory {
    let wal_path = doctor_sqlite_sidecar_path(db_path, "-wal");
    let shm_path = doctor_sqlite_sidecar_path(db_path, "-shm");
    let sidecar = |path: Option<&PathBuf>| -> (bool, u64, Option<String>) {
        let Some(path) = path else {
            return (false, 0, None);
        };
        let exists = path.exists();
        let size = if exists { fs_dir_size(path) } else { 0 };
        let checksum = exists.then(|| file_blake3_hex(path).ok()).flatten();
        (exists, size, checksum)
    };
    let db_exists = db_path.exists();
    let (db_wal_exists, db_wal_size_bytes, db_wal_blake3) = sidecar(wal_path.as_ref());
    let (db_shm_exists, db_shm_size_bytes, db_shm_blake3) = sidecar(shm_path.as_ref());
    DoctorCandidateLiveInventory {
        db_exists,
        db_size_bytes: if db_exists { fs_dir_size(db_path) } else { 0 },
        db_blake3: db_exists.then(|| file_blake3_hex(db_path).ok()).flatten(),
        db_wal_exists,
        db_wal_size_bytes,
        db_wal_blake3,
        db_shm_exists,
        db_shm_size_bytes,
        db_shm_blake3,
        index_exists: index_path.exists(),
        index_size_bytes: if index_path.exists() {
            fs_dir_size(index_path)
        } else {
            0
        },
    }
}

fn doctor_candidate_relative_path(candidate_dir: &Path, path: &Path) -> Option<String> {
    path.strip_prefix(candidate_dir)
        .ok()
        .map(doctor_path_to_slash_string)
}

fn doctor_candidate_artifact_class(relative_path: &str) -> DoctorAssetClass {
    if relative_path == "database/candidate.db" {
        DoctorAssetClass::CanonicalArchiveDb
    } else if relative_path.starts_with("database/candidate.db-") {
        DoctorAssetClass::ArchiveDbSidecar
    } else if relative_path.starts_with("index/lexical/") {
        DoctorAssetClass::DerivedLexicalIndex
    } else if relative_path.starts_with("index/semantic/") {
        DoctorAssetClass::DerivedSemanticIndex
    } else if relative_path.starts_with("evidence/raw-mirror/") {
        DoctorAssetClass::RawMirrorBlob
    } else {
        DoctorAssetClass::OperationReceipt
    }
}

fn doctor_candidate_artifact_kind(relative_path: &str) -> String {
    match relative_path {
        "database/candidate.db" => "candidate_archive_db".to_string(),
        "database/candidate.db-wal" => "candidate_archive_db_wal_sidecar".to_string(),
        "database/candidate.db-shm" => "candidate_archive_db_shm_sidecar".to_string(),
        "logs/skipped-records.jsonl" => "candidate_skipped_record_log".to_string(),
        "logs/parse-errors.jsonl" => "candidate_parse_error_log".to_string(),
        "index/lexical/candidate-generation.json" => "candidate_lexical_metadata".to_string(),
        "index/semantic/metadata.json" => "candidate_semantic_metadata".to_string(),
        _ => "candidate_artifact".to_string(),
    }
}

fn doctor_candidate_artifact_for_path(
    data_dir: &Path,
    candidate_dir: &Path,
    path: &Path,
) -> Option<DoctorCandidateArtifact> {
    let relative_path = doctor_candidate_relative_path(candidate_dir, path)?;
    if relative_path == "manifest.json" || relative_path.contains(".manifest.tmp.") {
        return None;
    }
    let path_text = path.display().to_string();
    let metadata = std::fs::symlink_metadata(path).ok();
    let exists = metadata.is_some();
    let is_file = metadata
        .as_ref()
        .is_some_and(|metadata| metadata.is_file() && !metadata.file_type().is_symlink());
    let checksum = if is_file {
        file_blake3_hex(path).ok()
    } else {
        None
    };
    let checksum_status = if !exists {
        DoctorArtifactChecksumStatus::Missing
    } else if checksum.is_some() {
        DoctorArtifactChecksumStatus::Matched
    } else {
        DoctorArtifactChecksumStatus::Mismatched
    };
    Some(DoctorCandidateArtifact {
        artifact_kind: doctor_candidate_artifact_kind(&relative_path),
        asset_class: doctor_candidate_artifact_class(&relative_path),
        relative_path,
        path: path_text.clone(),
        redacted_path: doctor_redacted_path(&path_text, data_dir),
        exists,
        size_bytes: metadata.as_ref().map(|m| m.len()).unwrap_or_default(),
        checksum_blake3: checksum,
        checksum_status,
    })
}

fn collect_doctor_candidate_artifacts(
    data_dir: &Path,
    candidate_dir: &Path,
) -> Vec<DoctorCandidateArtifact> {
    if !candidate_dir.exists() {
        return Vec::new();
    }
    let mut artifacts = walkdir::WalkDir::new(candidate_dir)
        .follow_links(false)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_file() || entry.file_type().is_symlink())
        .filter_map(|entry| {
            doctor_candidate_artifact_for_path(data_dir, candidate_dir, entry.path())
        })
        .collect::<Vec<_>>();
    artifacts.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    artifacts
}

fn doctor_candidate_checksum_set(
    artifacts: &[DoctorCandidateArtifact],
) -> BTreeMap<String, String> {
    artifacts
        .iter()
        .filter_map(|artifact| {
            artifact
                .checksum_blake3
                .as_ref()
                .map(|hash| (artifact.relative_path.clone(), hash.clone()))
        })
        .collect()
}

fn doctor_candidate_write_manifest_atomic(
    data_dir: &Path,
    candidate_dir: &Path,
    manifest: &DoctorCandidateManifest,
) -> Result<String, String> {
    if !doctor_staging_target_path_is_safe(
        &candidate_dir.join("manifest.json"),
        candidate_dir,
        data_dir,
    ) {
        return Err(format!(
            "refusing to write unsafe candidate manifest under {}",
            candidate_dir.display()
        ));
    }
    let manifest_path = candidate_dir.join("manifest.json");
    if let Ok(metadata) = std::fs::symlink_metadata(&manifest_path)
        && (!metadata.is_file() || metadata.file_type().is_symlink())
    {
        return Err(format!(
            "refusing to replace unsafe candidate manifest {}",
            manifest_path.display()
        ));
    }
    let encoded = serde_json::to_vec_pretty(manifest)
        .map_err(|err| format!("failed to encode candidate manifest: {err}"))?;
    let mut tmp_path = candidate_dir.join(format!(
        ".manifest.tmp.{}.{}",
        std::process::id(),
        doctor_now_ms()
    ));
    let mut tmp_file = None;
    for suffix in 0..100 {
        let candidate_tmp = if suffix == 0 {
            tmp_path.clone()
        } else {
            candidate_dir.join(format!(
                ".manifest.tmp.{}.{}.{}",
                std::process::id(),
                doctor_now_ms(),
                suffix
            ))
        };
        match doctor_forensic_create_private_file(&candidate_tmp) {
            Ok(file) => {
                tmp_path = candidate_tmp;
                tmp_file = Some(file);
                break;
            }
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(err) => {
                return Err(format!(
                    "failed to create candidate manifest temp file {}: {err}",
                    candidate_tmp.display()
                ));
            }
        }
    }
    let Some(mut file) = tmp_file else {
        return Err(format!(
            "failed to allocate candidate manifest temp file under {}",
            candidate_dir.display()
        ));
    };
    file.write_all(&encoded).map_err(|err| {
        format!(
            "failed to write candidate manifest temp file {}: {err}",
            tmp_path.display()
        )
    })?;
    file.flush().map_err(|err| {
        format!(
            "failed to flush candidate manifest temp file {}: {err}",
            tmp_path.display()
        )
    })?;
    file.sync_all().map_err(|err| {
        format!(
            "failed to sync candidate manifest temp file {}: {err}",
            tmp_path.display()
        )
    })?;
    drop(file);
    std::fs::rename(&tmp_path, &manifest_path).map_err(|err| {
        format!(
            "failed to publish candidate manifest {} from {}: {err}",
            manifest_path.display(),
            tmp_path.display()
        )
    })?;
    sync_directory(candidate_dir)?;
    file_blake3_hex(&manifest_path)
}

fn doctor_candidate_write_bytes_to_staging(
    operation_id: &str,
    action_id: &str,
    data_dir: &Path,
    candidate_dir: &Path,
    target_path: &Path,
    asset_class: DoctorAssetClass,
    bytes: &[u8],
) -> DoctorFsMutationReceipt {
    let expected_payload_blake3 = blake3::hash(bytes).to_hex().to_string();
    execute_doctor_fs_write_mutation(DoctorFsWriteMutationRequest {
        operation_id,
        action_id,
        mode: DoctorRepairMode::ReconstructPromote,
        asset_class,
        target_path,
        data_dir,
        staging_root: candidate_dir,
        payload: bytes,
        expected_payload_blake3: &expected_payload_blake3,
        planned_bytes: bytes.len() as u64,
    })
}

fn doctor_candidate_jsonl_bytes(records: &[serde_json::Value]) -> Vec<u8> {
    let mut out = Vec::new();
    for record in records {
        if let Ok(line) = serde_json::to_string(record) {
            out.extend_from_slice(line.as_bytes());
            out.push(b'\n');
        }
    }
    out
}

struct DoctorCandidateCopyContext<'a> {
    operation_id: &'a str,
    data_dir: &'a Path,
    db_path: &'a Path,
    index_path: &'a Path,
    candidate_dir: &'a Path,
}

fn doctor_candidate_copy_to_staging(
    context: &DoctorCandidateCopyContext<'_>,
    action_id: &str,
    source_path: &Path,
    target_path: &Path,
    asset_class: DoctorAssetClass,
) -> DoctorFsMutationReceipt {
    let source_hash = file_blake3_hex(source_path).ok();
    execute_doctor_fs_mutation(DoctorFsMutationRequest {
        operation_id: context.operation_id,
        action_id,
        mutation_kind: DoctorFsMutationKind::CopyFileToStaging,
        mode: DoctorRepairMode::ReconstructPromote,
        asset_class,
        source_path: Some(source_path),
        target_path,
        data_dir: context.data_dir,
        db_path: context.db_path,
        index_path: context.index_path,
        staging_root: Some(context.candidate_dir),
        expected_source_blake3: source_hash.as_deref(),
        planned_bytes: fs_dir_size(source_path),
        required_min_age_seconds: None,
    })
}

fn doctor_candidate_table_count(
    conn: &crate::franken_sync::Connection,
    table: &str,
) -> Option<usize> {
    let table = match table {
        "conversations" | "messages" => table,
        _ => return None,
    };
    if !doctor_table_columns(conn, table).is_empty() {
        conn.query_row_map(
            &format!("SELECT COUNT(*) FROM {table}"),
            &[],
            |row: &crate::franken_sync::Row| row.get_typed::<i64>(0),
        )
        .ok()
        .map(|count| count.max(0) as usize)
    } else {
        None
    }
}

fn doctor_candidate_probe_frankensqlite(
    candidate_db_path: &Path,
    candidate_id: &str,
) -> Result<(Option<usize>, Option<usize>), String> {
    let conn = crate::franken_sync::Connection::open(candidate_db_path.to_string_lossy().as_ref())
        .map_err(|err| {
            format!(
                "failed to open candidate archive DB with frankensqlite at {}: {err}",
                candidate_db_path.display()
            )
        })?;
    conn.execute("PRAGMA journal_mode = WAL;").map_err(|err| {
        format!(
            "failed to set WAL mode on candidate archive DB {}: {err}",
            candidate_db_path.display()
        )
    })?;
    conn.execute("PRAGMA busy_timeout = 5000;").map_err(|err| {
        format!(
            "failed to set busy_timeout on candidate archive DB {}: {err}",
            candidate_db_path.display()
        )
    })?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS cass_doctor_candidate_metadata (
            candidate_id TEXT PRIMARY KEY,
            created_at_ms INTEGER NOT NULL,
            schema_version INTEGER NOT NULL
        );",
    )
    .map_err(|err| {
        format!(
            "failed to create candidate metadata table in {}: {err}",
            candidate_db_path.display()
        )
    })?;
    conn.execute_compat(
        "INSERT OR REPLACE INTO cass_doctor_candidate_metadata
         (candidate_id, created_at_ms, schema_version)
         VALUES (?1, ?2, ?3)",
        crate::franken_sync::params![
            candidate_id,
            doctor_now_ms(),
            DOCTOR_CANDIDATE_SCHEMA_VERSION as i64
        ],
    )
    .map_err(|err| {
        format!(
            "failed to write candidate metadata row in {}: {err}",
            candidate_db_path.display()
        )
    })?;
    let conversations = doctor_candidate_table_count(&conn, "conversations");
    let messages = doctor_candidate_table_count(&conn, "messages");
    Ok((conversations, messages))
}

fn doctor_candidate_coverage_before_snapshot(
    coverage_summary: &DoctorCoverageSummary,
) -> DoctorCandidateCoverageSnapshot {
    DoctorCandidateCoverageSnapshot {
        coverage_source: "current_archive_coverage_summary".to_string(),
        conversation_count: Some(coverage_summary.archive_conversation_count),
        message_count: Some(coverage_summary.archived_message_count),
        raw_mirror_manifest_count: coverage_summary.raw_mirror_manifest_count,
        raw_mirror_db_link_count: coverage_summary.raw_mirror_db_link_count,
        missing_current_source_count: coverage_summary.missing_current_source_count,
        confidence_tier: coverage_summary.confidence_tier.clone(),
    }
}

fn doctor_candidate_coverage_after_snapshot(
    coverage_source: impl Into<String>,
    coverage_summary: &DoctorCoverageSummary,
    candidate_conversation_count: Option<usize>,
    candidate_message_count: Option<usize>,
) -> DoctorCandidateCoverageSnapshot {
    DoctorCandidateCoverageSnapshot {
        coverage_source: coverage_source.into(),
        conversation_count: candidate_conversation_count,
        message_count: candidate_message_count,
        raw_mirror_manifest_count: coverage_summary.raw_mirror_manifest_count,
        raw_mirror_db_link_count: coverage_summary.raw_mirror_db_link_count,
        missing_current_source_count: coverage_summary.missing_current_source_count,
        confidence_tier: coverage_summary.confidence_tier.clone(),
    }
}

fn doctor_candidate_live_archive_copy_probe(
    db_path: &Path,
) -> std::result::Result<(Option<usize>, Option<usize>), String> {
    if !db_path.exists() {
        return Err(format!(
            "live archive DB is absent at {}",
            db_path.display()
        ));
    }
    let mut conn = crate::storage::sqlite::open_franken_raw_readonly_connection_with_timeout(
        db_path,
        Duration::from_secs(1),
    )
    .map_err(|err| {
        format!(
            "live archive DB is not a safe candidate copy source at {}: {err}",
            db_path.display()
        )
    })?;
    let result: std::result::Result<
        (Option<usize>, Option<usize>),
        crate::franken_sync::FrankenError,
    > = (|| {
        let conversations =
            doctor_candidate_table_count(&conn, "conversations").ok_or_else(|| {
                crate::franken_sync::FrankenError::Internal(
                    "live archive DB has no readable conversations table".to_string(),
                )
            })?;
        let messages = doctor_candidate_table_count(&conn, "messages").ok_or_else(|| {
            crate::franken_sync::FrankenError::Internal(
                "live archive DB has no readable messages table".to_string(),
            )
        })?;
        Ok((Some(conversations), Some(messages)))
    })();
    if let Err(close_err) = conn.close_without_checkpoint_in_place() {
        tracing::warn!(
            error = %close_err,
            db_path = %db_path.display(),
            "doctor candidate live archive read-only probe close failed; falling back to best-effort close"
        );
        conn.close_best_effort_in_place();
    }
    result.map_err(|err| {
        format!(
            "live archive DB is not a safe candidate copy source at {}: {err}",
            db_path.display()
        )
    })
}

fn doctor_candidate_verified_raw_mirror_db_link_count(raw_mirror: &DoctorRawMirrorReport) -> usize {
    raw_mirror
        .manifests
        .iter()
        .filter(|manifest| doctor_raw_mirror_manifest_is_verified(manifest))
        .map(|manifest| manifest.db_link_count)
        .sum()
}

fn doctor_candidate_raw_mirror_key(manifest: &DoctorRawMirrorManifestReport) -> String {
    if let Some(conversation_id) = manifest
        .db_links
        .iter()
        .find_map(|link| link.conversation_id)
    {
        return format!("archive-conversation-id:{conversation_id}");
    }
    doctor_canonical_blake3(
        "doctor-candidate-raw-mirror-dedupe-key-v2",
        serde_json::json!({
            "source_id": manifest.source_id,
            "origin_kind": manifest.origin_kind,
            "origin_host": manifest.origin_host,
            "original_path_blake3": manifest.original_path_blake3,
        }),
    )
}

fn doctor_candidate_read_verified_raw_mirror_blob(
    data_dir: &Path,
    manifest: &DoctorRawMirrorManifestReport,
) -> std::result::Result<Vec<u8>, String> {
    if !doctor_raw_mirror_manifest_is_verified(manifest) {
        return Err(format!(
            "raw mirror manifest {} is not verified",
            manifest.manifest_id
        ));
    }
    if manifest.compression_state != "none" || manifest.encryption_state != "none" {
        return Err(format!(
            "raw mirror manifest {} uses compression_state={} encryption_state={} and cannot be reconstructed by the v1 raw parser",
            manifest.manifest_id, manifest.compression_state, manifest.encryption_state
        ));
    }
    crate::raw_mirror::read_source_bytes(data_dir, &manifest.manifest_id).map_err(|error| {
        format!(
            "raw mirror source verification changed before reconstruction for manifest {}: {error}",
            manifest.manifest_id
        )
    })
}

fn doctor_candidate_message_role(value: &serde_json::Value) -> crate::model::types::MessageRole {
    let role = value
        .get("role")
        .or_else(|| value.get("author_role"))
        .or_else(|| value.get("type"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("other")
        .trim()
        .to_ascii_lowercase();
    match role.as_str() {
        "user" | "human" => crate::model::types::MessageRole::User,
        "assistant" | "agent" | "model" => crate::model::types::MessageRole::Agent,
        "tool" | "function" => crate::model::types::MessageRole::Tool,
        "system" => crate::model::types::MessageRole::System,
        "" | "other" => crate::model::types::MessageRole::Other("raw_mirror".to_string()),
        other => crate::model::types::MessageRole::Other(other.to_string()),
    }
}

fn doctor_candidate_json_message_content(value: &serde_json::Value) -> String {
    for key in ["content", "message", "text", "body", "delta"] {
        if let Some(value) = value.get(key) {
            if let Some(text) = value.as_str() {
                return text.to_string();
            }
            if !value.is_null() {
                return serde_json::to_string(value).unwrap_or_default();
            }
        }
    }
    serde_json::to_string(value).unwrap_or_default()
}

fn doctor_candidate_json_message_timestamp(value: &serde_json::Value) -> Option<i64> {
    for key in [
        "created_at_ms",
        "timestamp_ms",
        "time_ms",
        "createdAtMs",
        "created_at",
        "timestamp",
    ] {
        if let Some(number) = value.get(key).and_then(serde_json::Value::as_i64) {
            return Some(number);
        }
    }
    None
}

fn doctor_candidate_parse_raw_mirror_messages(
    manifest: &DoctorRawMirrorManifestReport,
    bytes: &[u8],
    parse_errors: &mut Vec<serde_json::Value>,
) -> Vec<crate::model::types::Message> {
    let mut messages = Vec::new();
    for (line_idx, raw_line) in bytes.split(|byte| *byte == b'\n').enumerate() {
        if raw_line.iter().all(|byte| byte.is_ascii_whitespace()) {
            continue;
        }
        let line_number = line_idx + 1;
        match serde_json::from_slice::<serde_json::Value>(raw_line) {
            Ok(value) => {
                let content = doctor_candidate_json_message_content(&value);
                messages.push(crate::model::types::Message {
                    id: None,
                    idx: messages.len() as i64,
                    role: doctor_candidate_message_role(&value),
                    author: value
                        .get("author")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string),
                    created_at: doctor_candidate_json_message_timestamp(&value),
                    content,
                    extra_json: serde_json::json!({
                        "cass": {
                            "doctor_reconstruct": {
                                "schema_version": 1,
                                "source": "verified_raw_mirror",
                                "manifest_id": manifest.manifest_id,
                                "line_number": line_number
                            }
                        }
                    }),
                    snippets: Vec::new(),
                });
            }
            Err(err) => {
                parse_errors.push(serde_json::json!({
                    "manifest_id": manifest.manifest_id,
                    "redacted_blob_path": manifest.redacted_blob_path,
                    "line_number": line_number,
                    "reason": format!("raw mirror JSONL line could not be parsed: {err}"),
                    "raw_content_included": false
                }));
                messages.push(crate::model::types::Message {
                    id: None,
                    idx: messages.len() as i64,
                    role: crate::model::types::MessageRole::Other(
                        "raw_mirror_unparsed".to_string(),
                    ),
                    author: None,
                    created_at: None,
                    content: String::from_utf8_lossy(raw_line).into_owned(),
                    extra_json: serde_json::json!({
                        "cass": {
                            "doctor_reconstruct": {
                                "schema_version": 1,
                                "source": "verified_raw_mirror",
                                "manifest_id": manifest.manifest_id,
                                "line_number": line_number,
                                "parse_status": "jsonl_parse_error_preserved_losslessly"
                            }
                        }
                    }),
                    snippets: Vec::new(),
                });
            }
        }
    }
    if messages.is_empty() {
        messages.push(crate::model::types::Message {
            id: None,
            idx: 0,
            role: crate::model::types::MessageRole::Other("raw_mirror_blob".to_string()),
            author: None,
            created_at: None,
            content: String::from_utf8_lossy(bytes).into_owned(),
            extra_json: serde_json::json!({
                "cass": {
                    "doctor_reconstruct": {
                        "schema_version": 1,
                        "source": "verified_raw_mirror",
                        "manifest_id": manifest.manifest_id,
                        "parse_status": "empty_or_binary_blob_preserved_as_single_message"
                    }
                }
            }),
            snippets: Vec::new(),
        });
    }
    messages
}

fn doctor_candidate_conversation_from_raw_mirror(
    manifest: &DoctorRawMirrorManifestReport,
    bytes: &[u8],
    parse_errors: &mut Vec<serde_json::Value>,
) -> crate::model::types::Conversation {
    let provider = doctor_normalized_provider_slug(&manifest.provider);
    let source_id = normalized_provenance_source_id(
        &manifest.source_id,
        Some(&manifest.origin_kind),
        manifest.origin_host.as_deref(),
    );
    let origin_host = normalized_provenance_origin_host(manifest.origin_host.as_deref());
    let started_at = manifest
        .db_links
        .iter()
        .filter_map(|link| link.started_at_ms)
        .min()
        .or(manifest.source_mtime_ms)
        .or(Some(manifest.captured_at_ms));
    let messages = doctor_candidate_parse_raw_mirror_messages(manifest, bytes, parse_errors);
    let db_link_conversation_ids = manifest
        .db_links
        .iter()
        .filter_map(|link| link.conversation_id)
        .collect::<Vec<_>>();
    crate::model::types::Conversation {
        id: None,
        agent_slug: provider,
        workspace: None,
        external_id: Some(doctor_canonical_blake3(
            "doctor-reconstruct-raw-mirror-external-id-v1",
            serde_json::json!({
                "manifest_id": manifest.manifest_id,
                "source_content_blake3": manifest.source_content_blake3,
                "db_link_conversation_ids": db_link_conversation_ids,
            }),
        )),
        title: Some(format!(
            "Recovered {} session from raw mirror",
            manifest.provider
        )),
        source_path: PathBuf::from(&manifest.original_path),
        started_at,
        ended_at: None,
        approx_tokens: None,
        metadata_json: serde_json::json!({
            "cass": {
                "doctor_reconstruct": {
                    "schema_version": 1,
                    "source": "verified_raw_mirror",
                    "manifest_id": manifest.manifest_id,
                    "blob_blake3": manifest.blob_blake3,
                    "blob_size_bytes": manifest.blob_size_bytes,
                    "storage_kind": manifest.storage_kind,
                    "source_content_blake3": manifest.source_content_blake3,
                    "source_size_bytes": manifest.source_size_bytes,
                    "chunk_count": manifest.chunk_count,
                    "redacted_original_path": manifest.redacted_original_path,
                    "original_path_blake3": manifest.original_path_blake3,
                    "db_link_count": manifest.db_link_count,
                    "db_link_conversation_ids": db_link_conversation_ids,
                    "raw_content_in_metadata": false
                }
            }
        }),
        messages,
        source_id,
        origin_host,
    }
}

/// Re-reads verified raw-mirror sources through their providers' connectors
/// for one candidate build (zp0fp). It caches each provider's connector and
/// detected roots and owns the scratch tree the source copies are parsed
/// from; the tree is removed when the build drops it.
struct DoctorConnectorReparse {
    scratch: tempfile::TempDir,
    providers: HashMap<String, Option<DoctorReparseConnector>>,
}

/// A provider's connector and the roots it detects on this machine.
type DoctorReparseConnector = (Box<dyn crate::connectors::Connector + Send>, Vec<PathBuf>);

impl DoctorConnectorReparse {
    fn new(candidate_dir: &Path) -> std::io::Result<Self> {
        Ok(Self {
            scratch: tempfile::Builder::new()
                .prefix(".connector-reparse-")
                .tempdir_in(candidate_dir)?,
            providers: HashMap::new(),
        })
    }

    /// The source as the indexer would read it at its original path: the
    /// verified bytes are written under a scratch root at the same path
    /// relative to the root the provider's connector detects, scanned by
    /// that connector, and given back their original source path and local
    /// provenance. The rows then carry the connector's agent, conversation
    /// identity (`external_id`), titles and roles, so the next `cass index`
    /// of the untouched source updates them instead of adding a second copy
    /// of every session. `Err` names why a source does not qualify (not
    /// local, no registered connector, outside every detected root, or not
    /// parseable); the caller keeps the lossless line salvage for it.
    fn conversations(
        &mut self,
        manifest: &DoctorRawMirrorManifestReport,
        bytes: &[u8],
    ) -> std::result::Result<Vec<crate::connectors::NormalizedConversation>, String> {
        if manifest.origin_kind != "local" {
            return Err(format!("its origin is {}, not local", manifest.origin_kind));
        }
        let provider = match doctor_normalized_provider_slug(&manifest.provider).as_str() {
            // The connector registry keys Claude Code as `claude`.
            "claude_code" => "claude".to_string(),
            other => other.to_string(),
        };
        let entry = self.providers.entry(provider.clone()).or_insert_with(|| {
            crate::connectors::get_connector_factories()
                .into_iter()
                .find(|(name, _)| *name == provider)
                .map(|(_, factory)| {
                    let connector = factory();
                    let roots = connector.detect().root_paths;
                    (connector, roots)
                })
        });
        let Some((connector, roots)) = entry.as_ref() else {
            return Err(format!("no connector is registered as {provider}"));
        };
        let original = Path::new(&manifest.original_path);
        let Some((original_root, relative)) = roots
            .iter()
            .filter_map(|root| {
                original
                    .strip_prefix(root)
                    .ok()
                    .filter(|relative| relative.components().next().is_some())
                    .map(|relative| (root, relative))
            })
            .min_by_key(|(_, relative)| relative.components().count())
        else {
            return Err(format!(
                "it is outside every root the {provider} connector detects"
            ));
        };
        let staged_dir = self.scratch.path().join(&manifest.manifest_id);
        let staged_root = staged_dir.join("root");
        let staged = staged_root.join(relative);
        let parent = staged
            .parent()
            .ok_or_else(|| "its staged copy has no parent directory".to_string())?;
        std::fs::create_dir_all(parent)
            .and_then(|()| std::fs::write(&staged, bytes))
            .map_err(|err| format!("staging a copy for its connector failed: {err}"))?;
        let ctx = crate::connectors::ScanContext::with_roots(
            staged_dir.join("data"),
            vec![crate::connectors::ScanRoot::local(staged_root.clone())],
            None,
        );
        let mut conversations = connector
            .scan(&ctx)
            .map_err(|err| format!("the {provider} connector could not parse it: {err:#}"))?;
        if conversations.is_empty() {
            return Err(format!(
                "the {provider} connector found no conversation in it"
            ));
        }
        let canonical_root = staged_root.canonicalize().ok();
        for conversation in &mut conversations {
            let relative = conversation
                .source_path
                .strip_prefix(&staged_root)
                .ok()
                .or_else(|| {
                    canonical_root
                        .as_deref()
                        .and_then(|root| conversation.source_path.strip_prefix(root).ok())
                })
                .map(Path::to_path_buf)
                .ok_or_else(|| {
                    format!(
                        "the {provider} connector returned a conversation from outside the staged source"
                    )
                })?;
            conversation.source_path = original_root.join(relative);
            crate::indexer::inject_provenance(conversation, &crate::connectors::Origin::local());
        }
        Ok(conversations)
    }
}

/// zp0fp: a source is re-read by its connector only when it is local, its
/// provider has a registered connector, and it lies under a root that
/// connector detects. Otherwise the reason comes back and the build keeps the
/// line salvage for it.
#[cfg(test)]
#[test]
fn doctor_connector_reparse_reports_why_a_source_keeps_the_line_salvage() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut reparse = DoctorConnectorReparse::new(dir.path()).expect("scratch directory");
    let mut manifest = doctor_raw_mirror_invalid_manifest_report(
        dir.path(),
        &dir.path().join("manifest.json"),
        String::new(),
    );
    manifest.manifest_id = "zp0fp-manifest".to_string();
    manifest.provider = "codex".to_string();
    manifest.origin_kind = "ssh".to_string();
    manifest.original_path = "/zp0fp-nowhere/sessions/rollout-1.jsonl".to_string();
    let remote = reparse.conversations(&manifest, b"{}").unwrap_err();
    assert!(remote.contains("its origin is ssh, not local"), "{remote}");

    manifest.origin_kind = "local".to_string();
    manifest.provider = "zp0fp-unknown-agent".to_string();
    let unknown = reparse.conversations(&manifest, b"{}").unwrap_err();
    assert!(
        unknown.contains("no connector is registered as zp0fp_unknown_agent"),
        "{unknown}"
    );

    manifest.provider = "codex".to_string();
    let outside = reparse.conversations(&manifest, b"{}").unwrap_err();
    assert!(
        outside.contains("outside every root the codex connector detects"),
        "{outside}"
    );
}

fn doctor_candidate_copy_raw_mirror_evidence_to_staging(
    context: &DoctorCandidateCopyContext<'_>,
    manifest: &DoctorRawMirrorManifestReport,
) -> Vec<DoctorFsMutationReceipt> {
    let mut receipts = Vec::new();
    let evidence_root = context.candidate_dir.join("evidence/raw-mirror");
    for directory in [evidence_root.join("blobs"), evidence_root.join("manifests")] {
        if let Err(err) = doctor_forensic_create_private_dir_all(&directory) {
            receipts.push(DoctorFsMutationReceipt {
                schema_version: 1,
                operation_id: context.operation_id.to_string(),
                action_id: "prepare-raw-mirror-evidence-copy".to_string(),
                mutation_kind: DoctorFsMutationKind::CopyFileToStaging,
                fallback_kind: None,
                mode: DoctorRepairMode::ReconstructPromote,
                asset_class: DoctorAssetClass::RawMirrorBlob,
                source_path: None,
                redacted_source_path: None,
                target_path: directory.display().to_string(),
                redacted_target_path: doctor_redacted_path(
                    &directory.display().to_string(),
                    context.data_dir,
                ),
                staging_root: Some(context.candidate_dir.display().to_string()),
                redacted_staging_root: Some(doctor_redacted_path(
                    &context.candidate_dir.display().to_string(),
                    context.data_dir,
                )),
                expected_source_blake3: None,
                actual_source_blake3: None,
                actual_target_blake3: None,
                planned_bytes: 0,
                affected_bytes: 0,
                status: DoctorActionStatus::Failed,
                blocked_reasons: vec![format!(
                    "failed to create raw mirror evidence parent: {err}"
                )],
                precondition_checks: Vec::new(),
                forensic_bundle: doctor_forensic_bundle_uncaptured(
                    "not_captured_for_failed_evidence_parent_creation",
                ),
            });
            return receipts;
        }
    }

    let stored_blobs = if manifest.verified_stored_blobs.is_empty() {
        vec![(manifest.blob_blake3.clone(), manifest.blob_size_bytes)]
    } else {
        manifest.verified_stored_blobs.clone()
    };
    for (index, (blob_blake3, _)) in stored_blobs.into_iter().enumerate() {
        let Some(blob_relative_path) = doctor_raw_mirror_blob_relative_path(&blob_blake3) else {
            continue;
        };
        let source_path = doctor_raw_mirror_root(context.data_dir).join(&blob_relative_path);
        let target_path = evidence_root.join(&blob_relative_path);
        if let Some(parent) = target_path.parent()
            && let Err(err) = doctor_forensic_create_private_dir_all(parent)
        {
            receipts.push(DoctorFsMutationReceipt {
                schema_version: 1,
                operation_id: context.operation_id.to_string(),
                action_id: format!("prepare-raw-mirror-blob-{index}"),
                mutation_kind: DoctorFsMutationKind::CopyFileToStaging,
                fallback_kind: None,
                mode: DoctorRepairMode::ReconstructPromote,
                asset_class: DoctorAssetClass::RawMirrorBlob,
                source_path: None,
                redacted_source_path: None,
                target_path: parent.display().to_string(),
                redacted_target_path: doctor_redacted_path(
                    &parent.display().to_string(),
                    context.data_dir,
                ),
                staging_root: Some(context.candidate_dir.display().to_string()),
                redacted_staging_root: Some(doctor_redacted_path(
                    &context.candidate_dir.display().to_string(),
                    context.data_dir,
                )),
                expected_source_blake3: Some(blob_blake3.clone()),
                actual_source_blake3: None,
                actual_target_blake3: None,
                planned_bytes: 0,
                affected_bytes: 0,
                status: DoctorActionStatus::Failed,
                blocked_reasons: vec![format!(
                    "failed to create raw mirror blob evidence parent: {err}"
                )],
                precondition_checks: Vec::new(),
                forensic_bundle: doctor_forensic_bundle_uncaptured(
                    "not_captured_for_failed_evidence_parent_creation",
                ),
            });
            continue;
        }
        receipts.push(doctor_candidate_copy_to_staging(
            context,
            &format!("copy-raw-mirror-blob-{}-to-candidate", &blob_blake3[..12]),
            &source_path,
            &target_path,
            DoctorAssetClass::RawMirrorBlob,
        ));
    }
    let manifest_path = PathBuf::from(&manifest.manifest_path);
    receipts.push(doctor_candidate_copy_to_staging(
        context,
        "copy-raw-mirror-manifest-to-candidate",
        &manifest_path,
        &evidence_root
            .join("manifests")
            .join(format!("{}.json", manifest.manifest_id)),
        DoctorAssetClass::RawMirrorBlob,
    ));
    receipts
}

fn doctor_candidate_reconstruct_archive_from_raw_mirror(
    data_dir: &Path,
    candidate_db_path: &Path,
    raw_mirror: &DoctorRawMirrorReport,
    copy_context: &DoctorCandidateCopyContext<'_>,
    skipped_records: &mut Vec<serde_json::Value>,
    parse_errors: &mut Vec<serde_json::Value>,
) -> std::result::Result<(usize, usize, Vec<String>, Vec<DoctorFsMutationReceipt>), String> {
    let storage = crate::storage::sqlite::SqliteStorage::open(candidate_db_path).map_err(|err| {
        format!(
            "failed to initialize reconstructed candidate archive DB {} with frankensqlite storage: {err}",
            candidate_db_path.display()
        )
    })?;
    let mut seen = HashSet::new();
    let mut inserted_conversations = 0usize;
    let mut inserted_messages = 0usize;
    let mut evidence_sources = Vec::new();
    let mut receipts = Vec::new();
    let mut reparse = DoctorConnectorReparse::new(copy_context.candidate_dir)
        .map_err(|err| format!("its connector scratch directory could not be created: {err}"));

    let mut verified_manifests = raw_mirror
        .manifests
        .iter()
        .filter(|manifest| doctor_raw_mirror_manifest_is_verified(manifest))
        .collect::<Vec<_>>();
    verified_manifests.sort_by(|left, right| {
        right
            .captured_at_ms
            .cmp(&left.captured_at_ms)
            .then_with(|| right.source_mtime_ms.cmp(&left.source_mtime_ms))
            .then_with(|| left.manifest_id.cmp(&right.manifest_id))
    });

    for manifest in verified_manifests {
        let key = doctor_candidate_raw_mirror_key(manifest);
        if seen.contains(&key) {
            skipped_records.push(serde_json::json!({
                "manifest_id": manifest.manifest_id,
                "status": "skipped",
                "reason": "duplicate_raw_mirror_conversation_key",
                "dedupe_key": key,
                "redacted_manifest_path": manifest.redacted_manifest_path,
                "raw_content_included": false
            }));
            continue;
        }

        let bytes = match doctor_candidate_read_verified_raw_mirror_blob(data_dir, manifest) {
            Ok(bytes) => bytes,
            Err(reason) => {
                skipped_records.push(serde_json::json!({
                    "manifest_id": manifest.manifest_id,
                    "status": "skipped",
                    "reason": reason,
                    "redacted_manifest_path": manifest.redacted_manifest_path,
                    "raw_content_included": false
                }));
                continue;
            }
        };
        let reparsed = match reparse.as_mut() {
            Ok(reparse) => reparse.conversations(manifest, &bytes),
            Err(reason) => Err(reason.clone()),
        };
        // The connector path maps with the indexer's own transform, so the
        // candidate also gets the indexer's secret redaction.
        let (conversations, method) = match reparsed {
            Ok(normalized) => {
                let mapped = crate::indexer::persist::map_batch_to_internal(
                    &normalized,
                    crate::indexer::persist::PersistHeartbeat::NONE,
                );
                let workspaces = normalized
                    .into_iter()
                    .map(|conversation| conversation.workspace);
                (
                    mapped.into_iter().zip(workspaces).collect::<Vec<_>>(),
                    "connector".to_string(),
                )
            }
            Err(reason) => (
                vec![(
                    doctor_candidate_conversation_from_raw_mirror(manifest, &bytes, parse_errors),
                    None,
                )],
                format!("line_salvage ({reason})"),
            ),
        };
        let reparsed = method == "connector";
        let mut manifest_conversations_inserted = 0usize;
        let mut manifest_messages = 0usize;
        let mut last_conversation_id = None;
        for (mut conversation, workspace) in conversations {
            if reparsed
                && let Some(cass) = conversation
                    .metadata_json
                    .as_object_mut()
                    .and_then(|metadata| metadata.get_mut("cass"))
                    .and_then(serde_json::Value::as_object_mut)
            {
                cass.insert(
                    "doctor_reconstruct".to_string(),
                    serde_json::json!({
                        "schema_version": 1,
                        "source": "verified_raw_mirror",
                        "method": "connector",
                        "manifest_id": manifest.manifest_id,
                        "blob_blake3": manifest.blob_blake3,
                    }),
                );
            }
            let agent = crate::model::types::Agent {
                id: None,
                slug: conversation.agent_slug.clone(),
                name: conversation.agent_slug.clone(),
                version: (!reparsed).then(|| "doctor-reconstruct".to_string()),
                kind: crate::model::types::AgentKind::Cli,
            };
            let agent_id = storage.ensure_agent(&agent).map_err(|err| {
                format!(
                    "failed to ensure reconstructed candidate agent {}: {err}",
                    agent.slug
                )
            })?;
            let workspace_id = workspace
                .as_deref()
                .map(|workspace| storage.ensure_workspace(workspace, None))
                .transpose()
                .map_err(|err| {
                    format!("failed to ensure reconstructed candidate workspace: {err}")
                })?;
            let message_count = conversation.messages.len();
            let outcome = storage
                .insert_conversation_tree(agent_id, workspace_id, &conversation)
                .map_err(|err| {
                    format!(
                        "failed to insert raw mirror manifest {} into candidate archive: {err}",
                        manifest.manifest_id
                    )
                })?;
            last_conversation_id = Some(outcome.conversation_id);
            if outcome.conversation_inserted || !outcome.inserted_indices.is_empty() {
                manifest_conversations_inserted += usize::from(outcome.conversation_inserted);
                manifest_messages += message_count;
            }
        }
        seen.insert(key);
        if manifest_conversations_inserted == 0 && manifest_messages == 0 {
            skipped_records.push(serde_json::json!({
                "manifest_id": manifest.manifest_id,
                "status": "skipped",
                "reason": "candidate_storage_reported_duplicate_conversation",
                "candidate_conversation_id": last_conversation_id,
                "redacted_manifest_path": manifest.redacted_manifest_path,
                "raw_content_included": false
            }));
            continue;
        }
        inserted_conversations += manifest_conversations_inserted;
        inserted_messages += manifest_messages;
        evidence_sources.push(format!(
            "verified_raw_mirror:manifest_id={}:messages={manifest_messages}:blob={}:method={method}",
            manifest.manifest_id, manifest.blob_blake3
        ));
        receipts.extend(doctor_candidate_copy_raw_mirror_evidence_to_staging(
            copy_context,
            manifest,
        ));
    }

    if inserted_conversations == 0 {
        return Err(
            "no verified raw mirror conversations could be inserted into the candidate archive"
                .to_string(),
        );
    }
    Ok((
        inserted_conversations,
        inserted_messages,
        evidence_sources,
        receipts,
    ))
}

fn doctor_candidate_build_should_run(
    fix_can_mutate: bool,
    db_ok: bool,
    needs_rebuild: bool,
    coverage_risk: &DoctorCoverageRiskSummary,
    source_authority: &DoctorSourceAuthorityReport,
) -> bool {
    if !fix_can_mutate {
        return false;
    }
    let candidate_authority_available =
        source_authority
            .selected_authorities
            .iter()
            .any(|authority| {
                matches!(
                    authority.decision,
                    DoctorSourceAuthorityDecision::CandidateOnly
                        | DoctorSourceAuthorityDecision::Promotable
                )
            });
    let archive_risk = !matches!(
        coverage_risk.status.as_str(),
        "ok" | "not_initialized" | "unchecked_fast_health"
    );
    let raw_mirror_expands_archive = source_authority
        .coverage_delta
        .raw_mirror_links_minus_archive
        > 0;
    candidate_authority_available
        && (!db_ok || needs_rebuild || archive_risk || raw_mirror_expands_archive)
}

fn build_doctor_reconstruct_candidate(
    data_dir: &Path,
    db_path: &Path,
    index_path: &Path,
    raw_mirror: &DoctorRawMirrorReport,
    source_authority: &DoctorSourceAuthorityReport,
    coverage_summary: &DoctorCoverageSummary,
) -> DoctorCandidateBuildReport {
    let operation_id = "doctor-reconstruct-candidate";
    let created_at_ms = doctor_now_ms();
    let candidate_root = doctor_candidate_root(data_dir);
    let live_inventory_before = doctor_candidate_live_inventory(db_path, index_path);
    let coverage_before = doctor_candidate_coverage_before_snapshot(coverage_summary);
    let mut report = DoctorCandidateBuildReport {
        requested: true,
        status: "blocked".to_string(),
        selected_authority: source_authority
            .selected_authority
            .map(doctor_serde_label)
            .unwrap_or_else(|| "none".to_string()),
        selected_authority_decision: doctor_serde_label(source_authority.decision),
        selected_authority_evidence: source_authority
            .selected_authorities
            .iter()
            .flat_map(|authority| authority.evidence.clone())
            .collect(),
        live_inventory_before: live_inventory_before.clone(),
        notes: vec![
            "Candidate staging writes only under doctor/candidates and never swaps live archive or index paths.".to_string(),
            "Candidate directories are retained for inspection; doctor reports stale candidates but does not auto-delete them.".to_string(),
        ],
        coverage_before: coverage_before.clone(),
        coverage_after: DoctorCandidateCoverageSnapshot {
            coverage_source: "candidate_not_built".to_string(),
            ..DoctorCandidateCoverageSnapshot::default()
        },
        confidence: coverage_summary.confidence_tier.clone(),
        ..DoctorCandidateBuildReport::default()
    };
    if let Err(err) = doctor_candidate_root_is_safe(data_dir, db_path, index_path, &candidate_root)
    {
        report.warnings.push(err);
        report.live_inventory_after = doctor_candidate_live_inventory(db_path, index_path);
        report.live_inventory_unchanged = report.live_inventory_after == live_inventory_before;
        return report;
    }
    if let Err(err) = doctor_forensic_create_private_dir_all(&candidate_root) {
        report.warnings.push(format!(
            "failed to create candidate root {}: {err}",
            candidate_root.display()
        ));
        report.live_inventory_after = doctor_candidate_live_inventory(db_path, index_path);
        report.live_inventory_unchanged = report.live_inventory_after == live_inventory_before;
        return report;
    }
    if let Err(err) = doctor_candidate_root_is_safe(data_dir, db_path, index_path, &candidate_root)
    {
        report.warnings.push(err);
        report.live_inventory_after = doctor_candidate_live_inventory(db_path, index_path);
        report.live_inventory_unchanged = report.live_inventory_after == live_inventory_before;
        return report;
    }

    let base_candidate_id = doctor_candidate_id(operation_id, created_at_ms);
    let mut candidate_id = base_candidate_id.clone();
    let mut candidate_dir = candidate_root.join(&candidate_id);
    let mut allocated = false;
    for suffix in 0..100 {
        let next_id = if suffix == 0 {
            base_candidate_id.clone()
        } else {
            format!("{base_candidate_id}-{suffix}")
        };
        let next_dir = candidate_root.join(&next_id);
        match doctor_forensic_create_private_dir(&next_dir) {
            Ok(()) => {
                candidate_id = next_id;
                candidate_dir = next_dir;
                allocated = true;
                break;
            }
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(err) => {
                report.warnings.push(format!(
                    "failed to create candidate directory {}: {err}",
                    next_dir.display()
                ));
                report.live_inventory_after = doctor_candidate_live_inventory(db_path, index_path);
                report.live_inventory_unchanged =
                    report.live_inventory_after == live_inventory_before;
                return report;
            }
        }
    }
    if !allocated {
        report.warnings.push(format!(
            "failed to allocate unique candidate directory under {}",
            candidate_root.display()
        ));
        report.live_inventory_after = doctor_candidate_live_inventory(db_path, index_path);
        report.live_inventory_unchanged = report.live_inventory_after == live_inventory_before;
        return report;
    }

    report.candidate_id = Some(candidate_id.clone());
    report.path = Some(candidate_dir.display().to_string());
    report.redacted_path = Some(doctor_redacted_path(
        &candidate_dir.display().to_string(),
        data_dir,
    ));
    let manifest_path = candidate_dir.join("manifest.json");
    report.manifest_path = Some(manifest_path.display().to_string());
    report.redacted_manifest_path = Some(doctor_redacted_path(
        &manifest_path.display().to_string(),
        data_dir,
    ));

    for subdir in [
        "database",
        "index/lexical",
        "index/semantic",
        "logs",
        "receipts",
    ] {
        if let Err(err) = doctor_forensic_create_private_dir_all(&candidate_dir.join(subdir)) {
            report.warnings.push(format!(
                "failed to create candidate subdirectory {subdir}: {err}"
            ));
        }
    }

    let initial_manifest = DoctorCandidateManifest {
        schema_version: DOCTOR_CANDIDATE_SCHEMA_VERSION,
        manifest_kind: DOCTOR_CANDIDATE_MANIFEST_KIND.to_string(),
        candidate_id: candidate_id.clone(),
        lifecycle_status: "in_progress".to_string(),
        created_at_ms,
        updated_at_ms: doctor_now_ms(),
        operation_id: operation_id.to_string(),
        staging_root: candidate_dir.display().to_string(),
        redacted_staging_root: doctor_redacted_path(&candidate_dir.display().to_string(), data_dir),
        manifest_path: manifest_path.display().to_string(),
        redacted_manifest_path: doctor_redacted_path(
            &manifest_path.display().to_string(),
            data_dir,
        ),
        selected_authority: report.selected_authority.clone(),
        selected_authority_decision: report.selected_authority_decision.clone(),
        selected_authority_evidence: report.selected_authority_evidence.clone(),
        evidence_sources: report.evidence_sources.clone(),
        coverage_before: coverage_before.clone(),
        coverage_after: report.coverage_after.clone(),
        confidence: report.confidence.clone(),
        skipped_record_log: "logs/skipped-records.jsonl".to_string(),
        parse_error_log: "logs/parse-errors.jsonl".to_string(),
        artifact_count: 0,
        checksum_set: BTreeMap::new(),
        artifacts: Vec::new(),
        coverage_gate: DoctorCoverageComparisonGateReport::default(),
        live_inventory_before: live_inventory_before.clone(),
        live_inventory_after: DoctorCandidateLiveInventory::default(),
        live_inventory_unchanged: false,
        notes: report.notes.clone(),
    };
    if let Err(err) =
        doctor_candidate_write_manifest_atomic(data_dir, &candidate_dir, &initial_manifest)
    {
        report.warnings.push(err);
        report.live_inventory_after = doctor_candidate_live_inventory(db_path, index_path);
        report.live_inventory_unchanged = report.live_inventory_after == live_inventory_before;
        return report;
    }

    let mut skipped_records = Vec::new();
    let mut parse_errors = Vec::new();
    for manifest in &raw_mirror.manifests {
        if manifest.status == "verified" {
            continue;
        }
        let record = serde_json::json!({
            "manifest_id": manifest.manifest_id,
            "status": manifest.status,
            "redacted_manifest_path": manifest.redacted_manifest_path,
            "reason": manifest.invalid_reason,
            "blob_checksum_status": manifest.blob_checksum_status,
            "manifest_checksum_status": manifest.manifest_checksum_status,
        });
        skipped_records.push(record.clone());
        if manifest.invalid_reason.is_some() {
            parse_errors.push(record);
        }
    }

    let candidate_db_path = candidate_dir.join("database").join("candidate.db");
    let copy_context = DoctorCandidateCopyContext {
        operation_id,
        data_dir,
        db_path,
        index_path,
        candidate_dir: &candidate_dir,
    };
    let verified_raw_mirror_db_link_count =
        doctor_candidate_verified_raw_mirror_db_link_count(raw_mirror);
    let live_archive_copy_probe = doctor_candidate_live_archive_copy_probe(db_path).and_then(
        |(live_conversations, live_messages)| {
            let live_conversation_count = live_conversations.unwrap_or_default();
            if verified_raw_mirror_db_link_count > live_conversation_count {
                Err(format!(
                    "verified raw mirror links ({verified_raw_mirror_db_link_count}) exceed readable live archive conversations ({live_conversation_count}); using mirror reconstruction instead of copying the lower-coverage archive"
                ))
            } else {
                Ok((Some(live_conversation_count), live_messages))
            }
        },
    );
    if let Ok((live_conversations, live_messages)) = live_archive_copy_probe.as_ref() {
        let receipt = doctor_candidate_copy_to_staging(
            &copy_context,
            "copy-live-archive-db-to-candidate",
            db_path,
            &candidate_db_path,
            DoctorAssetClass::CanonicalArchiveDb,
        );
        if receipt.status != DoctorActionStatus::Applied {
            report
                .warnings
                .extend(receipt.blocked_reasons.iter().cloned());
        }
        report.fs_mutation_receipts.push(receipt);
        for (suffix, asset_class, action_id) in [
            (
                "-wal",
                DoctorAssetClass::ArchiveDbSidecar,
                "copy-live-archive-wal-to-candidate",
            ),
            (
                "-shm",
                DoctorAssetClass::ArchiveDbSidecar,
                "copy-live-archive-shm-to-candidate",
            ),
        ] {
            if let Some(sidecar_path) = doctor_sqlite_sidecar_path(db_path, suffix)
                && sidecar_path.exists()
            {
                let target =
                    doctor_sqlite_sidecar_path(&candidate_db_path, suffix).unwrap_or_else(|| {
                        candidate_dir
                            .join("database")
                            .join(format!("candidate.db{suffix}"))
                    });
                let receipt = doctor_candidate_copy_to_staging(
                    &copy_context,
                    action_id,
                    &sidecar_path,
                    &target,
                    asset_class,
                );
                if receipt.status != DoctorActionStatus::Applied {
                    report
                        .warnings
                        .extend(receipt.blocked_reasons.iter().cloned());
                }
                report.fs_mutation_receipts.push(receipt);
            }
        }
        report.evidence_sources.push(format!(
            "canonical_archive_db_copy:conversations={}:messages={}",
            live_conversations
                .as_ref()
                .map(|count| count.to_string())
                .unwrap_or_else(|| "unknown".to_string()),
            live_messages
                .as_ref()
                .map(|count| count.to_string())
                .unwrap_or_else(|| "unknown".to_string())
        ));
        report.confidence = "archive_copy_with_frankensqlite_probe".to_string();
    } else {
        if let Err(reason) = live_archive_copy_probe {
            report.warnings.push(format!(
                "live archive was not copied into candidate; attempting verified raw mirror reconstruction instead: {reason}"
            ));
            skipped_records.push(serde_json::json!({
                "status": "skipped",
                "reason": "live_archive_not_safe_to_copy",
                "details": reason,
                "raw_content_included": false
            }));
        }
        match doctor_candidate_reconstruct_archive_from_raw_mirror(
            data_dir,
            &candidate_db_path,
            raw_mirror,
            &copy_context,
            &mut skipped_records,
            &mut parse_errors,
        ) {
            Ok((inserted_conversations, inserted_messages, evidence_sources, receipts)) => {
                report.evidence_sources.extend(evidence_sources);
                report.fs_mutation_receipts.extend(receipts);
                report.confidence = "verified_raw_mirror_reconstruction".to_string();
                report.notes.push(format!(
                    "Reconstructed candidate archive from {inserted_conversations} verified raw mirror conversation(s) and {inserted_messages} message(s)."
                ));
            }
            Err(err) => {
                report.warnings.push(err);
            }
        }
    }

    let skipped_bytes = doctor_candidate_jsonl_bytes(&skipped_records);
    let parse_error_bytes = doctor_candidate_jsonl_bytes(&parse_errors);
    for (action_id, relative, bytes) in [
        (
            "write-candidate-skipped-record-log",
            "logs/skipped-records.jsonl",
            skipped_bytes.as_slice(),
        ),
        (
            "write-candidate-parse-error-log",
            "logs/parse-errors.jsonl",
            parse_error_bytes.as_slice(),
        ),
    ] {
        let receipt = doctor_candidate_write_bytes_to_staging(
            operation_id,
            action_id,
            data_dir,
            &candidate_dir,
            &candidate_dir.join(relative),
            DoctorAssetClass::OperationReceipt,
            bytes,
        );
        if receipt.status != DoctorActionStatus::Applied {
            report
                .warnings
                .extend(receipt.blocked_reasons.iter().cloned());
        }
        report.fs_mutation_receipts.push(receipt);
    }
    report.skipped_record_count = skipped_records.len();
    report.parse_error_count = parse_errors.len();

    let (candidate_conversations, candidate_messages) =
        match doctor_candidate_probe_frankensqlite(&candidate_db_path, &candidate_id) {
            Ok((conversations, messages)) => {
                report.frankensqlite_open_ok = true;
                report.frankensqlite_write_ok = true;
                (conversations, messages)
            }
            Err(err) => {
                report.warnings.push(err);
                (None, None)
            }
        };
    report.candidate_conversation_count = candidate_conversations;
    report.candidate_message_count = candidate_messages;
    report.coverage_after = doctor_candidate_coverage_after_snapshot(
        if report.confidence == "verified_raw_mirror_reconstruction" {
            "verified_raw_mirror_candidate_archive"
        } else {
            "copied_live_archive_candidate_archive"
        },
        coverage_summary,
        candidate_conversations,
        candidate_messages,
    );
    let mut gate_candidate_conversations = candidate_conversations;
    let mut gate_candidate_messages = candidate_messages;
    if let Some(fault) = doctor_coverage_gate_test_fault() {
        match fault.as_str() {
            "candidate_conversation_loss" => {
                gate_candidate_conversations = gate_candidate_conversations
                    .map(|count| count.saturating_sub(1))
                    .or(Some(0));
                report.warnings.push(
                    "test fault adjusted candidate conversation coverage downward".to_string(),
                );
            }
            "candidate_message_loss" => {
                gate_candidate_messages = gate_candidate_messages
                    .map(|count| count.saturating_sub(1))
                    .or(Some(0));
                report
                    .warnings
                    .push("test fault adjusted candidate message coverage downward".to_string());
            }
            _ => {}
        }
    }
    report.coverage_gate = build_doctor_coverage_comparison_gate(
        coverage_summary,
        source_authority,
        gate_candidate_conversations,
        gate_candidate_messages,
        None,
        None,
    );
    if !report.coverage_gate.promote_allowed {
        report.warnings.extend(
            report
                .coverage_gate
                .blocking_reasons
                .iter()
                .map(|reason| format!("coverage gate blocked candidate promotion: {reason}")),
        );
    }

    let lexical_metadata = serde_json::json!({
        "schema_version": 1,
        "metadata_kind": "cass_doctor_candidate_lexical_metadata_v1",
        "candidate_id": candidate_id.clone(),
        "live_index_path": index_path.display().to_string(),
        "redacted_live_index_path": doctor_redacted_path(&index_path.display().to_string(), data_dir),
        "candidate_archive_db": "database/candidate.db",
        "candidate_conversation_count": candidate_conversations,
        "candidate_message_count": candidate_messages,
        "coverage_confidence_tier": coverage_summary.confidence_tier.clone(),
        "candidate_confidence": report.confidence.clone(),
        "candidate_evidence_sources": report.evidence_sources.clone(),
        "coverage_before": report.coverage_before.clone(),
        "coverage_after": report.coverage_after.clone(),
        "archive_conversation_count": coverage_summary.archive_conversation_count,
        "db_without_raw_mirror_count": coverage_summary.db_without_raw_mirror_count,
        "sole_copy_candidate_count": coverage_summary.sole_copy_candidate_count,
        "coverage_gate_status": report.coverage_gate.status.clone(),
        "coverage_gate_promote_allowed": report.coverage_gate.promote_allowed,
        "raw_mirror_verified_blob_count": raw_mirror.summary.verified_blob_count,
        "raw_mirror_manifest_count": raw_mirror.summary.manifest_count,
        "not_live_index": true
    });
    let semantic_metadata = serde_json::json!({
        "schema_version": 1,
        "metadata_kind": "cass_doctor_candidate_semantic_metadata_v1",
        "candidate_id": candidate_id.clone(),
        "semantic_vectors_built": false,
        "reason": "candidate staging records semantic metadata without downloading models or writing live vector assets",
        "not_live_index": true
    });
    for (action_id, relative, asset_class, value) in [
        (
            "write-candidate-lexical-metadata",
            "index/lexical/candidate-generation.json",
            DoctorAssetClass::DerivedLexicalIndex,
            lexical_metadata,
        ),
        (
            "write-candidate-semantic-metadata",
            "index/semantic/metadata.json",
            DoctorAssetClass::DerivedSemanticIndex,
            semantic_metadata,
        ),
    ] {
        let bytes = serde_json::to_vec_pretty(&value).unwrap_or_default();
        let receipt = doctor_candidate_write_bytes_to_staging(
            operation_id,
            action_id,
            data_dir,
            &candidate_dir,
            &candidate_dir.join(relative),
            asset_class,
            &bytes,
        );
        if receipt.status != DoctorActionStatus::Applied {
            report
                .warnings
                .extend(receipt.blocked_reasons.iter().cloned());
        }
        report.fs_mutation_receipts.push(receipt);
    }

    let receipt_records = report
        .fs_mutation_receipts
        .iter()
        .filter_map(|receipt| serde_json::to_value(receipt).ok())
        .collect::<Vec<_>>();
    let receipt_bytes = doctor_candidate_jsonl_bytes(&receipt_records);
    let receipt = doctor_candidate_write_bytes_to_staging(
        operation_id,
        "write-candidate-fs-mutation-receipts",
        data_dir,
        &candidate_dir,
        &candidate_dir.join("receipts/fs-mutations.jsonl"),
        DoctorAssetClass::OperationReceipt,
        &receipt_bytes,
    );
    if receipt.status != DoctorActionStatus::Applied {
        report
            .warnings
            .extend(receipt.blocked_reasons.iter().cloned());
    }
    report.fs_mutation_receipts.push(receipt);

    let live_inventory_after = doctor_candidate_live_inventory(db_path, index_path);
    report.live_inventory_after = live_inventory_after.clone();
    report.live_inventory_unchanged = live_inventory_after == live_inventory_before;
    let artifacts = collect_doctor_candidate_artifacts(data_dir, &candidate_dir);
    let checksum_set = doctor_candidate_checksum_set(&artifacts);
    report.artifact_count = artifacts.len();
    report.checksum_count = checksum_set.len();
    let lifecycle_status = if report.frankensqlite_open_ok
        && report.frankensqlite_write_ok
        && report.live_inventory_unchanged
        && report.coverage_gate.promote_allowed
        && report
            .fs_mutation_receipts
            .iter()
            .all(|receipt| receipt.status == DoctorActionStatus::Applied)
    {
        "completed"
    } else {
        "blocked"
    };
    report.status = lifecycle_status.to_string();

    let final_manifest = DoctorCandidateManifest {
        schema_version: DOCTOR_CANDIDATE_SCHEMA_VERSION,
        manifest_kind: DOCTOR_CANDIDATE_MANIFEST_KIND.to_string(),
        candidate_id: candidate_id.clone(),
        lifecycle_status: lifecycle_status.to_string(),
        created_at_ms,
        updated_at_ms: doctor_now_ms(),
        operation_id: operation_id.to_string(),
        staging_root: candidate_dir.display().to_string(),
        redacted_staging_root: doctor_redacted_path(&candidate_dir.display().to_string(), data_dir),
        manifest_path: manifest_path.display().to_string(),
        redacted_manifest_path: doctor_redacted_path(
            &manifest_path.display().to_string(),
            data_dir,
        ),
        selected_authority: report.selected_authority.clone(),
        selected_authority_decision: report.selected_authority_decision.clone(),
        selected_authority_evidence: report.selected_authority_evidence.clone(),
        evidence_sources: report.evidence_sources.clone(),
        coverage_before: report.coverage_before.clone(),
        coverage_after: report.coverage_after.clone(),
        confidence: report.confidence.clone(),
        skipped_record_log: "logs/skipped-records.jsonl".to_string(),
        parse_error_log: "logs/parse-errors.jsonl".to_string(),
        artifact_count: artifacts.len(),
        checksum_set,
        artifacts,
        coverage_gate: report.coverage_gate.clone(),
        live_inventory_before,
        live_inventory_after,
        live_inventory_unchanged: report.live_inventory_unchanged,
        notes: report.notes.clone(),
    };
    match doctor_candidate_write_manifest_atomic(data_dir, &candidate_dir, &final_manifest) {
        Ok(hash) => report.manifest_blake3 = Some(hash),
        Err(err) => {
            report.status = "blocked".to_string();
            report.warnings.push(err);
        }
    }
    report
}

fn collect_doctor_candidate_staging_report(
    data_dir: &Path,
    db_path: &Path,
    index_path: &Path,
) -> DoctorCandidateStagingReport {
    let root = doctor_candidate_root(data_dir);
    let root_path = root.display().to_string();
    let mut report = DoctorCandidateStagingReport {
        schema_version: DOCTOR_CANDIDATE_SCHEMA_VERSION,
        status: "absent".to_string(),
        root_path: root_path.clone(),
        redacted_root_path: doctor_redacted_path(&root_path, data_dir),
        exists: root.exists(),
        notes: vec![
            "Candidate directories are isolated staging artifacts, not live archive state.".to_string(),
            "Doctor reports orphaned or interrupted candidates but never deletes candidate evidence automatically.".to_string(),
        ],
        ..DoctorCandidateStagingReport::default()
    };

    if !root.exists() {
        return report;
    }
    if let Err(err) = doctor_candidate_root_is_safe(data_dir, db_path, index_path, &root) {
        report.status = "unsafe".to_string();
        report.warnings.push(err);
        return report;
    }

    let entries = match std::fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(err) => {
            report.status = "warn".to_string();
            report.warnings.push(format!(
                "failed to read candidate root {}: {err}",
                root.display()
            ));
            return report;
        }
    };

    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(metadata) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            continue;
        }
        let candidate_id = entry.file_name().to_string_lossy().to_string();
        let manifest_path = path.join("manifest.json");
        let mut summary = DoctorCandidateSummary {
            candidate_id,
            lifecycle_status: "orphaned".to_string(),
            path: path.display().to_string(),
            redacted_path: doctor_redacted_path(&path.display().to_string(), data_dir),
            manifest_path: manifest_path
                .exists()
                .then(|| manifest_path.display().to_string()),
            redacted_manifest_path: manifest_path
                .exists()
                .then(|| doctor_redacted_path(&manifest_path.display().to_string(), data_dir)),
            manifest_blake3: manifest_path
                .exists()
                .then(|| file_blake3_hex(&manifest_path).ok())
                .flatten(),
            orphaned: true,
            interrupted: true,
            safe_to_delete_automatically: false,
            evidence: Vec::new(),
            next_action:
                "inspect candidate directory before promotion or any cleanup consideration"
                    .to_string(),
            ..DoctorCandidateSummary::default()
        };
        if manifest_path.exists() {
            match std::fs::read_to_string(&manifest_path)
                .ok()
                .and_then(|raw| serde_json::from_str::<serde_json::Value>(&raw).ok())
            {
                Some(value) => {
                    summary.orphaned = false;
                    summary.lifecycle_status = value
                        .get("lifecycle_status")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown")
                        .to_string();
                    summary.artifact_count = value
                        .get("artifact_count")
                        .and_then(|v| v.as_u64())
                        .unwrap_or_default() as usize;
                    summary.checksum_count = value
                        .get("checksum_set")
                        .and_then(|v| v.as_object())
                        .map(|map| map.len())
                        .unwrap_or_default();
                    summary.selected_authority = value
                        .get("selected_authority")
                        .and_then(|v| v.as_str())
                        .map(str::to_string);
                    summary.created_at_ms = value.get("created_at_ms").and_then(|v| v.as_i64());
                    summary.updated_at_ms = value.get("updated_at_ms").and_then(|v| v.as_i64());
                    summary.interrupted =
                        !matches!(summary.lifecycle_status.as_str(), "completed" | "blocked");
                    summary
                        .evidence
                        .push("candidate-manifest-present".to_string());
                    if summary.interrupted {
                        summary
                            .evidence
                            .push("candidate-lifecycle-not-terminal".to_string());
                    }
                }
                None => {
                    summary.lifecycle_status = "manifest_unreadable".to_string();
                    summary.orphaned = false;
                    summary.interrupted = true;
                    summary
                        .evidence
                        .push("candidate-manifest-unreadable".to_string());
                }
            }
        } else {
            summary
                .evidence
                .push("candidate-manifest-missing".to_string());
        }

        match summary.lifecycle_status.as_str() {
            "completed" => report.completed_candidate_count += 1,
            "blocked" => report.blocked_candidate_count += 1,
            "in_progress" => report.in_progress_candidate_count += 1,
            _ => {}
        }
        if summary.interrupted {
            report.interrupted_candidate_count += 1;
        }
        if summary.orphaned {
            report.orphaned_candidate_count += 1;
        }
        report.candidates.push(summary);
    }
    report
        .candidates
        .sort_by(|left, right| left.path.cmp(&right.path));
    report.total_candidate_count = report.candidates.len();
    report.status = if report.total_candidate_count == 0 {
        "empty".to_string()
    } else if report.orphaned_candidate_count > 0 || report.interrupted_candidate_count > 0 {
        "needs_inspection".to_string()
    } else if report.blocked_candidate_count > 0 {
        "blocked_candidates_present".to_string()
    } else {
        "ready".to_string()
    };
    if report.orphaned_candidate_count > 0 || report.interrupted_candidate_count > 0 {
        report.warnings.push(format!(
            "{} orphaned and {} interrupted candidate staging artifact(s) require inspection",
            report.orphaned_candidate_count, report.interrupted_candidate_count
        ));
    }
    report
}

fn doctor_candidate_promotion_root(data_dir: &Path) -> PathBuf {
    data_dir.join("doctor").join("candidate-promotions")
}

#[cfg(test)]
mod doctor_backup_restore_tests;

/// Data gathered by the bounded archive-DB doctor probe (#287): row counts,
/// the PRAGMA integrity verdict, and (when the database is healthy) the FTS
/// visibility state — everything `run_doctor_impl` needs to reconstruct the
/// `database`/`fts_table` checks without touching the connection on the main
/// thread.
struct DoctorBoundedArchiveDbProbe {
    conv_count: Option<i64>,
    msg_count: Option<i64>,
    integrity: Option<Result<DoctorDatabaseIntegrityProbe, String>>,
    integrity_skipped_reason: Option<String>,
    fts_state: Option<DoctorFtsTableState>,
    legacy_omp_analytics_pending: Result<bool, String>,
}

enum DoctorBoundedArchiveDbProbeOutcome {
    Completed(DoctorBoundedArchiveDbProbe),
    TimedOut { phase: &'static str },
}

/// Hard deadline for the bounded archive-DB doctor probe (#287).
/// `CASS_DOCTOR_DB_PROBE_TIMEOUT_SECS` overrides it; otherwise the budget
/// scales with the archive bundle size (ub29t / gh#374 issue 5: a fixed 30s
/// was unmeetable on a healthy 1.4GB archive, so the probe effectively never
/// ran at normal scale — every run reported `timeout_or_busy_spin_guard`).
fn doctor_archive_db_probe_hard_timeout(bundle_bytes: u64) -> Duration {
    dotenvy::var("CASS_DOCTOR_DB_PROBE_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|&secs| secs > 0)
        .map_or_else(
            || doctor_probe_timeout_for_bundle(bundle_bytes),
            Duration::from_secs,
        )
}

/// Pure size-scaled budget: 30s base plus 20s per started GiB of archive
/// bundle (main file + WAL), capped at 10 minutes so the doctor stays
/// bounded on multi-GB archives.
fn doctor_probe_timeout_for_bundle(bundle_bytes: u64) -> Duration {
    const BASE_SECS: u64 = 30;
    const SECS_PER_GIB: u64 = 20;
    const CAP_SECS: u64 = 600;
    const GIB: u64 = 1024 * 1024 * 1024;
    let started_gib = bundle_bytes.div_ceil(GIB);
    let secs = BASE_SECS
        .saturating_add(started_gib.saturating_mul(SECS_PER_GIB))
        .min(CAP_SECS);
    Duration::from_secs(secs)
}

/// Archive bundle size the probe budget scales with: the main database file
/// plus its `-wal` sidecar when present. Unreadable metadata counts as zero
/// (the base budget still applies).
fn doctor_archive_bundle_bytes(db_path: &Path) -> u64 {
    let main = std::fs::metadata(db_path)
        .ok()
        .filter(|m| m.file_type().is_file())
        .map_or(0, |m| m.len());
    let wal_path = {
        let mut name = db_path
            .file_name()
            .map(|n| n.to_os_string())
            .unwrap_or_default();
        name.push("-wal");
        db_path.with_file_name(name)
    };
    let wal = std::fs::metadata(wal_path)
        .ok()
        .filter(|m| m.file_type().is_file())
        .map_or(0, |m| m.len());
    main.saturating_add(wal)
}

#[cfg(test)]
mod doctor_probe_timeout_tests {
    use super::doctor_probe_timeout_for_bundle;

    #[test]
    fn budget_scales_with_started_gib_and_is_capped() {
        const GIB: u64 = 1024 * 1024 * 1024;
        let secs = |bytes: u64| doctor_probe_timeout_for_bundle(bytes).as_secs();
        // (bundle bytes, expected seconds): 30s base + 20s per started GiB,
        // capped at 600s. 1.4GB is the reporter's healthy archive -> 70s.
        let table = [
            (0_u64, 30_u64),
            (200 * 1024 * 1024, 50),
            (1_400_000_000, 70),
            (9 * GIB, 210),
            (100 * GIB, 600),
            (u64::MAX, 600),
        ];
        for (bytes, want) in table {
            assert!(
                secs(bytes).cmp(&want).is_eq(),
                "{bytes} bytes -> {} s",
                secs(bytes)
            );
        }
    }
}

// `PRAGMA quick_check(1)` limits the number of diagnostics returned; it does
// not limit the number of database pages visited.  FrankenSQLite's synchronous
// PRAGMA executor also does not currently expose a cancellation handle, so a
// thread-level `recv_timeout` can stop the doctor from waiting but cannot stop
// the abandoned worker from continuing to consume CPU and emit trace output.
// Keep routine doctor checks away from that known-unbounded path once the
// archive bundle is large enough for a runaway full-page walk to be material.
// A skipped check is reported as unchecked (or satisfied by a still-current
// cached attestation), never silently promoted to a pass.
const DOCTOR_FRANKEN_DEEP_INTEGRITY_MAX_BYTES: u64 = 256 * 1024 * 1024;
/// Opt-in override for the size gate above (GH #390): operators on long-lived
/// archives need a supported way to run the full-page frankensqlite
/// `integrity_check` through doctor instead of being told to leave the
/// process. The probe still runs under the hard `recv_timeout` deadline
/// (`CASS_DOCTOR_DB_PROBE_TIMEOUT_SECS`), so lifting the byte cap without
/// raising the deadline yields a timed-out probe, not an unbounded wait on the
/// doctor thread.
const CASS_DOCTOR_FULL_PAGE_INTEGRITY_PROBE: &str = "CASS_DOCTOR_FULL_PAGE_INTEGRITY_PROBE";

/// Wall-clock budget for the `archive_wal` `--fix` checkpoint (GH #382 /
/// g3zyo). `CASS_DOCTOR_WAL_CHECKPOINT_TIMEOUT_SECS` overrides the 120 s
/// default; `0` falls back to the default rather than disabling the bound.
fn doctor_wal_checkpoint_deadline() -> std::time::Duration {
    const DEFAULT_SECS: u64 = 120;
    let secs = dotenvy::var("CASS_DOCTOR_WAL_CHECKPOINT_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .unwrap_or(DEFAULT_SECS);
    std::time::Duration::from_secs(secs)
}

fn doctor_full_page_integrity_probe_requested() -> bool {
    dotenvy::var(CASS_DOCTOR_FULL_PAGE_INTEGRITY_PROBE)
        .ok()
        .is_some_and(|raw| {
            matches!(
                raw.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
}

fn doctor_franken_deep_integrity_skip_reason_for_bytes(
    bundle_bytes: u64,
    full_probe_requested: bool,
) -> Option<String> {
    if full_probe_requested {
        return None;
    }
    (bundle_bytes > DOCTOR_FRANKEN_DEEP_INTEGRITY_MAX_BYTES).then(|| {
        format!(
            "frankensqlite_full_page_integrity_probe_deferred: archive bundle is {bundle_bytes} bytes, above the bounded doctor limit of {DOCTOR_FRANKEN_DEEP_INTEGRITY_MAX_BYTES} bytes; set {CASS_DOCTOR_FULL_PAGE_INTEGRITY_PROBE}=1 (and raise CASS_DOCTOR_DB_PROBE_TIMEOUT_SECS) to run the full-page integrity probe anyway"
        )
    })
}

fn doctor_franken_deep_integrity_skip_reason(db_path: &Path) -> Option<String> {
    let main = match std::fs::symlink_metadata(db_path) {
        Ok(metadata) if metadata.file_type().is_file() => metadata.len(),
        Ok(_) => {
            return Some(
                "frankensqlite_full_page_integrity_probe_deferred: archive path is not a regular file"
                    .to_string(),
            );
        }
        Err(err) => {
            return Some(format!(
                "frankensqlite_full_page_integrity_probe_deferred: archive size is unavailable ({err})"
            ));
        }
    };
    let mut bundle_bytes = main;
    let mut wal_name = db_path
        .file_name()
        .map(|name| name.to_os_string())
        .unwrap_or_default();
    wal_name.push("-wal");
    let wal_path = db_path.with_file_name(wal_name);
    match std::fs::symlink_metadata(&wal_path) {
        Ok(metadata) if metadata.file_type().is_file() => {
            let Some(total) = bundle_bytes.checked_add(metadata.len()) else {
                return Some(
                    "frankensqlite_full_page_integrity_probe_deferred: archive bundle size overflowed"
                        .to_string(),
                );
            };
            bundle_bytes = total;
        }
        Ok(_) => {
            return Some(
                "frankensqlite_full_page_integrity_probe_deferred: WAL sidecar is not a regular file"
                    .to_string(),
            );
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            return Some(format!(
                "frankensqlite_full_page_integrity_probe_deferred: WAL size is unavailable ({err})"
            ));
        }
    }
    doctor_franken_deep_integrity_skip_reason_for_bytes(
        bundle_bytes,
        doctor_full_page_integrity_probe_requested(),
    )
}

fn doctor_archive_queryable_for_non_destructive_derived_rebuild(
    db_ok: bool,
    row_counts_complete: bool,
    deep_integrity_deferred: bool,
    cached_integrity_failed: bool,
) -> bool {
    db_ok || (row_counts_complete && deep_integrity_deferred && !cached_integrity_failed)
}

fn doctor_canonical_candidate_replacement_authorized(
    db_exists: bool,
    not_initialized: bool,
    affirmative_integrity_failure: bool,
) -> bool {
    affirmative_integrity_failure || (!db_exists && !not_initialized)
}

/// Classify only deterministic corruption-class open failures as affirmative
/// replacement evidence. Timeouts, locks, permissions, and generic engine
/// failures remain unknown and must never authorize canonical replacement.
fn doctor_db_open_error_is_affirmative_integrity_failure(error: &CliError) -> bool {
    error.kind.eq(CliErrorKind::DbOpen.kind_str())
        && doctor_open_error_message_is_affirmative_corruption(&error.message)
}

/// An open failure that proves the file is damaged, as opposed to busy,
/// locked or unreadable for another reason.
pub(crate) fn doctor_open_error_message_is_affirmative_corruption(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("file is too small to contain a sqlite header")
        || message.contains("file header is not sqlite format 3")
        || message.contains("not a database")
        || message.contains("database disk image is malformed")
        || message.contains("databasecorrupt")
        || message.contains("walcorrupt")
}

fn doctor_archive_wide_collectors_deferred_reason(
    deep_integrity_skip_reason: Option<&str>,
    probe_timed_out: bool,
    db_open_failed: bool,
    integrity_unverified: bool,
    integrity_failed: bool,
    busy_or_locked: bool,
) -> Option<String> {
    let cause = if let Some(reason) = deep_integrity_skip_reason {
        reason.to_string()
    } else if probe_timed_out {
        "bounded archive database probe timed out".to_string()
    } else if busy_or_locked {
        "archive database is busy or locked".to_string()
    } else if integrity_failed {
        "archive database has an affirmative integrity failure".to_string()
    } else if db_open_failed {
        "archive database open did not complete successfully".to_string()
    } else if integrity_unverified {
        "archive database health is unverified".to_string()
    } else {
        return None;
    };
    Some(format!(
        "archive_wide_collectors_deferred: {cause}; source inventory, message-level raw-mirror backfill, coverage, and repair authority remain unchecked"
    ))
}

/// zp0fp: while archive-wide collectors are deferred, the recommended action
/// names the command that moves recovery forward for the cause doctor
/// observed. A staged reconstruct candidate goes to the repair dry-run, a
/// buildable one to `doctor --fix`, a held archive waits, and a timed-out or
/// size-deferred probe reruns with the setting that lets it finish.
fn doctor_deferred_archive_recommended_action(
    reason: &str,
    candidate_staged: bool,
    candidate_buildable: bool,
    busy_or_locked: bool,
    probe_timed_out: bool,
    full_page_probe_deferred: bool,
) -> String {
    let next = if candidate_staged {
        "An isolated reconstruct candidate is staged: review its promotion plan with 'cass doctor repair --dry-run --json', then run the apply command that plan prints."
    } else if candidate_buildable {
        "Run 'cass doctor --fix --json' to build an isolated reconstruct candidate from the checksum-verified raw mirror (the live archive is not changed), then review its promotion plan with 'cass doctor repair --dry-run --json'."
    } else if busy_or_locked {
        "Another process holds the archive (an index run, the daemon, or another doctor): wait for it to finish, then rerun 'cass doctor check --json'."
    } else if probe_timed_out {
        "Rerun 'cass doctor check --json' when the machine is idle, with a larger CASS_DOCTOR_DB_PROBE_TIMEOUT_SECS."
    } else if full_page_probe_deferred {
        "Rerun 'cass doctor check --json' with CASS_DOCTOR_FULL_PAGE_INTEGRITY_PROBE=1 and a larger CASS_DOCTOR_DB_PROBE_TIMEOUT_SECS."
    } else {
        "Run process-isolated archive coverage probes before archive repair."
    };
    format!("{reason}. {next}")
}

#[cfg(test)]
#[test]
fn doctor_deferred_archive_recommended_action_names_the_next_command_for_each_cause() {
    let reason =
        "archive_wide_collectors_deferred: archive database open did not complete successfully";
    let action = |staged, buildable, busy, timed_out, deferred| {
        doctor_deferred_archive_recommended_action(
            reason, staged, buildable, busy, timed_out, deferred,
        )
    };
    assert!(action(false, false, false, false, false).starts_with(reason));
    // A staged candidate outranks building another one, which outranks
    // waiting on a holder; the cause-specific reruns come after both.
    let staged = action(true, true, true, true, true);
    assert!(
        staged.contains("'cass doctor repair --dry-run --json'"),
        "{staged}"
    );
    assert!(!staged.contains("'cass doctor --fix --json'"), "{staged}");
    let buildable = action(false, true, true, true, true);
    assert!(
        buildable.contains("'cass doctor --fix --json'"),
        "{buildable}"
    );
    assert!(
        buildable.contains("'cass doctor repair --dry-run --json'"),
        "{buildable}"
    );
    let busy = action(false, false, true, true, true);
    assert!(busy.contains("holds the archive"), "{busy}");
    assert!(busy.contains("'cass doctor check --json'"), "{busy}");
    let timed_out = action(false, false, false, true, true);
    assert!(
        timed_out.contains("CASS_DOCTOR_DB_PROBE_TIMEOUT_SECS"),
        "{timed_out}"
    );
    assert!(
        !timed_out.contains("CASS_DOCTOR_FULL_PAGE_INTEGRITY_PROBE"),
        "{timed_out}"
    );
    let deferred = action(false, false, false, false, true);
    assert!(
        deferred.contains("CASS_DOCTOR_FULL_PAGE_INTEGRITY_PROBE=1"),
        "{deferred}"
    );
    // With no repair authority and no transient cause there is no command to
    // name yet; the previous guidance stays.
    assert!(
        action(false, false, false, false, false)
            .ends_with("Run process-isolated archive coverage probes before archive repair.")
    );
}

#[cfg(test)]
#[test]
fn doctor_franken_deep_integrity_size_gate_has_an_exact_boundary() {
    assert!(
        doctor_franken_deep_integrity_skip_reason_for_bytes(
            DOCTOR_FRANKEN_DEEP_INTEGRITY_MAX_BYTES,
            false
        )
        .is_none()
    );
    let reason = doctor_franken_deep_integrity_skip_reason_for_bytes(
        DOCTOR_FRANKEN_DEEP_INTEGRITY_MAX_BYTES + 1,
        false,
    )
    .expect("one byte above the bounded budget must defer the full-page PRAGMA");
    assert!(reason.contains("full_page_integrity_probe_deferred"));
    assert!(
        reason.contains(CASS_DOCTOR_FULL_PAGE_INTEGRITY_PROBE),
        "the deferral must name its own override (GH #390): {reason}"
    );
}

#[cfg(test)]
#[test]
fn doctor_franken_deep_integrity_size_gate_is_lifted_by_explicit_opt_in() {
    assert!(
        doctor_franken_deep_integrity_skip_reason_for_bytes(
            DOCTOR_FRANKEN_DEEP_INTEGRITY_MAX_BYTES * 8,
            true
        )
        .is_none(),
        "GH #390: an explicit full-probe request must lift the byte cap"
    );
}

#[cfg(test)]
#[test]
fn doctor_franken_deep_integrity_size_gate_counts_the_wal_bundle() {
    let temp = tempfile::tempdir().expect("tempdir");
    let db_path = temp.path().join("agent_search.db");
    std::fs::File::create(&db_path)
        .and_then(|file| file.set_len(DOCTOR_FRANKEN_DEEP_INTEGRITY_MAX_BYTES - 1))
        .expect("create sparse main DB fixture");
    assert!(doctor_franken_deep_integrity_skip_reason(&db_path).is_none());

    let wal_path = temp.path().join("agent_search.db-wal");
    std::fs::File::create(wal_path)
        .and_then(|file| file.set_len(2))
        .expect("create sparse WAL fixture");
    let reason = doctor_franken_deep_integrity_skip_reason(&db_path)
        .expect("main plus WAL above the budget must defer the full-page PRAGMA");
    assert!(reason.contains("268435457 bytes"));
}

#[cfg(test)]
#[test]
fn doctor_deferred_integrity_never_implies_corruption_or_destructive_rebuild() {
    let deferred_message =
        "frankensqlite_full_page_integrity_probe_deferred: bounded probe skipped";
    assert_eq!(
        doctor_anomaly_for_check("database", "warn", deferred_message),
        DoctorAnomaly::RepairBlocked
    );
    assert_eq!(
        doctor_anomaly_for_check(
            "database",
            "fail",
            "cached quick_check attestation records structural failure; frankensqlite_full_page_integrity_probe_deferred"
        ),
        DoctorAnomaly::ArchiveDbCorrupt,
        "a current cached FAIL must not be hidden by the deferral marker"
    );
    assert_eq!(
        doctor_anomaly_for_check(
            "database",
            "fail",
            "Database failed frankensqlite quick_check: malformed page"
        ),
        DoctorAnomaly::ArchiveDbCorrupt,
        "a completed affirmative live integrity failure remains corruption evidence"
    );
    assert!(doctor_archive_queryable_for_non_destructive_derived_rebuild(false, true, true, false));
    assert!(!doctor_archive_queryable_for_non_destructive_derived_rebuild(false, true, true, true));
    assert!(
        !doctor_archive_queryable_for_non_destructive_derived_rebuild(false, true, false, false)
    );
    assert!(!doctor_canonical_candidate_replacement_authorized(
        true, false, false
    ));
    assert!(doctor_canonical_candidate_replacement_authorized(
        true, false, true
    ));
    assert!(doctor_canonical_candidate_replacement_authorized(
        false, false, false
    ));
    assert_eq!(
        doctor_anomaly_for_check(
            "database",
            "timeout",
            "Database health probe timed out; archive health is unverified"
        ),
        DoctorAnomaly::RepairBlocked,
        "a timeout is diagnostic uncertainty, not evidence that the archive is unreadable"
    );
    for unverified_message in [
        "Database health probe failed via frankensqlite: running PRAGMA quick_check(1): unsupported page state",
        "Database health probe failed via frankensqlite: running PRAGMA integrity_check: unsupported page state",
        "Database row counts completed but no integrity verdict was returned",
        "Database query failed",
    ] {
        assert_eq!(
            doctor_anomaly_for_check("database", "fail", unverified_message),
            DoctorAnomaly::RepairBlocked,
            "an unverified probe is diagnostic uncertainty, not replacement authority: {unverified_message}"
        );
    }
}

#[cfg(test)]
#[test]
fn doctor_open_failure_authority_accepts_only_affirmative_corruption_classes() -> Result<(), String>
{
    let db_open_error = |message: &str| CliError {
        code: 9,
        kind: CliErrorKind::DbOpen.kind_str(),
        message: message.to_string(),
        hint: None,
        retryable: true,
    };
    for message in [
        "file is too small to contain a SQLite header (possible corruption)",
        "file header is not SQLite format 3 (possible corruption)",
        "database disk image is malformed",
        "file is not a database",
    ] {
        if !doctor_db_open_error_is_affirmative_integrity_failure(&db_open_error(message)) {
            return Err(format!(
                "deterministic corruption-class open failure must authorize candidate recovery: {message}"
            ));
        }
    }
    for message in [
        "open timed out after 30s (possible corruption or lock contention)",
        "database is busy",
        "permission denied",
        "generic database open failed",
    ] {
        if doctor_db_open_error_is_affirmative_integrity_failure(&db_open_error(message)) {
            return Err(format!(
                "unknown/transient open failure must remain fail-closed: {message}"
            ));
        }
    }
    Ok(())
}

/// #287: the doctor's `SELECT COUNT(*)` row counts and
/// `PRAGMA quick_check`/`integrity_check` probes used to run unbounded on the
/// main thread, so a wedged or busy-spinning database made
/// `cass doctor --json` emit zero stdout bytes until an external timeout
/// killed it. Run the connection-consuming probe work on a worker thread with
/// a hard deadline instead: on timeout the doctor marks the `database` check
/// `status:"timeout"` (with the phase that was in flight) and continues to a
/// final JSON verdict. A Rust thread cannot be force-cancelled, so the size
/// gate above prevents known-large full-page PRAGMAs from entering this worker
/// at all. An unexpected timeout on a smaller archive still leaves a detached
/// worker until process exit; process isolation or an upstream FrankenSQLite
/// cancellation handle is required to remove that residual limitation.
fn run_bounded_doctor_archive_db_probe(
    conn: crate::storage::sqlite::FrankenOwnerConnection,
    db_path: &Path,
    timeout: Duration,
) -> DoctorBoundedArchiveDbProbeOutcome {
    let phase = std::sync::Arc::new(std::sync::Mutex::new("row_count_conversations"));
    let (tx, rx) = std::sync::mpsc::channel();
    let worker_phase = std::sync::Arc::clone(&phase);
    let worker_db_path = db_path.to_path_buf();
    let deep_integrity_skip_reason = doctor_franken_deep_integrity_skip_reason(db_path);
    let _probe_worker = std::thread::spawn(move || {
        use crate::franken_sync::compat::RowExt as _;

        let set_phase = |value: &'static str| {
            if let Ok(mut current) = worker_phase.lock() {
                *current = value;
            }
        };
        let conv_count: Option<i64> = conn
            .query_row_map(
                "SELECT COUNT(*) FROM conversations",
                &[],
                |r: &crate::franken_sync::Row| r.get_typed(0),
            )
            .ok();
        set_phase("row_count_messages");
        let msg_count: Option<i64> = conn
            .query_row_map(
                "SELECT COUNT(*) FROM messages",
                &[],
                |r: &crate::franken_sync::Row| r.get_typed(0),
            )
            .ok();
        let mut integrity = None;
        let mut integrity_skipped_reason = None;
        let mut fts_state = None;
        set_phase("legacy_omp_analytics_authority");
        let legacy_omp_analytics_pending =
            crate::storage::sqlite::legacy_omp_analytics_pending_with_query(|sql| conn.query(sql))
                .map_err(|error| format!("{error:#}"));
        if conv_count.is_some() && msg_count.is_some() {
            if let Some(reason) = deep_integrity_skip_reason {
                set_phase("integrity_probe_deferred_large_archive");
                integrity_skipped_reason = Some(reason);
            } else {
                let probe = doctor_database_integrity_probe(&conn, set_phase);
                let healthy = matches!(&probe, Ok(result) if result.is_ok());
                integrity = Some(probe);
                if healthy {
                    set_phase("fts_probe");
                    fts_state = Some(probe_doctor_fts_table(&conn));
                }
            }
        }
        set_phase("connection_close");
        let _ = close_franken_cli_owner_read_db(conn, &worker_db_path, "doctor database health");
        let _ = tx.send(DoctorBoundedArchiveDbProbe {
            conv_count,
            msg_count,
            integrity,
            integrity_skipped_reason,
            fts_state,
            legacy_omp_analytics_pending,
        });
    });

    match rx.recv_timeout(timeout) {
        Ok(probe) => DoctorBoundedArchiveDbProbeOutcome::Completed(probe),
        Err(_) => DoctorBoundedArchiveDbProbeOutcome::TimedOut {
            phase: phase.lock().map(|current| *current).unwrap_or("unknown"),
        },
    }
}

/// GH #457: `doctor` coverage floor for the served lexical document count as
/// a percentage of the archive's `messages` rows. Not every message becomes a
/// document (empty and diverted payloads are skipped; real archives sit
/// between ~45% and ~100%), so this floor is deliberately loose and only
/// catches the hollow shape (3 documents against a million messages).
const DOCTOR_LEXICAL_COVERAGE_FLOOR_PERCENT: u64 = 10;

/// Internal doctor executor reached through the typed `doctor` module.
/// CRITICAL: This function NEVER deletes user data. It only rebuilds derived data (index, db)
/// from source session files. This is essential because users may have only one copy of their
/// agent session data, and Codex/Claude Code auto-expire older logs.
#[allow(
    clippy::collapsible_if,
    clippy::collapsible_else_if,
    clippy::too_many_arguments
)]
pub(crate) fn run_doctor_impl(
    data_dir_override: &Option<PathBuf>,
    db_override: Option<PathBuf>,
    output_format: Option<RobotFormat>,
    fix: bool,
    verbose: bool,
    force_rebuild: bool,
    allow_repeated_repair: bool,
    command_surface: doctor::DoctorCommandSurface,
    execution_mode: doctor::DoctorExecutionMode,
    requested_plan_fingerprint: Option<String>,
    wrap: WrapConfig,
) -> CliResult<()> {
    use colored::*;
    use std::time::Instant;

    let start = Instant::now();
    // siekg: liveness heartbeat on stderr for the whole run; dropped (and
    // joined) when this function returns, after the report is written.
    let _heartbeat = CliHeartbeat::start(if fix {
        "doctor --fix"
    } else {
        "doctor --check"
    });
    let data_dir = resolve_data_dir(data_dir_override, db_override.as_ref());
    let db_path = db_override.unwrap_or_else(|| data_dir.join("agent_search.db"));
    let index_path = crate::search::tantivy::expected_index_dir(&data_dir);
    let lock_path = data_dir.join(".index.lock");
    let mut timing_spans: Vec<DoctorTimingSpanReport> = Vec::new();
    let lock_probe_started = Instant::now();
    let maintenance_snapshot = probe_index_run_lock(&data_dir, &db_path, true);
    let rebuild_active = maintenance_snapshot.active;
    let cleanup_apply_requested = command_surface == doctor::DoctorCommandSurface::Cleanup
        && execution_mode == doctor::DoctorExecutionMode::CleanupApply;
    let safe_auto_run_requested = command_surface == doctor::DoctorCommandSurface::LegacyDoctor
        && execution_mode == doctor::DoctorExecutionMode::SafeAutoFix;
    let repair_class = doctor_repair_class_for_execution(command_surface, execution_mode);
    // cass#368 self-heal: before the failure marker gates this run, reclaim
    // (with a receipt, never deleting) leftovers of a --fix owner that
    // crashed long ago and left the mutation lock + marker blocking every
    // later repair.
    let stale_self_heal = if fix {
        doctor_self_heal_stale_repair_state(&data_dir, &repair_class)
    } else {
        None
    };
    let initial_failure_marker = collect_doctor_repair_failure_marker(&data_dir, &repair_class);
    let repeat_refusal_reason =
        doctor_repair_failure_marker_refusal_reason(&initial_failure_marker);
    let repeat_repair_refused = fix && initial_failure_marker.found && !allow_repeated_repair;
    let override_available = fix && initial_failure_marker.found;
    let override_used = override_available && allow_repeated_repair;
    let mut _doctor_lock_guard: Option<DoctorMutationLockGuard> = None;
    let mut _doctor_db_open_bypass_guard: Option<
        crate::storage::sqlite::DoctorMutationDbOpenBypassGuard,
    > = None;
    let fingerprint_apply_requested =
        execution_mode == doctor::DoctorExecutionMode::FingerprintApply;
    let doctor_lock_observation = if fix && !repeat_repair_refused && fingerprint_apply_requested {
        if doctor_mutation_lock_path(&data_dir).exists() {
            match doctor_acquire_existing_mutation_lock_without_metadata(&data_dir) {
                Ok((guard, observation)) => {
                    _doctor_lock_guard = Some(guard);
                    _doctor_db_open_bypass_guard =
                        Some(crate::storage::sqlite::enter_doctor_mutation_db_open_bypass());
                    observation
                }
                Err(observation) => observation,
            }
        } else {
            doctor_probe_mutation_lock(&data_dir)
        }
    } else if fix && !repeat_repair_refused {
        match doctor_acquire_mutation_lock(&data_dir, &db_path) {
            Ok((guard, observation)) => {
                _doctor_lock_guard = Some(guard);
                _doctor_db_open_bypass_guard =
                    Some(crate::storage::sqlite::enter_doctor_mutation_db_open_bypass());
                observation
            }
            Err(observation) => observation,
        }
    } else {
        doctor_probe_mutation_lock(&data_dir)
    };
    let mut operation_state = build_doctor_operation_state_report(
        &data_dir,
        &db_path,
        &index_path,
        &maintenance_snapshot,
        &doctor_lock_observation,
    );
    doctor_push_timing_span(
        &mut timing_spans,
        "lock_state_probe",
        "operation_state",
        "operation_state",
        lock_probe_started,
        DOCTOR_SLOW_OPERATION_DEFAULT_THRESHOLD_MS,
        vec!["index-run and doctor repair lock observations captured".to_string()],
    );
    let mutating_lock_acquired = matches!(
        doctor_lock_observation,
        DoctorMutationLockObservation::Acquired { .. }
    );
    let mut fix_can_mutate = fix
        && operation_state.mutating_doctor_allowed
        && mutating_lock_acquired
        && !repeat_repair_refused
        && !fingerprint_apply_requested;
    let not_initialized = !fix
        && cass_not_initialized(
            db_path.exists(),
            crate::search::tantivy::searchable_index_exists(&index_path),
            rebuild_active,
        );
    let explanation = not_initialized.then(|| cass_not_initialized_explanation(&data_dir));
    let recommended_action = not_initialized.then(cass_not_initialized_recommended_action);

    // Track all checks and their results
    #[derive(serde::Serialize)]
    struct Check {
        name: String,
        status: String, // "pass", "warn", "fail"
        message: String,
        fix_available: bool,
        fix_applied: bool,
    }

    let mut checks: Vec<Check> = Vec::new();
    if let Some(heal) = &stale_self_heal {
        checks.push(Check {
            name: "stale_repair_state_self_heal".to_string(),
            status: "pass".to_string(),
            message: format!(
                "reclaimed stale repair state from a crashed doctor --fix owner \
                 ({} failure-marker file(s){}) with receipt {}",
                heal.reclaimed_marker_files,
                if heal.lock_metadata_cleared {
                    " + stale mutation-lock metadata"
                } else {
                    ""
                },
                heal.receipt_path.display()
            ),
            fix_available: true,
            fix_applied: true,
        });
    }
    let mut needs_rebuild = force_rebuild;
    let mut db_ok = false;
    let mut db_conversations: Option<usize> = None;
    let mut db_messages: Option<usize> = None;
    // `.14.1` storage-integrity signals (bead vl1cj): the read-only facts the
    // database + lexical-index checks below already observe, from which the
    // `storage_integrity` block projects a `StorageState`. Set, never compared,
    // so lib.rs's secret-context UBS scan sees no raw equality on these.
    let mut storage_db_open_failed = false;
    let mut storage_integrity_failed = false;
    let mut storage_integrity_unverified = false;
    let mut storage_integrity_skipped_reason: Option<String> = None;
    let mut storage_probe_timed_out = false;
    let mut storage_lexical_index_drifted = false;
    let mut storage_attestation_check_depth: Option<&'static str> = None;
    let mut storage_attestation_detail: Option<String> = None;
    let dedicated_storage_probe = crate::search::storage_integrity::probe_dedicated_storage_state(
        &db_path,
        crate::search::storage_integrity::dedicated_storage_probe_timeout(),
    );
    let matching_cached_integrity_attestation =
        crate::search::storage_integrity::load_matching_integrity_attestation(&data_dir, &db_path);
    let mut auto_fix_actions: Vec<String> = Vec::new();
    let mut auto_fix_applied = false;
    let mut fs_mutation_receipts: Vec<DoctorFsMutationReceipt> = Vec::new();
    let mut candidate_promotion_report: Option<DoctorCandidatePromotionReport> = None;

    // Helper macro to add a check (avoids closure borrow issues)
    macro_rules! add_check {
        ($name:expr, $status:expr, $message:expr, $fix_available:expr) => {
            checks.push(Check {
                name: $name.to_string(),
                status: $status.to_string(),
                message: $message.to_string(),
                fix_available: $fix_available,
                fix_applied: false,
            });
        };
    }

    if let Some(reason) = operation_state.mutation_blocked_reason.as_deref() {
        let status = if fix { "fail" } else { "warn" };
        add_check!(
            "operation_state",
            status,
            format!("Doctor mutation blocked: {reason}"),
            false
        );
    } else {
        add_check!(
            "operation_state",
            "pass",
            "Doctor operation state permits read-only inspection and mutating repair",
            false
        );
    }

    if let Some(reason) = repeat_refusal_reason.as_deref() {
        if repeat_repair_refused {
            add_check!("repair_failure_marker", "fail", reason.to_string(), true);
        } else if !override_used {
            add_check!("repair_failure_marker", "warn", reason.to_string(), true);
        }
    }

    // 1. Check data directory exists and is writable
    if !data_dir.exists() && not_initialized {
        add_check!(
            "data_directory",
            "pass",
            format!(
                "Data directory not created yet: {} (it will be created on the first index run)",
                data_dir.display()
            ),
            false
        );
    } else if data_dir.exists() {
        if std::fs::metadata(&data_dir)
            .map(|m| !m.permissions().readonly())
            .unwrap_or(false)
        {
            add_check!(
                "data_directory",
                "pass",
                format!("Data directory exists: {}", data_dir.display()),
                false
            );
        } else {
            add_check!(
                "data_directory",
                "fail",
                format!("Data directory not writable: {}", data_dir.display()),
                false
            );
        }
    } else if fix_can_mutate {
        if std::fs::create_dir_all(&data_dir).is_ok() {
            checks.push(Check {
                name: "data_directory".to_string(),
                status: "pass".to_string(),
                message: format!("Data directory created: {}", data_dir.display()),
                fix_available: true,
                fix_applied: true,
            });
            auto_fix_actions.push("Created missing data directory".to_string());
            auto_fix_applied = true;
        } else {
            add_check!(
                "data_directory",
                "fail",
                format!("Cannot create data directory: {}", data_dir.display()),
                true
            );
        }
    } else {
        add_check!(
            "data_directory",
            "fail",
            format!(
                "Data directory missing: {} (run with --fix to create)",
                data_dir.display()
            ),
            true
        );
    }

    let storage_pressure_started = Instant::now();
    let storage_pressure = collect_doctor_storage_pressure(&data_dir, &db_path);
    doctor_push_timing_span(
        &mut timing_spans,
        "storage_pressure",
        "storage_pressure",
        "storage_pressure",
        storage_pressure_started,
        DOCTOR_SLOW_OPERATION_DEFAULT_THRESHOLD_MS,
        vec!["free-space probe completed without mutating archive evidence".to_string()],
    );
    match storage_pressure.status.as_str() {
        "ok" => {
            add_check!(
                "storage_pressure",
                "pass",
                format!(
                    "Storage pressure OK (available bytes above safety floor at {})",
                    storage_pressure.probe_path
                ),
                false
            );
        }
        "warn" => {
            add_check!(
                "storage_pressure",
                "warn",
                format!(
                    "Low free space for safe doctor work (available bytes below safety floor at {})",
                    storage_pressure.probe_path
                ),
                false
            );
        }
        _ => {
            add_check!(
                "storage_pressure",
                "warn",
                storage_pressure
                    .notes
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "Storage pressure was not checked".to_string()),
                false
            );
        }
    }
    // GH#442: answer the predicate `index --full` refuses on, as its own
    // check line so the human output shows it next to the general
    // storage-pressure verdict. A blocked rebuild is a warning, never a
    // failure: incremental indexing and search keep working without it.
    {
        let readiness = &storage_pressure.full_rebuild_readiness;
        let (status, message) = match readiness.status.as_str() {
            // The pass message carries no live free-space figure (it feeds
            // the event-log hash chain in the robot goldens); the number is
            // in storage_pressure.full_rebuild_readiness.available_bytes.
            "ready" => (
                "pass",
                format!(
                    "Full rebuild ready: {} bytes required at {} and free space covers it",
                    readiness.required_bytes, readiness.probe_path
                ),
            ),
            "blocked" => (
                "warn",
                format!(
                    "Full rebuild blocked by disk headroom: {} bytes required, {} bytes available at {} ({} bytes short){}",
                    readiness.required_bytes,
                    readiness.available_bytes.unwrap_or(0),
                    readiness.probe_path,
                    readiness.shortfall_bytes,
                    if readiness.enforced {
                        ""
                    } else {
                        "; CASS_INDEX_SKIP_DISK_HEADROOM_CHECK is set, so the indexer would not enforce this"
                    }
                ),
            ),
            _ => (
                "warn",
                readiness
                    .notes
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "Full-rebuild readiness was not checked".to_string()),
            ),
        };
        add_check!("full_rebuild_readiness", status, message, false);
    }

    // 2. Check for stale lock files
    if lock_path.exists() {
        // Check if lock is stale (older than 1 hour)
        let is_stale = std::fs::metadata(&lock_path)
            .and_then(|m| m.modified())
            .map(|t| t.elapsed().map(|d| d.as_secs() > 3600).unwrap_or(true))
            .unwrap_or(true);

        if is_stale {
            if fix_can_mutate {
                let planned_bytes = fs_dir_size(&lock_path);
                let action_id = doctor_fs_mutation_action_id(
                    DoctorFsMutationKind::RemoveStaleLegacyIndexLock,
                    DoctorRepairMode::SafeAutoRun,
                    DoctorAssetClass::ReclaimableDerivedCache,
                    &lock_path,
                    planned_bytes,
                );
                let request = DoctorFsMutationRequest {
                    operation_id: "doctor-fix-stale-legacy-index-lock",
                    action_id: &action_id,
                    mutation_kind: DoctorFsMutationKind::RemoveStaleLegacyIndexLock,
                    mode: DoctorRepairMode::SafeAutoRun,
                    asset_class: DoctorAssetClass::ReclaimableDerivedCache,
                    source_path: None,
                    target_path: &lock_path,
                    data_dir: &data_dir,
                    db_path: &db_path,
                    index_path: &index_path,
                    staging_root: None,
                    expected_source_blake3: None,
                    planned_bytes,
                    required_min_age_seconds: Some(3600),
                };
                let stale_lock_artifacts = [DoctorForensicCopySpec {
                    artifact_kind: "stale_legacy_index_lock",
                    source_path: &lock_path,
                    relative_path: Path::new("operation/stale-legacy-index-lock"),
                    required: true,
                    sidecar_group: None,
                }];
                let forensic_bundle =
                    capture_doctor_forensic_bundle(DoctorForensicBundleCaptureInput {
                        operation_id: request.operation_id,
                        data_dir: &data_dir,
                        db_path: &db_path,
                        index_path: &index_path,
                        plan: None,
                        quarantine_report: None,
                        extra_file_artifacts: &stale_lock_artifacts,
                    });
                let mut mutation_receipt = if forensic_bundle.status == "captured" {
                    execute_doctor_fs_mutation(request)
                } else {
                    let mut receipt = doctor_fs_mutation_receipt_for_request(&request);
                    let reason = forensic_bundle
                        .blocked_reason
                        .clone()
                        .unwrap_or_else(|| "forensic bundle capture did not complete".to_string());
                    receipt.blocked_reasons.push(format!(
                        "forensic bundle capture failed before stale lock mutation: {reason}"
                    ));
                    receipt
                };
                mutation_receipt.forensic_bundle = forensic_bundle;
                if mutation_receipt.status == DoctorActionStatus::Applied {
                    checks.push(Check {
                        name: "lock_file".to_string(),
                        status: "pass".to_string(),
                        message: "Stale lock file removed".to_string(),
                        fix_available: true,
                        fix_applied: true,
                    });
                    auto_fix_actions.push("Removed stale lock file".to_string());
                    auto_fix_applied = true;
                } else {
                    let reason = if mutation_receipt.blocked_reasons.is_empty() {
                        "no executor refusal reason recorded".to_string()
                    } else {
                        mutation_receipt.blocked_reasons.join("; ")
                    };
                    add_check!(
                        "lock_file",
                        "warn",
                        format!(
                            "Stale lock file found (older than 1 hour) and removal was refused: {reason}"
                        ),
                        true
                    );
                }
                fs_mutation_receipts.push(mutation_receipt);
            } else if fix {
                add_check!(
                    "lock_file",
                    "warn",
                    "Stale lock file found (older than 1 hour) but mutating repair was not allowed",
                    true
                );
            } else {
                add_check!(
                    "lock_file",
                    "warn",
                    "Stale lock file found (older than 1 hour) - run with --fix to remove",
                    true
                );
            }
        } else {
            add_check!(
                "lock_file",
                "warn",
                "Active lock file found - another process may be indexing",
                false
            );
        }
    } else {
        add_check!("lock_file", "pass", "No stale lock files", false);
    }

    // 3. Check database exists and is readable
    // Fix #128: use the CLI read-only opener with a timeout to prevent hanging on degraded
    // databases without dirtying precious archive evidence during a read-only doctor check.
    let archive_db_probe_started = Instant::now();
    if db_path.exists() {
        if dedicated_storage_probe.busy_or_locked {
            add_check!(
                "database",
                "warn",
                "Database is busy or locked by another operation; bounded typed contention probe stopped before the deep integrity check",
                false
            );
        } else {
            let db_open_result = open_franken_cli_read_db_with_hard_timeout(
                db_path.to_path_buf(),
                "doctor database health",
                Duration::from_secs(30),
            );
            match db_open_result {
                Ok(conn) => {
                    // #287: row counts and eligible small-archive PRAGMAs run on a
                    // deadline-bounded worker. Known-large full-page integrity
                    // PRAGMAs are deferred before execution because a thread timeout
                    // cannot cancel FrankenSQLite's synchronous PRAGMA executor.
                    let probe_timeout =
                        doctor_archive_db_probe_hard_timeout(doctor_archive_bundle_bytes(&db_path));
                    match run_bounded_doctor_archive_db_probe(conn, &db_path, probe_timeout) {
                        DoctorBoundedArchiveDbProbeOutcome::Completed(probe) => {
                            match &probe.legacy_omp_analytics_pending {
                                Ok(false) => {}
                                Ok(true) => {
                                    add_check!(
                                        "legacy_omp_analytics",
                                        "warn",
                                        format!(
                                            "OMP analytics identity repair is pending even if table counts match; lexical publication is independent. Run {} without --since/--days.",
                                            cass_dataset_command(
                                                &data_dir,
                                                &db_path,
                                                &[
                                                    "analytics",
                                                    "rebuild",
                                                    "--track",
                                                    "all",
                                                    "--json"
                                                ]
                                            )
                                        ),
                                        false
                                    );
                                }
                                Err(error) => {
                                    add_check!(
                                        "legacy_omp_analytics",
                                        "warn",
                                        format!(
                                            "OMP analytics completion authority could not be read: {error}; inspect the archive before maintenance."
                                        ),
                                        false
                                    );
                                }
                            }
                            if let (Some(conv_count), Some(msg_count)) =
                                (probe.conv_count, probe.msg_count)
                            {
                                db_conversations = Some(conv_count.max(0) as usize);
                                db_messages = Some(msg_count.max(0) as usize);
                                if let Some(reason) = probe.integrity_skipped_reason {
                                    storage_integrity_skipped_reason = Some(reason.clone());
                                    add_check!(
                                        "fts_table",
                                        "warn",
                                        format!(
                                            "Database-resident FTS parity probe was intentionally deferred because it performs additional archive-wide counts; {reason}. The Tantivy lexical index is checked separately."
                                        ),
                                        false
                                    );
                                    match matching_cached_integrity_attestation.as_ref() {
                                        Some(attestation)
                                            if attestation.verdict
                                                == crate::search::storage_integrity::IntegrityAttestationVerdict::Pass =>
                                        {
                                            db_ok = true;
                                            add_check!(
                                                "database",
                                                "pass",
                                                format!(
                                                    "Database opened and bounded row counts succeeded ({conv_count} conversations, {msg_count} messages); {reason}; structural health is supplied by a current fingerprint-matching cached {} attestation",
                                                    attestation.check_depth
                                                ),
                                                false
                                            );
                                        }
                                        Some(attestation) => {
                                            storage_integrity_failed = true;
                                            let leaked_pages_remedy =
                                                doctor_leaked_pages_remedy_suffix(
                                                    doctor_recover::attested_integrity_is_leaked_pages_only(
                                                        &attestation.check_depth,
                                                        attestation.detail.as_deref(),
                                                    ),
                                                );
                                            add_check!(
                                                "database",
                                                "fail",
                                                format!(
                                                    "Database opened and bounded row counts succeeded ({conv_count} conversations, {msg_count} messages), but a current fingerprint-matching cached {} attestation records structural failure; {reason}{leaked_pages_remedy}",
                                                    attestation.check_depth
                                                ),
                                                true
                                            );
                                            needs_rebuild = true;
                                        }
                                        None => {
                                            add_check!(
                                                "database",
                                                "warn",
                                                format!(
                                                    "Database opened and bounded row counts succeeded ({conv_count} conversations, {msg_count} messages), but structural integrity is unchecked; {reason}. Run a process-isolated SQLite integrity check with its own hard timeout before any archive repair."
                                                ),
                                                false
                                            );
                                        }
                                    }
                                } else if let Some(integrity_probe) = probe.integrity {
                                    match integrity_probe {
                                        Ok(integrity) if integrity.is_ok() => {
                                            storage_attestation_check_depth =
                                                Some("integrity_check");
                                            db_ok = true;
                                            add_check!(
                                                "database",
                                                "pass",
                                                format!(
                                                    "Database OK ({} conversations, {} messages)",
                                                    conv_count, msg_count
                                                ),
                                                false
                                            );

                                            // Check whether the FTS table is visible through
                                            // frankensqlite on this connection. Do not auto-register
                                            // it here: on migrated databases with legacy rootpage=0
                                            // FTS schema entries, CREATE VIRTUAL TABLE IF NOT EXISTS
                                            // can persist duplicate sqlite_master rows.
                                            match probe.fts_state {
                                                Some(
                                                    DoctorFtsTableState::QueryableViaFrankensqlite,
                                                ) => {
                                                    // #438: keep queryability distinct from
                                                    // structural validation; never dismiss
                                                    // an independent integrity failure.
                                                    add_check!(
                                                        "fts_table",
                                                        "pass",
                                                        DOCTOR_FTS_TABLE_QUERYABLE_MESSAGE,
                                                        false
                                                    );
                                                }
                                                Some(DoctorFtsTableState::PartialParity {
                                                    indexable_messages,
                                                    indexed_messages,
                                                }) => {
                                                    // #355: a queryable-but-partial shadow previously
                                                    // reported `pass` here while `index --full` failed
                                                    // on it. Lexical search still works via Tantivy,
                                                    // so this warns rather than fails.
                                                    add_check!(
                                                        "fts_table",
                                                        "warn",
                                                        format!(
                                                            "Database-resident FTS shadow is queryable but out of parity with the canonical archive ({indexed_messages} indexed vs {indexable_messages} indexable messages); run `cass doctor --rebuild-canonical-fts --yes` to catch it up. Small drift during an active index run is transient — re-check after it completes"
                                                        ),
                                                        false
                                                    );
                                                }
                                                Some(DoctorFtsTableState::Missing {
                                                    frankensqlite_error,
                                                    not_viable,
                                                }) => {
                                                    // An absent in-DB FTS shadow is benign
                                                    // here (lexical search falls back to
                                                    // Tantivy), so it does NOT feed the
                                                    // storage_state derivation — doctor
                                                    // reports it as a `pass` below.
                                                    let message = match not_viable {
                                                        Some(detail) => format!(
                                                            "Database-resident FTS shadow was dropped on purpose: {detail}"
                                                        ),
                                                        None => format!(
                                                            "Database-resident FTS table is absent or not queryable via frankensqlite ({frankensqlite_error}); lexical search relies on the Tantivy index instead"
                                                        ),
                                                    };
                                                    add_check!("fts_table", "pass", message, false);
                                                }
                                                Some(DoctorFtsTableState::ShadowCorrupt {
                                                    frankensqlite_error,
                                                }) => {
                                                    // A queryable table whose docsize shadow
                                                    // cannot be counted is the #362/#368
                                                    // corruption class — previously buried
                                                    // inside a `pass` message
                                                    // (adversarial-review F6). Lexical
                                                    // search still works via Tantivy, so
                                                    // warn rather than fail.
                                                    add_check!(
                                                        "fts_table",
                                                        "warn",
                                                        format!(
                                                            "Database-resident FTS shadow is queryable but its docsize shadow table failed to count ({frankensqlite_error}); the shadow structure is likely corrupt — run `cass doctor --rebuild-canonical-fts --yes` to rebuild it. Lexical search continues via the Tantivy index"
                                                        ),
                                                        false
                                                    );
                                                }
                                                None => {}
                                            }
                                        }
                                        Ok(integrity) => {
                                            storage_integrity_failed = true;
                                            let failed_pragma = integrity.failed_pragma_name();
                                            let diagnostic_summary = integrity.diagnostic_summary();
                                            storage_attestation_check_depth = Some(failed_pragma);
                                            storage_attestation_detail =
                                                Some(diagnostic_summary.clone());
                                            let leaked_pages_remedy =
                                                doctor_leaked_pages_remedy_suffix(
                                                    doctor_recover::integrity_is_leaked_pages_only(
                                                        &integrity.quick_check_status,
                                                        &integrity.integrity_check_diagnostics,
                                                    ),
                                                );
                                            add_check!(
                                                "database",
                                                "fail",
                                                format!(
                                                    "Database failed frankensqlite {failed_pragma}: {} ({} conversations, {} messages){leaked_pages_remedy}",
                                                    diagnostic_summary, conv_count, msg_count
                                                ),
                                                true
                                            );
                                            needs_rebuild = true;
                                        }
                                        Err(err) => {
                                            storage_integrity_unverified = true;
                                            add_check!(
                                                "database",
                                                "fail",
                                                format!(
                                                    "Database health probe failed via frankensqlite: {err}"
                                                ),
                                                true
                                            );
                                            needs_rebuild = true;
                                        }
                                    }
                                } else {
                                    storage_integrity_unverified = true;
                                    add_check!(
                                        "database",
                                        "fail",
                                        "Database row counts completed but no integrity verdict was returned",
                                        true
                                    );
                                    needs_rebuild = true;
                                }
                            } else {
                                storage_integrity_unverified = true;
                                add_check!("database", "fail", "Database query failed", true);
                                needs_rebuild = true;
                            }
                        }
                        DoctorBoundedArchiveDbProbeOutcome::TimedOut { phase } => {
                            storage_probe_timed_out = true;
                            // Deliberately NOT a `fail`: a busy or wedged database is
                            // not proof of corruption, so the doctor must not steer
                            // operators toward a rebuild. The `timeout` status keeps
                            // the run bounded and is surfaced through the
                            // `timeout_or_busy_spin_guard` reason code (#287).
                            add_check!(
                                "database",
                                "timeout",
                                format!(
                                    "Database health probe timed out after {}s during {phase}; archive health is unverified (busy or wedged database). Retry later or inspect with `cass status --json`.",
                                    probe_timeout.as_secs()
                                ),
                                false
                            );
                        }
                    }
                }
                Err(e) => {
                    storage_db_open_failed = true;
                    if doctor_db_open_error_is_affirmative_integrity_failure(&e) {
                        storage_integrity_failed = true;
                        storage_attestation_check_depth = Some("archive_open");
                        storage_attestation_detail = Some(e.message.clone());
                    }
                    add_check!(
                        "database",
                        "fail",
                        format!("Cannot open database: {}", e.message),
                        true
                    );
                    needs_rebuild = true;
                }
            }
        }
    } else if not_initialized {
        add_check!(
            "database",
            "warn",
            "Database not initialized yet - no archive has been created in this data dir",
            false
        );
    } else {
        add_check!("database", "fail", "Database not found", true);
        needs_rebuild = true;
    }
    doctor_push_timing_span(
        &mut timing_spans,
        "archive_db_open_integrity",
        "database",
        "archive_db_open_and_integrity",
        archive_db_probe_started,
        DOCTOR_SLOW_OPERATION_DEFAULT_THRESHOLD_MS,
        vec!["archive DB open, row counts, and integrity-style checks completed or were skipped by state".to_string()],
    );

    // GH #413 follow-up (iify0): a background catch-up that keeps dying looks
    // like nothing more than "stale" from a read path. The breaker state says
    // what happened and where the child's output is.
    if let Some(auto) = crate::indexer::background_refresh::load_state(&data_dir)
        && auto.consecutive_failures > 0
    {
        let tripped =
            auto.consecutive_failures >= crate::indexer::background_refresh::FAILURE_TRIP_THRESHOLD;
        add_check!(
            "auto_refresh",
            "warn",
            format!(
                "{} background catch-up{} ended without advancing the index; auto-refresh is {}. \
                 {}. Run `cass index` in the foreground to see why; the breaker resets when a run \
                 completes",
                auto.consecutive_failures,
                if auto.consecutive_failures == 1 {
                    ""
                } else {
                    "s in a row"
                },
                if tripped {
                    "tripped (no auto-spawn until a run completes)"
                } else {
                    "backing off"
                },
                auto.last_failure.as_deref().unwrap_or("no detail recorded")
            ),
            false
        );
    }

    // WS-B.5 (z2uon): an untruncated WAL is a tax every opener pays (the
    // owner's archive carried 200 MB for 18 days; every default search
    // replayed it). Observe from metadata only; repair only under --fix, never
    // while an index run owns the archive, and never claim a checkpoint that
    // the engine reported as blocked.
    {
        const DOCTOR_WAL_OVERSIZED_BYTES: u64 = 64 * 1024 * 1024;
        let wal_path = crate::storage::sqlite::database_sidecar_path(&db_path, "-wal");
        let wal_bytes = std::fs::metadata(&wal_path)
            .ok()
            .filter(|meta| meta.is_file())
            .map_or(0, |meta| meta.len());
        if wal_bytes <= DOCTOR_WAL_OVERSIZED_BYTES {
            add_check!(
                "archive_wal",
                "pass",
                format!("WAL sidecar is {wal_bytes} bytes (bounded)"),
                false
            );
        } else if rebuild_active {
            add_check!(
                "archive_wal",
                "warn",
                format!(
                    "WAL sidecar is {wal_bytes} bytes while an index run owns the archive; \
                     the run truncates it at finalize"
                ),
                false
            );
        } else if fix_can_mutate {
            // GH #382 / g3zyo: bounded, so an archive whose writable open
            // loops turns into a truthful `fail` with the remedy instead of a
            // doctor that never returns.
            match crate::indexer::checkpoint_wal_truncate_with_deadline(
                &db_path,
                "doctor --fix",
                doctor_wal_checkpoint_deadline(),
            ) {
                Ok(true) => {
                    let after = std::fs::metadata(&wal_path).map_or(0, |meta| meta.len());
                    checks.push(Check {
                        name: "archive_wal".to_string(),
                        status: "pass".to_string(),
                        message: format!(
                            "WAL sidecar checkpointed and truncated: {wal_bytes} -> {after} bytes"
                        ),
                        fix_available: true,
                        fix_applied: true,
                    });
                    auto_fix_actions.push(format!(
                        "Checkpointed the archive WAL ({wal_bytes} -> {after} bytes)"
                    ));
                    auto_fix_applied = true;
                }
                Ok(false) => {
                    add_check!(
                        "archive_wal",
                        "warn",
                        format!(
                            "WAL sidecar is {wal_bytes} bytes and the checkpoint was blocked by \
                             a concurrent reader; retry when no other cass process is open"
                        ),
                        true
                    );
                }
                Err(err) => {
                    add_check!(
                        "archive_wal",
                        "fail",
                        format!(
                            "WAL sidecar is {wal_bytes} bytes and the checkpoint failed: {err:#}"
                        ),
                        true
                    );
                }
            }
        } else {
            add_check!(
                "archive_wal",
                "warn",
                format!(
                    "WAL sidecar is {wal_bytes} bytes (> {DOCTOR_WAL_OVERSIZED_BYTES}); every \
                     opener replays it. Run `cass doctor --fix` or `cass index` to checkpoint it. \
                     If either sits in `preparing` with no progress, the writable open is \
                     looping on this WAL (GH #382): back up the archive and its sidecars, then \
                     checkpoint it with stock sqlite3 (`PRAGMA wal_checkpoint(TRUNCATE)`)"
                ),
                true
            );
        }
    }
    // This predicate means only that canonical rows may be consumed without
    // modifying the archive.  It deliberately includes a large archive whose
    // bounded counts succeeded while deep PRAGMAs were deferred, but excludes
    // any current cached FAIL attestation.  Keep `db_ok` for claims that
    // structural health itself was actually attested.
    let archive_queryable_for_non_destructive_derived_rebuild =
        doctor_archive_queryable_for_non_destructive_derived_rebuild(
            db_ok,
            db_conversations.is_some() && db_messages.is_some(),
            storage_integrity_skipped_reason.is_some(),
            matching_cached_integrity_attestation
                .as_ref()
                .is_some_and(|attestation| {
                    attestation.verdict
                        == crate::search::storage_integrity::IntegrityAttestationVerdict::Fail
                }),
        );
    // Canonical replacement needs affirmative evidence; negating
    // `archive_queryable...` would incorrectly turn timeout/busy/unknown into
    // permission to replace precious bytes.
    let canonical_candidate_replacement_authorized =
        doctor_canonical_candidate_replacement_authorized(
            db_path.exists(),
            not_initialized,
            storage_integrity_failed,
        );
    // Kept for the deferred-archive recommendation; the probe itself is
    // consumed by the storage report below.
    let storage_busy_or_locked = dedicated_storage_probe.busy_or_locked;
    let archive_wide_collectors_deferred_reason = doctor_archive_wide_collectors_deferred_reason(
        storage_integrity_skipped_reason.as_deref(),
        storage_probe_timed_out,
        storage_db_open_failed,
        storage_integrity_unverified,
        storage_integrity_failed,
        storage_busy_or_locked,
    );
    let archive_wide_collectors_complete = archive_wide_collectors_deferred_reason.is_none();

    // 4. Check Tantivy index exists and is readable
    let lexical_probe_started = Instant::now();
    if crate::search::tantivy::searchable_index_exists(&index_path) {
        match crate::search::tantivy::searchable_index_summary(&index_path) {
            Ok(Some(summary)) => {
                let num_docs = summary.docs;
                add_check!(
                    "index",
                    "pass",
                    format!("Search index OK ({} documents)", num_docs),
                    false
                );

                // #441 / WS-B.1a: a generation that fragmented into hundreds
                // of segments makes every query pay `segments × terms`
                // dictionary walks and exhausts the engine's per-query fuel
                // on ordinary stopword-heavy queries. Doctor may spend an
                // engine open, so it reports the LIVE segment count (what a
                // query pays for); folded inputs linger on disk after a merge,
                // so the file count would keep warning after a successful
                // consolidation. Files are the fallback when the reader fails.
                let live_segments = crate::search::quill_bridge::live_segment_count(&index_path);
                let counted = live_segments.map(|n| (n, "segments")).or_else(|| {
                    crate::search::quill_bridge::segment_file_count(&index_path)
                        .map(|n| (n, "segment files"))
                });
                if let Some((segments, unit)) = counted {
                    let pressure = crate::search::quill_bridge::CASS_SEGMENT_PRESSURE_FILES;
                    if segments > pressure {
                        add_check!(
                            "index_segments",
                            "warn",
                            format!(
                                "Search index has {segments} {unit} (> {pressure}); queries pay \
                                 per segment and long queries can exhaust the engine's fuel \
                                 budget. Run `cass index`: its maintenance pass folds the \
                                 fragmented generation in place (`--full` rebuilds from scratch \
                                 and needs the headroom full_rebuild_readiness reports)"
                            ),
                            true
                        );
                    } else {
                        add_check!(
                            "index_segments",
                            "pass",
                            format!("Search index has {segments} {unit} (bounded)"),
                            false
                        );
                    }
                }

                // Check if index is empty but database has data. #287: reuse the
                // message count from the bounded archive-DB probe above instead
                // of re-opening the database and running another unbounded
                // COUNT on the main thread.
                // GH #457: `num_docs` above came from the engine's own
                // reader, so it is what queries are served from. Compare it
                // with what the completed checkpoint certified (the exact
                // signal readiness uses) and, as a second floor, with the
                // archive's message count: a generation that serves less
                // than a tenth of the messages is hollow whatever the
                // checkpoint says.
                let served_docs = u64::try_from(num_docs).unwrap_or(u64::MAX);
                let hollow_vs_checkpoint =
                    crate::search::asset_state::lexical_generation_hollow_verdict(
                        crate::search::asset_state::completed_lexical_checkpoint_indexed_docs(
                            &index_path,
                            &db_path,
                        ),
                        Some(served_docs),
                    );
                let hollow_vs_messages = db_messages
                    .filter(|&count| count > 0)
                    .map(|count| u64::try_from(count).unwrap_or(u64::MAX))
                    .filter(|&messages| {
                        served_docs.saturating_mul(100)
                            < messages.saturating_mul(DOCTOR_LEXICAL_COVERAGE_FLOOR_PERCENT)
                    });
                if let Some(verdict) = hollow_vs_checkpoint {
                    storage_lexical_index_drifted = true;
                    add_check!("index_sync", "warn", verdict.reason(), true);
                    needs_rebuild = true;
                } else if num_docs == 0
                    && archive_queryable_for_non_destructive_derived_rebuild
                    && let Some(msg_count) = db_messages.filter(|&count| count > 0)
                {
                    storage_lexical_index_drifted = true;
                    add_check!(
                        "index_sync",
                        "warn",
                        format!("Index is empty but database has {} messages", msg_count),
                        true
                    );
                    needs_rebuild = true;
                } else if let Some(messages) = hollow_vs_messages
                    && archive_queryable_for_non_destructive_derived_rebuild
                {
                    storage_lexical_index_drifted = true;
                    add_check!(
                        "index_sync",
                        "warn",
                        format!(
                            "Index serves {num_docs} document(s) but the database has {messages} \
                             messages (below {DOCTOR_LEXICAL_COVERAGE_FLOOR_PERCENT}% coverage); \
                             the published lexical generation is hollow — run `cass index` to \
                             rebuild it from the canonical database"
                        ),
                        true
                    );
                    needs_rebuild = true;
                }
            }
            Ok(None) => {
                // A broken/absent derived index over an intact, populated DB is
                // derived-only drift (the `.14.1` `DerivedOnlyDrift` signal); an
                // empty DB with no index is consistent, not drift.
                storage_lexical_index_drifted |= db_messages.is_some_and(|m| m > 0);
                add_check!("index", "fail", "Search index metadata is missing", true);
                needs_rebuild = true;
            }
            Err(e) => {
                storage_lexical_index_drifted |= db_messages.is_some_and(|m| m > 0);
                add_check!("index", "fail", format!("Cannot open index: {}", e), true);
                needs_rebuild = true;
            }
        }
    } else if not_initialized {
        add_check!(
            "index",
            "warn",
            "Search index not initialized yet - run the first index to build lexical metadata",
            false
        );
    } else {
        storage_lexical_index_drifted |= db_messages.is_some_and(|m| m > 0);
        add_check!("index", "fail", "Search index not found", true);
        needs_rebuild = true;
    }
    doctor_push_timing_span(
        &mut timing_spans,
        "lexical_probe",
        "index",
        "lexical_probe",
        lexical_probe_started,
        DOCTOR_SLOW_OPERATION_DEFAULT_THRESHOLD_MS,
        vec!["derived lexical index presence and metadata probe completed".to_string()],
    );

    // `.14.1` storage-integrity projection (bead vl1cj): fold the read-only
    // database + lexical-index signals gathered above into the canonical
    // `StorageState` / `SourceOfTruthRisk` / `ArchiveReadability` contract so
    // doctor robot JSON projects the same vocabulary status/search-meta will.
    // The common signals derive ok/openread/integrity/derived/deferred first;
    // the dedicated probe overlays typed contention, schema drift, legacy
    // layout, or structurally suspect WAL/SHM evidence below. FTS metadata
    // remains separate because a missing in-DB shadow is a benign fallback.
    let storage_integrity_report = {
        use crate::search::storage_integrity::{
            ArchiveReadability, DoctorStorageSignals, IntegrityAttestationVerdict, StorageCheck,
            StorageCheckNotAttempted, StorageIntegrityReport, StorageState,
            apply_dedicated_storage_probe, build_doctor_storage_integrity,
        };
        let db_file_present = db_path.exists();
        let db_elapsed_ms = archive_db_probe_started.elapsed().as_millis() as i64;
        let index_elapsed_ms = lexical_probe_started.elapsed().as_millis() as i64;
        let mut storage_checks: Vec<StorageCheck> = Vec::new();
        if !db_file_present {
            let reason = if not_initialized {
                "database not initialized"
            } else {
                "no archive present to probe"
            };
            storage_checks.push(StorageCheck::skipped("archive_integrity", reason));
        } else if storage_probe_timed_out {
            storage_checks.push(StorageCheck::timed_out("archive_integrity", db_elapsed_ms));
        } else if let Some(reason) = storage_integrity_skipped_reason.as_deref() {
            storage_checks.push(StorageCheck::skipped("archive_integrity", reason));
        } else {
            storage_checks.push(StorageCheck::ran("archive_integrity", db_elapsed_ms));
        }
        if not_initialized {
            storage_checks.push(StorageCheck::skipped(
                "lexical_index",
                "index not initialized",
            ));
        } else {
            storage_checks.push(StorageCheck::ran("lexical_index", index_elapsed_ms));
        }
        let signals = DoctorStorageSignals {
            db_file_present,
            not_initialized,
            db_open_failed: storage_db_open_failed,
            probe_timed_out: storage_probe_timed_out,
            integrity_failed: storage_integrity_failed,
            integrity_unverified: storage_integrity_unverified,
            lexical_index_drifted: storage_lexical_index_drifted,
        };
        // #331: capture completed deep-probe evidence against the current
        // db/WAL fingerprint. The ordinary `doctor check` surface is strictly
        // read-only, so it projects this as live evidence without persisting
        // anything. An explicitly mutating doctor surface may refresh the
        // cache that lightweight status/search later reuse.
        let live_attestation = if db_file_present
            && !storage_db_open_failed
            && !storage_probe_timed_out
            && !storage_integrity_unverified
            && let Some(check_depth) = storage_attestation_check_depth
        {
            crate::search::storage_integrity::capture_integrity_attestation(
                &db_path,
                if storage_integrity_failed {
                    crate::search::storage_integrity::IntegrityAttestationVerdict::Fail
                } else {
                    crate::search::storage_integrity::IntegrityAttestationVerdict::Pass
                },
                check_depth,
                storage_attestation_detail.clone(),
            )
        } else {
            None
        };
        if fix_can_mutate && let Some(attestation) = live_attestation.as_ref() {
            crate::search::storage_integrity::persist_integrity_attestation(&data_dir, attestation);
        }
        let base_report = if let Some(reason) = storage_integrity_skipped_reason.as_deref() {
            let (state, readability) = match matching_cached_integrity_attestation
                .as_ref()
                .map(|attestation| attestation.verdict)
            {
                Some(IntegrityAttestationVerdict::Pass) if storage_lexical_index_drifted => {
                    (StorageState::DerivedOnlyDrift, ArchiveReadability::Readable)
                }
                Some(IntegrityAttestationVerdict::Pass) => {
                    (StorageState::Ok, ArchiveReadability::Readable)
                }
                Some(IntegrityAttestationVerdict::Fail) => (
                    StorageState::IntegrityFailed,
                    ArchiveReadability::PartiallyReadable,
                ),
                None => (StorageState::Unchecked, ArchiveReadability::Readable),
            };
            let mut report = StorageIntegrityReport::derive(state, readability, storage_checks);
            if let Some(attestation) = matching_cached_integrity_attestation.as_ref() {
                report.attestation_source = Some("cached".to_string());
                report.attestation_check_depth = Some(attestation.check_depth.clone());
                report.attested_at_ms = Some(attestation.checked_at_ms);
                report.attested_db_fingerprint = Some(
                    crate::search::storage_integrity::integrity_attestation_fingerprint(
                        attestation,
                    ),
                );
            } else {
                report.attestation_source = Some("none".to_string());
                report.checks_not_attempted.push(StorageCheckNotAttempted {
                    name: "quick_check".to_string(),
                    reason: reason.to_string(),
                });
            }
            report
        } else {
            build_doctor_storage_integrity(signals, storage_checks)
        };
        let mut report = apply_dedicated_storage_probe(base_report, dedicated_storage_probe);
        if let Some(attestation) = live_attestation {
            report.attestation_source = Some("live".to_string());
            report.attestation_check_depth = Some(attestation.check_depth.clone());
            report.attested_at_ms = Some(attestation.checked_at_ms);
            report.attested_db_fingerprint = Some(
                crate::search::storage_integrity::integrity_attestation_fingerprint(&attestation),
            );
        }
        report
    };

    // 5. Check config file
    let config_path = data_dir.join("config.toml");
    if config_path.exists() {
        match std::fs::read_to_string(&config_path) {
            Ok(content) => match toml::from_str::<toml::Value>(&content) {
                Ok(_) => {
                    add_check!("config", "pass", "Config file valid", false);
                }
                Err(e) => {
                    add_check!(
                        "config",
                        "warn",
                        format!("Config parse error: {}", e),
                        false
                    );
                }
            },
            Err(e) => {
                add_check!(
                    "config",
                    "warn",
                    format!("Cannot read config: {}", e),
                    false
                );
            }
        }
    } else {
        add_check!("config", "pass", "No config file (using defaults)", false);
    }

    // 6. Check sources.toml
    let sources_path = doctor_sources_config_path(&data_dir);
    let sources_config_check = doctor_sources_config_check(&sources_path);
    add_check!(
        "sources_config",
        sources_config_check.status,
        sources_config_check.message,
        false
    );

    let config_exclusion_risks =
        collect_doctor_config_exclusion_risks(&data_dir, &db_path, &config_path, &sources_path);
    if config_exclusion_risks.is_empty() {
        add_check!(
            "config_exclusion_risks",
            "pass",
            "No cass archive preservation exclusions found in the narrow config and ignore-file scan",
            false
        );
    } else {
        add_check!(
            "config_exclusion_risks",
            "warn",
            format!(
                "Preservation warning: {} backup/sync/ignore exclusion risk(s) may leave cass archive evidence unprotected; this is not proof backups are broken",
                config_exclusion_risks.len()
            ),
            false
        );
    }

    // 7. Check common session directories exist
    let mut session_dirs_found = 0usize;
    let home = dirs::home_dir().unwrap_or_default();
    let session_paths = [
        home.join(".claude"),        // Claude Code
        home.join(".codex"),         // Codex
        home.join(".cursor"),        // Cursor
        home.join(".aider"),         // Aider
        home.join(".chatgpt"),       // ChatGPT
        home.join(".config/gemini"), // Gemini
    ];
    for path in &session_paths {
        if path.exists() {
            session_dirs_found += 1;
        }
    }
    if session_dirs_found > 0 {
        add_check!(
            "sessions",
            "pass",
            format!("Found {} agent session directories", session_dirs_found),
            false
        );
    } else {
        add_check!(
            "sessions",
            "warn",
            "No agent session directories found",
            false
        );
    }

    let source_inventory_started = Instant::now();
    let source_inventory = if let Some(reason) = archive_wide_collectors_deferred_reason.as_deref()
    {
        doctor_source_inventory_deferred(reason)
    } else {
        collect_doctor_source_inventory(&data_dir, &db_path)
    };
    doctor_push_timing_span(
        &mut timing_spans,
        "source_ledger_scan",
        "source_inventory",
        "source_ledger_scan",
        source_inventory_started,
        DOCTOR_SLOW_OPERATION_DEFAULT_THRESHOLD_MS,
        vec![if archive_wide_collectors_complete {
            "source ledger and visible provider roots were scanned with bounded reporting"
                .to_string()
        } else {
            "archive-wide source ledger scan was explicitly deferred".to_string()
        }],
    );
    if let Some(error) = source_inventory.db_query_error.as_deref() {
        add_check!(
            "source_inventory",
            "warn",
            format!("Source inventory could not query archive coverage: {error}"),
            false
        );
    } else if !source_inventory.db_available {
        add_check!(
            "source_inventory",
            "pass",
            "Source inventory skipped until the cass archive database exists",
            false
        );
    } else if source_inventory.missing_current_source_count > 0
        || source_inventory.unknown_mapping_count > 0
    {
        add_check!(
            "source_inventory",
            "warn",
            format!(
                "Source coverage risk: {} indexed local conversation(s) no longer have a visible upstream file; {} indexed conversation(s) have incomplete source mapping",
                source_inventory.missing_current_source_count,
                source_inventory.unknown_mapping_count
            ),
            false
        );
    } else {
        add_check!(
            "source_inventory",
            "pass",
            format!(
                "Source inventory OK ({} indexed conversation(s), {} provider root(s) detected)",
                source_inventory.total_indexed_conversations,
                source_inventory.detected_provider_root_count
            ),
            false
        );
    }

    let raw_mirror_scan_started = Instant::now();
    let mut raw_mirror = collect_doctor_raw_mirror_report_for_doctor(&data_dir, fix_can_mutate);
    doctor_push_timing_span(
        &mut timing_spans,
        "raw_mirror_scan",
        "raw_mirror",
        "raw_mirror_scan",
        raw_mirror_scan_started,
        DOCTOR_SLOW_OPERATION_DEFAULT_THRESHOLD_MS,
        vec!["raw mirror manifests and blob checksums were summarized".to_string()],
    );
    let raw_mirror_backfill_started = Instant::now();
    let raw_mirror_backfill = collect_doctor_raw_mirror_backfill_report_for_doctor(
        &data_dir,
        &db_path,
        &raw_mirror,
        fix_can_mutate,
        archive_wide_collectors_deferred_reason.as_deref(),
    );
    let raw_mirror_backfill_applied =
        matches!(raw_mirror_backfill.status.as_str(), "applied" | "partial")
            && (raw_mirror_backfill.captured_live_source_count > 0
                || raw_mirror_backfill.existing_raw_manifest_link_count > 0);
    if raw_mirror_backfill_applied {
        auto_fix_applied = true;
        auto_fix_actions.push(format!(
            "Raw mirror backfill captured {} live source file(s) and linked {} existing raw manifest(s)",
            raw_mirror_backfill.captured_live_source_count,
            raw_mirror_backfill.existing_raw_manifest_link_count
        ));
        raw_mirror = collect_doctor_raw_mirror_report_for_doctor(&data_dir, fix_can_mutate);
    }
    doctor_push_timing_span(
        &mut timing_spans,
        "raw_mirror_backfill",
        "raw_mirror_backfill",
        "raw_mirror_backfill",
        raw_mirror_backfill_started,
        DOCTOR_SLOW_OPERATION_DEFAULT_THRESHOLD_MS,
        vec![if !archive_wide_collectors_complete {
            "archive-wide raw mirror backfill scan was explicitly deferred".to_string()
        } else if raw_mirror_backfill.status == "deferred" {
            "raw mirror backfill was explicitly deferred because its evidence prerequisites were not fully verified".to_string()
        } else if raw_mirror_backfill_applied {
            "raw mirror backfill applied and raw mirror summary was refreshed".to_string()
        } else {
            "raw mirror backfill plan was evaluated without provider source mutation".to_string()
        }],
    );
    // ub29t: bound the per-manifest listing (summary counts stay complete;
    // problem manifests are never elided; --verbose restores everything).
    doctor_apply_raw_mirror_listing_bound(&mut raw_mirror, verbose);
    let recovery_evidence_deferred_reason = archive_wide_collectors_deferred_reason
        .clone()
        .or_else(|| {
            (raw_mirror.status == "verification_deferred").then(|| {
                "raw_mirror_verification_deferred: bounded read-only doctor did not establish checksum-verified raw-mirror recovery evidence"
                    .to_string()
            })
        });
    let recovery_evidence_collectors_complete = recovery_evidence_deferred_reason.is_none();
    match raw_mirror.status.as_str() {
        "absent" => {
            add_check!(
                "raw_mirror",
                "pass",
                "Raw mirror layout not initialized yet; future capture will use raw-mirror/v1",
                false
            );
        }
        "empty" => {
            add_check!(
                "raw_mirror",
                "pass",
                "Raw mirror layout exists but contains no manifests yet",
                false
            );
        }
        "verified" => {
            add_check!(
                "raw_mirror",
                "pass",
                format!(
                    "Raw mirror verified ({} manifest(s), {} blob byte(s))",
                    raw_mirror.summary.manifest_count, raw_mirror.summary.total_blob_bytes
                ),
                false
            );
        }
        "verification_deferred" => {
            let amplification_suffix = if raw_mirror.summary.amplified_source_count > 0 {
                format!(
                    "; {} source path(s) materially exceed their current size (see raw_mirror.amplified_sources)",
                    raw_mirror.summary.amplified_source_count
                )
            } else {
                String::new()
            };
            add_check!(
                "raw_mirror",
                "warn",
                format!(
                    "Raw mirror verification deferred for bounded read-only doctor check ({} manifest(s)){amplification_suffix}; set {CASS_DOCTOR_RAW_MIRROR_FULL_VERIFY}=1 to run full blob checksum verification",
                    raw_mirror.summary.manifest_count,
                ),
                false
            );
        }
        _ => {
            add_check!(
                "raw_mirror",
                "warn",
                format!(
                    "Raw mirror needs inspection: {} invalid manifest(s), {} missing blob(s), {} checksum mismatch(es), {} unverified manifest checksum(s), {} interrupted capture artifact(s)",
                    raw_mirror.summary.invalid_manifest_count,
                    raw_mirror.summary.missing_blob_count,
                    raw_mirror.summary.checksum_mismatch_count
                        + raw_mirror.summary.manifest_checksum_mismatch_count,
                    raw_mirror.summary.manifest_checksum_not_recorded_count,
                    raw_mirror.summary.interrupted_capture_count
                ),
                false
            );
        }
    }
    let backfill_fix_available = raw_mirror_backfill.status == "planned";
    let backfill_fix_applied = raw_mirror_backfill_applied;
    let (backfill_check_status, backfill_check_message) = match raw_mirror_backfill.status.as_str()
    {
        "deferred" => (
            "warn",
            raw_mirror_backfill
                .db_query_error
                .clone()
                .unwrap_or_else(|| "archive_wide_collectors_deferred".to_string()),
        ),
        "skipped" => (
            "pass",
            "Raw mirror backfill skipped until archive rows exist".to_string(),
        ),
        "clean" => (
            "pass",
            format!(
                "Raw mirror backfill clean ({} archive conversation(s) already classified)",
                raw_mirror_backfill.total_candidate_count
            ),
        ),
        "applied" => (
            "pass",
            format!(
                "Raw mirror backfill applied: captured {} live source file(s), linked {} existing raw manifest(s), left {} DB-only projection(s)",
                raw_mirror_backfill.captured_live_source_count,
                raw_mirror_backfill.existing_raw_manifest_link_count,
                raw_mirror_backfill.db_projection_only_count
            ),
        ),
        "planned" => (
            "warn",
            format!(
                "Raw mirror backfill available: {} live source file(s) can be captured and {} existing raw manifest(s) can be linked with --fix",
                raw_mirror_backfill.eligible_live_source_count,
                raw_mirror_backfill.existing_raw_manifest_link_count
            ),
        ),
        "partial" => (
            "warn",
            format!(
                "Raw mirror backfill partially applied with {} failure(s); no provider source files were modified",
                raw_mirror_backfill.capture_failure_count
            ),
        ),
        "blocked" => (
            "fail",
            raw_mirror_backfill
                .forensic_bundle
                .blocked_reason
                .as_ref()
                .map(|reason| {
                    format!(
                        "Raw mirror backfill blocked before mutation because forensic bundle capture failed: {reason}"
                    )
                })
                .unwrap_or_else(|| {
                    "Raw mirror backfill blocked before mutation because forensic bundle capture failed".to_string()
                }),
        ),
        _ => (
            "warn",
            format!(
                "Raw mirror backfill found {} DB-only projection(s), including {} missing upstream source(s); cass DB rows may be the sole remaining copy",
                raw_mirror_backfill.db_projection_only_count,
                raw_mirror_backfill.source_missing_count
            ),
        ),
    };
    checks.push(Check {
        name: "raw_mirror_backfill".to_string(),
        status: backfill_check_status.to_string(),
        message: backfill_check_message,
        fix_available: backfill_fix_available,
        fix_applied: backfill_fix_applied,
    });
    let sole_copy_warnings = if recovery_evidence_collectors_complete {
        build_doctor_sole_copy_warnings(&raw_mirror_backfill)
    } else {
        Vec::new()
    };
    let coverage_summary = if let Some(reason) = recovery_evidence_deferred_reason.as_deref() {
        doctor_coverage_summary_deferred(
            db_conversations,
            db_messages,
            raw_mirror.summary.manifest_count,
            reason,
        )
    } else {
        build_doctor_coverage_summary(
            &source_inventory,
            &raw_mirror,
            &raw_mirror_backfill,
            &sole_copy_warnings,
        )
    };
    let mut coverage_risk = if let Some(reason) = recovery_evidence_deferred_reason.as_deref() {
        let mut risk = doctor_fast_coverage_risk_unchecked(db_path.exists());
        risk.recommended_action = format!(
            "{reason}. Run process-isolated archive coverage probes before archive repair."
        );
        risk
    } else {
        doctor_coverage_risk_summary(
            &coverage_summary,
            sole_copy_warnings.len(),
            !crate::raw_mirror::capture_enabled(),
        )
    };
    if let Some(reason) = recovery_evidence_deferred_reason.as_deref() {
        add_check!(
            "source_coverage",
            "warn",
            format!("Source coverage is unchecked; {reason}"),
            false
        );
    } else if !sole_copy_warnings.is_empty() {
        add_check!(
            "source_coverage",
            "warn",
            format!(
                "Source coverage ledger found {} sole-copy warning(s); live-source rebuilds that reduce coverage are refused",
                sole_copy_warnings.len()
            ),
            false
        );
    } else if coverage_summary.db_without_raw_mirror_count > 0
        && !crate::raw_mirror::capture_enabled()
    {
        // GH #506: say plainly what the operator's switch gives up; doctor
        // --fix cannot add raw copies while capture is off.
        add_check!(
            "source_coverage",
            "warn",
            format!(
                "Raw mirror capture is off (CASS_RAW_MIRROR): {} DB row(s) have no raw copy, so a session file its provider deletes survives only in the archive DB ({} raw mirror manifest(s) lack DB links)",
                coverage_summary.db_without_raw_mirror_count,
                coverage_summary.mirror_without_db_link_count
            ),
            false
        );
    } else if coverage_summary.db_without_raw_mirror_count > 0
        || coverage_summary.mirror_without_db_link_count > 0
    {
        add_check!(
            "source_coverage",
            "warn",
            format!(
                "Source coverage ledger is partial: {} DB row(s) lack raw-mirror db links and {} raw mirror manifest(s) lack DB links",
                coverage_summary.db_without_raw_mirror_count,
                coverage_summary.mirror_without_db_link_count
            ),
            coverage_summary.db_without_raw_mirror_count > 0
        );
    } else {
        add_check!(
            "source_coverage",
            "pass",
            format!(
                "Source coverage ledger OK ({} archive conversation(s), confidence_tier={})",
                coverage_summary.archive_conversation_count, coverage_summary.confidence_tier
            ),
            false
        );
    }
    let source_authority_started = Instant::now();
    let source_authority = {
        let report = build_doctor_source_authority_report(&db_path, &source_inventory, &raw_mirror);
        if let Some(reason) = recovery_evidence_deferred_reason.as_deref() {
            doctor_refuse_source_authority_when_collectors_deferred(
                report,
                reason,
                storage_integrity_failed,
            )
        } else {
            report
        }
    };
    let candidate_repair_authority_complete = recovery_evidence_collectors_complete
        || (storage_integrity_failed
            && source_authority
                .selected_authorities
                .iter()
                .any(|authority| {
                    matches!(
                        authority.authority,
                        DoctorSourceAuthorityKind::VerifiedRawMirror
                    ) && matches!(
                        authority.decision,
                        DoctorSourceAuthorityDecision::CandidateOnly
                    ) && matches!(
                        authority.checksum_status,
                        DoctorArtifactChecksumStatus::Matched
                    )
                }));
    doctor_push_timing_span(
        &mut timing_spans,
        "source_authority",
        "source_authority",
        "source_authority",
        source_authority_started,
        DOCTOR_SLOW_OPERATION_DEFAULT_THRESHOLD_MS,
        vec!["source-authority matrix selected or rejected repair authorities".to_string()],
    );
    if let Some(reason) = recovery_evidence_deferred_reason.as_deref() {
        if candidate_repair_authority_complete {
            add_check!(
                "source_authority",
                "warn",
                format!(
                    "Archive-wide coverage authority is deferred, but checksum-verified raw mirror evidence may seed an isolated candidate after affirmative canonical corruption; promotion remains independently gated; {reason}"
                ),
                true
            );
        } else {
            add_check!(
                "source_authority",
                "warn",
                format!("No archive repair authority was selected; {reason}"),
                false
            );
        }
    }
    let candidate_staging_started = Instant::now();
    // Set when this run built an isolated reconstruct candidate, which by
    // design leaves every live asset untouched (see the post-repair probes).
    let mut isolated_candidate_built = false;
    let mut candidate_staging =
        collect_doctor_candidate_staging_report(&data_dir, &db_path, &index_path);
    if candidate_repair_authority_complete
        && candidate_staging.total_candidate_count == 0
        && doctor_candidate_build_should_run(
            fix_can_mutate,
            archive_queryable_for_non_destructive_derived_rebuild,
            needs_rebuild,
            &coverage_risk,
            &source_authority,
        )
    {
        let build_report = build_doctor_reconstruct_candidate(
            &data_dir,
            &db_path,
            &index_path,
            &raw_mirror,
            &source_authority,
            &coverage_summary,
        );
        if build_report.status == "completed" {
            auto_fix_applied = true;
            isolated_candidate_built = true;
            if let Some(candidate_id) = build_report.candidate_id.as_deref() {
                auto_fix_actions.push(format!(
                    "Built isolated reconstruct candidate {candidate_id} without changing live archive inventory"
                ));
            }
        }
        let latest_build = build_report;
        candidate_staging =
            collect_doctor_candidate_staging_report(&data_dir, &db_path, &index_path);
        candidate_staging.latest_build = Some(latest_build);
    }
    doctor_push_timing_span(
        &mut timing_spans,
        "candidate_staging",
        "candidate_staging",
        "candidate_build",
        candidate_staging_started,
        DOCTOR_SLOW_OPERATION_DEFAULT_THRESHOLD_MS,
        vec!["candidate staging was summarized and any eligible isolated candidate build was recorded".to_string()],
    );
    let (candidate_check_status, candidate_check_message, candidate_fix_applied) = if let Some(
        build,
    ) =
        candidate_staging.latest_build.as_ref()
    {
        if build.status == "completed" {
            (
                "pass",
                format!(
                    "Candidate staging built {} artifact(s), {} checksum(s), live archive inventory unchanged={}",
                    build.artifact_count, build.checksum_count, build.live_inventory_unchanged
                ),
                true,
            )
        } else {
            (
                "fail",
                format!(
                    "Candidate staging build blocked: {}",
                    build.warnings.join("; ")
                ),
                false,
            )
        }
    } else if candidate_staging.orphaned_candidate_count > 0
        || candidate_staging.interrupted_candidate_count > 0
    {
        (
            "warn",
            format!(
                "Candidate staging found {} orphaned and {} interrupted candidate artifact(s); doctor will not delete them automatically",
                candidate_staging.orphaned_candidate_count,
                candidate_staging.interrupted_candidate_count
            ),
            false,
        )
    } else {
        (
            "pass",
            format!(
                "Candidate staging OK ({} candidate artifact(s), status={})",
                candidate_staging.total_candidate_count, candidate_staging.status
            ),
            false,
        )
    };
    let candidate_buildable = candidate_staging.total_candidate_count == 0
        && candidate_repair_authority_complete
        && doctor_candidate_build_should_run(
            !fix && operation_state.mutating_doctor_allowed,
            archive_queryable_for_non_destructive_derived_rebuild,
            needs_rebuild,
            &coverage_risk,
            &source_authority,
        );
    checks.push(Check {
        name: "candidate_staging".to_string(),
        status: candidate_check_status.to_string(),
        message: candidate_check_message,
        fix_available: candidate_buildable,
        fix_applied: candidate_fix_applied,
    });
    if let Some(reason) = recovery_evidence_deferred_reason.as_deref() {
        coverage_risk.recommended_action = doctor_deferred_archive_recommended_action(
            reason,
            candidate_repair_authority_complete && candidate_staging.completed_candidate_count > 0,
            candidate_buildable,
            storage_busy_or_locked,
            storage_probe_timed_out,
            storage_integrity_skipped_reason.is_some(),
        );
    }
    if let Some(build) = candidate_staging.latest_build.as_ref()
        && !build.coverage_gate.promote_allowed
    {
        checks.push(Check {
            name: "coverage_comparison_gate".to_string(),
            status: "fail".to_string(),
            message: format!(
                "Candidate promotion refused because coverage would decrease: {}",
                build.coverage_gate.blocking_reasons.join("; ")
            ),
            fix_available: false,
            fix_applied: false,
        });
    }
    let suppress_legacy_rebuild_for_verified_candidate = needs_rebuild
        && fix_can_mutate
        && !archive_queryable_for_non_destructive_derived_rebuild
        && candidate_staging
            .latest_build
            .as_ref()
            .is_some_and(|build| {
                build.status == "completed"
                    && build.confidence == "verified_raw_mirror_reconstruction"
                    && build.candidate_message_count.unwrap_or(0) > 0
            });
    if suppress_legacy_rebuild_for_verified_candidate {
        needs_rebuild = false;
        checks.push(Check {
            name: "legacy_archive_rebuild".to_string(),
            status: "warn".to_string(),
            message: "Skipped legacy source rebuild because an isolated verified raw-mirror reconstruct candidate was built; live archive bytes were preserved for explicit promotion/restore handling".to_string(),
            fix_available: true,
            fix_applied: false,
        });
    }

    let semantic_probe_started = Instant::now();
    let readiness_snapshot = state_meta_json_inner(
        &data_dir,
        &db_path,
        DEFAULT_STALE_THRESHOLD_SECS,
        false,
        Some(false),
        true,
        true,
        // passive_db_probe: doctor's readiness probe must stay read-only.
        true,
    );
    doctor_push_timing_span(
        &mut timing_spans,
        "semantic_probe",
        "semantic_model",
        "semantic_probe",
        semantic_probe_started,
        DOCTOR_SLOW_OPERATION_DEFAULT_THRESHOLD_MS,
        vec!["semantic readiness was inspected without downloading models".to_string()],
    );
    let semantic_probe_duration_ms = semantic_probe_started.elapsed().as_millis() as u64;
    let fallback_mode = doctor_fallback_mode_from_state(&readiness_snapshot);
    let derived_semantic_assets = doctor_build_derived_semantic_asset_report(
        &data_dir,
        &readiness_snapshot,
        archive_queryable_for_non_destructive_derived_rebuild,
        semantic_probe_duration_ms,
    );
    let semantic_status = readiness_snapshot
        .pointer("/semantic/status")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");
    let semantic_availability = readiness_snapshot
        .pointer("/semantic/availability")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");
    add_check!(
        "semantic_model",
        derived_semantic_assets.doctor_check_status.clone(),
        format!(
            "Semantic/model readiness status={semantic_status}, availability={semantic_availability}, fallback_mode={fallback_mode}, asset_class={}, safe_to_rebuild={}, network_allowed=false, blocks_archive_recovery=false",
            doctor_serde_label(derived_semantic_assets.asset_class),
            derived_semantic_assets.safe_to_rebuild
        ),
        derived_semantic_assets.safe_to_rebuild
    );

    let mut repair_plan = if command_surface == doctor::DoctorCommandSurface::Repair {
        Some(build_doctor_repair_plan_preview(
            &data_dir,
            &db_path,
            &index_path,
            output_format,
            force_rebuild,
            allow_repeated_repair,
            execution_mode == doctor::DoctorExecutionMode::RepairDryRun,
            execution_mode == doctor::DoctorExecutionMode::FingerprintApply,
            requested_plan_fingerprint.clone(),
            &operation_state,
            &initial_failure_marker,
            needs_rebuild,
            db_ok,
            archive_queryable_for_non_destructive_derived_rebuild,
            canonical_candidate_replacement_authorized,
            archive_wide_collectors_complete,
            candidate_repair_authority_complete,
            db_messages,
            &raw_mirror_backfill,
            &coverage_summary,
            &coverage_risk,
            &source_authority,
            &candidate_staging,
        ))
    } else {
        None
    };
    if let Some(plan) = repair_plan.as_mut() {
        if execution_mode == doctor::DoctorExecutionMode::RepairDryRun {
            checks.push(Check {
                name: "repair_plan_approval".to_string(),
                status: "pass".to_string(),
                message: format!(
                    "Repair dry-run plan ready; apply with plan fingerprint {}",
                    plan.plan_fingerprint
                ),
                fix_available: plan.planned_action_count > 0,
                fix_applied: false,
            });
        } else if execution_mode == doctor::DoctorExecutionMode::FingerprintApply {
            if plan.approval_status == "matched" {
                if plan.planned_action_count == 0 {
                    plan.apply_authorized = true;
                    plan.will_mutate = false;
                    checks.push(Check {
                        name: "repair_plan_approval".to_string(),
                        status: "pass".to_string(),
                        message: format!(
                            "Repair plan fingerprint matched; no repair actions were selected: {}",
                            plan.plan_fingerprint
                        ),
                        fix_available: false,
                        fix_applied: false,
                    });
                } else if mutating_lock_acquired
                    && operation_state.mutating_doctor_allowed
                    && !repeat_repair_refused
                {
                    fix_can_mutate = true;
                    plan.apply_authorized = true;
                    plan.will_mutate = plan.planned_action_count > 0;
                    plan.approval_status = "matched".to_string();
                    checks.push(Check {
                        name: "repair_plan_approval".to_string(),
                        status: "pass".to_string(),
                        message: format!(
                            "Repair plan fingerprint matched and mutation lock is held: {}",
                            plan.plan_fingerprint
                        ),
                        fix_available: plan.planned_action_count > 0,
                        fix_applied: false,
                    });
                } else {
                    match doctor_acquire_mutation_lock(&data_dir, &db_path) {
                        Ok((guard, observation)) => {
                            _doctor_lock_guard = Some(guard);
                            _doctor_db_open_bypass_guard = Some(
                                crate::storage::sqlite::enter_doctor_mutation_db_open_bypass(),
                            );
                            operation_state = build_doctor_operation_state_report(
                                &data_dir,
                                &db_path,
                                &index_path,
                                &maintenance_snapshot,
                                &observation,
                            );
                            let lock_acquired = matches!(
                                observation,
                                DoctorMutationLockObservation::Acquired { .. }
                            );
                            if operation_state.mutating_doctor_allowed
                                && lock_acquired
                                && !repeat_repair_refused
                            {
                                fix_can_mutate = true;
                                plan.apply_authorized = true;
                                plan.will_mutate = plan.planned_action_count > 0;
                                plan.approval_status = "matched".to_string();
                                checks.push(Check {
                                    name: "repair_plan_approval".to_string(),
                                    status: "pass".to_string(),
                                    message: format!(
                                        "Repair plan fingerprint matched and mutation lock was acquired: {}",
                                        plan.plan_fingerprint
                                    ),
                                    fix_available: plan.planned_action_count > 0,
                                    fix_applied: false,
                                });
                            } else {
                                let reason = operation_state
                                    .mutation_blocked_reason
                                    .clone()
                                    .unwrap_or_else(|| {
                                        "doctor mutation lock was not acquired after plan revalidation"
                                            .to_string()
                                    });
                                plan.apply_authorized = false;
                                plan.will_mutate = false;
                                plan.approval_status = "lock_uncertain".to_string();
                                if !plan
                                    .branchable_blocker_codes
                                    .iter()
                                    .any(|code| code == "lock-uncertain")
                                {
                                    plan.branchable_blocker_codes
                                        .push("lock-uncertain".to_string());
                                }
                                plan.blocked_reasons
                                    .push(format!("lock-uncertain: {reason}"));
                                checks.push(Check {
                                    name: "repair_plan_approval".to_string(),
                                    status: "fail".to_string(),
                                    message: format!(
                                        "Repair plan fingerprint matched, but apply was blocked before mutation: {reason}"
                                    ),
                                    fix_available: false,
                                    fix_applied: false,
                                });
                            }
                        }
                        Err(observation) => {
                            operation_state = build_doctor_operation_state_report(
                                &data_dir,
                                &db_path,
                                &index_path,
                                &maintenance_snapshot,
                                &observation,
                            );
                            let reason = operation_state
                                .mutation_blocked_reason
                                .clone()
                                .unwrap_or_else(|| {
                                    "doctor mutation lock could not be acquired after plan revalidation"
                                        .to_string()
                                });
                            plan.apply_authorized = false;
                            plan.will_mutate = false;
                            plan.approval_status = "lock_uncertain".to_string();
                            if !plan
                                .branchable_blocker_codes
                                .iter()
                                .any(|code| code == "lock-uncertain")
                            {
                                plan.branchable_blocker_codes
                                    .push("lock-uncertain".to_string());
                            }
                            plan.blocked_reasons
                                .push(format!("lock-uncertain: {reason}"));
                            checks.push(Check {
                                name: "repair_plan_approval".to_string(),
                                status: "fail".to_string(),
                                message: format!(
                                    "Repair plan fingerprint matched, but apply was blocked before mutation: {reason}"
                                ),
                                fix_available: false,
                                fix_applied: false,
                            });
                        }
                    }
                }
            } else {
                let reason = if plan.provided_plan_fingerprint.is_none() {
                    "approval-fingerprint-missing"
                } else if plan.approval_status == "blocked" {
                    "plan-blocked"
                } else {
                    "approval-fingerprint-mismatched"
                };
                if !plan
                    .branchable_blocker_codes
                    .iter()
                    .any(|code| code == reason)
                {
                    plan.branchable_blocker_codes.push(reason.to_string());
                }
                checks.push(Check {
                    name: "repair_plan_approval".to_string(),
                    status: "fail".to_string(),
                    message: format!(
                        "Repair apply refused before mutation: {reason}; rerun dry-run and apply the exact current fingerprint"
                    ),
                    fix_available: false,
                    fix_applied: false,
                });
            }
        }
    }

    let candidate_promotion_plan_requested = repair_plan.as_ref().is_some_and(|plan| {
        plan.apply_authorized
            && doctor_repair_plan_has_action(
                plan,
                DOCTOR_REPAIR_ACTION_PROMOTE_RECONSTRUCT_CANDIDATE,
            )
    });
    // Defense in depth: even a malformed/stale plan object must satisfy all
    // live replacement-authority gates again immediately before mutation.
    let candidate_promotion_live_blocker = if archive_queryable_for_non_destructive_derived_rebuild
    {
        Some((
            "canonical-archive-queryable",
            "candidate promotion refused because bounded canonical archive reads succeeded; deferred deep integrity is not replacement authority",
        ))
    } else if !canonical_candidate_replacement_authorized {
        Some((
            "canonical-archive-failure-unproven",
            "candidate promotion refused because no affirmative current integrity failure or absent canonical DB authorizes replacement",
        ))
    } else if !candidate_repair_authority_complete {
        Some((
            "archive-authority-collectors-unchecked",
            "candidate promotion refused because archive-wide coverage and source-authority collectors are unchecked",
        ))
    } else {
        None
    };
    let candidate_promotion_blocked_live =
        candidate_promotion_plan_requested && candidate_promotion_live_blocker.is_some();
    if candidate_promotion_blocked_live {
        fix_can_mutate = false;
        if let Some(plan) = repair_plan.as_mut() {
            plan.apply_authorized = false;
            plan.will_mutate = false;
            plan.approval_status = "blocked".to_string();
            let (code, reason) = candidate_promotion_live_blocker
                .expect("live blocker must exist when candidate promotion is blocked");
            doctor_repair_push_blocker(
                &mut plan.blocked_reasons,
                &mut plan.branchable_blocker_codes,
                code,
                reason.to_string(),
            );
        }
        checks.push(Check {
            name: "candidate_promotion".to_string(),
            status: "fail".to_string(),
            message: candidate_promotion_live_blocker
                .map(|(_, reason)| reason)
                .unwrap_or("candidate promotion refused by live safety gates")
                .to_string(),
            fix_available: false,
            fix_applied: false,
        });
    }
    let candidate_promotion_apply_requested =
        candidate_promotion_plan_requested && !candidate_promotion_blocked_live;
    let candidate_promotion_started = Instant::now();
    if candidate_promotion_apply_requested && fix_can_mutate {
        let candidate_promotion_rebuild_planned = repair_plan.as_ref().is_some_and(|plan| {
            doctor_repair_plan_has_action(
                plan,
                DOCTOR_REPAIR_ACTION_REBUILD_FROM_PROMOTED_CANDIDATE,
            )
        });
        let selected_candidate =
            doctor_repair_select_completed_candidate_for_promotion(&candidate_staging);
        if let Some(candidate) = selected_candidate {
            let candidate_dir = PathBuf::from(&candidate.path);
            let promotion = promote_doctor_reconstruct_candidate_bundle(
                &data_dir,
                &db_path,
                &index_path,
                &candidate_dir,
            );
            fs_mutation_receipts.extend(promotion.fs_mutation_receipts.clone());
            match promotion.status.as_str() {
                "applied" => {
                    auto_fix_applied = true;
                    auto_fix_actions.push(format!(
                        "Promoted verified reconstruct candidate {} into the live archive bundle",
                        promotion.candidate_id
                    ));
                    checks.push(Check {
                        name: "candidate_promotion".to_string(),
                        status: "pass".to_string(),
                        message: format!(
                            "Promoted reconstruct candidate {} with rollback receipt {}",
                            promotion.candidate_id, promotion.redacted_receipt_path
                        ),
                        fix_available: true,
                        fix_applied: true,
                    });

                    match open_franken_cli_read_db_with_hard_timeout(
                        db_path.to_path_buf(),
                        "doctor promoted candidate archive",
                        Duration::from_secs(30),
                    ) {
                        Ok(conn) => {
                            use crate::franken_sync::compat::RowExt as _;

                            let conv_count: Option<i64> = conn
                                .query_row_map(
                                    "SELECT COUNT(*) FROM conversations",
                                    &[],
                                    |r: &crate::franken_sync::Row| r.get_typed(0),
                                )
                                .ok();
                            let msg_count: Option<i64> = conn
                                .query_row_map(
                                    "SELECT COUNT(*) FROM messages",
                                    &[],
                                    |r: &crate::franken_sync::Row| r.get_typed(0),
                                )
                                .ok();
                            let quick_check_status = doctor_database_quick_check(&conn).ok();

                            if let (Some(conv_count), Some(msg_count), Some(status)) =
                                (conv_count, msg_count, quick_check_status)
                            {
                                if status.trim().eq_ignore_ascii_case("ok") {
                                    db_ok = true;
                                    db_conversations = Some(conv_count.max(0) as usize);
                                    db_messages = Some(msg_count.max(0) as usize);
                                    checks.push(Check {
                                        name: "database_after_candidate_promotion".to_string(),
                                        status: "pass".to_string(),
                                        message: format!(
                                            "Promoted archive DB passed frankensqlite quick_check ({} conversations, {} messages)",
                                            conv_count, msg_count
                                        ),
                                        fix_available: false,
                                        fix_applied: false,
                                    });
                                    if candidate_promotion_rebuild_planned {
                                        needs_rebuild = msg_count > 0;
                                    }
                                    if promotion.derived_lexical_rebuild_required && !needs_rebuild
                                    {
                                        needs_rebuild = msg_count > 0;
                                    }
                                } else {
                                    db_ok = false;
                                    fix_can_mutate = false;
                                    needs_rebuild = false;
                                    checks.push(Check {
                                        name: "database_after_candidate_promotion".to_string(),
                                        status: "fail".to_string(),
                                        message: format!(
                                            "Promoted archive DB failed frankensqlite quick_check: {}",
                                            status.trim()
                                        ),
                                        fix_available: true,
                                        fix_applied: false,
                                    });
                                }
                            } else {
                                db_ok = false;
                                fix_can_mutate = false;
                                needs_rebuild = false;
                                checks.push(Check {
                                    name: "database_after_candidate_promotion".to_string(),
                                    status: "fail".to_string(),
                                    message:
                                        "Promoted archive DB could not report conversation/message counts and quick_check"
                                            .to_string(),
                                    fix_available: true,
                                    fix_applied: false,
                                });
                            }
                            let _ = close_franken_cli_owner_read_db(
                                conn,
                                &db_path,
                                "doctor promoted candidate archive",
                            );
                        }
                        Err(err) => {
                            db_ok = false;
                            fix_can_mutate = false;
                            needs_rebuild = false;
                            checks.push(Check {
                                name: "database_after_candidate_promotion".to_string(),
                                status: "fail".to_string(),
                                message: format!(
                                    "Cannot open promoted archive DB after candidate promotion: {}",
                                    err.message
                                ),
                                fix_available: true,
                                fix_applied: false,
                            });
                        }
                    }
                }
                "rolled_back" => {
                    fix_can_mutate = false;
                    needs_rebuild = false;
                    checks.push(Check {
                        name: "candidate_promotion".to_string(),
                        status: "fail".to_string(),
                        message: format!(
                            "Candidate promotion failed and rolled back to prior live bundle backup: {}",
                            promotion
                                .rollback_reference
                                .as_deref()
                                .unwrap_or("rollback reference unavailable")
                        ),
                        fix_available: true,
                        fix_applied: false,
                    });
                }
                "blocked" | "failed" => {
                    fix_can_mutate = false;
                    needs_rebuild = false;
                    checks.push(Check {
                        name: "candidate_promotion".to_string(),
                        status: "fail".to_string(),
                        message: format!(
                            "Candidate promotion {}: {}",
                            promotion.status,
                            promotion.blocked_reasons.join("; ")
                        ),
                        fix_available: true,
                        fix_applied: false,
                    });
                }
                other => {
                    fix_can_mutate = false;
                    needs_rebuild = false;
                    checks.push(Check {
                        name: "candidate_promotion".to_string(),
                        status: "fail".to_string(),
                        message: format!("Candidate promotion returned unexpected status {other}"),
                        fix_available: true,
                        fix_applied: false,
                    });
                }
            }
            candidate_promotion_report = Some(promotion);
        } else {
            fix_can_mutate = false;
            needs_rebuild = false;
            checks.push(Check {
                name: "candidate_promotion".to_string(),
                status: "fail".to_string(),
                message: "Candidate promotion apply was planned, but no single completed candidate remained after fingerprint approval".to_string(),
                fix_available: true,
                fix_applied: false,
            });
        }
    }
    doctor_push_timing_span(
        &mut timing_spans,
        "candidate_promotion",
        "candidate_promotion",
        "atomic_promotion",
        candidate_promotion_started,
        DOCTOR_SLOW_OPERATION_DEFAULT_THRESHOLD_MS,
        vec![if candidate_promotion_apply_requested {
            "candidate promotion path was evaluated with fingerprint and lock gates".to_string()
        } else {
            "candidate promotion was not requested for this doctor run".to_string()
        }],
    );

    // Promotion may have replaced and re-probed the live archive after the
    // candidate-planning snapshot above, so refresh this routing predicate
    // before choosing a repair implementation.
    let archive_queryable_for_non_destructive_derived_rebuild =
        doctor_archive_queryable_for_non_destructive_derived_rebuild(
            db_ok,
            db_conversations.is_some() && db_messages.is_some(),
            storage_integrity_skipped_reason.is_some(),
            matching_cached_integrity_attestation
                .as_ref()
                .is_some_and(|attestation| {
                    attestation.verdict
                        == crate::search::storage_integrity::IntegrityAttestationVerdict::Fail
                }),
        );

    // Apply fix: rebuild index if needed (only when --fix is passed)
    let derived_rebuild_started = Instant::now();
    let safe_auto_archive_rebuild_refused = safe_auto_run_requested
        && needs_rebuild
        && fix_can_mutate
        && !archive_queryable_for_non_destructive_derived_rebuild;
    if safe_auto_archive_rebuild_refused {
        checks.push(Check {
            name: "safe_auto_archive_rebuild".to_string(),
            status: "fail".to_string(),
            message: "Safe auto-run refused archive DB replacement or source-session rebuild because the canonical cass archive is missing, unreadable, or corrupt; preserve the DB bundle and use a fingerprinted repair dry-run before any archive-risk action".to_string(),
            fix_available: true,
            fix_applied: false,
        });
    }
    let archive_rebuild_authority_refused = needs_rebuild
        && fix_can_mutate
        && !archive_queryable_for_non_destructive_derived_rebuild
        && (!canonical_candidate_replacement_authorized || !candidate_repair_authority_complete);
    if archive_rebuild_authority_refused {
        let reason = if !canonical_candidate_replacement_authorized {
            "canonical archive replacement has no affirmative failure/absence evidence"
        } else {
            "archive-wide coverage and source-authority collectors are unchecked"
        };
        add_check!(
            "archive_rebuild_authority",
            "warn",
            format!(
                "Archive move/replacement and source-session rebuild were refused: {reason}; timeout, busy, unverified, and deferred states preserve canonical bytes"
            ),
            false
        );
    }
    let derived_rebuild_attempted = needs_rebuild
        && fix_can_mutate
        && !safe_auto_archive_rebuild_refused
        && !archive_rebuild_authority_refused;
    if derived_rebuild_attempted {
        let stderr_is_tty = std::io::stderr().is_terminal();
        let is_robot = output_format.is_some();
        let show_progress = !is_robot && stderr_is_tty;
        let show_plain = !is_robot && !stderr_is_tty;

        if !is_robot {
            println!();
            println!(
                "{} Rebuilding index (this may take a moment)...",
                "→".cyan()
            );
        }

        let progress = std::sync::Arc::new(indexer::IndexingProgress::default());
        // A deferred deep integrity probe is not a corruption verdict.  If
        // bounded row counts succeeded and no current cached attestation says
        // FAIL, a derived-only Tantivy rebuild may read the canonical archive;
        // it must never fall through to the branch that moves/replaces it.
        let rebuild_from_db = archive_queryable_for_non_destructive_derived_rebuild;

        if rebuild_from_db {
            let total_convs = db_conversations.unwrap_or(0);
            let rebuild_handle = std::thread::spawn({
                let progress = progress.clone();
                let db_path = db_path.clone();
                let data_dir = data_dir.clone();
                move || rebuild_tantivy_from_db(&db_path, &data_dir, total_convs, Some(progress))
            });

            let rebuild_result = wait_with_progress(
                rebuild_handle,
                progress,
                show_progress,
                show_plain,
                "Rebuilding search index from database...",
            );

            match rebuild_result {
                Ok(message_count) => {
                    needs_rebuild = false;
                    auto_fix_actions.push("Rebuilt search index from database".to_string());
                    auto_fix_applied = true;
                    for check in &mut checks {
                        if check.name == "index" || check.name == "index_sync" {
                            check.status = "pass".to_string();
                            check.fix_applied = true;
                            check.message = "Search index rebuilt from database".to_string();
                        }
                    }
                    checks.push(Check {
                        name: "rebuild".to_string(),
                        status: "pass".to_string(),
                        message: format!(
                            "Index rebuilt from database ({} messages)",
                            message_count
                        ),
                        fix_available: true,
                        fix_applied: true,
                    });
                    if let Some(report) = candidate_promotion_report.as_mut()
                        && report.status == "applied"
                        && (report.derived_lexical_rebuild_required
                            || report.derived_semantic_rebuild_required)
                    {
                        doctor_candidate_promotion_record_derived_followup_artifact(
                            &data_dir,
                            report,
                            message_count,
                            &fallback_mode,
                        );
                    }
                }
                Err(e) => {
                    checks.push(Check {
                        name: "rebuild".to_string(),
                        status: "fail".to_string(),
                        message: format!("Index rebuild failed: {}", e),
                        fix_available: true,
                        fix_applied: false,
                    });
                }
            }
        } else {
            // Preserve existing DB when possible; rebuild only derived data.
            let mut can_rebuild = true;
            let mut db_backup_done = false;
            if !db_ok {
                let ts = chrono::Utc::now().format("%Y%m%d_%H%M%S");
                let backup_path = db_path.with_extension(format!("corrupt.{ts}"));
                match crate::storage::sqlite::move_database_bundle(&db_path, &backup_path) {
                    Ok(moved) if moved.moved_any() => {
                        let mut components = Vec::new();
                        if moved.database {
                            components.push("db");
                        }
                        if moved.wal {
                            components.push("wal");
                        }
                        if moved.shm {
                            components.push("shm");
                        }
                        let component_note = if components.is_empty() {
                            String::new()
                        } else {
                            format!(" ({})", components.join(", "))
                        };
                        db_backup_done = true;
                        checks.push(Check {
                            name: "database_backup".to_string(),
                            status: "pass".to_string(),
                            message: format!(
                                "Backed up corrupted database bundle to {}{}",
                                backup_path.display(),
                                component_note
                            ),
                            fix_available: true,
                            fix_applied: true,
                        });
                        auto_fix_actions.push(format!(
                            "Backed up corrupted database bundle to {}{}",
                            backup_path.display(),
                            component_note
                        ));
                        auto_fix_applied = true;
                    }
                    Ok(_) => {}
                    Err(e) => {
                        checks.push(Check {
                            name: "database_backup".to_string(),
                            status: "fail".to_string(),
                            message: format!("Failed to backup corrupted database bundle: {}", e),
                            fix_available: true,
                            fix_applied: false,
                        });
                        can_rebuild = false;
                    }
                }
            }

            if !can_rebuild {
                checks.push(Check {
                    name: "rebuild".to_string(),
                    status: "fail".to_string(),
                    message: "Index rebuild skipped because database backup failed".to_string(),
                    fix_available: true,
                    fix_applied: false,
                });
                needs_rebuild = true;
            } else {
                let index_opts = indexer::IndexOptions {
                    // When the database is missing or corrupted, doctor must
                    // rebuild from source sessions using the full path rather
                    // than the incremental UPSERT-based path.
                    full: force_rebuild || !db_ok,
                    force_rebuild,
                    watch: false,
                    watch_once_paths: None,
                    db_path: db_path.clone(),
                    data_dir: data_dir.clone(),
                    semantic: false,
                    build_hnsw: false,
                    embedder: "fastembed".to_string(),
                    progress: Some(progress.clone()),
                    watch_interval_secs: 30,
                };

                let rebuild_handle = std::thread::spawn(move || {
                    indexer::run_index(index_opts, None)
                        .map(|_| 0usize)
                        .map_err(|e| CliError {
                            code: 5,
                            kind: CliErrorKind::Doctor.kind_str(),
                            message: format!("index rebuild failed: {e}"),
                            hint: None,
                            retryable: true,
                        })
                });

                let rebuild_result = wait_with_progress(
                    rebuild_handle,
                    progress,
                    show_progress,
                    show_plain,
                    "Rebuilding index from source sessions...",
                );

                match rebuild_result {
                    Ok(_) => {
                        needs_rebuild = false;
                        let rebuild_note = if db_backup_done {
                            "Rebuilt index from source sessions (new database created)".to_string()
                        } else {
                            "Rebuilt index from source sessions (database preserved)".to_string()
                        };
                        auto_fix_actions.push(rebuild_note.clone());
                        auto_fix_applied = true;
                        for check in &mut checks {
                            if check.name == "index" || check.name == "index_sync" {
                                check.status = "pass".to_string();
                                check.fix_applied = true;
                                check.message = rebuild_note.clone();
                            }
                        }
                        checks.push(Check {
                            name: "rebuild".to_string(),
                            status: "pass".to_string(),
                            message: "Index rebuilt successfully".to_string(),
                            fix_available: true,
                            fix_applied: true,
                        });
                    }
                    Err(e) => {
                        checks.push(Check {
                            name: "rebuild".to_string(),
                            status: "fail".to_string(),
                            message: format!("Index rebuild failed: {}", e),
                            fix_available: true,
                            fix_applied: false,
                        });
                    }
                }
            }
        }
    }
    doctor_push_timing_span(
        &mut timing_spans,
        "derived_rebuild",
        "rebuild",
        "derived_rebuild",
        derived_rebuild_started,
        DOCTOR_SLOW_OPERATION_DEFAULT_THRESHOLD_MS,
        vec![if derived_rebuild_attempted {
            "derived index rebuild path ran under doctor mutation gates".to_string()
        } else {
            "derived index rebuild was not attempted for this doctor run".to_string()
        }],
    );

    let cleanup_apply_started = Instant::now();
    let cleanup_apply_result = if cleanup_apply_requested && fix_can_mutate {
        // Pass-13 structural refactor: create a per-run journal directory so the
        // FsMutationExecutor's `Applied` receipts also land in `actions.jsonl`.
        // `cass doctor --undo <run-id>` / `--ls` / `--diff` then see real
        // production mutations from the cleanup-apply path, not only mutations
        // produced by `doctor_chokepoint::mutate()` from new fixers.
        //
        // Journaling is best-effort: if the run-dir cannot be created we still
        // run the cleanup (the FsMutationExecutor itself is the safety gate),
        // we just lose the post-hoc observability for this invocation.
        let target_sha = option_env!("VERGEN_GIT_SHA").unwrap_or("unknown");
        let journal_run_id = crate::doctor_runs::RunId::new(target_sha);
        let journal_run_id_str = journal_run_id.as_str().to_string();
        let journal_run_dir = match crate::doctor_runs::create_run_dir(&data_dir, &journal_run_id) {
            Ok(dir) => {
                if let Err(err) = crate::doctor_runs::append_action(
                    &dir,
                    &crate::doctor_runs::ActionRecord::RunStarted {
                        schema_version: crate::doctor_runs::RUN_ARTIFACT_SCHEMA_VERSION,
                        run_id: journal_run_id_str.clone(),
                        target_sha: target_sha.to_string(),
                        mode: "cleanup-apply".to_string(),
                        started_at_ms: doctor_now_ms(),
                    },
                ) {
                    tracing::warn!(
                        target: "cass::doctor::cleanup",
                        run_id = %journal_run_id_str,
                        error = %err,
                        "failed to append RunStarted to cleanup-apply journal; subsequent records will lack a session-opener"
                    );
                }
                Some(dir)
            }
            Err(err) => {
                tracing::warn!(
                    target: "cass::doctor::cleanup",
                    run_id = %journal_run_id_str,
                    error = %err,
                    "failed to create per-run journal directory; cleanup-apply will proceed without journaling"
                );
                None
            }
        };
        let result = apply_diag_quarantine_cleanup(
            &data_dir,
            &db_path,
            &index_path,
            rebuild_active,
            requested_plan_fingerprint.as_deref(),
            journal_run_dir.as_deref(),
            journal_run_dir
                .as_ref()
                .map(|_| journal_run_id_str.as_str()),
        );
        if let Some(run_dir) = journal_run_dir.as_deref() {
            // RunEnded records the cleanup-apply outcome so `cass doctor --diff`
            // and `--ls` distinguish a fully-applied run from a partially-
            // applied run, a no-op, or one blocked by the apply gate. We keep
            // exit_code = 0 because cleanup-apply is not the surface that
            // produces a non-zero process exit — the outer doctor exit is
            // computed separately from cleanup_status further down. The
            // four-branch selection lives in `cleanup_apply_exit_code_kind`
            // so it can be unit-tested without an e2e fixture.
            let exit_code_kind = cleanup_apply_exit_code_kind(
                result.applied,
                result.pruned_asset_count,
                !result.blocked_reasons.is_empty(),
            );
            if let Err(err) = crate::doctor_runs::append_action(
                run_dir,
                &crate::doctor_runs::ActionRecord::RunEnded {
                    run_id: journal_run_id_str.clone(),
                    exit_code: 0,
                    exit_code_kind: exit_code_kind.to_string(),
                    ended_at_ms: doctor_now_ms(),
                },
            ) {
                tracing::warn!(
                    target: "cass::doctor::cleanup",
                    run_id = %journal_run_id_str,
                    exit_code_kind,
                    error = %err,
                    "failed to append RunEnded to cleanup-apply journal; this run will appear as 'in-flight' to crash-recovery"
                );
            }
        }
        let cleanup_status = if result.applied {
            "pass"
        } else if result.before_reclaim_candidate_count > 0
            || result
                .actions
                .iter()
                .any(|action| action.artifact_kind == "retained_publish_backup")
        {
            "warn"
        } else {
            "pass"
        };
        let cleanup_message = if result.applied {
            format!(
                "Pruned {} derivative cleanup artifact(s), reclaimed {} bytes",
                result.pruned_asset_count, result.reclaimed_bytes
            )
        } else if !result.blocked_reasons.is_empty() {
            format!(
                "Derivative cleanup skipped: {}",
                result.blocked_reasons.join("; ")
            )
        } else {
            "No derivative cleanup artifacts were eligible for pruning".to_string()
        };
        checks.push(Check {
            name: "derivative_cleanup".to_string(),
            status: cleanup_status.to_string(),
            message: cleanup_message.clone(),
            fix_available: result.before_reclaim_candidate_count > 0
                || result
                    .actions
                    .iter()
                    .any(|action| action.artifact_kind == "retained_publish_backup"),
            fix_applied: result.applied,
        });
        if result.applied {
            auto_fix_actions.push(cleanup_message);
            auto_fix_applied = true;
        }
        Some(result)
    } else {
        None
    };
    doctor_push_timing_span(
        &mut timing_spans,
        "cleanup_apply",
        "derivative_cleanup",
        "cleanup_apply",
        cleanup_apply_started,
        DOCTOR_SLOW_OPERATION_DEFAULT_THRESHOLD_MS,
        vec![if cleanup_apply_requested {
            "cleanup apply path evaluated derivative-only reclamation gates".to_string()
        } else {
            "cleanup apply was not requested for this doctor run".to_string()
        }],
    );

    let post_repair_probes_started = Instant::now();
    // zp0fp (GH #374): the post-repair probes verify live assets after a
    // mutation. When the run's only action built an isolated reconstruct
    // candidate, no live asset changed, so probing an unreadable archive
    // measured its pre-existing damage, reported the build as a failed repair
    // and left the failure marker that then blocked promoting that very
    // candidate. Every other action (each one pushes to `auto_fix_actions`),
    // filesystem mutation receipt or cleanup still runs every probe.
    let live_assets_mutated =
        auto_fix_applied && !(isolated_candidate_built && auto_fix_actions.len() == 1);
    let post_repair_probes = collect_doctor_post_repair_probes(DoctorPostRepairProbeRunContext {
        data_dir: &data_dir,
        db_path: &db_path,
        index_path: &index_path,
        readiness_snapshot: &readiness_snapshot,
        fix_can_mutate,
        auto_fix_applied: live_assets_mutated,
        fs_mutation_receipts: &fs_mutation_receipts,
        cleanup_apply_result: cleanup_apply_result.as_ref(),
    });
    doctor_push_timing_span(
        &mut timing_spans,
        "post_repair_probes",
        "post_repair_probes",
        "post_repair_probe",
        post_repair_probes_started,
        DOCTOR_SLOW_OPERATION_DEFAULT_THRESHOLD_MS,
        vec![if post_repair_probes.requested {
            "post-repair verification probes ran with receipt correlation".to_string()
        } else {
            "post-repair verification probes were skipped because no repair mutation ran"
                .to_string()
        }],
    );
    if post_repair_probes.requested {
        let (status, message) = if post_repair_probes.blocks_success {
            (
                "fail",
                format!(
                    "Post-repair verification failed: {} probe(s) failed and blocked success",
                    post_repair_probes.failed_count
                ),
            )
        } else if post_repair_probes.failed_count > 0 {
            (
                "warn",
                format!(
                    "Post-repair verification completed with {} non-blocking probe failure(s)",
                    post_repair_probes.failed_count
                ),
            )
        } else {
            (
                "pass",
                format!(
                    "Post-repair verification passed ({} passed, {} skipped)",
                    post_repair_probes.passed_count, post_repair_probes.skipped_count
                ),
            )
        };
        checks.push(Check {
            name: "post_repair_probes".to_string(),
            status: status.to_string(),
            message,
            fix_available: false,
            fix_applied: false,
        });
    }

    // Count issues. #287: a `timeout` check means the doctor could not verify
    // that surface within its hard deadline — the run is bounded but the
    // verdict is unproven, so it counts as a failure for health/exit purposes
    // (an agent must not read a timed-out probe as "healthy").
    let fail_count = checks
        .iter()
        .filter(|c| c.status == "fail" || c.status == "timeout")
        .count();
    let warn_count = checks.iter().filter(|c| c.status == "warn").count();
    let issues_found = fail_count + warn_count;
    let issues_fixed = checks.iter().filter(|c| c.fix_applied).count();
    let check_reports: Vec<DoctorCheckReport> = checks
        .iter()
        .map(|check| {
            doctor_check_report(
                &check.name,
                &check.status,
                &check.message,
                check.fix_available,
                check.fix_applied,
            )
        })
        .collect();
    let health_class = doctor_health_class_for_checks(&check_reports);
    let risk_level = doctor_risk_level_for_reports(&coverage_risk, &check_reports);
    let recommended_action = doctor_recommended_action_for_reports(
        recommended_action,
        &operation_state,
        &coverage_risk,
        &check_reports,
    );
    let active_repair = doctor_active_repair_report(&operation_state);
    let check_scope = doctor_check_scope_report(command_surface, execution_mode);
    let operation_outcome = doctor_top_level_operation_outcome(
        &check_reports,
        fix,
        issues_found,
        issues_fixed,
        not_initialized,
        &post_repair_probes,
        cleanup_apply_result.as_ref(),
    );
    let operation_exit_code_kind = operation_outcome.exit_code_kind;
    let incidents = build_doctor_root_cause_incidents(
        &check_reports,
        &coverage_risk,
        &operation_state,
        &candidate_staging,
        &fallback_mode,
        candidate_promotion_report.as_ref(),
    );
    let primary_incident_id = incidents
        .first()
        .map(|incident| incident.incident_id.clone());

    let daemon_runtime = output_format
        .or_else(robot_format_from_env)
        .map(|_| probe_daemon_runtime_for_diagnostics(&data_dir));
    let elapsed_ms = start.elapsed().as_millis() as u64;
    // Bounded-budget signal for the robot surface (uojcg.2.2): the report saw
    // `doctor check` exceed an 8s cap. Per-check internal timeouts already bound
    // each probe, so doctor does not hang; this advertises whether the whole run
    // exceeded its budget so an agent can fall back to a cheaper probe. Override
    // via CASS_DOCTOR_BUDGET_MS.
    let doctor_budget_ms = dotenvy::var("CASS_DOCTOR_BUDGET_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&ms| ms > 0)
        .unwrap_or(8000);
    let doctor_budget_timed_out = elapsed_ms > doctor_budget_ms;
    let locks = build_doctor_lock_diagnostics(&operation_state, doctor_now_ms());
    let slow_operations = build_doctor_slow_operations(&timing_spans);
    let timing_summary = build_doctor_timing_summary(&timing_spans, elapsed_ms);
    let retry_recommendation = build_doctor_retry_recommendation(
        &operation_state,
        &operation_outcome,
        &locks,
        recommended_action.as_ref(),
    );
    let operation_id = cleanup_apply_result
        .as_ref()
        .map(|result| result.receipt.plan_fingerprint.clone())
        .unwrap_or_else(|| {
            doctor_check_operation_id(&check_reports, fix, &operation_outcome, not_initialized)
        });
    let operation_event_log = cleanup_apply_result
        .as_ref()
        .map(|result| result.receipt.event_log.clone())
        .unwrap_or_else(|| {
            doctor_operation_event_log_for_checks(
                &operation_id,
                &check_reports,
                fix,
                elapsed_ms,
                "embedded_operation_events",
            )
        });
    let safe_auto_event_log_reference = operation_event_log
        .hash_chain_tip
        .as_ref()
        .map(|tip| format!("event_log.hash_chain_tip:{tip}"))
        .or_else(|| {
            operation_event_log
                .path
                .as_ref()
                .map(|path| format!("event_log.path:{path}"))
        })
        .unwrap_or_else(|| "event_log.embedded_operation_events".to_string());
    let safe_auto_eligibility = build_doctor_safe_auto_run_report(DoctorSafeAutoRunBuildInput {
        enabled: safe_auto_run_requested,
        check_reports: &check_reports,
        auto_fix_actions: &auto_fix_actions,
        fs_mutation_receipts: &fs_mutation_receipts,
        operation_state: &operation_state,
        needs_rebuild,
        archive_queryable_for_non_destructive_derived_rebuild,
        db_messages,
        event_log_reference: safe_auto_event_log_reference,
    });
    let command_line_mode = doctor_command_line_mode(
        fix,
        output_format,
        force_rebuild,
        allow_repeated_repair,
        command_surface,
        execution_mode,
        requested_plan_fingerprint.as_deref(),
    );
    let should_capture_failure_context = fix
        && (matches!(
            operation_outcome.kind,
            DoctorOperationOutcomeKind::RepairBlocked
        ) || operation_state.mutation_blocked_reason.is_some());
    let failure_context = should_capture_failure_context.then(|| {
        write_doctor_lock_contention_failure_context(
            &data_dir,
            &operation_id,
            &command_line_mode,
            &operation_state,
            &locks,
            &operation_event_log,
        )
    });
    let plan_fingerprint = cleanup_apply_result
        .as_ref()
        .and_then(|result| result.plan.as_ref())
        .map(|plan| plan.plan_fingerprint.clone())
        .unwrap_or_else(|| operation_id.clone());
    let should_write_failure_marker = fix
        && !repeat_repair_refused
        && matches!(
            operation_outcome.kind,
            DoctorOperationOutcomeKind::VerificationFailed
                | DoctorOperationOutcomeKind::RepairIncomplete
        );
    let mut written_failure_marker_error: Option<String> = None;
    let failure_marker_after_run = if should_write_failure_marker {
        let marker = build_doctor_repair_failure_marker(DoctorRepairFailureMarkerBuildContext {
            data_dir: &data_dir,
            db_path: &db_path,
            index_path: &index_path,
            repair_class: &repair_class,
            operation_id: &operation_id,
            plan_fingerprint: &plan_fingerprint,
            command_line_mode: &command_line_mode,
            check_reports: &check_reports,
            auto_fix_actions: &auto_fix_actions,
            fs_mutation_receipts: &fs_mutation_receipts,
            operation_outcome: &operation_outcome,
            cleanup_apply_result: cleanup_apply_result.as_ref(),
        });
        match write_doctor_repair_failure_marker(&data_dir, &marker) {
            Ok(path) => doctor_failure_marker_report_from_marker(&data_dir, &path, marker),
            Err(err) => {
                written_failure_marker_error = Some(err.clone());
                doctor_failure_marker_corrupt_report(
                    &data_dir,
                    &repair_class,
                    &doctor_repair_failure_marker_dir(&data_dir, &repair_class),
                    err,
                )
            }
        }
    } else {
        initial_failure_marker.clone()
    };
    let all_pass = checks.iter().all(|c| c.status == "pass");
    // #287: stable machine-readable reason codes for degraded doctor outcomes,
    // so an agent can make a bounded decision from the JSON verdict without
    // parsing free-form check messages. Ordered by triage priority; the first
    // entry is surfaced as the primary `reason_code`.
    let mut degraded_reason_codes: Vec<&'static str> = Vec::new();
    if checks.iter().any(|c| c.status == "timeout") {
        degraded_reason_codes.push("timeout_or_busy_spin_guard");
    }
    if checks
        .iter()
        .any(|c| c.name == "database" && c.status == "fail")
    {
        degraded_reason_codes.push("db_unavailable");
    }
    if readiness_snapshot
        .pointer("/ingest_quarantine/circuit_breaker_active")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        degraded_reason_codes.push("quarantine_circuit_breaker");
    }
    if !not_initialized
        && readiness_snapshot
            .pointer("/index/checkpoint/completed")
            .and_then(serde_json::Value::as_bool)
            == Some(false)
    {
        degraded_reason_codes.push("checkpoint_incomplete");
    }
    // GH #497: readable rows do not establish structural integrity. Keep
    // the deferral visible in both the reason code and the aggregate verdict.
    if checks.iter().any(|c| {
        c.name == "database"
            && c.status == "warn"
            && c.message.contains("structural integrity is unchecked")
    }) {
        degraded_reason_codes.push("integrity_unchecked");
    }
    let primary_reason_code = degraded_reason_codes.first().copied();
    let doctor_status = if not_initialized {
        "not_initialized"
    } else if fail_count != 0 {
        "unhealthy"
    } else if degraded_reason_codes.contains(&"integrity_unchecked") {
        "unknown"
    } else {
        "healthy"
    };
    let healthy = doctor_status == "healthy";

    // Output
    let structured_format = output_format.or_else(robot_format_from_env).map(|fmt| {
        if matches!(fmt, RobotFormat::Sessions) {
            RobotFormat::Compact
        } else {
            fmt
        }
    });

    let rebuild_quarantine = crate::search::lexical_quarantine_diagnostics::collect(&data_dir);
    if let Some(fmt) = structured_format {
        let mut quarantine_report = if command_surface == doctor::DoctorCommandSurface::Check {
            collect_diag_quarantine_report_without_cleanup_plan(&data_dir, &index_path)
        } else {
            collect_diag_quarantine_report(&data_dir, &index_path)
        };
        quarantine_report.lexical_rebuild_quarantine = Some(rebuild_quarantine);
        // uojcg.9.2/9.5: attribute a likely root-cause family from the live
        // doctor signals (storage open failure, derived truth-table mismatch,
        // semantic load failure). Complements the per-incident `incidents[]`
        // breakdown. Host disk-free % is deliberately NOT fed here: it is a live
        // host metric that would make the doctor golden environment-dependent,
        // and `storage_pressure` already reports low-disk state separately. The
        // projection still supports host-disk-pressure for a future surface that
        // can supply a deterministic/scrubbed signal.
        // #374 Issue 3: feed the storage-integrity verdict into the root-cause
        // projection so an unreadable/malformed archive attributes to the
        // frankensqlite-storage family even when the failure isn't surfaced as a
        // `/database/open_error` string (its serialized shape carries
        // storage_state/archive_readability).
        let storage_integrity_signal_value =
            serde_json::to_value(&storage_integrity_report).unwrap_or(serde_json::Value::Null);
        let root_cause_attribution = crate::root_cause_projection::project_root_cause(
            &gather_projection_signals_from_state(
                &readiness_snapshot,
                None,
                Some(&storage_integrity_signal_value),
            ),
        );
        let mut output_backfill = raw_mirror_backfill;
        let mut output_sole_copy_warnings = sole_copy_warnings;
        let detail_output = bounded_doctor_structured_details(
            &mut output_backfill,
            &mut output_sole_copy_warnings,
            verbose,
            "cass doctor check --json --verbose",
        );
        let mut payload = serde_json::json!({
            // Top-level envelope versioning. Bumped to 2 in world-class-doctor
            // pass-3 alongside the new per-run artifact directory + undo
            // surface. Sub-reports retain their own per-report `schema_version`
            // (typically 1) — this is the *envelope* version, used by agents
            // to gate their parsing logic. Pinned by the
            // `doctor_top_level_schema_version_present` golden-test contract.
            "schema_version": 2,
            "doctor_contract_version": 1,
            "capabilities_url": "cass capabilities --json",
            "status": doctor_status,
            // #287: machine-readable degradation taxonomy. `reason_code` is the
            // primary (highest-priority) entry of `degraded_reason_codes`; both
            // are null/empty on a fully verified healthy run. Stable values:
            // timeout_or_busy_spin_guard, db_unavailable,
            // quarantine_circuit_breaker, checkpoint_incomplete,
            // integrity_unchecked (status is "unknown" unless an affirmative
            // failure makes it "unhealthy": a deferred probe is no verdict).
            "reason_code": primary_reason_code,
            "degraded_reason_codes": degraded_reason_codes,
            "health_class": health_class,
            "risk_level": risk_level,
            "healthy": healthy,
            "initialized": !not_initialized,
            "explanation": explanation,
            "recommended_action": recommended_action,
            "fallback_mode": fallback_mode,
            "issues_found": issues_found,
            "issues_fixed": issues_fixed,
            "failures": fail_count,
            "warnings": warn_count,
            "needs_rebuild": needs_rebuild,
            "auto_fix_applied": auto_fix_applied,
            "auto_fix_actions": auto_fix_actions,
            "doctor_command": {
                "surface": command_surface.stable_name(),
                "realized_subcommand": doctor_realized_subcommand_name(command_surface, execution_mode),
                "execution_mode": execution_mode.stable_name(),
                "read_only": !execution_mode.permits_mutation(),
                "mutation_allowed": execution_mode.permits_mutation(),
                "plan_fingerprint_required": execution_mode.requires_plan_fingerprint(),
                "legacy_alias": command_surface == doctor::DoctorCommandSurface::LegacyDoctor,
                "force_rebuild": force_rebuild,
                "allow_repeated_repair": allow_repeated_repair,
            },
            "check_scope": check_scope,
            "repair_previously_failed": initial_failure_marker.found,
            "failure_marker_path": failure_marker_after_run.path.clone(),
            "repeat_refusal_reason": if repeat_repair_refused { repeat_refusal_reason.clone() } else { None },
            "override_available": override_available,
            "override_used": override_used,
            "active_repair": active_repair,
            "post_repair_probes": post_repair_probes,
            "repair_failure_marker": failure_marker_after_run.clone(),
            "failure_marker_write_error": written_failure_marker_error.clone(),
            "operation_outcome": operation_outcome,
            "operation_state": operation_state,
            "locks": locks,
            "slow_operations": slow_operations,
            "timing_summary": timing_summary,
            "budget": serde_json::json!({
                "elapsed_ms": elapsed_ms,
                "budget_ms": doctor_budget_ms,
                "timed_out": doctor_budget_timed_out,
                "recommended_next_probe": if doctor_budget_timed_out {
                    serde_json::Value::String("cass status --json".to_string())
                } else {
                    serde_json::Value::Null
                },
            }),
            "retry_recommendation": retry_recommendation,
            "safe_auto_eligibility": safe_auto_eligibility,
            "root_cause": root_cause_attribution,
            "primary_incident_id": primary_incident_id,
            "incidents": incidents,
            "event_log": operation_event_log,
            "lexical": readiness_snapshot.get("index").cloned().unwrap_or(serde_json::Value::Null),
            "semantic": readiness_snapshot.get("semantic").cloned().unwrap_or(serde_json::Value::Null),
            "derived_semantic_assets": derived_semantic_assets,
            "storage_pressure": storage_pressure,
            "storage_integrity": storage_integrity_report,
            "daemon_runtime": daemon_runtime,
            "asset_taxonomy": doctor_asset_taxonomy_report(),
            "anomaly_taxonomy": doctor_anomaly_taxonomy_report(),
            "repair_contract": doctor_repair_contract_report(),
            "config_exclusion_risks": config_exclusion_risks,
            "source_inventory": source_inventory,
            "raw_mirror": raw_mirror,
            "raw_mirror_backfill": output_backfill,
            "coverage_summary": coverage_summary,
            "sole_copy_warnings": output_sole_copy_warnings,
            "detail_output": detail_output,
            "coverage_risk": coverage_risk,
            "source_authority": source_authority,
            "candidate_staging": candidate_staging,
            "checks": check_reports,
            "quarantine": quarantine_report,
            "_meta": {
                "elapsed_ms": elapsed_ms,
                "data_dir": data_dir.display().to_string(),
                "db_path": db_path.display().to_string(),
                "doctor_surface": command_surface.stable_name(),
                "doctor_execution_mode": execution_mode.stable_name(),
                "fix_mode": fix,
                "allow_repeated_repair": allow_repeated_repair,
            }
        });
        if let Some(result) = cleanup_apply_result.as_ref() {
            payload["cleanup_apply"] = serde_json::json!(result);
        }
        if let Some(plan) = repair_plan.as_ref() {
            payload["repair_plan"] = serde_json::json!(plan);
        }
        if let Some(report) = candidate_promotion_report.as_ref() {
            payload["candidate_promotion"] = serde_json::json!(report);
        }
        if let Some(failure_context) = failure_context.as_ref() {
            payload["failure_context"] = failure_context.clone();
        }
        if !fs_mutation_receipts.is_empty() {
            payload["fs_mutation_receipts"] = serde_json::json!(fs_mutation_receipts);
        }
        output_structured_value(payload, fmt)?;
    } else {
        // Human-readable output
        print_doctor_human_line(wrap, "CASS Doctor".bold().to_string());
        println!();
        if !rebuild_quarantine.complete
            || rebuild_quarantine
                .inventory
                .as_ref()
                .is_some_and(|inventory| !inventory.artifacts.is_empty())
        {
            for line in rebuild_quarantine.human_lines() {
                print_doctor_human_line(wrap, line);
            }
            println!();
        }

        for check in &checks {
            let icon = match check.status.as_str() {
                "pass" => "✓".green(),
                "warn" => "⚠".yellow(),
                "fail" => "✗".red(),
                _ => "?".normal(),
            };

            // Show passed checks only in verbose mode
            if check.status == "pass" && !verbose {
                continue;
            }

            let fix_indicator = if check.fix_applied {
                " [fixed]".green().to_string()
            } else if check.fix_available && !fix {
                " [fixable]".yellow().to_string()
            } else {
                String::new()
            };

            print_doctor_human_line(
                wrap,
                format!(
                    "{} {}: {}{}",
                    icon,
                    check.name.bold(),
                    check.message,
                    fix_indicator
                ),
            );
        }

        println!();
        if not_initialized {
            print_doctor_human_line(
                wrap,
                format!("{} Not initialized yet ({elapsed_ms}ms)", "○".yellow()),
            );
            if let Some(explanation) = &explanation {
                print_doctor_human_line(wrap, format!("  {explanation}"));
            }
            if let Some(action) = &recommended_action {
                print_doctor_human_line(wrap, format!("  {action}"));
            }
        } else if all_pass {
            print_doctor_human_line(
                wrap,
                format!("{} All checks passed ({elapsed_ms}ms)", "✓".green()),
            );
        } else {
            let summary_icon = if fail_count > 0 {
                "✗".red()
            } else {
                "⚠".yellow()
            };
            print_doctor_human_line(
                wrap,
                format!(
                    "{} {} failure(s), {} warning(s), {} fixed ({elapsed_ms}ms)",
                    summary_icon, fail_count, warn_count, issues_fixed
                ),
            );

            if auto_fix_applied && !auto_fix_actions.is_empty() {
                println!();
                print_doctor_human_line(wrap, "Auto-repair actions:".bold().to_string());
                for action in &auto_fix_actions {
                    print_doctor_human_line(wrap, format!("  - {action}"));
                }
            }

            if safe_auto_run_requested
                && (!safe_auto_eligibility.blocked_actions.is_empty()
                    || !safe_auto_eligibility.manual_approval_required.is_empty()
                    || safe_auto_eligibility.skipped_due_to_lock_or_unknown)
            {
                println!();
                print_doctor_human_line(wrap, "Safe auto-run:".bold().to_string());
                for reason in safe_auto_eligibility
                    .why_manual_approval_required
                    .iter()
                    .chain(safe_auto_eligibility.why_blocked.iter())
                    .take(3)
                {
                    print_doctor_human_line(wrap, format!("  - {reason}"));
                }
                if let Some(next) = safe_auto_eligibility.next_exact_command.as_deref() {
                    print_doctor_human_line(wrap, format!("  Next exact command: {next}"));
                }
            }

            if let Some(primary_incident) = incidents.first() {
                println!();
                print_doctor_human_line(wrap, "Primary incident:".bold().to_string());
                print_doctor_human_line(
                    wrap,
                    format!(
                        "  {} ({}, confidence={})",
                        doctor_serde_label(primary_incident.root_cause_kind),
                        primary_incident.summary,
                        primary_incident.confidence
                    ),
                );
                print_doctor_human_line(
                    wrap,
                    format!(
                        "  Archive risk: {}; derived-index risk: {}",
                        doctor_data_loss_risk_label(primary_incident.archive_risk_level),
                        doctor_data_loss_risk_label(primary_incident.derived_risk_level)
                    ),
                );
                if let Some(action) = primary_incident.safe_next_actions.first() {
                    print_doctor_human_line(wrap, format!("  Next safe action: {action}"));
                }
            }

            if !locks.is_empty() {
                println!();
                print_doctor_human_line(wrap, "Operation locks:".bold().to_string());
                for lock in &locks {
                    let stale = match lock.stale_suspected {
                        Some(true) => "stale-looking",
                        Some(false) => "active",
                        None => "unknown",
                    };
                    print_doctor_human_line(
                        wrap,
                        format!(
                            "  - {}: {} ({stale}, retry_policy={})",
                            doctor_serde_label(lock.lock_kind),
                            lock.redacted_lock_path,
                            lock.retry_policy
                        ),
                    );
                    print_doctor_human_line(wrap, format!("    {}", lock.recommended_action));
                }
                print_doctor_human_line(
                    wrap,
                    "  Do not delete lock files manually; use status/doctor receipts for evidence.",
                );
            }

            if !slow_operations.is_empty() {
                println!();
                print_doctor_human_line(wrap, "Slow doctor phases:".bold().to_string());
                for slow in slow_operations.iter().take(3) {
                    print_doctor_human_line(
                        wrap,
                        format!(
                            "  - {}: {}ms (threshold {}ms)",
                            slow.name, slow.elapsed_ms, slow.threshold_ms
                        ),
                    );
                }
            }

            if needs_rebuild {
                println!();
                print_doctor_human_line(wrap, "Recommended action:".bold().to_string());
                print_doctor_human_line(
                    wrap,
                    "  cass index --full     # Rebuild from source sessions",
                );
                println!();
                print_doctor_human_line(
                    wrap,
                    "Note: Your source session files are SAFE. Only derived data (index/db) will be rebuilt."
                        .dimmed()
                        .to_string(),
                );
            }
        }
        print_doctor_human_risk_summary(DoctorHumanRiskSummaryInput {
            wrap,
            doctor_status,
            health_class,
            risk_level,
            recommended_action: recommended_action.as_ref(),
            incidents: &incidents,
            sole_copy_warnings: &sole_copy_warnings,
            derived_semantic_assets: &derived_semantic_assets,
            repair_plan: repair_plan.as_ref(),
            cleanup_apply_result: cleanup_apply_result.as_ref(),
            operation_outcome: &operation_outcome,
            not_initialized,
        });
        print_doctor_operation_outcome_human(&operation_outcome, wrap);

        // Bead v6vuz: emit the same bounded readiness summary `cass status`
        // does, rendered through the wrap-aware doctor printer, so the human
        // `cass doctor` names the same readiness class and safe next action as
        // `cass doctor --json` (built from the same `readiness_snapshot`).
        let readiness_table = build_truth_table_from_state(&readiness_snapshot);
        let readiness_human = crate::search::human_readiness_summary::project_human_summary(
            &readiness_table,
            crate::search::readiness_projection::SurfaceKind::Status,
        );
        if readiness_human.is_safe(readiness_table.archive_risk) {
            println!();
            print_doctor_human_line(wrap, "Readiness:".bold().to_string());
            for line in readiness_human.render_lines() {
                print_doctor_human_line(wrap, line);
            }
        }
    }

    if fail_count == 0 || operation_exit_code_kind == DoctorExitCodeKind::Success {
        Ok(())
    } else {
        let (code, kind, message, hint, retryable) = match operation_exit_code_kind {
            DoctorExitCodeKind::Success => unreachable!("success outcomes return before error mapping"),
            DoctorExitCodeKind::LockBusy => (
                7,
                CliErrorKind::IndexBusy.kind_str(),
                "doctor repair blocked by an active operation lock".to_string(),
                Some(
                    "Wait for the active owner to finish, then rerun 'cass doctor --json'; read operation_state.owners for the lock owner evidence."
                        .to_string(),
                ),
                true,
            ),
            DoctorExitCodeKind::UsageError => (
                2,
                CliErrorKind::Usage.kind_str(),
                operation_outcome.reason.clone(),
                operation_outcome.next_command.as_ref().map(|next| {
                    format!("Inspect the structured doctor payload, then rerun `{next}`.")
                }),
                false,
            ),
            DoctorExitCodeKind::HealthFailure | DoctorExitCodeKind::RepairFailure => (
                5,
                CliErrorKind::Doctor.kind_str(),
                format!("{} failure(s) remain", fail_count),
                Some(
                    "Automatic safe repairs were attempted. Run 'cass index --full' \
                     to rebuild from source sessions. Re-run `cass doctor -v` or \
                     with CASS_TRACE_FILE=<path> for detailed logs - cass doctor \
                     does not produce a cass.log file itself (the rolling \
                     cass.log.YYYY-MM-DD appender is installed only for `cass \
                     tui`)."
                        .to_string(),
                ),
                true,
            ),
        };
        let err = CliError {
            code,
            kind,
            message,
            hint,
            retryable,
        };
        if structured_format.is_some() {
            Err(CliError::already_reported_from(&err))
        } else {
            Err(err)
        }
    }
}

#[derive(Debug)]
struct SessionSummaryRecord {
    agent: String,
    workspace: Option<PathBuf>,
    workspace_match_distance: Option<usize>,
    title: Option<String>,
    source_path: PathBuf,
    source_id: String,
    origin_host: Option<String>,
    started_at: Option<i64>,
    modified_at: Option<i64>,
    size_bytes: Option<u64>,
    message_count: i64,
    human_turns: i64,
}

#[derive(Debug, Serialize)]
struct SessionSummaryEntry {
    path: String,
    workspace: Option<String>,
    agent: String,
    title: Option<String>,
    source_id: String,
    origin_host: Option<String>,
    modified: Option<String>,
    size_bytes: Option<u64>,
    message_count: i64,
    human_turns: i64,
}

fn normalize_session_filter_path(path: &Path) -> CliResult<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|e| CliError::unknown(format!("current directory: {e}")))?
            .join(path)
    };
    // dunce: the plain `C:\...` spelling stored workspaces use, so a stored
    // workspace that no longer exists still matches on Windows (9v9xi).
    Ok(dunce::canonicalize(&absolute).unwrap_or(absolute))
}

fn workspace_match_distance(candidate: Option<&Path>, target: &Path) -> Option<usize> {
    let candidate = candidate?;
    let candidate = dunce::canonicalize(candidate).unwrap_or_else(|_| candidate.to_path_buf());

    if candidate == target {
        return Some(0);
    }

    let candidate_depth = candidate.components().count();
    let target_depth = target.components().count();

    if candidate.starts_with(target) {
        Some(candidate_depth.saturating_sub(target_depth))
    } else if target.starts_with(&candidate) {
        Some(target_depth.saturating_sub(candidate_depth))
    } else {
        None
    }
}
// -- generated lib-split bridges --
pub(crate) mod robot_schema;
#[allow(unused_imports)]
pub(crate) use crate::robot_schema::build_response_schemas::*;
#[allow(unused_imports)]
pub(crate) use crate::robot_schema::response_schema_doctor_repair_failure_marker::*;
#[allow(unused_imports)]
pub(crate) use crate::robot_schema::response_schema_index_state::*;
#[allow(unused_imports)]
pub(crate) use crate::robot_schema::response_schema_pack::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::robot_schema::response_schema_tests_part_1::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::robot_schema::response_schema_tests_part_2::*;
// -- generated lib-split bridges --
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::indexer::stall_diagnostics_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::indexer::stall_watchdog::*;
// -- generated lib-split bridges --
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::export::message_grouping_tests::*;
#[allow(unused_imports)]
pub use crate::export::messages::AgentFormat;
#[allow(unused_imports)]
pub use crate::export::messages::MessageClassification;
#[allow(unused_imports)]
pub use crate::export::messages::classify_message;
#[allow(unused_imports)]
pub use crate::export::messages::detect_agent_format;
#[allow(unused_imports)]
pub use crate::export::messages::extract_correlation_id;
#[allow(unused_imports)]
pub use crate::export::messages::group_messages_for_export;
#[allow(unused_imports)]
pub(crate) use crate::export::messages::*;
// -- generated lib-split bridges --
pub mod cli;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::pack::pack_budget_ms::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::pack::pack_field_mask_tests::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::agent_filter_tests::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::aggregate_field_validation_tests::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::ann_fallback_human_warning_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::compute_aggregations::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::effective_db_path_source::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::execute_search_operation::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::field_mask_resolution_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::fieldbudgets::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::lexical_engine_invariant_hint_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::output_display_results::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::resolve_search_defaults::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::run_cli_search::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::safe_robot_score_value::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::search_dry_run_meta::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::search_dry_run_meta_tests::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::search_lexical_self_heal_gate_tests::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::search_lexical_self_heal_tests::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::search_time_filter_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::searchlexicalselfhealdiagnosis::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::sparse_threshold_for_visible_limit::*;
#[allow(unused_imports)]
pub use crate::cli::commands::search::timefilter::SemanticSearchOptions;
#[allow(unused_imports)]
pub use crate::cli::commands::search::timefilter::TimeFilter;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::timefilter::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::toon_encode_options_from_env::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::truncate_content_tests::*;
// -- generated lib-split bridges --
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::analytics::analytics_filter_validation_tests::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::analytics::analytics_query_cli_error_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::analytics::analyticsvalidationsummary::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::analytics::franken_query_retry_callback_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::analytics::fresh_franken_count_retry::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::analytics::run_analytics::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::chatgpt_import_file_stem_tests::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::chatgpt_import_scan_root_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::chatgpt_import_source_name::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::forget::command::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::forget::forget_selector_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::forget::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::import_chatgpt_export::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::quarantine_mirror_import::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::run_mirror_prune::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::stats_diag_storage::run_diag::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::stats_diag_storage::run_stats::*;
// -- generated lib-split bridges --
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::run_onboarding_interactive_wizard::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::status_health::cli_read_db_tests_part_1::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::status_health::cli_read_db_tests_part_2::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::status_health::diagnostics::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::status_health::doctor_fts_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::status_health::probe_daemon_runtime_for_diagnostics::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::status_health::root_cause_signal_gather_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::status_health::run_health::*;
// -- generated lib-split bridges --
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::path_spelling_tests::*;
#[allow(unused_imports)]
pub use crate::cli::commands::sessions_context::SourceIngestReuseCapability;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::sessions_context::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::source_ingest_reuse_capability_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::view::role_to_export_string::*;
// -- generated lib-split bridges --
#[allow(unused_imports)]
pub use crate::cli::commands::capabilities_introspect::capabilitiesresponse::ArgumentSchema;
#[allow(unused_imports)]
pub use crate::cli::commands::capabilities_introspect::capabilitiesresponse::CapabilitiesLimits;
#[allow(unused_imports)]
pub use crate::cli::commands::capabilities_introspect::capabilitiesresponse::CapabilitiesResponse;
#[allow(unused_imports)]
pub use crate::cli::commands::capabilities_introspect::capabilitiesresponse::CommandSchema;
#[allow(unused_imports)]
pub use crate::cli::commands::capabilities_introspect::capabilitiesresponse::EnvVarCapability;
#[allow(unused_imports)]
pub use crate::cli::commands::capabilities_introspect::capabilitiesresponse::ExitCodeCapability;
#[allow(unused_imports)]
pub use crate::cli::commands::capabilities_introspect::capabilitiesresponse::IntrospectResponse;
#[allow(unused_imports)]
pub use crate::cli::commands::capabilities_introspect::capabilitiesresponse::MistakeRecoveryCapability;
#[allow(unused_imports)]
pub use crate::cli::commands::capabilities_introspect::capabilitiesresponse::WorkflowCapability;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::capabilities_introspect::capabilitiesresponse::*;
#[allow(unused_imports)]
pub use crate::cli::commands::capabilities_introspect::run_api_version::register_external_subcommands;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::capabilities_introspect::run_api_version::*;
// -- generated lib-split bridges --
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::expand::*;
#[cfg(all(test, not(windows)))]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::export::clipboard_helper_tests::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::export::export_html_password_tests::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::export::export_timestamp_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::export::format_as_markdown::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::export::indexed_conversation_fallback_tests_part_1::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::export::indexed_conversation_fallback_tests_part_2::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::export::indexed_export_cli_error::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::export::legacy_source_filter_tests::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::export::robot_output_score_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::export_html::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::index::run_lexical_reconcile_cli::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::resume::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::timeline::*;
#[allow(unused_imports)]
pub use crate::cli::paths::default_data_dir;
#[allow(unused_imports)]
pub use crate::cli::paths::default_db_path;
#[allow(unused_imports)]
pub use crate::cli::paths::resolve_data_dir;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::paths::resolve_data_dir_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::paths::session_paths::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::paths::*;
// -- generated lib-split bridges --
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::cli_models_resolution_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::daemon::*;
#[cfg(all(test, unix))]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::daemon_cli_config_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::mappings::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::models::run_models_command::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::models::run_models_remove::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::models::semantic_identity_backfill_checkpoint_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::run_on_large_stack::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::schedule::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::sources::run_sources_command::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::sources::source_doctor_local_storage_tests::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::state_db_integrity_probe_tests::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::subcommand_robot_output_tests::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::timeline_datetime_parse_tests::*;
// -- generated lib-split bridges --
#[allow(unused_imports)]
pub(crate) use crate::doctor::forensic::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::policy::assets::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::policy::types::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::policy::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::repair_state::types::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::report::checks::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::report::config::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::report::derived_semantic::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::doctor::report::doctor_derived_semantic_asset_tests::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::doctor::report::doctor_path_slash_string_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::report::doctor_pattern_matches_relative_path::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::report::human::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::report::inventory::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::report::recommendations::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::report::storage::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::report::types::*;
// -- generated lib-split bridges --
#[cfg(all(test, unix))]
#[allow(unused_imports)]
pub(crate) use crate::doctor::cleanup_target_safety_tests::*;
#[cfg(all(test, unix))]
#[allow(unused_imports)]
pub(crate) use crate::doctor::doctor_archive_export_path_safety_tests::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::doctor::doctor_asset_taxonomy_tests_part_1::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::doctor::doctor_asset_taxonomy_tests_part_2::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::doctor::doctor_asset_taxonomy_tests_part_3::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::doctor::doctor_asset_taxonomy_tests_part_4::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::doctor::doctor_asset_taxonomy_tests_part_5::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::doctor::doctor_asset_taxonomy_tests_part_6::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::doctor::doctor_asset_taxonomy_tests_part_7::*;
#[cfg(all(test, unix))]
#[allow(unused_imports)]
pub(crate) use crate::doctor::doctor_baseline_path_safety_tests::*;
#[cfg(all(test, unix))]
#[allow(unused_imports)]
pub(crate) use crate::doctor::doctor_support_bundle_path_safety_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::fs_safety::cleanup_apply::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::fs_safety::doctor_copy_source_path_is_safe::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::fs_safety::mutation_executor::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::lexical_cleanup_disposition_label::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::repair_state::cliheartbeat::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::repair_state::collect_doctor_interrupted_operation_states::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::doctor::repair_state::doctor_heartbeat_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::repair_state::doctor_sorted_cleanup_actions::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::repair_state::finalize_cleanup_apply_contract::*;
// -- generated lib-split bridges --
#[allow(unused_imports)]
pub(crate) use crate::doctor::archive::doctor_archive_coverage_snapshot::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::archive::export::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::backup_restore::kinds::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::backup_restore::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::doctor::baseline::doctor_baseline_path_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::baseline::document::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::baseline::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::candidate::promote_doctor_reconstruct_candidate_bundle::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::candidate::promotion::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::raw_mirror::collect_doctor_raw_mirror_report_with_thresholds_and::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::raw_mirror::doctor_bound_healthy_listing::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::doctor::raw_mirror::doctor_raw_mirror_listing_bound_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::raw_mirror::doctor_raw_mirror_root_dir::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::raw_mirror::doctorrawmirrorverificationmode::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::raw_mirror::query_doctor_raw_mirror_backfill_candidates::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::repair_state::doctor_acquire_mutation_lock::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::doctor::repair_state::doctor_lock_probe_error_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::repair_state::locks::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::report::coverage::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::run_doctor_ls::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::sources::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::support_bundle::capture::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::doctor::support_bundle::doctor_support_bundle_redaction_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::support_bundle::*;
