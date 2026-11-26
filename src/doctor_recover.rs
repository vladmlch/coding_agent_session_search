//! Corrupt-archive recovery surfaces for `cass doctor` (#285).
//!
//! When the read-only pre-index health gate refuses to index because the
//! canonical `agent_search.db` is corrupt, the operator previously hit a wall:
//! `doctor repair` refuses an unreadable archive, a plain `.recover`
//! rebuild can be rejected on readonly open, and the only working
//! path was a hand-rolled JSONL reconstruction from cass's own preserved
//! events. This module turns that working recovery into first-class commands:
//!
//! * [`run_doctor_recover_from_archive`] rebuilds the source JSONL tree from the
//!   canonical archive's preserved `extra_json`/`extra_bin` envelopes so the
//!   data can be re-ingested into a fresh SQLite archive — no
//!   `.recover` and no external SQLite tool needed.
//! * [`run_doctor_rebuild_canonical_fts`] inspects exact FTS5 parity, resumes
//!   partial shadows in bounded batches, transactionally creates an absent
//!   shadow, and refuses destructive in-place work on unqueryable artifacts.
//! * [`run_doctor_cleanup_interrupted_artifacts`] quarantines interrupted
//!   `raw_mirror_capture` staging dirs that otherwise block doctor mutation,
//!   without forcing the operator to `rm` inside cass's own data dir.
//! * [`run_doctor_repair_leaked_pages`] reclaims pages that left the freelist
//!   without entering a tree (integrity_check's "Page N: never used"), after
//!   proving that is the only damage and backing up the live bundle.
//!
//! None of these surfaces ever delete canonical rows or source data: recovery
//! is additive (writes reconstructed files), the FTS5 shadow is fully
//! rebuildable from the canonical `messages`, interrupted artifacts are moved
//! into a quarantine dir rather than deleted, and the leaked-page repair frees
//! only pages that no table, index or freelist owns.

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::{CliError, CliResult};

mod cleanup;
mod fts;
mod leaked_pages;
mod recover;
#[cfg(test)]
mod tests;

pub use cleanup::run_doctor_cleanup_interrupted_artifacts;
pub use fts::run_doctor_rebuild_canonical_fts;
pub use leaked_pages::run_doctor_repair_leaked_pages;
pub(crate) use leaked_pages::{
    LEAKED_PAGES_DRY_RUN_COMMAND, attested_integrity_is_leaked_pages_only,
    integrity_is_leaked_pages_only,
};
pub use recover::run_doctor_recover_from_archive;

/// Page size for streaming conversations during reconstruction. Keeps memory
/// bounded on multi-GB archives (the exact failure surface from #285/#266).
const RECOVER_CONVERSATION_PAGE: i64 = 256;
/// Maximum canonical and FTS row IDs inspected by a read-only dry-run. The
/// mutating path still performs exact parity validation before changing data.
/// #345 plan of record: when the divergence scan hits this cap the dry-run
/// stops and reports ">= N divergent" instead of paying for an exact count on
/// a multi-million-row archive. Override with `CASS_FTS_DRYRUN_CAP`.
const FTS_DRY_RUN_ROWID_COMPARISON_CAP: usize = 4_096;

/// #345: the effective dry-run cap — `CASS_FTS_DRYRUN_CAP` (positive integer)
/// or the default. Pure parse half kept separate for unit testing.
fn parse_fts_dry_run_cap(raw: Option<&str>) -> usize {
    raw.and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|cap| *cap > 0)
        .unwrap_or(FTS_DRY_RUN_ROWID_COMPARISON_CAP)
}

fn fts_dry_run_rowid_comparison_cap() -> usize {
    parse_fts_dry_run_cap(dotenvy::var("CASS_FTS_DRYRUN_CAP").ok().as_deref())
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn resolve_db_path(data_dir: &Path, db_override: Option<&Path>) -> PathBuf {
    db_override
        .map(Path::to_path_buf)
        .unwrap_or_else(|| data_dir.join("agent_search.db"))
}

fn io_error(message: impl Into<String>, hint: Option<&str>) -> CliError {
    CliError {
        code: 14,
        kind: "io",
        message: message.into(),
        hint: hint.map(str::to_string),
        retryable: true,
    }
}

fn storage_error(message: impl Into<String>, hint: Option<&str>) -> CliError {
    CliError {
        code: 13,
        kind: "storage",
        message: message.into(),
        hint: hint.map(str::to_string),
        retryable: false,
    }
}

/// True when an FTS-repair failure is the FTS5 segment-writer
/// leaf-offset ceiling rather than corruption of the operator's data (GH #369).
///
/// A single-leaf-per-segment writer stores each term's byte offset inside that
/// leaf in a `u16`. When a single insert batch's combined terms + doclists
/// encode past 65,535 bytes, segment encoding hard-fails with
/// `segment leaf term offset exceeds u16` (surfaced as
/// `fts5: corrupt %_data record: …`) and the failure-atomic rebuild rolls back
/// without publishing a partial shadow. This is a *content-dependent, sticky*
/// engine limitation — not archive corruption — so it deserves a distinct,
/// reassuring operator diagnostic instead of the generic storage-error wall.
/// Note the `gh362` overlong-*term* tokenizer cap (`FTS5_MAX_TERM_BYTES`) does
/// not address this: the overflow is cumulative across many in-cap terms, not a
/// single oversized token.
fn is_fts5_oversized_leaf_error(err: &anyhow::Error) -> bool {
    // Match the full rendered chain so it is robust to however the storage
    // error was wrapped on the way up (context strings, `{e:#}`, etc.).
    let rendered = format!("{err:#}");
    rendered.contains("segment leaf term offset exceeds u16")
        || rendered.contains("segment leaf rowid offset exceeds u16")
        || rendered.contains("segment leaf footer offset exceeds u16")
        || rendered.contains("segment footer offset exceeds u16")
        || (rendered.contains("corrupt %_data record") && rendered.contains("segment leaf"))
}

/// #368 defect 3: does this error indicate the FTS5 `%_data` shadow structure
/// is corrupt enough to fail a read or open (e.g. C SQLite's
/// `"fts5: corrupt structure record for table ..."` or legacy
/// `"corrupt %_data record"`)? Such a shadow can be rebuilt by dropping and
/// recreating it from canonical rows. The oversized-*leaf* (gh#369) shape is
/// excluded — that is a content-dependent write-time limitation with its own
/// reassuring diagnostic, not a persisted corruption.
fn is_fts5_shadow_open_corruption_error(err: &anyhow::Error) -> bool {
    let rendered = format!("{err:#}");
    (rendered.contains("corrupt %_data record") || rendered.contains("corrupt structure record"))
        && !is_fts5_oversized_leaf_error(err)
}

/// #434 defect 2: schema-level open refusals that name a derived FTS5 shadow
/// object — the reported shape is `database disk image is malformed:
/// sqlite_master is missing implicit autoindex slot 1 for table
/// \`fts_messages_config\``, which fails during the schema reload BEFORE the
/// ordinary FTS5 deferral applies, so `--rebuild-canonical-fts` used to refuse
/// to open the very archive it exists to repair. Every `fts_messages*` object
/// is fully derived from canonical rows, so the right repair is the same
/// deferred-open drop+recreate the corrupt-`%_data` class uses — attempted
/// below; when even the deferred open cannot get past the schema reload,
/// doctor now says so explicitly and routes to the raw-mirror recovery path
/// instead of repeating the refusal.
pub(crate) fn is_fts_shadow_schema_level_open_failure(err: &anyhow::Error) -> bool {
    let rendered = format!("{err:#}");
    rendered.contains("fts_messages")
        && (rendered.contains("missing implicit autoindex slot")
            || (rendered.contains("database disk image is malformed")
                && rendered.contains("sqlite_master")))
}

/// Whether the pinned storage engine's deferred-FTS5 repair open accepts an
/// FTS5 shadow table whose declared implicit autoindex has no `sqlite_master`
/// row: the catalog cass wrote for `fts_messages_config` before the #434
/// writer fix (GH #503). Since the repair open binds/accepts the missing
/// autoindex, `--rebuild-canonical-fts` rebuilds that shadow in place.
/// `pinned_engine_deferred_open_matches_the_legacy_shadow_catalog_policy`
/// runs the real repair open on a schema-v1 archive and fails when a pin bump
/// changes the answer, so this cannot silently go stale.
pub(crate) const ENGINE_REPAIRS_MISSING_FTS5_SHADOW_AUTOINDEX: bool = true;

/// The GH #503 open refusal: an `fts_messages*` shadow table declares an
/// implicit autoindex that `sqlite_master` does not have.
pub(crate) fn is_missing_fts5_shadow_autoindex_failure(err: &anyhow::Error) -> bool {
    let rendered = format!("{err:#}");
    rendered.contains("missing implicit autoindex slot") && rendered.contains("`fts_messages")
}

/// The refusal both `--rebuild-canonical-fts --dry-run` and `--yes` report
/// for the GH #503 catalog while the pinned engine cannot open it for repair:
/// no plan is offered that the apply could not carry out.
fn legacy_fts_shadow_catalog_unrepairable_error(
    db_path: &Path,
    open_err: &anyhow::Error,
) -> CliError {
    storage_error(
        format!(
            "the fts_messages FTS5 shadow in {} uses a legacy catalog (a table declaring a primary key with no sqlite_master autoindex row, written by cass before the #434 fix); this build's storage engine refuses that catalog even for the shadow repair, so it cannot be rebuilt in place ({open_err:#})",
            db_path.display()
        ),
        Some(
            "Nothing was changed, and the conversations and messages are not what the engine rejected. Keep the archive as it is and do not run 'cass doctor --fix'. Support for repairing this catalog in place needs a storage-engine release (GH #503). Meanwhile 'cass index --full --data-dir <NEW_DIR>' re-indexes the sessions whose source files still exist, without touching this archive.",
        ),
    )
}

/// The distinct, non-alarming diagnostic for the GH #369 oversized-leaf case:
/// canonical rows and the Tantivy index are intact and fully serve search; only
/// the optional SQLite-side FTS5 shadow cannot be materialized for this corpus.
fn fts5_oversized_leaf_shadow_error(db_path: &Path) -> CliError {
    CliError {
        code: 13,
        kind: "fts5-oversized-leaf-shadow-unbuildable",
        message: format!(
            "the canonical SQLite FTS5 shadow cannot be built for {} because a single insert \
             batch in this corpus encodes past the SQLite FTS5 segment-leaf u16 offset \
             ceiling (one-leaf-per-segment limitation, GH #369) — this is an engine limitation, \
             not corruption of your archive, and the failed rebuild was rolled back without \
             publishing a partial shadow",
            db_path.display()
        ),
        hint: Some(
            "No action is needed and no data was lost: the canonical SQLite tables and the \
             Tantivy lexical index are intact and fully serve search — only the optional \
             SQLite-side `fts_messages` shadow is affected, and `cass doctor check` stays \
             healthy. This is tracked upstream for a multi-leaf FTS5 segment writer; re-run \
             `--rebuild-canonical-fts --yes` once the pinned SQLite build ships that fix."
                .to_string(),
        ),
        retryable: false,
    }
}

fn print_json(envelope: &serde_json::Value) -> CliResult<()> {
    let rendered = serde_json::to_string_pretty(envelope).map_err(|e| CliError {
        code: 9,
        kind: "internal",
        message: format!("serialize recovery envelope: {e}"),
        hint: None,
        retryable: false,
    })?;
    println!("{rendered}");
    Ok(())
}

/// One reconstructed session file (or a skip with the reason).
#[derive(Debug)]
struct ReconstructedSession {
    /// `None` when the canonical row's `id` itself was unreadable (#391).
    conversation_id: Option<i64>,
    external_id: Option<String>,
    relative_or_source_path: String,
    written_path: Option<PathBuf>,
    line_count: usize,
    skipped_reason: Option<String>,
    /// Columns whose stored type disagreed with the schema and were coerced
    /// while reading the canonical row (empty for a healthy row).
    coercions: Vec<String>,
}

/// Compute the on-disk output path for a reconstructed session.
///
/// We deliberately do NOT write back over the original `source_path`: the
/// recovery target is an operator-chosen directory so nothing existing is
/// clobbered. Each session is keyed by its `external_id` when present (stable,
/// collision-free across machines) and otherwise by its conversation id, with
/// the original file name preserved as a `.jsonl` suffix for readability.
fn reconstruction_output_path(
    target_dir: &Path,
    conversation_id: i64,
    external_id: Option<&str>,
    source_path: &Path,
) -> PathBuf {
    let stem = external_id
        .map(sanitize_path_component)
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("conversation-{conversation_id}"));
    // Preserve a hint of the original file name without trusting it as a path.
    let original_hint = source_path
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .map(|s| sanitize_path_component(&s))
        .filter(|s| !s.is_empty());
    let file_name = match original_hint {
        Some(hint) if hint != stem => format!("{stem}__{hint}.jsonl"),
        _ => format!("{stem}.jsonl"),
    };
    target_dir.join(file_name)
}

/// Replace path-unsafe characters so reconstructed file names never escape the
/// recovery dir or collide on case-insensitive filesystems.
fn sanitize_path_component(raw: &str) -> String {
    raw.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim_matches('.')
        .to_string()
}
