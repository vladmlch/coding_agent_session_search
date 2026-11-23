//! Indexed `list_sessions` for the `cass_owned_queries_v1` read lane.
//!
//! One bounded statement per page — `WHERE` + `ORDER BY` + `LIMIT limit+1`
//! with a keyset cursor. Never an offset walk and never a whole-catalog
//! materialization: the planner gets one statement whose LIMIT is the page
//! bound plus one, and continuation rows are reached through a
//! `(pinned, sort-value, conversation_id)` keyset predicate instead of a
//! scanned offset.
//!
//! Semantics mirror the viewer oracle
//! (`viewer/backends/cass/catalog_filters.py` + `summary.py`):
//!
//! * **activity** uses canonical tail-state precedence: a
//!   `conversation_tail_state` row replaces the denormalized
//!   `conversations` columns wholesale (`activity = last_message_created_at
//!   ?? ended_at ?? started_at`), exactly as `catalog.rs` decodes it.
//! * **sort order** is the shared oracle's total order exactly:
//!   `pinned DESC, knownness DESC, sort-value <dir>, public tie key ASC,
//!   conversation_id ASC` — pinned first (the list default), known values
//!   ordered, unknowns last, and ties resolved by the verified public
//!   identity (`{provider}:{native}`), not an internal coordinate. The
//!   tuple is persisted on `viewer_session_sort` (schema v5): pin state
//!   and the five scalar sort values are trigger-maintained mirrors; the
//!   tie key is recomputed through the same `identity` module the emit
//!   path verifies with (`viewer_extensions::sort` writes it inside the
//!   ingest generation transaction; the maintenance lanes converge it).
//! * **classification filters** (`is_subagent`, `category`) read the sort
//!   row's facts mirrors: known values filter membership; a surviving
//!   candidate with an *unknown* classification is a typed
//!   `data_unavailable`/`facts_pending` refusal — never silently dropped,
//!   never silently kept. Default browse keeps unknowns (with a
//!   `warnings` entry) and excludes only proven subagents.
//! * **activity/created windows** on rows whose timestamp is unknown are a
//!   `data_unavailable` refusal (oracle rule: unknown ≠ excluded).
//! * **unresolved identities** inside the candidate set refuse rather than
//!   emit an incomparable order: a pending key is `facts_pending`, a
//!   persisted resolver conflict surfaces its own kind.
//! * **unsupported filters/sorts** refuse `unsupported_filter`.
//!
//! A page is a handful of bounded statements, never one sorter pass: the
//! order query walks one per-sort composite index on `viewer_session_sort`
//! (`is_pinned DESC, knownness DESC, value, tie_key` — rowid is the
//! implicit final key) and returns at most `limit+1` ids plus their order
//! keys; payload hydration then runs one `WHERE id IN (page ids)` probe
//! per side table — repeated primary-key seeks under FrankenSQLite —
//! and assembles the payload rows in Rust. `pin_first=false` runs the
//! same indexed scan once per pin partition and merge-selects, still
//! `O(limit)`.

use anyhow::Result;

use super::protocol::refuse;

/// Per-column text guard mirroring `catalog.rs`: absurd payloads become
/// NULL, never a multi-MiB cell in a bounded page.
const TEXT_GUARD_BYTES: i64 = 512 * 1024;
/// Canonical-row byte budget (same profile as `catalog.rs::ROW_BYTES`).
const ROW_BYTES: i64 = 512 * 1024;
/// Cursor payload bound; a cursor is client-echoed opaque state.
const CURSOR_MAX_BYTES: usize = 4_096;

/// Canonical activity expression semantics: a tail-state row replaces
/// the denormalized columns when present; activity =
/// `last_message_created_at ?? ended_at ?? started_at`. Persisted into
/// `viewer_session_sort` by the v5 triggers and reproduced in the Rust
/// hydration assembly (`hydrate_rows`).

// ---------------------------------------------------------------------------
// Sort plan
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SortKey {
    Activity,
    Created,
    Project,
    StepCount,
    ArtifactCount,
}

impl SortKey {
    /// Persisted sort-value column on `viewer_session_sort` (NULL = unknown).
    fn value_column(self) -> &'static str {
        match self {
            Self::Activity => "vs.activity_ms",
            Self::Created => "vs.created_ms",
            Self::Project => "vs.project_path",
            Self::StepCount => "vs.step_count",
            Self::ArtifactCount => "vs.artifact_count",
        }
    }

    /// Persisted knownness flag for the sort value (`1` = known).
    fn known_column(self) -> &'static str {
        match self {
            Self::Activity => "vs.activity_known",
            Self::Created => "vs.created_known",
            Self::Project => "vs.project_known",
            Self::StepCount => "vs.steps_known",
            Self::ArtifactCount => "vs.artifacts_known",
        }
    }

    /// Cursor value type: `project_asc` sorts by workspace path TEXT.
    fn cursor_value_is_text(self) -> bool {
        matches!(self, Self::Project)
    }
}

fn sort_plan(sort: Option<&str>) -> Result<(&'static str, SortKey, bool)> {
    // `relevance` belongs to the search lane, never to browsing.
    let unsupported = || {
        refuse(
            "unsupported_filter",
            format!("Unsupported session list sort: {}", sort.unwrap_or("")),
        )
    };
    Ok(match sort.unwrap_or("updated_desc") {
        "updated_desc" => ("updated_desc", SortKey::Activity, true),
        "updated_asc" => ("updated_asc", SortKey::Activity, false),
        "created_desc" => ("created_desc", SortKey::Created, true),
        "created_asc" => ("created_asc", SortKey::Created, false),
        "project_asc" => ("project_asc", SortKey::Project, false),
        "step_desc" => ("step_desc", SortKey::StepCount, true),
        "artifact_desc" => ("artifact_desc", SortKey::ArtifactCount, true),
        _ => return Err(unsupported()),
    })
}

#[path = "query/filters.rs"]
mod filters;
#[path = "query/hydrate.rs"]
mod hydrate;
#[path = "query/pages.rs"]
mod pages;

// Dual-mounted: `cass-viewer-bridge` consumes `list_*` / `*_public` while
// `tests/viewer_bridge_extensions.rs` mounts `query.rs` via `#[path]` for
// `list_sessions` / `planned_page_sql`; each target leaves the other's
// re-exports unused.
#[allow(unused_imports)]
pub(crate) use filters::{ResolvedFilters, resolve_filters_public, run_probes_public};
#[allow(unused_imports)]
pub use pages::{PlannedPage, list_annotations, list_sessions, planned_page_sql};
