//! Persisted browse sort keys (`viewer_session_sort`, schema v5).
//!
//! Browse pages are served by a single indexed scan over the persisted
//! ORDER-key tuple `(is_pinned, knownness, sort value, public tie key,
//! rowid)` — no join sorter, no full-candidate sort. The `viewer_sort_*`
//! triggers (installed by `schema::ensure_schema`) keep every
//! SQL-computable column transactionally in step with its source table on
//! ANY connection. This module owns the one column SQL cannot compute:
//! `tie_key`, the verified public identity (`{provider}:{native}`) that
//! orders unknowns and equal values exactly the way the shared Viewer
//! oracle does.
//!
//! Write lanes:
//! * ingest — `publish_ingest_generation` resolves the conversation's
//!   public identity on the just-committed canonical row and writes the
//!   key inside the same generation transaction as facts+receipt;
//! * maintenance — `repair_sort_keys` (run by the authorized
//!   `--init-extensions`/`--backfill-viewer-extensions` lanes) converges
//!   every NULL tie row: unresolvable rows record their typed refusal kind
//!   in `tie_error` so browse refuses honestly instead of guessing order.
//!
//! The identity recomputation is shared VERBATIM with the bridge read path
//! (`viewer_bridge::identity` — provider-native CID derivation from
//! canonical provenance, `identity_conflict`/`provenance_conflict`
//! refusals), so a persisted tie key can never diverge from the overlay
//! the read path verifies at emit time.
#[path = "../viewer_bridge/extra.rs"]
mod extra;
// The include shares the WHOLE resolver verbatim — the reverse-direction
// half (public-id → conversation_id) is unused on this lane, which is the
// price of never forking the evidence rules.
#[allow(dead_code)]
#[path = "../viewer_bridge/identity.rs"]
mod identity;

/// Minimal refusal envelope for the shared `identity` module's
/// `super::protocol::refuse` calls — mirrors
/// `viewer_bridge::protocol::Refusal` so kind strings downcast identically
/// inside this crate.
mod protocol {
    #[derive(Debug, thiserror::Error)]
    #[error("{message}")]
    pub struct Refusal {
        pub kind: &'static str,
        pub message: String,
    }

    pub fn refuse(kind: &'static str, message: impl Into<String>) -> anyhow::Error {
        Refusal {
            kind,
            message: message.into(),
        }
        .into()
    }
}

use anyhow::Result;
use coding_agent_search::franken_sync::compat::{ConnectionExt, RowExt};
use coding_agent_search::franken_sync::{Connection, params};

use super::facts::FactsDb;

/// Resolved public tie key, or the typed refusal kind the shared resolver
/// raised — persisted verbatim so browse can refuse with the same kind the
/// oracle would surface.
pub enum SortTie {
    /// Verified `{provider}:{native}` public identity.
    Resolved(String),
    /// The resolver refused (`identity_conflict`, `provenance_conflict`,
    /// `session_not_found`, …); persisted in `tie_error`.
    Refused(&'static str),
}

/// Resolve one conversation's public tie key. Engine failures propagate
/// (the caller decides between abort and retry); only protocol refusals —
/// evidence-bound identity conflicts — become `SortTie::Refused`.
fn resolve_sort_tie(connection: &Connection, conversation_id: i64) -> Result<SortTie> {
    match identity::resolve_public_id(connection, conversation_id) {
        Ok(verified) => Ok(SortTie::Resolved(verified.public_id)),
        Err(error) => match error.downcast_ref::<protocol::Refusal>() {
            Some(refusal) => Ok(SortTie::Refused(refusal.kind)),
            None => Err(error),
        },
    }
}

/// Write a resolved/refused tie key onto the sort row, inserting the bare
/// row when the conversations-INSERT trigger somehow missed it (the other
/// mirrors stay at their defaults until the next source-table write
/// recomputes them — the repair lane below only ever owes the tie key).
pub fn write_sort_tie(db: &impl FactsDb, conversation_id: i64, tie: &SortTie) -> Result<()> {
    db.facts_execute(
        "INSERT OR IGNORE INTO viewer_session_sort (conversation_id) VALUES (?1)",
        params![conversation_id],
    )?;
    let (key, error): (Option<&str>, Option<&str>) = match tie {
        SortTie::Resolved(key) => (Some(key.as_str()), None),
        SortTie::Refused(kind) => (None, Some(*kind)),
    };
    db.facts_execute(
        "UPDATE viewer_session_sort \
         SET tie_key = ?2, tie_error = ?3 WHERE conversation_id = ?1",
        params![conversation_id, key, error],
    )?;
    Ok(())
}

/// Resolve and persist the tie key for one freshly published conversation
/// — the ingest-generation hook. Called inside the generation transaction
/// against the just-committed canonical row; a refusal is persisted (not
/// raised) because an unverifiable identity is durable state, never a
/// reason to roll back a committed facts publication.
pub fn publish_sort_tie(db: &impl FactsDb, conversation_id: i64, tie: &SortTie) -> Result<()> {
    write_sort_tie(db, conversation_id, tie)
}

/// Resolve the tie key for a conversation whose canonical row just
/// committed — the pre-transaction half of the ingest hook (reads
/// committed state; the write lands via `publish_sort_tie` inside the
/// generation transaction).
pub fn resolve_published_tie(connection: &Connection, conversation_id: i64) -> Result<SortTie> {
    resolve_sort_tie(connection, conversation_id)
}

/// Outcome of a maintenance-lane convergence pass.
#[derive(Debug, Default, Clone, Copy)]
pub struct SortRepairStats {
    /// Rows whose verified public key was persisted.
    pub resolved: usize,
    /// Rows whose resolver refusal kind was persisted into `tie_error`.
    pub refused: usize,
}

/// Converge every NULL tie row (`tie_key IS NULL AND tie_error IS NULL`)
/// by recomputing each conversation's public identity through the shared
/// resolver. Rows that previously recorded a refusal kind are left alone —
/// the kind is durable until the canonical identity inputs move (the
/// `viewer_sort_*` triggers clear it back to pending, which the next pass
/// re-resolves).
///
/// Batched: `resolve_public_ids` loads the `agents`/`sources` lookups once
/// per chunk; a chunk containing a refused row falls back to per-row
/// resolution so one conflict doesn't poison its neighbours' verified keys.
pub fn repair_sort_keys(connection: &Connection) -> Result<SortRepairStats> {
    let pending: Vec<i64> = connection.query_map_collect(
        "SELECT conversation_id FROM viewer_session_sort \
         WHERE tie_key IS NULL AND tie_error IS NULL \
         ORDER BY conversation_id",
        params![],
        |row| row.get_typed(0),
    )?;
    let mut stats = SortRepairStats::default();
    for chunk in pending.chunks(512) {
        match identity::resolve_public_ids(connection, chunk) {
            Ok(resolved_map) => {
                for (conversation_id, verified) in resolved_map {
                    write_sort_tie(
                        connection,
                        conversation_id,
                        &SortTie::Resolved(verified.public_id),
                    )?;
                    stats.resolved += 1;
                }
            }
            Err(_) => {
                for &conversation_id in chunk {
                    let tie = resolve_sort_tie(connection, conversation_id)?;
                    match tie {
                        SortTie::Resolved(_) => stats.resolved += 1,
                        SortTie::Refused(_) => stats.refused += 1,
                    }
                    write_sort_tie(connection, conversation_id, &tie)?;
                }
            }
        }
    }
    Ok(stats)
}
