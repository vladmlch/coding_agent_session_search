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
//!   `--init-extensions`/`--backfill-viewer-extensions` lanes and by the
//!   ingest profile guard) converges every NULL tie row through the same
//!   evidence-guarded write: unresolvable rows record their typed refusal
//!   kind in `tie_error` so browse refuses honestly instead of guessing
//!   order, and a canonical identity change racing a repair chunk can
//!   never leave a stale key trusted.
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

use anyhow::{Context, Result};
use coding_agent_search::franken_sync::compat::{ConnectionExt, RowExt, TransactionExt};
use coding_agent_search::franken_sync::{Connection, Row, params};

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
/// The row-ensure is `WHERE EXISTS`-guarded like the guarded writer's: a
/// conversation deleted between the pending-list read and this write
/// (FKs are off on the ingest writer) must not regain a dangling sort
/// row that would wedge on a permanent `session_not_found`.
///
/// Low-level primitive: every in-repo write lane goes through
/// [`publish_sort_tie_guarded`], which additionally re-verifies the
/// canonical identity evidence inside the commit; this unguarded-UPDATE
/// form is retained for callers that already hold the commit boundary.
pub fn write_sort_tie(db: &impl FactsDb, conversation_id: i64, tie: &SortTie) -> Result<()> {
    db.facts_execute(
        "INSERT OR IGNORE INTO viewer_session_sort (conversation_id) \
         SELECT ?1 WHERE EXISTS (SELECT 1 FROM conversations c WHERE c.id = ?1)",
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

/// The identity-bearing canonical columns the tie resolution consumes —
/// the exact set `viewer_sort_conversations_upd` NULLs `tie_key` on
/// (`agent_id`, `source_id`, `external_id`, `source_path`, `origin_host`,
/// `metadata_bin`, `metadata_json`). Captured BEFORE the resolver runs: a
/// canonical change racing the two probes can only make the captured
/// evidence *older* than the resolved basis — the guarded in-transaction
/// write (`publish_sort_tie_guarded`) then refuses and the caller
/// re-resolves — never a stale verified key persisted silently.
#[derive(Debug)]
pub struct TieEvidence {
    agent_id: Option<i64>,
    source_id: Option<String>,
    external_id: Option<String>,
    source_path: Option<String>,
    origin_host: Option<String>,
    metadata_bin: Option<Vec<u8>>,
    metadata_json: Option<String>,
}

/// Canonical evidence probe behind [`resolve_published_tie_with_evidence`]:
/// one primary-key read over the same column set `FORWARD_ROW_SQL` feeds
/// the resolver. `None` = the canonical row is already gone.
fn read_tie_evidence(
    connection: &Connection,
    conversation_id: i64,
) -> Result<Option<TieEvidence>> {
    let rows = connection.query_map_collect(
        "SELECT c.agent_id, c.source_id, c.external_id, c.source_path, \
         c.origin_host, c.metadata_bin, c.metadata_json \
         FROM conversations c WHERE c.id = ?1 LIMIT 2",
        params![conversation_id],
        |row| {
            Ok(TieEvidence {
                agent_id: row.get_typed(0)?,
                source_id: row.get_typed(1)?,
                external_id: row.get_typed(2)?,
                source_path: row.get_typed(3)?,
                origin_host: row.get_typed(4)?,
                metadata_bin: row.get_typed(5)?,
                metadata_json: row.get_typed(6)?,
            })
        },
    )?;
    Ok(rows.into_iter().next())
}

/// Evidence extraction for a [`Row`] carried out of
/// `identity::resolve_public_ids_with_evidence`: the same seven
/// identity-bearing columns at `FORWARD_ROW_SQL` positions 1..=7 (index 0
/// is `c.id`). Unlike [`read_tie_evidence`] this adds no probe — the row
/// the batch resolver verified IS the evidence, so the guarded write can
/// only refuse on a change committed after that exact basis, never on a
/// stale re-read.
fn tie_evidence_from_row(row: &Row) -> Result<TieEvidence> {
    Ok(TieEvidence {
        agent_id: row.get_typed(1)?,
        source_id: row.get_typed(2)?,
        external_id: row.get_typed(3)?,
        source_path: row.get_typed(4)?,
        origin_host: row.get_typed(5)?,
        metadata_bin: row.get_typed(6)?,
        metadata_json: row.get_typed(7)?,
    })
}

/// `resolve_sort_tie` plus the canonical identity evidence captured
/// at resolve time. The generation transaction re-binds the persisted key
/// to this evidence (`publish_sort_tie_guarded`) so a cross-connection
/// canonical identity change between resolve and commit can never leave a
/// stale key behind: the guard's in-transaction canonical re-read no longer
/// matches, the write is skipped, and the caller rolls back to re-resolve.
pub fn resolve_published_tie_with_evidence(
    connection: &Connection,
    conversation_id: i64,
) -> Result<(SortTie, Option<TieEvidence>)> {
    // Evidence FIRST: ordering matters. Reading evidence before the
    // resolver's own probe means any canonical change between the two
    // reads fails the later guard (evidence older than the resolved
    // basis); the reverse order could pair a stale resolution with fresh
    // evidence and slip a stale key past the guard.
    let evidence = read_tie_evidence(connection, conversation_id)?;
    let tie = resolve_sort_tie(connection, conversation_id)?;
    Ok((tie, evidence))
}

/// Write a resolved/refused tie key, guarded on the canonical identity
/// evidence captured at resolve time. The UPDATE is a no-op when any
/// identity-bearing column moved between the resolve probes and this
/// in-transaction write — including the cross-connection UPDATE whose
/// trigger already NULLed `tie_key`; a blind overwrite there would
/// persist a stale verified key that nothing re-invalidates.
///
/// Returns `true` when the key persisted. `false` means the canonical
/// basis moved (or the row is gone): the caller must roll back, re-resolve
/// and retry — the publish protocol's idempotence makes the retry honest.
pub fn publish_sort_tie_guarded(
    db: &impl FactsDb,
    conversation_id: i64,
    tie: &SortTie,
    evidence: Option<&TieEvidence>,
) -> Result<bool> {
    let Some(evidence) = evidence else {
        return Ok(false);
    };
    // Row-ensure shares the guard's honesty: a deleted conversation must
    // not gain a dangling sort row under a writer with FK enforcement off.
    db.facts_execute(
        "INSERT OR IGNORE INTO viewer_session_sort (conversation_id) \
         SELECT ?1 WHERE EXISTS (SELECT 1 FROM conversations c WHERE c.id = ?1)",
        params![conversation_id],
    )?;
    let (key, error): (Option<&str>, Option<&str>) = match tie {
        SortTie::Resolved(key) => (Some(key.as_str()), None),
        SortTie::Refused(kind) => (None, Some(*kind)),
    };
    let written = db.facts_execute(
        "UPDATE viewer_session_sort \
         SET tie_key = ?2, tie_error = ?3 \
         WHERE conversation_id = ?1 \
           AND EXISTS ( \
               SELECT 1 FROM conversations c WHERE c.id = ?1 \
                 AND c.agent_id IS ?4 AND c.source_id IS ?5 \
                 AND c.external_id IS ?6 AND c.source_path IS ?7 \
                 AND c.origin_host IS ?8 AND c.metadata_bin IS ?9 \
                 AND c.metadata_json IS ?10 \
           )",
        params![
            conversation_id,
            key,
            error,
            evidence.agent_id,
            evidence.source_id.as_deref(),
            evidence.external_id.as_deref(),
            evidence.source_path.as_deref(),
            evidence.origin_host.as_deref(),
            evidence.metadata_bin.as_deref(),
            evidence.metadata_json.as_deref(),
        ],
    )?;
    Ok(written > 0)
}

/// One bounded `idx_viewer_sort_tie_pending` probe: `true` while any sort
/// row still waits on the canonical-identity recomputation (`tie_key IS
/// NULL AND tie_error IS NULL` — persisted resolver refusals are durable
/// answers, not pending work). The steady-state cost inside the ingest
/// profile guard.
pub fn sort_ties_pending(connection: &Connection) -> Result<bool> {
    Ok(!connection
        .query(
            "SELECT 1 FROM viewer_session_sort \
             WHERE tie_key IS NULL AND tie_error IS NULL LIMIT 1",
        )?
        .is_empty())
}

/// Outcome of a maintenance-lane convergence pass. Counts only rows whose
/// write actually committed — a row the evidence guard refused (a racing
/// canonical writer moved its basis mid-pass) is counted nowhere: it
/// stays pending for the next pass.
#[derive(Debug, Default, Clone, Copy)]
pub struct SortRepairStats {
    /// Rows whose verified public key was persisted.
    pub resolved: usize,
    /// Rows whose resolver refusal kind was persisted into `tie_error`.
    pub refused: usize,
}

/// Deterministic stale-tie-race injection for the maintenance lane
/// (test-only): `CASS_TEST_VIEWER_REPAIR_RACE_SQL` holds SQL executed on
/// this connection ONCE per repair pass — after the first chunk's
/// committed-state resolution reads and before its write transaction
/// opens. That is exactly the interleaving of a cross-connection
/// canonical identity write racing a repair chunk: the conversations
/// trigger NULLs `tie_key`, and the evidence-guarded write must leave the
/// row pending rather than persist the stale resolved key.
fn repair_race_probe() -> Option<String> {
    dotenvy::var("CASS_TEST_VIEWER_REPAIR_RACE_SQL")
        .ok()
        .filter(|sql| !sql.is_empty())
}

/// Converge every NULL tie row (`tie_key IS NULL AND tie_error IS NULL`)
/// by recomputing each conversation's public identity through the shared
/// resolver. Rows that previously recorded a refusal kind are left alone —
/// the kind is durable until the canonical identity inputs move (the
/// `viewer_sort_*` triggers clear it back to pending, which the next pass
/// re-resolves).
///
/// Batched: `resolve_public_ids_with_evidence` loads the
/// `agents`/`sources` lookups once per chunk and returns each row's
/// resolution basis (the `FORWARD_ROW_SQL` columns) alongside its
/// verified identity; a chunk containing a refused row falls back to
/// per-row `resolve_published_tie_with_evidence` so one conflict doesn't
/// poison its neighbours' verified keys. Resolution reads committed
/// state on the bare connection; each chunk's writes then commit inside
/// ONE bounded transaction — the ingest-lane caller
/// (`ensure_profile_for_ingest`) converges archive-scale backlogs in
/// 512-row commit units, never one giant pass and never a per-row fsync
/// storm.
///
/// Every write goes through [`publish_sort_tie_guarded`]: a canonical
/// identity change committed between a chunk's resolve probes and its
/// write transaction (a supported reality — concurrent writers hit the
/// same archive) fires the NULL trigger, and the guard's in-transaction
/// evidence re-check refuses the stale key instead of blindly
/// overwriting it with a value nothing re-invalidates. Guard-refused
/// rows — raced, or deleted mid-pass — simply stay pending for the next
/// pass; a deleted conversation never regains a dangling sort row
/// because the row-ensure is itself `WHERE EXISTS`-guarded.
///
/// Must NOT be called with a transaction already open on this connection.
pub fn repair_sort_keys(connection: &Connection) -> Result<SortRepairStats> {
    let pending: Vec<i64> = connection.query_map_collect(
        "SELECT conversation_id FROM viewer_session_sort \
         WHERE tie_key IS NULL AND tie_error IS NULL \
         ORDER BY conversation_id",
        params![],
        |row| row.get_typed(0),
    )?;
    let mut stats = SortRepairStats::default();
    let mut race_probe = repair_race_probe();
    for chunk in pending.chunks(512) {
        // Resolve the whole chunk first (committed-state reads), then
        // commit its writes as one bounded transaction.
        let mut ties: Vec<(i64, SortTie, Option<TieEvidence>)> =
            Vec::with_capacity(chunk.len());
        match identity::resolve_public_ids_with_evidence(connection, chunk) {
            Ok(resolved_map) => {
                for (conversation_id, (verified, evidence_row)) in resolved_map {
                    ties.push((
                        conversation_id,
                        SortTie::Resolved(verified.public_id),
                        Some(tie_evidence_from_row(&evidence_row)?),
                    ));
                }
            }
            Err(_) => {
                for &conversation_id in chunk {
                    let (tie, evidence) =
                        resolve_published_tie_with_evidence(connection, conversation_id)?;
                    ties.push((conversation_id, tie, evidence));
                }
            }
        }
        if ties.is_empty() {
            continue;
        }
        // Test-only: inject the cross-connection canonical mutation at
        // exactly the race window — resolved but not yet written.
        if let Some(race_sql) = race_probe.take() {
            connection
                .execute_batch(&race_sql)
                .context("injecting repair-lane tie-race canonical mutation")?;
        }
        let mut tx = connection
            .transaction()
            .context("opening sort tie repair batch transaction")?;
        for (conversation_id, tie, evidence) in &ties {
            if publish_sort_tie_guarded(&tx, *conversation_id, tie, evidence.as_ref())? {
                match tie {
                    SortTie::Resolved(_) => stats.resolved += 1,
                    SortTie::Refused(_) => stats.refused += 1,
                }
                continue;
            }
            // Guard refused: the canonical basis moved (or the row is
            // gone) between resolve and this in-transaction write. The
            // row stays pending — the next pass re-resolves it against
            // the moved identity; a stale key is never persisted.
            if evidence.is_none() {
                // Already gone at resolve time: this pending sort row is
                // an orphan the conversations-del trigger would have
                // removed. Converge it honestly — the re-check inside
                // the transaction leaves a same-id row re-created
                // mid-pass pending for the next pass instead.
                tx.facts_execute(
                    "DELETE FROM viewer_session_sort \
                     WHERE conversation_id = ?1 \
                       AND NOT EXISTS (\
                           SELECT 1 FROM conversations c WHERE c.id = ?1\
                       )",
                    params![*conversation_id],
                )?;
            }
        }
        tx.commit()
            .context("committing sort tie repair batch transaction")?;
    }
    Ok(stats)
}
