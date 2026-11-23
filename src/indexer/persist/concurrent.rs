use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use rand::RngExt;
use rayon::prelude::*;

use crate::connectors::NormalizedConversation;
use crate::franken_sync::FrankenError;
use crate::franken_sync::compat::{ConnectionExt, RowExt};
use crate::indexer::{
    LexicalPopulationStrategy, lexical_population_strategy_requires_inline_tantivy,
};
use crate::model::types::{Agent, AgentKind, Conversation};
use crate::search::tantivy::TantivyIndex;
use crate::storage::sqlite::{FrankenStorage, InsertOutcome};

use super::batch_map::{PersistHeartbeat, extract_provenance, map_batch_to_internal};
use super::canonical_replay::{
    PersistBatchOutcome, changed_message_indices, lexical_packet_for_changed_messages,
    packet_semantic_delta_for_outcome, positional_indices_for_inserted,
    publish_changed_packet_messages, record_persisted_raw_mirror_db_link_groups,
    replay_canonical_conversation_for_rebuild, should_defer_incremental_lexical_update_after_error,
    should_inject_incremental_lexical_update_oom, stage_persisted_region_docs,
};

pub(super) fn begin_concurrent_writes_enabled() -> bool {
    dotenvy::var("CASS_INDEXER_BEGIN_CONCURRENT")
        .map(|v| !(v == "0" || v.eq_ignore_ascii_case("false")))
        .unwrap_or(false)
}

// Caps on the begin-concurrent knobs below. Each is chosen to be
// generous (covers every realistic workload) while bounding the
// blast radius of a typo / pathological env value:
//
// - Retries cap: worst-case wall time = max_retries × (256 ms backoff
//   + 256 ms jitter) ≈ ~16 s at 32 retries. Beyond that the outer
//   RetryableFallback path should kick in; spinning longer just burns
//   CPU.
// - Chunk size cap: rayon parallelism is proportional; 512 convos per
//   chunk × 32-core hosts is already deep in oversubscription land.
// - Writer cache KiB cap: applied per parallel chunk writer. With the
//   default chunk count this is ~32 × 64 MiB ≈ 2 GiB peak, below
//   anything a CI host or dev laptop would OOM on.
pub(super) const BEGIN_CONCURRENT_RETRY_MAX: usize = 32;
pub(super) const BEGIN_CONCURRENT_CHUNK_SIZE_MAX: usize = 512;
pub(super) const BEGIN_CONCURRENT_WRITER_CACHE_KIB_MAX: i64 = 65_536;

fn env_usize_bounded(var: &str, default: usize, max: usize) -> usize {
    match dotenvy::var(var).ok().and_then(|v| v.parse::<usize>().ok()) {
        Some(0) => default,
        Some(v) if v > max => {
            tracing::warn!(
                env_var = var,
                requested = v,
                cap = max,
                "env var exceeds safe cap; clamping"
            );
            max
        }
        Some(v) => v,
        None => default,
    }
}

fn env_i64_bounded(var: &str, default: i64, max: i64) -> i64 {
    match dotenvy::var(var).ok().and_then(|v| v.parse::<i64>().ok()) {
        Some(v) if v <= 0 => default,
        Some(v) if v > max => {
            tracing::warn!(
                env_var = var,
                requested = v,
                cap = max,
                "env var exceeds safe cap; clamping"
            );
            max
        }
        Some(v) => v,
        None => default,
    }
}

pub(in crate::indexer) fn begin_concurrent_retry_limit() -> usize {
    env_usize_bounded(
        "CASS_INDEXER_BEGIN_CONCURRENT_RETRIES",
        6,
        BEGIN_CONCURRENT_RETRY_MAX,
    )
}

pub(super) fn begin_concurrent_chunk_size() -> usize {
    env_usize_bounded(
        "CASS_INDEXER_BEGIN_CONCURRENT_CHUNK_SIZE",
        32,
        BEGIN_CONCURRENT_CHUNK_SIZE_MAX,
    )
}

pub(super) fn begin_concurrent_writer_cache_kib() -> i64 {
    env_i64_bounded(
        "CASS_INDEXER_BEGIN_CONCURRENT_WRITER_CACHE_KIB",
        4096,
        BEGIN_CONCURRENT_WRITER_CACHE_KIB_MAX,
    )
}

pub(super) fn serial_batch_chunk_size() -> usize {
    dotenvy::var("CASS_INDEXER_SERIAL_CHUNK_SIZE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(128)
}

fn index_writer_busy_timeout_ms() -> u64 {
    // GH473: keep engine-local waits short; complete idempotent write
    // transactions already have bounded retries at the CASS boundary.
    dotenvy::var("CASS_INDEX_WRITER_BUSY_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(10)
}

/// WAL autocheckpoint threshold (pages) for the index writer.
///
/// GH#320: bulk-import mode used to disable autocheckpoints entirely
/// (`0`), deferring the whole checkpoint to post-publish. That let the
/// WAL grow with the corpus (1 GB+ on large fleets) and — because the
/// frankensqlite pager keeps WAL-resident state in memory — drove the
/// ingest-phase physical footprint to roughly 3.5-4x the WAL size
/// (13-14 GB peaks on 16 GB hosts). A bounded-but-large threshold keeps
/// bulk import's amortized-checkpoint intent (one checkpoint per
/// ~256 MB of WAL at the default 4 KiB page size) while capping the
/// WAL-proportional memory. Operators can restore the old unbounded
/// behavior with `CASS_INDEX_WRITER_WAL_AUTOCHECKPOINT_PAGES=0`.
pub(in crate::indexer) const BULK_IMPORT_WAL_AUTOCHECKPOINT_PAGES: i64 = 65_536;

pub(super) fn index_writer_wal_autocheckpoint_pages(defer_checkpoints: bool) -> i64 {
    dotenvy::var("CASS_INDEX_WRITER_WAL_AUTOCHECKPOINT_PAGES")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .filter(|v| *v >= 0)
        .unwrap_or(if defer_checkpoints {
            BULK_IMPORT_WAL_AUTOCHECKPOINT_PAGES
        } else {
            1000
        })
}

pub(in crate::indexer) fn defer_lexical_updates_enabled() -> bool {
    dotenvy::var("CASS_DEFER_LEXICAL_UPDATES")
        .map(|v| !(v == "0" || v.eq_ignore_ascii_case("false")))
        .unwrap_or(false)
}

fn apply_begin_concurrent_writer_tuning(storage: &FrankenStorage, defer_checkpoints: bool) {
    let cache_kib = begin_concurrent_writer_cache_kib();
    let pragma = format!("PRAGMA cache_size = -{cache_kib};");
    if let Err(err) = storage.raw().execute(&pragma) {
        tracing::debug!(
            cache_kib,
            error = %err,
            "failed_to_apply_begin_concurrent_writer_cache_size"
        );
    }
    apply_index_writer_checkpoint_policy(storage, defer_checkpoints);
}

pub(in crate::indexer) fn apply_index_writer_busy_timeout(storage: &FrankenStorage) {
    let busy_timeout_ms = index_writer_busy_timeout_ms();
    if storage.index_writer_busy_timeout_ms() == Some(busy_timeout_ms) {
        return;
    }
    let pragma = format!("PRAGMA busy_timeout = {busy_timeout_ms};");
    if let Err(err) = storage.raw().execute(&pragma) {
        tracing::debug!(
            busy_timeout_ms,
            error = %err,
            "failed_to_apply_index_writer_busy_timeout"
        );
    } else {
        storage.mark_index_writer_busy_timeout_ms(busy_timeout_ms);
    }
}

/// GH#320/#425: WAL-size threshold (bytes) above which bulk ingest resets
/// the WAL between batches. `0` disables the bound and restores the
/// fully-deferred pre-#320 behavior.
///
/// Rationale: bulk-import mode defers checkpoints so the hot insert loop
/// never stalls on backfill I/O, but a corpus-sized WAL (1-2 GB+ on large
/// fleets) drags a proportional slice of pager state into memory and makes
/// the single post-publish TRUNCATE checkpoint — the #319/#321 wedge
/// window — enormous. A between-batches single-connection TRUNCATE now
/// resets the WAL instead of repeatedly backfilling it without shrinking.
const BULK_INGEST_WAL_CHECKPOINT_DEFAULT_BYTES: u64 = 512 * 1024 * 1024;

fn bulk_ingest_wal_checkpoint_threshold_bytes() -> u64 {
    dotenvy::var("CASS_INDEX_INGEST_WAL_CHECKPOINT_BYTES")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(BULK_INGEST_WAL_CHECKPOINT_DEFAULT_BYTES)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BulkIngestWalCheckpointMode {
    Passive,
    Full,
    Restart,
    Truncate,
}

impl BulkIngestWalCheckpointMode {
    pub(super) fn from_env() -> Self {
        let Ok(raw) = dotenvy::var("CASS_INDEX_INGEST_WAL_CHECKPOINT_MODE") else {
            return Self::Truncate;
        };
        match raw.trim().to_ascii_uppercase().as_str() {
            "PASSIVE" => Self::Passive,
            "FULL" => Self::Full,
            "RESTART" => Self::Restart,
            "TRUNCATE" => Self::Truncate,
            invalid => {
                tracing::warn!(
                    value = invalid,
                    "invalid CASS_INDEX_INGEST_WAL_CHECKPOINT_MODE; using TRUNCATE"
                );
                Self::Truncate
            }
        }
    }

    const fn sql(self) -> &'static str {
        match self {
            Self::Passive => "PRAGMA wal_checkpoint(PASSIVE);",
            Self::Full => "PRAGMA wal_checkpoint(FULL);",
            Self::Restart => "PRAGMA wal_checkpoint(RESTART);",
            Self::Truncate => "PRAGMA wal_checkpoint(TRUNCATE);",
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Passive => "PASSIVE",
            Self::Full => "FULL",
            Self::Restart => "RESTART",
            Self::Truncate => "TRUNCATE",
        }
    }
}

/// Reset an oversized bulk-ingest WAL from a true single-connection state.
///
/// #425 showed that PASSIVE backfilled the complete WAL at every batch but
/// never reset its write position because the cached ephemeral writer kept
/// fsqlite's process-local connection count at two. Before checkpointing,
/// close that idle writer and execute one primary-connection read to bring
/// its MVCC commit clock past the writer's last commit. Then switch the
/// storage permanently to the primary writer and issue TRUNCATE (default).
/// Reopening a second writer after the reset would recreate both defects.
///
/// Only active in deferred-checkpoint (non-watch) mode. Failures remain
/// nonfatal, but a non-shrinking externally pinned WAL is not retried until
/// it grows by another threshold, avoiding an O(WAL) backfill every batch.
pub(in crate::indexer) fn maybe_checkpoint_bulk_ingest_wal(
    storage: &FrankenStorage,
    defer_checkpoints: bool,
) {
    if !defer_checkpoints {
        return;
    }
    let threshold = bulk_ingest_wal_checkpoint_threshold_bytes();
    if threshold == 0 {
        return;
    }
    let Ok(db_path) = storage.database_path() else {
        return;
    };
    let mut wal_path = db_path.clone().into_os_string();
    wal_path.push("-wal");
    let wal_bytes = match std::fs::metadata(std::path::Path::new(&wal_path)) {
        Ok(metadata) => metadata.len(),
        Err(_) => return,
    };
    let retry_at_bytes = storage.bulk_checkpoint_retry_at_bytes();
    if wal_bytes < threshold || (retry_at_bytes > 0 && wal_bytes < retry_at_bytes) {
        return;
    }

    let cached_writer_closed = match storage.close_idle_cached_ephemeral_writer() {
        Ok(closed) => closed,
        Err(err) => {
            tracing::warn!(
                db_path = %db_path.display(),
                wal_bytes,
                error = %err,
                "bulk ingest WAL reset skipped because the cached writer could not be closed"
            );
            storage.set_bulk_checkpoint_retry_at_bytes(wal_bytes.saturating_add(threshold));
            return;
        }
    };
    if storage.cached_ephemeral_writer_in_use() {
        tracing::warn!(
            db_path = %db_path.display(),
            wal_bytes,
            "bulk ingest WAL reset reached a non-quiescent writer boundary; deferring instead of checkpointing with two live connections"
        );
        storage.set_bulk_checkpoint_retry_at_bytes(wal_bytes.saturating_add(threshold));
        return;
    }
    // fsqlite #384 / cass #425: after the cached writer committed, the
    // long-lived primary may still hold an older pager clock. Any real
    // primary read refreshes it. TRUNCATE without this read can make every
    // later write fail BusySnapshot even though the checkpoint succeeded.
    if let Err(err) = storage
        .raw()
        .query("SELECT value FROM meta WHERE key = 'schema_version';")
    {
        tracing::warn!(
            db_path = %db_path.display(),
            wal_bytes,
            cached_writer_closed,
            error = %err,
            "bulk ingest WAL reset skipped because the primary MVCC clock could not be refreshed"
        );
        storage.set_bulk_checkpoint_retry_at_bytes(wal_bytes.saturating_add(threshold));
        return;
    }

    // This transition is intentionally one-way for the lifetime of the
    // storage handle. A new writer opened after the reset can inherit the
    // pre-reset clock and raises the process-local connection count again.
    storage.enable_bulk_single_connection();
    let mode = BulkIngestWalCheckpointMode::from_env();
    let started = std::time::Instant::now();
    match storage.raw().query(mode.sql()) {
        Ok(rows) => {
            let status = rows.first().map(|row| {
                (
                    row.get_typed::<i64>(0).unwrap_or(-1),
                    row.get_typed::<i64>(1).unwrap_or(-1),
                    row.get_typed::<i64>(2).unwrap_or(-1),
                )
            });
            let (busy, log_frames, checkpointed_frames) = status.unwrap_or((-1, -1, -1));
            let wal_bytes_after = match std::fs::metadata(std::path::Path::new(&wal_path)) {
                Ok(metadata) => metadata.len(),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => 0,
                Err(err) => {
                    tracing::warn!(
                        db_path = %db_path.display(),
                        error = %err,
                        "could not measure the bulk-ingest WAL after checkpoint; conservatively treating it as unshrunk"
                    );
                    wal_bytes
                }
            };
            let shrank = wal_bytes_after < wal_bytes;
            storage.set_bulk_checkpoint_retry_at_bytes(if shrank {
                0
            } else {
                wal_bytes.saturating_add(threshold)
            });
            if shrank {
                tracing::info!(
                    db_path = %db_path.display(),
                    wal_bytes_before = wal_bytes,
                    wal_bytes_after,
                    threshold_bytes = threshold,
                    checkpoint_mode = mode.label(),
                    cached_writer_closed,
                    busy,
                    log_frames,
                    checkpointed_frames,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "bulk ingest WAL checkpoint reset the WAL (#320/#425 memory bound)"
                );
            } else {
                tracing::warn!(
                    db_path = %db_path.display(),
                    wal_bytes_before = wal_bytes,
                    wal_bytes_after,
                    threshold_bytes = threshold,
                    checkpoint_mode = mode.label(),
                    cached_writer_closed,
                    busy,
                    log_frames,
                    checkpointed_frames,
                    retry_at_bytes = storage.bulk_checkpoint_retry_at_bytes(),
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "bulk ingest WAL checkpoint did not shrink the WAL; an external reader may be pinning it"
                );
            }
        }
        Err(err) => {
            storage.set_bulk_checkpoint_retry_at_bytes(wal_bytes.saturating_add(threshold));
            tracing::warn!(
                db_path = %db_path.display(),
                wal_bytes,
                checkpoint_mode = mode.label(),
                cached_writer_closed,
                retry_at_bytes = storage.bulk_checkpoint_retry_at_bytes(),
                error = %err,
                "bulk ingest WAL checkpoint failed; retry deferred until the WAL grows by another threshold"
            );
        }
    }
}

pub(in crate::indexer) fn apply_index_writer_checkpoint_policy(
    storage: &FrankenStorage,
    defer_checkpoints: bool,
) {
    let wal_autocheckpoint_pages = index_writer_wal_autocheckpoint_pages(defer_checkpoints);
    if storage.index_writer_checkpoint_pages() == Some(wal_autocheckpoint_pages) {
        return;
    }
    let pragma = format!("PRAGMA wal_autocheckpoint = {wal_autocheckpoint_pages};");
    if let Err(err) = storage.raw().execute(&pragma) {
        tracing::debug!(
            wal_autocheckpoint_pages,
            error = %err,
            "failed_to_apply_index_writer_checkpoint_policy"
        );
    } else {
        storage.mark_index_writer_checkpoint_pages(wal_autocheckpoint_pages);
    }
}

pub(in crate::indexer) fn with_ephemeral_writer<T, F>(
    storage: &FrankenStorage,
    defer_checkpoints: bool,
    context: &str,
    f: F,
) -> Result<T>
where
    F: FnOnce(&FrankenStorage) -> Result<T>,
{
    let db_path = storage
        .database_path()
        .with_context(|| format!("resolving database path for {context}"))?;
    // Keep the long-lived handle's connection-local checkpoint state aligned
    // with the short-lived writer so watch-mode observability and follow-up
    // policy transitions still reflect the active ingestion mode.
    apply_index_writer_checkpoint_policy(storage, defer_checkpoints);

    if storage.bulk_single_connection_enabled() {
        // Preflight is a write too; tune it before entering the retryable operation.
        apply_index_writer_busy_timeout(storage);
        if !storage.ephemeral_writer_preflight_verified() {
            with_concurrent_retry(SERIAL_CHUNK_CONTENTION_RETRIES, || {
                storage
                    .raw()
                    .execute("UPDATE meta SET value = value WHERE key = 'schema_version'")
                    .with_context(|| {
                        format!(
                            "primary writer preflight failed for {context} after the bulk WAL reset at {}",
                            db_path.display()
                        )
                    })
            })?;
            storage.mark_ephemeral_writer_preflight_verified();
        }
        apply_index_writer_checkpoint_policy(storage, defer_checkpoints);
        if let Err(err) = storage.raw().execute("PRAGMA foreign_keys = OFF") {
            tracing::debug!(
                error = %err,
                context,
                "failed to disable FK enforcement on primary single-connection writer"
            );
        }
        return f(storage);
    }

    // Writer acquisition and the idempotent preflight happen before any
    // canonical transaction. Short engine waits must not turn a transient
    // cold-open/preflight conflict into a fatal ingest failure (GH473).
    // Retry only setup: replaying f or close here could lose the outcomes
    // of already-committed chunks and therefore skip lexical publication.
    let (writer, reusable) = with_concurrent_retry(SERIAL_CHUNK_CONTENTION_RETRIES, || {
        let (writer, reusable) = storage.acquire_cached_ephemeral_writer().with_context(|| {
            format!(
                "opening short-lived frankensqlite writer for {context}: {}",
                db_path.display()
            )
        })?;

        let discard_writer = |mut writer: FrankenStorage| {
            if reusable {
                storage.discard_cached_ephemeral_writer(writer);
            } else {
                writer.close_best_effort_in_place();
            }
        };

        // A cold/reused writer must not inherit a long timeout for its first write.
        apply_index_writer_busy_timeout(&writer);
        if !storage.ephemeral_writer_preflight_verified() {
            // CASS #162 item 2: Preflight write check to catch "attempt to write
            // a readonly database" early with a clear diagnostic instead of letting
            // it surface deep inside a batched insert. Once one writer open has
            // succeeded on this storage handle, repeating the same idempotent
            // meta-table write on every steady-state persist just adds fixed
            // overhead without improving correctness.
            if let Err(err) = writer
                .raw()
                .execute("UPDATE meta SET value = value WHERE key = 'schema_version'")
            {
                discard_writer(writer);
                // Preserve the typed engine error for outer retry/fallback classification.
                return Err(anyhow::Error::new(err).context(format!(
                    "ephemeral writer preflight write failed for {context} at {}. \
                     The database may be locked by another process or opened in \
                     readonly mode. Try closing other cass instances and retrying.",
                    db_path.display()
                )));
            }
            storage.mark_ephemeral_writer_preflight_verified();
        }

        Ok((writer, reusable))
    })?;

    let release_writer = |writer: FrankenStorage| -> Result<()> {
        if reusable {
            storage.release_cached_ephemeral_writer(writer);
            Ok(())
        } else {
            writer.close().with_context(|| {
                format!(
                    "closing short-lived frankensqlite writer for {context}: {}",
                    db_path.display()
                )
            })
        }
    };

    apply_index_writer_checkpoint_policy(&writer, defer_checkpoints);

    // CASS #169: Disable database-level FK enforcement on ephemeral writers.
    // All insert paths already guarantee FK parent records exist at the
    // application level (ensure_agent, ensure_workspace,
    // ensure_embedded_source_registered), so SQLite FK checks are redundant.
    // After sustained write activity (~39 min), frankensqlite's cursor cache
    // can report false-positive FK constraint violations that PRAGMA
    // foreign_key_check confirms do not actually exist.
    if let Err(err) = writer.raw().execute("PRAGMA foreign_keys = OFF") {
        tracing::debug!(
            error = %err,
            context,
            "failed to disable FK enforcement on ephemeral writer"
        );
    }

    let result = f(&writer);

    match result {
        Ok(value) => {
            release_writer(writer)?;
            Ok(value)
        }
        Err(err) => {
            if let Err(close_err) = release_writer(writer) {
                tracing::warn!(
                    error = %close_err,
                    db_path = %db_path.display(),
                    context,
                    "failed to close short-lived writer cleanly after write error"
                );
            }
            Err(err)
        }
    }
}

fn transient_franken_error(err: &anyhow::Error) -> Option<&FrankenError> {
    err.downcast_ref::<FrankenError>()
        .or_else(|| err.root_cause().downcast_ref::<FrankenError>())
}

pub(super) fn is_retryable_franken_error(err: &anyhow::Error) -> bool {
    transient_franken_error(err).is_some_and(|inner| {
        // DatabaseCorrupt is intentionally NOT retried here — retrying on
        // corruption can amplify damage by hammering corrupt pages and
        // spreading partial writes through the WAL.  Let it fail fast so
        // the caller can trigger the backup/quarantine path instead.
        matches!(
            inner,
            FrankenError::Busy
                | FrankenError::BusyRecovery
                | FrankenError::BusySnapshot { .. }
                | FrankenError::WriteConflict { .. }
                | FrankenError::SerializationFailure { .. }
        )
    })
}

/// Bounded retries for one serial persist chunk on retryable engine
/// contention (GH #401/#406). With the 4ms→256ms doubling jittered
/// backoff in [`with_concurrent_retry`] this is roughly 1.5s of total
/// waiting before the conflict is surfaced as exit 7 — enough to ride
/// out a peer epoch unwind, short enough that a real lock holder still
/// fails fast.
pub(in crate::indexer) const SERIAL_CHUNK_CONTENTION_RETRIES: usize = 8;

/// Retry wrapper for any retryable FrankenError (BusySnapshot, WriteConflict, etc.)
pub(in crate::indexer) fn with_concurrent_retry<F, T>(max_retries: usize, mut f: F) -> Result<T>
where
    F: FnMut() -> Result<T>,
{
    let mut rng = rand::rng();
    let mut backoff_ms = 4_u64;
    for attempt in 0..=max_retries {
        match f() {
            Ok(val) => return Ok(val),
            Err(err) if attempt < max_retries && is_retryable_franken_error(&err) => {
                let sleep_ms = backoff_ms + rng.random_range(0..=backoff_ms);
                tracing::debug!(
                    target: "cass::indexer::persist",
                    attempt = attempt + 1,
                    max_retries,
                    backoff_ms = sleep_ms,
                    error = %err,
                    "begin_concurrent_retry"
                );
                std::thread::sleep(Duration::from_millis(sleep_ms));
                backoff_ms = (backoff_ms * 2).min(256);
            }
            Err(err) => return Err(err),
        }
    }
    Err(anyhow!("exhausted begin-concurrent retries"))
}

enum ChunkPersistResult {
    Completed(Vec<(usize, InsertOutcome)>),
    RetryableFallback {
        completed: Vec<(usize, InsertOutcome)>,
        remaining_range: Range<usize>,
        error: anyhow::Error,
    },
}

fn persist_chunk_with_writer(
    franken: &FrankenStorage,
    base_idx: usize,
    chunk: &[NormalizedConversation],
    internal_chunk: &[Conversation],
    max_retries: usize,
) -> Result<ChunkPersistResult> {
    debug_assert_eq!(
        chunk.len(),
        internal_chunk.len(),
        "parallel pre-map must produce one Conversation per NormalizedConversation"
    );
    let mut outcomes = Vec::with_capacity(chunk.len());
    let mut agent_cache: HashMap<String, i64> = HashMap::new();
    let mut workspace_cache: HashMap<std::path::PathBuf, i64> = HashMap::new();

    for (offset, (conv, internal)) in chunk.iter().zip(internal_chunk.iter()).enumerate() {
        let idx = base_idx + offset;

        // Wrap the entire ensure_agent + ensure_workspace +
        // insert_conversation_tree sequence in the retry loop, since
        // those writes can all hit page conflicts under begin-concurrent.
        let agent_slug = conv.agent_slug.clone();
        let workspace = conv.workspace.clone();

        match with_concurrent_retry(max_retries, || {
            let agent_id = if let Some(id) = agent_cache.get(&agent_slug) {
                *id
            } else {
                let agent = Agent {
                    id: None,
                    slug: agent_slug.clone(),
                    name: agent_slug.clone(),
                    version: None,
                    kind: AgentKind::Cli,
                };
                let id = franken.ensure_agent(&agent)?;
                agent_cache.insert(agent_slug.clone(), id);
                id
            };
            let workspace_id = if let Some(ws) = &workspace {
                if let Some(id) = workspace_cache.get(ws) {
                    Some(*id)
                } else {
                    let id = franken.ensure_workspace(ws, None)?;
                    workspace_cache.insert(ws.clone(), id);
                    Some(id)
                }
            } else {
                None
            };
            let outcome = franken.insert_conversation_tree(agent_id, workspace_id, internal)?;
            // One coherent extension generation (facts + structural
            // projection + durable viewer_meta receipt) rides the same
            // ingest pass — one transaction, receipt and rows land
            // together, or a durable failed mark the backfill lane
            // reconciles.
            crate::viewer_extensions::projection::publish_ingest_generation_best_effort(
                franken.raw(),
                internal,
                outcome.conversation_id,
            );
            // Task 11: post-commit change-journal notify — canonical
            // events were already journaled by the observer triggers
            // inside the ingest commit; this coalesces them and stamps
            // canonical_ready (SQLite journal is truth, IPC is wakeup-only).
            crate::viewer_extensions::changes::notify_committed_change_best_effort(franken.raw());
            // Task 12: wake any bridge event listener on this archive.
            // Wakeup-only — a missed wake only delays the poll fallback.
            if let Ok(db_path) = franken.database_path() {
                crate::viewer_extensions::changes::send_bridge_wakeup_best_effort(&db_path);
            }
            Ok(outcome)
        }) {
            Ok(outcome) => outcomes.push((idx, outcome)),
            Err(err) if is_retryable_franken_error(&err) => {
                return Ok(ChunkPersistResult::RetryableFallback {
                    completed: outcomes,
                    remaining_range: idx..(base_idx + chunk.len()),
                    error: err,
                });
            }
            Err(err) => return Err(err),
        }
    }

    Ok(ChunkPersistResult::Completed(outcomes))
}

fn persist_chunk_serial_fallback(
    db_path: &std::path::Path,
    base_idx: usize,
    chunk: &[NormalizedConversation],
    internal_chunk: &[Conversation],
    max_retries: usize,
    defer_checkpoints: bool,
) -> Result<Vec<(usize, InsertOutcome)>> {
    let franken = FrankenStorage::open_writer(db_path).with_context(|| {
        format!(
            "opening frankensqlite writer for begin-concurrent serial fallback: {}",
            db_path.display()
        )
    })?;
    apply_begin_concurrent_writer_tuning(&franken, defer_checkpoints);
    // CASS #169: Disable FK enforcement — see with_ephemeral_writer for rationale.
    if let Err(err) = franken.raw().execute("PRAGMA foreign_keys = OFF") {
        tracing::debug!(
            error = %err,
            "failed to disable FK enforcement on serial fallback writer"
        );
    }
    let fallback_retries = max_retries.max(12);
    let result =
        persist_chunk_with_writer(&franken, base_idx, chunk, internal_chunk, fallback_retries);
    let close_result = franken.close().with_context(|| {
        format!(
            "closing frankensqlite writer for begin-concurrent serial fallback: {}",
            db_path.display()
        )
    });

    match result {
        Ok(ChunkPersistResult::Completed(outcomes)) => {
            close_result?;
            Ok(outcomes)
        }
        Ok(ChunkPersistResult::RetryableFallback {
            completed,
            remaining_range,
            error,
        }) => {
            if let Err(close_err) = close_result {
                tracing::warn!(
                    error = %close_err,
                    db_path = %db_path.display(),
                    "failed to close serial fallback writer cleanly after retry exhaustion"
                );
            }
            ordered_bail_serial_fallback(completed.len(), remaining_range, error)
        }
        Err(err) => {
            if let Err(close_err) = close_result {
                tracing::warn!(
                    error = %close_err,
                    db_path = %db_path.display(),
                    "failed to close serial fallback writer cleanly after index error"
                );
            }
            Err(err)
        }
    }
}

fn ordered_bail_serial_fallback(
    completed: usize,
    remaining_range: Range<usize>,
    error: anyhow::Error,
) -> Result<Vec<(usize, InsertOutcome)>> {
    Err(anyhow!(
        "begin-concurrent serial fallback exhausted retryable conflicts after persisting {completed} conversations; remaining range {}..{}: {error}",
        remaining_range.start,
        remaining_range.end
    ))
}

pub(super) fn duplicate_conversation_keys_present(convs: &[NormalizedConversation]) -> bool {
    let mut seen = HashSet::with_capacity(convs.len());
    for conv in convs {
        let (source_id, _) = extract_provenance(&conv.metadata);
        let key = if let Some(external_id) = conv.external_id.as_deref() {
            (
                conv.agent_slug.clone(),
                source_id,
                Some(external_id.to_owned()),
                None,
                conv.started_at,
            )
        } else {
            (
                conv.agent_slug.clone(),
                source_id,
                None,
                Some(conv.source_path.to_string_lossy().to_string()),
                None,
            )
        };
        if !seen.insert(key) {
            return true;
        }
    }
    false
}

#[allow(clippy::too_many_arguments)]
pub(super) fn persist_conversations_batched_begin_concurrent(
    storage: &FrankenStorage,
    db_path: &Path,
    mut t_index: Option<&mut TantivyIndex>,
    convs: &[NormalizedConversation],
    lexical_strategy: LexicalPopulationStrategy,
    defer_checkpoints: bool,
    capture_semantic_delta: bool,
    raw_mirror_data_dir: Option<&Path>,
    heartbeat: PersistHeartbeat<'_>,
) -> Result<PersistBatchOutcome> {
    if lexical_population_strategy_requires_inline_tantivy(lexical_strategy) && t_index.is_none() {
        anyhow::bail!(
            "begin-concurrent batched persistence requires a Tantivy writer for {}",
            lexical_strategy.as_str()
        );
    }
    let max_retries = begin_concurrent_retry_limit();
    let chunk_size = begin_concurrent_chunk_size().min(convs.len().max(1));

    // Pre-compute internal conversations for the whole batch before we
    // enter the per-chunk writer loops. map_to_internal is pure, CPU-heavy
    // (string clones + optional secret redaction), and independent across
    // conversations. Running it up front on the rayon pool pulls the
    // allocator work out of every writer's retry window — so conflict
    // retries re-run only SQLite I/O, not the allocation cost. See the
    // matching hoist in the serial persist_conversations_batched path.
    let internal_convs: Vec<Conversation> = map_batch_to_internal(convs, heartbeat);

    let indexed_chunks: Vec<Result<ChunkPersistResult>> = convs
        .par_chunks(chunk_size)
        .enumerate()
        .map(|(chunk_idx, chunk)| {
            let base_idx = chunk_idx * chunk_size;
            let internal_chunk = &internal_convs[base_idx..base_idx + chunk.len()];
            // Card 1 / Silo shadow observer — observes only, NO commit
            // semantics change. When CASS_INDEXER_PARALLEL_WAL=shadow is
            // set, this records per-chunk wall-clock so future sessions
            // can compare what a parallel-WAL coordinator would have
            // decided. Explicit `off` mode returns None; default shadow
            // mode records a tiny bounded evidence manifest.
            let shadow_guard =
                crate::indexer::parallel_wal_shadow::start_chunk(chunk_idx, base_idx, chunk.len());
            let franken = FrankenStorage::open_writer(db_path).with_context(|| {
                format!(
                    "opening frankensqlite writer for begin-concurrent mode: {}",
                    db_path.display()
                )
            })?;
            apply_begin_concurrent_writer_tuning(&franken, defer_checkpoints);
            // CASS #169: Disable FK enforcement — see with_ephemeral_writer for rationale.
            if let Err(err) = franken.raw().execute("PRAGMA foreign_keys = OFF") {
                tracing::debug!(
                    error = %err,
                    chunk_idx,
                    "failed to disable FK enforcement on begin-concurrent writer"
                );
            }
            let result =
                persist_chunk_with_writer(&franken, base_idx, chunk, internal_chunk, max_retries);
            let close_result = franken.close().with_context(|| {
                format!(
                    "closing frankensqlite writer for begin-concurrent mode: {}",
                    db_path.display()
                )
            });
            match result {
                Ok(outcomes) => {
                    close_result?;
                    if let Some(g) = shadow_guard {
                        g.finish_ok();
                    }
                    // gh373/oeu5a: each persisted chunk is live work even
                    // while the batch-level `current` bump waits for the
                    // whole persist call to return.
                    heartbeat.tick();
                    Ok(outcomes)
                }
                Err(err) => {
                    if let Err(close_err) = close_result {
                        tracing::warn!(
                            error = %close_err,
                            db_path = %db_path.display(),
                            "failed to close begin-concurrent writer cleanly after index error"
                        );
                    }
                    if let Some(g) = shadow_guard {
                        g.finish_err();
                    }
                    Err(err)
                }
            }
        })
        .collect();

    let mut ordered = Vec::with_capacity(convs.len());
    let mut fallback_ranges = Vec::new();
    for chunk in indexed_chunks {
        match chunk? {
            ChunkPersistResult::Completed(outcomes) => ordered.extend(outcomes),
            ChunkPersistResult::RetryableFallback {
                completed,
                remaining_range,
                error,
            } => {
                tracing::warn!(
                    error = %error,
                    completed = completed.len(),
                    remaining = remaining_range.len(),
                    start = remaining_range.start,
                    end = remaining_range.end,
                    "begin-concurrent chunk exhausted retryable conflicts; falling back to serial replay"
                );
                ordered.extend(completed);
                fallback_ranges.push(remaining_range);
            }
        }
    }

    for remaining_range in fallback_ranges {
        let fallback_outcomes = persist_chunk_serial_fallback(
            db_path,
            remaining_range.start,
            &convs[remaining_range.clone()],
            &internal_convs[remaining_range.clone()],
            max_retries,
            defer_checkpoints,
        )?;
        ordered.extend(fallback_outcomes);
    }
    ordered.sort_by_key(|(idx, _)| *idx);
    if let Some(data_dir) = raw_mirror_data_dir {
        record_persisted_raw_mirror_db_link_groups(
            data_dir,
            ordered
                .iter()
                .filter_map(|(idx, outcome)| convs.get(*idx).map(|conv| (conv, outcome))),
        );
    }

    let defer_lexical_updates = defer_lexical_updates_enabled();
    let mut batch_outcome = PersistBatchOutcome::default();

    let rebuild_inline = !ordered.is_empty()
        && !defer_lexical_updates
        && lexical_strategy == LexicalPopulationStrategy::InlineRebuildFromScan;
    if rebuild_inline {
        // Quill resolves replacement identities against published state.
        // Publish any preceding incremental additions once at this batch
        // boundary, rather than sealing every canonical conversation.
        // The same commit seals region documents that preceding batch
        // staged — flush its queued marks stamps `published` with it.
        t_index
            .as_deref_mut()
            .expect("inline rebuild requires Tantivy writer")
            .commit_with_region_doc_stamps(storage.raw())?;
    }
    let mut rebuilt_conversation_ids = HashSet::new();
    let mut skip_inline_lexical_updates = false;
    // Task 5 region-doc prelude: stage every admitted session's region
    // documents — including the pending-mark convergence sweep — BEFORE
    // any canonical staging. Quill's `upsert` publishes pending work
    // when a re-staged identity already lives in the committed snapshot
    // (a republish), so a region upsert issued after canonical staging
    // would flush the batch mid-generation: a failed outer commit would
    // then leave canonical docs searchable without their publication
    // receipt. Region-only early commits stay honest — marks remain
    // `pending` and session reads gate on them until the post-commit
    // stamp. The per-conversation hooks inside the loop dedupe.
    //
    // Whole-archive republish deferral: when the run latched the writer
    // (a REGION_VERSION bump left more stale marks than the deferral
    // threshold), this prelude stages NOTHING — the lane converges the
    // accumulated pending set once, bounded, right before its final
    // commit. Staging here per batch would publish one successor
    // segment per batch (the streaming limits make every oversized
    // session its own batch) and re-validate the whole archive each
    // time; the marks stay `pending` in between, which is exactly the
    // state the final drain and any crash recovery re-converge.
    if !defer_lexical_updates && let Some(index) = t_index.as_deref_mut() {
        let defer_region_docs = lexical_strategy == LexicalPopulationStrategy::IncrementalInline
            && index.region_doc_convergence_deferred_to_final_drain();
        if !defer_region_docs {
            index.converge_pending_search_region_docs(storage.raw(), 4096)?;
            for (_, outcome) in &ordered {
                stage_persisted_region_docs(
                    index,
                    storage.raw(),
                    outcome.conversation_id,
                    lexical_strategy,
                )?;
            }
        }
    }
    for (_, outcome) in ordered {
        batch_outcome.record_insert_outcome(&outcome);
        if defer_lexical_updates || skip_inline_lexical_updates {
            // The prelude may already have staged this session's region
            // docs; rescind the queued stamp so the persisted mark stays
            // `pending` with the canonical debt instead of publishing
            // region docs without their messages.
            if let Some(index) = t_index.as_deref_mut() {
                index.unstage_search_region_docs(outcome.conversation_id);
            }
            if capture_semantic_delta {
                let (inputs, max_message_id) =
                    packet_semantic_delta_for_outcome(storage, &outcome)?;
                batch_outcome.extend_semantic_delta(inputs, max_message_id);
            }
            continue;
        }

        // ibuuh.32 / 5b9p0: route the begin-concurrent lexical
        // sink through the packet pipeline. Same shape as the
        // serial batched path above; the regression test
        // persist_packet_pipeline_matches_legacy_for_incremental_inline
        // pins both paths produce identical CassDocuments.
        match lexical_strategy {
            // Targeted watch recovery replays lexical packets after this
            // batch, but still needs the actual semantic mutation delta.
            LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild => {}
            LexicalPopulationStrategy::InlineRebuildFromScan => {
                // All outcomes are already persisted, so the first read
                // for this identity contains its final canonical snapshot.
                if rebuilt_conversation_ids.insert(outcome.conversation_id) {
                    replay_canonical_conversation_for_rebuild(
                        storage,
                        t_index
                            .as_deref_mut()
                            .expect("inline rebuild requires Tantivy writer"),
                        outcome.conversation_id,
                    )?;
                    // Task 5: region documents ride the same writer —
                    // the persisted region rows are already committed
                    // by the generation publication above. Staging
                    // only queues the marks stamp; `published` lands
                    // after this writer's commit.
                    t_index
                        .as_deref_mut()
                        .expect("inline rebuild requires Tantivy writer")
                        .stage_search_region_docs(storage.raw(), outcome.conversation_id)?;
                }
            }
            LexicalPopulationStrategy::IncrementalInline => {
                let changed_indices = changed_message_indices(&outcome);
                if !changed_indices.is_empty() {
                    let packet = lexical_packet_for_changed_messages(
                        storage,
                        outcome.conversation_id,
                        &changed_indices,
                    )?;
                    let positional = positional_indices_for_inserted(&packet, &changed_indices);
                    if !positional.is_empty() {
                        let add_result = if should_inject_incremental_lexical_update_oom() {
                            Err(anyhow::anyhow!("out of memory"))
                        } else {
                            publish_changed_packet_messages(
                                t_index
                                    .as_deref_mut()
                                    .expect("incremental inline updates require Tantivy writer"),
                                &packet,
                                &positional,
                                outcome.conversation_id,
                                !outcome.updated_indices.is_empty(),
                            )
                        };
                        if let Err(error) = add_result {
                            if should_defer_incremental_lexical_update_after_error(&error) {
                                batch_outcome.record_deferred_lexical_update(&error);
                                skip_inline_lexical_updates = true;
                                // The prelude already staged this
                                // session's region docs; rescind its
                                // queued stamp so the mark stays
                                // `pending` with the canonical debt.
                                if let Some(index) = t_index.as_deref_mut() {
                                    index.unstage_search_region_docs(outcome.conversation_id);
                                }
                                tracing::warn!(
                                    error = %error,
                                    "incremental lexical update ran out of memory; preserving SQLite ingest and deferring lexical repair"
                                );
                            } else {
                                return Err(error);
                            }
                        } else {
                            // Task 5: region documents converge with
                            // the message documents — the generation
                            // publication already committed the new
                            // region inventory for this session.
                            // Staging only queues the marks stamp;
                            // `published` lands after this writer's
                            // commit.
                            //
                            // Whole-archive republish deferral: skip
                            // this hook too — per-outcome staging here
                            // would defeat the latched writer's
                            // accumulate-then-drain discipline one
                            // session at a time. The final drain
                            // converges this session with every other
                            // pending one.
                            let region_docs_deferred = t_index.as_ref().is_some_and(|index| {
                                index.region_doc_convergence_deferred_to_final_drain()
                            });
                            if !region_docs_deferred {
                                stage_persisted_region_docs(
                                    t_index.as_deref_mut().expect(
                                        "incremental inline updates require Tantivy writer",
                                    ),
                                    storage.raw(),
                                    outcome.conversation_id,
                                    lexical_strategy,
                                )?;
                            }
                        }
                    }
                }
            }
        }

        if capture_semantic_delta {
            let (inputs, max_message_id) = packet_semantic_delta_for_outcome(storage, &outcome)?;
            batch_outcome.extend_semantic_delta(inputs, max_message_id);
        }
    }
    // The pending-mark convergence sweep runs in the pre-canonical
    // prelude above: sessions whose region inventory was republished
    // but whose Quill region documents are still pending reconcile
    // inside this index run. Sessions whose canonical staging was
    // skipped (flag or mid-batch OOM) had their stamps rescinded in
    // the loop and keep `pending` marks with the canonical debt.
    if rebuild_inline {
        // The commit seals the staged region documents; the queued
        // marks stamps land `published` only after it succeeds —
        // never on documents a failed commit could have dropped.
        t_index
            .expect("inline rebuild requires Tantivy writer")
            .commit_with_region_doc_stamps(storage.raw())?;
    }

    Ok(batch_outcome)
}
