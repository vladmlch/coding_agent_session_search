use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result};

use crate::franken_sync::compat::{ConnectionExt, ParamValue, RowExt};
use crate::storage::sqlite::FrankenStorage;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LexicalRebuildDbState {
    pub(crate) db_path: String,
    pub(crate) total_conversations: usize,
    #[serde(default)]
    pub(crate) total_messages: usize,
    pub(crate) storage_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub(crate) struct LexicalRebuildCheckpoint {
    pub db_path: String,
    pub total_conversations: usize,
    pub storage_fingerprint: String,
    pub committed_offset: i64,
    pub committed_conversation_id: Option<i64>,
    pub processed_conversations: usize,
    pub indexed_docs: usize,
    /// Task 5: the share of `indexed_docs` that is search-region
    /// documents; `indexed_docs - indexed_region_docs` is the canonical
    /// message count.
    pub indexed_region_docs: usize,
    pub schema_hash: String,
    pub page_size: i64,
    pub completed: bool,
    pub updated_at_ms: i64,
}

pub(crate) fn lexical_rebuild_state_path(index_path: &Path) -> PathBuf {
    index_path.join(".lexical-rebuild-state.json")
}

#[allow(dead_code)]
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredPendingLexicalCommit {
    next_offset: i64,
    #[serde(default)]
    next_conversation_id: Option<i64>,
    processed_conversations: usize,
    indexed_docs: usize,
    base_meta_fingerprint: Option<String>,
    #[serde(default)]
    region_docs: usize,
}

#[allow(dead_code)]
#[derive(Default, serde::Deserialize)]
#[serde(default)]
struct StoredLexicalRebuildPipelineRuntimeSnapshot {
    queue_depth: usize,
    inflight_message_bytes: usize,
    max_message_bytes_in_flight: usize,
    pending_batch_conversations: usize,
    pending_batch_message_bytes: usize,
    page_prep_workers: usize,
    active_page_prep_jobs: usize,
    ordered_buffered_pages: usize,
    budget_generation: usize,
    producer_budget_wait_count: usize,
    producer_budget_wait_ms: usize,
    #[serde(skip)]
    producer_budget_waiter_count: usize,
    #[serde(skip)]
    producer_state: String,
    #[serde(skip)]
    reservation_next_sequence: u64,
    producer_handoff_wait_count: usize,
    producer_handoff_wait_ms: usize,
    host_loadavg_1m_milli: Option<u32>,
    host_available_memory_bytes: Option<u64>,
    process_rss_bytes: Option<u64>,
    controller_mode: String,
    controller_reason: String,
    staged_merge_workers_max: usize,
    staged_merge_allowed_jobs: usize,
    staged_merge_active_jobs: usize,
    staged_merge_ready_artifacts: usize,
    staged_merge_ready_groups: usize,
    staged_merge_controller_reason: String,
    staged_shard_build_workers_max: usize,
    staged_shard_build_allowed_jobs: usize,
    staged_shard_build_active_jobs: usize,
    staged_shard_build_pending_jobs: usize,
    staged_shard_build_controller_reason: String,
    staged_shard_build_memory_reserve_bytes: usize,
    staged_shard_build_emergency_memory_reserve_bytes: usize,
    staged_shard_build_completed_jobs: usize,
    staged_shard_build_last_shard_index: Option<usize>,
    staged_shard_build_last_message_bytes: usize,
    staged_shard_build_last_index_size_bytes: u64,
    staged_shard_build_last_duration_ms: u64,
    staged_shard_build_last_amplification_milli: Option<u64>,
    staged_shard_build_observed_amplification_milli: Option<u64>,
    updated_at_ms: i64,
}

#[allow(dead_code)]
#[derive(Default, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum StoredLexicalRebuildExecutionMode {
    #[default]
    SharedWriter,
    StagedSingleIndex,
    LiveSingleIndex,
    StagedShardBuild,
    CanonicalMetadataRepair,
}

#[allow(dead_code)]
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredLexicalRebuildState {
    version: u8,
    schema_hash: String,
    db: LexicalRebuildDbState,
    page_size: i64,
    committed_offset: i64,
    #[serde(default)]
    committed_conversation_id: Option<i64>,
    processed_conversations: usize,
    indexed_docs: usize,
    #[serde(default)]
    indexed_region_docs: usize,
    committed_meta_fingerprint: Option<String>,
    pending: Option<StoredPendingLexicalCommit>,
    completed: bool,
    updated_at_ms: i64,
    #[serde(default)]
    execution_mode: Option<StoredLexicalRebuildExecutionMode>,
    #[serde(default)]
    runtime: StoredLexicalRebuildPipelineRuntimeSnapshot,
}

pub(crate) fn load_lexical_rebuild_checkpoint(
    index_path: &Path,
) -> Result<Option<LexicalRebuildCheckpoint>> {
    let path = lexical_rebuild_state_path(index_path);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(err)
                .with_context(|| format!("reading lexical rebuild state {}", path.display()));
        }
    };

    let state = match serde_json::from_slice::<StoredLexicalRebuildState>(&bytes) {
        Ok(state) => state,
        Err(err) => {
            tracing::warn!(
                path = %path.display(),
                error = %err,
                "ignoring malformed lexical rebuild checkpoint"
            );
            return Ok(None);
        }
    };

    let processed_conversations = state
        .pending
        .as_ref()
        .map(|pending| pending.processed_conversations)
        .unwrap_or(state.processed_conversations);
    let committed_conversation_id = state
        .pending
        .as_ref()
        .and_then(|pending| pending.next_conversation_id)
        .or(state.committed_conversation_id);
    let indexed_docs = state
        .pending
        .as_ref()
        .map(|pending| pending.indexed_docs)
        .unwrap_or(state.indexed_docs);

    Ok(Some(LexicalRebuildCheckpoint {
        db_path: state.db.db_path,
        total_conversations: state.db.total_conversations,
        storage_fingerprint: state.db.storage_fingerprint,
        committed_offset: state.committed_offset,
        committed_conversation_id,
        processed_conversations,
        indexed_docs,
        indexed_region_docs: state.indexed_region_docs,
        schema_hash: state.schema_hash,
        page_size: state.page_size,
        completed: state.completed,
        updated_at_ms: state.updated_at_ms,
    }))
}

pub(crate) fn lexical_rebuild_content_fingerprint_value(
    total_conversations: usize,
    max_conversation_id: i64,
    max_message_id: i64,
) -> String {
    format!("content-v1:{total_conversations}:{max_conversation_id}:{max_message_id}")
}

pub(crate) fn lexical_rebuild_deferred_content_fingerprint(total_conversations: usize) -> String {
    format!("content-pending-v1:{total_conversations}")
}

pub(crate) fn completed_lexical_storage_fingerprint_matches_total(
    fingerprint: &str,
    expected_total_conversations: usize,
) -> bool {
    let mut parts = match fingerprint.strip_prefix("content-v1:") {
        Some(rest) => rest.split(':'),
        None => return false,
    };
    let parsed_total = parts.next().and_then(|part| part.parse::<usize>().ok());
    let max_conversation_id = parts.next().and_then(|part| part.parse::<i64>().ok());
    let max_message_id = parts.next().and_then(|part| part.parse::<i64>().ok());
    parsed_total == Some(expected_total_conversations)
        && max_conversation_id.is_some_and(|id| id >= 0)
        && max_message_id.is_some_and(|id| id >= 0)
        && parts.next().is_none()
}

pub(crate) fn lexical_rebuild_content_fingerprint(
    storage: &FrankenStorage,
    total_conversations: usize,
) -> Result<String> {
    let prep_profile = std::env::var_os("CASS_PREP_PROFILE").is_some();
    let conversations_started = Instant::now();
    let max_conversation_id: i64 = storage
        .raw()
        .query_row_map(
            "SELECT COALESCE(MAX(id), 0) FROM conversations",
            &[] as &[ParamValue],
            |row| row.get_typed(0),
        )
        .context("computing lexical rebuild conversation fingerprint")?;
    if prep_profile {
        eprintln!(
            "CASS_PREP_PROFILE step=fingerprint_conversations step_ms={}",
            conversations_started.elapsed().as_millis()
        );
    }
    let messages_started = Instant::now();
    let max_message_id: i64 = storage
        .raw()
        .query_row_map(
            "SELECT COALESCE(MAX(id), 0) FROM messages",
            &[] as &[ParamValue],
            |row| row.get_typed(0),
        )
        .context("computing lexical rebuild message fingerprint")?;
    if prep_profile {
        eprintln!(
            "CASS_PREP_PROFILE step=fingerprint_messages step_ms={}",
            messages_started.elapsed().as_millis()
        );
    }
    Ok(lexical_rebuild_content_fingerprint_value(
        total_conversations,
        max_conversation_id,
        max_message_id,
    ))
}

pub(crate) fn lexical_rebuild_storage_fingerprint(db_path: &Path) -> Result<String> {
    let mut storage = FrankenStorage::open_readonly(db_path).with_context(|| {
        format!(
            "opening readonly storage to compute lexical fingerprint for {}",
            db_path.display()
        )
    })?;
    let total_conversations = count_total_conversations_exact(&storage)?;
    let fingerprint = lexical_rebuild_content_fingerprint(&storage, total_conversations)?;
    storage.close_best_effort_in_place();
    Ok(fingerprint)
}

pub(crate) fn count_total_conversations_exact(storage: &FrankenStorage) -> Result<usize> {
    let total_conversations: i64 = storage
        .raw()
        .query_row_map(
            "SELECT COUNT(*) FROM conversations",
            &[] as &[ParamValue],
            |row| row.get_typed(0),
        )
        .context("counting canonical conversations for lexical rebuild state")?;
    Ok(usize::try_from(total_conversations.max(0)).unwrap_or(usize::MAX))
}

/// Render rowids as a literal SQL list (`1,2,3`) for an `IN (...)` over an
/// `INTEGER PRIMARY KEY`.
///
/// Integer literals avoid bind-variable count limits and allow direct
/// primary-key `IN` list seeks across SQLite hydration chunks at the full
/// chunk size. The ids come from cass's own index (never from user text)
/// and `i64`'s `Display` emits only an optional `-` and ASCII digits, so
/// embedding them cannot form SQL.
pub fn sql_rowid_literal_list(ids: &[i64]) -> String {
    let mut out = String::with_capacity(ids.len().saturating_mul(8));
    for (idx, id) in ids.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        // `write!` into a String cannot fail.
        let _ = write!(out, "{id}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_checkpoint_json() -> serde_json::Value {
        serde_json::json!({
            "version": 3,
            "schema_hash": "test-schema-hash",
            "db": {
                "db_path": "/tmp/archive/agent_search.db",
                "total_conversations": 12,
                "total_messages": 48,
                "storage_fingerprint": "content-v1:12:12:48"
            },
            "page_size": 1024,
            "committed_offset": 5,
            "committed_conversation_id": 50,
            "processed_conversations": 5,
            "indexed_docs": 20,
            "indexed_region_docs": 2,
            "committed_meta_fingerprint": "meta-fp-1",
            "pending": null,
            "completed": false,
            "updated_at_ms": 1_733_000_000_000_i64,
            "execution_mode": "staged_single_index",
            "runtime": {}
        })
    }

    #[test]
    fn load_lexical_rebuild_checkpoint_rejects_malformed_runtime_snapshot() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let state_path = lexical_rebuild_state_path(dir.path());

        let mut malformed_string = base_checkpoint_json();
        malformed_string["runtime"] = serde_json::json!("not-an-object");
        fs::write(&state_path, serde_json::to_vec(&malformed_string)?)?;
        assert_eq!(load_lexical_rebuild_checkpoint(dir.path())?, None);

        let mut malformed_number = base_checkpoint_json();
        malformed_number["runtime"] = serde_json::json!(42);
        fs::write(&state_path, serde_json::to_vec(&malformed_number)?)?;
        assert_eq!(load_lexical_rebuild_checkpoint(dir.path())?, None);

        let mut malformed_field = base_checkpoint_json();
        malformed_field["runtime"] = serde_json::json!({ "queue_depth": "bad" });
        fs::write(&state_path, serde_json::to_vec(&malformed_field)?)?;
        assert_eq!(load_lexical_rebuild_checkpoint(dir.path())?, None);

        let mut valid_default_runtime = base_checkpoint_json();
        valid_default_runtime["runtime"] = serde_json::json!({
            "queue_depth": 3,
            "controller_mode": "steady"
        });
        fs::write(&state_path, serde_json::to_vec(&valid_default_runtime)?)?;
        let loaded = load_lexical_rebuild_checkpoint(dir.path())?
            .expect("valid runtime snapshot should deserialize");
        assert_eq!(loaded.processed_conversations, 5);
        assert_eq!(loaded.committed_conversation_id, Some(50));
        assert_eq!(loaded.indexed_docs, 20);
        Ok(())
    }

    #[test]
    fn load_lexical_rebuild_checkpoint_applies_pending_commit_overlay() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let state_path = lexical_rebuild_state_path(dir.path());

        let mut with_pending_id = base_checkpoint_json();
        with_pending_id["pending"] = serde_json::json!({
            "next_offset": 9,
            "next_conversation_id": 99,
            "processed_conversations": 9,
            "indexed_docs": 37,
            "base_meta_fingerprint": "meta-fp-1",
            "region_docs": 4
        });
        fs::write(&state_path, serde_json::to_vec(&with_pending_id)?)?;
        let checkpoint = load_lexical_rebuild_checkpoint(dir.path())?
            .expect("checkpoint with pending commit should load");
        assert_eq!(checkpoint.processed_conversations, 9);
        assert_eq!(checkpoint.committed_conversation_id, Some(99));
        assert_eq!(checkpoint.indexed_docs, 37);
        assert_eq!(checkpoint.total_conversations, 12);
        assert_eq!(checkpoint.committed_offset, 5);
        assert_eq!(checkpoint.indexed_region_docs, 2);

        let mut without_pending_id = base_checkpoint_json();
        without_pending_id["pending"] = serde_json::json!({
            "next_offset": 8,
            "next_conversation_id": null,
            "processed_conversations": 8,
            "indexed_docs": 31,
            "base_meta_fingerprint": "meta-fp-1",
            "region_docs": 3
        });
        fs::write(&state_path, serde_json::to_vec(&without_pending_id)?)?;
        let fallback_checkpoint = load_lexical_rebuild_checkpoint(dir.path())?
            .expect("checkpoint with null pending conversation id should fall back");
        assert_eq!(fallback_checkpoint.processed_conversations, 8);
        assert_eq!(fallback_checkpoint.committed_conversation_id, Some(50));
        assert_eq!(fallback_checkpoint.indexed_docs, 31);
        assert_eq!(fallback_checkpoint.total_conversations, 12);
        assert_eq!(fallback_checkpoint.committed_offset, 5);
        assert_eq!(fallback_checkpoint.indexed_region_docs, 2);
        Ok(())
    }
}
