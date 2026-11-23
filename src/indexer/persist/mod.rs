mod batch_map;
mod canonical_replay;
mod concurrent;
#[cfg(test)]
#[path = "tests/mod.rs"]
mod persist_internal_tests;
mod serial_batch;

pub(crate) use batch_map::{PersistHeartbeat, map_batch_to_internal};
#[cfg(test)]
pub(in crate::indexer) use batch_map::{
    PersistSliceLimits, persist_conversations_batched, persist_conversations_batched_sliced,
    prepare_cursor_workspace_repair,
};
pub use batch_map::{bench_map_batch_to_internal, map_to_internal, persist_conversation};
#[cfg(test)]
pub(crate) use batch_map::{
    map_to_internal_with_redactor, map_to_internal_with_redactor_and_heartbeat,
};
pub(in crate::indexer) use batch_map::{
    persist_conversations_batched_inner, persist_conversations_batched_with_raw_mirror_links,
    persist_conversations_batched_with_semantic_delta_and_raw_mirror_links,
};
#[cfg(test)]
pub(in crate::indexer) use canonical_replay::REGION_DOC_REPUBLISH_DEFER_MIN_STALE_SESSIONS;
pub(in crate::indexer) use canonical_replay::{
    PersistBatchOutcome, maybe_defer_region_doc_convergence_for_republish,
    replay_watch_canonical_conversation,
};
#[cfg(test)]
pub(in crate::indexer) use concurrent::{
    BULK_IMPORT_WAL_AUTOCHECKPOINT_PAGES, SERIAL_CHUNK_CONTENTION_RETRIES,
};
pub(in crate::indexer) use concurrent::{
    apply_index_writer_busy_timeout, apply_index_writer_checkpoint_policy,
    begin_concurrent_retry_limit, defer_lexical_updates_enabled, maybe_checkpoint_bulk_ingest_wal,
    with_concurrent_retry, with_ephemeral_writer,
};

#[cfg(test)]
use batch_map::{
    INNER_REDACTION_MIN_MESSAGES, INNER_REDACTION_MIN_TEXT_BYTES, MAP_HEARTBEAT_MESSAGE_STRIDE,
    map_message_to_internal, persist_conversation_slice, persist_conversation_with_profile,
    persist_slice_ranges, should_slice_conversation, use_inner_message_redaction,
};
#[cfg(test)]
use canonical_replay::{
    PersistConversationPerfProfile, canonical_lexical_conversation, canonical_lexical_packet,
    lexical_packet_for_changed_messages, load_inserted_message_ids_by_idx,
    packet_semantic_delta_for_outcome, positional_indices_for_inserted,
    replay_canonical_conversation_in_chunks, stage_persisted_region_docs,
};
#[cfg(test)]
use concurrent::{
    BEGIN_CONCURRENT_CHUNK_SIZE_MAX, BEGIN_CONCURRENT_RETRY_MAX,
    BEGIN_CONCURRENT_WRITER_CACHE_KIB_MAX, BulkIngestWalCheckpointMode,
    begin_concurrent_chunk_size, begin_concurrent_writer_cache_kib,
    begin_concurrent_writes_enabled, duplicate_conversation_keys_present,
    index_writer_wal_autocheckpoint_pages, is_retryable_franken_error,
    persist_conversations_batched_begin_concurrent,
};
