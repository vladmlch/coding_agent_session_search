mod backfill;
mod batch_prep;
mod build_ops;
mod config;
mod replay_cache;
mod replay_selection;
mod scheduler;
mod storage_paths;
mod types;

pub use build_ops::SemanticIndexer;
pub use config::{CancelCheck, HASH_VECTOR_SPACE_REVISION, expected_vector_space_revision};
pub use types::{
    EmbeddedMessage, EmbeddingInput, SemanticBackfillBatchOutcome, SemanticBackfillBatchPlan,
    SemanticBackfillStoragePlan, SemanticShardBuildOutcome, SemanticShardBuildPlan,
};

#[cfg(test)]
pub(crate) use replay_selection::packet_embedding_inputs_from_storage;
pub(crate) use replay_selection::{
    CanonicalIncrementalEmbeddingBatch, SemanticPacketContext, message_id_from_db,
    packet_embedding_inputs_from_storage_for_message_ids,
    packet_embedding_inputs_from_storage_since, packet_embedding_inputs_from_storage_with_progress,
    saturating_u32_from_i64, semantic_inputs_from_packets,
    visit_packet_embedding_inputs_from_storage,
};
#[cfg(test)]
pub(super) use scheduler::{
    SemanticBackfillSchedulerReason, SemanticBackfillSchedulerState,
    semantic_backfill_scheduler_decision_for_capacity,
};
pub(crate) use scheduler::{
    SemanticBackfillSchedulerSignals, semantic_backfill_scheduler_decision,
};
pub(crate) use storage_paths::{
    invalidate_identity_stale_semantic_shards, semantic_doc_id_for_input,
};

// Imports consumed by existing child module `replay_cache` via `use super::*;`.
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Result;

use crate::franken_sync::compat::{ConnectionExt, ParamValue, RowExt};
use crate::indexer::responsiveness;
use crate::indexer::semantic_progress::{
    SemanticProgressEvent, SemanticProgressFields, SemanticProgressSink,
};
use crate::search::policy::{CHUNKING_STRATEGY_VERSION, SEMANTIC_SCHEMA_VERSION};
use crate::search::semantic_manifest::TierKind;
use crate::search::vector_index::VECTOR_INDEX_DIR;
use crate::storage::sqlite::FrankenStorage;

use config::{
    DEFAULT_SEMANTIC_RECONCILIATION_SCAN_CONVERSATIONS, resolved_env_usize,
    saturating_u64_from_usize,
};
use replay_selection::SemanticCheckpointCaps;
use types::BackfillFilePair;

// Test-only imports consumed by child/grandchild test modules via `use super::*;`.
#[cfg(test)]
use std::collections::HashMap;
#[cfg(test)]
use std::time::Instant;

#[cfg(test)]
use anyhow::{Context, bail};
#[cfg(test)]
use frankensearch::index::{
    HnswIndex as FsHnswIndex, Quantization as FsQuantization, VectorIndex as FsVectorIndex,
    wal_path_for as fsvi_wal_path_for,
};
#[cfg(test)]
use indicatif::ProgressBar;

#[cfg(test)]
use crate::indexer::memoization::ContentAddressedMemoCache;
#[cfg(test)]
use crate::model::conversation_packet::{ConversationPacket, ConversationPacketProvenance};
#[cfg(test)]
use crate::model::types::{Conversation, Message};
#[cfg(test)]
use crate::search::canonicalize::content_hash;
#[cfg(test)]
use crate::search::embedder::Embedder;
#[cfg(test)]
use crate::search::fastembed_embedder::FastEmbedder;
#[cfg(test)]
use crate::search::hash_embedder::HashEmbedder;
#[cfg(test)]
use crate::search::policy::SemanticPolicy;
#[cfg(test)]
use crate::search::semantic_manifest::{
    ArtifactRecord, BuildCheckpoint, SemanticManifest, SemanticShardManifest,
};
#[cfg(test)]
use crate::search::tantivy::normalized_index_source_id;
#[cfg(test)]
use crate::search::vector_index::{ROLE_USER, role_code_from_str, vector_index_path};

#[cfg(test)]
use batch_prep::{
    ExactEmbeddingReuse, Prepared, flush_prepared_batch, length_aware_batches, prepare_window,
    prepare_window_with_memo, semantic_prep_memo_key, validate_embedding_vector,
};
#[cfg(test)]
use config::{
    DEFAULT_SEMANTIC_BATCH_SIZE, DEFAULT_SEMANTIC_EMBED_BATCH_FAIL_AFTER_MS,
    DEFAULT_SEMANTIC_EMBED_BATCH_WARN_AFTER_MS, DEFAULT_SEMANTIC_MAX_BYTES_PER_CHECKPOINT,
    DEFAULT_SEMANTIC_MAX_MESSAGES_PER_CHECKPOINT, SEMANTIC_PREP_MEMO_ALGORITHM,
    SEMANTIC_PREP_MEMO_VERSION, parallel_prep_enabled, resolved_semantic_embed_batch_fail_after_ms,
    resolved_semantic_embed_batch_warn_after_ms,
};
#[cfg(test)]
use replay_selection::{
    CanonicalEmbeddingConversationRow, CheckpointCappedSelection,
    cached_semantic_total_conversations, canonical_embedding_conversation,
    canonical_embedding_packet_provenance, fetch_bounded_semantic_candidate_conversation_ids,
    fetch_canonical_embedding_batch, fetch_canonical_embedding_batch_inner,
    fetch_canonical_embedding_batch_inner_with_caps,
    fetch_canonical_embedding_batch_inner_with_caps_and_total,
    fetch_canonical_embedding_conversations,
    packet_embedding_inputs_from_materialized_canonical_messages,
    packet_embedding_inputs_from_selected_canonical_messages,
    select_checkpoint_capped_conversations_lazily, semantic_conversation_has_message_after,
    semantic_tail_state_has_message_after, total_semantic_conversations,
    visit_packet_embedding_inputs_from_storage_with_limits,
};
#[cfg(test)]
use storage_paths::{
    hnsw_index_path, now_ms, semantic_doc_id_for_embedded, semantic_staging_index_path,
    validated_semantic_index_ids,
};
#[cfg(all(test, unix))]
use types::backfill_file_stamp;

#[cfg(test)]
mod gh525_tests;
#[cfg(test)]
mod shard_publication_tests;
#[cfg(test)]
mod snapshot_tests;
#[cfg(test)]
mod tests;
