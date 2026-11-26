//! Schema v1 replaces the redundant table-level `UNIQUE(conversation_id, idx)`
//! autoindex with the sole named unique index `idx_messages_conv_idx`. These
//! are the first-attempt queries of the two lexical-rebuild tail readers,
//! pinned to `idx_messages_conv_idx`. The existing "no such index" fallbacks
//! stay in place for archives lacking the index.

/// First-attempt query of
/// `raise_lexical_rebuild_footprints_from_tail_max_idx`.
pub(super) const TAIL_MAX_IDX_SQL: &str = "SELECT conversation_id, MAX(idx) AS last_message_idx
             FROM messages INDEXED BY idx_messages_conv_idx
             GROUP BY conversation_id
             ORDER BY conversation_id ASC";

/// First-attempt query of `fill_missing_lexical_rebuild_footprint_tails`.
pub(super) const MISSING_TAIL_SQL: &str = "SELECT conversation_id, MAX(idx) AS last_message_idx
             FROM messages INDEXED BY idx_messages_conv_idx
             GROUP BY conversation_id
             ORDER BY conversation_id ASC";
