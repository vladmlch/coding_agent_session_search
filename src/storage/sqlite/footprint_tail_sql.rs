//! Local fix (review 2026-11-02, S3). V16 dropped `idx_messages_conv_idx`;
//! `UNIQUE(conversation_id, idx)` provides `sqlite_autoindex_messages_1`
//! with the same key. These are the first-attempt queries of the two
//! lexical-rebuild tail readers, re-hinted to the live index. The existing
//! "no such index" fallbacks stay in place for archives with an
//! unexpected autoindex name.

/// First-attempt query of
/// `raise_lexical_rebuild_footprints_from_tail_max_idx`.
pub(super) const TAIL_MAX_IDX_SQL: &str = "SELECT conversation_id, MAX(idx) AS last_message_idx
             FROM messages INDEXED BY sqlite_autoindex_messages_1
             GROUP BY conversation_id
             ORDER BY conversation_id ASC";

/// First-attempt query of `fill_missing_lexical_rebuild_footprint_tails`.
pub(super) const MISSING_TAIL_SQL: &str = "SELECT conversation_id, MAX(idx) AS last_message_idx
             FROM messages INDEXED BY sqlite_autoindex_messages_1
             GROUP BY conversation_id
             ORDER BY conversation_id ASC";
