use super::backup_and_integrity::{
    RecoveryConversationRow, franken_read_message_extra_compat, franken_read_metadata_compat,
    recovery_conversation_row_from_lenient_columns,
};
use super::batch_ingest::normalized_storage_source_parts;
use super::footprint_tail_sql;
use super::fts_rebuild::role_str;
use super::orphan_fk_cleanup::{
    error_indicates_missing_column, error_indicates_missing_table, franken_table_column_names,
};
use super::source_and_historical_import::message_extra_to_source_jsonl_line;
use super::storage_core::FrankenStorage;
use crate::franken_sync::{
    Row as FrankenRow, SqliteValue,
    compat::{ConnectionExt as FrankenConnectionExt, RowExt as FrankenRowExt},
};
use crate::model::types::{Conversation, Message, MessageRole};
use anyhow::{Context, Result, anyhow};
use smallvec::SmallVec;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

/// Row from the embedding_jobs table.
#[derive(Debug, Clone)]
pub struct EmbeddingJobRow {
    pub id: i64,
    pub db_path: String,
    pub model_id: String,
    pub status: String,
    pub total_docs: i64,
    pub completed_docs: i64,
    pub error_message: Option<String>,
    pub created_at: String,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
}

/// Lightweight conversation projection used while rebuilding the lexical index.
///
/// This intentionally omits `metadata_json` / `metadata_bin` and other bulky
/// fields because Tantivy only needs the stable envelope plus provenance
/// identifiers, avoiding large overflow-chain reads before the first lexical
/// checkpoint is committed.
#[derive(Debug, Clone)]
pub struct LexicalRebuildConversationRow {
    pub id: Option<i64>,
    pub agent_slug: String,
    pub workspace: Option<PathBuf>,
    pub external_id: Option<String>,
    pub title: Option<String>,
    pub source_path: PathBuf,
    pub started_at: Option<i64>,
    pub ended_at: Option<i64>,
    pub source_id: String,
    pub origin_host: Option<String>,
}

/// Lightweight per-conversation footprint used to pre-plan lexical rebuild
/// shard boundaries without re-reading full message bodies in the hot path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LexicalRebuildConversationFootprintRow {
    pub conversation_id: i64,
    pub message_count: usize,
    pub message_bytes: usize,
}

pub(crate) const LEXICAL_REBUILD_PLANNER_ESTIMATED_BYTES_PER_MESSAGE: usize = 4 * 1024;
const LEXICAL_REBUILD_FOOTPRINT_POINT_TAIL_FALLBACK_LIMIT: usize = 64;

fn lexical_rebuild_tail_metadata_coverage_is_sufficient(
    total_conversations: usize,
    covered_conversations: usize,
) -> bool {
    total_conversations == 0
        || total_conversations.saturating_sub(covered_conversations.min(total_conversations))
            <= LEXICAL_REBUILD_FOOTPRINT_POINT_TAIL_FALLBACK_LIMIT
}

fn lexical_rebuild_message_count_from_tail_idx(last_message_idx: Option<i64>) -> Option<usize> {
    let last_message_idx = u64::try_from(last_message_idx?).ok()?;
    let high_water = last_message_idx.checked_add(1)?;
    usize::try_from(high_water).ok()
}

fn lexical_rebuild_conversation_footprint_from_count(
    conversation_id: i64,
    message_count: usize,
) -> LexicalRebuildConversationFootprintRow {
    LexicalRebuildConversationFootprintRow {
        conversation_id,
        message_count,
        message_bytes: message_count
            .saturating_mul(LEXICAL_REBUILD_PLANNER_ESTIMATED_BYTES_PER_MESSAGE),
    }
}

/// Lightweight message projection used by the streaming lexical rebuild path.
#[derive(Debug, Clone)]
pub struct LexicalRebuildMessageRow {
    pub conversation_id: i64,
    pub id: i64,
    pub idx: i64,
    pub role: String,
    pub author: Option<String>,
    pub created_at: Option<i64>,
    pub content: String,
}

/// Even lighter message projection used only by the grouped lexical rebuild
/// stream hot path. It keeps just the per-message fields the rebuild consumes
/// and tracks the final message id at conversation scope instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LexicalRebuildGroupedMessageRow {
    pub idx: i64,
    pub is_tool_role: bool,
    pub created_at: Option<i64>,
    pub content: String,
}

pub type LexicalRebuildGroupedMessageRows = SmallVec<[LexicalRebuildGroupedMessageRow; 32]>;

/// Default per-message lexical-content byte ceiling (#290).
///
/// 8 MiB of one message's text is far more than lexical search needs (tokens,
/// not raw blobs) and keeps a single pasted image/base64 payload from
/// dominating a rebuild's working set.
pub const LEXICAL_MAX_MESSAGE_CONTENT_BYTES_DEFAULT: usize = 8 * 1024 * 1024;

/// Per-MESSAGE lexical-content byte ceiling, overridable via
/// `CASS_LEXICAL_MAX_MESSAGE_CONTENT_BYTES` (#290).
///
/// It bounds one message (a pasted image/base64 blob), never a conversation:
/// a cumulative per-conversation cap blanked every message past a long
/// session's first 8 MiB, which on the owner's archive was 39.6% of its
/// messages (bgn6s). Long conversations are instead read in bounded chunks.
/// `0` is rejected (treated as "use default").
pub fn lexical_max_message_content_bytes() -> usize {
    dotenvy::var("CASS_LEXICAL_MAX_MESSAGE_CONTENT_BYTES")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(LEXICAL_MAX_MESSAGE_CONTENT_BYTES_DEFAULT)
}

/// Largest byte length `<= cap` that ends on a UTF-8 char boundary of `content`.
fn lexical_content_truncation_boundary(content: &str, cap: usize) -> usize {
    if content.len() <= cap {
        return content.len();
    }
    let mut boundary = cap;
    while boundary > 0 && !content.is_char_boundary(boundary) {
        boundary -= 1;
    }
    boundary
}

/// Marker error a lexical-rebuild row callback raises to stop a stream early;
/// never surfaced to callers.
const LEXICAL_REBUILD_STREAM_STOPPED: &str = "cass lexical rebuild stream stopped by caller";

/// One lexical-rebuild projection row (`id, idx, role, author, created_at,
/// capped content, source byte length`) as a [`Message`] whose text is bounded
/// by `cap` bytes on a UTF-8 boundary (#290).
pub(super) fn lexical_rebuild_message_from_row(
    row: &FrankenRow,
    cap: usize,
) -> std::result::Result<Message, crate::franken_sync::FrankenError> {
    let role: String = row.get_typed(2)?;
    let mut content: String = row.get_typed(5)?;
    let boundary = lexical_content_truncation_boundary(&content, cap);
    if boundary < content.len() {
        // GH #466: truncate would retain the projected cell's whole allocation.
        content = content[..boundary].to_owned();
    }
    Ok(Message {
        id: Some(row.get_typed(0)?),
        idx: row.get_typed(1)?,
        role: match role.as_str() {
            "user" => MessageRole::User,
            "agent" | "assistant" => MessageRole::Agent,
            "tool" => MessageRole::Tool,
            "system" => MessageRole::System,
            other => MessageRole::Other(other.to_string()),
        },
        author: row.get_typed(3)?,
        created_at: row.get_typed(4)?,
        content,
        extra_json: serde_json::Value::Null,
        snippets: Vec::new(),
    })
}

/// Cap each message's lexical content at `cap` bytes on a UTF-8 char boundary
/// (#290), independently of every other message. Test mirror of the per-row
/// bound the rebuild fetch applies; message rows are always preserved.
#[cfg(test)]
pub(super) fn truncate_lexical_rebuild_message_content(messages: &mut [Message], cap: usize) {
    for message in messages.iter_mut() {
        let boundary = lexical_content_truncation_boundary(&message.content, cap);
        message.content.truncate(boundary);
    }
}

impl FrankenStorage {
    /// Messages of one conversation, counted through `idx_messages_conv_idx`
    /// where the schema has it, avoiding a whole-table scan of `messages`.
    pub(super) fn conversation_message_count(&self, conversation_id: i64) -> Result<usize> {
        let indexed: bool = self
            .conn
            .query_map_collect("PRAGMA index_list(messages)", fparams![], |row| {
                row.get_typed::<String>(1)
            })?
            .iter()
            .any(|name| name == "idx_messages_conv_idx");
        let sql = if indexed {
            "SELECT idx FROM messages INDEXED BY idx_messages_conv_idx WHERE conversation_id = ?1"
        } else {
            "SELECT idx FROM messages WHERE conversation_id = ?1"
        };
        let mut count = 0_usize;
        self.conn.query_with_params_for_each(
            sql,
            &[SqliteValue::Integer(conversation_id)],
            |_| {
                count += 1;
                Ok(())
            },
        )?;
        Ok(count)
    }

    /// List all registered workspaces.
    pub fn list_workspaces(&self) -> Result<Vec<crate::model::types::Workspace>> {
        self.conn
            .query_map_collect(
                "SELECT id, path, display_name FROM workspaces ORDER BY path",
                fparams![],
                |row| {
                    let path_str: String = row.get_typed(1)?;
                    Ok(crate::model::types::Workspace {
                        id: Some(row.get_typed(0)?),
                        path: Path::new(&path_str).to_path_buf(),
                        display_name: row.get_typed(2)?,
                    })
                },
            )
            .with_context(|| "listing workspaces")
    }

    /// List conversations with pagination.
    pub fn list_conversations(&self, limit: i64, offset: i64) -> Result<Vec<Conversation>> {
        // Avoid the multi-table JOIN with LIMIT/OFFSET by using correlated
        // subqueries for the tiny agents (~20 rows) and workspaces (~30 rows)
        // lookup tables, and degrade NULL agent_id to the same 'unknown'
        // sentinel that 8a0c547c established for the lexical rebuild path.
        // Pre-v15 archives reached through no-migration lanes lack
        // conversation_tail_state; fall back to the parent ended_at there.
        let ended_at_projection = if self.conversation_tail_state_table_present() {
            "COALESCE(
                 (SELECT ts.ended_at
                  FROM conversation_tail_state ts
                  WHERE ts.conversation_id = c.id),
                 c.ended_at
             )"
        } else {
            "c.ended_at"
        };
        self.conn
            .query_map_collect(
                &format!(
                    "SELECT c.id,
                         COALESCE((SELECT a.slug FROM agents a WHERE a.id = c.agent_id), 'unknown'),
                         (SELECT w.path FROM workspaces w WHERE w.id = c.workspace_id),
                         c.external_id, c.title, c.source_path,
                         c.started_at,
                         {ended_at_projection},
                         c.approx_tokens, c.metadata_json,
                         c.source_id, c.origin_host, c.metadata_bin
                FROM conversations c
                ORDER BY CASE WHEN c.started_at IS NULL THEN 1 ELSE 0 END, c.started_at DESC, c.id DESC
                LIMIT ?1 OFFSET ?2"
                ),
                fparams![limit, offset],
                |row| {
                    let workspace_path: Option<String> = row.get_typed(2)?;
                    let source_path: String = row.get_typed(5)?;
                    let raw_source_id: Option<String> = row.get_typed(10)?;
                    let raw_origin_host: Option<String> = row.get_typed(11)?;
                    let (source_id, _, origin_host) = normalized_storage_source_parts(
                        raw_source_id.as_deref(),
                        None,
                        raw_origin_host.as_deref(),
                    );
                    Ok(Conversation {
                        id: Some(row.get_typed(0)?),
                        agent_slug: row.get_typed(1)?,
                        workspace: workspace_path.map(|p| Path::new(&p).to_path_buf()),
                        external_id: row.get_typed(3)?,
                        title: row.get_typed(4)?,
                        source_path: Path::new(&source_path).to_path_buf(),
                        started_at: row.get_typed(6)?,
                        ended_at: row.get_typed(7)?,
                        approx_tokens: row.get_typed(8)?,
                        metadata_json: franken_read_metadata_compat(row, 9, 12),
                        messages: Vec::new(),
                        source_id,
                        origin_host,
                    })
                },
            )
            .with_context(|| "listing conversations")
    }

    /// Page canonical conversations for `doctor --recover-from-archive`,
    /// tolerating rows whose stored types no longer match the schema.
    ///
    /// A page-aliasing corruption (#391: a 26-column `conversations` payload
    /// reachable from the 9-column `messages` tree, integers stored where
    /// `TEXT` is declared) makes the strict [`Self::list_conversations`] mapper
    /// fail with `type mismatch: expected text, got integer` at the first bad
    /// row — and the recovery export, whose whole purpose is a damaged
    /// archive, aborted with nothing written. This reader keys by `id` (no
    /// `ORDER BY started_at` sort over a damaged tree) and maps each row
    /// leniently: numeric/blob values in text columns are coerced to text and
    /// reported as coercions, non-integer timestamps are dropped, a row whose
    /// identity columns were not text is returned as
    /// [`RecoveryConversationRow::Quarantined`] (a mis-typed aliased record,
    /// not a conversation), and a row whose `id` is unreadable is returned as
    /// [`RecoveryConversationRow::Unreadable`] instead of failing the page.
    /// Engine-level page errors still surface as `Err`.
    pub fn list_conversations_for_recovery(
        &self,
        after_id: i64,
        limit: i64,
    ) -> Result<Vec<RecoveryConversationRow>> {
        self.conn
            .query_map_collect(
                "SELECT c.id,
                        COALESCE((SELECT a.slug FROM agents a WHERE a.id = c.agent_id), 'unknown'),
                        (SELECT w.path FROM workspaces w WHERE w.id = c.workspace_id),
                        c.external_id, c.title, c.source_path,
                        c.started_at, c.ended_at, c.approx_tokens,
                        c.metadata_json, c.source_id, c.origin_host, c.metadata_bin
                 FROM conversations c
                 WHERE c.id > ?1
                 ORDER BY c.id
                 LIMIT ?2",
                fparams![after_id, limit],
                |row| Ok(recovery_conversation_row_from_lenient_columns(row)),
            )
            .with_context(|| "listing conversations for recovery")
    }

    /// Build lookup maps for agents and workspaces to avoid JOINs in
    /// paged conversation queries.  Both tables are tiny (tens of rows)
    /// so this is effectively free.
    pub fn build_lexical_rebuild_lookups(
        &self,
    ) -> Result<(HashMap<i64, String>, HashMap<i64, PathBuf>)> {
        let agents: HashMap<i64, String> = self
            .conn
            .query_map_collect("SELECT id, slug FROM agents", fparams![], |row| {
                Ok((row.get_typed::<i64>(0)?, row.get_typed::<String>(1)?))
            })
            .with_context(|| "loading agent lookup for lexical rebuild")?
            .into_iter()
            .collect();
        let workspaces: HashMap<i64, PathBuf> = self
            .conn
            .query_map_collect("SELECT id, path FROM workspaces", fparams![], |row| {
                let path_str: String = row.get_typed(1)?;
                Ok((row.get_typed::<i64>(0)?, PathBuf::from(path_str)))
            })
            .with_context(|| "loading workspace lookup for lexical rebuild")?
            .into_iter()
            .collect();
        Ok((agents, workspaces))
    }

    /// List per-conversation message footprints in primary-key order.
    ///
    /// This deliberately avoids rebuild-path JOINs. Instead we merge ordered
    /// single-table reads over `conversations` and the narrow
    /// `conversation_tail_state` cache in Rust, then use `last_message_idx + 1`
    /// as a planning estimate.
    ///
    /// The planner only needs a sizing heuristic; exact message and byte
    /// accounting is performed later by the rebuild packet pipeline as it reads
    /// message content for indexing. Rows missing both tail-cache sources fall
    /// back to `MAX(messages.idx) + 1`, which preserves legacy upgraded
    /// databases without treating populated conversations as empty.
    pub fn list_conversation_footprints_for_lexical_rebuild(
        &self,
    ) -> Result<Vec<LexicalRebuildConversationFootprintRow>> {
        let tail_state_rows: Vec<(i64, Option<i64>)> = match self.conn.query_map_collect(
            "SELECT conversation_id, last_message_idx
             FROM conversation_tail_state
             ORDER BY conversation_id ASC",
            fparams![],
            |row| Ok((row.get_typed::<i64>(0)?, row.get_typed::<Option<i64>>(1)?)),
        ) {
            Ok(rows) => rows,
            Err(err) if error_indicates_missing_table(&err) => Vec::new(),
            Err(err) => {
                return Err(err).with_context(|| "listing lexical rebuild tail-state estimates");
            }
        };
        let tail_state_by_conversation: HashMap<i64, Option<i64>> =
            tail_state_rows.into_iter().collect();

        let rows: Vec<(i64, Option<i64>)> = match self.conn.query_map_collect(
            "SELECT id, last_message_idx
             FROM conversations
             ORDER BY id ASC",
            fparams![],
            |row| Ok((row.get_typed::<i64>(0)?, row.get_typed::<Option<i64>>(1)?)),
        ) {
            Ok(rows) => rows,
            Err(err) if error_indicates_missing_column(&err) => self
                .conn
                .query_map_collect(
                    "SELECT id
                     FROM conversations
                     ORDER BY id ASC",
                    fparams![],
                    |row| Ok((row.get_typed::<i64>(0)?, None)),
                )
                .with_context(|| {
                    "listing lexical rebuild conversation ids after missing tail column fallback"
                })?,
            Err(err) => {
                return Err(err)
                    .with_context(|| "listing lexical rebuild conversation footprint estimates");
            }
        };

        let mut footprints = Vec::with_capacity(rows.len());
        let mut missing_tail_positions = HashMap::new();
        for (conversation_id, conversation_last_message_idx) in rows {
            let last_message_idx = tail_state_by_conversation
                .get(&conversation_id)
                .copied()
                .flatten()
                .or(conversation_last_message_idx);
            let Some(message_count) = lexical_rebuild_message_count_from_tail_idx(last_message_idx)
            else {
                missing_tail_positions.insert(conversation_id, footprints.len());
                footprints.push(LexicalRebuildConversationFootprintRow {
                    conversation_id,
                    message_count: 0,
                    message_bytes: 0,
                });
                continue;
            };
            footprints.push(lexical_rebuild_conversation_footprint_from_count(
                conversation_id,
                message_count,
            ));
        }

        let every_footprint_was_missing_tail = missing_tail_positions.len() == footprints.len();
        if !missing_tail_positions.is_empty() {
            self.fill_missing_lexical_rebuild_footprint_tails(
                &mut footprints,
                &missing_tail_positions,
            )?;
        }
        // Tail metadata can UNDERSTATE reality: both `conversations.
        // last_message_idx` and `conversation_tail_state` are caches, and a
        // stale-low entry silently under-plans the lexical shards, tripping
        // the doc>plan invariant during rebuild. Trusting complete tail
        // coverage without checking is therefore not safe.
        //
        // This raise reads MAX(idx) straight from the covering index
        // `idx_messages_conv_idx(conversation_id, idx)` — a per-conversation
        // seek, and the same query `fill_missing_lexical_rebuild_footprint_
        // tails` already runs in production. It is deliberately NOT the
        // exact-count pass below: that one is an unindexed
        // `COUNT(*) ... GROUP BY conversation_id`, which is what thrashed for
        // 10-20+ minutes on multi-GB archives during plan_lexical_shards (the
        // 0.6.23 prepare wedge).
        //
        // MAX(idx)+1 over-estimates when idx values are sparse. That is the
        // safe direction: over-planning costs shards, under-planning corrupts
        // the rebuild.
        self.raise_lexical_rebuild_footprints_from_tail_max_idx(&mut footprints)?;

        // The exact-count pass stays restricted to the mixed-coverage case it
        // was narrowed to; MAX(idx) already covers the common understatement.
        if !missing_tail_positions.is_empty() && !every_footprint_was_missing_tail {
            self.raise_lexical_rebuild_footprints_to_exact_message_counts(&mut footprints)?;
        }

        Ok(footprints)
    }

    /// Raise footprints whose cached tail understates the real message tail.
    ///
    /// Uses the covering index so this stays a per-conversation seek rather
    /// than a full scan of `messages`. Falls back to an unhinted query when
    /// the index is absent, mirroring
    /// `fill_missing_lexical_rebuild_footprint_tails`.
    fn raise_lexical_rebuild_footprints_from_tail_max_idx(
        &self,
        footprints: &mut [LexicalRebuildConversationFootprintRow],
    ) -> Result<()> {
        if footprints.is_empty() {
            return Ok(());
        }

        let positions_by_conversation: HashMap<i64, usize> = footprints
            .iter()
            .enumerate()
            .map(|(position, footprint)| (footprint.conversation_id, position))
            .collect();

        self.raise_lexical_rebuild_footprints_from_grouped_max_idx(
            footprints,
            &positions_by_conversation,
            footprint_tail_sql::TAIL_MAX_IDX_SQL,
        )
        .or_else(|err| {
            if err.to_string().contains("no such index") {
                return self.raise_lexical_rebuild_footprints_from_grouped_max_idx(
                    footprints,
                    &positions_by_conversation,
                    "SELECT conversation_id, MAX(idx) AS last_message_idx
                     FROM messages
                     GROUP BY conversation_id
                     ORDER BY conversation_id ASC",
                );
            }
            Err(err)
        })
        .or_else(|err| {
            // A missing `messages` table means there is nothing to raise
            // against; the tail estimates already in hand stand.
            if error_indicates_missing_table(&err) {
                return Ok(());
            }
            Err(err)
        })
        .with_context(|| "raising lexical rebuild footprints from grouped message tails")?;
        Ok(())
    }

    fn raise_lexical_rebuild_footprints_from_grouped_max_idx(
        &self,
        footprints: &mut [LexicalRebuildConversationFootprintRow],
        positions_by_conversation: &HashMap<i64, usize>,
        sql: &str,
    ) -> Result<()> {
        self.conn
            .query_with_params_for_each(sql, &[] as &[SqliteValue], |row| {
                let conversation_id: i64 = row.get_typed(0)?;
                let last_message_idx: Option<i64> = row.get_typed(1)?;
                let Some(position) = positions_by_conversation.get(&conversation_id) else {
                    return Ok(());
                };
                let Some(message_count) =
                    lexical_rebuild_message_count_from_tail_idx(last_message_idx)
                else {
                    return Ok(());
                };
                let footprint = &mut footprints[*position];
                if message_count > footprint.message_count {
                    let raised = lexical_rebuild_conversation_footprint_from_count(
                        conversation_id,
                        message_count,
                    );
                    footprint.message_count = raised.message_count;
                    footprint.message_bytes = footprint.message_bytes.max(raised.message_bytes);
                }
                Ok(())
            })
            .with_context(|| "grouping lexical rebuild tail maxima")
    }

    pub fn lexical_rebuild_has_tail_footprint_metadata(&self) -> Result<bool> {
        let total_conversations: i64 = self
            .conn
            .query_row_map("SELECT COUNT(*) FROM conversations", fparams![], |row| {
                row.get_typed(0)
            })
            .with_context(|| "counting conversations for lexical rebuild tail metadata coverage")?;
        let total_conversations = usize::try_from(total_conversations.max(0)).unwrap_or(usize::MAX);
        if total_conversations == 0 {
            return Ok(true);
        }

        let conversation_columns = franken_table_column_names(&self.conn, "conversations")?;
        let conversations_have_tail_column = conversation_columns.contains("last_message_idx");
        let tail_state_has_tail_column =
            match franken_table_column_names(&self.conn, "conversation_tail_state") {
                Ok(columns) => columns.contains("last_message_idx"),
                Err(err) if error_indicates_missing_table(&err) => false,
                Err(err) => {
                    return Err(err)
                        .with_context(|| "reading lexical rebuild tail-state metadata columns");
                }
            };
        if !conversations_have_tail_column && !tail_state_has_tail_column {
            return Ok(false);
        }

        let covered_sql = match (conversations_have_tail_column, tail_state_has_tail_column) {
            (true, true) => {
                "SELECT COUNT(*)
                 FROM conversations c
                 LEFT JOIN conversation_tail_state ts ON ts.conversation_id = c.id
                 WHERE c.last_message_idx IS NOT NULL
                    OR ts.last_message_idx IS NOT NULL"
            }
            (true, false) => {
                "SELECT COUNT(*)
                 FROM conversations
                 WHERE last_message_idx IS NOT NULL"
            }
            (false, true) => {
                "SELECT COUNT(*)
                 FROM conversations c
                 WHERE EXISTS (
                     SELECT 1
                     FROM conversation_tail_state ts
                     WHERE ts.conversation_id = c.id
                       AND ts.last_message_idx IS NOT NULL
                 )"
            }
            (false, false) => unreachable!("checked before covered_sql selection"),
        };
        let covered_conversations: i64 = self
            .conn
            .query_row_map(covered_sql, fparams![], |row| row.get_typed(0))
            .with_context(
                || "counting conversations covered by lexical rebuild tail footprint metadata",
            )?;
        let covered_conversations =
            usize::try_from(covered_conversations.max(0)).unwrap_or(usize::MAX);

        Ok(lexical_rebuild_tail_metadata_coverage_is_sufficient(
            total_conversations,
            covered_conversations,
        ))
    }

    fn raise_lexical_rebuild_footprints_to_exact_message_counts(
        &self,
        footprints: &mut [LexicalRebuildConversationFootprintRow],
    ) -> Result<()> {
        if footprints.is_empty() {
            return Ok(());
        }

        let positions_by_conversation: HashMap<i64, usize> = footprints
            .iter()
            .enumerate()
            .map(|(position, footprint)| (footprint.conversation_id, position))
            .collect();
        self.conn
            .query_with_params_for_each(
                "SELECT conversation_id, COUNT(*) AS message_count
                 FROM messages
                 GROUP BY conversation_id
                 ORDER BY conversation_id ASC",
                &[] as &[SqliteValue],
                |row| {
                    let conversation_id: i64 = row.get_typed(0)?;
                    let exact_count: i64 = row.get_typed(1)?;
                    let Some(position) = positions_by_conversation.get(&conversation_id) else {
                        return Ok(());
                    };
                    let exact_count = usize::try_from(exact_count.max(0)).unwrap_or(usize::MAX);
                    let footprint = &mut footprints[*position];
                    if exact_count > footprint.message_count {
                        footprint.message_count = exact_count;
                        footprint.message_bytes =
                            footprint.message_bytes.max(exact_count.saturating_mul(
                                LEXICAL_REBUILD_PLANNER_ESTIMATED_BYTES_PER_MESSAGE,
                            ));
                    }
                    Ok(())
                },
            )
            .with_context(|| "raising lexical rebuild footprints to exact message counts")?;
        Ok(())
    }

    fn fill_missing_lexical_rebuild_footprint_tails(
        &self,
        footprints: &mut [LexicalRebuildConversationFootprintRow],
        missing_tail_positions: &HashMap<i64, usize>,
    ) -> Result<()> {
        self.fill_missing_lexical_rebuild_footprint_tails_from_grouped_messages(
            footprints,
            missing_tail_positions,
            footprint_tail_sql::MISSING_TAIL_SQL,
        )
        .or_else(|err| {
            if err.to_string().contains("no such index") {
                return self.fill_missing_lexical_rebuild_footprint_tails_from_grouped_messages(
                    footprints,
                    missing_tail_positions,
                    "SELECT conversation_id, MAX(idx) AS last_message_idx
                     FROM messages
                     GROUP BY conversation_id
                     ORDER BY conversation_id ASC",
                );
            }
            Err(err)
        })
        .with_context(|| "grouping missing lexical rebuild tail estimates from messages")?;

        Ok(())
    }

    fn fill_missing_lexical_rebuild_footprint_tails_from_grouped_messages(
        &self,
        footprints: &mut [LexicalRebuildConversationFootprintRow],
        missing_tail_positions: &HashMap<i64, usize>,
        sql: &str,
    ) -> Result<()> {
        self.conn
            .query_with_params_for_each(sql, &[] as &[SqliteValue], |row| {
                let conversation_id: i64 = row.get_typed(0)?;
                let last_message_idx: Option<i64> = row.get_typed(1)?;
                let Some(position) = missing_tail_positions.get(&conversation_id) else {
                    return Ok(());
                };
                if let Some(message_count) =
                    lexical_rebuild_message_count_from_tail_idx(last_message_idx)
                {
                    footprints[*position] = lexical_rebuild_conversation_footprint_from_count(
                        conversation_id,
                        message_count,
                    );
                }
                Ok(())
            })
            .with_context(|| "grouping lexical rebuild missing tail estimates")
    }

    /// List conversation ids in the stable order used by lexical rebuilds.
    pub fn list_conversation_ids_for_lexical_rebuild(&self) -> Result<Vec<i64>> {
        self.conn
            .query_map_collect(
                "SELECT id FROM conversations ORDER BY id ASC",
                fparams![],
                |row| row.get_typed(0),
            )
            .with_context(|| "listing conversation ids for lexical rebuild")
    }
    /// Legacy OFFSET-based traversal for one-time checkpoint migration only.
    ///
    /// Whether the schema-v15 `conversation_tail_state` cache table exists on
    /// this connection's schema image. Pre-v15 archives reached through the
    /// no-migration lanes (readonly opens, the schema-only FTS-repair open)
    /// legitimately lack it — the v0.6.25 gate hit exactly that on the
    /// schema-13 search fixture.
    fn conversation_tail_state_table_present(&self) -> bool {
        match franken_table_column_names(&self.conn, "conversation_tail_state") {
            // PRAGMA table_info on a missing table yields an EMPTY column
            // set, not an error — presence means at least one column.
            Ok(columns) => !columns.is_empty(),
            Err(err) if error_indicates_missing_table(&err) => false,
            // Fail open to the richer projection; the listing query will
            // surface the real error with full context.
            Err(_) => true,
        }
    }

    /// `ended_at` projection for the lexical-rebuild listings: prefer the
    /// tail-state cache when present, fall back to the parent column.
    fn lexical_rebuild_ended_at_projection(&self) -> &'static str {
        if self.conversation_tail_state_table_present() {
            "COALESCE(
                 (SELECT ts.ended_at
                  FROM conversation_tail_state ts
                  WHERE ts.conversation_id = conversations.id),
                 ended_at
             )"
        } else {
            "ended_at"
        }
    }

    /// New code must use `list_conversations_for_lexical_rebuild_after_id`
    /// for keyset pagination.
    pub fn list_conversations_for_lexical_rebuild_by_offset(
        &self,
        limit: i64,
        offset: i64,
        agent_slugs: &HashMap<i64, String>,
        workspace_paths: &HashMap<i64, PathBuf>,
    ) -> Result<Vec<LexicalRebuildConversationRow>> {
        // Single-table query avoids the 3-table JOIN on the paged rebuild path.
        self.conn
            .query_map_collect(
                &format!(
                    "SELECT id, agent_id, workspace_id, external_id, title, source_path,
                       started_at,
                       {},
                       source_id, origin_host
                FROM conversations
                ORDER BY id ASC
                LIMIT ?1 OFFSET ?2",
                    self.lexical_rebuild_ended_at_projection()
                ),
                fparams![limit, offset],
                |row| {
                    let agent_id: Option<i64> = row.get_typed(1)?;
                    let workspace_id: Option<i64> = row.get_typed(2)?;
                    let source_path: String = row.get_typed(5)?;
                    let raw_source_id: Option<String> = row.get_typed(8)?;
                    let raw_origin_host: Option<String> = row.get_typed(9)?;
                    let (source_id, _, origin_host) = normalized_storage_source_parts(
                        raw_source_id.as_deref(),
                        None,
                        raw_origin_host.as_deref(),
                    );
                    Ok(LexicalRebuildConversationRow {
                        id: Some(row.get_typed(0)?),
                        agent_slug: agent_id
                            .and_then(|aid| agent_slugs.get(&aid).cloned())
                            .unwrap_or_else(|| "unknown".to_string()),
                        workspace: workspace_id.and_then(|wid| workspace_paths.get(&wid).cloned()),
                        external_id: row.get_typed(3)?,
                        title: row.get_typed(4)?,
                        source_path: Path::new(&source_path).to_path_buf(),
                        started_at: row.get_typed(6)?,
                        ended_at: row.get_typed(7)?,
                        source_id,
                        origin_host,
                    })
                },
            )
            .with_context(|| "listing conversations for lexical rebuild")
    }

    /// List lexical rebuild conversations strictly after the given primary key.
    ///
    /// Keyset pagination keeps later rebuild pages as cheap as earlier ones,
    /// avoiding the ever-growing `OFFSET` scan cost during large rebuilds.
    pub fn list_conversations_for_lexical_rebuild_after_id(
        &self,
        limit: i64,
        after_conversation_id: i64,
        agent_slugs: &HashMap<i64, String>,
        workspace_paths: &HashMap<i64, PathBuf>,
    ) -> Result<Vec<LexicalRebuildConversationRow>> {
        self.conn
            .query_map_collect(
                &format!(
                    "SELECT id, agent_id, workspace_id, external_id, title, source_path,
                       started_at,
                       {},
                       source_id, origin_host
                FROM conversations
                WHERE id > ?2
                ORDER BY id ASC
                LIMIT ?1",
                    self.lexical_rebuild_ended_at_projection()
                ),
                fparams![limit, after_conversation_id],
                |row| {
                    let agent_id: Option<i64> = row.get_typed(1)?;
                    let workspace_id: Option<i64> = row.get_typed(2)?;
                    let source_path: String = row.get_typed(5)?;
                    let raw_source_id: Option<String> = row.get_typed(8)?;
                    let raw_origin_host: Option<String> = row.get_typed(9)?;
                    let (source_id, _, origin_host) = normalized_storage_source_parts(
                        raw_source_id.as_deref(),
                        None,
                        raw_origin_host.as_deref(),
                    );
                    Ok(LexicalRebuildConversationRow {
                        id: Some(row.get_typed(0)?),
                        agent_slug: agent_id
                            .and_then(|aid| agent_slugs.get(&aid).cloned())
                            .unwrap_or_else(|| "unknown".to_string()),
                        workspace: workspace_id.and_then(|wid| workspace_paths.get(&wid).cloned()),
                        external_id: row.get_typed(3)?,
                        title: row.get_typed(4)?,
                        source_path: Path::new(&source_path).to_path_buf(),
                        started_at: row.get_typed(6)?,
                        ended_at: row.get_typed(7)?,
                        source_id,
                        origin_host,
                    })
                },
            )
            .with_context(|| {
                format!(
                    "listing conversations for lexical rebuild after id {after_conversation_id}"
                )
            })
    }

    /// List lexical rebuild conversations inside an `(after_id, through_id]`
    /// primary-key window.
    ///
    /// This lets the rebuild producer respect planned shard boundaries without
    /// falling back to client-side trimming or multi-table joins.
    pub fn list_conversations_for_lexical_rebuild_after_id_through_id(
        &self,
        limit: i64,
        after_conversation_id: i64,
        through_conversation_id: i64,
        agent_slugs: &HashMap<i64, String>,
        workspace_paths: &HashMap<i64, PathBuf>,
    ) -> Result<Vec<LexicalRebuildConversationRow>> {
        if through_conversation_id <= after_conversation_id {
            return Ok(Vec::new());
        }
        self.conn
            .query_map_collect(
                &format!(
                    "SELECT id, agent_id, workspace_id, external_id, title, source_path,
                       started_at,
                       {},
                       source_id, origin_host
                FROM conversations
                WHERE id > ?2 AND id <= ?3
                ORDER BY id ASC
                LIMIT ?1",
                    self.lexical_rebuild_ended_at_projection()
                ),
                fparams![limit, after_conversation_id, through_conversation_id],
                |row| {
                    let agent_id: Option<i64> = row.get_typed(1)?;
                    let workspace_id: Option<i64> = row.get_typed(2)?;
                    let source_path: String = row.get_typed(5)?;
                    let raw_source_id: Option<String> = row.get_typed(8)?;
                    let raw_origin_host: Option<String> = row.get_typed(9)?;
                    let (source_id, _, origin_host) = normalized_storage_source_parts(
                        raw_source_id.as_deref(),
                        None,
                        raw_origin_host.as_deref(),
                    );
                    Ok(LexicalRebuildConversationRow {
                        id: Some(row.get_typed(0)?),
                        agent_slug: agent_id
                            .and_then(|aid| agent_slugs.get(&aid).cloned())
                            .unwrap_or_else(|| "unknown".to_string()),
                        workspace: workspace_id.and_then(|wid| workspace_paths.get(&wid).cloned()),
                        external_id: row.get_typed(3)?,
                        title: row.get_typed(4)?,
                        source_path: Path::new(&source_path).to_path_buf(),
                        started_at: row.get_typed(6)?,
                        ended_at: row.get_typed(7)?,
                        source_id,
                        origin_host,
                    })
                },
            )
            .with_context(|| {
                format!(
                    "listing conversations for lexical rebuild after id {after_conversation_id} through id {through_conversation_id}"
                )
            })
    }

    /// Fetch messages for a conversation.
    pub fn fetch_messages(&self, conversation_id: i64) -> Result<Vec<Message>> {
        let hinted_sql = "SELECT id, idx, role, author, created_at, content, extra_json, extra_bin \
             FROM messages INDEXED BY idx_messages_conv_idx \
             WHERE conversation_id = ?1 ORDER BY idx";
        let fallback_sql = "SELECT id, idx, role, author, created_at, content, extra_json, extra_bin \
             FROM messages \
             WHERE conversation_id = ?1 ORDER BY idx";

        self.conn
            .query_map_collect(hinted_sql, fparams![conversation_id], |row| {
                let role: String = row.get_typed(2)?;
                Ok(Message {
                    id: Some(row.get_typed(0)?),
                    idx: row.get_typed(1)?,
                    role: match role.as_str() {
                        "user" => MessageRole::User,
                        "agent" | "assistant" => MessageRole::Agent,
                        "tool" => MessageRole::Tool,
                        "system" => MessageRole::System,
                        other => MessageRole::Other(other.to_string()),
                    },
                    author: row.get_typed(3)?,
                    created_at: row.get_typed(4)?,
                    content: row.get_typed(5)?,
                    extra_json: franken_read_message_extra_compat(row, 6, 7),
                    snippets: Vec::new(),
                })
            })
            .or_else(|err| {
                if err
                    .to_string()
                    .contains("no such index: idx_messages_conv_idx")
                {
                    return self.conn.query_map_collect(
                        fallback_sql,
                        fparams![conversation_id],
                        |row| {
                            let role: String = row.get_typed(2)?;
                            Ok(Message {
                                id: Some(row.get_typed(0)?),
                                idx: row.get_typed(1)?,
                                role: match role.as_str() {
                                    "user" => MessageRole::User,
                                    "agent" | "assistant" => MessageRole::Agent,
                                    "tool" => MessageRole::Tool,
                                    "system" => MessageRole::System,
                                    other => MessageRole::Other(other.to_string()),
                                },
                                author: row.get_typed(3)?,
                                created_at: row.get_typed(4)?,
                                content: row.get_typed(5)?,
                                extra_json: franken_read_message_extra_compat(row, 6, 7),
                                snippets: Vec::new(),
                            })
                        },
                    );
                }
                Err(err)
            })
            .with_context(|| format!("fetching messages for conversation {conversation_id}"))
    }

    /// Reconstruct the source JSONL lines for a single conversation from the
    /// canonical archive's preserved per-message envelopes.
    ///
    /// cass stores the verbatim original source event for every message as
    /// `extra_json` (a `{__cass_historical_raw_json__: "<line>"}` wrapper) or
    /// `extra_bin` (the same event MessagePack-encoded). That makes the
    /// canonical archive self-sufficient for regenerating the on-disk source
    /// file of any session whose original file is gone or whose derived
    /// structures are corrupt — no stock-sqlite `.recover` is needed.
    ///
    /// Returns one `String` per message in `idx` order. Each string is a single
    /// JSONL line (no trailing newline): the preserved verbatim line when one
    /// exists, otherwise the decoded event re-serialized as compact JSON.
    /// Messages whose envelope decodes to `null` (no preserved event) are
    /// skipped, since they carry no source line to regenerate.
    pub fn reconstruct_source_jsonl_for_conversation(
        &self,
        conversation_id: i64,
    ) -> Result<Vec<String>> {
        let messages = self.fetch_messages(conversation_id)?;
        let mut lines = Vec::with_capacity(messages.len());
        for message in messages {
            if let Some(line) = message_extra_to_source_jsonl_line(&message.extra_json) {
                lines.push(line);
            }
        }
        Ok(lines)
    }

    /// Fetch messages for lexical index rebuilds without deserializing extra metadata.
    ///
    /// Tantivy only needs message text and core envelope fields, so avoiding
    /// `extra_json` here prevents rebuilds from rehydrating enormous historical
    /// payloads that are irrelevant to lexical search.
    ///
    /// Each message's indexed text is capped at
    /// [`lexical_max_message_content_bytes`] (#290: one pasted image/base64
    /// blob). Every message keeps its own text however long the conversation is
    /// (bgn6s); a caller that must bound memory for a long conversation reads it
    /// in index ranges ([`Self::lexical_rebuild_message_footprints`],
    /// [`Self::fetch_messages_for_lexical_rebuild_idx_range`]) or stops early
    /// ([`Self::fetch_messages_for_lexical_rebuild_within`]).
    pub fn fetch_messages_for_lexical_rebuild(&self, conversation_id: i64) -> Result<Vec<Message>> {
        let mut messages = Vec::new();
        self.for_each_lexical_rebuild_message(conversation_id, None, |message| {
            messages.push(message);
            Ok(true)
        })?;
        Ok(messages)
    }

    /// [`Self::fetch_messages_for_lexical_rebuild`], but stops reading as soon
    /// as the conversation's indexed text exceeds `max_bytes` and returns
    /// `None`, so a batch never materializes a long conversation it will reject.
    pub fn fetch_messages_for_lexical_rebuild_within(
        &self,
        conversation_id: i64,
        max_bytes: usize,
    ) -> Result<Option<Vec<Message>>> {
        let mut messages = Vec::new();
        let mut bytes = 0usize;
        let completed =
            self.for_each_lexical_rebuild_message(conversation_id, None, |message| {
                bytes = bytes.saturating_add(message.content.len());
                if bytes > max_bytes {
                    return Ok(false);
                }
                messages.push(message);
                Ok(true)
            })?;
        Ok(completed.then_some(messages))
    }

    /// Messages with `first_idx <= idx <= last_idx`, in `idx` order, each
    /// capped as in [`Self::fetch_messages_for_lexical_rebuild`].
    pub fn fetch_messages_for_lexical_rebuild_idx_range(
        &self,
        conversation_id: i64,
        first_idx: i64,
        last_idx: i64,
    ) -> Result<Vec<Message>> {
        let mut messages = Vec::new();
        self.for_each_lexical_rebuild_message(
            conversation_id,
            Some((first_idx, last_idx)),
            |message| {
                messages.push(message);
                Ok(true)
            },
        )?;
        Ok(messages)
    }

    /// Every message with `id > after_message_id`, as `(conversation_id,
    /// message)` in id order, each capped as in
    /// [`Self::fetch_messages_for_lexical_rebuild`]: a rowid-range scan that
    /// reads only rows appended after a memo, never the older text of the
    /// conversations they belong to.
    pub(crate) fn for_each_lexical_rebuild_message_after_id<F>(
        &self,
        after_message_id: i64,
        mut f: F,
    ) -> Result<()>
    where
        F: FnMut(i64, Message) -> Result<()>,
    {
        let cap = lexical_max_message_content_bytes().min(i32::MAX as usize);
        let sql = format!(
            "SELECT id, idx, role, author, created_at, \
                 substr(content, 1, {cap}), COALESCE(octet_length(content), 0), conversation_id \
                 FROM messages WHERE id > ?1 ORDER BY id"
        );
        let mut callback_error = None;
        let outcome = self.conn.query_with_params_for_each(
            &sql,
            &[SqliteValue::from(after_message_id)],
            |row| {
                let conversation_id: i64 = row.get_typed(7)?;
                let message = lexical_rebuild_message_from_row(row, cap)?;
                if let Err(err) = f(conversation_id, message) {
                    callback_error = Some(err);
                    return Err(crate::franken_sync::FrankenError::Internal(
                        LEXICAL_REBUILD_STREAM_STOPPED.to_string(),
                    ));
                }
                Ok(())
            },
        );
        if let Some(err) = callback_error {
            return Err(err);
        }
        outcome.with_context(|| {
            format!("streaming lexical messages appended after message id {after_message_id}")
        })
    }

    /// `(idx, indexed text bytes)` for every message of a conversation in
    /// `idx` order, read from record metadata without the text itself: the
    /// input for splitting a long conversation into bounded chunks.
    pub fn lexical_rebuild_message_footprints(
        &self,
        conversation_id: i64,
    ) -> Result<Vec<(i64, usize)>> {
        let cap = lexical_max_message_content_bytes().min(i32::MAX as usize);
        let sql = |hinted: bool| {
            format!(
                "SELECT idx, COALESCE(octet_length(content), 0) FROM messages{} \
                 WHERE conversation_id = ?1 ORDER BY idx",
                if hinted {
                    " INDEXED BY idx_messages_conv_idx"
                } else {
                    ""
                }
            )
        };
        let params = [SqliteValue::from(conversation_id)];
        let read = |sql: &str| -> Result<Vec<(i64, usize)>> {
            let mut footprints = Vec::new();
            self.conn
                .query_with_params_for_each(sql, &params, |row| {
                    let bytes = usize::try_from(row.get_typed::<i64>(1)?.max(0))
                        .unwrap_or(usize::MAX)
                        .min(cap);
                    footprints.push((row.get_typed::<i64>(0)?, bytes));
                    Ok(())
                })
                .with_context(|| {
                    format!("reading lexical footprints for conversation {conversation_id}")
                })?;
            Ok(footprints)
        };
        read(&sql(true)).or_else(|err| {
            if format!("{err:#}").contains("no such index: idx_messages_conv_idx") {
                return read(&sql(false));
            }
            Err(err)
        })
    }

    /// Stream a conversation's lexical projections (optionally one inclusive
    /// `idx` range) through the row callback instead of collecting the
    /// complete result. SQL bounds each projected text cell by `cap`
    /// Unicode scalar values and the callback applies the stricter per-message
    /// UTF-8 byte cap before handing the row on. `f` returns `false` to stop
    /// early; the return value says whether every row was visited.
    pub(crate) fn for_each_lexical_rebuild_message<F>(
        &self,
        conversation_id: i64,
        idx_range: Option<(i64, i64)>,
        mut f: F,
    ) -> Result<bool>
    where
        F: FnMut(Message) -> Result<bool>,
    {
        // `substr(content, 1, cap)` uses a literal signed-32-bit prefix
        // length. This value is parsed from our own numeric cap, never from
        // SQL/user text, so embedding it cannot introduce SQL injection.
        let cap = lexical_max_message_content_bytes().min(i32::MAX as usize);
        let range_clause = if idx_range.is_some() {
            " AND idx >= ?2 AND idx <= ?3"
        } else {
            ""
        };
        let sql = |hinted: bool| {
            format!(
                "SELECT id, idx, role, author, created_at, \
                     substr(content, 1, {cap}), COALESCE(octet_length(content), 0) \
                     FROM messages{} \
                     WHERE conversation_id = ?1{range_clause} ORDER BY idx",
                if hinted {
                    " INDEXED BY idx_messages_conv_idx"
                } else {
                    ""
                }
            )
        };
        let mut params = vec![SqliteValue::from(conversation_id)];
        if let Some((first_idx, last_idx)) = idx_range {
            params.push(SqliteValue::from(first_idx));
            params.push(SqliteValue::from(last_idx));
        }
        let mut run = |sql: &str| -> Result<bool> {
            let mut stopped = false;
            let mut callback_error = None;
            let outcome = self.conn.query_with_params_for_each(sql, &params, |row| {
                let message = lexical_rebuild_message_from_row(row, cap)?;
                match f(message) {
                    Ok(true) => Ok(()),
                    Ok(false) => {
                        stopped = true;
                        Err(crate::franken_sync::FrankenError::Internal(
                            LEXICAL_REBUILD_STREAM_STOPPED.to_string(),
                        ))
                    }
                    Err(err) => {
                        callback_error = Some(err);
                        Err(crate::franken_sync::FrankenError::Internal(
                            LEXICAL_REBUILD_STREAM_STOPPED.to_string(),
                        ))
                    }
                }
            });
            if let Some(err) = callback_error {
                return Err(err);
            }
            match outcome {
                Ok(()) => Ok(true),
                Err(_) if stopped => Ok(false),
                Err(err) => Err(anyhow::Error::new(err).context(format!(
                    "streaming bounded lexical rebuild content for conversation {conversation_id}"
                ))),
            }
        };
        run(&sql(true)).or_else(|err| {
            if format!("{err:#}").contains("no such index: idx_messages_conv_idx") {
                return run(&sql(false));
            }
            Err(err)
        })
    }

    /// Fetch messages for multiple conversations during lexical rebuilds.
    ///
    /// This preserves the lightweight lexical-rebuild projection while avoiding
    /// one round-trip per conversation when rebuilding large canonical indexes.
    pub fn fetch_messages_for_lexical_rebuild_batch(
        &self,
        conversation_ids: &[i64],
        max_messages: Option<usize>,
        max_content_bytes: Option<usize>,
    ) -> Result<HashMap<i64, Vec<Message>>> {
        if conversation_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let mut grouped: HashMap<i64, Vec<Message>> =
            HashMap::with_capacity(conversation_ids.len());
        let mut fetched_conversation_ids = HashSet::with_capacity(conversation_ids.len());
        let mut total_messages = 0usize;
        let mut total_content_bytes = 0usize;

        // Reuse the hinted per-conversation primary-key lookup instead of a
        // broad `WHERE conversation_id IN (...) ORDER BY ...` query on large
        // live databases.
        for conversation_id in conversation_ids {
            if !fetched_conversation_ids.insert(*conversation_id) {
                continue;
            }

            // With a byte budget, stop reading a conversation as soon as it
            // cannot fit: a long conversation is never materialized only to be
            // rejected here (the page-prep fallback then streams it in chunks).
            let messages = match max_content_bytes {
                Some(limit) => self
                    .fetch_messages_for_lexical_rebuild_within(
                        *conversation_id,
                        limit.saturating_sub(total_content_bytes),
                    )
                    .with_context(|| {
                        format!(
                            "fetching lexical rebuild messages for conversation {conversation_id}"
                        )
                    })?
                    .ok_or_else(|| {
                        anyhow!(
                            "lexical rebuild batch fetch exceeded content-byte guardrail: bytes>{} limit={limit} conversations={}",
                            limit.saturating_sub(total_content_bytes),
                            conversation_ids.len()
                        )
                    })?,
                None => self
                    .fetch_messages_for_lexical_rebuild(*conversation_id)
                    .with_context(|| {
                        format!(
                            "fetching lexical rebuild messages for conversation {conversation_id}"
                        )
                    })?,
            };
            total_messages = total_messages.saturating_add(messages.len());
            if let Some(limit) = max_messages
                && total_messages > limit
            {
                return Err(anyhow!(
                    "lexical rebuild batch fetch exceeded message guardrail: messages={total_messages} limit={limit} conversations={}",
                    conversation_ids.len()
                ));
            }

            let message_bytes = messages
                .iter()
                .map(|message| message.content.len())
                .sum::<usize>();
            total_content_bytes = total_content_bytes.saturating_add(message_bytes);
            if let Some(limit) = max_content_bytes
                && total_content_bytes > limit
            {
                return Err(anyhow!(
                    "lexical rebuild batch fetch exceeded content-byte guardrail: bytes={total_content_bytes} limit={limit} conversations={}",
                    conversation_ids.len()
                ));
            }

            if !messages.is_empty() {
                grouped.insert(*conversation_id, messages);
            }
        }

        Ok(grouped)
    }

    /// Stream lexical rebuild message rows in `(conversation_id, idx)` order
    /// without materializing the full result set.
    pub fn stream_messages_for_lexical_rebuild_between_conversation_ids<F>(
        &self,
        start_conversation_id: i64,
        end_conversation_id: i64,
        mut f: F,
    ) -> Result<()>
    where
        F: FnMut(LexicalRebuildMessageRow) -> Result<()>,
    {
        if end_conversation_id < start_conversation_id {
            return Ok(());
        }

        let conversation_ids: Vec<i64> = self
            .conn
            .query_map_collect(
                "SELECT id FROM conversations WHERE id >= ?1 AND id <= ?2 ORDER BY id ASC",
                fparams![start_conversation_id, end_conversation_id],
                |row| row.get_typed(0),
            )
            .with_context(|| "listing conversation ids for streamed lexical rebuild")?;

        for conversation_id in conversation_ids {
            let messages = self
                .fetch_messages_for_lexical_rebuild(conversation_id)
                .with_context(|| {
                    format!("streaming lexical rebuild messages for conversation {conversation_id}")
                })?;

            for message in messages {
                let message_id = message.id.ok_or_else(|| {
                    anyhow!(
                        "lexical rebuild message missing id for conversation {conversation_id} idx {}",
                        message.idx
                    )
                })?;
                f(LexicalRebuildMessageRow {
                    conversation_id,
                    id: message_id,
                    idx: message.idx,
                    role: role_str(&message.role),
                    author: message.author,
                    created_at: message.created_at,
                    content: message.content,
                })?;
            }
        }

        Ok(())
    }

    /// Stream grouped lexical rebuild message rows in `(conversation_id, idx)`
    /// order by reusing the canonical per-message stream and coalescing rows
    /// per conversation.
    pub fn stream_grouped_messages_for_lexical_rebuild_between_conversation_ids<F>(
        &self,
        start_conversation_id: i64,
        end_conversation_id: i64,
        mut f: F,
    ) -> Result<()>
    where
        F: FnMut(i64, LexicalRebuildGroupedMessageRows, i64) -> Result<()>,
    {
        if end_conversation_id < start_conversation_id {
            return Ok(());
        }

        let mut current_conversation_id: Option<i64> = None;
        let mut current_messages: LexicalRebuildGroupedMessageRows = SmallVec::new();
        let mut current_last_message_id = 0i64;
        let mut flush_current = |current_conversation_id: &mut Option<i64>,
                                 current_messages: &mut LexicalRebuildGroupedMessageRows,
                                 current_last_message_id: &mut i64|
         -> Result<()> {
            let Some(conversation_id) = current_conversation_id.take() else {
                return Ok(());
            };
            let messages = std::mem::take(current_messages);
            let last_message_id = std::mem::take(current_last_message_id);
            f(conversation_id, messages, last_message_id)
        };

        self.stream_messages_for_lexical_rebuild_between_conversation_ids(
            start_conversation_id,
            end_conversation_id,
            |row| {
                if current_conversation_id != Some(row.conversation_id) {
                    flush_current(
                        &mut current_conversation_id,
                        &mut current_messages,
                        &mut current_last_message_id,
                    )?;
                    current_conversation_id = Some(row.conversation_id);
                }
                current_last_message_id = row.id;
                current_messages.push(LexicalRebuildGroupedMessageRow {
                    idx: row.idx,
                    is_tool_role: row.role == "tool",
                    created_at: row.created_at,
                    content: row.content,
                });
                Ok(())
            },
        )
        .with_context(|| "streaming grouped lexical rebuild messages")?;

        flush_current(
            &mut current_conversation_id,
            &mut current_messages,
            &mut current_last_message_id,
        )
        .with_context(|| "flushing grouped lexical rebuild messages")
    }

    /// Stream grouped lexical rebuild message rows from a starting conversation
    /// id to the end of the table.
    pub fn stream_grouped_messages_for_lexical_rebuild_from_conversation_id<F>(
        &self,
        start_conversation_id: i64,
        f: F,
    ) -> Result<()>
    where
        F: FnMut(i64, LexicalRebuildGroupedMessageRows, i64) -> Result<()>,
    {
        self.stream_grouped_messages_for_lexical_rebuild_between_conversation_ids(
            start_conversation_id,
            i64::MAX,
            f,
        )
    }

    /// Stream lexical rebuild message rows from a starting conversation id to
    /// the end of the table.
    pub fn stream_messages_for_lexical_rebuild_from_conversation_id<F>(
        &self,
        start_conversation_id: i64,
        f: F,
    ) -> Result<()>
    where
        F: FnMut(LexicalRebuildMessageRow) -> Result<()>,
    {
        self.stream_messages_for_lexical_rebuild_between_conversation_ids(
            start_conversation_id,
            i64::MAX,
            f,
        )
    }
}
