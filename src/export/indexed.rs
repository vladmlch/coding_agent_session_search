//! Index-first export resolution.
//!
//! Loads the canonical indexed conversation for an export request, treating
//! the supplied source path as an opaque identity key. Never opens, sniffs,
//! or decompresses the original agent file: all content comes from the
//! canonical archive, opened read-only in a single consistent snapshot.
//!
//! SDD implementation for `docs/superpowers/plans/2026-11-02-index-first-export.md`
//! (Task 2). Keep these signatures stable — `run_export`,
//! `run_export_html` and the TUI export tasks are wired against them.

use std::path::{Path, PathBuf};

use crate::franken_sync::Row;
use crate::franken_sync::compat::{ConnectionExt, RowExt, TransactionExt};
use crate::storage::sqlite::FrankenStorage;
use crate::ui::data::{ConversationView, load_conversation_by_id_uncached};

/// Identity of the requested conversation. `source_id` is the normalized
/// provenance id (`local`, a remote name); `conversation_id` is the canonical
/// archive row id used by TUI search hits and `--conversation-id`.
#[derive(Debug, Clone)]
pub(crate) struct ExportSelector {
    pub source_path: PathBuf,
    pub source_id: Option<String>,
    pub conversation_id: Option<i64>,
}

/// One candidate row that matched `source_path` during ambiguous resolution,
/// surfaced so callers can print actionable disambiguation.
#[derive(Debug, Clone)]
pub(crate) struct AmbiguousCandidate {
    pub conversation_id: Option<i64>,
    pub title: Option<String>,
    pub started_at: Option<i64>,
    pub source_id: String,
}

/// Typed failures of index-only resolution. Callers map these onto
/// `CliError`/`CliErrorKind` or TUI error shapes. No fallback to the
/// original source file is permitted on any of these.
///
/// `Display`/`Error` are implemented manually (the `ManifestError`
/// precedent): `thiserror` cannot derive a variant whose `source` field is
/// a `String`, since it reserves that field name for `Error::source`
/// chaining. The rendered text matches the SDD stub contract.
#[derive(Debug)]
pub(crate) enum IndexedExportError {
    /// The archive itself is missing, unreadable, or corrupt. Opening must
    /// not create, repair, or migrate the database.
    IndexUnavailable { path: PathBuf, source: String },
    /// Archive opened fine, but no conversation matches the selector.
    ConversationNotIndexed {
        source_path: PathBuf,
        source_id: Option<String>,
    },
    /// A selector without `conversation_id` matched more than one indexed
    /// conversation. Never silently pick the newest row.
    AmbiguousIdentity {
        source_path: PathBuf,
        candidates: Vec<AmbiguousCandidate>,
    },
    /// Archive opened but reading the conversation/messages failed.
    IndexReadFailure { path: PathBuf, source: String },
}

impl std::fmt::Display for IndexedExportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IndexUnavailable { path, source } => {
                write!(f, "index unavailable at {}: {source}", path.display())
            }
            Self::ConversationNotIndexed {
                source_path,
                source_id,
            } => write!(
                f,
                "no indexed conversation for {} (source {source_id:?})",
                source_path.display()
            ),
            Self::AmbiguousIdentity {
                source_path,
                candidates,
            } => write!(
                f,
                "ambiguous identity for {}: {candidates:?}",
                source_path.display()
            ),
            Self::IndexReadFailure { path, source } => {
                write!(f, "index read failure at {}: {source}", path.display())
            }
        }
    }
}

impl std::error::Error for IndexedExportError {}

/// Normalized source-identity expression for `conversations`, mirrored
/// verbatim from `src/ui/data.rs::normalized_ui_source_identity_sql_expr`
/// (which is module-private and unreachable from here) — identical to
/// `src/lib.rs::normalized_source_identity_sql_expr`.
///
/// `source_id` wins when non-blank; a blank `source_id` falls back to
/// `origin_host` and then to `local`. Any case variant of `local` folds to
/// `local`. Do not weaken this: remote/local path collisions depend on it.
fn normalized_source_identity_sql_expr(source_id_column: &str, origin_host_column: &str) -> String {
    format!(
        "CASE WHEN TRIM(COALESCE({source_id_column}, '')) = '' THEN CASE WHEN TRIM(COALESCE({origin_host_column}, '')) = '' THEN '{local}' ELSE TRIM(COALESCE({origin_host_column}, '')) END \
         WHEN LOWER(TRIM(COALESCE({source_id_column}, ''))) = '{local}' THEN '{local}' \
         ELSE TRIM(COALESCE({source_id_column}, '')) END",
        local = crate::sources::provenance::LOCAL_SOURCE_ID,
    )
}

/// Normalized origin-kind expression, mirrored verbatim from
/// `src/ui/data.rs::normalized_ui_source_origin_kind_sql_expr` (module
/// private). The registered `sources.kind` classification is authoritative
/// when a `sources` row exists; `source_id`/`origin_host` are the fallback
/// signal for archives predating the registry.
fn normalized_source_origin_kind_sql_expr(
    source_id_column: &str,
    origin_kind_column: &str,
    origin_host_column: &str,
) -> String {
    format!(
        "CASE \
            WHEN LOWER(TRIM(COALESCE({origin_kind_column}, ''))) = '{local}' THEN '{local}' \
            WHEN TRIM(COALESCE({origin_kind_column}, '')) != '' THEN 'remote' \
            WHEN LOWER(TRIM(COALESCE({source_id_column}, ''))) = '{local}' THEN '{local}' \
            WHEN TRIM(COALESCE({source_id_column}, '')) != '' THEN 'remote' \
            WHEN TRIM(COALESCE({origin_host_column}, '')) != '' THEN 'remote' \
            ELSE '{local}' \
         END",
        local = crate::sources::provenance::LOCAL_SOURCE_ID,
    )
}

/// Normalize the selector's `source_id` the same way `run_export` does via
/// `src/lib.rs::canonical_followup_source_id` (module private): trim, map any
/// case variant of `local` to `local`, and treat empty/absent as unscoped
/// (`None`), which then resolves with the local-first semantics below.
fn normalize_selector_source_id(source_id: Option<&str>) -> Option<String> {
    let trimmed = source_id?.trim();
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.eq_ignore_ascii_case(crate::sources::provenance::LOCAL_SOURCE_ID) {
        Some(crate::sources::provenance::LOCAL_SOURCE_ID.to_string())
    } else {
        Some(trimmed.to_string())
    }
}

/// One `conversations` row matching the selector's `source_path`, carrying
/// its normalized provenance for scoped matching / disambiguation.
#[derive(Debug)]
struct IdentityCandidate {
    candidate: AmbiguousCandidate,
    /// `normalized_source_origin_kind_sql_expr` result: `local` or `remote`.
    normalized_origin_kind: String,
}

/// Collect every `conversations` row whose `source_path` equals the
/// selector's, with normalized identity columns. When `source_id` is
/// `Some`, the rows are additionally filtered to that exact normalized
/// identity. Ordered deterministically for the `AmbiguousIdentity` report;
/// the caller never picks from this list by position.
fn identity_candidates(
    storage: &FrankenStorage,
    source_path: &str,
    source_id: Option<&str>,
) -> Result<Vec<IdentityCandidate>, crate::franken_sync::FrankenError> {
    let identity_sql = normalized_source_identity_sql_expr("c.source_id", "c.origin_host");
    let kind_sql = normalized_source_origin_kind_sql_expr("c.source_id", "s.kind", "c.origin_host");
    let (sql, params) = if let Some(source_id) = source_id {
        (
            format!(
                "SELECT c.id, c.title, c.started_at, {identity_sql}, {kind_sql} \
                 FROM conversations c \
                 LEFT JOIN sources s ON c.source_id = s.id \
                 WHERE c.source_path = ?1 AND {identity_sql} = ?2 \
                 ORDER BY c.started_at DESC, c.id"
            ),
            crate::franken_sync::params![source_path, source_id],
        )
    } else {
        (
            format!(
                "SELECT c.id, c.title, c.started_at, {identity_sql}, {kind_sql} \
                 FROM conversations c \
                 LEFT JOIN sources s ON c.source_id = s.id \
                 WHERE c.source_path = ?1 \
                 ORDER BY c.started_at DESC, c.id"
            ),
            crate::franken_sync::params![source_path],
        )
    };
    storage.raw().query_map_collect(&sql, params, |row: &Row| {
        Ok(IdentityCandidate {
            candidate: AmbiguousCandidate {
                conversation_id: Some(row.get_typed::<i64>(0)?),
                title: row.get_typed::<Option<String>>(1)?,
                started_at: row.get_typed::<Option<i64>>(2)?,
                source_id: row.get_typed::<String>(3)?,
            },
            normalized_origin_kind: row.get_typed::<String>(4)?,
        })
    })
}

/// Load the conversation row named by `conversation_id` and verify it
/// belongs to the selector's source identity — never substituting whatever
/// the id happens to point at (stale TUI hits, a different archive).
fn resolve_by_conversation_id(
    storage: &FrankenStorage,
    archive_path: &Path,
    selector: &ExportSelector,
    conversation_id: i64,
    normalized_source_id: Option<&str>,
    source_path: &str,
) -> Result<ConversationView, IndexedExportError> {
    let view = load_conversation_by_id_uncached(storage, conversation_id).map_err(|err| {
        IndexedExportError::IndexReadFailure {
            path: archive_path.to_path_buf(),
            source: format!("{err:#}"),
        }
    })?;
    let Some(view) = view else {
        return Err(IndexedExportError::ConversationNotIndexed {
            source_path: selector.source_path.clone(),
            source_id: normalized_source_id.map(str::to_string),
        });
    };
    if view.convo.source_path.to_string_lossy() != source_path {
        return Err(IndexedExportError::ConversationNotIndexed {
            source_path: selector.source_path.clone(),
            source_id: normalized_source_id.map(str::to_string),
        });
    }
    // `view.convo.source_id` is already normalized by the UI row mapper
    // (blank `source_id` falls back through `origin_host` to `local`), so it
    // can be compared to the normalized selector value directly.
    if normalized_source_id.is_some_and(|expected| view.convo.source_id != expected) {
        return Err(IndexedExportError::ConversationNotIndexed {
            source_path: selector.source_path.clone(),
            source_id: normalized_source_id.map(str::to_string),
        });
    }
    Ok(view)
}

/// Resolve by path + source identity. With an explicit `source_id`, rows are
/// exact matches or absent. Without one, the codebase's unscoped-lookup
/// semantics are local-first (`load_conversation_uncached` orders normalized
/// `local` kind before remote): local-kind candidates are the match set when
/// any exist, otherwise every same-path candidate is. Within the match set,
/// exactly one row may claim the identity — `0` is `ConversationNotIndexed`,
/// `>1` is `AmbiguousIdentity`; never an `ORDER BY ... LIMIT 1` pick.
fn resolve_by_identity(
    storage: &FrankenStorage,
    archive_path: &Path,
    selector: &ExportSelector,
    normalized_source_id: Option<&str>,
    source_path: &str,
) -> Result<ConversationView, IndexedExportError> {
    let candidates =
        identity_candidates(storage, source_path, normalized_source_id).map_err(|err| {
            IndexedExportError::IndexReadFailure {
                path: archive_path.to_path_buf(),
                source: format!("{err}"),
            }
        })?;

    let matched: Vec<&IdentityCandidate> = if normalized_source_id.is_some() {
        candidates.iter().collect()
    } else {
        let local: Vec<&IdentityCandidate> = candidates
            .iter()
            .filter(|candidate| {
                candidate.normalized_origin_kind == crate::sources::provenance::LOCAL_SOURCE_ID
            })
            .collect();
        if local.is_empty() {
            candidates.iter().collect()
        } else {
            local
        }
    };

    match matched.as_slice() {
        [] => Err(IndexedExportError::ConversationNotIndexed {
            source_path: selector.source_path.clone(),
            source_id: normalized_source_id.map(str::to_string),
        }),
        [only] => {
            let conversation_id = only.candidate.conversation_id.unwrap_or(-1);
            let view =
                load_conversation_by_id_uncached(storage, conversation_id).map_err(|err| {
                    IndexedExportError::IndexReadFailure {
                        path: archive_path.to_path_buf(),
                        source: format!("{err:#}"),
                    }
                })?;
            view.ok_or_else(|| IndexedExportError::ConversationNotIndexed {
                source_path: selector.source_path.clone(),
                source_id: normalized_source_id.map(str::to_string),
            })
        }
        many => Err(IndexedExportError::AmbiguousIdentity {
            source_path: selector.source_path.clone(),
            candidates: many
                .iter()
                .map(|candidate| candidate.candidate.clone())
                .collect(),
        }),
    }
}

/// Resolve the exact indexed conversation for `selector` from the canonical
/// archive at `archive_path`, opened via `FrankenStorage::open_strict_readonly`.
///
/// Contract (see plan, Task 2 + Invariants 1-3):
/// - never calls `source_path.exists()` or opens the original file;
/// - never creates/repairs/migrates the archive;
/// - metadata and messages are read in one consistent snapshot;
/// - ambiguity fails instead of `ORDER BY started_at DESC LIMIT 1` selection;
/// - a provided `conversation_id` must match `source_path`/`source_id` exactly.
pub(crate) fn load_indexed_export(
    archive_path: &Path,
    selector: &ExportSelector,
) -> Result<ConversationView, IndexedExportError> {
    // The archive path is the only filesystem object this resolver may
    // touch; `selector.source_path` is an opaque identity key and is never
    // stat'ed, opened, or extension-checked.
    if !archive_path.exists() {
        return Err(IndexedExportError::IndexUnavailable {
            path: archive_path.to_path_buf(),
            source: "archive file does not exist".to_string(),
        });
    }

    // `open_strict_readonly` — not `open_readonly`: the ordinary read opener
    // creates `doctor/locks/` objects and performs sanctioned repairs
    // (duplicate-FTS dedupe via the sqlite3 bridge, dirty-WAL checkpoint
    // recovery) before opening. The strict opener is the non-mutating read
    // lane (`SQLITE_OPEN_READ_ONLY`, no create/repair/migrate, no lock-file
    // creation); a corrupt, busy, or schema-conflicting archive surfaces as
    // an open error here instead of being healed under the export path.
    let storage = FrankenStorage::open_strict_readonly(archive_path).map_err(|err| {
        IndexedExportError::IndexUnavailable {
            path: archive_path.to_path_buf(),
            source: format!("{err:#}"),
        }
    })?;

    let normalized_source_id = normalize_selector_source_id(selector.source_id.as_deref());
    let source_path = selector.source_path.to_string_lossy().into_owned();

    // One consistent snapshot: a deferred read transaction pins the
    // candidate listing, the conversation row, and `fetch_messages` to the
    // same archive view, so a concurrent writer committing mid-resolution
    // cannot mix conversation metadata from one commit with messages from
    // another (the same pattern `run_timeline` uses for its snapshot).
    let mut snapshot =
        storage
            .raw()
            .transaction()
            .map_err(|err| IndexedExportError::IndexReadFailure {
                path: archive_path.to_path_buf(),
                source: format!("starting read snapshot: {err}"),
            })?;

    let resolved = if let Some(conversation_id) = selector.conversation_id {
        resolve_by_conversation_id(
            &storage,
            archive_path,
            selector,
            conversation_id,
            normalized_source_id.as_deref(),
            &source_path,
        )
    } else {
        resolve_by_identity(
            &storage,
            archive_path,
            selector,
            normalized_source_id.as_deref(),
            &source_path,
        )
    };

    match resolved {
        Ok(view) => {
            snapshot
                .commit()
                .map_err(|err| IndexedExportError::IndexReadFailure {
                    path: archive_path.to_path_buf(),
                    source: format!("finishing read snapshot: {err}"),
                })?;
            Ok(view)
        }
        Err(err) => {
            // Settle the read snapshot immediately rather than leaving a
            // lazy cleanup obligation on the connection.
            let _ = snapshot.rollback();
            Err(err)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
    use tempfile::TempDir;

    fn open_test_archive(dir: &TempDir) -> (PathBuf, FrankenStorage) {
        let db_path = dir.path().join("export-test.db");
        let storage = FrankenStorage::open(&db_path).expect("open test archive");
        (db_path, storage)
    }

    fn ensure_test_agent(storage: &FrankenStorage) -> i64 {
        storage
            .ensure_agent(&Agent {
                id: None,
                slug: "claude_code".into(),
                name: "Claude Code".into(),
                version: None,
                kind: AgentKind::Cli,
            })
            .expect("ensure agent")
    }

    fn test_message(idx: i64, role: MessageRole, content: &str) -> Message {
        Message {
            id: None,
            idx,
            role,
            author: None,
            created_at: Some(1_700_000_000_000 + idx),
            content: content.to_string(),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        }
    }

    fn test_conversation(
        source_path: &str,
        source_id: &str,
        external_id: &str,
        title: &str,
        started_at: i64,
        messages: Vec<Message>,
    ) -> Conversation {
        Conversation {
            id: None,
            agent_slug: "claude_code".into(),
            workspace: None,
            external_id: Some(external_id.to_string()),
            title: Some(title.to_string()),
            source_path: PathBuf::from(source_path),
            started_at: Some(started_at),
            ended_at: Some(started_at + 999),
            approx_tokens: None,
            metadata_json: serde_json::Value::Null,
            messages,
            source_id: source_id.to_string(),
            origin_host: None,
        }
    }

    fn test_remote_conversation(
        source_path: &str,
        source_id: &str,
        origin_host: &str,
        external_id: &str,
        started_at: i64,
        messages: Vec<Message>,
    ) -> Conversation {
        let mut conv = test_conversation(
            source_path,
            source_id,
            external_id,
            "remote session",
            started_at,
            messages,
        );
        conv.origin_host = Some(origin_host.to_string());
        conv.metadata_json = serde_json::json!({"cass": {"origin": {"kind": "ssh"}}});
        conv
    }

    fn seed_conversation(storage: &FrankenStorage, agent_id: i64, conv: &Conversation) -> i64 {
        storage
            .insert_conversation_tree(agent_id, None, conv)
            .expect("seed conversation")
            .conversation_id
    }

    fn selector(
        source_path: &str,
        source_id: Option<&str>,
        conversation_id: Option<i64>,
    ) -> ExportSelector {
        ExportSelector {
            source_path: PathBuf::from(source_path),
            source_id: source_id.map(str::to_string),
            conversation_id,
        }
    }

    #[test]
    fn resolves_local_conversation_unscoped() {
        let dir = TempDir::new().unwrap();
        let (db_path, storage) = open_test_archive(&dir);
        let agent_id = ensure_test_agent(&storage);
        // The source path is never created on disk: export must resolve
        // purely from the archive.
        let convo_id = seed_conversation(
            &storage,
            agent_id,
            &test_conversation(
                "/nonexistent/dir/session.jsonl",
                "local",
                "local-ext-1",
                "local session",
                1_700_000_000_000,
                vec![
                    test_message(0, MessageRole::User, "hello"),
                    test_message(1, MessageRole::Agent, "hi there"),
                ],
            ),
        );
        storage.close().expect("close seed archive");

        let view = load_indexed_export(
            &db_path,
            &selector("/nonexistent/dir/session.jsonl", None, None),
        )
        .expect("indexed local conversation resolves");

        assert_eq!(view.convo.id, Some(convo_id));
        assert_eq!(view.convo.title.as_deref(), Some("local session"));
        assert_eq!(view.convo.source_id, "local");
        assert_eq!(
            view.messages.iter().map(|m| m.idx).collect::<Vec<_>>(),
            vec![0, 1],
            "fetch_messages orders by idx"
        );
        assert_eq!(view.messages[0].content, "hello");
        assert_eq!(view.messages[1].content, "hi there");
    }

    #[test]
    fn resolves_local_conversation_with_explicit_local_source() {
        let dir = TempDir::new().unwrap();
        let (db_path, storage) = open_test_archive(&dir);
        let agent_id = ensure_test_agent(&storage);
        seed_conversation(
            &storage,
            agent_id,
            &test_conversation(
                "/sessions/a.jsonl",
                "local",
                "local-ext-2",
                "scoped local",
                1_700_000_100_000,
                vec![test_message(0, MessageRole::User, "scoped")],
            ),
        );
        storage.close().expect("close seed archive");

        let view = load_indexed_export(
            &db_path,
            &selector("/sessions/a.jsonl", Some("local"), None),
        )
        .expect("explicit local source resolves");
        assert_eq!(view.messages[0].content, "scoped");
    }

    #[test]
    fn resolves_remote_conversation_by_source_id() {
        let dir = TempDir::new().unwrap();
        let (db_path, storage) = open_test_archive(&dir);
        let agent_id = ensure_test_agent(&storage);
        seed_conversation(
            &storage,
            agent_id,
            &test_remote_conversation(
                "/shared/session.jsonl",
                "work-laptop",
                "builder-5",
                "remote-ext-1",
                1_700_000_200_000,
                vec![test_message(0, MessageRole::User, "remote body")],
            ),
        );
        storage.close().expect("close seed archive");

        let view = load_indexed_export(
            &db_path,
            &selector("/shared/session.jsonl", Some("work-laptop"), None),
        )
        .expect("remote source resolves");
        assert_eq!(view.convo.source_id, "work-laptop");
        assert_eq!(view.messages[0].content, "remote body");

        let err = load_indexed_export(
            &db_path,
            &selector("/shared/session.jsonl", Some("other-host"), None),
        )
        .expect_err("a different source id must not match");
        assert!(matches!(
            err,
            IndexedExportError::ConversationNotIndexed { .. }
        ));
    }

    #[test]
    fn unscoped_lookup_prefers_local_over_remote_same_path() {
        let dir = TempDir::new().unwrap();
        let (db_path, storage) = open_test_archive(&dir);
        let agent_id = ensure_test_agent(&storage);
        let local_id = seed_conversation(
            &storage,
            agent_id,
            &test_conversation(
                "/collide/session.jsonl",
                "local",
                "local-ext-3",
                "local copy",
                1_700_000_300_000,
                vec![test_message(0, MessageRole::User, "local body")],
            ),
        );
        let remote_id = seed_conversation(
            &storage,
            agent_id,
            &test_remote_conversation(
                "/collide/session.jsonl",
                "work-laptop",
                "builder-5",
                "remote-ext-3",
                1_700_000_300_100,
                vec![test_message(0, MessageRole::User, "remote body")],
            ),
        );
        storage.close().expect("close seed archive");

        // Unscoped: local-first semantics pick the local copy even though a
        // remote row shares the path.
        let view = load_indexed_export(&db_path, &selector("/collide/session.jsonl", None, None))
            .expect("unscoped resolves the local candidate");
        assert_eq!(view.convo.id, Some(local_id));

        // Scoped to the remote source, the remote copy resolves exactly.
        let view = load_indexed_export(
            &db_path,
            &selector("/collide/session.jsonl", Some("work-laptop"), None),
        )
        .expect("remote-scoped resolves the remote candidate");
        assert_eq!(view.convo.id, Some(remote_id));
    }

    #[test]
    fn unscoped_lookup_resolves_single_remote_when_no_local() {
        let dir = TempDir::new().unwrap();
        let (db_path, storage) = open_test_archive(&dir);
        let agent_id = ensure_test_agent(&storage);
        let remote_id = seed_conversation(
            &storage,
            agent_id,
            &test_remote_conversation(
                "/remote-only/session.jsonl",
                "work-laptop",
                "builder-5",
                "remote-ext-4",
                1_700_000_400_000,
                vec![test_message(0, MessageRole::User, "remote only")],
            ),
        );
        storage.close().expect("close seed archive");

        let view = load_indexed_export(
            &db_path,
            &selector("/remote-only/session.jsonl", None, None),
        )
        .expect("a single remote candidate resolves unscoped");
        assert_eq!(view.convo.id, Some(remote_id));
    }

    #[test]
    fn missing_archive_is_index_unavailable() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("absent.db");
        let err = load_indexed_export(&db_path, &selector("/x.jsonl", None, None))
            .expect_err("missing archive must fail");
        assert!(
            matches!(err, IndexedExportError::IndexUnavailable { .. }),
            "unexpected error: {err:?}"
        );
        // The check must never create the archive it reports as missing.
        assert!(!db_path.exists());
    }

    #[test]
    fn corrupt_archive_is_index_unavailable() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("corrupt.db");
        std::fs::write(&db_path, b"this is not a sqlite archive").unwrap();
        let err = load_indexed_export(&db_path, &selector("/x.jsonl", None, None))
            .expect_err("corrupt archive must fail at open");
        assert!(
            matches!(err, IndexedExportError::IndexUnavailable { .. }),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn missing_conversation_is_not_indexed() {
        let dir = TempDir::new().unwrap();
        let (db_path, storage) = open_test_archive(&dir);
        storage.close().expect("close empty archive");
        let err = load_indexed_export(&db_path, &selector("/absent.jsonl", None, None))
            .expect_err("no conversation at that path");
        assert!(matches!(
            err,
            IndexedExportError::ConversationNotIndexed { .. }
        ));
    }

    #[test]
    fn ambiguous_same_path_same_source_errors() {
        let dir = TempDir::new().unwrap();
        let (db_path, storage) = open_test_archive(&dir);
        let agent_id = ensure_test_agent(&storage);
        // Distinct external ids -> distinct merge keys -> two indexed rows
        // claiming the same (source_path, source_id) identity.
        let first_id = seed_conversation(
            &storage,
            agent_id,
            &test_conversation(
                "/dupe/session.jsonl",
                "local",
                "dupe-ext-a",
                "first",
                1_700_000_500_000,
                vec![test_message(0, MessageRole::User, "first")],
            ),
        );
        let second_id = seed_conversation(
            &storage,
            agent_id,
            &test_conversation(
                "/dupe/session.jsonl",
                "local",
                "dupe-ext-b",
                "second",
                1_700_000_500_100,
                vec![test_message(0, MessageRole::User, "second")],
            ),
        );
        storage.close().expect("close seed archive");

        for source_id in [None, Some("local")] {
            let err =
                load_indexed_export(&db_path, &selector("/dupe/session.jsonl", source_id, None))
                    .expect_err("two claimants must never resolve silently");
            match err {
                IndexedExportError::AmbiguousIdentity { candidates, .. } => {
                    assert_eq!(candidates.len(), 2);
                    let ids: Vec<Option<i64>> =
                        candidates.iter().map(|c| c.conversation_id).collect();
                    assert!(ids.contains(&Some(first_id)));
                    assert!(ids.contains(&Some(second_id)));
                    assert!(
                        candidates.iter().all(|c| c.source_id == "local"),
                        "candidates carry normalized source ids: {candidates:?}"
                    );
                }
                other => panic!("expected AmbiguousIdentity, got {other:?}"),
            }
        }
    }

    #[test]
    fn conversation_id_resolves_exact_row() {
        let dir = TempDir::new().unwrap();
        let (db_path, storage) = open_test_archive(&dir);
        let agent_id = ensure_test_agent(&storage);
        let wanted = seed_conversation(
            &storage,
            agent_id,
            &test_conversation(
                "/exact/target.jsonl",
                "local",
                "exact-ext-1",
                "target",
                1_700_000_600_000,
                vec![test_message(0, MessageRole::User, "target body")],
            ),
        );
        seed_conversation(
            &storage,
            agent_id,
            &test_conversation(
                "/exact/other.jsonl",
                "local",
                "exact-ext-2",
                "other",
                1_700_000_600_100,
                vec![test_message(0, MessageRole::User, "other body")],
            ),
        );
        storage.close().expect("close seed archive");

        let view = load_indexed_export(
            &db_path,
            &selector("/exact/target.jsonl", None, Some(wanted)),
        )
        .expect("conversation_id resolves");
        assert_eq!(view.convo.id, Some(wanted));
        assert_eq!(view.messages[0].content, "target body");

        // The id still resolves when the source id is supplied and matches.
        let view = load_indexed_export(
            &db_path,
            &selector("/exact/target.jsonl", Some("local"), Some(wanted)),
        )
        .expect("conversation_id + matching source resolves");
        assert_eq!(view.convo.id, Some(wanted));
    }

    #[test]
    fn conversation_id_path_mismatch_is_not_indexed() {
        let dir = TempDir::new().unwrap();
        let (db_path, storage) = open_test_archive(&dir);
        let agent_id = ensure_test_agent(&storage);
        let other_id = seed_conversation(
            &storage,
            agent_id,
            &test_conversation(
                "/mismatch/other.jsonl",
                "local",
                "mismatch-ext-1",
                "other",
                1_700_000_700_000,
                vec![test_message(0, MessageRole::User, "other")],
            ),
        );
        storage.close().expect("close seed archive");

        // The id resolves but names a different source_path: the selector
        // must not inherit a conversation it did not ask for.
        let err = load_indexed_export(
            &db_path,
            &selector("/mismatch/requested.jsonl", None, Some(other_id)),
        )
        .expect_err("stale conversation_id must not substitute another path");
        assert!(matches!(
            err,
            IndexedExportError::ConversationNotIndexed { .. }
        ));

        // An id that exists in no row fails the same way.
        let err = load_indexed_export(
            &db_path,
            &selector("/mismatch/other.jsonl", None, Some(999_999)),
        )
        .expect_err("unknown conversation_id must not be indexed");
        assert!(matches!(
            err,
            IndexedExportError::ConversationNotIndexed { .. }
        ));
    }

    #[test]
    fn conversation_id_source_mismatch_is_not_indexed() {
        let dir = TempDir::new().unwrap();
        let (db_path, storage) = open_test_archive(&dir);
        let agent_id = ensure_test_agent(&storage);
        let remote_id = seed_conversation(
            &storage,
            agent_id,
            &test_remote_conversation(
                "/mismatch/remote.jsonl",
                "work-laptop",
                "builder-5",
                "mismatch-ext-2",
                1_700_000_800_000,
                vec![test_message(0, MessageRole::User, "remote")],
            ),
        );
        storage.close().expect("close seed archive");

        // Path matches but the normalized source identity does not.
        let err = load_indexed_export(
            &db_path,
            &selector("/mismatch/remote.jsonl", Some("local"), Some(remote_id)),
        )
        .expect_err("source mismatch must not pass");
        assert!(matches!(
            err,
            IndexedExportError::ConversationNotIndexed { .. }
        ));
    }

    #[test]
    fn readonly_resolution_does_not_modify_archive() {
        let dir = TempDir::new().unwrap();
        let (db_path, storage) = open_test_archive(&dir);
        let agent_id = ensure_test_agent(&storage);
        seed_conversation(
            &storage,
            agent_id,
            &test_conversation(
                "/readonly/session.jsonl",
                "local",
                "readonly-ext-1",
                "read only",
                1_700_000_900_000,
                vec![test_message(0, MessageRole::User, "body")],
            ),
        );
        storage.close().expect("close seed archive");

        let bytes_before = std::fs::read(&db_path).expect("read db before");
        let entries_before: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();

        load_indexed_export(&db_path, &selector("/readonly/session.jsonl", None, None))
            .expect("resolves read-only");

        let bytes_after = std::fs::read(&db_path).expect("read db after");
        assert_eq!(
            bytes_before, bytes_after,
            "read-only open must not modify archive bytes"
        );
        let entries_after: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            entries_before, entries_after,
            "no -wal/-shm/lock sidecars or directories may be created by a read-only resolve"
        );
    }
}
