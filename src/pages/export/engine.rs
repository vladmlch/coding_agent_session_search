use super::publish_guard::{acquire_export_publish_guard, unpredictable_atomic_sidecar_path};
use super::publish_replace::{
    cleanup_sqlite_temp_artifacts, create_staged_export_file,
    enforce_private_candidate_permissions, finalize_staged_sqlite_sidecars,
    reject_existing_sqlite_sidecars, replace_file_from_temp,
};
#[cfg(windows)]
use super::publish_replace::{
    recover_or_refuse_interrupted_export_publish, reject_non_regular_existing_publish_destination,
};
use super::sql_helpers::{
    MessageExportRow, build_message_export_query, derive_attachment_refs, derive_message_model,
    existing_regular_files_share_identity, normalize_optional_text, table_columns_in_transaction,
    table_exists_in_transaction,
};
use crate::franken_sync::compat::{ConnectionExt, ParamValue, RowExt, TransactionExt};
use crate::franken_sync::{Connection, Row as FrankenRow, params};
use crate::pages::profiles::ShareProfile;
use crate::pages::redact::RedactionEngine;
use crate::pages::summary::ExclusionSet;
use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use clap::ValueEnum;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug, Clone)]
pub struct ExportFilter {
    pub agents: Option<Vec<String>>,
    pub workspaces: Option<Vec<PathBuf>>,
    pub since: Option<DateTime<Utc>>,
    pub until: Option<DateTime<Utc>>,
    pub path_mode: PathMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum PathMode {
    Relative,
    Basename,
    Full,
    Hash,
}

pub struct ExportEngine {
    pub(super) source_db_path: PathBuf,
    pub(super) output_path: PathBuf,
    filter: ExportFilter,
    exclusions: ExclusionSet,
    share: Option<ShareRedaction>,
}

/// The share profile an export applies to every text it copies (2l1b0.60).
struct ShareRedaction {
    profile: ShareProfile,
    engine: RedactionEngine,
}

/// Redactions applied per kind (`home_path`, `username`, ...); recorded in
/// `export_meta` as counts only, never the values.
type RedactionCounts = std::collections::BTreeMap<&'static str, u64>;

#[derive(Debug)]
pub struct ExportStats {
    pub conversations_processed: usize,
    pub messages_processed: usize,
}

type SnippetExportRow = (
    Option<String>,
    Option<i64>,
    Option<i64>,
    Option<String>,
    String,
);

impl ExportEngine {
    pub fn new(source_db_path: &Path, output_path: &Path, filter: ExportFilter) -> Self {
        Self {
            source_db_path: source_db_path.to_path_buf(),
            output_path: output_path.to_path_buf(),
            filter,
            exclusions: ExclusionSet::new(),
            share: None,
        }
    }

    /// Apply wizard review exclusions to the rows eligible for this export.
    ///
    /// Direct and config-driven exports do not call this method, so their
    /// existing positive-filter behavior remains unchanged.
    pub fn with_exclusions(mut self, exclusions: ExclusionSet) -> Self {
        self.exclusions = exclusions;
        self
    }

    /// Redact every exported text with `profile`'s preset: conversation
    /// titles, workspace and source paths, metadata values, message bodies
    /// (and the search indexes built from them), attachment references and
    /// snippets. The profile and per-kind counts land in `export_meta`.
    pub fn with_share_profile(mut self, profile: ShareProfile) -> Self {
        self.share = Some(ShareRedaction {
            profile,
            engine: RedactionEngine::new(profile.export_redaction_config()),
        });
        self
    }

    fn share_redact<'a>(
        &self,
        value: &'a str,
        workspace: bool,
        counts: &mut RedactionCounts,
    ) -> std::borrow::Cow<'a, str> {
        let Some(share) = &self.share else {
            return std::borrow::Cow::Borrowed(value);
        };
        let redacted = if workspace {
            share.engine.redact_workspace(value)
        } else {
            share.engine.redact_text(value)
        };
        if redacted.changes.is_empty() {
            return std::borrow::Cow::Borrowed(value);
        }
        for change in &redacted.changes {
            *counts.entry(change.kind.label()).or_default() += 1;
        }
        std::borrow::Cow::Owned(redacted.output)
    }

    /// Redact the string values of a JSON document, keeping it valid JSON;
    /// text that does not parse is redacted as plain text.
    fn share_redact_json(&self, value: &str, counts: &mut RedactionCounts) -> String {
        if self.share.is_none() {
            return value.to_string();
        }
        fn walk(engine: &ExportEngine, node: &mut Value, counts: &mut RedactionCounts) {
            match node {
                Value::String(text) => {
                    let redacted = match engine.share_redact(text, false, counts) {
                        std::borrow::Cow::Owned(redacted) => Some(redacted),
                        std::borrow::Cow::Borrowed(_) => None,
                    };
                    if let Some(redacted) = redacted {
                        *text = redacted;
                    }
                }
                Value::Array(items) => {
                    for item in items {
                        walk(engine, item, counts);
                    }
                }
                Value::Object(map) => {
                    for item in map.values_mut() {
                        walk(engine, item, counts);
                    }
                }
                _ => {}
            }
        }
        match serde_json::from_str::<Value>(value) {
            Ok(mut document) => {
                let before: u64 = counts.values().sum();
                walk(self, &mut document, counts);
                if counts.values().sum::<u64>() == before {
                    value.to_string()
                } else {
                    document.to_string()
                }
            }
            Err(_) => self.share_redact(value, false, counts).into_owned(),
        }
    }

    pub fn execute<F>(&self, progress: F, running: Option<Arc<AtomicBool>>) -> Result<ExportStats>
    where
        F: Fn(usize, usize),
    {
        self.execute_verified(progress, running, |_| Ok(()))
            .map(|(stats, ())| stats)
    }

    /// Build the export in a private sidecar, verify those exact bytes, and
    /// only then publish them through the platform's verified replacement
    /// protocol at the requested output path.
    ///
    /// The verifier is deliberately invoked after the destination transaction
    /// is committed and closed but before `replace_file_from_temp`. A failed
    /// verifier therefore leaves any prior output untouched and prevents an
    /// unapproved generation from becoming visible.
    pub fn execute_verified<F, V, T>(
        &self,
        progress: F,
        running: Option<Arc<AtomicBool>>,
        verifier: V,
    ) -> Result<(ExportStats, T)>
    where
        F: Fn(usize, usize),
        V: FnOnce(&Path) -> Result<T>,
    {
        let output_path = resolve_export_output_path(&self.source_db_path, &self.output_path)?;
        let publish_guard = acquire_export_publish_guard(&output_path)?;
        #[cfg(windows)]
        let output_path = {
            recover_or_refuse_interrupted_export_publish(&output_path, &publish_guard)?;
            // Recovery can make a formerly absent destination name an alias
            // of the source (for example through a pre-existing hard link).
            // Re-run the source-identity proof against the restored entry
            // before opening either database.
            resolve_export_output_path(&self.source_db_path, &output_path)?
        };
        #[cfg(windows)]
        reject_non_regular_existing_publish_destination(&output_path)?;

        if output_path.exists() && output_path.is_dir() {
            bail!(
                "output path points to a directory, expected a file: {}",
                output_path.display()
            );
        }

        // 1. Open source DB
        let src = crate::pages::open_existing_sqlite_db(&self.source_db_path)
            .context("Failed to open source database")?;

        // 2. Build into a private writer database, then ask FrankenSQLite to
        // produce a separate, self-contained candidate with VACUUM INTO. A
        // brand-new on-disk connection permanently retains its bootstrap WAL;
        // the engine's bounded image contract therefore explicitly requires
        // VACUUM INTO rather than publishing an in-place writer database.
        let builder_path =
            unpredictable_atomic_sidecar_path(&output_path, "builder", "pages_export.db")?;
        let temp_output_path =
            unpredictable_atomic_sidecar_path(&output_path, "tmp", "pages_export.db")?;
        let mut retain_temp_on_replace_error = false;
        let mut builder_owned = false;
        let mut candidate_owned = false;
        let result = (|| -> Result<(ExportStats, T)> {
            create_staged_export_file(&builder_path)?;
            builder_owned = true;
            // Deliberately NOT named `output_path`: shadowing the publish
            // destination here once routed the final install onto the builder
            // path (gh#418).
            let builder_db_path = builder_path.to_string_lossy().to_string();
            let dest =
                Connection::open(&builder_db_path).context("Failed to create output database")?;

            dest.execute_batch(
                // Pages exports are encrypted/copied as one portable SQLite file.
                // WAL would allow committed schema/data to remain in a sidecar
                // that is not part of the encrypted payload.
                "PRAGMA journal_mode = 'delete';
                 PRAGMA synchronous = NORMAL;
                 PRAGMA busy_timeout = 5000;
                 PRAGMA foreign_keys = ON;",
            )
            .context("Failed to set destination database PRAGMAs")?;

            // Every source row that contributes to one export must come from
            // one SQLite generation. In particular, conversation counts and
            // the later per-conversation message/snippet reads must not straddle
            // concurrent indexing commits.
            let mut src_tx = src
                .transaction()
                .context("Failed to start source database read snapshot")?;
            let mut tx = match dest
                .transaction()
                .context("Failed to start destination export transaction")
            {
                Ok(tx) => tx,
                Err(destination_error) => {
                    return match src_tx
                        .rollback()
                        .context("Failed to close source database read snapshot")
                    {
                        Ok(()) => Err(destination_error),
                        Err(rollback_error) => Err(destination_error.context(format!(
                            "source read-snapshot rollback also failed: {rollback_error:#}"
                        ))),
                    };
                }
            };

            let export_result = (|| -> Result<(usize, usize)> {
                let message_cols = table_columns_in_transaction(&src_tx, "messages")?;
                let has_snippets_table = table_exists_in_transaction(&src_tx, "snippets")?;
                let msg_query = build_message_export_query(&message_cols);

                // 3. Create Schema (Split into individual statements)
                tx.execute(
                    "CREATE TABLE conversations (
                id INTEGER PRIMARY KEY,
                agent TEXT NOT NULL,
                workspace TEXT,
                title TEXT,
                source_path TEXT NOT NULL,
                started_at INTEGER,
                ended_at INTEGER,
                message_count INTEGER,
                metadata_json TEXT
            )",
                )
                .context("Failed to create conversations table")?;

                tx.execute(
                    "CREATE TABLE messages (
                id INTEGER PRIMARY KEY,
                conversation_id INTEGER NOT NULL,
                idx INTEGER NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                created_at INTEGER,
                updated_at INTEGER,
                model TEXT,
                attachment_refs TEXT,
                FOREIGN KEY (conversation_id) REFERENCES conversations(id)
            )",
                )
                .context("Failed to create messages table")?;

                tx.execute(
                    "CREATE TABLE snippets (
                id INTEGER PRIMARY KEY,
                message_id INTEGER NOT NULL,
                file_path TEXT,
                start_line INTEGER,
                end_line INTEGER,
                language TEXT,
                snippet_text TEXT,
                FOREIGN KEY (message_id) REFERENCES messages(id)
            )",
                )
                .context("Failed to create snippets table")?;

                tx.execute(
                    "CREATE TABLE export_meta (
                key TEXT PRIMARY KEY,
                value TEXT
            )",
                )
                .context("Failed to create export_meta table")?;

                tx.execute(
                    "CREATE VIRTUAL TABLE messages_fts USING fts5(
                content,
                tokenize='porter unicode61 remove_diacritics 2'
            )",
                )
                .context("Failed to create messages_fts table")?;

                tx.execute(
                    r#"CREATE VIRTUAL TABLE messages_code_fts USING fts5(
                content,
                tokenize="unicode61 tokenchars '-_./:@#$%\\'"
            )"#,
                )
                .context("Failed to create messages_code_fts table")?;

                // 4. Query Source.  LEFT JOIN + COALESCE on agents so the
                // export path includes legacy NULL-agent conversations
                // (otherwise the exported archive silently omits them).
                // Agent filter becomes an EXISTS guard against the agents
                // table so it works correctly without the joined column.
                let mut from_where = String::from(
                    " FROM conversations c
             LEFT JOIN agents a ON c.agent_id = a.id
             LEFT JOIN workspaces w ON c.workspace_id = w.id
             WHERE 1=1",
                );
                let mut params: Vec<ParamValue> = Vec::new();

                if let Some(agents) = &self.filter.agents {
                    if agents.is_empty() {
                        from_where.push_str(" AND 1=0");
                    } else {
                        from_where.push_str(" AND EXISTS (SELECT 1 FROM agents a2 WHERE a2.id = c.agent_id AND a2.slug IN (");
                        for (i, agent) in agents.iter().enumerate() {
                            if i > 0 {
                                from_where.push_str(", ");
                            }
                            from_where.push('?');
                            params.push(ParamValue::from(agent.clone()));
                        }
                        from_where.push_str("))");
                    }
                }

                // Note: Workspace filtering in source DB might be string matching if paths aren't normalized consistently.
                // Assuming strict matching for now.
                if let Some(workspaces) = &self.filter.workspaces {
                    if workspaces.is_empty() {
                        from_where.push_str(" AND 1=0");
                    } else {
                        from_where.push_str(" AND w.path IN (");
                        for (i, ws) in workspaces.iter().enumerate() {
                            if i > 0 {
                                from_where.push_str(", ");
                            }
                            from_where.push('?');
                            params.push(ParamValue::from(ws.to_string_lossy().to_string()));
                        }
                        from_where.push(')');
                    }
                }

                if let Some(since) = self.filter.since {
                    from_where.push_str(" AND c.started_at >= ?");
                    params.push(ParamValue::from(since.timestamp_millis()));
                }

                if let Some(until) = self.filter.until {
                    from_where.push_str(" AND c.started_at <= ?");
                    params.push(ParamValue::from(until.timestamp_millis()));
                }

                let query = format!(
                    "SELECT c.id, COALESCE(a.slug, 'unknown') as agent, w.path as workspace, c.title, c.source_path, c.started_at, c.ended_at,
             (SELECT COUNT(*) FROM messages m WHERE m.conversation_id = c.id) as message_count,
             c.metadata_json
             {from_where}
             ORDER BY c.id"
                );

                // Execute Main Query - collect all conversation rows
                type ConversationExportRow = (
                    i64,
                    String,
                    Option<String>,
                    Option<String>,
                    String,
                    Option<i64>,
                    Option<i64>,
                    i64,
                    Option<String>,
                );
                let mut conv_rows: Vec<ConversationExportRow> =
                    src_tx.query_map_collect(&query, &params, |row: &FrankenRow| {
                        Ok((
                            row.get_typed::<i64>(0)?,
                            row.get_typed::<String>(1)?,
                            row.get_typed::<Option<String>>(2)?,
                            row.get_typed::<Option<String>>(3)?,
                            row.get_typed::<String>(4)?,
                            row.get_typed::<Option<i64>>(5)?,
                            row.get_typed::<Option<i64>>(6)?,
                            row.get_typed::<i64>(7)?,
                            row.get_typed::<Option<String>>(8)?,
                        ))
                    })?;
                conv_rows.retain(|(id, _, workspace, title, _, _, _, _, _)| {
                    !self.exclusions.should_exclude(
                        workspace.as_deref(),
                        *id,
                        title.as_deref().unwrap_or(""),
                    )
                });
                let total_convs = conv_rows.len();

                let mut processed = 0;
                let mut msg_processed = 0;
                let mut redactions = RedactionCounts::new();

                for (
                    id,
                    agent,
                    workspace,
                    title,
                    source_path,
                    started_at,
                    ended_at,
                    message_count,
                    metadata_json,
                ) in &conv_rows
                {
                    if let Some(r) = &running
                        && !r.load(Ordering::Relaxed)
                    {
                        return Err(anyhow::anyhow!("Export cancelled"));
                    }

                    // Transform Path
                    let transformed_path = self.transform_path(source_path, workspace);
                    // 019i2: the workspace is a path too. Under the
                    // obfuscating modes (hash/basename) it must not survive
                    // verbatim — a "hidden metadata" bundle previously kept
                    // every exact local workspace path here.
                    let transformed_workspace = self.transform_workspace(workspace);
                    // 2l1b0.60: the share profile applies after the path
                    // policy, to every text the bundle carries.
                    let shared_workspace = transformed_workspace
                        .as_deref()
                        .map(|value| self.share_redact(value, true, &mut redactions).into_owned());
                    let shared_path = self
                        .share_redact(&transformed_path, false, &mut redactions)
                        .into_owned();
                    let shared_title = title.as_deref().map(|value| {
                        self.share_redact(value, false, &mut redactions)
                            .into_owned()
                    });
                    let shared_metadata = metadata_json
                        .as_deref()
                        .map(|value| self.share_redact_json(value, &mut redactions));

                    tx.execute_compat(
                    "INSERT INTO conversations (id, agent, workspace, title, source_path, started_at, ended_at, message_count, metadata_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                    params![
                        *id,
                        agent.as_str(),
                        shared_workspace.as_deref(),
                        shared_title.as_deref(),
                        shared_path.as_str(),
                        *started_at,
                        *ended_at,
                        *message_count,
                        shared_metadata.as_deref()
                    ],
                )?;

                    // Fetch messages for this conversation
                    let msg_rows: Vec<MessageExportRow> = src_tx.query_map_collect(
                        &msg_query,
                        crate::franken_sync::params![*id],
                        |row: &FrankenRow| {
                            Ok((
                                row.get_typed::<i64>(0)?,
                                row.get_typed::<String>(1)?,
                                row.get_typed::<String>(2)?,
                                row.get_typed::<Option<i64>>(3)?,
                                row.get_typed::<i64>(4)?,
                                row.get_typed::<Option<i64>>(5)?,
                                row.get_typed::<Option<String>>(6)?,
                                row.get_typed::<Option<String>>(7)?,
                                row.get_typed::<Option<String>>(8)?,
                            ))
                        },
                    )?;

                    for (
                        source_message_id,
                        role,
                        content,
                        created_at,
                        idx,
                        updated_at,
                        model,
                        attachment_refs,
                        extra_json,
                    ) in &msg_rows
                    {
                        let resolved_model = normalize_optional_text(model.clone())
                            .or_else(|| derive_message_model(extra_json.as_deref()));
                        let resolved_attachment_refs =
                            normalize_optional_text(attachment_refs.clone())
                                .or_else(|| derive_attachment_refs(extra_json.as_deref()))
                                .map(|refs| self.share_redact_json(&refs, &mut redactions));
                        // The search indexes are built from the same redacted
                        // text, so the bundle's FTS cannot find what the
                        // message body no longer shows.
                        let content = self.share_redact(content, false, &mut redactions);

                        tx.execute_compat(
                            "INSERT INTO messages (id, conversation_id, idx, role, content, created_at, updated_at, model, attachment_refs)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                            params![
                                *source_message_id,
                                *id,
                                *idx,
                                role.as_str(),
                                &*content,
                                *created_at,
                                *updated_at,
                                resolved_model.as_deref(),
                                resolved_attachment_refs.as_deref()
                            ],
                        )?;

                        // Populate FTS
                        tx.execute_compat(
                            "INSERT INTO messages_fts (rowid, content) VALUES (?1, ?2)",
                            params![*source_message_id, &*content],
                        )?;
                        tx.execute_compat(
                            "INSERT INTO messages_code_fts (rowid, content) VALUES (?1, ?2)",
                            params![*source_message_id, &*content],
                        )?;

                        // 5. Migrate Snippets for this message (bd-4x92)
                        let snip_rows: Vec<SnippetExportRow> = if has_snippets_table {
                            src_tx.query_map_collect(
                                "SELECT file_path, start_line, end_line, language, snippet_text FROM snippets WHERE message_id = ?1",
                                params![*source_message_id],
                                |row: &FrankenRow| {
                                    Ok((
                                        row.get_typed::<Option<String>>(0)?,
                                        row.get_typed::<Option<i64>>(1)?,
                                        row.get_typed::<Option<i64>>(2)?,
                                        row.get_typed::<Option<String>>(3)?,
                                        row.get_typed::<String>(4)?,
                                    ))
                                },
                            )?
                        } else {
                            Vec::new()
                        };

                        for (fpath, start, end, lang, stext) in snip_rows {
                            // 019i2: snippet file paths follow the same path
                            // policy as source paths (relative strips the
                            // workspace; hash/basename obfuscate).
                            let fpath = fpath.as_deref().map(|path| {
                                let path = self.transform_path(path, workspace);
                                self.share_redact(&path, false, &mut redactions)
                                    .into_owned()
                            });
                            let stext = self.share_redact(&stext, false, &mut redactions);
                            tx.execute_compat(
                                "INSERT INTO snippets (message_id, file_path, start_line, end_line, language, snippet_text)
                                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                                params![*source_message_id, fpath, start, end, lang, &*stext],
                            )?;
                        }

                        msg_processed += 1;
                    }

                    processed += 1;
                    progress(processed, total_convs);
                }

                // Metadata
                tx.execute("INSERT INTO export_meta (key, value) VALUES ('schema_version', '1')")?;
                // 019i2: record the realized path policy so verify/summary
                // surfaces can report what the bundle actually contains.
                tx.execute_compat(
                    "INSERT INTO export_meta (key, value) VALUES ('path_mode', ?1)",
                    params![self.filter.path_mode.as_meta_str()],
                )?;
                let exported_at = Utc::now().to_rfc3339();
                tx.execute_compat(
                    "INSERT INTO export_meta (key, value) VALUES ('exported_at', ?1)",
                    params![exported_at.as_str()],
                )?;
                // 2l1b0.60: which share profile shaped this bundle and how
                // many values of each kind it rewrote (counts, not values).
                if let Some(share) = &self.share {
                    tx.execute_compat(
                        "INSERT INTO export_meta (key, value) VALUES ('share_profile', ?1)",
                        params![share.profile.label()],
                    )?;
                    tx.execute_compat(
                        "INSERT INTO export_meta (key, value) VALUES ('share_redactions', ?1)",
                        params![serde_json::to_string(&redactions)?],
                    )?;
                }

                Ok((processed, msg_processed))
            })();
            let export_result = match export_result {
                Ok(stats) => match tx
                    .commit()
                    .context("Failed to commit completed destination export transaction")
                {
                    Ok(()) => Ok(stats),
                    Err(commit_error) => match tx
                        .rollback()
                        .context("Failed to roll back destination after commit failure")
                    {
                        Ok(()) => Err(commit_error),
                        Err(rollback_error) => Err(commit_error.context(format!(
                            "destination rollback also failed: {rollback_error:#}"
                        ))),
                    },
                },
                Err(export_error) => match tx
                    .rollback()
                    .context("Failed to roll back incomplete destination export transaction")
                {
                    Ok(()) => Err(export_error),
                    Err(rollback_error) => Err(export_error.context(format!(
                        "destination rollback also failed: {rollback_error:#}"
                    ))),
                },
            };
            // The transaction commits/rolls back through `&mut self`, so the
            // binding still borrows `dest` until it is dropped — and
            // `dest.close()` below moves the connection. End the borrow here.
            drop(tx);
            let source_rollback_result = src_tx
                .rollback()
                .context("Failed to close source database read snapshot");
            let (processed, msg_processed) = match (export_result, source_rollback_result) {
                (Ok(stats), Ok(())) => stats,
                (Err(export_error), Ok(())) => return Err(export_error),
                (Ok(_), Err(rollback_error)) => return Err(rollback_error),
                (Err(export_error), Err(rollback_error)) => {
                    return Err(export_error.context(format!(
                        "source read-snapshot rollback also failed: {rollback_error:#}"
                    )));
                }
            };

            let candidate_path = temp_output_path.to_string_lossy();
            dest.execute_compat("VACUUM INTO ?1;", params![candidate_path.as_ref()])
                .context("Failed to materialize self-contained Pages export candidate")?;
            candidate_owned = true;
            dest.close()
                .context("Failed to close and checkpoint Pages export builder")?;
            enforce_private_candidate_permissions(&temp_output_path)?;
            // Cleanup may remove the main path before reporting a companion
            // error. Relinquish pathname ownership before it starts so an
            // error path never retries against a possible replacement entry.
            builder_owned = false;
            cleanup_sqlite_temp_artifacts(&builder_path)
                .context("Failed to remove closed Pages export builder artifacts")?;
            finalize_staged_sqlite_sidecars(&temp_output_path)
                .context("Failed to finalize staged Pages export as one SQLite main file")?;

            let verification =
                verifier(&temp_output_path).context("Staged Pages export verification failed")?;
            reject_existing_sqlite_sidecars(&temp_output_path, "verified staged database")
                .context("Staged Pages export verifier left an unbound SQLite sidecar")?;

            replace_file_from_temp(
                &temp_output_path,
                &output_path,
                &mut retain_temp_on_replace_error,
                &publish_guard,
            )
            .context("Failed to install completed export database")?;
            candidate_owned = false;

            Ok((
                ExportStats {
                    conversations_processed: processed,
                    messages_processed: msg_processed,
                },
                verification,
            ))
        })();

        let result = if builder_owned {
            match cleanup_sqlite_temp_artifacts(&builder_path) {
                Ok(()) => result,
                Err(cleanup_error) => match result {
                    Ok(_) => Err(cleanup_error.context(
                        "completed Pages export was not published because its private builder could not be removed",
                    )),
                    Err(export_error) => Err(export_error.context(format!(
                        "failed to remove private Pages export builder artifacts: {cleanup_error:#}"
                    ))),
                },
            }
        } else {
            result
        };

        match result {
            // Only the catastrophic Windows backup/restore failure retains the
            // owned candidate for recovery. Every ordinary rejection after a
            // successful VACUUM reservation removes that exact artifact family;
            // a pre-reservation path collision is preserved rather than guessed
            // to belong to this export.
            Err(export_error) if candidate_owned && !retain_temp_on_replace_error => {
                match cleanup_sqlite_temp_artifacts(&temp_output_path) {
                    Ok(()) => Err(export_error),
                    Err(cleanup_error) => Err(export_error.context(format!(
                        "failed to remove rejected staged export artifacts: {cleanup_error:#}"
                    ))),
                }
            }
            other => other,
        }
    }

    /// 019i2: workspace paths under the obfuscating modes. `Relative` and
    /// `Full` keep the workspace verbatim (relative source paths are only
    /// meaningful against it); `Basename` keeps the last component; `Hash`
    /// keeps a stable 16-hex digest so per-workspace grouping still joins.
    fn transform_workspace(&self, workspace: &Option<String>) -> Option<String> {
        let ws = workspace.as_deref()?;
        Some(match self.filter.path_mode {
            PathMode::Relative | PathMode::Full => ws.to_string(),
            PathMode::Basename => Path::new(ws)
                .file_name()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_else(|| ws.to_string()),
            PathMode::Hash => hash_path16(ws),
        })
    }

    pub(super) fn transform_path(&self, path: &str, workspace: &Option<String>) -> String {
        match self.filter.path_mode {
            PathMode::Relative => {
                if let Some(ws) = workspace {
                    let ws_path = Path::new(ws);
                    let path_obj = Path::new(path);
                    if let Ok(stripped) = path_obj.strip_prefix(ws_path) {
                        return stripped
                            .to_string_lossy()
                            .trim_start_matches(['/', '\\'])
                            .to_string();
                    }
                }
                path.to_string()
            }
            PathMode::Basename => Path::new(path)
                .file_name()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_else(|| path.to_string()),
            PathMode::Full => path.to_string(),
            PathMode::Hash => hash_path16(path),
        }
    }
}

/// Stable 16-hex-char SHA-256 prefix used by `PathMode::Hash` for every
/// exported path-bearing field (source, workspace, snippet).
fn hash_path16(path: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(path.as_bytes());
    // sha2 ≥ 0.11 dropped `LowerHex` on the digest output;
    // `hex::encode` gives the same lowercase-hex string.
    hex::encode(hasher.finalize())[..16].to_string()
}

impl PathMode {
    /// Stable lowercase name recorded in `export_meta.path_mode`.
    pub fn as_meta_str(self) -> &'static str {
        match self {
            PathMode::Relative => "relative",
            PathMode::Basename => "basename",
            PathMode::Full => "full",
            PathMode::Hash => "hash",
        }
    }
}

/// Resolve the destination entry only after its parent exists, then prove it
/// does not name the source database.
///
/// Canonicalizing a not-yet-created output path and falling back to its raw
/// spelling is unsafe: creating a missing parent can make a path containing
/// `..` start resolving to an existing source file. Resolve the parent first
/// and use that stable directory spelling for staging and publication so the
/// alias check and the eventual rename address the same entry.
pub(super) fn resolve_export_output_path(
    source_db_path: &Path,
    output_path: &Path,
) -> Result<PathBuf> {
    let source_canonical = std::fs::canonicalize(source_db_path).with_context(|| {
        format!(
            "Failed to resolve source database path {}",
            source_db_path.display()
        )
    })?;
    let output_name = output_path.file_name().ok_or_else(|| {
        anyhow::anyhow!(
            "export output path has no file name: {}",
            output_path.display()
        )
    })?;
    let output_parent = output_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(output_parent).with_context(|| {
        format!(
            "Failed to create export output directory {}",
            output_parent.display()
        )
    })?;
    let resolved_parent = std::fs::canonicalize(output_parent).with_context(|| {
        format!(
            "Failed to resolve export output directory {}",
            output_parent.display()
        )
    })?;
    let resolved_output = resolved_parent.join(output_name);

    match std::fs::canonicalize(&resolved_output) {
        Ok(output_canonical) if output_canonical == source_canonical => {
            bail!("output path must be different from source database path");
        }
        Ok(_) if existing_regular_files_share_identity(&source_canonical, &resolved_output)? => {
            bail!(
                "output path must not refer to the same filesystem object as the source database"
            );
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "Failed to resolve export output path {}",
                    resolved_output.display()
                )
            });
        }
    }

    Ok(resolved_output)
}
