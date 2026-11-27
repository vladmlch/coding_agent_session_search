use super::detectors::{
    bounded_keyset_page_where, build_where_clause_for_columns, is_allowlisted,
    is_fully_allowlisted, redact_report_provenance, scan_text,
};
use super::types::{
    BUILTIN_PATTERNS, REDACTED_CONTEXT, REDACTED_METADATA_CONTEXT, SCAN_PAGE_ROWS, ScanContext,
    SecretFinding, SecretLocation, SecretScanConfig, SecretScanFilters, SecretScanReport,
    SecretScanSummary, SecretSeverity, StagedSecretScan,
};
use crate::franken_sync::compat::{ConnectionExt, ParamValue, RowExt, params_from_iter};
use crate::franken_sync::params;
use anyhow::{Context, Result, bail};
use indicatif::ProgressBar;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io::{Cursor, Read};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

struct StructuredMetadataScan {
    text: String,
    value: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SecretScanCheckpoint {
    BeforeOpen,
    AfterOpen,
    BeforeConversationHighWatermark,
    BeforeConversationPage,
    ConversationRow,
    AfterConversations,
    BeforeMessageHighWatermark,
    BeforeMessagePage,
    MessageRow,
    AfterMessages,
    BeforeSnippetHighWatermark,
    BeforeSnippetPage,
    SnippetRow,
    AfterSnippets,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SecretScanSchema {
    Archive,
    PagesExport,
}

impl SecretScanSchema {
    fn detect(conn: &crate::franken_sync::Connection) -> Result<Self> {
        let has_export_agent = table_has_column(conn, "conversations", "agent")?;
        let has_export_workspace = table_has_column(conn, "conversations", "workspace")?;
        if has_export_agent && has_export_workspace {
            return Ok(Self::PagesExport);
        }

        let has_archive_agent = table_has_column(conn, "conversations", "agent_id")?;
        let has_archive_workspace = table_has_column(conn, "conversations", "workspace_id")?;
        if has_archive_agent && has_archive_workspace {
            return Ok(Self::Archive);
        }

        bail!(
            "Unsupported secret-scan database schema: conversations must contain either agent/workspace export columns or agent_id/workspace_id archive columns"
        )
    }

    fn agent_expression(self) -> &'static str {
        match self {
            Self::Archive => "COALESCE(a.slug, 'unknown')",
            Self::PagesExport => "COALESCE(c.agent, 'unknown')",
        }
    }

    fn workspace_expression(self) -> &'static str {
        match self {
            Self::Archive => "w.path",
            Self::PagesExport => "c.workspace",
        }
    }

    fn conversation_joins(self) -> &'static str {
        match self {
            Self::Archive => {
                "\n             LEFT JOIN agents a ON c.agent_id = a.id\n             LEFT JOIN workspaces w ON c.workspace_id = w.id"
            }
            Self::PagesExport => "",
        }
    }
}

fn ensure_secret_scan_running(
    cancellation_requested: &mut impl FnMut(SecretScanCheckpoint) -> bool,
    checkpoint: SecretScanCheckpoint,
) -> Result<()> {
    if cancellation_requested(checkpoint) {
        bail!("Secret scan cancelled before completion");
    }
    Ok(())
}

pub fn scan_database<P: AsRef<Path>>(
    db_path: P,
    filters: &SecretScanFilters,
    config: &SecretScanConfig,
    running: Option<Arc<AtomicBool>>,
    progress: Option<&ProgressBar>,
) -> Result<SecretScanReport> {
    scan_database_with_cancel_check(db_path, filters, config, progress, |_| {
        running
            .as_ref()
            .is_some_and(|flag| !flag.load(Ordering::Relaxed))
    })
}

/// Scan an immutable Pages export and bind the report to its exact bytes.
///
/// The digest is checked on both sides of the scan. Any concurrent mutation or
/// a finding-cap truncation is an error, never an approvable/clean result.
pub fn scan_staged_export_database<P: AsRef<Path>>(
    db_path: P,
    config: &SecretScanConfig,
) -> Result<StagedSecretScan> {
    let db_path = db_path.as_ref();
    ensure_staged_export_has_no_sidecars(db_path, "before verification")?;
    let digest_before = sha256_file(db_path)?;
    ensure_staged_export_has_no_sidecars(db_path, "after pre-scan hashing")?;
    let filters = SecretScanFilters {
        agents: None,
        workspaces: None,
        since_ts: None,
        until_ts: None,
    };
    let report = scan_database(db_path, &filters, config, None, None)?;
    ensure_staged_export_has_no_sidecars(db_path, "after the secret scan")?;
    let digest_after = sha256_file(db_path)?;
    ensure_staged_export_has_no_sidecars(db_path, "after post-scan hashing")?;

    if digest_before != digest_after {
        bail!("Staged Pages export changed while its secret scan was running; refusing approval");
    }
    if report.summary.truncated {
        bail!(
            "Staged Pages export secret scan reached its finding cap; refusing incomplete approval"
        );
    }

    Ok(StagedSecretScan {
        report,
        artifact_sha256: digest_after,
    })
}

pub(super) fn ensure_staged_export_has_no_sidecars(db_path: &Path, phase: &str) -> Result<()> {
    // Legacy namespace identity records are exempt: older VFS layers stamped
    // them next to any database they touched and they hold no database content,
    // so they cannot change what the attested main file contains.
    for sidecar_path in crate::pages::sqlite_content_bearing_artifact_paths(db_path)? {
        match std::fs::symlink_metadata(&sidecar_path) {
            Ok(_) => {
                bail!(
                    "Staged Pages export has SQLite sidecar {} {phase}; refusing a main-file-only artifact attestation",
                    sidecar_path.display()
                );
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "Failed to inspect staged SQLite sidecar {} {phase}",
                        sidecar_path.display()
                    )
                });
            }
        }
    }
    Ok(())
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut file = std::fs::File::open(path).with_context(|| {
        format!(
            "Failed to open staged export {} for hashing",
            path.display()
        )
    })?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .with_context(|| format!("Failed to hash staged export {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

pub(super) fn scan_database_with_cancel_check<P: AsRef<Path>>(
    db_path: P,
    filters: &SecretScanFilters,
    config: &SecretScanConfig,
    progress: Option<&ProgressBar>,
    mut cancellation_requested: impl FnMut(SecretScanCheckpoint) -> bool,
) -> Result<SecretScanReport> {
    // Cancellation must never be represented as a complete (and potentially
    // false-clean) report. Check before opening the database as well as at
    // every query/row boundary below, and return a distinct error on abort.
    ensure_secret_scan_running(
        &mut cancellation_requested,
        SecretScanCheckpoint::BeforeOpen,
    )?;
    let conn = crate::pages::open_existing_sqlite_db(db_path.as_ref())
        .context("Failed to open database for secret scan")?;
    ensure_secret_scan_running(&mut cancellation_requested, SecretScanCheckpoint::AfterOpen)?;
    conn.execute("BEGIN TRANSACTION")
        .context("Failed to start secret-scan read snapshot")?;

    let scan_result = scan_database_snapshot(&conn, filters, config, progress, |checkpoint| {
        cancellation_requested(checkpoint)
    });
    let rollback_result = conn
        .execute("ROLLBACK")
        .map(|_| ())
        .context("Failed to close secret-scan read snapshot");
    match (scan_result, rollback_result) {
        (Ok(report), Ok(())) => Ok(report),
        (Err(scan_error), Ok(())) => Err(scan_error),
        (Ok(_), Err(rollback_error)) => Err(rollback_error),
        (Err(scan_error), Err(rollback_error)) => Err(scan_error.context(format!(
            "secret-scan read-snapshot rollback also failed: {rollback_error:#}"
        ))),
    }
}

/// Read every secret-scan surface from the caller-owned transaction.
/// High-water marks bound inserts, but the transaction also prevents updates
/// to already-scanned rows from producing a mixed, false-clean report.
fn scan_database_snapshot(
    conn: &crate::franken_sync::Connection,
    filters: &SecretScanFilters,
    config: &SecretScanConfig,
    progress: Option<&ProgressBar>,
    mut cancellation_requested: impl FnMut(SecretScanCheckpoint) -> bool,
) -> Result<SecretScanReport> {
    let schema = SecretScanSchema::detect(conn)?;

    let mut findings: Vec<SecretFinding> = Vec::new();
    let mut seen = Vec::new();
    let mut truncated = false;

    // LEFT JOIN + COALESCE on agents so secret scanning also covers legacy
    // conversations with NULL agent_id — dropping them would hide credential
    // leaks rather than exposing them.
    let has_metadata_bin = table_has_column(conn, "conversations", "metadata_bin")?;
    let metadata_bin_projection = if has_metadata_bin {
        "c.metadata_bin"
    } else {
        "NULL"
    };
    let (conv_where, conv_params) = build_where_clause_for_columns(
        filters,
        schema.agent_expression(),
        schema.workspace_expression(),
    )?;
    ensure_secret_scan_running(
        &mut cancellation_requested,
        SecretScanCheckpoint::BeforeConversationHighWatermark,
    )?;
    let conv_high_watermark = table_max_id(conn, "conversations")?;
    let conv_select = format!(
        "SELECT c.id, c.title, c.metadata_json, c.source_path, {}, {}, {metadata_bin_projection}\n         FROM conversations c{}",
        schema.agent_expression(),
        schema.workspace_expression(),
        schema.conversation_joins(),
    );
    let mut last_conv_id = None;
    while !truncated {
        ensure_secret_scan_running(
            &mut cancellation_requested,
            SecretScanCheckpoint::BeforeConversationPage,
        )?;
        let page_where = bounded_keyset_page_where(&conv_where, "c.id", last_conv_id);
        let conv_sql = format!("{conv_select}{page_where} ORDER BY c.id LIMIT {SCAN_PAGE_ROWS}");
        let mut page_params = conv_params.clone();
        page_params.push(ParamValue::from(conv_high_watermark));
        if let Some(last_id) = last_conv_id {
            page_params.push(ParamValue::from(last_id));
        }
        let page_param_values = params_from_iter(page_params);
        let conv_rows = conn.query_with_params(&conv_sql, &page_param_values)?;
        if conv_rows.is_empty() {
            break;
        }
        let page_len = conv_rows.len();

        for row in &conv_rows {
            ensure_secret_scan_running(
                &mut cancellation_requested,
                SecretScanCheckpoint::ConversationRow,
            )?;
            let conv_id: i64 = row.get_typed(0)?;
            let title: Option<String> = row.get_typed(1)?;
            let metadata_json: Option<String> = row.get_typed(2)?;
            let source_path: String = row.get_typed(3)?;
            let agent_slug: String = row.get_typed(4)?;
            let workspace_path: Option<String> = row.get_typed(5)?;
            let metadata_bin: Option<Vec<u8>> = row.get_typed(6)?;
            last_conv_id = Some(conv_id);

            let ctx = ScanContext {
                agent: Some(agent_slug),
                workspace: workspace_path,
                source_path: Some(source_path),
                conversation_id: Some(conv_id),
                message_id: None,
                message_idx: None,
            };

            // These provenance fields are part of the exported database too.
            // Merely redacting them in the diagnostic report would otherwise
            // allow a credential-bearing path/workspace to yield a false-clean
            // approval while the original value remains in the payload.
            for provenance in [
                ctx.agent.as_deref(),
                ctx.workspace.as_deref(),
                ctx.source_path.as_deref(),
            ]
            .into_iter()
            .flatten()
            {
                scan_text(
                    provenance,
                    SecretLocation::ConversationMetadata,
                    &ctx,
                    config,
                    &mut findings,
                    &mut seen,
                    &mut truncated,
                );
            }

            if let Some(title_text) = title {
                scan_text(
                    &title_text,
                    SecretLocation::ConversationTitle,
                    &ctx,
                    config,
                    &mut findings,
                    &mut seen,
                    &mut truncated,
                );
            }
            if let Some(meta) = structured_metadata_scan_text(
                metadata_bin.as_deref(),
                metadata_json.as_deref(),
                "conversations.metadata_bin",
                "conversations.metadata_json",
                conv_id,
            )? {
                let first_metadata_finding = findings.len();
                scan_text(
                    &meta.text,
                    SecretLocation::ConversationMetadata,
                    &ctx,
                    config,
                    &mut findings,
                    &mut seen,
                    &mut truncated,
                );
                redact_structured_metadata_contexts(&mut findings[first_metadata_finding..]);
                if let Some(value) = meta.value.as_ref() {
                    scan_sensitive_json_fields(
                        value,
                        SecretLocation::ConversationMetadata,
                        &ctx,
                        config,
                        &mut findings,
                        &mut truncated,
                    );
                }
            }

            if truncated {
                break;
            }

            if let Some(pb) = progress {
                pb.inc(1);
            }
        }

        if truncated || page_len < SCAN_PAGE_ROWS {
            break;
        }
    }
    ensure_secret_scan_running(
        &mut cancellation_requested,
        SecretScanCheckpoint::AfterConversations,
    )?;

    if !truncated {
        let has_extra_json = table_has_column(conn, "messages", "extra_json")?;
        let extra_json_projection = if has_extra_json {
            "m.extra_json"
        } else {
            "NULL"
        };
        let has_extra_bin = table_has_column(conn, "messages", "extra_bin")?;
        let extra_bin_projection = if has_extra_bin { "m.extra_bin" } else { "NULL" };
        let has_attachment_refs = table_has_column(conn, "messages", "attachment_refs")?;
        let attachment_refs_projection =
            if schema == SecretScanSchema::PagesExport && has_attachment_refs { // ubs:ignore[rust.security.constant-time-compare] — schema enum comparison, not a cryptographic secret
                "m.attachment_refs"
            } else {
                "NULL"
            };
        let has_model = table_has_column(conn, "messages", "model")?;
        let model_projection = if has_model { "m.model" } else { "NULL" };
        let (msg_where, msg_params) = build_where_clause_for_columns(
            filters,
            schema.agent_expression(),
            schema.workspace_expression(),
        )?;
        ensure_secret_scan_running(
            &mut cancellation_requested,
            SecretScanCheckpoint::BeforeMessageHighWatermark,
        )?;
        let msg_high_watermark = table_max_id(conn, "messages")?;
        let msg_select = format!(
            "SELECT m.id, m.idx, m.content, {extra_json_projection}, c.id, c.source_path, {}, {}, {extra_bin_projection}, {attachment_refs_projection}, m.role, {model_projection}\n             FROM messages m\n             JOIN conversations c ON m.conversation_id = c.id{}",
            schema.agent_expression(),
            schema.workspace_expression(),
            schema.conversation_joins(),
        );
        let mut last_msg_id = None;
        while !truncated {
            ensure_secret_scan_running(
                &mut cancellation_requested,
                SecretScanCheckpoint::BeforeMessagePage,
            )?;
            let page_where = bounded_keyset_page_where(&msg_where, "m.id", last_msg_id);
            let msg_sql = format!("{msg_select}{page_where} ORDER BY m.id LIMIT {SCAN_PAGE_ROWS}");
            let mut page_params = msg_params.clone();
            page_params.push(ParamValue::from(msg_high_watermark));
            if let Some(last_id) = last_msg_id {
                page_params.push(ParamValue::from(last_id));
            }
            let page_param_values = params_from_iter(page_params);
            let msg_rows = conn.query_with_params(&msg_sql, &page_param_values)?;
            if msg_rows.is_empty() {
                break;
            }
            let page_len = msg_rows.len();

            for row in &msg_rows {
                ensure_secret_scan_running(
                    &mut cancellation_requested,
                    SecretScanCheckpoint::MessageRow,
                )?;
                let msg_id: i64 = row.get_typed(0)?;
                let msg_idx: i64 = row.get_typed(1)?;
                let content: String = row.get_typed(2)?;
                let extra_json: Option<String> = row.get_typed(3)?;
                let conv_id: i64 = row.get_typed(4)?;
                let source_path: String = row.get_typed(5)?;
                let agent_slug: String = row.get_typed(6)?;
                let workspace_path: Option<String> = row.get_typed(7)?;
                let extra_bin: Option<Vec<u8>> = row.get_typed(8)?;
                let attachment_refs: Option<String> = row.get_typed(9)?;
                let role: String = row.get_typed(10)?;
                let model: Option<String> = row.get_typed(11)?;
                last_msg_id = Some(msg_id);

                let ctx = ScanContext {
                    agent: Some(agent_slug),
                    workspace: workspace_path,
                    source_path: Some(source_path),
                    conversation_id: Some(conv_id),
                    message_id: Some(msg_id),
                    message_idx: Some(msg_idx),
                };

                scan_text(
                    &content,
                    SecretLocation::MessageContent,
                    &ctx,
                    config,
                    &mut findings,
                    &mut seen,
                    &mut truncated,
                );
                if let Some(attachment_refs) = attachment_refs.as_deref() {
                    scan_text(
                        attachment_refs,
                        SecretLocation::MessageMetadata,
                        &ctx,
                        config,
                        &mut findings,
                        &mut seen,
                        &mut truncated,
                    );
                }
                for metadata_text in [Some(role.as_str()), model.as_deref()]
                    .into_iter()
                    .flatten()
                {
                    scan_text(
                        metadata_text,
                        SecretLocation::MessageMetadata,
                        &ctx,
                        config,
                        &mut findings,
                        &mut seen,
                        &mut truncated,
                    );
                }
                if let Some(extra) = structured_metadata_scan_text(
                    extra_bin.as_deref(),
                    extra_json.as_deref(),
                    "messages.extra_bin",
                    "messages.extra_json",
                    msg_id,
                )? {
                    let first_metadata_finding = findings.len();
                    scan_text(
                        &extra.text,
                        SecretLocation::MessageMetadata,
                        &ctx,
                        config,
                        &mut findings,
                        &mut seen,
                        &mut truncated,
                    );
                    redact_structured_metadata_contexts(&mut findings[first_metadata_finding..]);
                    if let Some(value) = extra.value.as_ref() {
                        scan_sensitive_json_fields(
                            value,
                            SecretLocation::MessageMetadata,
                            &ctx,
                            config,
                            &mut findings,
                            &mut truncated,
                        );
                    }
                }

                if truncated {
                    break;
                }

                if let Some(pb) = progress {
                    pb.inc(1);
                }
            }

            if truncated || page_len < SCAN_PAGE_ROWS {
                break;
            }
        }
    }
    ensure_secret_scan_running(
        &mut cancellation_requested,
        SecretScanCheckpoint::AfterMessages,
    )?;

    if !truncated && table_exists(conn, "snippets")? {
        let (snip_where, snip_params) = build_where_clause_for_columns(
            filters,
            schema.agent_expression(),
            schema.workspace_expression(),
        )?;
        ensure_secret_scan_running(
            &mut cancellation_requested,
            SecretScanCheckpoint::BeforeSnippetHighWatermark,
        )?;
        let snip_high_watermark = table_max_id(conn, "snippets")?;
        let snip_select = format!(
            "SELECT s.id, s.snippet_text, m.id, m.idx, c.id, c.source_path, {}, {}, s.file_path, s.language\n             FROM snippets s\n             JOIN messages m ON s.message_id = m.id\n             JOIN conversations c ON m.conversation_id = c.id{}",
            schema.agent_expression(),
            schema.workspace_expression(),
            schema.conversation_joins(),
        );
        let mut last_snippet_id = None;
        while !truncated {
            ensure_secret_scan_running(
                &mut cancellation_requested,
                SecretScanCheckpoint::BeforeSnippetPage,
            )?;
            let page_where = bounded_keyset_page_where(&snip_where, "s.id", last_snippet_id);
            let snip_sql =
                format!("{snip_select}{page_where} ORDER BY s.id LIMIT {SCAN_PAGE_ROWS}");
            let mut page_params = snip_params.clone();
            page_params.push(ParamValue::from(snip_high_watermark));
            if let Some(last_id) = last_snippet_id {
                page_params.push(ParamValue::from(last_id));
            }
            let page_param_values = params_from_iter(page_params);
            let snip_rows = conn.query_with_params(&snip_sql, &page_param_values)?;
            if snip_rows.is_empty() {
                break;
            }
            let page_len = snip_rows.len();

            for row in &snip_rows {
                ensure_secret_scan_running(
                    &mut cancellation_requested,
                    SecretScanCheckpoint::SnippetRow,
                )?;
                let snippet_id: i64 = row.get_typed(0)?;
                let snippet_text: String = row.get_typed(1)?;
                let msg_id: i64 = row.get_typed(2)?;
                let msg_idx: i64 = row.get_typed(3)?;
                let conv_id: i64 = row.get_typed(4)?;
                let source_path: String = row.get_typed(5)?;
                let agent_slug: String = row.get_typed(6)?;
                let workspace_path: Option<String> = row.get_typed(7)?;
                let snippet_file_path: Option<String> = row.get_typed(8)?;
                let snippet_language: Option<String> = row.get_typed(9)?;
                last_snippet_id = Some(snippet_id);

                let ctx = ScanContext {
                    agent: Some(agent_slug),
                    workspace: workspace_path,
                    source_path: Some(source_path),
                    conversation_id: Some(conv_id),
                    message_id: Some(msg_id),
                    message_idx: Some(msg_idx),
                };

                scan_text(
                    &snippet_text,
                    SecretLocation::MessageSnippet,
                    &ctx,
                    config,
                    &mut findings,
                    &mut seen,
                    &mut truncated,
                );
                if let Some(snippet_file_path) = snippet_file_path.as_deref() {
                    scan_text(
                        snippet_file_path,
                        SecretLocation::MessageMetadata,
                        &ctx,
                        config,
                        &mut findings,
                        &mut seen,
                        &mut truncated,
                    );
                }
                if let Some(snippet_language) = snippet_language.as_deref() {
                    scan_text(
                        snippet_language,
                        SecretLocation::MessageMetadata,
                        &ctx,
                        config,
                        &mut findings,
                        &mut seen,
                        &mut truncated,
                    );
                }

                if truncated {
                    break;
                }

                if let Some(pb) = progress {
                    pb.inc(1);
                }
            }

            if truncated || page_len < SCAN_PAGE_ROWS {
                break;
            }
        }
    }
    ensure_secret_scan_running(
        &mut cancellation_requested,
        SecretScanCheckpoint::AfterSnippets,
    )?;

    findings.sort_by(|a, b| {
        a.severity
            .rank()
            .cmp(&b.severity.rank())
            .then_with(|| a.kind.cmp(&b.kind))
    });

    let mut by_severity: BTreeMap<SecretSeverity, usize> = BTreeMap::new();
    for finding in &findings {
        *by_severity.entry(finding.severity).or_insert(0) += 1;
    }

    let has_critical = by_severity
        .get(&SecretSeverity::Critical)
        .copied()
        .unwrap_or(0)
        > 0;

    Ok(SecretScanReport {
        summary: SecretScanSummary {
            total: findings.len(),
            by_severity,
            has_critical,
            truncated,
        },
        findings,
    })
}

pub(super) fn table_exists(
    conn: &crate::franken_sync::Connection,
    table_name: &str,
) -> Result<bool> {
    if !table_name
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        bail!("Invalid SQLite identifier while inspecting secret-scan schema");
    }

    let pragma = format!("PRAGMA table_info({table_name})");
    conn.query_map_collect(&pragma, params![], |row| row.get_typed::<String>(1))
        .map(|columns| !columns.is_empty())
        .with_context(|| format!("Failed to inspect {table_name} schema for secret scan"))
}

fn table_max_id(conn: &crate::franken_sync::Connection, table_name: &str) -> Result<Option<i64>> {
    if !table_name
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        bail!("Invalid SQLite identifier while paging secret-scan rows");
    }

    let sql = format!("SELECT MAX(id) FROM {table_name}");
    conn.query_row_map(&sql, params![], |row| row.get_typed(0))
        .with_context(|| format!("Failed to bound {table_name} secret-scan rows"))
}

fn table_has_column(
    conn: &crate::franken_sync::Connection,
    table_name: &str,
    column_name: &str,
) -> Result<bool> {
    if !table_name
        .chars()
        .chain(column_name.chars())
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        bail!("Invalid SQLite identifier while inspecting secret-scan schema");
    }

    let pragma = format!("PRAGMA table_info({table_name})");
    let columns = conn
        .query_map_collect(&pragma, params![], |row| row.get_typed::<String>(1))
        .with_context(|| format!("Failed to inspect {table_name} schema for secret scan"))?;
    Ok(columns.iter().any(|column| column == column_name))
}

/// Select the authoritative structured metadata representation for scanning.
///
/// New archive rows store MessagePack in `*_bin`; legacy rows store JSON text.
/// A non-empty binary value is authoritative and must never silently fall back
/// to a stale JSON shadow. In particular, malformed binary is an integrity
/// error: falling back could hide secrets that exist only in the canonical
/// binary payload.
fn structured_metadata_scan_text(
    binary: Option<&[u8]>,
    legacy_json: Option<&str>,
    binary_column: &str,
    legacy_column: &str,
    row_id: i64,
) -> Result<Option<StructuredMetadataScan>> {
    if let Some(bytes) = binary.filter(|bytes| !bytes.is_empty()) {
        let mut deserializer = rmp_serde::Deserializer::new(Cursor::new(bytes));
        let value = serde_json::Value::deserialize(&mut deserializer).with_context(|| {
            format!(
                "Failed to decode non-empty {binary_column} MessagePack for row {row_id}; refusing legacy JSON fallback"
            )
        })?;
        let consumed = usize::try_from(deserializer.get_ref().position()).with_context(|| {
            format!("Decoded {binary_column} position does not fit usize for row {row_id}")
        })?;
        if consumed != bytes.len() {
            bail!(
                "Non-empty {binary_column} for row {row_id} contains trailing bytes after its MessagePack value; refusing legacy JSON fallback"
            );
        }
        return serde_json::to_string(&value)
            .with_context(|| {
                format!("Failed to serialize decoded {binary_column} value for row {row_id}")
            })
            .map(|text| {
                Some(StructuredMetadataScan {
                    text,
                    value: Some(value),
                })
            });
    }

    let Some(text) = legacy_json.filter(|text| !text.trim().is_empty()) else {
        return Ok(None);
    };
    let value = serde_json::from_str(text).with_context(|| {
        format!(
            "Failed to decode non-empty {legacy_column} JSON for row {row_id}; refusing a text-only scan that could miss credential-bearing fields"
        )
    })?;
    Ok(Some(StructuredMetadataScan {
        text: text.to_owned(),
        value: Some(value),
    }))
}

fn redact_structured_metadata_contexts(findings: &mut [SecretFinding]) {
    for finding in findings {
        finding.context = REDACTED_METADATA_CONTEXT.to_string();
    }
}

/// Report credential-bearing structured fields that text patterns cannot see.
///
/// The ingestion redactor treats field names such as `pin`, `cookie`, and
/// `private_key` as authoritative even when their values are too short to
/// satisfy a token regex. The post-hoc scanner must apply the same structural
/// floor or it can return a false-clean report for historical rows. A field
/// already covered by an equal-or-higher-severity text detector is skipped so
/// the structural floor does not inflate an equivalent existing finding. A
/// lower-severity heuristic must not suppress this high-severity floor.
fn scan_sensitive_json_fields(
    value: &serde_json::Value,
    location: SecretLocation,
    ctx: &ScanContext,
    config: &SecretScanConfig,
    findings: &mut Vec<SecretFinding>,
    truncated: &mut bool,
) {
    if *truncated {
        return;
    }

    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                scan_sensitive_json_fields(
                    value,
                    location.clone(),
                    ctx,
                    config,
                    findings,
                    truncated,
                );
                if *truncated {
                    return;
                }
            }
        }
        serde_json::Value::Object(fields) => {
            for (field, field_value) in fields {
                if crate::indexer::redact_secrets::is_sensitive_json_field(field) {
                    if !structured_value_contains_material(field_value)
                        || structured_value_is_fully_allowlisted(field_value, config)
                        || structured_field_has_equal_or_higher_text_detector(
                            field,
                            field_value,
                            config,
                            SecretSeverity::High,
                        )
                    {
                        continue;
                    }
                    if findings.len() >= config.max_findings {
                        *truncated = true;
                        return;
                    }
                    findings.push(SecretFinding {
                        severity: SecretSeverity::High,
                        kind: "sensitive_metadata_field".to_string(),
                        pattern: "sensitive_json_field".to_string(),
                        match_redacted: REDACTED_CONTEXT.to_string(),
                        context: REDACTED_METADATA_CONTEXT.to_string(),
                        location: location.clone(),
                        agent: redact_report_provenance(&ctx.agent, config),
                        workspace: redact_report_provenance(&ctx.workspace, config),
                        source_path: redact_report_provenance(&ctx.source_path, config),
                        conversation_id: ctx.conversation_id,
                        message_id: ctx.message_id,
                        message_idx: ctx.message_idx,
                    });
                } else {
                    scan_sensitive_json_fields(
                        field_value,
                        location.clone(),
                        ctx,
                        config,
                        findings,
                        truncated,
                    );
                    if *truncated {
                        return;
                    }
                }
            }
        }
        _ => {}
    }
}

fn structured_value_contains_material(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Null => false,
        serde_json::Value::String(text) => {
            let trimmed = text.trim();
            !trimmed.is_empty() && !trimmed.eq_ignore_ascii_case(REDACTED_CONTEXT)
        }
        serde_json::Value::Array(values) => values.iter().any(structured_value_contains_material),
        serde_json::Value::Object(fields) => {
            fields.values().any(structured_value_contains_material)
        }
        serde_json::Value::Bool(_) | serde_json::Value::Number(_) => true,
    }
}

fn structured_value_is_fully_allowlisted(
    value: &serde_json::Value,
    config: &SecretScanConfig,
) -> bool {
    match value {
        serde_json::Value::Null => true,
        serde_json::Value::String(text) => {
            text.trim().is_empty()
                || text.trim().eq_ignore_ascii_case(REDACTED_CONTEXT)
                || is_fully_allowlisted(text.trim(), config)
        }
        serde_json::Value::Array(values) => values
            .iter()
            .filter(|value| structured_value_contains_material(value))
            .all(|value| structured_value_is_fully_allowlisted(value, config)),
        serde_json::Value::Object(fields) => fields
            .values()
            .filter(|value| structured_value_contains_material(value))
            .all(|value| structured_value_is_fully_allowlisted(value, config)),
        serde_json::Value::Bool(value) => is_fully_allowlisted(&value.to_string(), config),
        serde_json::Value::Number(value) => is_fully_allowlisted(&value.to_string(), config),
    }
}

fn structured_field_has_equal_or_higher_text_detector(
    field: &str,
    value: &serde_json::Value,
    config: &SecretScanConfig,
    structural_severity: SecretSeverity,
) -> bool {
    let Ok(field_text) = serde_json::to_string(field) else {
        return false;
    };
    let Ok(value_text) = serde_json::to_string(value) else {
        return false;
    };
    let mut text = String::with_capacity(
        field_text
            .len()
            .saturating_add(value_text.len())
            .saturating_add(3),
    );
    text.push('{');
    text.push_str(&field_text);
    text.push(':');
    text.push_str(&value_text);
    text.push('}');

    if SecretSeverity::Critical.rank() <= structural_severity.rank()
        && config
            .denylist
            .iter()
            .any(|pattern| pattern.is_match(&text))
    {
        return true;
    }
    BUILTIN_PATTERNS.iter().any(|pattern| {
        pattern.severity.rank() <= structural_severity.rank()
            && pattern
                .regex
                .find_iter(&text)
                .any(|matched| !is_allowlisted(matched.as_str(), config))
    })
}
