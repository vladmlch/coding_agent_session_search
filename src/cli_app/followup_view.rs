#[allow(unused_imports)]
use crate::franken_sync::compat::{
    ConnectionExt, OpenFlags as FrankenOpenFlags, RowExt,
    open_with_flags as open_franken_with_flags,
};
#[allow(unused_imports)]
use crate::indexer::IndexOptions;
#[allow(unused_imports)]
use crate::model::cli_error_kind::ErrorKind as CliErrorKind;
#[allow(unused_imports)]
use crate::robot_budget_envelope::BudgetSnapshot;
#[allow(unused_imports)]
use crate::{cli_app::*, cli_root::*, lib_doctor::*, *};
#[allow(unused_imports)]
use anyhow::Result;
#[allow(unused_imports)]
use base64::prelude::*;
#[allow(unused_imports)]
use chrono::Utc;
#[allow(unused_imports)]
use clap::{Arg, ArgAction, Command, CommandFactory, Parser, Subcommand, ValueEnum, ValueHint};
#[allow(unused_imports)]
use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
#[allow(unused_imports)]
use std::fs::{File, OpenOptions};
#[allow(unused_imports)]
use std::io::{self, BufRead, BufReader, IsTerminal, Read, Seek, SeekFrom, Write};
#[allow(unused_imports)]
use std::path::{Path, PathBuf};
#[allow(unused_imports)]
use std::sync::{Arc, Mutex};
#[allow(unused_imports)]
use std::time::{Duration, Instant};
#[allow(unused_imports)]
use tracing::{info, warn};
#[allow(unused_imports)]
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

pub(crate) fn role_to_export_string(role: &crate::model::types::MessageRole) -> String {
    match role {
        crate::model::types::MessageRole::User => "user".to_string(),
        crate::model::types::MessageRole::Agent => "assistant".to_string(),
        crate::model::types::MessageRole::Tool => "tool".to_string(),
        crate::model::types::MessageRole::System => "system".to_string(),
        crate::model::types::MessageRole::Other(s) => s.clone(),
    }
}

pub(crate) fn conversation_view_to_raw_messages(
    view: &crate::conversation_view::ConversationView,
) -> Vec<serde_json::Value> {
    view.messages
        .iter()
        .map(|msg| {
            let mut json_msg = serde_json::json!({
                "role": role_to_export_string(&msg.role),
                "content": msg.content,
            });
            if let Some(ts) = msg.created_at {
                json_msg["timestamp"] = serde_json::json!(ts);
            }
            if let Some(author) = &msg.author {
                json_msg["author"] = serde_json::json!(author);
            }
            json_msg
        })
        .collect()
}

pub(crate) fn canonical_followup_source_id(source_id: Option<&str>) -> Option<String> {
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

pub(crate) fn followup_source_is_local(source_id: Option<&str>) -> bool {
    canonical_followup_source_id(source_id).as_deref()
        == Some(crate::sources::provenance::LOCAL_SOURCE_ID)
}

pub(crate) fn normalized_provenance_origin_host(origin_host: Option<&str>) -> Option<String> {
    origin_host
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

pub(crate) fn normalized_provenance_source_id(
    source_id: &str,
    origin_kind: Option<&str>,
    origin_host: Option<&str>,
) -> String {
    if let Some(source_id) = canonical_followup_source_id(Some(source_id)) {
        return source_id;
    }

    if let Some(host) = normalized_provenance_origin_host(origin_host) {
        return host;
    }

    if let Some(kind) = origin_kind.map(str::trim).filter(|value| !value.is_empty())
        && (kind.eq_ignore_ascii_case("ssh") || kind.eq_ignore_ascii_case("remote"))
    {
        return "remote".to_string();
    }

    crate::sources::provenance::LOCAL_SOURCE_ID.to_string()
}

pub(crate) fn normalized_provenance_origin_kind(
    normalized_source_id: &str,
    origin_kind: Option<&str>,
) -> String {
    if let Some(kind) = origin_kind.map(str::trim).filter(|value| !value.is_empty()) {
        if kind.eq_ignore_ascii_case("local") {
            return crate::sources::provenance::LOCAL_SOURCE_ID.to_string();
        }
        if kind.eq_ignore_ascii_case("ssh") || kind.eq_ignore_ascii_case("remote") {
            return "remote".to_string();
        }
        return kind.to_ascii_lowercase();
    }

    if followup_source_is_local(Some(normalized_source_id)) {
        crate::sources::provenance::LOCAL_SOURCE_ID.to_string()
    } else {
        "remote".to_string()
    }
}

pub(crate) fn normalized_source_identity_sql_expr(
    source_id_column: &str,
    origin_host_column: &str,
) -> String {
    format!(
        "CASE WHEN TRIM(COALESCE({source_id_column}, '')) = '' THEN CASE WHEN TRIM(COALESCE({origin_host_column}, '')) = '' THEN '{local}' ELSE TRIM(COALESCE({origin_host_column}, '')) END          WHEN LOWER(TRIM(COALESCE({source_id_column}, ''))) = '{local}' THEN '{local}'          ELSE TRIM(COALESCE({source_id_column}, '')) END",
        local = crate::sources::provenance::LOCAL_SOURCE_ID,
    )
}

pub(crate) fn normalized_source_origin_kind_sql_expr(
    source_id_column: &str,
    origin_kind_column: &str,
    origin_host_column: &str,
) -> String {
    format!(
        "CASE \
            WHEN LOWER(TRIM(COALESCE({origin_kind_column}, ''))) = '{local}' THEN '{local}' \
            WHEN LOWER(TRIM(COALESCE({origin_kind_column}, ''))) IN ('ssh', 'remote') THEN 'remote' \
            WHEN TRIM(COALESCE({origin_kind_column}, '')) != '' THEN LOWER(TRIM({origin_kind_column})) \
            WHEN LOWER(TRIM(COALESCE({source_id_column}, ''))) = '{local}' THEN '{local}' \
            WHEN TRIM(COALESCE({source_id_column}, '')) != '' THEN 'remote' \
            WHEN TRIM(COALESCE({origin_host_column}, '')) != '' THEN 'remote' \
            ELSE '{local}' \
         END",
        local = crate::sources::provenance::LOCAL_SOURCE_ID,
    )
}

pub(crate) fn validate_followup_source_id(source_id: &str, command: &str) -> CliResult<()> {
    let trimmed = source_id.trim();
    let keyword = trimmed.to_ascii_lowercase();
    if trimmed.is_empty() || matches!(keyword.as_str(), "all" | "remote") {
        return Err(CliError {
            code: 2,
            kind: CliErrorKind::AmbiguousSource.kind_str(),
            message: format!(
                "{command} requires an exact source_id, not '{source_id}'"
            ),
            hint: Some(
                "Use the source_id field from search results, e.g. --source local or --source work-laptop"
                    .to_string(),
            ),
            retryable: false,
        });
    }
    Ok(())
}

pub(crate) fn prefers_direct_view_file(path: &Path, source_id: Option<&str>) -> bool {
    if (!followup_source_is_local(source_id) && source_id.is_some()) || !path.exists() {
        return false;
    }

    matches!(
        path.extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.to_ascii_lowercase())
            .as_deref(),
        Some("jsonl" | "md" | "markdown" | "txt")
    )
}

pub(crate) fn prefers_direct_jsonl_file(path: &Path, source_id: Option<&str>) -> bool {
    if (!followup_source_is_local(source_id) && source_id.is_some()) || !path.exists() {
        return false;
    }

    matches!(
        path.extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.to_ascii_lowercase())
            .as_deref(),
        Some("jsonl")
    )
}

pub(crate) fn try_load_indexed_conversation_from_db_with_source(
    source_path: &Path,
    db_path: &Path,
    source_id: Option<&str>,
    conversation_id: Option<i64>,
) -> Option<crate::conversation_view::ConversationView> {
    if !db_path.exists() {
        return None;
    }
    // `cass view` is an inspection surface. Archive fallback must therefore use
    // the read-only connection lane and must never run migrations or schema
    // repair merely because an incident-summary pointer was followed.
    let storage = crate::storage::sqlite::FrankenStorage::open_readonly(db_path).ok()?;
    let source_path = source_path.to_string_lossy();
    let source_id = canonical_followup_source_id(source_id);
    if let Some(conversation_id) = conversation_id {
        let view =
            crate::conversation_view::load_conversation_by_id_uncached(&storage, conversation_id)
                .ok()??;
        if view.convo.source_path.to_string_lossy() != source_path {
            return None;
        }
        if source_id
            .as_deref()
            .is_some_and(|expected| view.convo.source_id != expected)
        {
            return None;
        }
        return Some(view);
    }
    if let Some(source_id) = source_id.as_deref() {
        return crate::conversation_view::load_conversation_for_source(
            &storage,
            source_id,
            &source_path,
        )
        .ok()?;
    }
    if let Some(local_view) = crate::conversation_view::load_conversation_for_source(
        &storage,
        crate::sources::provenance::LOCAL_SOURCE_ID,
        &source_path,
    )
    .ok()?
    {
        return Some(local_view);
    }
    crate::conversation_view::load_conversation(&storage, &source_path).ok()?
}

pub(crate) fn serialize_indexed_view_lines(
    view: &crate::conversation_view::ConversationView,
) -> CliResult<Vec<String>> {
    conversation_view_to_raw_messages(view)
        .into_iter()
        .map(|msg| {
            serde_json::to_string(&msg).map_err(|e| CliError {
                code: 9,
                kind: CliErrorKind::SerializeMessage.kind_str(),
                message: format!("Failed to serialize indexed message: {e}"),
                hint: Some(
                    "The indexed conversation contains unexpected data that could not be re-rendered as JSON."
                        .into(),
                ),
                retryable: false,
            })
        })
        .collect::<CliResult<Vec<_>>>()
}

pub(crate) fn read_followup_file_lines(path: &Path) -> CliResult<Vec<String>> {
    use std::fs::File;
    use std::io::{BufRead, BufReader};

    let file = File::open(path).map_err(|e| CliError {
        code: 9,
        kind: CliErrorKind::FileOpen.kind_str(),
        message: format!("Failed to open file: {e}"),
        hint: None,
        retryable: false,
    })?;
    let reader = BufReader::new(file);
    reader
        .lines()
        .collect::<std::io::Result<Vec<_>>>()
        .map_err(|e| CliError {
            code: 9,
            kind: CliErrorKind::FileRead.kind_str(),
            message: format!("Failed to read session file: {e}"),
            hint: Some("The session file may be truncated or contain invalid UTF-8".into()),
            retryable: false,
        })
}

pub(crate) fn infer_followup_agent_and_workspace(path: &Path) -> (Option<String>, Option<String>) {
    let path_str = path.to_string_lossy();
    let path_lower = path_str.to_ascii_lowercase().replace('\\', "/");

    let agent_name = [
        (".local/share/opencode", "opencode"),
        (".claude", "claude_code"),
        (".codex", "codex"),
        ("cursor", "cursor"),
        (".config/gemini", "gemini"),
        (".gemini", "gemini"),
    ]
    .into_iter()
    .find_map(|(marker, slug)| path_lower.contains(marker).then(|| slug.to_string()));

    let workspace = path.parent().map(|parent| parent.display().to_string());
    (agent_name, workspace)
}

pub(crate) struct ResolvedViewLine {
    pub(crate) number: usize,
    pub(crate) content: String,
    pub(crate) highlighted: bool,
}

pub(crate) struct ResolvedView {
    pub(crate) lines: Vec<ResolvedViewLine>,
    pub(crate) total_lines: usize,
    pub(crate) source_exists: bool,
    pub(crate) archive_only: bool,
}

pub(crate) enum BoundedViewResolution {
    Completed(CliResult<ResolvedView>),
    TimedOut,
}

pub(crate) fn resolve_view(
    path: &Path,
    db_path: &Path,
    source_id: Option<&str>,
    conversation_id: Option<i64>,
    line: Option<usize>,
    context: usize,
) -> CliResult<ResolvedView> {
    // The deterministic stall lives inside the same worker as the real
    // file/archive read, so the deadline covers the operation itself.
    maybe_test_view_delay();

    let indexed_view = try_load_indexed_conversation_from_db_with_source(
        path,
        db_path,
        source_id,
        conversation_id,
    );
    let allow_direct_file =
        conversation_id.is_none() && (followup_source_is_local(source_id) || source_id.is_none());
    let prefer_direct_file = conversation_id.is_none() && prefers_direct_view_file(path, source_id);
    let source_exists = path.exists();

    let (lines, archive_used): (Vec<String>, bool) = if prefer_direct_file {
        match read_followup_file_lines(path) {
            Ok(lines) => (lines, false),
            Err(err) => {
                if let Some(view) = indexed_view.as_ref() {
                    (serialize_indexed_view_lines(view)?, true)
                } else {
                    return Err(err);
                }
            }
        }
    } else if let Some(view) = indexed_view.as_ref() {
        (serialize_indexed_view_lines(view)?, true)
    } else if allow_direct_file && source_exists {
        (read_followup_file_lines(path)?, false)
    } else {
        let (kind, message, hint) = match (conversation_id, source_id) {
            (Some(conversation_id), _) => (
                CliErrorKind::IndexedSessionRequired.kind_str(),
                format!(
                    "No archived row matched conversation_id {conversation_id}, source {}, and path {}",
                    source_id.unwrap_or("[unspecified]"),
                    path.display()
                ),
                "Use the exact --db, --conversation-id, --source, and path from the incident report's suggested_command.argv."
                    .to_string(),
            ),
            (None, Some(source_id)) => (
                CliErrorKind::IndexedSessionRequired.kind_str(),
                format!(
                    "No archived row for source '{source_id}' at {} (archive row missing)",
                    path.display()
                ),
                "Use the exact source_id from search output, or re-run index to archive this session."
                    .to_string(),
            ),
            (None, None) if !source_exists => (
                CliErrorKind::FileNotFound.kind_str(),
                format!(
                    "Source file missing and no archived row found: {}",
                    path.display()
                ),
                "The source file moved or was pruned and is not archived; re-run index, then use the source_path from search output."
                    .to_string(),
            ),
            (None, None) => (
                CliErrorKind::FileNotFound.kind_str(),
                format!("File not found: {}", path.display()),
                "Path may be virtual (e.g. Cursor composer). Re-run index, then use the exact source_path from search output."
                    .to_string(),
            ),
        };
        return Err(CliError {
            code: 3,
            kind,
            message,
            hint: Some(hint),
            retryable: false,
        });
    };
    let archive_only = archive_used && !source_exists;

    if lines.is_empty() {
        return Err(CliError {
            code: 9,
            kind: CliErrorKind::EmptyFile.kind_str(),
            message: format!("File is empty: {}", path.display()),
            hint: None,
            retryable: false,
        });
    }

    let target_line = line.unwrap_or(1);
    if target_line > lines.len() {
        return Err(CliError {
            code: 2,
            kind: CliErrorKind::LineOutOfRange.kind_str(),
            message: format!(
                "Line {} exceeds file length ({} lines)",
                target_line,
                lines.len()
            ),
            hint: Some(format!("Use -n {} for the last line", lines.len())),
            retryable: false,
        });
    }

    let start = target_line.saturating_sub(context.saturating_add(1));
    let end = target_line.saturating_add(context).min(lines.len());
    let highlight_line = line.is_some();
    let total_lines = lines.len();
    let lines = lines
        .into_iter()
        .enumerate()
        .skip(start)
        .take(end - start)
        .map(|(index, content)| ResolvedViewLine {
            number: index + 1,
            content,
            highlighted: highlight_line && index + 1 == target_line,
        })
        .collect();

    Ok(ResolvedView {
        lines,
        total_lines,
        source_exists,
        archive_only,
    })
}

pub(crate) fn resolve_view_with_hard_timeout(
    path: PathBuf,
    db_path: PathBuf,
    source_id: Option<String>,
    conversation_id: Option<i64>,
    line: Option<usize>,
    context: usize,
    timeout: Duration,
) -> CliResult<BoundedViewResolution> {
    let (tx, rx) = std::sync::mpsc::channel();
    let _view_worker = std::thread::Builder::new()
        .name("cass-view-reader".to_string())
        .spawn(move || {
            let result = resolve_view(
                &path,
                &db_path,
                source_id.as_deref(),
                conversation_id,
                line,
                context,
            );
            let _ = tx.send(result);
        })
        .map_err(|err| CliError {
            code: 9,
            kind: CliErrorKind::FileRead.kind_str(),
            message: format!("Failed to start bounded view reader: {err}"),
            hint: Some("Retry `cass view`; the archive and source file were not modified.".into()),
            retryable: true,
        })?;

    match rx.recv_timeout(timeout) {
        Ok(result) => Ok(BoundedViewResolution::Completed(result)),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => Ok(BoundedViewResolution::TimedOut),
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => Err(CliError {
            code: 9,
            kind: CliErrorKind::FileRead.kind_str(),
            message: "Bounded view reader disconnected before returning a result".to_string(),
            hint: Some("Retry `cass view`; the archive and source file were not modified.".into()),
            retryable: true,
        }),
    }
}

#[derive(Clone, Copy)]
pub(crate) struct ViewWindow {
    pub(crate) line: Option<usize>,
    pub(crate) context: usize,
}

pub(crate) fn bounded_view_retry_command(
    path: &Path,
    db_path: &Path,
    source_id: Option<&str>,
    conversation_id: Option<i64>,
    window: ViewWindow,
    previous_budget_ms: u64,
    format: RobotFormat,
) -> String {
    let retry_budget_ms = previous_budget_ms.saturating_mul(2).max(10_000);
    let mut command = vec![
        "cass".to_string(),
        "--db".to_string(),
        shell_quote_arg(&db_path.display().to_string()),
        "view".to_string(),
        shell_quote_arg(&path.display().to_string()),
    ];
    if let Some(source_id) = source_id {
        command.push("--source".to_string());
        command.push(shell_quote_arg(source_id));
    }
    if let Some(conversation_id) = conversation_id {
        command.push("--conversation-id".to_string());
        command.push(conversation_id.to_string());
    }
    if let Some(line) = window.line {
        command.push("--line".to_string());
        command.push(line.to_string());
    }
    command.push("--context".to_string());
    command.push(window.context.to_string());
    match format {
        RobotFormat::Json => command.push("--json".to_string()),
        RobotFormat::Jsonl => {
            command.extend(["--robot-format".to_string(), "jsonl".to_string()]);
        }
        RobotFormat::Compact | RobotFormat::Sessions => {
            command.extend(["--robot-format".to_string(), "compact".to_string()]);
        }
        RobotFormat::Toon => {
            command.extend(["--robot-format".to_string(), "toon".to_string()]);
        }
    }
    command.push("--timeout".to_string());
    command.push(retry_budget_ms.to_string());
    command.join(" ")
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn output_bounded_view_partial(
    path: &Path,
    db_path: &Path,
    source_id: Option<&str>,
    conversation_id: Option<i64>,
    line: Option<usize>,
    context: usize,
    view_budget_ms: u64,
    view_budget: &crate::robot_budget_envelope::RobotBudget,
    format: RobotFormat,
    skipped_sections: Vec<String>,
) -> CliResult<()> {
    let recommended_next_probe = bounded_view_retry_command(
        path,
        db_path,
        source_id,
        conversation_id,
        ViewWindow { line, context },
        view_budget_ms,
        format,
    );
    let mut budget = crate::robot_budget_envelope::BudgetBlock::from_budget(
        view_budget,
        skipped_sections,
        Some(recommended_next_probe),
    );
    budget.elapsed_ms = budget.elapsed_ms.max(view_budget_ms);
    budget.timed_out = true;
    output_structured_value(
        serde_json::json!({
            "path": path.display().to_string(),
            "target_line": line,
            "context": context,
            "lines": [],
            "budget": budget,
        }),
        format,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn run_bounded_view(
    path: &Path,
    db_override: Option<PathBuf>,
    source_id: Option<&str>,
    conversation_id: Option<i64>,
    line: Option<usize>,
    context: usize,
    format: RobotFormat,
    timeout_ms: Option<u64>,
) -> CliResult<()> {
    let view_start = std::time::Instant::now();
    let view_budget_ms = timeout_ms.unwrap_or_else(|| {
        dotenvy::var("CASS_VIEW_BUDGET_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|&milliseconds| milliseconds > 0)
            .unwrap_or(10_000)
    });
    if let Some(source_id) = source_id {
        validate_followup_source_id(source_id, "cass view")?;
    }
    let target_line = line.unwrap_or(1);
    if target_line == 0 {
        return Err(CliError {
            code: 2,
            kind: CliErrorKind::InvalidLine.kind_str(),
            message: "Line numbers start at 1, not 0".to_string(),
            hint: Some("Use -n 1 for the first line".to_string()),
            retryable: false,
        });
    }

    let normalized_source_id = canonical_followup_source_id(source_id);
    let source_id = normalized_source_id.as_deref();
    let db_path = db_override.unwrap_or_else(default_db_path);
    let highlight_line = line.is_some();
    let view_budget =
        crate::robot_budget_envelope::RobotBudget::with_start(view_budget_ms, view_start);
    let outcome = if view_budget.is_exhausted() {
        BoundedViewResolution::TimedOut
    } else {
        resolve_view_with_hard_timeout(
            path.to_path_buf(),
            db_path.clone(),
            normalized_source_id.clone(),
            conversation_id,
            line,
            context,
            Duration::from_millis(view_budget.remaining_ms().max(1)),
        )?
    };
    let resolved = match outcome {
        BoundedViewResolution::Completed(result) => result?,
        BoundedViewResolution::TimedOut => {
            return output_bounded_view_partial(
                path,
                &db_path,
                source_id,
                conversation_id,
                line,
                context,
                view_budget_ms,
                &view_budget,
                format,
                vec!["view_content".to_string(), "source_provenance".to_string()],
            );
        }
    };

    let remaining_ms = view_budget.remaining_ms();
    let path_text = path.display().to_string();
    let projection_budget = view_budget;
    let encoded = run_read_only_search_worker(remaining_ms, move || {
        maybe_test_search_worker_delay("CASS_TEST_VIEW_PROJECTION_SLOW_MS");
        let content_lines: Vec<serde_json::Value> = resolved
            .lines
            .iter()
            .map(|line| {
                serde_json::json!({
                    "line": line.number,
                    "content": line.content,
                    "highlighted": line.highlighted,
                })
            })
            .collect();
        let budget = crate::robot_budget_envelope::BudgetBlock::from_budget(
            &projection_budget,
            Vec::new(),
            None,
        );
        encode_structured_value(
            serde_json::json!({
                "path": path_text,
                "target_line": if highlight_line { Some(target_line) } else { None::<usize> },
                "context": context,
                "lines": content_lines,
                "total_lines": resolved.total_lines,
                "source_exists": resolved.source_exists,
                "archive_only": resolved.archive_only,
                "budget": budget,
            }),
            format,
        )
    })?;
    if let Some(encoded) = encoded {
        if matches!(format, RobotFormat::Toon) {
            print!("{encoded}");
        } else {
            println!("{encoded}");
        }
        return Ok(());
    }

    output_bounded_view_partial(
        path,
        &db_path,
        source_id,
        conversation_id,
        line,
        context,
        view_budget_ms,
        &view_budget,
        format,
        vec![
            "view_content".to_string(),
            "source_provenance".to_string(),
            "output_projection".to_string(),
        ],
    )
}

pub(crate) fn run_view(
    path: &Path,
    db_override: Option<PathBuf>,
    source_id: Option<&str>,
    conversation_id: Option<i64>,
    window: ViewWindow,
    output_format: Option<RobotFormat>,
    timeout_ms: Option<u64>,
) -> CliResult<()> {
    let ViewWindow { line, context } = window;
    let structured_format = output_format.or_else(robot_format_from_env).map(|format| {
        if matches!(format, RobotFormat::Sessions) {
            RobotFormat::Compact
        } else {
            format
        }
    });
    if let Some(format) = structured_format {
        return run_bounded_view(
            path,
            db_override,
            source_id,
            conversation_id,
            line,
            context,
            format,
            timeout_ms,
        );
    }

    // Bounded-budget signal (uojcg.2.6 / 2.2): the report saw `cass view` fail
    // under a 10s cap. View is a single bounded read (file fast-path or DB/archive
    // fallback), so it sheds nothing; this reports whether the read exceeded its
    // budget so an agent can fall back. Override via CASS_VIEW_BUDGET_MS.
    let view_start = std::time::Instant::now();
    let view_budget_ms = dotenvy::var("CASS_VIEW_BUDGET_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&ms| ms > 0)
        .unwrap_or(10_000);
    maybe_test_view_delay();
    if let Some(source_id) = source_id {
        validate_followup_source_id(source_id, "cass view")?;
    }
    let normalized_source_id = canonical_followup_source_id(source_id);
    let source_id = normalized_source_id.as_deref();

    let db_path = db_override.unwrap_or_else(default_db_path);
    let indexed_view = try_load_indexed_conversation_from_db_with_source(
        path,
        &db_path,
        source_id,
        conversation_id,
    );
    let allow_direct_file =
        conversation_id.is_none() && (followup_source_is_local(source_id) || source_id.is_none());

    let prefer_direct_file = conversation_id.is_none() && prefers_direct_view_file(path, source_id);
    let source_exists = path.exists();

    // Archive-only resolution (uojcg.2.3): when the source file is stale, moved,
    // vanished, or belongs to a remote/mapped workspace, fall back to the
    // canonical DB/archive row rather than failing. File reads are a fast path,
    // not the only path. `archive_used` records whether the content came from the
    // archive so the output can advertise source_exists / archive_only.
    let (lines, archive_used): (Vec<String>, bool) = if prefer_direct_file {
        match read_followup_file_lines(path) {
            Ok(lines) => (lines, false),
            Err(err) => {
                if let Some(view) = indexed_view.as_ref() {
                    (serialize_indexed_view_lines(view)?, true)
                } else {
                    return Err(err);
                }
            }
        }
    } else if let Some(view) = indexed_view.as_ref() {
        (serialize_indexed_view_lines(view)?, true)
    } else if allow_direct_file && source_exists {
        (read_followup_file_lines(path)?, false)
    } else {
        // No archive row was found. Distinguish a missing ARCHIVE ROW from a
        // missing SOURCE FILE so an agent knows whether to reindex vs. fix the
        // path (uojcg.2.3).
        let (kind, message, hint) = match (conversation_id, source_id) {
            (Some(conversation_id), _) => (
                CliErrorKind::IndexedSessionRequired.kind_str(),
                format!(
                    "No archived row matched conversation_id {conversation_id}, source {}, and path {}",
                    source_id.unwrap_or("[unspecified]"),
                    path.display()
                ),
                "Use the exact --db, --conversation-id, --source, and path from the incident report's suggested_command.argv."
                    .to_string(),
            ),
            (None, Some(source_id)) => (
                CliErrorKind::IndexedSessionRequired.kind_str(),
                format!(
                    "No archived row for source '{source_id}' at {} (archive row missing)",
                    path.display()
                ),
                "Use the exact source_id from search output, or re-run index to archive this session."
                    .to_string(),
            ),
            (None, None) if !source_exists => (
                CliErrorKind::FileNotFound.kind_str(),
                format!(
                    "Source file missing and no archived row found: {}",
                    path.display()
                ),
                "The source file moved or was pruned and is not archived; re-run index, then use the source_path from search output."
                    .to_string(),
            ),
            (None, None) => (
                CliErrorKind::FileNotFound.kind_str(),
                format!("File not found: {}", path.display()),
                "Path may be virtual (e.g. Cursor composer). Re-run index, then use the exact source_path from search output."
                    .to_string(),
            ),
        };
        return Err(CliError {
            code: 3,
            kind,
            message,
            hint: Some(hint),
            retryable: false,
        });
    };
    let archive_only = archive_used && !source_exists;

    if lines.is_empty() {
        return Err(CliError {
            code: 9,
            kind: CliErrorKind::EmptyFile.kind_str(),
            message: format!("File is empty: {}", path.display()),
            hint: None,
            retryable: false,
        });
    }

    let target_line = line.unwrap_or(1);

    if target_line == 0 {
        return Err(CliError {
            code: 2,
            kind: CliErrorKind::InvalidLine.kind_str(),
            message: "Line numbers start at 1, not 0".to_string(),
            hint: Some("Use -n 1 for the first line".to_string()),
            retryable: false,
        });
    }

    if target_line > lines.len() {
        return Err(CliError {
            code: 2,
            kind: CliErrorKind::LineOutOfRange.kind_str(),
            message: format!(
                "Line {} exceeds file length ({} lines)",
                target_line,
                lines.len()
            ),
            hint: Some(format!("Use -n {} for the last line", lines.len())),
            retryable: false,
        });
    }

    let start = target_line.saturating_sub(context.saturating_add(1));
    let end = target_line.saturating_add(context).min(lines.len());
    let highlight_line = line.is_some();

    let structured_format = output_format.or_else(robot_format_from_env).map(|fmt| {
        if matches!(fmt, RobotFormat::Sessions) {
            RobotFormat::Compact
        } else {
            fmt
        }
    });

    if let Some(fmt) = structured_format {
        let content_lines: Vec<serde_json::Value> = lines
            .iter()
            .enumerate()
            .skip(start)
            .take(end - start)
            .map(|(i, l)| {
                serde_json::json!({
                    "line": i + 1,
                    "content": l,
                    "highlighted": highlight_line && i + 1 == target_line,
                })
            })
            .collect();

        let view_budget =
            crate::robot_budget_envelope::RobotBudget::with_start(view_budget_ms, view_start);
        let recommended_next_probe = view_budget
            .is_exhausted()
            .then(|| "cass health --json".to_string());
        let budget = crate::robot_budget_envelope::BudgetBlock::from_budget(
            &view_budget,
            Vec::new(),
            recommended_next_probe,
        );
        let payload = serde_json::json!({
            "path": path.display().to_string(),
            "target_line": if highlight_line { Some(target_line) } else { None::<usize> },
            "context": context,
            "lines": content_lines,
            "total_lines": lines.len(),
            // Provenance so agents can tell a live-file read from an archive-only
            // resolution (uojcg.2.3): archive_only=true means the source file is
            // gone/stale and this content came from the canonical DB/archive row.
            "source_exists": source_exists,
            "archive_only": archive_only,
            // Bounded-budget signal (uojcg.2.6): elapsed vs. the view budget.
            "budget": budget,
        });
        return output_structured_value(payload, fmt);
    }

    println!("File: {}", path.display());
    if highlight_line {
        println!("Line: {target_line} (context: {context})");
    }
    println!("----------------------------------------");
    for (i, l) in lines.iter().enumerate().skip(start).take(end - start) {
        let line_num = i + 1;
        let marker = if highlight_line && line_num == target_line {
            ">"
        } else {
            " "
        };
        println!("{marker}{line_num:5} | {l}");
    }
    println!("----------------------------------------");
    if lines.len() > end {
        println!("... ({} more lines)", lines.len() - end);
    }

    Ok(())
}

/// Deterministic slowdown for the `cass view` budget regression test.
///
/// A real one-line file read can legitimately complete inside the timer's
/// millisecond truncation window on fast hosts, so a tiny budget alone cannot
/// prove the timeout branch. This mirrors `CASS_TEST_STATUS_SLOW_MS` and is
/// inert unless an explicit test-only environment variable is present.
pub(crate) fn maybe_test_view_delay() {
    if let Ok(raw) = dotenvy::var("CASS_TEST_VIEW_SLOW_MS")
        && let Ok(ms) = raw.parse::<u64>()
        && ms > 0
    {
        std::thread::sleep(Duration::from_millis(ms));
    }
}
