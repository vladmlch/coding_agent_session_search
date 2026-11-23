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

/// Map an index-resolution failure onto the CLI error taxonomy (plan Task 4,
/// "Error contract"). Every variant is terminal: export never falls back to
/// the original source file. Identity problems (not-indexed, ambiguous) are
/// user-input failures (code 3); archive health problems (unavailable, read
/// failure, no renderable messages) are operational failures (code 9).
/// Hints name `cass index` because the exported state is the indexed
/// snapshot — freshness is repaired by indexing, never by re-reading files.
pub(crate) fn indexed_export_cli_error(
    err: crate::export::indexed::IndexedExportError,
) -> CliError {
    use crate::export::indexed::IndexedExportError;
    match err {
        IndexedExportError::IndexUnavailable { path, source } => CliError {
            code: 9,
            kind: CliErrorKind::IndexUnavailable.kind_str(),
            message: format!("Session index unavailable at {}: {source}", path.display()),
            hint: Some("Run 'cass index' to (re)build the index, then export again.".to_string()),
            retryable: false,
        },
        IndexedExportError::ConversationNotIndexed {
            source_path,
            source_id,
            conversation_id,
        } => CliError {
            code: 3,
            kind: CliErrorKind::ConversationNotIndexed.kind_str(),
            message: format!(
                "No indexed conversation found for {} (source: {}{})",
                source_path.display(),
                source_id.as_deref().unwrap_or("any"),
                conversation_id
                    .map(|id| format!(", conversation_id: {id}"))
                    .unwrap_or_default()
            ),
            hint: Some("Run 'cass index' to index it, then export again.".to_string()),
            retryable: false,
        },
        IndexedExportError::AmbiguousIdentity {
            source_path,
            candidates,
        } => {
            let listing = candidates
                .iter()
                .map(|candidate| {
                    format!(
                        "id={} title={} started_at={} source_id={}",
                        candidate
                            .conversation_id
                            .map(|id| id.to_string())
                            .unwrap_or_else(|| "?".to_string()),
                        candidate.title.as_deref().unwrap_or("<untitled>"),
                        candidate
                            .started_at
                            .map(|ts| ts.to_string())
                            .unwrap_or_else(|| "?".to_string()),
                        candidate.source_id,
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            CliError {
                code: 3,
                kind: CliErrorKind::AmbiguousIdentity.kind_str(),
                message: format!(
                    "Export identity for {} is ambiguous: {} indexed conversations share this path/source: {listing}",
                    source_path.display(),
                    candidates.len()
                ),
                hint: Some(
                    "Re-run with --source <source_id> or --conversation-id <id> to select exactly one conversation."
                        .to_string(),
                ),
                retryable: false,
            }
        }
        IndexedExportError::IndexReadFailure { path, source } => CliError {
            code: 9,
            kind: CliErrorKind::IndexReadFailure.kind_str(),
            message: format!(
                "Failed to read the session index at {}: {source}",
                path.display()
            ),
            hint: Some("Run 'cass index' to rebuild the index, then export again.".to_string()),
            retryable: false,
        },
    }
}

/// Map a projection failure onto the CLI error taxonomy. A non-empty indexed
/// conversation must never become an empty successful export.
pub(crate) fn export_projection_cli_error(
    err: crate::export::projection::ExportProjectionError,
) -> CliError {
    use crate::export::projection::ExportProjectionError;
    match err {
        ExportProjectionError::NoRenderableMessages { indexed } => CliError {
            code: 9,
            kind: CliErrorKind::NoRenderableMessages.kind_str(),
            message: format!(
                "The indexed session has {indexed} message(s) but none are renderable (only tool or bookkeeping records were archived)"
            ),
            hint: Some(
                "Retry with --include-tools, or run 'cass index' to refresh the archived content."
                    .to_string(),
            ),
            retryable: false,
        },
    }
}

/// Export a conversation to markdown or other formats
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_export(
    path: &Path,
    db_override: Option<PathBuf>,
    source_id: Option<&str>,
    conversation_id: Option<i64>,
    format: ConvExportFormat,
    output: Option<&Path>,
    clipboard: bool,
    include_tools: bool,
    include_skills: bool,
    include_secrets: bool,
) -> CliResult<()> {
    use std::fs::File;
    use std::io::Write;

    if let Some(source_id) = source_id {
        validate_followup_source_id(source_id, "cass export")?;
    }
    let normalized_source_id = canonical_followup_source_id(source_id);

    let db_path = db_override.unwrap_or_else(default_db_path);

    // Index-first export (plan Invariants 1-3): the canonical indexed
    // conversation is the sole source of truth. `path` is an opaque
    // identity key — never opened, sniffed, or decompressed — and every
    // index/projection failure is terminal: no fallback to the original
    // source file on any of them.
    let view = crate::export::indexed::load_indexed_export(
        &db_path,
        &crate::export::indexed::ExportSelector {
            source_path: path.to_path_buf(),
            source_id: normalized_source_id.clone(),
            conversation_id,
        },
    )
    .map_err(indexed_export_cli_error)?;

    let conversation = crate::export::projection::project_indexed_conversation(
        &view,
        &crate::export::projection::ExportOptions { include_tools },
    )
    .map_err(export_projection_cli_error)?;

    tracing::debug!(
        indexed = conversation.stats.total_indexed,
        emitted = conversation.stats.emitted,
        tool_payloads_indexed = conversation.stats.tool_payloads_indexed,
        tool_payloads_emitted = conversation.stats.tool_payloads_emitted,
        unknown_roles = conversation.stats.unknown_roles,
        "export projection stats"
    );
    let mut messages = crate::export::projection::to_raw_messages(&conversation);
    let mut session_title: Option<String> = conversation.title.clone();
    let session_start: Option<i64> = conversation.started_at;

    let mut privacy = crate::export::ExportPrivacy::new(include_secrets);
    privacy.redact_messages(&mut messages);
    privacy.redact_optional_text(&mut session_title);

    if !include_skills {
        messages.retain(|msg| {
            let content = extract_text_content(msg);
            !crate::export::is_skill_injection(&content)
        });
    }

    if messages.is_empty() {
        return Err(CliError {
            code: 9,
            kind: CliErrorKind::EmptySession.kind_str(),
            message: format!("No messages found in: {}", path.display()),
            hint: Some(
                "The indexed conversation produced no exportable messages; run 'cass index' to refresh it or pass --include-tools/--include-skills."
                    .to_string(),
            ),
            retryable: false,
        });
    }

    if session_title.is_none() {
        for msg in &messages {
            let role = extract_role(msg);
            if role == "user" {
                let content = extract_text_content(msg);
                if !content.is_empty() {
                    let first_line = content.lines().next().unwrap_or("Untitled Session");
                    session_title = Some(smart_truncate(first_line, 80));
                    break;
                }
            }
        }
    }

    privacy.announce();
    let formatted = match format {
        ConvExportFormat::Markdown => {
            format_as_markdown(&messages, &session_title, session_start, include_tools)
        }
        ConvExportFormat::Text => format_as_text(&messages, include_tools),
        ConvExportFormat::Json => serde_json::to_string_pretty(&messages).unwrap_or_default(),
        ConvExportFormat::Html => {
            format_as_html(&messages, &session_title, session_start, include_tools)
        }
    };

    if let Some(out_path) = output {
        let mut out_file = File::create(out_path).map_err(|e| CliError {
            code: 9,
            kind: CliErrorKind::FileCreate.kind_str(),
            message: format!("Failed to create output file: {e}"),
            hint: None,
            retryable: false,
        })?;
        out_file
            .write_all(formatted.as_bytes())
            .map_err(|e| CliError {
                code: 9,
                kind: CliErrorKind::FileWrite.kind_str(),
                message: format!("Failed to write output: {e}"),
                hint: None,
                retryable: false,
            })?;
        println!("Exported to: {}", out_path.display());
    } else if clipboard {
        match copy_to_system_clipboard(&formatted) {
            Ok(tool) => {
                let bytes = formatted.len();
                eprintln!(
                    "Copied {bytes} bytes to clipboard via {tool}. Paste into your coding agent's chat to resume the conversation."
                );
            }
            Err(err) => {
                // Headless/SSH and similar — fall back to stdout so the
                // caller still gets the export, but make the failure
                // legible on stderr so a wrapper script knows clipboard
                // handoff didn't happen.
                eprintln!("warning: clipboard not available ({err}); falling back to stdout.");
                println!("{formatted}");
            }
        }
    } else {
        println!("{formatted}");
    }

    Ok(())
}

/// Stream `text` into the system clipboard by spawning the platform's
/// canonical clipboard tool. Returns the name of the tool used on success.
///
/// Used by `cass export --clipboard` so users can hand a freshly-found
/// thread into their coding agent (Cursor, Claude Code, …) with one
/// keystroke. Shells out instead of taking a clipboard-crate dependency
/// because (a) no current dep covers all four targets we care about and
/// (b) the sub-process approach degrades cleanly to the stdout fallback
/// when no tool is on PATH (headless servers, SSH sessions).
pub(crate) fn copy_to_system_clipboard(text: &str) -> Result<&'static str, String> {
    use std::process::Command;

    // (program, args). First match wins. Order matters on Linux:
    // Wayland sessions ignore X11 selections, so wl-copy must be tried
    // before xclip / xsel even though the latter are more common.
    let candidates: &[(&str, &[&str])] = if cfg!(target_os = "macos") {
        &[("pbcopy", &[])]
    } else if cfg!(target_os = "windows") {
        &[("clip", &[])]
    } else {
        &[
            ("wl-copy", &[]),
            ("xclip", &["-selection", "clipboard"]),
            ("xsel", &["--clipboard", "--input"]),
        ]
    };

    let mut last_err: Option<String> = None;
    for (program, args) in candidates {
        let mut command = Command::new(program); // ubs:ignore[rust.security.command-executable] — Program comes only from the fixed platform clipboard-tool list above; text is written to stdin.
        command.args(args.iter().copied());
        match write_clipboard_command(command, text) {
            Ok(status) if status.success() => return Ok(program),
            Ok(status) => {
                last_err = Some(format!("{program} exited with status {status}"));
            }
            Err(e) => {
                last_err = Some(format!("{program}: {e}"));
            }
        }
    }

    let tried = candidates
        .iter()
        .map(|(p, _)| *p)
        .collect::<Vec<_>>()
        .join(", ");
    Err(match last_err {
        Some(detail) => {
            format!("no clipboard tool succeeded (tried {tried}; last error: {detail})")
        }
        None => format!("no clipboard tool found on PATH (tried {tried})"),
    })
}

pub(crate) fn write_clipboard_command(
    mut command: std::process::Command,
    text: &str,
) -> std::io::Result<std::process::ExitStatus> {
    use std::io::Write;
    use std::process::Stdio;

    // Unlike a ChildStdin pipe, a socket write can be kept from raising
    // SIGPIPE even when main has restored SIG_DFL for stdout: std sends with
    // MSG_NOSIGNAL on Linux, and macOS, which lacks that flag, needs
    // SO_NOSIGPIPE on our end. The unnamed pair keeps clipboard bytes off
    // disk and delivers ordinary readable stdin.
    #[cfg(unix)]
    let (mut stdin, child_stdin) = std::os::unix::net::UnixStream::pair()?;
    #[cfg(target_os = "macos")]
    socket2::SockRef::from(&stdin).set_nosigpipe(true)?;
    #[cfg(unix)]
    command.stdin(Stdio::from(std::os::fd::OwnedFd::from(child_stdin)));
    #[cfg(not(unix))]
    command.stdin(Stdio::piped());

    // Clipboard tools may fork a selection daemon that inherits these
    // descriptors. Null output plus wait() avoids waiting for daemon EOF.
    command.stdout(Stdio::null()).stderr(Stdio::null());
    let mut child = command.spawn()?;
    // Command retains its configured stdin handle after spawn. Close that
    // copy now so a child that exits early leaves no reader in this process.
    drop(command);
    #[cfg(not(unix))]
    let mut stdin = match child.stdin.take() {
        Some(stdin) => stdin,
        None => {
            let _ = child.wait();
            return Err(std::io::Error::other("clipboard stdin pipe missing"));
        }
    };
    let write_result = stdin.write_all(text.as_bytes());
    drop(stdin);
    let status = child.wait();
    write_result?;
    status
}

pub(crate) fn format_export_duration(
    session_start: Option<i64>,
    session_end: Option<i64>,
) -> Option<String> {
    let (Some(start), Some(end)) = (session_start, session_end) else {
        return None;
    };
    let elapsed_ms = end.checked_sub(start).filter(|elapsed| *elapsed > 0)?;
    let mins = elapsed_ms / 60_000;
    if mins >= 60 {
        Some(format!("{}h {}m", mins / 60, mins % 60))
    } else if mins > 0 {
        Some(format!("{}m", mins))
    } else {
        Some("< 1m".to_string())
    }
}

pub(crate) fn publish_unique_export_output_file(
    output_directory: &Path,
    final_filename: &str,
    output_path: &mut PathBuf,
    write_staged: impl FnOnce(&mut std::fs::File) -> io::Result<()>,
) -> io::Result<()> {
    const OUTPUT_CREATE_ATTEMPTS: usize = 1000;

    let mut staged = tempfile::Builder::new()
        .prefix(".cass-export-")
        .suffix(".tmp")
        .tempfile_in(output_directory)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        staged
            .as_file()
            .set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }

    write_staged(staged.as_file_mut())?;
    staged.as_file_mut().flush()?;
    staged.as_file().sync_all()?;

    for _ in 0..OUTPUT_CREATE_ATTEMPTS {
        match staged.persist_noclobber(output_path.as_path()) {
            Ok(published) => {
                published.sync_all()?;
                sync_export_parent_directory(output_path)?;
                return Ok(());
            }
            Err(err) if err.error.kind() == io::ErrorKind::AlreadyExists => {
                staged = err.file;
                *output_path = html_export::unique_filename(output_directory, final_filename);
            }
            Err(err) => return Err(err.error),
        }
    }

    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        format!("could not reserve a unique output filename after {OUTPUT_CREATE_ATTEMPTS} tries"),
    ))
}

pub(crate) fn write_unique_export_output_file(
    output_directory: &Path,
    final_filename: &str,
    output_path: &mut PathBuf,
    contents: &[u8],
) -> io::Result<()> {
    publish_unique_export_output_file(output_directory, final_filename, output_path, |file| {
        file.write_all(contents)
    })
}

#[cfg(not(windows))]
pub(crate) fn sync_export_parent_directory(path: &Path) -> io::Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    std::fs::File::open(parent)?.sync_all()
}

#[cfg(windows)]
pub(crate) fn sync_export_parent_directory(_path: &Path) -> io::Result<()> {
    // Windows does not document FlushFileBuffers for directory handles. The
    // staged file itself is synced before and after the no-clobber publish.
    Ok(())
}

/// Export a session as a self-contained HTML file.
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_export_html(
    session_path: &Path,
    db_override: Option<PathBuf>,
    source_id: Option<&str>,
    conversation_id: Option<i64>,
    output_dir: Option<&Path>,
    filename: Option<&str>,
    include_tools: bool,
    show_timestamps: bool,
    include_skills: bool,
    include_secrets: bool,
    theme: &str,
    dry_run: bool,
    explain: bool,
    open: bool,
    output_format: Option<RobotFormat>,
) -> CliResult<()> {
    use chrono::TimeZone;
    use html_export::{
        ExportOptions as HtmlExportOptions, HtmlExporter, Message, TemplateMetadata,
        generate_full_filename, get_downloads_dir, is_valid_filename, unique_filename,
    };
    use std::io;

    if let Some(source_id) = source_id {
        validate_followup_source_id(source_id, "cass export-html")?;
    }
    let normalized_source_id = canonical_followup_source_id(source_id);
    let structured_error_output = output_format.or_else(robot_format_from_env).is_some();
    let emit_structured_error = |err: &CliError| {
        if structured_error_output {
            println!(
                "{}",
                serde_json::json!({
                    "success": false,
                    "error": {
                        "code": err.code,
                        "kind": err.kind,
                        "message": err.message,
                        "hint": err.hint,
                        "retryable": err.retryable
                    }
                })
            );
        }
    };

    let db_path = db_override.unwrap_or_else(default_db_path);

    // Index-first export (plan Invariants 1-3): the canonical indexed
    // conversation is the sole source of truth. `session_path` is an opaque
    // identity key — never opened, sniffed, or decompressed — and every
    // index/projection failure is terminal: no fallback to the original
    // source file on any of them.
    let view = match crate::export::indexed::load_indexed_export(
        &db_path,
        &crate::export::indexed::ExportSelector {
            source_path: session_path.to_path_buf(),
            source_id: normalized_source_id.clone(),
            conversation_id,
        },
    ) {
        Ok(view) => view,
        Err(err) => {
            let err = indexed_export_cli_error(err);
            emit_structured_error(&err);
            return Err(err);
        }
    };

    // --- Project the indexed conversation into renderer messages ---
    //
    // `to_raw_messages` merges index-retained tool payloads (`message`,
    // `type`, `payload`, flat tool keys) onto the `{role, content,
    // timestamp?, author?}` envelope, so `extract_tool_calls`/`extract_*`
    // resolve the same shapes the old whole-envelope swap produced — pi/omp
    // tool detail still reaches `extract_tool_calls` under `--include-tools`.
    let conversation = match crate::export::projection::project_indexed_conversation(
        &view,
        &crate::export::projection::ExportOptions { include_tools },
    ) {
        Ok(conversation) => conversation,
        Err(err) => {
            let err = export_projection_cli_error(err);
            emit_structured_error(&err);
            return Err(err);
        }
    };

    tracing::debug!(
        indexed = conversation.stats.total_indexed,
        emitted = conversation.stats.emitted,
        tool_payloads_indexed = conversation.stats.tool_payloads_indexed,
        tool_payloads_emitted = conversation.stats.tool_payloads_emitted,
        unknown_roles = conversation.stats.unknown_roles,
        "export projection stats"
    );
    let mut raw_messages = crate::export::projection::to_raw_messages(&conversation);
    let (path_agent_name, path_workspace) = infer_followup_agent_and_workspace(session_path);
    let mut session_title: Option<String> = conversation.title.clone();
    let session_start: Option<i64> = conversation.started_at;
    let session_end: Option<i64> = conversation.ended_at;
    let mut agent_name: Option<String> = Some(conversation.agent_slug.clone()).or(path_agent_name);
    let mut workspace: Option<String> = view
        .convo
        .workspace
        .as_ref()
        .map(|path| path.display().to_string())
        .or(path_workspace);

    if raw_messages.is_empty() {
        let err = CliError {
            code: 9,
            kind: CliErrorKind::EmptySession.kind_str(),
            message: format!("No messages found in: {}", session_path.display()),
            hint: Some(
                "The indexed conversation produced no exportable messages; run 'cass index' to refresh it or pass --include-tools/--include-skills."
                    .to_string(),
            ),
            retryable: false,
        };
        emit_structured_error(&err);
        return Err(err);
    }

    let mut privacy = crate::export::ExportPrivacy::new(include_secrets);
    privacy.redact_messages(&mut raw_messages);
    privacy.redact_optional_text(&mut session_title);
    privacy.redact_optional_text(&mut workspace);
    privacy.redact_optional_text(&mut agent_name);

    // Find title from first user message
    if session_title.is_none() {
        for msg in &raw_messages {
            let role = extract_role(msg);
            if role == "user" {
                let content = extract_text_content(msg);
                if !content.is_empty() {
                    let first_line = content.lines().next().unwrap_or("Untitled Session");
                    session_title = Some(smart_truncate(first_line, 80));
                    break;
                }
            }
        }
    }

    let pi_family_export = matches!(agent_name.as_deref(), Some("pi_agent" | "omp"));

    // --- Convert to renderer::Message format (filtering empty messages) ---
    let messages: Vec<Message> = raw_messages
        .iter()
        .enumerate()
        .flat_map(|(i, msg)| {
            let role = extract_role(msg);
            if !include_tools && role == "tool" {
                // Several providers encode tool results as standalone
                // messages. Honor `--include-tools=false` from the message
                // role itself even when the provider cannot be inferred from
                // an arbitrary direct path.
                return Vec::new();
            }
            let ts = extract_message_timestamp(msg);
            let timestamp = ts
                .and_then(|ts| chrono::Utc.timestamp_millis_opt(ts).single())
                .map(|dt| dt.to_rfc3339());

            // Extract all structured tool blocks if present. A single Claude
            // message can contain multiple tool_use blocks; preserve each one
            // as its own renderer message so grouping can attach all results.
            let tool_calls = if include_tools {
                extract_tool_calls(msg)
            } else {
                Vec::new()
            };

            let content = if include_tools {
                if !tool_calls.is_empty() {
                    strip_tool_marker(&extract_text_content_without_tool_blocks(msg))
                } else {
                    extract_text_content(msg)
                }
            } else {
                let content = extract_text_content_without_tool_blocks(msg);
                if pi_family_export && role == "assistant" {
                    // Older/incomplete indexed Pi-family rows may lack a
                    // recoverable source envelope. Their normalized assistant
                    // text contains one flattened `[Tool: ...] ...` line per
                    // invocation, so fail closed on those lines when tools
                    // were explicitly excluded.
                    strip_flattened_tool_call_lines(&content)
                } else {
                    content
                }
            };

            // --- Drop entire messages that are skill injections (unless opted in) ---
            // When Claude Code/Codex/Gemini load a skill, the FULL SKILL.md body is
            // injected as a user message starting with "Base directory for this skill:".
            // These are often highly proprietary. DROP THE ENTIRE MESSAGE — don't try
            // to parse, redact, or pattern-match the content. Just skip it.
            if !include_skills && crate::export::is_skill_injection(&content) {
                return Vec::new();
            }

            // Skip non-message records (queue-operation, summary, etc.)
            // These are internal bookkeeping entries, not actual conversation messages.
            // Only user, assistant, system, tool, and unknown are valid message roles.
            match role.as_str() {
                "user" | "assistant" | "system" | "tool" | "unknown" => {}
                _ => return Vec::new(),
            }

            // Skip empty messages: no content AND no tool call AND unknown role
            // This filters out malformed/empty entries that would look broken
            if content.is_empty() && tool_calls.is_empty() && role == "unknown" {
                return Vec::new();
            }

            // Also skip messages that are completely empty (no content, no tool call)
            // but keep tool calls even if content is empty (shows the tool interaction)
            if content.is_empty() && tool_calls.is_empty() {
                return Vec::new();
            }

            if tool_calls.is_empty() {
                return vec![Message {
                    role,
                    content,
                    timestamp,
                    tool_call: None,
                    index: Some(i),
                    author: None,
                }];
            }

            tool_calls
                .into_iter()
                .enumerate()
                .map(|(tool_index, tool_call)| Message {
                    role: role.clone(),
                    content: if tool_index == 0 {
                        content.clone()
                    } else {
                        String::new()
                    },
                    timestamp: timestamp.clone(),
                    tool_call: Some(tool_call),
                    index: Some(i),
                    author: None,
                })
                .collect::<Vec<_>>()
        })
        .collect();

    // Count message types from RAW messages for accuracy.
    // Must distinguish human-typed prompts from tool results, which both have role "user".
    // Tool results have content as an array containing {"type": "tool_result", ...} blocks.
    // Also exclude skill injections from counts when !include_skills, since those messages
    // are dropped from the rendered output and shouldn't inflate the prompt count.
    //
    // Count over a tool-inclusive projection: `--include-tools=false` withholds
    // the merged tool envelopes from `raw_messages`, which would otherwise hide
    // `tool_use`/`tool_result` blocks from the metadata counts (a
    // `toolResult`-envelope row under a `user` role would also inflate
    // `human_turns`). The rendered output is unaffected — the per-message
    // filters above apply `include_tools` at render time. A failed
    // re-projection cannot happen when the flagged one already succeeded, but
    // degrade to the rendered set rather than error the export for a stat.
    let counting_raws_storage;
    let counting_raws: &[serde_json::Value] = if include_tools {
        &raw_messages
    } else {
        match crate::export::projection::project_indexed_conversation(
            &view,
            &crate::export::projection::ExportOptions {
                include_tools: true,
            },
        ) {
            Ok(count_conversation) => {
                counting_raws_storage =
                    crate::export::projection::to_raw_messages(&count_conversation);
                &counting_raws_storage
            }
            Err(_) => &raw_messages,
        }
    };
    let mut human_turns = 0usize;
    let mut assistant_msgs = 0usize;
    let mut tool_use_count = 0usize;
    for msg in counting_raws {
        let role = extract_role(msg);
        let text = extract_text_content(msg);

        // Skip messages that would be dropped by skill filtering
        if !include_skills && crate::export::is_skill_injection(&text) {
            continue;
        }

        match role.as_str() {
            "user" => {
                // Check if this is a tool result return or a human-typed message.
                // Tool results have content as an array with tool_result type blocks.
                let is_tool_result = msg
                    .get("message")
                    .or(Some(msg))
                    .and_then(|m| m.get("content"))
                    .and_then(|c| c.as_array())
                    .is_some_and(|arr| {
                        arr.iter().any(|item| {
                            item.get("type").and_then(|t| t.as_str()) == Some("tool_result")
                        })
                    });
                if !is_tool_result {
                    human_turns += 1;
                }
            }
            "assistant" => {
                assistant_msgs += 1;
                // Count tool_use blocks within assistant messages
                if let Some(content) = msg
                    .get("message")
                    .or(Some(msg))
                    .and_then(|m| m.get("content"))
                    .and_then(|c| c.as_array())
                {
                    tool_use_count += content
                        .iter()
                        .filter(|item| {
                            matches!(
                                item.get("type").and_then(|t| t.as_str()),
                                Some("tool_use" | "toolCall")
                            )
                        })
                        .count();
                }
            }
            _ => {}
        }
    }
    let message_count = messages.len();

    // --- Build metadata ---
    let duration = format_export_duration(session_start, session_end);

    let metadata = TemplateMetadata {
        timestamp: session_start.map(|ts| {
            chrono::Utc
                .timestamp_millis_opt(ts)
                .single()
                .map(|dt| dt.format("%Y-%m-%d %H:%M UTC").to_string())
                .unwrap_or_default()
        }),
        agent: agent_name.clone(),
        message_count,
        human_turns,
        assistant_msgs,
        tool_use_count,
        duration,
        project: workspace.clone(),
    };

    // --- Generate output path ---
    let output_directory = output_dir
        .map(|p| p.to_path_buf())
        .unwrap_or_else(get_downloads_dir);

    let workspace_path = workspace.as_deref().map(Path::new);

    let mut final_filename = if let Some(name) = filename {
        name.to_string()
    } else {
        generate_full_filename(
            agent_name.as_deref().unwrap_or("cass"),
            workspace_path,
            session_start,
            session_title.as_deref(),
            session_title.as_deref(),
        )
    };

    if Path::new(&final_filename).extension().is_none() {
        final_filename.push_str(".html");
    }

    if filename.is_some() && !is_valid_filename(&final_filename) {
        let err = CliError {
            code: 4,
            kind: CliErrorKind::InvalidFilename.kind_str(),
            message: format!("Invalid output filename: {final_filename}"),
            hint: Some("Avoid path separators and reserved characters".to_string()),
            retryable: false,
        };
        emit_structured_error(&err);
        return Err(err);
    }

    let mut output_path = unique_filename(&output_directory, &final_filename);

    // Estimate file size (rough: 200 bytes per message + overhead)
    let estimated_size = message_count * 200 + 15000;

    // --- Explain mode ---
    if explain {
        let plan = serde_json::json!({
            "plan": {
                "session_path": session_path.display().to_string(),
                "agent": agent_name,
                "messages": message_count,
                "output_path": output_path.display().to_string(),
                "estimated_size_bytes": estimated_size,
                "options": {
                    "include_tools": include_tools,
                    "show_timestamps": show_timestamps,
                    "default_theme": theme
                }
            },
            "warnings": [],
            "privacy": privacy.metadata()
        });
        println!(
            "{}",
            serde_json::to_string_pretty(&plan).unwrap_or_else(|_| "{}".to_string())
        );
        return Ok(());
    }

    // --- Dry run mode ---
    if dry_run {
        let result = serde_json::json!({
            "dry_run": true,
            "valid": true,
            "session_path": session_path.display().to_string(),
            "output_path": output_path.display().to_string(),
            "messages": message_count,
            "estimated_size_bytes": estimated_size,
            "privacy": privacy.metadata()
        });
        println!(
            "{}",
            serde_json::to_string_pretty(&result).unwrap_or_else(|_| "{}".to_string())
        );
        return Ok(());
    }

    // --- Build export options ---
    let export_options = HtmlExportOptions {
        title: session_title.clone(),
        syntax_highlighting: true,
        include_search: true,
        include_theme_toggle: true,
        default_theme: theme.to_string(),
        print_styles: true,
        agent_name: agent_name.clone(),
        show_timestamps,
        show_tool_calls: include_tools,
    };

    privacy.announce();

    // --- Export ---
    let exporter = HtmlExporter::with_options(export_options);
    let title = session_title.as_deref().unwrap_or("Conversation Export");

    // Group messages for consolidated rendering (tool calls with parent messages)
    let message_groups = group_messages_for_export(messages);

    let html = exporter
        .export_messages(title, &message_groups, metadata)
        .map_err(|e| {
            let err = CliError {
                code: 5,
                kind: CliErrorKind::ExportFailed.kind_str(),
                message: format!("Failed to export HTML: {e}"),
                hint: None,
                retryable: false,
            };
            emit_structured_error(&err);
            err
        })?;

    // --- Write file ---
    let output_parent = output_path.parent().unwrap_or(Path::new("."));
    std::fs::create_dir_all(output_parent).map_err(|e| {
        let err = CliError {
            code: 4,
            kind: CliErrorKind::OutputNotWritable.kind_str(),
            message: format!("Could not create output directory: {e}"),
            hint: Some(format!(
                "Check permissions for {}",
                output_directory.display()
            )),
            retryable: false,
        };
        emit_structured_error(&err);
        err
    })?;
    write_unique_export_output_file(
        &output_directory,
        &final_filename,
        &mut output_path,
        html.as_bytes(),
    )
    .map_err(|e| {
        let output_unavailable = matches!(
            e.kind(),
            io::ErrorKind::PermissionDenied
                | io::ErrorKind::NotFound
                | io::ErrorKind::AlreadyExists
        );
        let err = CliError {
            code: 4,
            kind: if output_unavailable {
                CliErrorKind::OutputNotWritable.kind_str()
            } else {
                CliErrorKind::WriteFailed.kind_str()
            },
            message: if output_unavailable {
                format!("Could not create output file: {e}")
            } else {
                format!("Failed to publish complete output file: {e}")
            },
            hint: Some(format!(
                "Check permissions and available space for {}",
                output_directory.display()
            )),
            retryable: false,
        };
        emit_structured_error(&err);
        err
    })?;

    let file_size = html.len();

    // --- Open in browser if requested ---
    if open {
        #[cfg(target_os = "macos")]
        {
            let _ = std::process::Command::new("open").arg(&output_path).spawn();
        }
        #[cfg(target_os = "linux")]
        {
            let _ = std::process::Command::new("xdg-open")
                .arg(&output_path)
                .spawn();
        }
        #[cfg(target_os = "windows")]
        {
            let _ = std::process::Command::new("explorer")
                .arg(&output_path)
                .spawn();
        }
    }

    // --- Output result ---
    let structured_format = output_format.or_else(robot_format_from_env).map(|fmt| {
        if matches!(fmt, RobotFormat::Sessions) {
            RobotFormat::Compact
        } else {
            fmt
        }
    });

    if let Some(_fmt) = structured_format {
        let result = serde_json::json!({
        "success": true,
        "privacy": privacy.metadata(),
        "exported": {
            "session_path": session_path.display().to_string(),
            "output_path": output_path.display().to_string(),
                "filename": output_path.file_name().map(|n| n.to_string_lossy().to_string()),
                "size_bytes": file_size,
                "messages_count": message_count,
                "agent": agent_name,
                "workspace": workspace,
                "title": session_title
            }
        });
        println!(
            "{}",
            serde_json::to_string_pretty(&result).unwrap_or_else(|_| "{}".to_string())
        );
    } else {
        println!("✓ Exported to {}", output_path.display());
        if human_turns > 0 {
            println!(
                "  {} prompts, {} responses, {} tool uses, {} bytes",
                human_turns, assistant_msgs, tool_use_count, file_size
            );
        } else {
            println!("  {} messages, {} bytes", message_count, file_size);
        }
    }

    Ok(())
}
