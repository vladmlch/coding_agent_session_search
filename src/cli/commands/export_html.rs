use crate::cli::args_sub::RobotFormat;
use crate::cli::commands::expand::{
    extract_message_timestamp, extract_role, extract_text_content,
    extract_text_content_without_tool_blocks, smart_truncate, strip_flattened_tool_call_lines,
    strip_tool_marker,
};
use crate::cli::commands::export::indexed_export_cli_error::{
    export_projection_cli_error, format_export_duration, indexed_export_cli_error,
    strip_stdin_line_ending, write_unique_export_output_file,
};
use crate::cli::commands::search::fieldbudgets::robot_format_from_env;
use crate::cli::commands::view::role_to_export_string::{
    canonical_followup_source_id, infer_followup_agent_and_workspace, validate_followup_source_id,
};
use crate::cli::error::{CliError, CliResult};
use crate::cli::paths::default_db_path;
use crate::export::messages::group_messages_for_export;
use crate::html_export;
use crate::model::cli_error_kind::ErrorKind as CliErrorKind;
use std::path::{Path, PathBuf};

/// Export a session as a beautiful, self-contained HTML file with optional encryption.
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_export_html(
    session_path: &Path,
    db_override: Option<PathBuf>,
    source_id: Option<&str>,
    conversation_id: Option<i64>,
    output_dir: Option<&Path>,
    filename: Option<&str>,
    encrypt: bool,
    password_stdin: bool,
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

    // --- Get password if encryption requested ---
    let final_password: Option<zeroize::Zeroizing<String>> = if encrypt {
        if password_stdin {
            let mut pwd = String::new();
            io::stdin().read_line(&mut pwd).map_err(|e| CliError {
                code: 6,
                kind: CliErrorKind::PasswordReadError.kind_str(),
                message: format!("Failed to read password from stdin: {e}"),
                hint: None,
                retryable: false,
            })?;
            let pwd = strip_stdin_line_ending(pwd);
            if pwd.is_empty() {
                let err = CliError {
                    code: 6,
                    kind: CliErrorKind::PasswordRequired.kind_str(),
                    message: "Password read from stdin was empty".to_string(),
                    hint: Some("Provide a non-empty password via --password-stdin".to_string()),
                    retryable: false,
                };
                emit_structured_error(&err);
                return Err(err);
            }
            Some(zeroize::Zeroizing::new(pwd))
        } else {
            let err = CliError {
                code: 6,
                kind: CliErrorKind::PasswordRequired.kind_str(),
                message: "Password required for encryption".to_string(),
                hint: Some("Use --password-stdin".to_string()),
                retryable: false,
            };
            let structured_format = output_format.or_else(robot_format_from_env).map(|fmt| {
                if matches!(fmt, RobotFormat::Sessions) {
                    RobotFormat::Compact
                } else {
                    fmt
                }
            });

            if let Some(_fmt) = structured_format {
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
            return Err(err);
        }
    } else {
        None
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
    } else if encrypt {
        // An encrypted export is meant to be shared; its default name must not
        // carry the workspace or the opening prompt that the ciphertext hides
        // (2l1b0.67). An explicit --filename is honored as given.
        generate_full_filename(
            agent_name.as_deref().unwrap_or("cass"),
            None,
            session_start,
            None,
            None,
        )
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
                    "encrypted": encrypt,
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
            "encrypted": encrypt,
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
        encrypt,
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
        .export_messages(
            title,
            &message_groups,
            metadata,
            final_password.as_ref().map(|password| password.as_str()),
        )
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
                "encrypted": encrypt,
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
        if encrypt {
            println!("  🔒 Encrypted with Web Crypto (AES-256-GCM)");
        }
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

pub(crate) fn stringify_tool_value(value: &serde_json::Value) -> String {
    if value.is_object() || value.is_array() {
        serde_json::to_string_pretty(value).unwrap_or_default()
    } else if let Some(s) = value.as_str() {
        s.to_string()
    } else {
        value.to_string()
    }
}

/// Extract all tool call/result blocks from a message for HTML export.
///
/// Supports multiple formats:
/// 1. Claude/Anthropic format: `content` array with `type: "tool_use"` or `type: "tool_result"` blocks
/// 2. Cursor/generic format: `type: "tool"` at top level with `message.tool_name`, `message.tool_input`, `message.tool_output`
/// 3. Pi-family format: camelCase `toolCall` blocks and `toolResult` messages
pub(crate) fn extract_tool_calls(msg: &serde_json::Value) -> Vec<html_export::ToolCall> {
    let inner = msg.get("message").unwrap_or(msg);
    if inner.get("role").and_then(serde_json::Value::as_str) == Some("toolResult") {
        let output = extract_text_content(msg);
        let output = if output.is_empty() {
            inner.get("content").map(stringify_tool_value)
        } else {
            Some(output)
        };
        let status = if inner
            .get("isError")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
        {
            html_export::ToolStatus::Error
        } else {
            html_export::ToolStatus::Success
        };
        return vec![html_export::ToolCall {
            name: inner
                .get("toolName")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("tool_result")
                .to_string(),
            input: String::new(),
            output,
            status: Some(status),
            correlation_id: extract_tool_correlation_id(inner)
                .or_else(|| extract_tool_correlation_id(msg)),
        }];
    }

    // Format 2: Cursor/generic format - check for top-level type: "tool"
    if let Some(msg_type) = msg.get("type").and_then(|t| t.as_str())
        && msg_type == "tool"
    {
        // Look for tool info in the message object
        let inner = msg.get("message").unwrap_or(msg);

        let tool_name = inner
            .get("tool_name")
            .and_then(|n| n.as_str())
            .unwrap_or("tool");

        let tool_input = inner.get("tool_input").map(stringify_tool_value);

        let tool_output = inner.get("tool_output").map(stringify_tool_value);

        // Determine status from explicit status field or presence of output
        let status_str = inner
            .get("status")
            .and_then(|s| s.as_str())
            .or_else(|| msg.get("status").and_then(|s| s.as_str()));

        let status = match status_str {
            Some("success") => Some(html_export::ToolStatus::Success),
            Some("error") => Some(html_export::ToolStatus::Error),
            Some("pending") => Some(html_export::ToolStatus::Pending),
            _ if tool_output.is_some() => Some(html_export::ToolStatus::Success),
            // For exported conversations, don't show "pending" - just hide the status badge
            _ => None,
        };

        return vec![html_export::ToolCall {
            name: tool_name.to_string(),
            input: tool_input.unwrap_or_default(),
            output: tool_output,
            status,
            correlation_id: extract_tool_correlation_id(inner)
                .or_else(|| extract_tool_correlation_id(msg)),
        }];
    }

    // Format 1: Claude/Anthropic format - content array with tool_use/tool_result blocks
    let content = msg
        .get("message")
        .and_then(|m| m.get("content"))
        .or_else(|| msg.get("content"));

    let mut tool_calls = Vec::new();
    if let Some(arr) = content.and_then(|c| c.as_array()) {
        for block in arr {
            if let Some(block_type) = block.get("type").and_then(|t| t.as_str()) {
                match block_type {
                    "tool_use" | "toolCall" => {
                        let name = block.get("name").and_then(|n| n.as_str()).unwrap_or("tool");
                        let input = block
                            .get("input")
                            .or_else(|| block.get("arguments"))
                            .map(stringify_tool_value);
                        tool_calls.push(html_export::ToolCall {
                            name: name.to_string(),
                            input: input.unwrap_or_default(),
                            output: None,
                            // For exported conversations, don't show "pending" for tool invocations
                            // without explicit status - the output may be in a separate message
                            status: None,
                            correlation_id: extract_tool_correlation_id(block),
                        });
                    }
                    "tool_result" => {
                        let content = block.get("content").map(stringify_tool_value);
                        tool_calls.push(html_export::ToolCall {
                            name: "tool_result".to_string(),
                            input: String::new(),
                            output: content,
                            status: Some(html_export::ToolStatus::Success),
                            correlation_id: extract_tool_correlation_id(block),
                        });
                    }
                    _ => {}
                }
            }
        }
    }

    tool_calls
}

pub(crate) fn extract_tool_correlation_id(value: &serde_json::Value) -> Option<String> {
    value
        .get("tool_use_id")
        .or_else(|| value.get("tool_call_id"))
        .or_else(|| value.get("call_id"))
        .or_else(|| value.get("toolCallId"))
        .or_else(|| value.get("callId"))
        .or_else(|| value.get("id"))
        .and_then(|id| id.as_str())
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .map(str::to_string)
}
