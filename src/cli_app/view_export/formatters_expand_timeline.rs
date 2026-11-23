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
use crate::timeline_json;
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

pub(crate) fn format_as_markdown(
    messages: &[serde_json::Value],
    title: &Option<String>,
    start_ts: Option<i64>,
    include_tools: bool,
) -> String {
    use chrono::{TimeZone, Utc};
    let mut md = String::new();
    md.push_str("# ");
    md.push_str(title.as_deref().unwrap_or("Conversation Export"));
    md.push('\n');

    if let Some(ts) = start_ts
        && let Some(dt) = Utc.timestamp_millis_opt(ts).single()
    {
        md.push_str(&format!(
            "\n*Started: {}*\n",
            dt.format("%Y-%m-%d %H:%M UTC")
        ));
    }
    md.push_str("\n---\n\n");

    for msg in messages {
        let role = extract_role(msg);
        // Skip non-message records (queue-operation, summary, etc.)
        if !matches!(
            role.as_str(),
            "user" | "assistant" | "system" | "tool" | "unknown"
        ) {
            continue;
        }
        match role.as_str() {
            "user" => md.push_str("## 👤 User\n\n"),
            "assistant" => md.push_str("## 🤖 Assistant\n\n"),
            _ => md.push_str(&format!("## {}\n\n", role)),
        }

        let content = extract_text_content(msg);
        if !content.is_empty() {
            md.push_str(&content);
            md.push_str("\n\n");
        }

        // Also handle tool blocks if include_tools is set
        if include_tools {
            let content_val = msg
                .get("message")
                .and_then(|m| m.get("content"))
                .or_else(|| msg.get("content"));
            if let Some(arr) = content_val.and_then(|c| c.as_array()) {
                for block in arr {
                    if let Some(block_type) = block.get("type").and_then(|t| t.as_str()) {
                        match block_type {
                            "tool_use" => {
                                let name =
                                    block.get("name").and_then(|n| n.as_str()).unwrap_or("tool");
                                md.push_str(&format!("**Tool: {}**\n", name));
                                if let Some(input) = block.get("input") {
                                    md.push_str("```json\n");
                                    md.push_str(
                                        &serde_json::to_string_pretty(input).unwrap_or_default(),
                                    );
                                    md.push_str("\n```\n\n");
                                }
                            }
                            "tool_result" => {
                                md.push_str("**Tool Result:**\n");
                                if let Some(c) = block.get("content").and_then(|c| c.as_str()) {
                                    let preview: String = c.chars().take(500).collect();
                                    md.push_str("```\n");
                                    md.push_str(&preview);
                                    if c.len() > 500 {
                                        md.push_str("\n... (truncated)");
                                    }
                                    md.push_str("\n```\n\n");
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
        md.push_str("---\n\n");
    }
    md
}

pub(crate) fn format_as_text(messages: &[serde_json::Value], include_tools: bool) -> String {
    let mut text = String::new();
    for msg in messages {
        let role = extract_role(msg);
        // Skip non-message records (queue-operation, summary, etc.)
        if !matches!(
            role.as_str(),
            "user" | "assistant" | "system" | "tool" | "unknown"
        ) {
            continue;
        }
        text.push_str(&format!("=== {} ===\n\n", role.to_uppercase()));

        let content = extract_text_content(msg);
        if !content.is_empty() {
            text.push_str(&content);
            text.push_str("\n\n");
        }

        // Also handle tool blocks if include_tools is set
        if include_tools {
            // Check nested message.content for tool blocks
            let content_val = msg
                .get("message")
                .and_then(|m| m.get("content"))
                .or_else(|| msg.get("content"));
            if let Some(arr) = content_val.and_then(|c| c.as_array()) {
                for block in arr {
                    if let Some(block_type) = block.get("type").and_then(|t| t.as_str())
                        && block_type == "tool_use"
                    {
                        let name = block.get("name").and_then(|n| n.as_str()).unwrap_or("tool");
                        text.push_str(&format!("[Tool: {}]\n", name));
                    } else if block.get("type").and_then(serde_json::Value::as_str)
                        == Some("tool_result")
                        && let Some(output) = block.get("content")
                    {
                        text.push_str("[Tool Result]\n");
                        text.push_str(&stringify_tool_value(output));
                        text.push_str("\n\n");
                    }
                }
            }
        }
    }
    text
}

pub(crate) fn format_as_html(
    messages: &[serde_json::Value],
    title: &Option<String>,
    start_ts: Option<i64>,
    include_tools: bool,
) -> String {
    use chrono::{TimeZone, Utc};
    let title_raw = title.as_deref().unwrap_or("Conversation Export");
    let title_str = html_escape(title_raw);
    let date_str = start_ts
        .and_then(|ts| Utc.timestamp_millis_opt(ts).single())
        .map(|dt| dt.format("%Y-%m-%d %H:%M UTC").to_string())
        .unwrap_or_default();

    let mut html = format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>{title_str}</title>
    <style>
        body {{ font-family: system-ui, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; background: #0d0d12; color: #e0e0e5; }}
        .message {{ background: #18181f; border-radius: 8px; padding: 16px; margin: 12px 0; box-shadow: 0 1px 3px rgba(0,0,0,0.3); }}
        .user {{ border-left: 4px solid #3b82f6; }}
        .assistant {{ border-left: 4px solid #22c55e; }}
        .role {{ font-weight: bold; color: #a0a0a8; margin-bottom: 8px; }}
        .content {{ white-space: pre-wrap; line-height: 1.6; }}
        h1 {{ color: #f0f0f5; }}
        .meta {{ color: #6b7280; font-size: 0.9em; }}
        /* Tool badges */
        .tool-badges {{ display: flex; flex-wrap: wrap; gap: 4px; margin-top: 8px; }}
        .tool-badge {{ position: relative; display: inline-flex; align-items: center; gap: 4px; padding: 2px 8px; font-size: 11px; font-family: 'JetBrains Mono', ui-monospace, monospace; background: #1e1e26; border: 1px solid #2e2e38; border-radius: 4px; cursor: pointer; transition: all 0.15s; }}
        .tool-badge:hover {{ border-color: #f59e0b; background: rgba(245,158,11,0.1); }}
        .tool-badge-name {{ font-weight: 600; color: #f59e0b; }}
        .tool-popover {{ position: absolute; bottom: calc(100% + 8px); left: 50%; transform: translateX(-50%) scale(0.95); min-width: 280px; max-width: 400px; padding: 12px; background: #18181f; border: 1px solid #2e2e38; border-radius: 8px; box-shadow: 0 8px 32px rgba(0,0,0,0.4); opacity: 0; visibility: hidden; transition: all 0.15s; z-index: 1000; pointer-events: none; text-align: left; white-space: normal; }}
        .tool-badge:hover .tool-popover {{ opacity: 1; visibility: visible; transform: translateX(-50%) scale(1); pointer-events: auto; }}
        .tool-popover::after {{ content: ''; position: absolute; top: 100%; left: 50%; transform: translateX(-50%); border: 6px solid transparent; border-top-color: #2e2e38; }}
        .tool-popover-header {{ display: flex; align-items: center; gap: 8px; padding-bottom: 8px; margin-bottom: 8px; border-bottom: 1px solid #2e2e38; font-weight: 600; color: #f59e0b; }}
        .tool-popover-section {{ margin-bottom: 8px; }}
        .tool-popover-label {{ font-size: 9px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.8px; color: #6b7280; margin-bottom: 4px; display: block; }}
        .tool-popover pre {{ margin: 0; padding: 8px; font-size: 10px; background: #0d0d12; border-radius: 4px; max-height: 150px; overflow: auto; white-space: pre-wrap; word-break: break-word; }}
    </style>
</head>
<body>
    <h1>{title_str}</h1>
    <p class="meta">{date_str}</p>
"#
    );

    for msg in messages {
        let role = extract_role(msg);
        // Skip non-message records (queue-operation, summary, etc.)
        if !matches!(
            role.as_str(),
            "user" | "assistant" | "system" | "tool" | "unknown"
        ) {
            continue;
        }
        let role_class = if role == "user" { "user" } else { "assistant" };
        let role_display = match role.as_str() {
            "user" => "👤 User",
            "assistant" => "🤖 Assistant",
            "system" => "⚙️ System",
            _ => "💬 Message",
        };

        html.push_str(&format!(
            r#"    <div class="message {role_class}">
        <div class="role">{role_display}</div>
        <div class="content">"#
        ));

        // Use extract_text_content for consistent content extraction
        let content = extract_text_content(msg);
        // Strip "[Tool: X]" markers when we're showing tool badges
        let content = if include_tools {
            strip_tool_marker(&content)
        } else {
            content
        };
        html.push_str(&html_escape(&content));

        // Also handle tool use blocks if requested
        if include_tools {
            // Check for tool_use in nested message.content array
            let content_val = msg
                .get("message")
                .and_then(|m| m.get("content"))
                .or_else(|| msg.get("content"));
            if let Some(arr) = content_val.and_then(|c| c.as_array()) {
                let mut tool_badges = Vec::new();
                for block in arr {
                    if block.get("type").and_then(serde_json::Value::as_str) == Some("tool_result")
                        && let Some(output) = block.get("content")
                    {
                        html.push_str(
                            "<div class=\"tool-result\"><strong>Tool Result:</strong><pre>",
                        );
                        html.push_str(&html_escape(&stringify_tool_value(output)));
                        html.push_str("</pre></div>");
                    }
                    if let Some("tool_use") = block.get("type").and_then(|t| t.as_str()) {
                        let name = block.get("name").and_then(|n| n.as_str()).unwrap_or("tool");
                        let icon = match name.to_lowercase().as_str() {
                            "bash" | "shell" => "💻",
                            "read" | "read_file" => "📖",
                            "write" | "write_file" => "📝",
                            "edit" => "✏️",
                            "glob" | "find" => "🔍",
                            "grep" | "search" => "🔎",
                            "webfetch" | "fetch" | "http" => "🌐",
                            "websearch" => "🔍",
                            "task" => "🚀",
                            _ => "🔧",
                        };
                        // Get input preview (truncated)
                        let input_preview = block
                            .get("input")
                            .map(|v| {
                                let s = serde_json::to_string_pretty(v).unwrap_or_default();
                                if s.len() > 400 {
                                    format!("{}…", &s[..s.floor_char_boundary(400)])
                                } else {
                                    s
                                }
                            })
                            .unwrap_or_default();

                        let popover_content = if !input_preview.is_empty() {
                            format!(
                                r#"<div class="tool-popover-section"><span class="tool-popover-label">Input</span><pre>{}</pre></div>"#,
                                html_escape(&input_preview)
                            )
                        } else {
                            String::new()
                        };

                        tool_badges.push(format!(
                            r#"<span class="tool-badge" tabindex="0">
                                <span>{icon}</span>
                                <span class="tool-badge-name">{name}</span>
                                <div class="tool-popover">
                                    <div class="tool-popover-header">{icon} {name}</div>
                                    {popover}
                                </div>
                            </span>"#,
                            icon = icon,
                            name = html_escape(name),
                            popover = popover_content,
                        ));
                    }
                }
                if !tool_badges.is_empty() {
                    html.push_str(r#"<div class="tool-badges">"#);
                    for badge in tool_badges {
                        html.push_str(&badge);
                    }
                    html.push_str("</div>");
                }
            }
        }

        html.push_str("</div>\n    </div>\n");
    }
    html.push_str("</body>\n</html>\n");
    html
}

pub(crate) fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

/// Show messages around a specific line in a session file
pub(crate) fn run_expand(
    path: &Path,
    db_override: Option<PathBuf>,
    source_id: Option<&str>,
    line: usize,
    context: usize,
    output_format: Option<RobotFormat>,
) -> CliResult<()> {
    if let Some(source_id) = source_id {
        validate_followup_source_id(source_id, "cass expand")?;
    }
    let normalized_source_id = canonical_followup_source_id(source_id);
    let source_id = normalized_source_id.as_deref();
    if line == 0 {
        return Err(CliError {
            code: 2,
            kind: CliErrorKind::InvalidLine.kind_str(),
            message: "Line numbers start at 1, not 0".to_string(),
            hint: Some("Use -n 1 for the first line".to_string()),
            retryable: false,
        });
    }

    let db_path = db_override.unwrap_or_else(default_db_path);
    let indexed_view =
        try_load_indexed_conversation_from_db_with_source(path, &db_path, source_id, None);
    let allow_direct_file = followup_source_is_local(source_id) || source_id.is_none();

    let prefer_direct_file = prefers_direct_jsonl_file(path, source_id);

    let raw_lines: Vec<String> = if prefer_direct_file {
        match read_followup_file_lines(path) {
            Ok(lines) => lines,
            Err(err) => {
                if let Some(view) = indexed_view.as_ref() {
                    serialize_indexed_view_lines(view)?
                } else {
                    return Err(err);
                }
            }
        }
    } else if let Some(view) = indexed_view.as_ref() {
        serialize_indexed_view_lines(view)?
    } else if allow_direct_file && path.exists() {
        return Err(CliError {
            code: 9,
            kind: CliErrorKind::IndexedSessionRequired.kind_str(),
            message: format!(
                "Local expand for '{}' requires an indexed conversation or a JSONL session",
                path.display()
            ),
            hint: Some(
                "Run 'cass index --full' first, or expand directly from a JSONL session."
                    .to_string(),
            ),
            retryable: false,
        });
    } else {
        return Err(CliError {
            code: 3,
            kind: CliErrorKind::FileNotFound.kind_str(),
            message: match source_id {
                Some(source_id) => format!(
                    "No indexed session found for source '{}' at {}",
                    source_id,
                    path.display()
                ),
                None => format!("Session file not found: {}", path.display()),
            },
            hint: Some(match source_id {
                Some(_) => "Use the exact source_id from search output or omit --source to prefer the local file/path.".to_string(),
                None => "Use 'cass search' to find session paths".to_string(),
            }),
            retryable: false,
        });
    };

    let mut messages: Vec<(usize, serde_json::Value)> = Vec::new();
    let mut target_msg_idx: Option<usize> = None;
    let mut current_line: usize = 0;

    for raw_line in raw_lines {
        current_line += 1;
        if raw_line.trim().is_empty() {
            continue;
        }
        if let Ok(msg) = serde_json::from_str::<serde_json::Value>(&raw_line) {
            if current_line == line {
                target_msg_idx = Some(messages.len());
            }
            messages.push((current_line, msg));
        }
    }

    // GH493: only an exact raw-line match can be marked as the target.

    let target_idx = target_msg_idx.ok_or_else(|| CliError {
        code: 2,
        kind: CliErrorKind::LineNotFound.kind_str(),
        message: format!("No message found at exact line {}", line),
        hint: Some(format!("File has {} messages", messages.len())),
        retryable: false,
    })?;

    let start = target_idx.saturating_sub(context);
    let end = target_idx
        .saturating_add(context)
        .saturating_add(1)
        .min(messages.len());

    let context_messages: Vec<_> = messages[start..end]
        .iter()
        .enumerate()
        .map(|(i, (line_num, msg))| {
            let is_target = start + i == target_idx;
            (line_num, msg, is_target)
        })
        .collect();

    let structured_format = output_format.or_else(robot_format_from_env).map(|fmt| {
        if matches!(fmt, RobotFormat::Sessions) {
            RobotFormat::Compact
        } else {
            fmt
        }
    });

    if let Some(fmt) = structured_format {
        let output: Vec<serde_json::Value> = context_messages
            .iter()
            .map(|(line_num, msg, is_target)| {
                let role = extract_role(msg);
                let content = extract_text_content(msg);
                serde_json::json!({
                    "line": line_num,
                    "role": role,
                    "is_target": is_target,
                    "content": content,
                })
            })
            .collect();
        return output_structured_value(serde_json::Value::Array(output), fmt);
    }

    println!("\n📍 Context around line {} in {}\n", line, path.display());
    println!("{}", "─".repeat(60));

    for (line_num, msg, is_target) in context_messages {
        let role = extract_role(msg);
        let content = extract_text_content(msg);
        let preview: String = content.chars().take(300).collect();
        let marker = if is_target { ">>>" } else { "   " };
        let role_icon = match role.as_str() {
            "user" => "👤",
            "assistant" => "🤖",
            _ => "📝",
        };

        println!(
            "{} L{:>4} {} {}",
            marker,
            line_num,
            role_icon,
            role.to_uppercase()
        );
        println!("        {}", preview.replace('\n', " "));
        if content.len() > 300 {
            println!("        ... ({} more chars)", content.len() - 300);
        }
        println!();
    }

    println!("{}", "─".repeat(60));
    println!(
        "Showing messages {} to {} of {} total",
        start + 1,
        end,
        messages.len()
    );
    Ok(())
}

pub(crate) fn extract_message_timestamp(msg: &serde_json::Value) -> Option<i64> {
    msg.get("timestamp")
        .and_then(crate::connectors::parse_timestamp)
        .or_else(|| {
            msg.get("message")
                .and_then(|m| m.get("timestamp"))
                .and_then(crate::connectors::parse_timestamp)
        })
        .or_else(|| {
            msg.get("payload")
                .and_then(|p| p.get("timestamp"))
                .and_then(crate::connectors::parse_timestamp)
        })
}

pub(crate) fn flatten_pi_family_text_content(content: &serde_json::Value) -> Option<String> {
    let blocks = content.as_array()?;
    if !blocks.iter().any(|block| {
        matches!(
            block.get("type").and_then(serde_json::Value::as_str),
            Some("thinking" | "toolCall")
        )
    }) {
        return None;
    }

    let mut parts = Vec::new();
    for block in blocks {
        let block_type = block.get("type").and_then(serde_json::Value::as_str);
        let part = if let Some(text) = block.as_str() {
            Some(text.to_string())
        } else {
            match block_type {
                None | Some("text" | "input_text" | "output_text") => block
                    .get("text")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string),
                Some("thinking") => block
                    .get("thinking")
                    .and_then(serde_json::Value::as_str)
                    .filter(|thinking| !thinking.is_empty())
                    .map(|thinking| format!("[Thinking] {thinking}")),
                // Tool data is rendered structurally only when the caller
                // opted into `--include-tools`.
                Some("toolCall") => None,
                _ => None,
            }
        };
        if let Some(part) = part.filter(|part| !part.is_empty()) {
            parts.push(part);
        }
    }

    (!parts.is_empty()).then(|| parts.join("\n"))
}

pub(crate) fn extract_text_content_without_tool_blocks(msg: &serde_json::Value) -> String {
    fn flatten_without_tool_blocks(content: &serde_json::Value) -> Option<String> {
        if let Some(s) = content.as_str() {
            return (!s.is_empty()).then(|| s.to_string());
        }

        if let Some(text) = flatten_pi_family_text_content(content) {
            return Some(text);
        }

        let arr = content.as_array()?;
        let mut result = String::new();
        for item in arr {
            let item_type = item.get("type").and_then(|v| v.as_str());
            if matches!(item_type, Some("tool_use" | "tool_result" | "toolCall")) {
                continue;
            }

            let part = if let Some(text) = item.as_str() {
                Some(text.to_string())
            } else if matches!(
                item_type,
                None | Some("text" | "input_text" | "output_text")
            ) {
                item.get("text")
                    .and_then(|v| v.as_str())
                    .map(str::to_string)
            } else {
                None
            };

            let Some(part) = part else {
                continue;
            };
            if part.is_empty() {
                continue;
            }
            if !result.is_empty() {
                result.push('\n');
            }
            result.push_str(&part);
        }

        (!result.is_empty()).then_some(result)
    }

    if let Some(content) = msg.get("content")
        && let Some(text) = flatten_without_tool_blocks(content)
    {
        return text;
    }
    if let Some(inner) = msg.get("message")
        && let Some(content) = inner.get("content")
        && let Some(text) = flatten_without_tool_blocks(content)
    {
        return text;
    }
    if let Some(payload) = msg.get("payload")
        && let Some(content) = payload.get("content")
        && let Some(text) = flatten_without_tool_blocks(content)
    {
        return text;
    }
    String::new()
}

pub(crate) fn extract_text_content(msg: &serde_json::Value) -> String {
    // Use the well-tested flatten_content helper from connectors module
    // It handles: direct strings, {"type": "text"}, {"type": "input_text"},
    // blocks with "text" but no "type", and tool_use blocks
    fn try_flatten(content: &serde_json::Value) -> Option<String> {
        if let Some(result) = flatten_pi_family_text_content(content) {
            return Some(result);
        }
        let result = crate::connectors::flatten_content(content);
        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }

    // Try direct content first (standard format)
    if let Some(content) = msg.get("content")
        && let Some(text) = try_flatten(content)
    {
        return text;
    }
    // Try nested message.content (Claude Code format)
    if let Some(inner) = msg.get("message")
        && let Some(content) = inner.get("content")
        && let Some(text) = try_flatten(content)
    {
        return text;
    }
    // Try nested payload.content (Codex format: {"type": "response_item", "payload": {"content": ...}})
    if let Some(payload) = msg.get("payload")
        && let Some(content) = payload.get("content")
        && let Some(text) = try_flatten(content)
    {
        return text;
    }
    String::new()
}

pub(crate) fn normalized_export_role(role: &str) -> &str {
    match role {
        "toolResult" | "tool_result" => "tool",
        other => other,
    }
}

/// Extract role from message (supports various formats)
pub(crate) fn extract_role(msg: &serde_json::Value) -> String {
    // Try direct role
    if let Some(role) = msg.get("role").and_then(|r| r.as_str()) {
        return normalized_export_role(role).to_string();
    }
    // Try nested message.role (Claude Code format)
    if let Some(inner) = msg.get("message")
        && let Some(role) = inner.get("role").and_then(|r| r.as_str())
    {
        return normalized_export_role(role).to_string();
    }
    // Try nested payload.role (Codex format: {"type": "response_item", "payload": {"role": "user", ...}})
    if let Some(payload) = msg.get("payload")
        && let Some(role) = payload.get("role").and_then(|r| r.as_str())
    {
        return normalized_export_role(role).to_string();
    }
    // Try type field (Claude Code also uses "type": "user" or "type": "assistant")
    if let Some(type_val) = msg.get("type").and_then(|t| t.as_str()) {
        match type_val {
            "user" => return "user".to_string(),
            "assistant" => return "assistant".to_string(),
            // Return the actual type for non-message records (e.g. "queue-operation",
            // "system", etc.) so callers can filter them rather than rendering as blank
            other => return other.to_string(),
        }
    }
    "unknown".to_string()
}

/// Remove lossy tool-call lines from normalized Pi-family assistant text.
///
/// This is only used when the canonical source envelope is unavailable and
/// the caller explicitly disabled tools. FAD emits each flattened invocation
/// on its own line as `[Tool: name] arguments`; without the envelope there is
/// no structured representation left to filter.
pub(crate) fn strip_flattened_tool_call_lines(content: &str) -> String {
    content
        .lines()
        .filter(|line| {
            let trimmed = line.trim_start();
            !(trimmed.starts_with("[Tool:") && trimmed.contains(']'))
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Strip redundant "[Tool: X]" markers from content when tool call is shown separately.
pub(crate) fn strip_tool_marker(content: &str) -> String {
    let trimmed = content.trim();

    // Check if content starts with "[Tool: X]" pattern
    if trimmed.starts_with("[Tool:")
        && let Some(close_idx) = trimmed.find(']')
    {
        // Get content after the tool marker
        let after = trimmed[close_idx + 1..].trim();
        if after.is_empty() {
            // Entire content was just "[Tool: X]" - return empty
            return String::new();
        }
        // Return the content after the marker
        return after.to_string();
    }

    content.to_string()
}

/// Truncate a string smartly at word boundaries with ellipsis.
///
/// Returns the original string if it fits within max_len.
/// Otherwise, truncates at the last word boundary before max_len and adds "…".
pub(crate) fn smart_truncate(s: &str, max_len: usize) -> String {
    let s = s.trim();
    if s.chars().count() <= max_len {
        return s.to_string();
    }

    // Find the last space before the limit
    let char_indices: Vec<_> = s.char_indices().take(max_len).collect();
    if char_indices.is_empty() {
        return "…".to_string();
    }

    // Look for last word boundary (space)
    let end_byte = char_indices
        .last()
        .map(|(i, c)| i + c.len_utf8())
        .unwrap_or(0);
    let truncated = &s[..end_byte];

    // Find last space to break at word boundary
    if let Some(last_space) = truncated.rfind(|c: char| c.is_whitespace())
        && last_space > max_len / 2
    {
        // Only break at word if we're not losing too much
        return format!("{}…", truncated[..last_space].trim_end());
    }

    // No good word boundary, just truncate
    format!("{}…", truncated.trim_end())
}

/// Show activity timeline for a time range
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_timeline(
    since: Option<&str>,
    until: Option<&str>,
    today: bool,
    agents: &[String],
    data_dir: &Option<PathBuf>,
    db_override: Option<PathBuf>,
    output_format: Option<RobotFormat>,
    group_by: TimelineGrouping,
    source: Option<String>,
) -> CliResult<()> {
    use crate::franken_sync::compat::{ConnectionExt, ParamValue, RowExt, TransactionExt};
    use crate::sources::provenance::SourceFilter;
    use chrono::{Local, TimeZone, Utc};
    use std::collections::HashMap;

    let agents = normalize_agent_filters(agents)?;

    // Parse source filter (P3.2)
    let source_filter = source.as_ref().map(|s| SourceFilter::parse(s));

    let now = Local::now();
    let (start_ts, end_ts) = if today {
        let start_of_day = now
            .date_naive()
            .and_hms_opt(0, 0, 0)
            .expect("valid start of day");
        let local_start = match Local.from_local_datetime(&start_of_day) {
            chrono::LocalResult::Single(dt) => dt,
            chrono::LocalResult::Ambiguous(dt, _) => dt,
            chrono::LocalResult::None => Local.from_utc_datetime(&start_of_day),
        };
        (local_start.timestamp_millis(), now.timestamp_millis())
    } else {
        // The same grammar as search's --since/--until. An unparseable value
        // is a usage error: it used to become the 7-day default silently, so
        // `--since 5m` ran a week's timeline with exit 0.
        let start = match since {
            Some(raw) => parse_search_time_bound("--since", raw, TimeBound::Since)?,
            None => (now - chrono::Duration::days(7)).timestamp_millis(),
        };
        let end = match until {
            Some(raw) => parse_search_time_bound("--until", raw, TimeBound::Until)?,
            None => now.timestamp_millis(),
        };
        (start, end)
    };

    // Fail loudly on an inverted range rather than silently returning 0 rows.
    // A bad relative offset (e.g. a future `--since`) used to compute
    // start > end and match nothing without explanation (#280).
    if start_ts > end_ts {
        return Err(CliError::usage(
            "--since resolves to a time after --until (empty range)",
            Some(format!(
                "Computed window start ({start_ts}ms) is after end ({end_ts}ms). \
                 Check the --since/--until values; relative offsets like `-7d` mean \"7 days ago\"."
            )),
        ));
    }

    let db_path = analytics_db_path(data_dir, db_override.as_ref());
    let conn = open_franken_analytics_db(data_dir, db_override.as_ref())?;

    // LEFT JOIN + COALESCE keeps legacy conversations with NULL agent_id.
    // Filter on the same projected identity so --agent unknown includes them.
    let mut sql = String::from(
        "SELECT c.id, COALESCE(a.slug, 'unknown') as agent, c.title, c.started_at, c.ended_at, c.source_path,
                0 as message_count, COALESCE(c.source_id, 'local') as source_id, c.origin_host, s.kind as origin_kind
         FROM conversations c
         LEFT JOIN agents a ON c.agent_id = a.id
         LEFT JOIN sources s ON c.source_id = s.id
         WHERE c.started_at >= ?1 AND c.started_at <= ?2",
    );

    let mut params: Vec<ParamValue> = vec![start_ts.into(), end_ts.into()];

    if !agents.is_empty() {
        sql.push_str(" AND COALESCE(a.slug, 'unknown') IN (");
        for (i, agent) in agents.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&format!("?{}", params.len() + 1));
            params.push(agent.clone().into());
        }
        sql.push(')');
    }

    // Source filter (P3.2)
    if let Some(ref filter) = source_filter {
        append_source_filter_condition(&mut sql, &mut params, filter);
    }

    // GH#521: joining messages into GROUP BY sends this query through the
    // engine's materializing fallback, including message bodies outside the
    // requested window. Select only conversation metadata first; none of the
    // time/agent/source predicates may depend on the messages table.
    sql.push_str(" ORDER BY c.started_at DESC");

    // Keep metadata and the later indexed counts on the same read snapshot,
    // just as the former single SELECT did while indexing runs concurrently.
    let mut snapshot = conn.transaction().map_err(|e| CliError {
        code: 9,
        kind: CliErrorKind::DbQuery.kind_str(),
        message: format!("Failed to start timeline snapshot: {e}"),
        hint: None,
        retryable: false,
    })?;
    let rows = conn
        .query_map_collect(&sql, &params, |row: &crate::franken_sync::Row| {
            Ok((
                row.get_typed::<i64>(0)?,            // id
                row.get_typed::<String>(1)?,         // agent
                row.get_typed::<Option<String>>(2)?, // title
                row.get_typed::<i64>(3)?,            // started_at
                row.get_typed::<Option<i64>>(4)?,    // ended_at
                row.get_typed::<String>(5)?,         // source_path
                row.get_typed::<i64>(6)?,            // message_count
                row.get_typed::<String>(7)?,         // source_id (P3.2)
                row.get_typed::<Option<String>>(8)?, // origin_host (P3.5)
                row.get_typed::<Option<String>>(9)?, // origin_kind (P3.5)
            ))
        })
        .map_err(|e| CliError {
            code: 9,
            kind: CliErrorKind::DbQuery.kind_str(),
            message: format!("Query failed: {e}"),
            hint: None,
            retryable: false,
        })?;
    #[allow(clippy::type_complexity)]
    let mut sessions: Vec<(
        i64,
        String,
        Option<String>,
        i64,
        Option<i64>,
        String,
        i64,
        String,
        Option<String>,
        Option<String>,
    )> = rows;
    for session in &mut sessions {
        // COUNT(*) with a WHERE clause also scans the whole messages table in
        // this engine version (see run_sessions). Stream the covering index's
        // idx column instead: no message bodies or per-session Vec of rows,
        // even for a single conversation with hundreds of thousands of turns.
        conn.query_with_params_for_each(
            "SELECT idx FROM messages INDEXED BY sqlite_autoindex_messages_1
             WHERE conversation_id = ?1",
            &[crate::franken_sync::SqliteValue::Integer(session.0)],
            |_| {
                session.6 += 1;
                Ok(())
            },
        )
        .map_err(|e| CliError {
            code: 9,
            kind: CliErrorKind::DbQuery.kind_str(),
            message: format!("Failed to count timeline messages: {e}"),
            hint: None,
            retryable: false,
        })?;
    }
    snapshot.commit().map_err(|e| CliError {
        code: 9,
        kind: CliErrorKind::DbQuery.kind_str(),
        message: format!("Failed to finish timeline snapshot: {e}"),
        hint: None,
        retryable: false,
    })?;
    drop(snapshot);
    close_franken_cli_read_db(conn, &db_path, "timeline")?;

    let structured_format = output_format.or_else(robot_format_from_env).map(|fmt| {
        if matches!(fmt, RobotFormat::Sessions) {
            RobotFormat::Compact
        } else {
            fmt
        }
    });

    // GH#521: serialize borrowed summaries directly, rather than cloning all
    // selected title/path strings into a second JSON Value tree. Keep the
    // other robot formats on their existing projection/formatting path.
    if let Some(fmt) = structured_format
        && matches!(fmt, RobotFormat::Json | RobotFormat::Compact)
    {
        let stdout = std::io::stdout();
        let mut writer = std::io::BufWriter::new(stdout.lock());
        timeline_json::write(
            &mut writer,
            &sessions,
            start_ts,
            end_ts,
            group_by,
            matches!(fmt, RobotFormat::Json),
        )
        .and_then(|()| writer.flush())
        .map_err(|error| CliError {
            code: 9,
            kind: CliErrorKind::EncodeJson.kind_str(),
            message: format!("failed to write structured output: {error}"),
            hint: None,
            retryable: false,
        })?;
        return Ok(());
    }

    if let Some(fmt) = structured_format {
        let output = match group_by {
            TimelineGrouping::None => {
                let items: Vec<serde_json::Value> = sessions
                    .iter()
                    .map(
                        |(
                            id,
                            agent,
                            title,
                            started,
                            ended,
                            path,
                            msg_count,
                            source_id,
                            origin_host,
                            origin_kind,
                        )| {
                            let duration_ms = ended.map(|e| e - started);
                            let duration_secs = duration_ms.map(|ms| ms / 1000);
                            let normalized_source_id = normalized_provenance_source_id(
                                source_id,
                                origin_kind.as_deref(),
                                origin_host.as_deref(),
                            );
                            let normalized_origin_kind = normalized_provenance_origin_kind(
                                normalized_source_id.as_str(),
                                origin_kind.as_deref(),
                            );
                            let normalized_origin_host =
                                normalized_provenance_origin_host(origin_host.as_deref());
                            serde_json::json!({
                                "id": id, "agent": agent, "title": title,
                                "started_at": started, "ended_at": ended,
                                "duration_seconds": duration_secs, "source_path": path,
                                "message_count": msg_count,
                                // Provenance fields (P3.5)
                                "source_id": normalized_source_id,
                                "origin_kind": normalized_origin_kind,
                                "origin_host": normalized_origin_host,
                            })
                        },
                    )
                    .collect();
                serde_json::json!({
                    "range": { "start": start_ts, "end": end_ts },
                    "total_sessions": sessions.len(),
                    "sessions": items,
                })
            }
            TimelineGrouping::Hour | TimelineGrouping::Day => {
                let mut groups: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
                for (
                    id,
                    agent,
                    title,
                    started,
                    ended,
                    path,
                    msg_count,
                    source_id,
                    origin_host,
                    origin_kind,
                ) in &sessions
                {
                    let dt = Utc
                        .timestamp_millis_opt(*started)
                        .single()
                        .unwrap_or_else(Utc::now);
                    let key = match group_by {
                        TimelineGrouping::Hour => dt.format("%Y-%m-%d %H:00").to_string(),
                        TimelineGrouping::Day => dt.format("%Y-%m-%d").to_string(),
                        _ => unreachable!(),
                    };
                    let normalized_source_id = normalized_provenance_source_id(
                        source_id,
                        origin_kind.as_deref(),
                        origin_host.as_deref(),
                    );
                    let normalized_origin_kind = normalized_provenance_origin_kind(
                        normalized_source_id.as_str(),
                        origin_kind.as_deref(),
                    );
                    let normalized_origin_host =
                        normalized_provenance_origin_host(origin_host.as_deref());
                    groups.entry(key).or_default().push(serde_json::json!({
                        "id": id, "agent": agent, "title": title,
                        "started_at": started, "ended_at": ended,
                        "source_path": path, "message_count": msg_count,
                        // Provenance fields (P3.5)
                        "source_id": normalized_source_id,
                        "origin_kind": normalized_origin_kind,
                        "origin_host": normalized_origin_host,
                    }));
                }
                serde_json::json!({
                    "range": { "start": start_ts, "end": end_ts },
                    "total_sessions": sessions.len(),
                    "groups": groups,
                })
            }
        };
        return output_structured_value(output, fmt);
    }

    let start_dt = Utc
        .timestamp_millis_opt(start_ts)
        .single()
        .unwrap_or_else(Utc::now);
    let end_dt = Utc
        .timestamp_millis_opt(end_ts)
        .single()
        .unwrap_or_else(Utc::now);

    println!("\n📅 Activity Timeline");
    println!(
        "   {} to {}",
        start_dt.format("%Y-%m-%d %H:%M"),
        end_dt.format("%Y-%m-%d %H:%M")
    );
    println!("{}", "─".repeat(70));

    if sessions.is_empty() {
        println!("\n   No sessions found in this time range.\n");
        return Ok(());
    }

    let mut current_group = String::new();
    for (
        _id,
        agent,
        title,
        started,
        ended,
        _path,
        msg_count,
        source_id,
        origin_host,
        origin_kind,
    ) in &sessions
    {
        let dt = Utc
            .timestamp_millis_opt(*started)
            .single()
            .unwrap_or_else(Utc::now);

        let group_key = match group_by {
            TimelineGrouping::Hour => dt.format("%Y-%m-%d %H:00").to_string(),
            TimelineGrouping::Day => dt.format("%Y-%m-%d (%A)").to_string(),
            TimelineGrouping::None => String::new(),
        };

        if group_key != current_group && group_by != TimelineGrouping::None {
            println!("\n  📆 {}", group_key);
            current_group = group_key;
        }

        let duration = ended.map(|e| {
            // Timestamps are in milliseconds, divide by 60_000 to get minutes.
            // Guard against negative durations from clock skew or corrupt data.
            let mins = (e - started).max(0) / 60_000;
            if mins < 60 {
                format!("{}m", mins)
            } else {
                format!("{}h{}m", mins / 60, mins % 60)
            }
        });

        let title_str = title.as_deref().unwrap_or("(untitled)");
        let title_preview: String = title_str.chars().take(40).collect();

        let agent_icon = match agent.as_str() {
            "claude_code" => "🟣",
            "codex" => "🟢",
            "gemini" => "🔵",
            "amp" => "🟡",
            "cursor" => "⚪",
            "pi_agent" => "🟠",
            "omp" => "🟤",
            _ => "⚫",
        };

        // Source badge for remote sessions (P3.2, P3.5)
        // Prefer origin_host if available, otherwise use normalized source_id.
        let normalized_source_id = normalized_provenance_source_id(
            source_id,
            origin_kind.as_deref(),
            origin_host.as_deref(),
        );
        let normalized_origin_kind = normalized_provenance_origin_kind(
            normalized_source_id.as_str(),
            origin_kind.as_deref(),
        );
        let source_badge = if normalized_origin_kind != crate::sources::provenance::LOCAL_SOURCE_ID
        {
            let label = origin_host
                .as_deref()
                .map(str::trim)
                .filter(|label| !label.is_empty())
                .unwrap_or(normalized_source_id.as_str());
            if label.is_empty() {
                String::new()
            } else {
                format!(" [{}]", label)
            }
        } else {
            String::new()
        };

        println!(
            "     {} {} {:>5} │ {:>3} msgs │ {}{}",
            dt.format("%H:%M"),
            agent_icon,
            duration.as_deref().unwrap_or(""),
            msg_count,
            title_preview,
            source_badge
        );
    }

    println!("\n{}", "─".repeat(70));
    println!("   Total: {} sessions\n", sessions.len());
    Ok(())
}
