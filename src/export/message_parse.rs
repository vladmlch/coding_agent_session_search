//! Pure message parsing, text extraction, and export formatting helpers.

use chrono::{TimeZone, Utc};
use serde_json::Value;
use std::fmt::Write as _;

use crate::html_export;

pub fn format_as_markdown(
    messages: &[Value],
    title: &Option<String>,
    start_ts: Option<i64>,
    include_tools: bool,
) -> String {
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
            _ => {
                let _ = write!(md, "## {role}\n\n");
            }
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
                                let _ = writeln!(md, "**Tool: {name}**");
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

pub fn format_as_text(messages: &[Value], include_tools: bool) -> String {
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
        let _ = write!(text, "=== {} ===\n\n", role.to_uppercase());

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
                        let _ = writeln!(text, "[Tool: {name}]");
                    } else if block.get("type").and_then(Value::as_str) == Some("tool_result")
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

pub fn format_as_html(
    messages: &[Value],
    title: &Option<String>,
    start_ts: Option<i64>,
    include_tools: bool,
) -> String {
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

        let _ = write!(
            html,
            r#"    <div class="message {role_class}">
        <div class="role">{role_display}</div>
        <div class="content">"#
        );

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
                    if block.get("type").and_then(Value::as_str) == Some("tool_result")
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

pub fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

pub fn flatten_pi_family_text_content(content: &Value) -> Option<String> {
    let blocks = content.as_array()?;
    if !blocks.iter().any(|block| {
        matches!(
            block.get("type").and_then(Value::as_str),
            Some("thinking" | "toolCall")
        )
    }) {
        return None;
    }

    let mut parts = Vec::new();
    for block in blocks {
        let block_type = block.get("type").and_then(Value::as_str);
        let part = if let Some(text) = block.as_str() {
            Some(text.to_string())
        } else {
            match block_type {
                None | Some("text" | "input_text" | "output_text") => block
                    .get("text")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                Some("thinking") => block
                    .get("thinking")
                    .and_then(Value::as_str)
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

pub fn extract_text_content_without_tool_blocks(msg: &Value) -> String {
    fn flatten_without_tool_blocks(content: &Value) -> Option<String> {
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

pub fn extract_text_content(msg: &Value) -> String {
    // Use the well-tested flatten_content helper from connectors module
    // It handles: direct strings, {"type": "text"}, {"type": "input_text"},
    // blocks with "text" but no "type", and tool_use blocks
    fn try_flatten(content: &Value) -> Option<String> {
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

pub fn normalized_export_role(role: &str) -> &str {
    match role {
        "toolResult" | "tool_result" => "tool",
        other => other,
    }
}

/// Extract role from message (supports various formats)
pub fn extract_role(msg: &Value) -> String {
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
pub fn strip_flattened_tool_call_lines(content: &str) -> String {
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
pub fn strip_tool_marker(content: &str) -> String {
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
pub fn smart_truncate(s: &str, max_len: usize) -> String {
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

pub fn stringify_tool_value(value: &Value) -> String {
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
pub fn extract_tool_calls(msg: &Value) -> Vec<html_export::ToolCall> {
    let inner = msg.get("message").unwrap_or(msg);
    if inner.get("role").and_then(Value::as_str) == Some("toolResult") {
        let output = extract_text_content(msg);
        let output = if output.is_empty() {
            inner.get("content").map(stringify_tool_value)
        } else {
            Some(output)
        };
        let status = if inner
            .get("isError")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            html_export::ToolStatus::Error
        } else {
            html_export::ToolStatus::Success
        };
        return vec![html_export::ToolCall {
            name: inner
                .get("toolName")
                .and_then(Value::as_str)
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

pub fn extract_tool_correlation_id(value: &Value) -> Option<String> {
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
