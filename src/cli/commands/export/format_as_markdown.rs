use crate::cli::commands::expand::{extract_role, extract_text_content, strip_tool_marker};
use crate::cli::commands::export_html::stringify_tool_value;

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
