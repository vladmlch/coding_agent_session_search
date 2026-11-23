use std::time::Instant;

use tracing::{debug, info, trace};

use crate::html_export::template::html_escape;

use super::agent_identity::{
    ICON_BOT, ICON_DATABASE, ICON_FILE_TEXT, ICON_GLOBE, ICON_MAIL, ICON_PENCIL, ICON_SEARCH,
    ICON_SETTINGS, ICON_SPARKLES, ICON_TERMINAL, ICON_USER, ICON_WRENCH, agent_css_class,
};
use super::render_message::{
    byte_index_for_char_count, format_json_or_raw, format_timestamp, render_content,
};
use super::types::{
    MessageGroup, MessageGroupType, RenderError, RenderOptions, ToolCallWithResult,
};

// ============================================================================
// MessageGroup Rendering (Consolidated Tool Calls)
// ============================================================================

/// Maximum number of tool badges to show before overflow indicator.
pub(super) const MAX_VISIBLE_BADGES: usize = 6;

/// Render a list of message groups to HTML (consolidated rendering).
///
/// This is the preferred rendering method when messages have been grouped
/// via `group_messages_for_export()`. Each group renders as a single article
/// with all associated tool calls shown as compact badges.
pub fn render_message_groups(
    groups: &[MessageGroup],
    options: &RenderOptions,
) -> Result<String, RenderError> {
    let started = Instant::now();
    let mut html = String::with_capacity(groups.len() * 3000);

    // Add agent-specific class to conversation wrapper if specified
    let agent_class = options
        .agent_slug
        .as_ref()
        .map(|s| agent_css_class(s))
        .unwrap_or("");

    info!(
        component = "renderer",
        operation = "render_message_groups",
        group_count = groups.len(),
        agent_slug = options.agent_slug.as_deref().unwrap_or(""),
        "Rendering conversation from message groups"
    );

    if !agent_class.is_empty() {
        html.push_str(&format!(
            r#"<div class="conversation-messages {}">"#,
            agent_class
        ));
        html.push('\n');
    }

    for (idx, group) in groups.iter().enumerate() {
        html.push_str(&render_message_group(group, idx, options)?);
        html.push('\n');
    }

    if !agent_class.is_empty() {
        html.push_str("</div>\n");
    }

    debug!(
        component = "renderer",
        operation = "render_message_groups_complete",
        duration_ms = started.elapsed().as_millis(),
        bytes = html.len(),
        groups = groups.len(),
        "Message groups rendered"
    );

    Ok(html)
}

/// Render a single message group to HTML.
///
/// A message group consists of:
/// - A primary message (user/assistant/system)
/// - Zero or more associated tool calls with their results
///
/// The output is a single `<article>` element with tool badges in the header.
pub(super) fn render_message_group(
    group: &MessageGroup,
    index: usize,
    options: &RenderOptions,
) -> Result<String, RenderError> {
    let started = Instant::now();
    trace!(
        component = "renderer",
        operation = "render_message_group",
        index = index,
        group_type = ?group.group_type,
        tool_count = group.tool_count(),
        "Rendering message group"
    );

    // Role class based on group type
    let role_class = match group.group_type {
        MessageGroupType::User => "message-user",
        MessageGroupType::Assistant => "message-assistant",
        MessageGroupType::System => "message-system",
        MessageGroupType::ToolOnly => "message-tool",
    };

    // Role icon
    let role_icon = match group.group_type {
        MessageGroupType::User => ICON_USER,
        MessageGroupType::Assistant => ICON_BOT,
        MessageGroupType::System => ICON_SETTINGS,
        MessageGroupType::ToolOnly => ICON_WRENCH,
    };

    // Author display
    let author_display = group
        .primary
        .author
        .as_ref()
        .map(|a| html_escape(a))
        .unwrap_or_else(|| match group.group_type {
            MessageGroupType::User => "You".to_string(),
            MessageGroupType::Assistant => "Assistant".to_string(),
            MessageGroupType::System => "System".to_string(),
            MessageGroupType::ToolOnly => "Tool".to_string(),
        });

    // Message anchor
    let anchor_id = group
        .primary
        .index
        .or(Some(index))
        .map(|idx| format!(r#" id="msg-{}""#, idx))
        .unwrap_or_default();

    // Timestamp
    let timestamp_html = if options.show_timestamps {
        if let Some(ts) = &group.start_timestamp {
            format!(
                r#"<time class="message-time" datetime="{}">{}</time>"#,
                html_escape(ts),
                html_escape(&format_timestamp(ts))
            )
        } else {
            String::new()
        }
    } else {
        String::new()
    };

    // Render content
    let content_html = render_content(&group.primary.content, options);

    // Render tool badges with overflow handling
    let (tool_badges_html, overflow_count) =
        if options.show_tool_calls && !group.tool_calls.is_empty() {
            render_tool_badges_with_overflow(&group.tool_calls, options)
        } else {
            (String::new(), 0)
        };

    // ARIA label for the article
    let aria_label = if group.tool_calls.is_empty() {
        format!("{} message", group.group_type.role_icon())
    } else {
        format!(
            "{} message with {} tool call{}",
            group.group_type.role_icon(),
            group.tool_calls.len(),
            if group.tool_calls.len() == 1 { "" } else { "s" }
        )
    };

    // Check for content collapse
    let content_bytes = group.primary.content.len();
    let mut content_chars = 0; // Calculated lazily
    let should_collapse =
        options.collapse_threshold > 0 && content_bytes > options.collapse_threshold && {
            let mut chars = group.primary.content.chars();
            let mut count = 0;
            while count <= options.collapse_threshold && chars.next().is_some() {
                count += 1;
            }
            content_chars = if count > options.collapse_threshold {
                // We know it exceeds, but we need the full count for display
                count + chars.count()
            } else {
                count
            };
            content_chars > options.collapse_threshold
        };

    let (content_wrapper_start, content_wrapper_end) = if should_collapse {
        let preview_chars = options.collapse_threshold.min(500);
        let safe_len = byte_index_for_char_count(&group.primary.content, preview_chars);
        let preview = group.primary.content.get(..safe_len).unwrap_or("");
        (
            format!(
                r#"<details class="message-collapse">
                    <summary>
                        <span class="message-preview">{}</span>
                        <span class="message-expand-hint">Click to expand ({} chars)</span>
                    </summary>
                    <div class="message-expanded">"#,
                html_escape(preview),
                content_chars
            ),
            "</div></details>".to_string(),
        )
    } else {
        (String::new(), String::new())
    };

    // Only render content div if there's actual content
    let content_section = if content_html.trim().is_empty() {
        String::new()
    } else {
        format!(
            r#"
                <div class="message-content">
                    {wrapper_start}{content}{wrapper_end}
                </div>"#,
            wrapper_start = content_wrapper_start,
            content = content_html,
            wrapper_end = content_wrapper_end,
        )
    };

    // Tool badges container with accessibility
    let tool_container = if !tool_badges_html.is_empty() {
        format!(
            r#"<div class="message-header-right" role="group" aria-label="Tool calls{}">
                        {badges}
                    </div>"#,
            if overflow_count > 0 {
                format!(" ({} shown, {} more)", MAX_VISIBLE_BADGES, overflow_count)
            } else {
                String::new()
            },
            badges = tool_badges_html,
        )
    } else {
        r#"<div class="message-header-right"></div>"#.to_string()
    };

    let rendered = format!(
        r#"            <article class="message {role_class}"{anchor} role="article" aria-label="{aria_label}">
                <header class="message-header">
                    <div class="message-header-left">
                        <span class="message-icon" aria-hidden="true">{role_icon}</span>
                        <span class="message-author">{author}</span>
                        {timestamp}
                    </div>
                    {tool_container}
                </header>{content_section}
            </article>"#,
        role_class = role_class,
        anchor = anchor_id,
        aria_label = html_escape(&aria_label),
        role_icon = role_icon,
        author = author_display,
        timestamp = timestamp_html,
        tool_container = tool_container,
        content_section = content_section,
    );

    debug!(
        component = "renderer",
        operation = "render_message_group_complete",
        index = index,
        duration_ms = started.elapsed().as_millis(),
        bytes = rendered.len(),
        "Message group rendered"
    );

    Ok(rendered)
}

/// Render tool badges with overflow handling.
///
/// When there are more than `MAX_VISIBLE_BADGES` tool calls, shows the first N
/// badges plus a "+X more" overflow indicator.
pub(super) fn render_tool_badges_with_overflow(
    tools: &[ToolCallWithResult],
    _options: &RenderOptions,
) -> (String, usize) {
    if tools.is_empty() {
        return (String::new(), 0);
    }

    if tools.len() <= MAX_VISIBLE_BADGES {
        // Render all badges
        let badges: String = tools
            .iter()
            .map(|tool| render_single_tool_badge(tool, false))
            .collect::<Vec<_>>()
            .join("\n                        ");
        (badges, 0)
    } else {
        // Render all badges so the overflow control can reveal the extra tools.
        // The extra badges are hidden by CSS until the header is expanded.
        let badges: String = tools
            .iter()
            .enumerate()
            .map(|(idx, tool)| render_single_tool_badge(tool, idx >= MAX_VISIBLE_BADGES))
            .collect::<Vec<_>>()
            .join("\n                        ");

        let overflow_count = tools.len() - MAX_VISIBLE_BADGES;
        let overflow_badge = format!(
            r#"<button class="tool-badge tool-overflow"
                    aria-label="{count} more tool{s}"
                    aria-expanded="false"
                    data-overflow-count="{count}">
                <span class="tool-badge-text">+{count}</span>
            </button>"#,
            count = overflow_count,
            s = if overflow_count == 1 { "" } else { "s" },
        );

        (
            format!("{}\n                        {}", badges, overflow_badge),
            overflow_count,
        )
    }
}

/// Render a single tool badge as a button with Lucide SVG icon.
pub(super) fn render_single_tool_badge(tool: &ToolCallWithResult, overflow_extra: bool) -> String {
    let icon = get_tool_lucide_icon(&tool.call.name);
    let status = tool.effective_status();
    let status_class = status.css_class();
    let status_label = status.label();
    let status_icon = status.icon_svg();
    let overflow_extra_class = if overflow_extra {
        " tool-overflow-extra"
    } else {
        ""
    };

    // Format input/output for popover (full content, pretty-printed if JSON)
    let formatted_input = format_json_or_raw(&tool.call.input);
    let formatted_output = tool
        .result
        .as_ref()
        .map(|r| format_json_or_raw(&r.content))
        .unwrap_or_default();

    let popover_input = if !formatted_input.trim().is_empty() {
        format!(
            r#"<div class="tool-popover-section"><span class="tool-popover-label">Input</span><pre><code>{}</code></pre></div>"#,
            html_escape(&formatted_input)
        )
    } else {
        String::new()
    };

    let popover_output = if !formatted_output.trim().is_empty() {
        format!(
            r#"<div class="tool-popover-section"><span class="tool-popover-label">Output</span><pre><code>{}</code></pre></div>"#,
            html_escape(&formatted_output)
        )
    } else {
        String::new()
    };

    let status_badge = if !status_label.is_empty() {
        format!(
            r#"<span class="tool-badge-status {}">{}</span>"#,
            status_label, status_icon
        )
    } else {
        String::new()
    };

    format!(
        r#"<button class="tool-badge {status_class}{overflow_extra_class}"
                aria-label="{name}: {status_label}"
                aria-expanded="false"
                data-tool-name="{name}">
            <span class="tool-badge-icon">{icon}</span>
            <span class="tool-badge-status">{status_icon}</span>
            <div class="tool-popover" role="tooltip">
                <div class="tool-popover-header">{icon} <span>{name}</span> {status_badge}</div>
                {input}{output}
            </div>
        </button>"#,
        status_class = status_class,
        overflow_extra_class = overflow_extra_class,
        name = html_escape(&tool.call.name),
        status_label = status_label,
        icon = icon,
        status_icon = status_icon,
        status_badge = status_badge,
        input = popover_input,
        output = popover_output,
    )
}

/// Get the appropriate Lucide SVG icon for a tool by name.
pub(super) fn get_tool_lucide_icon(tool_name: &str) -> &'static str {
    match tool_name.to_lowercase().as_str() {
        "bash" | "shell" | "terminal" => ICON_TERMINAL,
        "read" | "read_file" | "readfile" => ICON_FILE_TEXT,
        "write" | "write_file" | "writefile" | "edit" => ICON_PENCIL,
        "glob" | "find" | "grep" | "search" | "websearch" => ICON_SEARCH,
        "webfetch" | "fetch" | "http" | "curl" => ICON_GLOBE,
        "task" | "agent" => ICON_SPARKLES,
        n if n.starts_with("mcp__mcp-agent-mail") => ICON_MAIL,
        n if n.contains("sql") || n.contains("db") || n.contains("database") => ICON_DATABASE,
        _ => ICON_WRENCH,
    }
}
